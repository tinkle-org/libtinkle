package com.tinkle.sdk

import java.nio.ByteBuffer
import org.junit.Assert.*
import org.junit.Test
import kotlinx.coroutines.CancellationException

/** Pure policy checks: these do not load a Rust library or claim Android runtime evidence. */
class AndroidVideoPolicyTest {
    private val limits = FfiVideoDecodeLimits(1024u, 4096u, 30u, 1000u, 32u, 1000u)

    @Test fun avcInOtherContainersCannotBecomeMp4Evidence() {
        AndroidVideoPolicy.requireMp4(byteArrayOf(0, 0, 0, 32) + "ftypisom".toByteArray())
        assertThrows(FfiMediaException.Unsupported::class.java) {
            AndroidVideoPolicy.requireMp4(byteArrayOf(0, 0, 0, 32) + "ftypqt  ".toByteArray())
        }
        assertThrows(FfiMediaException.Unsupported::class.java) { AndroidVideoPolicy.requireMp4(ByteArray(12)) }
    }

    @Test fun measuresDecodedCadenceAndIncludesLastFrame() {
        val timing = VideoFrameTiming(limits)
        listOf(0L, 33333L, 66667L).forEach(timing::add)
        assertEquals(3u, timing.count)
        assertEquals(101uL, timing.durationMs())
    }

    @Test fun rejectsUnmeasurableDiscontinuousAndOverBudgetTimelines() {
        assertThrows(FfiMediaException.Unsupported::class.java) { VideoFrameTiming(limits).apply { add(0) }.durationMs() }
        assertThrows(FfiMediaException.Unsupported::class.java) { VideoFrameTiming(limits).add(10) }
        assertThrows(FfiMediaException.Invalid::class.java) { VideoFrameTiming(limits).apply { add(0); add(0) } }
        assertThrows(FfiMediaException.Unsupported::class.java) {
            VideoFrameTiming(limits).apply { add(0); add(1000); add(3000) }
        }
        assertThrows(FfiMediaException.Budget::class.java) {
            VideoFrameTiming(limits.copy(maxFrames = 1u)).apply { add(0); add(1000) }
        }
        assertThrows(FfiMediaException.Budget::class.java) {
            VideoFrameTiming(limits).apply { add(0); add(600000) }.durationMs()
        }
        assertThrows(FfiMediaException.Budget::class.java) { VideoFrameTiming(limits).apply { add(0); add(Long.MAX_VALUE) } }
    }

    @Test fun containerCannotExtendFinalFrameBeyondMeasuredCadenceOrBudget() {
        val timing = VideoFrameTiming(limits).apply { add(0); add(100000); add(200000); add(300000) }
        timing.verifyTrackEnd(400000)
        assertThrows(FfiMediaException.Unsupported::class.java) { timing.verifyTrackEnd(0) }
        assertThrows(FfiMediaException.Unsupported::class.java) { timing.verifyTrackEnd(900000) }
        assertThrows(FfiMediaException.Budget::class.java) { timing.verifyTrackEnd(1000001) }
    }

    @Test fun verifiesSourceProfilesBeforeAnyDecoderDownconversion() {
        // Annex-B SPS prefixes with baseline/main/high 8-bit 4:2:0 syntax.
        for (profile in listOf(66, 77)) AndroidVideoPolicy.requireAvcSps(sps(profile, 0x80))
        AndroidVideoPolicy.requireAvcSps(sps(100, 0xac)) // SPS id=0, chroma=1, bit depths=0.
        assertThrows(FfiMediaException.Unsupported::class.java) { AndroidVideoPolicy.requireAvcSps(sps(110, 0x80)) }
        assertThrows(FfiMediaException.Unsupported::class.java) { AndroidVideoPolicy.requireAvcSps(sps(100, 0x90)) }
        assertThrows(FfiMediaException.Invalid::class.java) {
            AndroidVideoPolicy.requireAvcSps(byteArrayOf(0, 0, 0, 1, 0x67, 100))
        }
        assertThrows(FfiMediaException.Unsupported::class.java) { AndroidVideoPolicy.requireAvcSps(byteArrayOf(1, 2, 3)) }
        val accessUnit = byteArrayOf(0, 0, 0, 1, 0x65, 0x11) + sps(110, 0x80)
        assertThrows(FfiMediaException.Unsupported::class.java) {
            AndroidVideoPolicy.checkInBandSps(ByteBuffer.wrap(accessUnit), accessUnit.size)
        }
    }

    @Test fun mapsClockwiseDisplayRotationWithoutLosingPixels() {
        assertEquals(0 to 1, AndroidVideoPolicy.unrotate(0, 0, 3, 2, 90))
        assertEquals(2 to 0, AndroidVideoPolicy.unrotate(1, 2, 3, 2, 90))
        assertEquals(2 to 1, AndroidVideoPolicy.unrotate(0, 0, 3, 2, 180))
        assertEquals(2 to 0, AndroidVideoPolicy.unrotate(0, 0, 3, 2, 270))
        for (rotation in listOf(0, 90, 180, 270)) {
            val width = if (rotation % 180 == 0) 3 else 2
            val height = if (rotation % 180 == 0) 2 else 3
            val pixels = (0 until width).flatMap { x -> (0 until height).map { y ->
                AndroidVideoPolicy.unrotate(x, y, 3, 2, rotation)
            } }.toSet()
            assertEquals(6, pixels.size)
            assertTrue(pixels.all { it.first in 0..2 && it.second in 0..1 })
        }
    }

    @Test fun derivesExplicitOpaqueSrgbRatherThanRelabelingVideoGamma() {
        val pixels = ByteArray(12)
        AndroidVideoPolicy.writeSrgb(pixels, 0, 16, 128, 128, true)
        AndroidVideoPolicy.writeSrgb(pixels, 4, 235, 128, 128, true)
        AndroidVideoPolicy.writeSrgb(pixels, 8, 126, 128, 128, true)
        assertArrayEquals(byteArrayOf(0, 0, 0, -1), pixels.copyOfRange(0, 4))
        assertArrayEquals(byteArrayOf(-1, -1, -1, -1), pixels.copyOfRange(4, 8))
        assertTrue((pixels[8].toInt() and 255) > 128) // inverse BT.709 -> linear -> sRGB.
        assertEquals(pixels[8], pixels[9])
        assertEquals(pixels[9], pixels[10])
        assertEquals(-1, pixels[11].toInt())
    }

    @Test fun cleanupPreservesCancellationAndAttemptsAllReleases() {
        val cancellation = CancellationException("cancelled")
        val releases = mutableListOf<String>()
        val thrown = assertThrows(CancellationException::class.java) {
            withVideoCleanup({ releases.add("extractor") }) {
                withVideoCleanup({
                    withVideoCleanup({ releases.add("release"); error("release failure") }) {
                        releases.add("stop"); error("stop failure")
                    }
                }) { throw cancellation }
            }
        }
        assertSame(cancellation, thrown)
        assertEquals(listOf("stop", "release", "extractor"), releases)
        assertEquals("stop failure", thrown.suppressed.single().message)
        assertEquals("release failure", thrown.suppressed.single().suppressed.single().message)
    }

    /** Minimal SPS prefix; full malformed bitstreams are MediaCodec's responsibility. */
    private fun sps(profile: Int, fields: Int) = byteArrayOf(0, 0, 0, 1, 0x67, profile.toByte(), 0, 30, fields.toByte())
}
