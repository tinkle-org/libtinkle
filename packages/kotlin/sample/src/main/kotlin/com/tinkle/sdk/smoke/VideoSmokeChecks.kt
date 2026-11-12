package com.tinkle.sdk.smoke

import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Color
import android.media.ExifInterface
import android.media.MediaExtractor
import android.media.MediaFormat
import android.media.MediaCodec
import android.media.MediaMuxer
import android.os.Looper
import android.util.Base64
import com.tinkle.sdk.*
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.PI
import kotlin.math.sin
import kotlinx.coroutines.*

/** Keep SDK/coroutine calls in the target APK: the test APK cannot call R8-renamed APIs. */
public object MediaSmokeChecks {
    @JvmStatic public fun pcmVoiceAndImportedAudio(context: Context): Unit = runBlocking {
        TinkleSdk.initialize()
        val source = File.createTempFile("voice-", ".pcm", context.cacheDir)
        try {
            source.outputStream().use { output ->
                val block = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN)
                for (i in 0 until 19200) {
                    if (block.remaining() < 2) { output.write(block.array(), 0, block.position()); block.clear() }
                    block.putShort((sin(i * 2.0 * PI * 440 / 48000) * 16000).toInt().toShort())
                }
                output.write(block.array(), 0, block.position())
            }
            prepareNativeMedia(source.path, FfiNativeMediaKind.Voice(48000u, 1u, 16u), AndroidMediaPreparer()).use { voice ->
                check(voice.mimeType() == "audio/mp4" && voice.facts().audioSamples == 19200uL)
                check(voice.facts().width == 0u && voice.facts().height == 0u && voice.facts().audioChannels == 1.toUByte())
                check(voice.waveform().size == 100 && voice.sourceSha256().size == 32)
                requireAacOnly(File(voice.path()))
                prepareNativeMedia(voice.path(), FfiNativeMediaKind.Audio, AndroidMediaPreparer()).use { audio ->
                    check(audio.waveform().isEmpty() && audio.facts().audioChannels == 1.toUByte())
                    requireAacOnly(File(audio.path()))
                }
            }
        } finally { source.delete() }
    }

    @JvmStatic public fun alphaPngAndOrientedJpeg(context: Context): Unit = runBlocking {
        TinkleSdk.initialize()
        val bitmap = Bitmap.createBitmap(32, 16, Bitmap.Config.ARGB_8888)
        bitmap.eraseColor(0x80204060.toInt())
        try {
            for (jpeg in listOf(false, true)) {
                val source = File.createTempFile("image-", if (jpeg) ".jpg" else ".png", context.cacheDir)
                try {
                    source.outputStream().use { check(bitmap.compress(if (jpeg) Bitmap.CompressFormat.JPEG else Bitmap.CompressFormat.PNG, 100, it)) }
                    if (jpeg) ExifInterface(source.path).apply { setAttribute(ExifInterface.TAG_ORIENTATION, "6"); saveAttributes() }
                    prepareNativeMedia(source.path, FfiNativeMediaKind.Image, AndroidMediaPreparer()).use { result ->
                        check(result.mimeType() == "image/png" && File(result.path()).length() in 1..10_000_000)
                        check(result.facts().width == (if (jpeg) 16u else 32u) && result.facts().height == (if (jpeg) 32u else 16u))
                        val decoded = BitmapFactory.decodeFile(result.path()) ?: error("PNG output did not decode")
                        try { if (!jpeg) check(Color.alpha(decoded.getPixel(0, 0)) == 128) } finally { decoded.recycle() }
                    }
                } finally { source.delete() }
            }
        } finally { bitmap.recycle() }
    }

    /** Normal host wiring encodes before offline upload; retained retries need no encoder/source. */
    @JvmStatic public fun normalVoiceAndAdoption(context: Context): Unit = runBlocking {
        TinkleSdk.initialize()
        val directory = File(context.cacheDir, "sending-${System.nanoTime()}")
        check(directory.mkdir())
        val storage = SqliteStorage.openAsync(File(directory, "cache.sqlite").path)
        val trust = TrustSourceFfi.bootstrap(ServerTrustFfi(ByteArray(32) { 7 }, 7u, "native-sending-test"))
        val client = TinkleClient.newSqlite(ClientConfigFfi(ClientInfoFfi("fixture", "android", "1", "1", "1",
            ClientOptionsFfi("", "", "", "", "", "")), "127.0.0.1", 1u, null), trust, storage)
        try {
            client.setPrincipalUserId(42)
            client.configureAndroidMediaPreparer()
            val source = File(directory, "voice.pcm")
            val pcm = ByteBuffer.allocate(19200 * 2).order(ByteOrder.LITTLE_ENDIAN)
            repeat(19200) { pcm.putShort((sin(it * 2.0 * PI * 440 / 48000) * 16000).toInt().toShort()) }
            source.writeBytes(pcm.array())
            val kind = FfiMediaKind.Voice(48000u, 1u, 16u)
            suspend fun prepare(): FfiAttachmentManifest {
                try {
                    TransferControl().use { client.sendingPrepareAttachment("voice", source.path, kind, it) }
                    error("offline upload unexpectedly completed")
                } catch (e: FfiSendException.Rpc) { check(e.error is FfiException.Sync) }
                return checkNotNull(client.sendingAttachmentManifest("voice"))
            }
            val manifest = prepare()
            check(manifest.mimeType == "audio/mp4" && manifest.nativeFacts?.audioSamples == 19200uL)
            prepareNativeMedia(source.path, FfiNativeMediaKind.Voice(48000u, 1u, 16u), AndroidMediaPreparer()).use { artifact ->
                try {
                    TransferControl().use { client.sendingAdoptNativeAttachment("adopt", source.path, artifact, it) }
                    error("offline adoption unexpectedly completed")
                } catch (e: FfiSendException.Rpc) { check(e.error is FfiException.Sync) }
                check(checkNotNull(client.sendingAttachmentManifest("adopt")).outputSha256.contentEquals(artifact.outputSha256()))
            }
            client.clearNativeMediaPreparer()
            check(source.delete())
            val retry = prepare()
            check(retry.artifactPath == manifest.artifactPath && retry.outputSha256.contentEquals(manifest.outputSha256))
        } finally {
            withContext(NonCancellable) { client.shutdown() }
            client.close()
            storage.clearPrincipal("user:42")
            storage.close()
            trust.close()
            directory.deleteRecursively()
        }
    }

    /** Independently inspect the encoded track/profile instead of trusting returned MIME metadata. */
    private fun requireAacOnly(file: File) {
        val extractor = MediaExtractor()
        try {
            extractor.setDataSource(file.path)
            check(extractor.trackCount == 1)
            val format = extractor.getTrackFormat(0)
            check(format.getString(MediaFormat.KEY_MIME) == "audio/mp4a-latm")
            val config = checkNotNull(format.getByteBuffer("csd-0"))
            check(config.remaining() >= 2 && (config.get(config.position()).toInt() and 255) ushr 3 == 2)
        } finally { extractor.release() }
    }
}

/** Real MP4 checks in the target APK, so R8 processes the actual adapter and native call sites. */
public object VideoSmokeChecks {
    // Kotlin 2.3 boxes the inferred-width UInt product as Long, then casts it to ULong.
    private val limits = FfiVideoDecodeLimits(1024uL * 1024uL, 4096uL, 10u, 1000uL, 32u, 5000u)

    /** Decodes four actual AVC frames and validates bounded derived sRGB, not header facts. */
    @JvmStatic public fun decodePlatform(context: Context): Unit = runBlocking {
        fixture(context) { source ->
            val input = LocalLease(source)
            val decoded = AndroidVideoDecoder().inspectAndDecode(input, limits)
            check(input.uniffiIsDestroyed)
            check(decoded.evidence == FfiVideoEvidence.NATIVE_DECODED)
            check(decoded.width == 64u && decoded.height == 48u)
            check(decoded.frameCount == 4u && decoded.durationMs == 400uL)
            check(decoded.bitDepth == 8.toUByte() && decoded.hasAlpha == false)
            val thumb = checkNotNull(decoded.thumbnail)
            check(thumb.width == 32u && thumb.height == 24u && thumb.rgba.size == 32 * 24 * 4)
            check((thumb.rgba[0].toInt() and 255) > 200 && (thumb.rgba[1].toInt() and 255) < 70)
            // Frame zero is red/green; every later frame is blue. A nearby/random frame fails.
            val right = (32 - 1) * 4
            check((thumb.rgba[right + 1].toInt() and 255) > (thumb.rgba[right].toInt() and 255))
            check(thumb.rgba.filterIndexed { i, _ -> i % 4 == 3 }.all { it == (-1).toByte() })
        }
    }

    /** Exercises container display rotation against a real decoded frame, not metadata alone. */
    @JvmStatic public fun decodeDisplayRotation(context: Context): Unit = runBlocking {
        fixture(context) { source ->
            val rotated = File(source.parentFile, "rotated.mp4")
            val extractor = MediaExtractor()
            try {
                extractor.setDataSource(source.path)
                extractor.selectTrack(0)
                val muxer = MediaMuxer(rotated.path, MediaMuxer.OutputFormat.MUXER_OUTPUT_MPEG_4)
                var started = false
                try {
                    muxer.setOrientationHint(90)
                    val track = muxer.addTrack(extractor.getTrackFormat(0))
                    muxer.start()
                    started = true
                    val buffer = ByteBuffer.allocate(65536)
                    while (extractor.sampleTime >= 0) {
                        buffer.clear()
                        val count = extractor.readSampleData(buffer, 0)
                        check(count in 1..buffer.capacity())
                        val flags = if (extractor.sampleFlags and MediaExtractor.SAMPLE_FLAG_SYNC != 0) MediaCodec.BUFFER_FLAG_KEY_FRAME else 0
                        val info = MediaCodec.BufferInfo().apply { set(0, count, extractor.sampleTime, flags) }
                        muxer.writeSampleData(track, buffer, info)
                        extractor.advance()
                    }
                } finally {
                    try { if (started) muxer.stop() } finally { muxer.release() }
                }
            } finally { extractor.release() }
            val decoded = AndroidVideoDecoder().inspectAndDecode(LocalLease(rotated), limits)
            check(decoded.width == 48u && decoded.height == 64u && decoded.frameCount == 4u)
            val thumbnail = checkNotNull(decoded.thumbnail)
            check(thumbnail.width == 24u && thumbnail.height == 32u)
            val bottom = (32 - 1) * 24 * 4
            check((thumbnail.rgba[0].toInt() and 255) > 200)
            check((thumbnail.rgba[bottom + 1].toInt() and 255) > (thumbnail.rgba[bottom].toInt() and 255))
        }
    }

    /** Resource cleanup is exercised for pixel/frame/deadline budgets and malformed input. */
    @JvmStatic public fun rejectsInvalidAndOverBudget(context: Context): Unit = runBlocking {
        fixture(context) { source ->
            for (budget in listOf(limits.copy(maxPixels = 1u), limits.copy(maxFrames = 2u), limits.copy(deadlineMs = 0u))) {
                val input = LocalLease(source)
                try {
                    AndroidVideoDecoder().inspectAndDecode(input, budget)
                    error("Budget must fail")
                } catch (_: FfiMediaException.Budget) { }
                check(input.uniffiIsDestroyed && source.exists())
            }
            source.writeText("not an MP4")
            val input = LocalLease(source)
            try {
                AndroidVideoDecoder().inspectAndDecode(input, limits)
                error("Malformed media must fail")
            } catch (_: FfiMediaException) { }
            check(input.uniffiIsDestroyed)
        }
    }

    /** A cancelled Kotlin waiter cannot release a lease while codec work is still executing. */
    @JvmStatic public fun cancellationJoinsWorkerBeforeClosingLease(context: Context): Unit = runBlocking {
        fixture(context) { source ->
            val entered = CountDownLatch(1)
            val unblock = CountDownLatch(1)
            val input = object : LocalLease(source) {
                private val polls = AtomicInteger()
                override fun isCancelled(): Boolean {
                    check(source.isFile && !uniffiIsDestroyed)
                    // Poll 3 is the first drain iteration, after MediaCodec.start().
                    if (polls.incrementAndGet() == 3) {
                        entered.countDown()
                        check(unblock.await(5, TimeUnit.SECONDS))
                    }
                    return false
                }
            }
            val work = async(Dispatchers.Default) { AndroidVideoDecoder().inspectAndDecode(input, limits) }
            try {
                check(entered.await(5, TimeUnit.SECONDS))
                work.cancel()
                check(!input.uniffiIsDestroyed && source.isFile)
                check(!work.isCompleted) // cancellation request is not completion of stop/release.
            } finally {
                unblock.countDown()
                work.cancelAndJoin()
            }
            check(input.uniffiIsDestroyed)
            // No orphan codec task prevents the following real decode.
            AndroidVideoDecoder().inspectAndDecode(LocalLease(source), limits)
        }
    }

    /**
     * Requires the final native AAR: real Rust callback suspension, typed failure mapping,
     * preparer registration, cancellation and opaque-source lifetime. No server/upload needed:
     * the success probe deliberately returns Codec after checking real encoded results.
     */
    @JvmStatic public fun nativeCallbackAndLease(context: Context): Unit = runBlocking {
        TinkleSdk.initialize()
        fixture(context) { source ->
            val db = File(source.parentFile, "video.sqlite")
            SqliteStorage.openAsync(db.path).use { storage ->
                TrustSourceFfi.bootstrap(ServerTrustFfi(ByteArray(32) { 7 }, 1u, "android-video-fixture")).use { trust ->
                    val config = ClientConfigFfi(
                        ClientInfoFfi("video-fixture", "android", "24+", "1", "1", ClientOptionsFfi("", "", "", "", "", "")),
                        "127.0.0.1", 1u, null,
                    )
                    val client = TinkleClient.newSqlite(config, trust, storage)
                    try {
                        client.setPrincipalUserId(42)
                        // Decoder-only registration cannot satisfy normal sending's encoder contract.
                        client.configureAndroidVideoDecoder()
                        TransferControl().use { control ->
                            expectMedia<FfiMediaException.Unsupported> { client.sendingPrepareAttachment("missing", source.path, FfiMediaKind.Video, control) }
                        }
                        client.clearVideoDecoder()
                        client.configureAndroidMediaPreparer()
                        client.clearNativeMediaPreparer()
                        val result = AtomicReference<FfiNativeMediaOutput>()
                        val lease = AtomicReference<FfiNativeMediaInput>()
                        val frozenPath = AtomicReference<String>()
                        client.configureNativeMediaPreparer(object : FfiPlatformMediaPreparer {
                            override suspend fun prepare(input: FfiNativeMediaInput): FfiNativeMediaOutput {
                                lease.set(input)
                                frozenPath.set(input.sourcePath())
                                var handedOff = false
                                try {
                                    delay(10) // Exercise the generated foreign-future suspension machinery.
                                    handedOff = true
                                    result.set(AndroidMediaPreparer().prepare(input))
                                    throw FfiMediaException.Codec() // Stop before UploadStart; preserve typed errors.
                                } finally {
                                    // Once handed off, only the adapter can establish native release.
                                    if (!handedOff) input.close()
                                }
                            }
                        })
                        TransferControl().use { control ->
                            expectMedia<FfiMediaException.Codec> { client.sendingPrepareAttachment("decoded", source.path, FfiMediaKind.Video, control) }
                        }
                        check(result.get().frameCount == 4u)
                        check(lease.get().uniffiIsDestroyed)
                        withTimeout(5000) { while (File(frozenPath.get()).exists()) delay(10) }
                        nativeCancellation(client, source)
                    } finally {
                        withContext(NonCancellable) { client.shutdown() }
                        client.close()
                    }
                }
            }
        }
    }

    /** Holds an actual native lease while an actual started codec waits at a controlled poll. */
    private suspend fun nativeCancellation(client: TinkleClient, source: File) = coroutineScope {
        val entered = CountDownLatch(1)
        val unblock = CountDownLatch(1)
        val finished = CountDownLatch(1)
        val lease = AtomicReference<FfiNativeMediaInput>()
        val frozenPath = AtomicReference<String>()
        client.configureNativeMediaPreparer(object : FfiPlatformMediaPreparer {
            override suspend fun prepare(input: FfiNativeMediaInput): FfiNativeMediaOutput {
                lease.set(input)
                frozenPath.set(input.sourcePath())
                val polls = AtomicInteger()
                // A test wrapper keeps the *object*, not just its pathname, through native cleanup.
                val held = object : FfiNativeMediaInput(NoHandle) {
                    override fun sourcePath() = input.sourcePath()
                    override fun sizeBytes() = input.sizeBytes()
                    override fun outputPath() = input.outputPath()
                    override fun kind() = input.kind()
                    override fun maxOutputBytes() = input.maxOutputBytes()
                    override fun maxFrames() = input.maxFrames()
                    override fun maxDurationMs() = input.maxDurationMs()
                    override fun deadlineMs() = input.deadlineMs()
                    override fun isCancelled(): Boolean {
                        check(File(frozenPath.get()).isFile && !input.uniffiIsDestroyed)
                        if (polls.incrementAndGet() == 3) {
                            entered.countDown()
                            check(unblock.await(5, TimeUnit.SECONDS))
                        }
                        return input.isCancelled()
                    }
                    override fun close() { input.close(); super.close() }
                }
                try {
                    return AndroidMediaPreparer().prepare(held)
                } finally { finished.countDown() }
            }
        })
        TransferControl().use { control ->
            val operation = async(Dispatchers.Default) {
                expectMedia<FfiMediaException.Cancelled> { client.sendingPrepareAttachment("cancelled", source.path, FfiMediaKind.Video, control) }
            }
            try {
                check(entered.await(5, TimeUnit.SECONDS))
                control.cancel()
                withTimeout(5000) { operation.await() }
                // Rust's waiter has returned, but its source still belongs to the running codec.
                check(!lease.get().uniffiIsDestroyed && File(frozenPath.get()).isFile)
                check(finished.count == 1L)
            } finally {
                unblock.countDown()
                withContext(NonCancellable) {
                    operation.cancelAndJoin()
                    check(finished.await(5, TimeUnit.SECONDS))
                }
            }
            check(lease.get().uniffiIsDestroyed)
            withTimeout(5000) { while (File(frozenPath.get()).exists()) delay(10) }
        }
    }

    /** Assert the Rust sending error preserves the callback's typed media variant. */
    private suspend inline fun <reified E : FfiMediaException> expectMedia(block: () -> Unit) {
        try { block(); error("Expected typed media failure") }
        catch (error: FfiSendException.Media) { check(error.error is E) }
    }

    /** Local lease is a platform test double, never used as evidence of native callback behavior. */
    private open class LocalLease(private val source: File) : FfiVideoInput(NoHandle) {
        override fun sourcePath() = source.path
        override fun sizeBytes() = source.length().toULong()
        override fun sourceSha256(): ByteArray = MessageDigest.getInstance("SHA-256").digest(source.readBytes())
        override fun isCancelled(): Boolean { check(Looper.myLooper() != Looper.getMainLooper()); return false }
    }

    /** Four real 64x48 AVC-baseline frames, 10 fps, tagged BT.709 limited SDR. */
    private suspend fun fixture(context: Context, block: suspend (File) -> Unit) {
        val dir = File(context.cacheDir, "video-${System.nanoTime()}")
        check(dir.mkdirs())
        val source = File(dir, "red.mp4")
        source.writeBytes(Base64.decode(MP4, Base64.DEFAULT))
        try {
            // Check fixture identity through the real extractor before handing it to the adapter.
            val extractor = MediaExtractor()
            try {
                extractor.setDataSource(source.path)
                check(extractor.getTrackFormat(0).getString(MediaFormat.KEY_MIME) == "video/avc")
            } finally { extractor.release() }
            block(source)
        } finally { dir.deleteRecursively() }
    }

    // Reproduce with python3 scripts/make-video-fixture.py (existing ffmpeg required).
    // Frame zero is red on the left and green on the right; frames 1..3 are blue.
    private const val MP4 = "AAAAIGZ0eXBpc29tAAACAGlzb21pc28yYXZjMW1wNDEAAANNbW9vdgAAAGxtdmhkAAAAAAAAAAAAAAAAAAAD6AAAAZAAAQAAAQAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAnd0cmFrAAAAXHRraGQAAAADAAAAAAAAAAAAAAABAAAAAAAAAZAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAABAAAAAAEAAAAAwAAAAAAAkZWR0cwAAABxlbHN0AAAAAAAAAAEAAAGQAAAAAAABAAAAAAHvbWRpYQAAACBtZGhkAAAAAAAAAAAAAAAAAAAoAAAAEABVxAAAAAAALWhkbHIAAAAAAAAAAHZpZGUAAAAAAAAAAAAAAABWaWRlb0hhbmRsZXIAAAABmm1pbmYAAAAUdm1oZAAAAAEAAAAAAAAAAAAAACRkaW5mAAAAHGRyZWYAAAAAAAAAAQAAAAx1cmwgAAAAAQAAAVpzdGJsAAAAznN0c2QAAAAAAAAAAQAAAL5hdmMxAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAEAAMABIAAAASAAAAAAAAAABFUxhdmM2Mi4yOC4xMDIgbGlieDI2NAAAAAAAAAAAAAAAGP//AAAAMWF2Y0MBQsAK/+EAGWdCwArZBHsBagICAoAAAAMAgAAACgeJEyQBAAVoy4PLIAAAABNjb2xybmNseAABAAEAAQAAAAAQcGFzcAAAAAEAAAABAAAAFGJ0cnQAAAAAAAA4LAAAAAAAAAAYc3R0cwAAAAAAAAABAAAABAAABAAAAAAYc3RzcwAAAAAAAAACAAAAAQAAAAIAAAAcc3RzYwAAAAAAAAABAAAAAQAAAAQAAAABAAAAJHN0c3oAAAAAAAAAAAAAAAQAAAKaAAAAIQAAAAoAAAAKAAAAFHN0Y28AAAAAAAAAAQAAA30AAABidWR0YQAAAFptZXRhAAAAAAAAACFoZGxyAAAAAAAAAABtZGlyYXBwbAAAAAAAAAAAAAAAAC1pbHN0AAAAJal0b28AAAAdZGF0YQAAAAEAAAAATGF2ZjYyLjEyLjEwMgAAAAhmcmVlAAAC121kYXQAAAJvBgX//2vcRem95tlIt5Ys2CDZI+7veDI2NCAtIGNvcmUgMTY1IHIzMjIyIGIzNTYwNWEgLSBILjI2NC9NUEVHLTQgQVZDIGNvZGVjIC0gQ29weWxlZnQgMjAwMy0yMDI1IC0gaHR0cDovL3d3dy52aWRlb2xhbi5vcmcveDI2NC5odG1sIC0gb3B0aW9uczogY2FiYWM9MCByZWY9MyBkZWJsb2NrPTE6MDowIGFuYWx5c2U9MHgxOjB4MTExIG1lPWhleCBzdWJtZT03IHBzeT0xIHBzeV9yZD0xLjAwOjAuMDAgbWl4ZWRfcmVmPTEgbWVfcmFuZ2U9MTYgY2hyb21hX21lPTEgdHJlbGxpcz0xIDh4OGRjdD0wIGNxbT0wIGRlYWR6b25lPTIxLDExIGZhc3RfcHNraXA9MSBjaHJvbWFfcXBfb2Zmc2V0PS0yIHRocmVhZHM9MSBsb29rYWhlYWRfdGhyZWFkcz0xIHNsaWNlZF90aHJlYWRzPTAgbnI9MCBkZWNpbWF0ZT0xIGludGVybGFjZWQ9MCBibHVyYXlfY29tcGF0PTAgY29uc3RyYWluZWRfaW50cmE9MCBiZnJhbWVzPTAgd2VpZ2h0cD0wIGtleWludD0xMCBrZXlpbnRfbWluPTEgc2NlbmVjdXQ9NDAgaW50cmFfcmVmcmVzaD0wIHJjX2xvb2thaGVhZD0xMCByYz1jcmYgbWJ0cmVlPTEgY3JmPTIzLjAgcWNvbXA9MC42MCBxcG1pbj0wIHFwbWF4PTY5IHFwc3RlcD00IGlwX3JhdGlvPTEuNDAgYXE9MToxLjAwAIAAAAAjZYiEDfEYoAAlnxwABFRjgAChRJxHHMcAAV+ZOuunrrrp68AAAAAdZYiCA/xGKAALSMcAAVco4AAhgycnJ111111114AAAAAGQZo4HeNgAAAABkGaVAb42A=="
}
