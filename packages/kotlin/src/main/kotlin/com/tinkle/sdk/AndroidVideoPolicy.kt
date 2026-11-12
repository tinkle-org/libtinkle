package com.tinkle.sdk

import android.media.MediaFormat
import java.nio.ByteBuffer
import kotlin.math.pow

/** Fail-closed format and bounded thumbnail policy shared with focused JVM tests. */
internal object AndroidVideoPolicy {
    /** Narrow ISO MP4 allowlist, checked in addition to actual platform track decoding. */
    fun requireMp4(header: ByteArray) {
        if (header.size != 12) throw FfiMediaException.Invalid()
        val size = ByteBuffer.wrap(header).int
        val type = String(header, 4, 4, Charsets.US_ASCII)
        val brand = String(header, 8, 4, Charsets.US_ASCII)
        if (size !in 16..4096 || type != "ftyp" ||
            brand !in listOf("isom", "iso2", "iso3", "iso4", "iso5", "iso6", "avc1", "mp41", "mp42", "M4V ")) {
            throw FfiMediaException.Unsupported()
        }
    }

    /** Reject HDR before the decoder has any chance to silently tone-map its output. */
    fun requireSdr(format: MediaFormat) {
        val transfer = format.intOrNull(MediaFormat.KEY_COLOR_TRANSFER)
        if (transfer == MediaFormat.COLOR_TRANSFER_HLG || transfer == MediaFormat.COLOR_TRANSFER_ST2084 ||
            format.containsKey(MediaFormat.KEY_HDR_STATIC_INFO) ||
            (format.intOrNull("bit-depth")?.let { it != 8 } == true)) throw FfiMediaException.Unsupported()
    }

    /** Checks AVC sequence parameter sets, not just the decoder's potentially reduced output. */
    fun requireAvc8(format: MediaFormat) {
        if (format.getString(MediaFormat.KEY_MIME) != "video/avc") throw FfiMediaException.Unsupported()
        requireSdr(format)
        val csd = format.getByteBuffer("csd-0")?.duplicate() ?: throw FfiMediaException.Unsupported()
        if (csd.remaining() !in 5..65536) throw FfiMediaException.Invalid()
        val bytes = ByteArray(csd.remaining())
        csd.get(bytes)
        requireAvcSps(bytes)
        val sarWidth = format.intOrNull("sar-width") ?: 1
        val sarHeight = format.intOrNull("sar-height") ?: 1
        if (sarWidth != sarHeight) throw FfiMediaException.Unsupported()
    }

    /** Accepts only SPS profiles whose 8-bit 4:2:0 interpretation we can establish. */
    fun requireAvcSps(bytes: ByteArray) {
        var found = false
        var i = 0
        while (i + 3 < bytes.size) {
            val prefix = when {
                bytes[i] == 0.toByte() && bytes[i + 1] == 0.toByte() && bytes[i + 2] == 1.toByte() -> 3
                i + 4 < bytes.size && bytes[i] == 0.toByte() && bytes[i + 1] == 0.toByte() &&
                    bytes[i + 2] == 0.toByte() && bytes[i + 3] == 1.toByte() -> 4
                else -> { i++; continue }
            }
            val start = i + prefix
            var end = start + 1
            while (end + 2 < bytes.size && !(bytes[end] == 0.toByte() && bytes[end + 1] == 0.toByte() &&
                    (bytes[end + 2] == 1.toByte() ||
                        (end + 3 < bytes.size && bytes[end + 2] == 0.toByte() && bytes[end + 3] == 1.toByte())))) end++
            if (end + 2 >= bytes.size) end = bytes.size
            if (bytes[start].toInt() and 31 == 7) {
                found = true
                val bits = SpsBits(bytes.copyOfRange(start + 1, end))
                val profile = bits.read(8)
                bits.read(8) // constraint flags
                bits.read(8) // level
                bits.ue() // SPS id
                when (profile) {
                    66, 77 -> Unit
                    100 -> {
                        if (bits.ue() != 1 || bits.ue() != 0 || bits.ue() != 0) throw FfiMediaException.Unsupported()
                    }
                    else -> throw FfiMediaException.Unsupported()
                }
            }
            i = end
        }
        if (!found) throw FfiMediaException.Unsupported()
    }

    /** Reject an in-band profile change before feeding it into a potentially downconverting codec. */
    fun checkInBandSps(buffer: ByteBuffer, count: Int) {
        // Extractor returns AVC access units as Annex B, as it does codec-specific data.
        // Only copy bounded SPS NALs, never an entire compressed frame into the JVM heap.
        var i = 0
        while (i + 4 < count) {
            if (buffer.get(i) != 0.toByte() || buffer.get(i + 1) != 0.toByte()) { i++; continue }
            val prefix = when {
                buffer.get(i + 2) == 1.toByte() -> 3
                buffer.get(i + 2) == 0.toByte() && buffer.get(i + 3) == 1.toByte() -> 4
                else -> { i++; continue }
            }
            val nal = i + prefix
            if (buffer.get(nal).toInt() and 31 == 7) {
                var end = nal + 1
                while (end + 2 < count && !(buffer.get(end) == 0.toByte() && buffer.get(end + 1) == 0.toByte() &&
                        (buffer.get(end + 2) == 1.toByte() || (end + 3 < count && buffer.get(end + 2) == 0.toByte() &&
                            buffer.get(end + 3) == 1.toByte())))) end++
                if (end + 2 >= count) end = count
                if (end - i > 65536) throw FfiMediaException.Invalid()
                val sps = ByteArray(end - i)
                buffer.duplicate().apply { position(i); limit(end) }.get(sps)
                requireAvcSps(sps)
                i = end
            } else i = nal + 1
        }
    }

    /** Validate both coded allocation and cropped display sizes before using a native buffer. */
    fun dimensions(format: MediaFormat, maxPixels: ULong): Pair<Int, Int> {
        val width = format.getInteger(MediaFormat.KEY_WIDTH)
        val height = format.getInteger(MediaFormat.KEY_HEIGHT)
        if (width <= 0 || height <= 0) throw FfiMediaException.Invalid()
        if (width.toULong() * height.toULong() > maxPixels) throw FfiMediaException.Budget()
        val left = format.intOrNull("crop-left") ?: 0
        val top = format.intOrNull("crop-top") ?: 0
        val right = format.intOrNull("crop-right") ?: (width - 1)
        val bottom = format.intOrNull("crop-bottom") ?: (height - 1)
        if (left < 0 || top < 0 || right !in left until width || bottom !in top until height) {
            throw FfiMediaException.Invalid()
        }
        return (right - left + 1) to (bottom - top + 1)
    }

    /** Maps display coordinates back to the unrotated decoded crop. */
    fun unrotate(x: Int, y: Int, width: Int, height: Int, rotation: Int): Pair<Int, Int> = when (rotation) {
        0 -> x to y
        90 -> y to (height - 1 - x)
        180 -> (width - 1 - x) to (height - 1 - y)
        270 -> (width - 1 - y) to x
        else -> throw FfiMediaException.Unsupported()
    }

    /** BT.709 matrix + inverse video transfer + sRGB encoding, with no source mutation. */
    fun writeSrgb(out: ByteArray, offset: Int, y: Int, u: Int, v: Int, limited: Boolean) {
        val luma = if (limited) (y - 16) / 219.0 else y / 255.0
        val cb = (u - 128) / if (limited) 224.0 else 255.0
        val cr = (v - 128) / if (limited) 224.0 else 255.0
        fun channel(value: Double): Byte {
            val video = value.coerceIn(0.0, 1.0)
            val linear = if (video < 0.081) video / 4.5 else ((video + 0.099) / 1.099).pow(1.0 / 0.45)
            val srgb = if (linear <= 0.0031308) linear * 12.92 else 1.055 * linear.pow(1.0 / 2.4) - 0.055
            return (srgb * 255 + 0.5).toInt().coerceIn(0, 255).toByte()
        }
        out[offset] = channel(luma + 1.5748 * cr)
        out[offset + 1] = channel(luma - 0.187324 * cb - 0.468124 * cr)
        out[offset + 2] = channel(luma + 1.8556 * cb)
        out[offset + 3] = 255.toByte()
    }
}

/** Minimal bounded SPS prefix reader; MediaCodec validates the complete stream afterwards. */
private class SpsBits(escaped: ByteArray) {
    private val bytes = escaped.filterIndexed { index, value ->
        !(index >= 2 && value == 3.toByte() && escaped[index - 1] == 0.toByte() && escaped[index - 2] == 0.toByte())
    }.toByteArray()
    private var bit = 0

    /** Read at most 24 bits, rejecting truncated codec configuration. */
    fun read(count: Int): Int {
        if (count !in 0..24 || bit + count > bytes.size * 8) throw FfiMediaException.Invalid()
        var value = 0
        repeat(count) {
            value = (value shl 1) or ((bytes[bit / 8].toInt() ushr (7 - bit % 8)) and 1)
            bit++
        }
        return value
    }

    /** Read a bounded unsigned Exp-Golomb field. */
    fun ue(): Int {
        var zeros = 0
        while (read(1) == 0) {
            if (++zeros > 20) throw FfiMediaException.Invalid()
        }
        return (1 shl zeros) - 1 + read(zeros)
    }
}

/** Measures decoded presentation cadence, never the container's claimed duration/frame count. */
internal class VideoFrameTiming(private val limits: FfiVideoDecodeLimits) {
    var count: UInt = 0u
        private set
    private var lastUs = 0L
    private var cadenceUs = 0L

    /** Reject discontinuities/variable cadence rather than guess the final frame duration. */
    fun add(timestampUs: Long) {
        if (count >= limits.maxFrames) throw FfiMediaException.Budget()
        if (timestampUs < 0 || (count == 0u && timestampUs != 0L)) throw FfiMediaException.Unsupported()
        if (count > 0u) {
            val delta = timestampUs - lastUs
            if (delta <= 0) throw FfiMediaException.Invalid()
            // MP4 timebase rounding can alternate by one microsecond.
            if (cadenceUs > 0 && kotlin.math.abs(delta - cadenceUs) > 1) throw FfiMediaException.Unsupported()
            cadenceUs = maxOf(cadenceUs, delta)
        }
        lastUs = timestampUs
        count++
        if (timestampUs.toULong() / 1000uL > limits.maxDurationMs) throw FfiMediaException.Budget()
    }

    /** Includes one measured frame interval; single-frame movies have no measurable cadence. */
    fun durationUs(): ULong {
        if (count < 2u || cadenceUs == 0L) throw FfiMediaException.Unsupported()
        val duration = lastUs.toULong() + cadenceUs.toULong()
        if (duration / 1000uL > limits.maxDurationMs ||
            (duration / 1000uL == limits.maxDurationMs && duration % 1000uL != 0uL)) throw FfiMediaException.Budget()
        return duration
    }

    /** Ceil without overflowing an unsigned microsecond total. */
    fun durationMs(): ULong {
        val micros = durationUs()
        return micros / 1000uL + if (micros % 1000uL == 0uL) 0uL else 1uL
    }

    /** Reject a different final sample hold/edit list; allow only sub-millisecond timebase rounding. */
    fun verifyTrackEnd(declaredUs: Long) {
        val measured = durationUs()
        if (declaredUs <= 0) throw FfiMediaException.Unsupported()
        val declared = declaredUs.toULong()
        if (declared / 1000uL > limits.maxDurationMs ||
            (declared / 1000uL == limits.maxDurationMs && declared % 1000uL != 0uL)) throw FfiMediaException.Budget()
        val difference = if (declared > measured) declared - measured else measured - declared
        if (difference >= 1000uL) throw FfiMediaException.Unsupported()
    }
}

/** Always attempt cleanup without replacing cancellation or the original codec failure. */
internal inline fun <T> withVideoCleanup(cleanup: () -> Unit, block: () -> T): T {
    var failure: Throwable? = null
    try {
        return block()
    } catch (error: Throwable) {
        failure = error
        throw error
    } finally {
        try { cleanup() } catch (error: Throwable) {
            if (failure == null) throw error
            if (failure !== error) failure.addSuppressed(error)
        }
    }
}
