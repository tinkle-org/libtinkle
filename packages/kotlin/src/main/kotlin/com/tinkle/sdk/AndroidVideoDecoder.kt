package com.tinkle.sdk

import android.graphics.ImageFormat
import android.media.Image
import android.media.MediaCodec
import android.media.MediaCodecInfo
import android.media.MediaCodecList
import android.media.MediaExtractor
import android.media.MediaFormat
import android.os.SystemClock
import java.io.DataInputStream
import java.io.FileInputStream
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import kotlin.math.max
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext

/**
 * API-24 video adapter. Fully drains one unencrypted AVC 8-bit 4:2:0 track before
 * reporting NATIVE_DECODED. Other video profiles and ambiguous multi-video files
 * fail closed. Audio tracks are not decoded. The frozen source is never modified.
 *
 * A thumbnail is derived from the first decoded frame, not a nearby sync frame.
 * Only explicitly tagged BT.709 SDR can be converted to display-oriented sRGB;
 * unknown color metadata omits the thumbnail. HDR/10-bit sources are unsupported.
 * No full-resolution RGBA copy is allocated or passed across FFI.
 * Only zero-origin, constant-cadence presentations with at least two frames are
 * supported: duration is the measured presentation span plus one measured interval,
 * cross-checked against the track endpoint (which defines the final sample's hold).
 * At most two invocations run across all instances; excess calls return Busy.
 */
public class AndroidVideoDecoder : FfiPlatformVideoDecoder {
    /**
     * Owns the callback's input handle, including on cancellation before dispatch.
     * Cancellation is cooperative: a vendor codec call may outlive the deadline.
     * This function cannot finish cleanup or release the lease until that call and
     * synchronous stop/release actually return. Job cancellation alone is not a join.
     */
    override suspend fun inspectAndDecode(input: FfiVideoInput, limits: FfiVideoDecodeLimits): FfiDecodedVideo {
        val startedAt = SystemClock.elapsedRealtime()
        val budget = limits.copy()
        if (!workers.tryAcquire()) {
            input.close()
            throw FfiMediaException.Busy()
        }
        val resources = NativeResources(input)
        return withVideoCleanup({ resources.finish() }) {
            try {
                withContext(Dispatchers.IO) {
                    val check = {
                        coroutineContext.ensureActive()
                        if (input.isCancelled()) throw FfiMediaException.Cancelled()
                        if (SystemClock.elapsedRealtime() - startedAt >= budget.deadlineMs.toLong()) {
                            throw FfiMediaException.Budget()
                        }
                    }
                    check()
                    if (input.sizeBytes() == 0uL) throw FfiMediaException.Invalid()
                    if (input.sizeBytes() > budget.maxInputBytes) throw FfiMediaException.Budget()
                    decode(input.sourcePath(), budget, resources, check)
                }
            } catch (error: CancellationException) {
                throw error
            } catch (error: FfiMediaException) {
                throw error
            } catch (error: MediaCodec.CodecException) {
                throw FfiMediaException.Codec().apply { initCause(error) }
            } catch (error: IOException) {
                throw FfiMediaException.Io().apply { initCause(error) }
            } catch (error: SecurityException) {
                throw FfiMediaException.Io().apply { initCause(error) }
            } catch (error: IllegalArgumentException) {
                throw FfiMediaException.Invalid().apply { initCause(error) }
            } catch (error: IllegalStateException) {
                throw FfiMediaException.Codec().apply { initCause(error) }
            }
        }
    }

    /** Every native object is created, used and released on this one worker invocation. */
    private fun decode(path: String, limits: FfiVideoDecodeLimits, resources: NativeResources, check: () -> Unit): FfiDecodedVideo {
        // Container identity alone is not decode evidence; it just prevents labelling AVC-in-TS
        // or Matroska as an MP4 after a successful full track decode.
        DataInputStream(FileInputStream(path)).use { stream ->
            val header = ByteArray(12)
            stream.readFully(header)
            AndroidVideoPolicy.requireMp4(header)
        }
        val extractor = MediaExtractor()
        resources.acquired()
        return withVideoCleanup({ extractor.release(); resources.released() }) {
            extractor.setDataSource(path)
            if (extractor.trackCount !in 1..32) throw FfiMediaException.Budget()
            val tracks = (0 until extractor.trackCount).filter {
                extractor.getTrackFormat(it).getString(MediaFormat.KEY_MIME)?.startsWith("video/") == true
            }
            if (tracks.size != 1) throw FfiMediaException.Unsupported()
            val track = tracks.single()
            val format = extractor.getTrackFormat(track)
            AndroidVideoPolicy.requireAvc8(format)
            val encodedSize = AndroidVideoPolicy.dimensions(format, limits.maxPixels)
            val maxSample = minOf(limits.maxInputBytes, 16uL * 1024uL * 1024uL).toInt()
            if (maxSample <= 0) throw FfiMediaException.Budget()
            val declaredSample = format.intOrNull(MediaFormat.KEY_MAX_INPUT_SIZE)
            if (declaredSample != null && (declaredSample <= 0 || declaredSample > maxSample)) {
                throw FfiMediaException.Budget()
            }
            format.setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, declaredSample ?: maxSample)
            val rotation = format.intOrNull(MediaFormat.KEY_ROTATION) ?: 0
            if (rotation !in listOf(0, 90, 180, 270)) throw FfiMediaException.Unsupported()
            // Byte-buffer images are unrotated. Apply display rotation only to bounded output.
            format.setInteger(MediaFormat.KEY_ROTATION, 0)
            format.setInteger(MediaFormat.KEY_COLOR_FORMAT, MediaCodecInfo.CodecCapabilities.COLOR_FormatYUV420Flexible)
            check()
            val name = MediaCodecList(MediaCodecList.REGULAR_CODECS).findDecoderForFormat(format)
                ?: throw FfiMediaException.Unsupported()
            val codec = MediaCodec.createByCodecName(name)
            resources.acquired()
            var started = false
            withVideoCleanup({
                withVideoCleanup({ codec.release(); resources.released() }) {
                    if (started) codec.stop()
                }
            }) {
                codec.configure(format, null, null, 0)
                codec.start()
                started = true
                extractor.selectTrack(track)
                drain(codec, extractor, format, encodedSize, rotation, limits, check)
            }
        }
    }

    /** Counts decoded frames through output EOS and checks no queued access unit was lost. */
    private fun drain(
        codec: MediaCodec,
        extractor: MediaExtractor,
        sourceFormat: MediaFormat,
        encodedSize: Pair<Int, Int>,
        rotation: Int,
        limits: FfiVideoDecodeLimits,
        check: () -> Unit,
    ): FfiDecodedVideo {
        val info = MediaCodec.BufferInfo()
        val pending = HashSet<Long>()
        val timing = VideoFrameTiming(limits)
        var queued = 0u
        var inputEnded = false
        var outputEnded = false
        var outputSize: Pair<Int, Int>? = null
        var thumbnail: FfiDecodedVideoFrame? = null
        while (!outputEnded) {
            check()
            if (!inputEnded) {
                val slot = codec.dequeueInputBuffer(10_000)
                if (slot >= 0) {
                    val buffer = codec.getInputBuffer(slot) ?: throw FfiMediaException.Codec()
                    buffer.clear()
                    val timestamp = extractor.sampleTime
                    if (timestamp < 0) {
                        codec.queueInputBuffer(slot, 0, 0, 0, MediaCodec.BUFFER_FLAG_END_OF_STREAM)
                        inputEnded = true
                    } else {
                        if (queued >= limits.maxFrames || timestamp.toULong() / 1000uL > limits.maxDurationMs) {
                            throw FfiMediaException.Budget()
                        }
                        if (extractor.sampleFlags and (MediaExtractor.SAMPLE_FLAG_ENCRYPTED or MediaExtractor.SAMPLE_FLAG_PARTIAL_FRAME) != 0) {
                            throw FfiMediaException.Unsupported()
                        }
                        val count = extractor.readSampleData(buffer, 0)
                        if (count <= 0 || count > buffer.capacity() || !pending.add(timestamp)) {
                            throw FfiMediaException.Invalid()
                        }
                        AndroidVideoPolicy.checkInBandSps(buffer, count)
                        check()
                        codec.queueInputBuffer(slot, 0, count, timestamp, 0)
                        queued++
                        extractor.advance()
                    }
                }
            }
            val slot = codec.dequeueOutputBuffer(info, 10_000)
            if (slot >= 0) {
                withVideoCleanup({ codec.releaseOutputBuffer(slot, false) }) {
                    if (info.size > 0 && info.flags and MediaCodec.BUFFER_FLAG_CODEC_CONFIG == 0) {
                        if (!pending.remove(info.presentationTimeUs)) throw FfiMediaException.Invalid()
                        timing.add(info.presentationTimeUs)
                        val output = codec.getOutputFormat(slot)
                        AndroidVideoPolicy.requireSdr(output)
                        val size = AndroidVideoPolicy.dimensions(output, limits.maxPixels)
                        if (size != encodedSize) throw FfiMediaException.Unsupported()
                        outputSize = size
                        // Source SPS was checked before decode; require the expected 8-bit layout too.
                        val image = codec.getOutputImage(slot) ?: throw FfiMediaException.Unsupported()
                        withVideoCleanup({ image.close() }) {
                            if (image.format != ImageFormat.YUV_420_888 || image.planes.size != 3) {
                                throw FfiMediaException.Unsupported()
                            }
                            if (image.cropRect.width() != size.first || image.cropRect.height() != size.second) {
                                throw FfiMediaException.Invalid()
                            }
                            if (timing.count == 1u && limits.thumbnailEdge > 0u) {
                                thumbnail = thumbnail(image, sourceFormat, output, rotation, limits.thumbnailEdge, check)
                            }
                        }
                    }
                    outputEnded = info.flags and MediaCodec.BUFFER_FLAG_END_OF_STREAM != 0
                }
            }
        }
        check()
        if (!inputEnded || pending.isNotEmpty() || queued != timing.count) throw FfiMediaException.Invalid()
        val size = outputSize ?: throw FfiMediaException.Invalid()
        if (!sourceFormat.containsKey(MediaFormat.KEY_DURATION)) throw FfiMediaException.Unsupported()
        timing.verifyTrackEnd(sourceFormat.getLong(MediaFormat.KEY_DURATION))
        val display = if (rotation == 90 || rotation == 270) size.second to size.first else size
        return FfiDecodedVideo(display.first.toUInt(), display.second.toUInt(), timing.durationMs(),
            timing.count, false, 8u, FfiVideoEvidence.NATIVE_DECODED, thumbnail)
    }

    /** Samples only a bounded RGBA destination from the selected frame's YUV planes. */
    private fun thumbnail(
        image: Image, source: MediaFormat, output: MediaFormat, rotation: Int, edge: UInt, check: () -> Unit,
    ): FfiDecodedVideoFrame? {
        fun color(key: String): Int? = output.intOrNull(key) ?: source.intOrNull(key)
        val transfer = color(MediaFormat.KEY_COLOR_TRANSFER)
        val range = color(MediaFormat.KEY_COLOR_RANGE)
        if (color(MediaFormat.KEY_COLOR_STANDARD) != MediaFormat.COLOR_STANDARD_BT709 ||
            transfer != MediaFormat.COLOR_TRANSFER_SDR_VIDEO ||
            range !in listOf(MediaFormat.COLOR_RANGE_FULL, MediaFormat.COLOR_RANGE_LIMITED)) return null
        val crop = image.cropRect
        val displayWidth = if (rotation == 90 || rotation == 270) crop.height() else crop.width()
        val displayHeight = if (rotation == 90 || rotation == 270) crop.width() else crop.height()
        val boundedEdge = minOf(edge.toULong(), 320uL).toInt()
        val scale = max(displayWidth, displayHeight)
        val width = max(1L, displayWidth.toLong() * minOf(boundedEdge, scale) / scale).toInt()
        val height = max(1L, displayHeight.toLong() * minOf(boundedEdge, scale) / scale).toInt()
        val rgba = ByteArray(width * height * 4)
        val planes = image.planes
        fun sample(plane: Int, x: Int, y: Int): Int {
            val p = planes[plane]
            val buffer = p.buffer
            val index = buffer.position().toLong() + y.toLong() * p.rowStride + x.toLong() * p.pixelStride
            if (index < buffer.position() || index >= buffer.limit()) throw FfiMediaException.Invalid()
            return buffer.get(index.toInt()).toInt() and 255
        }
        for (y in 0 until height) {
            check()
            for (x in 0 until width) {
                val dx = (x.toLong() * displayWidth / width).toInt()
                val dy = (y.toLong() * displayHeight / height).toInt()
                val (sx, sy) = AndroidVideoPolicy.unrotate(dx, dy, crop.width(), crop.height(), rotation)
                val px = sx + crop.left
                val py = sy + crop.top
                AndroidVideoPolicy.writeSrgb(rgba, (y * width + x) * 4,
                    sample(0, px, py), sample(1, px / 2, py / 2), sample(2, px / 2, py / 2),
                    range == MediaFormat.COLOR_RANGE_LIMITED)
            }
        }
        return FfiDecodedVideoFrame(width.toUInt(), height.toUInt(), rgba)
    }

    /** Failed native release cannot be treated as proof that the source is no longer in use. */
    private class NativeResources(private val input: FfiVideoInput) {
        private var unreleased = 0
        /** Account for one platform reader before using it. */
        fun acquired() { unreleased++ }
        /** Called only after the platform's release method actually returns successfully. */
        fun released() { unreleased-- }
        /** withContext has joined the worker; retain admission if native release failed. */
        fun finish() {
            if (unreleased != 0) {
                // Fail closed on a broken platform release: retain source AND admission until
                // process death, rather than deleting a file a vendor codec may still read.
                quarantinedInputs.add(input)
                return
            }
            try { input.close() } finally { workers.release() }
        }
    }

    private companion object {
        val workers = Semaphore(2)
        val quarantinedInputs: MutableSet<FfiVideoInput> = ConcurrentHashMap.newKeySet()
    }
}

/** Installs the platform adapter without retaining the client in its callback. */
public fun TinkleClient.configureAndroidVideoDecoder() {
    TinkleSdk.initialize()
    configureVideoDecoder(AndroidVideoDecoder())
}

/** Reads only present integer keys; absent color information must not become guessed facts. */
internal fun MediaFormat.intOrNull(key: String): Int? {
    if (!containsKey(key)) return null
    return try { getInteger(key) } catch (_: ClassCastException) { throw FfiMediaException.Unsupported() }
}
