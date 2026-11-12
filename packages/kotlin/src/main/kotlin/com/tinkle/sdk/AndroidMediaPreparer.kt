package com.tinkle.sdk

import android.graphics.ImageFormat
import android.media.AudioFormat
import android.media.Image
import android.media.MediaCodec
import android.media.MediaCodecInfo
import android.media.MediaExtractor
import android.media.MediaFormat
import android.media.MediaMuxer
import android.os.SystemClock
import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext

/** LOCAL API-24 H.264/AAC-LC encoder. No client reference, network, upload, or publication. */
public class AndroidMediaPreparer : FfiPlatformMediaPreparer {
    override suspend fun prepare(input: FfiNativeMediaInput): FfiNativeMediaOutput {
        if (!workers.tryAcquire()) { input.close(); throw FfiMediaException.Busy() }
        val resources = Resources(input)
        val start = SystemClock.elapsedRealtime()
        return withVideoCleanup({ resources.finish() }) {
            try {
                withContext(Dispatchers.IO) {
                    val check = {
                        coroutineContext.ensureActive()
                        if (input.isCancelled()) throw FfiMediaException.Cancelled()
                        if (SystemClock.elapsedRealtime() - start >= input.deadlineMs().toLong()) throw FfiMediaException.Budget()
                    }
                    check()
                    val source = File(input.sourcePath())
                    val destination = File(input.outputPath())
                    if (!source.isFile || destination.exists()) throw FfiMediaException.Invalid()
                    when (val kind = input.kind()) {
                        FfiNativeMediaKind.Image -> AndroidImagePreparation.prepare(source, destination, input.maxOutputBytes(), check)
                        FfiNativeMediaKind.Video -> movie(source, destination, input, resources, check)
                        FfiNativeMediaKind.Audio -> audio(source, destination, null, input, resources, check)
                        is FfiNativeMediaKind.Voice -> audio(source, destination, kind, input, resources, check)
                    }
                }
            } catch (e: CancellationException) { throw e
            } catch (e: FfiMediaException) { throw e
            } catch (e: java.io.IOException) { throw FfiMediaException.Io().apply { initCause(e) }
            } catch (e: SecurityException) { throw FfiMediaException.Io().apply { initCause(e) }
            } catch (e: IllegalArgumentException) { throw FfiMediaException.Unsupported().apply { initCause(e) }
            } catch (e: ClassCastException) { throw FfiMediaException.Unsupported().apply { initCause(e) }
            } catch (e: IllegalStateException) { throw FfiMediaException.Codec().apply { initCause(e) } }
        }
    }

    /** Encode video and optional audio independently, then mux their already encoded access units. */
    private fun movie(source: File, destination: File, input: FfiNativeMediaInput, r: Resources, check: () -> Unit): FfiNativeMediaOutput {
        val videoFile = File(destination.parentFile, "video.mp4")
        val audioFile = File(destination.parentFile, "audio.mp4")
        return withVideoCleanup({ videoFile.delete(); audioFile.delete() }) {
            val tracks = extractor(source, r) { extractor ->
                if (extractor.trackCount !in 1..2) throw FfiMediaException.Unsupported()
                (0 until extractor.trackCount).map { extractor.getTrackFormat(it).getString(MediaFormat.KEY_MIME) ?: "" }
            }
            if (tracks.count { it.startsWith("video/") } != 1 || tracks.any { !it.startsWith("video/") && !it.startsWith("audio/") }) throw FfiMediaException.Unsupported()
            val video = encodeVideo(source, videoFile, input, r, check)
            val facts = video.facts
            val sound = if (tracks.any { it.startsWith("audio/") }) audio(source, audioFile, null, input, r, check) else null
            mux(listOfNotNull(videoFile, if (sound == null) null else audioFile), destination, input.maxOutputBytes(), r, check)
            // The same real decoder independently drains the encoded video file. Audio is decoded below.
            verifyVideo(destination, video, input, r, check)
            if (sound != null) verifyAudio(destination, sound.audioSampleRate.toInt(), sound.audioChannels.toInt(), sound.audioSamples.toLong(), input, r, check)
            facts.copy(audioChannels = sound?.audioChannels ?: 0u, audioSampleRate = sound?.audioSampleRate ?: 0u,
                audioSamples = sound?.audioSamples ?: 0uL)
        }
    }

    /** One decoded image and one encoder input image at a time; no full-resolution JVM raster. */
    private data class EncodedVideo(val facts: FfiNativeMediaOutput, val timestamps: List<Long>, val endUs: Long)

    private fun encodeVideo(source: File, destination: File, input: FfiNativeMediaInput, r: Resources, check: () -> Unit): EncodedVideo = extractor(source, r) { extractor ->
        val track = (0 until extractor.trackCount).singleOrNull { extractor.getTrackFormat(it).getString(MediaFormat.KEY_MIME)?.startsWith("video/") == true }
            ?: throw FfiMediaException.Unsupported()
        val format = extractor.getTrackFormat(track)
        AndroidVideoPolicy.requireAvc8(format)
        val original = AndroidVideoPolicy.dimensions(format, 16uL * 1024uL * 1024uL)
        val rotation = format.intOrNull(MediaFormat.KEY_ROTATION) ?: 0
        if (rotation !in listOf(0, 90, 180, 270)) throw FfiMediaException.Unsupported()
        val dimensions = mediaVideoDimensions(original.first.toUInt(), original.second.toUInt())
        val width = dimensions.width.toInt(); val height = dimensions.height.toInt()
        format.setInteger(MediaFormat.KEY_ROTATION, 0)
        format.setInteger(MediaFormat.KEY_COLOR_FORMAT, MediaCodecInfo.CodecCapabilities.COLOR_FormatYUV420Flexible)
        format.setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, 16 * 1024 * 1024)
        val frameRate = if (!format.containsKey(MediaFormat.KEY_FRAME_RATE)) 30f else try {
            format.getInteger(MediaFormat.KEY_FRAME_RATE).toFloat()
        } catch (_: ClassCastException) { format.getFloat(MediaFormat.KEY_FRAME_RATE) }
        if (!frameRate.isFinite() || frameRate <= 0 || frameRate > 240) throw FfiMediaException.Unsupported()
        val encodeFormat = MediaFormat.createVideoFormat("video/avc", width, height).apply {
            setInteger(MediaFormat.KEY_COLOR_FORMAT, MediaCodecInfo.CodecCapabilities.COLOR_FormatYUV420Flexible)
            setInteger(MediaFormat.KEY_PROFILE, MediaCodecInfo.CodecProfileLevel.AVCProfileBaseline)
            setInteger(MediaFormat.KEY_BIT_RATE, 8_000_000)
            setFloat(MediaFormat.KEY_FRAME_RATE, frameRate)
            setInteger(MediaFormat.KEY_I_FRAME_INTERVAL, 2)
        }
        val limits = FfiVideoDecodeLimits(256uL * 1024uL * 1024uL, 16uL * 1024uL * 1024uL, input.maxFrames(), input.maxDurationMs(), 0u, input.deadlineMs())
        val timestamps = ArrayList<Long>()
        val endUs = format.getLong(MediaFormat.KEY_DURATION)
        if (endUs <= 0 || endUs.toULong() > input.maxDurationMs() * 1000uL) throw FfiMediaException.Budget()
        extractor.selectTrack(track)
        codec(format, false, r) { decoder ->
            Encoder(destination, encodeFormat, rotation, input.maxOutputBytes(), r, check).use { encoder ->
                decode(decoder, extractor, input.maxFrames(), input.maxDurationMs(), check) { slot, info ->
                    AndroidVideoPolicy.requireSdr(decoder.getOutputFormat(slot))
                    val decoded = decoder.getOutputImage(slot) ?: throw FfiMediaException.Unsupported()
                    withVideoCleanup({ decoded.close() }) {
                        if (AndroidVideoPolicy.dimensions(decoder.getOutputFormat(slot), limits.maxPixels) != original) throw FfiMediaException.Unsupported()
                        if (timestamps.size >= input.maxFrames().toLong() || info.presentationTimeUs >= endUs ||
                            (timestamps.lastOrNull()?.let { info.presentationTimeUs <= it } ?: (info.presentationTimeUs != 0L))) throw FfiMediaException.Unsupported()
                        timestamps.add(info.presentationTimeUs)
                        val next = encoder.inputSlot()
                        val image = encoder.codec.getInputImage(next) ?: throw FfiMediaException.Unsupported()
                        withVideoCleanup({ image.close() }) { scaleYuv(decoded, image, width, height, check) }
                        encoder.codec.queueInputBuffer(next, 0, width * height * 3 / 2, info.presentationTimeUs, 0)
                        encoder.drain(false)
                    }
                }
                if (timestamps.isEmpty()) throw FfiMediaException.Codec()
                encoder.end(endUs)
            }
        }
        val swap = rotation == 90 || rotation == 270
        EncodedVideo(FfiNativeMediaOutput(if (swap) dimensions.height else dimensions.width, if (swap) dimensions.width else dimensions.height,
            timestamps.size.toUInt(), ((endUs + 999) / 1000).toULong(), 0u, 0u, 0uL), timestamps, endUs)
    }

    /** AAC-LC output from imported audio or interleaved PCM16 voice, never FLAC or packet passthrough. */
    private fun audio(source: File, destination: File, voice: FfiNativeMediaKind.Voice?, input: FfiNativeMediaInput, r: Resources, check: () -> Unit): FfiNativeMediaOutput {
        var samples = 0L
        var rate = 0
        var channels = 0
        fun encode(format: MediaFormat, feed: (Encoder) -> Unit) {
            rate = format.getInteger(MediaFormat.KEY_SAMPLE_RATE); channels = format.getInteger(MediaFormat.KEY_CHANNEL_COUNT)
            if (rate !in 8000..48000 || channels !in 1..2) throw FfiMediaException.Unsupported()
            val target = MediaFormat.createAudioFormat("audio/mp4a-latm", rate, channels).apply {
                setInteger(MediaFormat.KEY_AAC_PROFILE, MediaCodecInfo.CodecProfileLevel.AACObjectLC)
                setInteger(MediaFormat.KEY_BIT_RATE, 128_000)
            }
            Encoder(destination, target, 0, input.maxOutputBytes(), r, check).use { encoder -> feed(encoder); encoder.end() }
        }
        fun pcm(data: ByteBuffer, pts: Long, encoder: Encoder) {
            val stride = channels * 2
            if (data.remaining() % stride != 0 || kotlin.math.abs(pts - samples * 1_000_000L / rate) > 1000) throw FfiMediaException.Unsupported()
            while (data.hasRemaining()) {
                check()
                val slot = encoder.inputSlot()
                val buffer = encoder.codec.getInputBuffer(slot) ?: throw FfiMediaException.Codec()
                buffer.clear()
                val size = minOf(buffer.remaining(), data.remaining()) / stride * stride
                if (size == 0) throw FfiMediaException.Codec()
                val piece = data.duplicate().apply { limit(position() + size) }
                buffer.put(piece); data.position(data.position() + size)
                val timestamp = samples * 1_000_000L / rate
                samples += size / stride
                if (samples * 1000 / rate > input.maxDurationMs().toLong()) throw FfiMediaException.Budget()
                encoder.codec.queueInputBuffer(slot, 0, size, timestamp, 0)
                encoder.drain(false)
            }
        }
        if (voice != null) {
            if (voice.bitsPerSample != 16.toUByte() || voice.channels.toInt() !in 1..2 || voice.sampleRate.toInt() !in 8000..48000 ||
                source.length() == 0L || source.length() % (voice.channels.toLong() * 2) != 0L) throw FfiMediaException.Unsupported()
            encode(MediaFormat.createAudioFormat("audio/raw", voice.sampleRate.toInt(), voice.channels.toInt())) { encoder ->
                FileInputStream(source).use { stream ->
                    val bytes = ByteArray(65536)
                    while (true) {
                        check()
                        val size = stream.read(bytes)
                        if (size < 0) break
                        pcm(ByteBuffer.wrap(bytes, 0, size), samples * 1_000_000L / rate, encoder)
                    }
                }
            }
        } else extractor(source, r) { extractor ->
            val tracks = (0 until extractor.trackCount).filter { extractor.getTrackFormat(it).getString(MediaFormat.KEY_MIME)?.startsWith("audio/") == true }
            if (tracks.size != 1 || (input.kind() == FfiNativeMediaKind.Audio && extractor.trackCount != 1)) throw FfiMediaException.Unsupported()
            val format = extractor.getTrackFormat(tracks.single())
            format.setInteger(MediaFormat.KEY_PCM_ENCODING, AudioFormat.ENCODING_PCM_16BIT)
            format.setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, 1024 * 1024)
            extractor.selectTrack(tracks.single())
            encode(format) { encoder ->
                codec(format, false, r) { decoder ->
                    decode(decoder, extractor, 100_000u, input.maxDurationMs(), check) { slot, info ->
                        val actual = decoder.getOutputFormat(slot)
                        if ((actual.intOrNull(MediaFormat.KEY_PCM_ENCODING) ?: AudioFormat.ENCODING_PCM_16BIT) != AudioFormat.ENCODING_PCM_16BIT ||
                            actual.getInteger(MediaFormat.KEY_SAMPLE_RATE) != rate || actual.getInteger(MediaFormat.KEY_CHANNEL_COUNT) != channels) throw FfiMediaException.Unsupported()
                        val data = decoder.getOutputBuffer(slot) ?: throw FfiMediaException.Codec()
                        data.position(info.offset); data.limit(info.offset + info.size)
                        pcm(data, info.presentationTimeUs, encoder)
                    }
                }
            }
        }
        if (samples == 0L) throw FfiMediaException.Invalid()
        verifyAudio(destination, rate, channels, samples, input, r, check)
        return FfiNativeMediaOutput(0u, 0u, 0u, ((samples * 1000 + rate - 1) / rate).toULong(), channels.toUByte(), rate.toUInt(), samples.toULong())
    }

    /** Fully drain output AAC and check sample count, channels/rate and AudioSpecificConfig object type. */
    private fun verifyAudio(file: File, rate: Int, channels: Int, samples: Long, input: FfiNativeMediaInput, r: Resources, check: () -> Unit) = extractor(file, r) { extractor ->
        if (extractor.trackCount !in 1..2) throw FfiMediaException.Codec()
        val track = (0 until extractor.trackCount).singleOrNull { extractor.getTrackFormat(it).getString(MediaFormat.KEY_MIME) == "audio/mp4a-latm" } ?: throw FfiMediaException.Codec()
        val format = extractor.getTrackFormat(track)
        val asc = format.getByteBuffer("csd-0") ?: throw FfiMediaException.Codec()
        if (format.getString(MediaFormat.KEY_MIME) != "audio/mp4a-latm" || asc.remaining() < 2 || (asc.get(asc.position()).toInt() and 255) ushr 3 != 2 ||
            format.getInteger(MediaFormat.KEY_SAMPLE_RATE) != rate || format.getInteger(MediaFormat.KEY_CHANNEL_COUNT) != channels) throw FfiMediaException.Codec()
        extractor.selectTrack(track)
        format.setInteger(MediaFormat.KEY_PCM_ENCODING, AudioFormat.ENCODING_PCM_16BIT)
        var decoded = 0L
        codec(format, false, r) { codec -> decode(codec, extractor, 100_000u, input.maxDurationMs() + 1000uL, check) { slot, info ->
            val actual = codec.getOutputFormat(slot)
            if ((actual.intOrNull(MediaFormat.KEY_PCM_ENCODING) ?: AudioFormat.ENCODING_PCM_16BIT) != AudioFormat.ENCODING_PCM_16BIT ||
                actual.getInteger(MediaFormat.KEY_SAMPLE_RATE) != rate || actual.getInteger(MediaFormat.KEY_CHANNEL_COUNT) != channels ||
                info.size % (channels * 2) != 0) throw FfiMediaException.Codec()
            decoded += info.size / (channels * 2)
        } }
        if (decoded < samples || decoded - samples > 4096) throw FfiMediaException.Codec()
    }

    private fun verifyVideo(file: File, video: EncodedVideo, input: FfiNativeMediaInput, r: Resources, check: () -> Unit) = extractor(file, r) { extractor ->
        val facts = video.facts
        val track = (0 until extractor.trackCount).single { extractor.getTrackFormat(it).getString(MediaFormat.KEY_MIME) == "video/avc" }
        val format = extractor.getTrackFormat(track)
        if (kotlin.math.abs(format.getLong(MediaFormat.KEY_DURATION) - video.endUs) > 1000) throw FfiMediaException.Codec()
        AndroidVideoPolicy.requireAvc8(format)
        val size = AndroidVideoPolicy.dimensions(format, 1920uL * 1080uL)
        val rotated = (format.intOrNull(MediaFormat.KEY_ROTATION) ?: 0) in listOf(90, 270)
        if ((if (rotated) size.second else size.first).toUInt() != facts.width || (if (rotated) size.first else size.second).toUInt() != facts.height) throw FfiMediaException.Codec()
        format.setInteger(MediaFormat.KEY_ROTATION, 0)
        format.setInteger(MediaFormat.KEY_COLOR_FORMAT, MediaCodecInfo.CodecCapabilities.COLOR_FormatYUV420Flexible)
        extractor.selectTrack(track)
        var count = 0u
        codec(format, false, r) { codec -> decode(codec, extractor, input.maxFrames(), input.maxDurationMs(), check) { slot, info ->
            if (count.toInt() >= video.timestamps.size || kotlin.math.abs(info.presentationTimeUs - video.timestamps[count.toInt()]) > 100) throw FfiMediaException.Codec()
            val image = codec.getOutputImage(slot) ?: throw FfiMediaException.Codec()
            try { if (image.format != ImageFormat.YUV_420_888) throw FfiMediaException.Codec() } finally { image.close() }
            count++
        } }
        if (count != facts.frameCount) throw FfiMediaException.Codec()
    }

    /** EOS is mandatory on both sides. Pending access units cannot disappear into a partial success. */
    private fun decode(codec: MediaCodec, extractor: MediaExtractor, maxFrames: UInt, maxDuration: ULong, check: () -> Unit, output: (Int, MediaCodec.BufferInfo) -> Unit) {
        var inputDone = false; var outputDone = false; var queued = 0u; var returned = 0u
        val info = MediaCodec.BufferInfo()
        val video = codec.inputFormat.getString(MediaFormat.KEY_MIME) == "video/avc"
        val pending = HashSet<Long>()
        while (!outputDone) {
            check()
            if (!inputDone) {
                val slot = codec.dequeueInputBuffer(1000)
                if (slot >= 0) {
                    val buffer = codec.getInputBuffer(slot) ?: throw FfiMediaException.Codec()
                    buffer.clear()
                    val pts = extractor.sampleTime
                    if (pts < 0) { codec.queueInputBuffer(slot, 0, 0, 0, MediaCodec.BUFFER_FLAG_END_OF_STREAM); inputDone = true }
                    else {
                        if (queued >= maxFrames || pts.toULong() / 1000uL > maxDuration) throw FfiMediaException.Budget()
                        if (extractor.sampleFlags and (MediaExtractor.SAMPLE_FLAG_ENCRYPTED or MediaExtractor.SAMPLE_FLAG_PARTIAL_FRAME) != 0) throw FfiMediaException.Unsupported()
                        val count = extractor.readSampleData(buffer, 0)
                        if (count <= 0 || count > buffer.capacity()) throw FfiMediaException.Invalid()
                        if (video) { AndroidVideoPolicy.checkInBandSps(buffer, count); if (!pending.add(pts)) throw FfiMediaException.Unsupported() }
                        codec.queueInputBuffer(slot, 0, count, pts, 0); queued++; extractor.advance()
                    }
                }
            }
            val slot = codec.dequeueOutputBuffer(info, 1000)
            if (slot >= 0) withVideoCleanup({ codec.releaseOutputBuffer(slot, false) }) {
                if (info.size > 0 && info.flags and MediaCodec.BUFFER_FLAG_CODEC_CONFIG == 0) {
                    if (returned >= maxFrames || (video && !pending.remove(info.presentationTimeUs))) throw FfiMediaException.Codec()
                    output(slot, info); returned++
                }
                outputDone = info.flags and MediaCodec.BUFFER_FLAG_END_OF_STREAM != 0
            }
        }
        if (!inputDone || returned == 0u || pending.isNotEmpty()) throw FfiMediaException.Codec()
    }

    /** Nearest-neighbor YUV scaling keeps 8-bit 4:2:0, with row/pixel-stride-aware native planes. */
    private fun scaleYuv(source: Image, destination: Image, width: Int, height: Int, check: () -> Unit) {
        if (source.format != ImageFormat.YUV_420_888 || destination.format != ImageFormat.YUV_420_888) throw FfiMediaException.Unsupported()
        val crop = source.cropRect
        for (plane in 0..2) {
            val divisor = if (plane == 0) 1 else 2
            val src = source.planes[plane]; val dst = destination.planes[plane]
            val w = width / divisor; val h = height / divisor
            for (y in 0 until h) {
                check()
                val sy = crop.top / divisor + y.toLong() * (crop.height() / divisor) / h
                for (x in 0 until w) {
                    val sx = crop.left / divisor + x.toLong() * (crop.width() / divisor) / w
                    val from = src.buffer.position().toLong() + sy * src.rowStride + sx * src.pixelStride
                    val to = dst.buffer.position().toLong() + y.toLong() * dst.rowStride + x.toLong() * dst.pixelStride
                    if (from !in src.buffer.position().toLong() until src.buffer.limit().toLong() || to !in dst.buffer.position().toLong() until dst.buffer.limit().toLong()) throw FfiMediaException.Invalid()
                    dst.buffer.put(to.toInt(), src.buffer.get(from.toInt()))
                }
            }
        }
    }

    /** Bounded encoder/muxer backpressure. Native stop/release must return before ownership release. */
    private class Encoder(file: File, format: MediaFormat, rotation: Int, private val maximum: ULong, private val r: Resources, private val check: () -> Unit) : AutoCloseable {
        val codec = MediaCodec.createEncoderByType(format.getString(MediaFormat.KEY_MIME)!!).also { r.acquired() }
        private var muxer: MediaMuxer? = null
        private val file = file
        private var started = false
        private var muxStarted = false
        private var track = -1
        private var eos = false
        private var written = 0uL
        init {
            try {
                codec.configure(format, null, null, MediaCodec.CONFIGURE_FLAG_ENCODE)
                val mux = MediaMuxer(file.path, MediaMuxer.OutputFormat.MUXER_OUTPUT_MPEG_4).also { r.acquired(); muxer = it }
                mux.setOrientationHint(rotation)
                codec.start(); started = true
            } catch (e: Throwable) { close(); throw e }
        }
        fun inputSlot(): Int {
            while (true) { check(); drain(false); val slot = codec.dequeueInputBuffer(1000); if (slot >= 0) return slot }
        }
        fun drain(wait: Boolean) {
            val info = MediaCodec.BufferInfo()
            do {
                check()
                val slot = codec.dequeueOutputBuffer(info, if (wait) 1000 else 0)
                if (slot == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED) {
                    if (muxStarted) throw FfiMediaException.Codec()
                    val output = codec.outputFormat
                    if (output.getString(MediaFormat.KEY_MIME) == "video/avc") AndroidVideoPolicy.requireAvc8(output)
                    track = muxer!!.addTrack(output); muxer!!.start(); muxStarted = true
                } else if (slot >= 0) withVideoCleanup({ codec.releaseOutputBuffer(slot, false) }) {
                    if (info.size > 0 && info.flags and MediaCodec.BUFFER_FLAG_CODEC_CONFIG == 0) {
                        if (!muxStarted) throw FfiMediaException.Codec()
                        written += info.size.toULong()
                        if (written + 1024uL * 1024uL > maximum || file.length().toULong() > maximum) throw FfiMediaException.Budget()
                        val data = codec.getOutputBuffer(slot) ?: throw FfiMediaException.Codec()
                        data.position(info.offset); data.limit(info.offset + info.size)
                        muxer!!.writeSampleData(track, data, info)
                    }
                    eos = info.flags and MediaCodec.BUFFER_FLAG_END_OF_STREAM != 0
                } else if (!wait) return
            } while (!eos)
        }
        fun end(endUs: Long? = null) {
            val slot = inputSlot(); codec.queueInputBuffer(slot, 0, 0, endUs ?: 0, MediaCodec.BUFFER_FLAG_END_OF_STREAM); drain(true)
            if (endUs != null) {
                if (!muxStarted) throw FfiMediaException.Codec()
                muxer!!.writeSampleData(track, ByteBuffer.allocate(0), MediaCodec.BufferInfo().apply { set(0, 0, endUs, MediaCodec.BUFFER_FLAG_END_OF_STREAM) })
            }
        }
        override fun close() {
            withVideoCleanup({
                muxer?.let { mux -> withVideoCleanup({ mux.release(); r.released() }) { if (muxStarted) mux.stop() } }
            }) { withVideoCleanup({ codec.release(); r.released() }) { if (started) codec.stop() } }
        }
    }

    private fun mux(files: List<File>, output: File, maximum: ULong, r: Resources, check: () -> Unit) {
        val mux = MediaMuxer(output.path, MediaMuxer.OutputFormat.MUXER_OUTPUT_MPEG_4).also { r.acquired() }
        val extractors = mutableListOf<MediaExtractor>()
        var started = false
        withVideoCleanup({
            withVideoCleanup({ mux.release(); r.released() }) {
                withVideoCleanup({ if (started) mux.stop() }) { for (extractor in extractors) { extractor.release(); r.released() } }
            }
        }) {
            val tracks = files.map { file ->
                val extractor = MediaExtractor().also { r.acquired(); extractors.add(it) }
                extractor.setDataSource(file.path)
                if (extractor.trackCount != 1) throw FfiMediaException.Codec()
                val format = extractor.getTrackFormat(0)
                if (format.getString(MediaFormat.KEY_MIME) == "video/avc") mux.setOrientationHint(format.intOrNull(MediaFormat.KEY_ROTATION) ?: 0)
                extractor.selectTrack(0); mux.addTrack(format)
            }
            mux.start(); started = true
            val buffer = ByteBuffer.allocateDirect(16 * 1024 * 1024)
            var written = 0uL
            for ((index, extractor) in extractors.withIndex()) {
            while (extractor.sampleTime >= 0) {
                check(); buffer.clear()
                val size = extractor.readSampleData(buffer, 0)
                if (size <= 0 || size > buffer.capacity()) throw FfiMediaException.Codec()
                written += size.toULong()
                if (written + 1024uL * 1024uL > maximum || output.length().toULong() > maximum) throw FfiMediaException.Budget()
                val info = MediaCodec.BufferInfo().apply { set(0, size, extractor.sampleTime, extractor.sampleFlags) }
                mux.writeSampleData(tracks[index], buffer, info); extractor.advance()
            }
            val endUs = extractor.getTrackFormat(0).getLong(MediaFormat.KEY_DURATION)
            mux.writeSampleData(tracks[index], ByteBuffer.allocate(0), MediaCodec.BufferInfo().apply { set(0, 0, endUs, MediaCodec.BUFFER_FLAG_END_OF_STREAM) })
            }
        }
    }

    private inline fun <T> extractor(file: File, r: Resources, block: (MediaExtractor) -> T): T {
        val extractor = MediaExtractor().also { r.acquired() }
        return withVideoCleanup({ extractor.release(); r.released() }) { extractor.setDataSource(file.path); block(extractor) }
    }
    private inline fun <T> codec(format: MediaFormat, encode: Boolean, r: Resources, block: (MediaCodec) -> T): T {
        val codec = (if (encode) MediaCodec.createEncoderByType(format.getString(MediaFormat.KEY_MIME)!!) else MediaCodec.createDecoderByType(format.getString(MediaFormat.KEY_MIME)!!)).also { r.acquired() }
        var started = false
        return withVideoCleanup({ withVideoCleanup({ codec.release(); r.released() }) { if (started) codec.stop() } }) {
            codec.configure(format, null, null, if (encode) MediaCodec.CONFIGURE_FLAG_ENCODE else 0); codec.start(); started = true; block(codec)
        }
    }

    private class Resources(val input: FfiNativeMediaInput) {
        private var outstanding = 0
        fun acquired() { outstanding++ }
        fun released() { outstanding-- }
        fun finish() {
            if (outstanding != 0) { quarantine.add(input); return }
            try { input.close() } finally { workers.release() }
        }
    }
    private companion object {
        val workers = Semaphore(2)
        val quarantine: MutableSet<FfiNativeMediaInput> = ConcurrentHashMap.newKeySet()
    }
}
