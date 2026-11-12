package com.tinkle.sdk.smoke

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Color
import android.media.ExifInterface
import android.media.MediaExtractor
import android.media.MediaFormat
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.tinkle.sdk.*
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.math.PI
import kotlin.math.sin
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith

/** Requires the installed native SDK and real OS codecs; these are not JVM codec simulations. */
@RunWith(AndroidJUnit4::class)
class AndroidNativeMediaPreparationTest {
    @Test fun pcmVoiceAndImportedAudioAreRealAacOnlyMp4() = runBlocking {
        tinkleFfiInit()
        val source = File.createTempFile("voice-", ".pcm", InstrumentationRegistry.getInstrumentation().targetContext.cacheDir)
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

    @Test fun alphaPngAndOrientedJpegBecomeVerifiedLosslessImages() = runBlocking {
        tinkleFfiInit()
        val cache = InstrumentationRegistry.getInstrumentation().targetContext.cacheDir
        val bitmap = Bitmap.createBitmap(32, 16, Bitmap.Config.ARGB_8888)
        bitmap.eraseColor(0x80204060.toInt())
        try {
            for (jpeg in listOf(false, true)) {
                val source = File.createTempFile("image-", if (jpeg) ".jpg" else ".png", cache)
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
