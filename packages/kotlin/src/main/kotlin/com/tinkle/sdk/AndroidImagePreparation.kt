package com.tinkle.sdk

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.BitmapRegionDecoder
import android.graphics.Canvas
import android.graphics.Matrix
import android.graphics.Paint
import android.graphics.Rect
import android.graphics.RectF
import android.media.ExifInterface
import java.io.File
import java.io.FileOutputStream
import java.io.FilterOutputStream
import java.io.IOException
import java.io.RandomAccessFile

/** Static Image import: bounded region decode, oriented final raster, lossless PNG, atomic output. */
internal object AndroidImagePreparation {
    /** Animation is prepared by Rust before this adapter; it must never become a first-frame bitmap. */
    fun prepare(source: File, destination: File, maximum: ULong, check: () -> Unit): FfiNativeMediaOutput {
        val bounds = BitmapFactory.Options().apply { inJustDecodeBounds = true }
        BitmapFactory.decodeFile(source.path, bounds)
        if (bounds.outMimeType !in listOf("image/jpeg", "image/png", "image/webp", "image/heic", "image/heif")) throw FfiMediaException.Unsupported()
        requireStatic(source, bounds.outMimeType)
        if (bounds.outWidth <= 0 || bounds.outHeight <= 0) throw FfiMediaException.Invalid()
        if (bounds.outWidth.toLong() * bounds.outHeight > 100_000_000L || maxOf(bounds.outWidth, bounds.outHeight) > 100_000) throw FfiMediaException.Budget()
        val orientation = if (bounds.outMimeType in listOf("image/jpeg", "image/heic", "image/heif")) {
            try { ExifInterface(source.path).getAttributeInt(ExifInterface.TAG_ORIENTATION, 1) } catch (_: IOException) { throw FfiMediaException.Unsupported() }
        } else 1
        if (orientation !in 1..8) throw FfiMediaException.Unsupported()
        val swap = orientation in 5..8
        val originalWidth = if (swap) bounds.outHeight else bounds.outWidth
        val originalHeight = if (swap) bounds.outWidth else bounds.outHeight
        val stage = File(destination.parentFile, "image.partial")
        try {
            for (attempt in 0u..64u) {
                check()
                val dimensions = mediaImageDimensions(originalWidth.toUInt(), originalHeight.toUInt(), attempt)
                val width = dimensions.width.toInt(); val height = dimensions.height.toInt()
                if (width.toLong() * height > 16L * 1024 * 1024) throw FfiMediaException.Budget()
                val bitmap = Bitmap.createBitmap(width, height, Bitmap.Config.ARGB_8888)
                try {
                    val canvas = Canvas(bitmap)
                    val w = bounds.outWidth.toFloat(); val h = bounds.outHeight.toFloat()
                    val t = when (orientation) {
                        1 -> floatArrayOf(1f, 0f, 0f, 0f, 1f, 0f)
                        2 -> floatArrayOf(-1f, 0f, w, 0f, 1f, 0f)
                        3 -> floatArrayOf(-1f, 0f, w, 0f, -1f, h)
                        4 -> floatArrayOf(1f, 0f, 0f, 0f, -1f, h)
                        5 -> floatArrayOf(0f, 1f, 0f, 1f, 0f, 0f)
                        6 -> floatArrayOf(0f, -1f, h, 1f, 0f, 0f)
                        7 -> floatArrayOf(0f, -1f, h, -1f, 0f, w)
                        else -> floatArrayOf(0f, 1f, 0f, -1f, 0f, w)
                    }
                    val sx = width.toFloat() / originalWidth; val sy = height.toFloat() / originalHeight
                    canvas.concat(Matrix().apply { setValues(floatArrayOf(t[0] * sx, t[1] * sx, t[2] * sx, t[3] * sy, t[4] * sy, t[5] * sy, 0f, 0f, 1f)) })
                    // 1024x256 tiles <=1 MiB, plus one <=64 MiB final raster. No 100-Mpixel bitmap.
                    @Suppress("DEPRECATION")
                    val decoder = try { BitmapRegionDecoder.newInstance(source.path, false) } catch (_: IOException) { throw FfiMediaException.Unsupported() }
                    try {
                        val paint = Paint().apply { isFilterBitmap = false; isAntiAlias = false }
                        for (top in 0 until bounds.outHeight step 256) for (left in 0 until bounds.outWidth step 1024) {
                            check()
                            val rect = Rect(left, top, minOf(left + 1024, bounds.outWidth), minOf(top + 256, bounds.outHeight))
                            val tile = decoder.decodeRegion(rect, BitmapFactory.Options().apply { inPreferredConfig = Bitmap.Config.ARGB_8888 }) ?: throw FfiMediaException.Codec()
                            try { canvas.drawBitmap(tile, null, RectF(rect), paint) } finally { tile.recycle() }
                        }
                    } finally { decoder.recycle() }
                    val output = LimitedOutput(FileOutputStream(stage), maximum)
                    val encoded = try { output.use { bitmap.compress(Bitmap.CompressFormat.PNG, 100, it) } } catch (e: IOException) { if (!output.exceeded) throw e; false }
                    check()
                    if (output.exceeded) continue
                    if (!encoded || stage.length() <= 0) throw FfiMediaException.Codec()
                    // Verification holds a second <=64 MiB raster and only one scanline on the JVM.
                    val decoded = BitmapFactory.decodeFile(stage.path) ?: throw FfiMediaException.Codec()
                    try {
                        if (decoded.width != width || decoded.height != height) throw FfiMediaException.Codec()
                        val expected = IntArray(width); val actual = IntArray(width)
                        for (y in 0 until height) {
                            check()
                            bitmap.getPixels(expected, 0, width, 0, y, width, 1)
                            decoded.getPixels(actual, 0, width, 0, y, width, 1)
                            if (!expected.contentEquals(actual)) throw FfiMediaException.Codec()
                        }
                    } finally { decoded.recycle() }
                    if (!stage.renameTo(destination)) throw FfiMediaException.Io()
                    return FfiNativeMediaOutput(dimensions.width, dimensions.height, 1u, 0uL, 0u, 0u, 0uL)
                } finally { bitmap.recycle(); stage.delete() }
            }
            throw FfiMediaException.Budget()
        } finally { stage.delete() }
    }

    /** Scan framed animation chunks, not byte substrings in an arbitrary metadata prefix. */
    private fun requireStatic(file: File, mime: String?) {
        if (mime == "image/heic" || mime == "image/heif") {
            RandomAccessFile(file, "r").use { input ->
                val size = input.readInt()
                if (size !in 16..4096 || input.readInt() != 0x66747970 || size.toLong() > input.length()) throw FfiMediaException.Unsupported()
                val brand = input.readInt(); input.readInt()
                val brands = mutableListOf(brand)
                while (input.filePointer + 4 <= size) brands.add(input.readInt())
                if (brands.any { it == 0x6d736631 || it == 0x68657663 || it == 0x68657678 }) throw FfiMediaException.Unsupported()
            }
            return
        }
        if (mime != "image/png" && mime != "image/webp") return
        RandomAccessFile(file, "r").use { input ->
            val png = mime == "image/png"
            var offset = if (png) 8L else 12L
            var chunks = 0
            while (offset < input.length()) {
                if (++chunks > 4096 || input.length() - offset < 8) throw FfiMediaException.Invalid()
                input.seek(offset)
                val first = input.readInt(); val second = input.readInt()
                val tag = if (png) second else first
                if (tag == 0x6163544c || tag == 0x414e494d) throw FfiMediaException.Unsupported()
                val size = (if (png) first else Integer.reverseBytes(second)).toLong() and 0xffffffffL
                offset += size + if (png) 12 else 8 + size % 2
                if (offset > input.length()) throw FfiMediaException.Invalid()
            }
        }
    }

    /** Stop native PNG writes at the byte ceiling, before a giant temporary output is produced. */
    private class LimitedOutput(output: FileOutputStream, private val maximum: ULong) : FilterOutputStream(output) {
        var exceeded = false
            private set
        private var written = 0uL
        override fun write(value: Int) { write(byteArrayOf(value.toByte()), 0, 1) }
        override fun write(bytes: ByteArray, offset: Int, count: Int) {
            if (written + count.toULong() > maximum) { exceeded = true; throw IOException("image output budget") }
            out.write(bytes, offset, count); written += count.toULong()
        }
    }
}
