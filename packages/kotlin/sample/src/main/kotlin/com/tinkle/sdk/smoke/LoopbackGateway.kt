package com.tinkle.sdk.smoke

import android.util.Base64
import java.io.BufferedInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.EOFException
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.security.MessageDigest
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

/**
 * Test fixture only, mirroring libtinkle's client_integration.rs gateway.
 * Field IDs come from syntra.v1.Frame and tinkle.v1 journal/history responses.
 * It serves one loopback client and two RPCs; it is not a protocol implementation.
 */
internal class LoopbackGateway(private val deletionTargets: Set<String> = emptySet()) : AutoCloseable {
    private val listener = ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))
    private val failure = AtomicReference<Throwable?>()
    @Volatile private var connection: Socket? = null
    val port: Int get() = listener.localPort
    val requests = ConcurrentLinkedQueue<Pair<String, Long>>()
    val historyRequests = ConcurrentLinkedQueue<String>()
    private val worker = thread(name = "tinkle-smoke-gateway", isDaemon = true) {
        try {
            listener.accept().use { socket ->
                connection = socket
                socket.soTimeout = 20_000
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = socket.getOutputStream()
                val headers = ByteArrayOutputStream()
                while (!headers.toString("US-ASCII").endsWith("\r\n\r\n")) {
                    check(headers.size() < 16_384)
                    headers.write(input.readUnsignedByte())
                }
                val key = headers.toString("US-ASCII").lineSequence()
                    .first { it.startsWith("Sec-WebSocket-Key:", ignoreCase = true) }
                    .substringAfter(':').trim()
                val accept = Base64.encodeToString(MessageDigest.getInstance("SHA-1")
                    .digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").toByteArray()), Base64.NO_WRAP)
                output.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n" +
                    "Connection: Upgrade\r\nSec-WebSocket-Protocol: syntra.v1\r\n" +
                    "Sec-WebSocket-Accept: $accept\r\n\r\n").toByteArray())
                output.flush()
                val hello = readBinary(input, output) ?: error("No client hello")
                check(fields(hello)[10] is ByteArray)
                send(output, number(1, 1) + bytes(11, bytes(1, "smoke".toByteArray())))
                while (true) {
                    val frame = fields(readBinary(input, output) ?: break)
                    val request = frame[12] as? ByteArray ?: continue
                    val rpc = fields(request)
                    val method = (rpc[1] as ByteArray).toString(Charsets.UTF_8)
                    val payload = fields(rpc[2] as? ByteArray ?: byteArrayOf())
                    val target = fields(payload[1] as ByteArray)
                    val targetKey = when {
                        target[1] is Long -> "target:user:${target[1]}"
                        target[2] is Long -> "target:room:${target[2]}"
                        else -> error("Missing fixture target")
                    }
                    val response = when (method) {
                        "/tinkle.v1.MessagesService/GetTargetChanges" -> {
                            val cursor = payload[2] as? Long ?: 0L
                            requests.add(targetKey to cursor)
                            if (targetKey in deletionTargets && cursor == 0L) {
                                // A deletion drives a journal commit without body hydration.
                                val change = number(1, 101) + number(2, 2) + number(4, 1)
                                bytes(1, change) + number(2, 0) + number(3, 1)
                            } else {
                                // Empty pages must not invent journal progress.
                                number(2, 0) + number(3, cursor)
                            }
                        }
                        "/tinkle.v1.MessagesService/GetHistory" -> {
                            // SQLite reconciliation also discovers new messages after
                            // journal paging. An empty, terminal history window is valid
                            // and must not advance the journal acceptance position.
                            check(payload[5] == 100L) { "Unexpected fixture history limit" }
                            historyRequests.add(targetKey)
                            number(6, 0) // GetHistoryResponse.more_available = false.
                        }
                        else -> error("Unexpected fixture RPC: $method")
                    }
                    send(output, number(1, 1) + number(2, frame[2] as Long) + bytes(13, bytes(1, response)))
                }
            }
        } catch (error: Throwable) {
            if (!listener.isClosed) failure.set(error)
        }
    }

    override fun close() {
        listener.close()
        connection?.close()
        worker.join(2_000)
        check(!worker.isAlive) { "Gateway did not stop" }
        failure.get()?.let { throw AssertionError("Loopback gateway failed: ${it.javaClass.simpleName}: ${it.message}", it) }
    }

    private fun readBinary(input: DataInputStream, output: OutputStream): ByteArray? {
        while (true) {
            val first = input.read()
            if (first == -1) return null
            check(first and 0x80 != 0) { "Fragmented fixture frame" }
            val second = input.readUnsignedByte()
            val masked = second and 0x80 != 0
            val shortLength = second and 0x7f
            val length = when (shortLength) {
                126 -> input.readUnsignedShort().toLong()
                127 -> input.readLong()
                else -> shortLength.toLong()
            }
            check(length in 0..1_048_576)
            val mask = ByteArray(if (masked) 4 else 0).also { input.readFully(it) }
            val body = ByteArray(length.toInt()).also { input.readFully(it) }
            if (masked) body.indices.forEach { body[it] = (body[it].toInt() xor mask[it % 4].toInt()).toByte() }
            when (first and 0xf) {
                2 -> return body
                8 -> { send(output, body, 8); return null }
                9 -> send(output, body, 10)
                10 -> Unit
                else -> error("Unexpected fixture opcode")
            }
        }
    }

    private fun send(output: OutputStream, body: ByteArray, opcode: Int = 2) {
        check(body.size < 126)
        output.write(0x80 or opcode)
        output.write(body.size)
        output.write(body)
        output.flush()
    }

    private fun varint(value: Long): ByteArray {
        var remaining = value
        val output = ByteArrayOutputStream()
        do {
            val byte = (remaining and 127).toInt()
            remaining = remaining ushr 7
            output.write(byte or if (remaining != 0L) 128 else 0)
        } while (remaining != 0L)
        return output.toByteArray()
    }

    private fun number(field: Int, value: Long): ByteArray = varint((field * 8).toLong()) + varint(value)
    private fun bytes(field: Int, value: ByteArray): ByteArray =
        varint((field * 8 + 2).toLong()) + varint(value.size.toLong()) + value

    private fun fields(message: ByteArray): Map<Int, Any> {
        var position = 0
        fun readVarint(): Long {
            var value = 0L
            for (shift in 0..63 step 7) {
                if (position == message.size) throw EOFException()
                val byte = message[position++].toInt() and 255
                value = value or ((byte and 127).toLong() shl shift)
                if (byte and 128 == 0) return value
            }
            error("Invalid fixture varint")
        }
        val result = mutableMapOf<Int, Any>()
        while (position < message.size) {
            val key = readVarint().toInt()
            check(key ushr 3 > 0)
            val value: Any = when (key and 7) {
                0 -> readVarint()
                2 -> {
                    val length = readVarint()
                    check(length in 0..(message.size - position).toLong())
                    message.copyOfRange(position, position + length.toInt()).also { position += length.toInt() }
                }
                else -> error("Unsupported fixture field")
            }
            result[key ushr 3] = value
        }
        return result
    }
}
