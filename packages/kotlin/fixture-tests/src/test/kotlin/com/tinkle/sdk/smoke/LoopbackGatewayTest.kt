package com.tinkle.sdk.smoke

import java.io.DataInputStream
import java.io.OutputStream
import java.net.Socket
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Test

/** Wire-level regressions run before spending time building APKs or booting an emulator. */
class LoopbackGatewayTest {
    @Test
    fun sqliteJournalThenHistoryAndResumeHaveIndependentWireShapes() {
        LoopbackGateway(setOf("target:room:7")).use { gateway ->
            connect(gateway).use { socket ->
                val input = DataInputStream(socket.getInputStream())
                val output = socket.getOutputStream()
                rpc(output, 1, "GetTargetChanges", bytes(1, number(2, 7)))
                val deletion = number(1, 101) + number(2, 2) + number(4, 1)
                assertArrayEquals(response(1, bytes(1, deletion) + number(2, 0) + number(3, 1)), frame(input))

                // This valid RPC used to kill the worker after the SQLite journal commit.
                rpc(output, 2, "GetHistory", bytes(1, number(2, 7)) + number(5, 100))
                assertArrayEquals(response(2, number(6, 0)), frame(input))
                assertEquals(listOf("target:room:7" to 0L), gateway.requests.toList())
                assertEquals(listOf("target:room:7"), gateway.historyRequests.toList())

                rpc(output, 3, "GetTargetChanges", bytes(1, number(2, 7)) + number(2, 1))
                assertArrayEquals(response(3, number(2, 0) + number(3, 1)), frame(input))
                assertEquals(listOf("target:room:7" to 0L, "target:room:7" to 1L), gateway.requests.toList())
                send(output, byteArrayOf(), 8)
                assertArrayEquals(byteArrayOf(), frame(input, 8))
            }
        }
    }

    @Test
    fun emptyHistoryDoesNotInventJournalRequestsOrAcceptancePositions() {
        LoopbackGateway().use { gateway ->
            connect(gateway).use { socket ->
                val input = DataInputStream(socket.getInputStream())
                rpc(socket.getOutputStream(), 1, "GetHistory", bytes(1, number(1, 42)) + number(5, 100))
                assertArrayEquals(response(1, number(6, 0)), frame(input))
                assertEquals(emptyList<Pair<String, Long>>(), gateway.requests.toList())
                assertEquals(listOf("target:user:42"), gateway.historyRequests.toList())
                send(socket.getOutputStream(), byteArrayOf(), 8)
                frame(input, 8)
            }
        }
    }

    private fun connect(gateway: LoopbackGateway): Socket {
        val socket = Socket("127.0.0.1", gateway.port)
        socket.soTimeout = 3_000
        val output = socket.getOutputStream()
        output.write(("GET /syntra/v1/ws HTTP/1.1\r\nHost: localhost\r\n" +
            "Upgrade: websocket\r\nConnection: Upgrade\r\n" +
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").toByteArray())
        output.flush()
        val input = DataInputStream(socket.getInputStream())
        val headers = StringBuilder()
        while (!headers.endsWith("\r\n\r\n")) headers.append(input.readUnsignedByte().toChar())
        check(headers.startsWith("HTTP/1.1 101"))
        check(headers.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="))
        send(output, number(1, 1) + bytes(10, byteArrayOf()))
        assertArrayEquals(number(1, 1) + bytes(11, bytes(1, "smoke".toByteArray())), frame(input))
        return socket
    }

    private fun rpc(output: OutputStream, id: Int, method: String, payload: ByteArray) {
        val request = bytes(1, "/tinkle.v1.MessagesService/$method".toByteArray()) + bytes(2, payload)
        send(output, number(1, 1) + number(2, id) + bytes(12, request))
    }

    private fun response(id: Int, payload: ByteArray) = number(1, 1) + number(2, id) + bytes(13, bytes(1, payload))
    private fun number(field: Int, value: Int) = byteArrayOf((field * 8).toByte(), value.toByte())
    private fun bytes(field: Int, value: ByteArray) = byteArrayOf((field * 8 + 2).toByte(), value.size.toByte()) + value

    private fun send(output: OutputStream, body: ByteArray, opcode: Int = 2) {
        check(body.size < 126)
        val mask = byteArrayOf(1, 2, 3, 4)
        output.write(byteArrayOf((0x80 or opcode).toByte(), (0x80 or body.size).toByte()) + mask)
        output.write(body.mapIndexed { i, byte -> (byte.toInt() xor mask[i % 4].toInt()).toByte() }.toByteArray())
        output.flush()
    }

    private fun frame(input: DataInputStream, opcode: Int = 2): ByteArray {
        assertEquals(0x80 or opcode, input.readUnsignedByte())
        val length = input.readUnsignedByte()
        check(length < 126)
        return ByteArray(length).also { input.readFully(it) }
    }
}
