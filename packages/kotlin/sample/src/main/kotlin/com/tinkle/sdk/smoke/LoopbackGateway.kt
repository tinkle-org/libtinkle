package com.tinkle.sdk.smoke

import com.tinkle.sdk.ServerTrustFfi
import com.tinkle.sdk.TrustSourceFfi
import java.net.Socket
import org.json.JSONArray
import org.json.JSONObject

/** Host Rust fixture control only. The published Syntra SDK owns encrypted TCP. */
internal class LoopbackGateway(deletionTargets: Set<String> = emptySet()) : AutoCloseable {
    val host: String = "10.0.2.2" // Android emulator's host-loopback alias.
    private val fixture = control(JSONObject().put("op", "create").put("deletions", JSONArray(deletionTargets)))
    private val id = fixture.getLong("id")
    val port: Int = fixture.getInt("port")

    /** Create one-use Bootstrap trust from this host fixture's reported server pin. */
    fun trustSource(): TrustSourceFfi {
        val pin = fixture.getJSONArray("pin")
        return TrustSourceFfi.bootstrap(ServerTrustFfi(ByteArray(32) { pin.getInt(it).toByte() }, 7u, "tinkle-mobile-fixture"))
    }

    /** Read journal requests with their exact 64-bit acceptance cursors. */
    val requests: List<Pair<String, Long>> get() {
        val rows = control(JSONObject().put("op", "requests").put("id", id)).getJSONArray("requests")
        return (0 until rows.length()).map { index ->
            val row = rows.getJSONObject(index)
            "target:${row.getString("target_kind")}:${row.getLong("target_id")}" to row.getLong("after_accept_seq")
        }
    }

    /** Read the conversation keys whose history the native consumer requested. */
    val historyRequests: List<String> get() {
        val rows = control(JSONObject().put("op", "requests").put("id", id)).getJSONArray("history")
        return (0 until rows.length()).map { rows.getString(it) }
    }

    /** Ask the host runner to close this fixture and its owned connections. */
    override fun close() { control(JSONObject().put("op", "close").put("id", id)) }

    /** Exchange one newline-delimited JSON command with bounded connect and read waits. */
    private fun control(request: JSONObject): JSONObject = Socket().use { socket ->
        socket.connect(java.net.InetSocketAddress(host, 48080), 5_000)
        socket.soTimeout = 5_000
        socket.getOutputStream().write((request.toString() + "\n").toByteArray())
        val line = checkNotNull(socket.getInputStream().bufferedReader().readLine()) { "Rust fixture closed" }
        JSONObject(line)
    }
}
