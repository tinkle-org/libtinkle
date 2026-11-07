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

    fun trustSource(): TrustSourceFfi {
        val pin = fixture.getJSONArray("pin")
        return TrustSourceFfi.bootstrap(ServerTrustFfi(ByteArray(32) { pin.getInt(it).toByte() }, 7u, "tinkle-mobile-fixture"))
    }

    val requests: List<Pair<String, Long>> get() {
        val rows = control(JSONObject().put("op", "requests").put("id", id)).getJSONArray("requests")
        return (0 until rows.length()).map { index ->
            val row = rows.getJSONObject(index)
            "target:${row.getString("target_kind")}:${row.getLong("target_id")}" to row.getLong("after_accept_seq")
        }
    }

    val historyRequests: List<String> get() {
        val rows = control(JSONObject().put("op", "requests").put("id", id)).getJSONArray("history")
        return (0 until rows.length()).map { rows.getString(it) }
    }

    override fun close() { control(JSONObject().put("op", "close").put("id", id)) }

    private fun control(request: JSONObject): JSONObject = Socket().use { socket ->
        socket.connect(java.net.InetSocketAddress(host, 48080), 5_000)
        socket.soTimeout = 5_000
        socket.getOutputStream().write((request.toString() + "\n").toByteArray())
        val line = checkNotNull(socket.getInputStream().bufferedReader().readLine()) { "Rust fixture closed" }
        JSONObject(line)
    }
}
