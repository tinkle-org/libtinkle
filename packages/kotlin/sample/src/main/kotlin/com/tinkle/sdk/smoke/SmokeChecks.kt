package com.tinkle.sdk.smoke

import com.tinkle.sdk.ClientConfigFfi
import com.tinkle.sdk.FfiCursorStore
import com.tinkle.sdk.FfiException
import com.tinkle.sdk.FfiSyncEvent
import com.tinkle.sdk.InMemoryFfiStore
import com.tinkle.sdk.SqliteStorage
import com.tinkle.sdk.TargetSub
import com.tinkle.sdk.TinkleClient
import com.tinkle.sdk.TinkleSdk
import com.tinkle.sdk.uniffiEnsureInitialized
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue

/** Lives in the target APK so R8 optimizes the actual SDK call sites. No UI. */
public object SmokeChecks {
    @JvmStatic
    public fun run(): Unit = runBlocking {
        withTimeout(30_000) {
            TinkleSdk.initialize()
            TinkleSdk.initialize()
            uniffiEnsureInitialized()
            InMemoryFfiStore().destroy()
            phase("foreign journal callbacks") { foreignJournalCallbacks() }
            phase("SQLite persistence and journal resume") { sqlitePersistence() }
        }
    }

    private suspend fun phase(name: String, block: suspend () -> Unit) {
        try {
            block()
        } catch (error: Exception) {
            if (error is kotlinx.coroutines.CancellationException) throw error
            throw AssertionError("Smoke phase failed: $name: ${error.message}", error)
        } catch (error: AssertionError) {
            throw AssertionError("Smoke phase failed: $name: ${error.message}", error)
        }
    }

    private fun config(gateway: LoopbackGateway) = ClientConfigFfi(
        "ws://127.0.0.1:${gateway.port}/syntra/v1/ws", true, null, grpcEndpoint = null,
    )

    private suspend fun foreignJournalCallbacks() {
        val journals = mapOf(
            ("user:42" to "target:room:7") to 37L,
            ("user:42" to "target:user:7") to 53L,
            ("user:43" to "target:room:7") to 73L,
            ("user:43" to "target:user:7") to 89L,
        )
        val loads = ConcurrentLinkedQueue<Pair<String, String>>()
        val saves = ConcurrentLinkedQueue<Triple<String, String, Long>>()
        val foreignStore = object : FfiCursorStore {
            override suspend fun loadJournal(principal: String, targetKey: String): Long? {
                delay(10) // Force the async foreign callback to suspend and resume.
                loads.add(principal to targetKey)
                if (targetKey == "target:room:8") throw FfiException.Storage("smoke load failure")
                return journals[principal to targetKey]
            }
            override suspend fun saveJournal(principal: String, targetKey: String, acceptSeq: Long) {
                delay(10)
                saves.add(Triple(principal, targetKey, acceptSeq))
                if (targetKey == "target:room:9") throw FfiException.Storage("smoke save failure")
            }
            override suspend fun removeTarget(principal: String, targetKey: String) = Unit
            override suspend fun clearPrincipal(principal: String) = Unit
        }
        LoopbackGateway(setOf("target:room:9", "target:room:10")).use { gateway ->
            val client = TinkleClient(config(gateway), "local-smoke-fixture", foreignStore)
            try {
                client.connect()
                client.setPrincipalUserId(42)
                val targets = listOf(TargetSub("room", 7L), TargetSub("user", 7L))
                val response = client.subscribe(targets, emptyList())
                check(response.targets.toSet() == setOf("target:room:7", "target:user:7"))
                // These values passed from Kotlin through Rust to the wire.
                check(gateway.requests.toList() == listOf("target:room:7" to 37L, "target:user:7" to 53L))
                client.setPrincipalUserId(43)
                check(client.subscribe(targets, emptyList()).targets.toSet() == response.targets.toSet())
                check(loads.toList() == journals.keys.toList())
                check(gateway.requests.toList() == listOf(
                    "target:room:7" to 37L, "target:user:7" to 53L,
                    "target:room:7" to 73L, "target:user:7" to 89L,
                ))
                check(saves.isEmpty()) // Empty pages never checkpoint fake progress.
                client.subscribe(listOf(TargetSub("room", 10)), emptyList())
                val saved = nextMatchingEvent(client) {
                    it is FfiSyncEvent.ConversationChanged && it.targetKey == "target:room:10"
                } as FfiSyncEvent.ConversationChanged
                check(saved.appliedAcceptSeq == 1L)
                check(saves.contains(Triple("user:43", "target:room:10", 1L)))

                // Foreign exceptions survive the Rust adapter as diagnostic events.
                client.subscribe(listOf(TargetSub("room", 8L)), emptyList())
                val loadError = nextMatchingEvent(client) {
                    it is FfiSyncEvent.ReconcileFailed && it.targetKey == "target:room:8"
                } as FfiSyncEvent.ReconcileFailed
                check(loadError.error.contains("smoke load failure"))
                check(loads.contains("user:43" to "target:room:8"))
                check(gateway.requests.none { it.first == "target:room:8" })

                client.subscribe(listOf(TargetSub("room", 9L)), emptyList())
                val saveError = nextMatchingEvent(client) {
                    it is FfiSyncEvent.CheckpointFailed && it.targetKey == "target:room:9"
                } as FfiSyncEvent.CheckpointFailed
                check(saveError.acceptSeq == 1L)
                check(saveError.error.contains("smoke save failure"))
                check(saves.contains(Triple("user:43", "target:room:9", 1L)))
                val reconcileError = nextMatchingEvent(client) {
                    it is FfiSyncEvent.ReconcileFailed && it.targetKey == "target:room:9"
                } as FfiSyncEvent.ReconcileFailed
                check(reconcileError.error.contains("smoke save failure"))
                check(client.targetCursor("target:room:9") != 1L)
            } finally {
                shutdown(client)
            }
        }
    }

    private suspend fun nextMatchingEvent(client: TinkleClient, matches: (FfiSyncEvent) -> Boolean): FfiSyncEvent {
        while (true) {
            val event = checkNotNull(client.nextEvent()) { "Event stream closed before expected event" }
            if (matches(event)) return event
        }
    }

    private suspend fun sqlitePersistence() {
        val file = File.createTempFile("tinkle-sdk-smoke-", ".sqlite")
        // Minimal tinkle.v1.User.empty { id: 42 } and Chat { chat_id: 7 }.
        val userProto = byteArrayOf(0x0a, 0x02, 0x08, 0x2a)
        val chatProto = byteArrayOf(0x08, 0x07)
        try {
            SqliteStorage.openAsync(file.absolutePath).use { storage ->
                storage.saveUserProto("user:42", 42, userProto)
                storage.saveUserProto("user:43", 42, userProto)
                storage.saveChatProto("user:42", 7, chatProto)
                try {
                    storage.saveUserProto("user:42", 99, userProto)
                    error("Mismatched protobuf id must fail")
                } catch (_: FfiException.Codec) {
                    check(storage.getUserProto("user:42", 99) == null)
                }
                LoopbackGateway(setOf("target:room:7")).use { gateway ->
                    val client = TinkleClient.newSqlite(config(gateway), "local-smoke-fixture", storage)
                    try {
                        client.connect()
                        client.setPrincipalUserId(42)
                        client.subscribe(listOf(TargetSub("room", 7)), emptyList())
                        val changed = nextMatchingEvent(client) {
                            it is FfiSyncEvent.ConversationChanged && it.targetKey == "target:room:7"
                        } as FfiSyncEvent.ConversationChanged
                        check(changed.appliedAcceptSeq == 1L)
                        check(storage.loadJournal("user:42", "target:room:7") == 1L)
                        check(gateway.requests.toList() == listOf("target:room:7" to 0L))
                        check(gateway.historyRequests.toList() == listOf("target:room:7"))
                    } finally {
                        shutdown(client)
                    }
                }
            }
            check(file.length() > 0)
            // Release every Rust handle, reopen the same file, and resume at 1.
            SqliteStorage.openAsync(file.absolutePath).use { storage ->
                check(storage.getUserProto("user:42", 42)?.contentEquals(userProto) == true)
                check(storage.getChatProto("user:42", 7)?.contentEquals(chatProto) == true)
                check(storage.loadJournal("user:42", "target:room:7") == 1L)
                check(storage.loadJournal("user:43", "target:room:7") == null)
                LoopbackGateway().use { gateway ->
                    val client = TinkleClient.newSqlite(config(gateway), "local-smoke-fixture", storage)
                    try {
                        client.connect()
                        client.setPrincipalUserId(42)
                        client.subscribe(listOf(TargetSub("room", 7)), emptyList())
                        check(gateway.requests.toList() == listOf("target:room:7" to 1L))
                        check(gateway.historyRequests.toList() == listOf("target:room:7"))
                        check(storage.loadJournal("user:42", "target:room:7") == 1L)
                    } finally {
                        shutdown(client)
                    }
                }
                // Clear only after stopping clients, as required by the scope fence.
                storage.clearPrincipal("user:42")
            }
            SqliteStorage.openAsync(file.absolutePath).use { storage ->
                check(storage.getUserProto("user:42", 42) == null)
                check(storage.getChatProto("user:42", 7) == null)
                check(storage.loadJournal("user:42", "target:room:7") == null)
                check(storage.getUserProto("user:43", 42)?.contentEquals(userProto) == true)
            }
            try {
                SqliteStorage.openAsync("").use { error("Empty database path must fail") }
            } catch (_: FfiException.Storage) {
                // Typed native errors reach Kotlin rather than a panic or silent success.
            }
        } finally {
            listOf("", "-wal", "-shm", "-journal").forEach { File(file.absolutePath + it).delete() }
        }
    }

    private suspend fun shutdown(client: TinkleClient) {
        try {
            client.shutdown()
        } finally {
            client.destroy()
        }
    }
}
