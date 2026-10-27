import TinkleSDK
import XCTest

/// Tests the generated Swift API against the real Rust binary and a local gateway.
final class TinkleSDKTests: XCTestCase {
    func testRuntimeInitializationAndRustStoreConstruction() {
        tinkleFfiInit()
        tinkleFfiInit()
        // Construction and release cross the ABI and check binding checksums.
        _ = InMemoryFfiStore()
    }

    func testInMemoryJournalStoreIsolation() async throws {
        let store = MemoryCursorStore()
        try await store.saveJournal(principal: "alice", targetKey: "target:room:1", acceptSeq: 42)
        try await store.saveJournal(principal: "bob", targetKey: "target:room:1", acceptSeq: 9)
        try await store.saveJournal(principal: "alice", targetKey: "target:user:1", acceptSeq: 7)
        let alice = try await store.loadJournal(principal: "alice", targetKey: "target:room:1")
        let bob = try await store.loadJournal(principal: "bob", targetKey: "target:room:1")
        XCTAssertEqual(alice, 42)
        XCTAssertEqual(bob, 9)

        try await store.removeTarget(principal: "alice", targetKey: "target:room:1")
        let removed = try await store.loadJournal(principal: "alice", targetKey: "target:room:1")
        let otherTarget = try await store.loadJournal(principal: "alice", targetKey: "target:user:1")
        XCTAssertNil(removed)
        XCTAssertEqual(otherTarget, 7)
        try await store.clearPrincipal(principal: "alice")
        let cleared = try await store.loadJournal(principal: "alice", targetKey: "target:user:1")
        let retained = try await store.loadJournal(principal: "bob", targetKey: "target:room:1")
        XCTAssertNil(cleared)
        XCTAssertEqual(retained, 9)
    }

    func testClientAsyncErrorsAndLifecycle() async throws {
        tinkleFfiInit()
        let store = MemoryCursorStore()
        // Insecure WebSockets are rejected before opening any connection.
        let client = try TinkleClient(
            config: ClientConfigFfi(
                url: "ws://127.0.0.1:9/syntra/v1/ws",
                allowInsecure: false,
                userAgent: "TinkleSDKTests",
                grpcEndpoint: nil
            ),
            credential: "local-smoke-test",
            store: store
        )
        do {
            try await client.connect()
            XCTFail("An insecure URL must be rejected")
        } catch FfiError.Retryable(let message) {
            XCTAssertTrue(message.contains("insecure transport denied"))
        }
        do {
            _ = try await client.sendMessage(targetKind: "room", targetId: 0, text: "", randomId: 1)
            XCTFail("A zero target ID must fail validation")
        } catch FfiError.Codec(let message) {
            XCTAssertTrue(message.contains("target id must be positive"))
        }
        do {
            _ = try await client.sendMessage(targetKind: "room", targetId: 1, text: "offline", randomId: 1)
            XCTFail("A disconnected client must reject writes")
        } catch FfiError.NotConnected {
            // Typed Rust errors and async completion crossed the ABI.
        }
        await client.suspend()
        await client.shutdown()
        let event = await client.nextEvent()
        XCTAssertNil(event)
    }

    func testRustCallsSwiftAsyncCursorStore() async throws {
        let gateway = try LoopbackGateway()
        defer { try? gateway.finish() }
        let store = MemoryCursorStore()
        let roomSeq: Int64 = 1 << 40
        try await store.saveJournal(principal: "user:42", targetKey: "target:room:7", acceptSeq: roomSeq)
        try await store.saveJournal(principal: "user:42", targetKey: "target:user:7", acceptSeq: 9)
        try await store.saveJournal(principal: "other-user", targetKey: "target:room:7", acceptSeq: 999)
        let client = try makeLoopbackClient(gateway, store: store)
        do {
            try await client.setPrincipalUserId(userId: 42)
            try await client.connect()
            let subscription = try await client.subscribe(targets: [
                TargetSub(targetKind: "room", targetId: 7),
                TargetSub(targetKind: "user", targetId: 7),
            ], watch: [])
            XCTAssertEqual(Set(subscription.targets), ["target:room:7", "target:user:7"])
            // These are decoded wire requests, not the subscribe confirmation.
            // Rust must await Swift loadJournal() and lift optional Int64s to
            // send the principal- and target-specific acceptance positions.
            XCTAssertEqual(gateway.targetChangesRequests, [
                .init(targetKind: "room", targetId: 7, afterAcceptSeq: roomSeq),
                .init(targetKind: "user", targetId: 7, afterAcceptSeq: 9),
            ])
            // The manual subscribe path awaits its initial reconcile pass.
            let roomStatus = await client.targetStatus(targetKey: "target:room:7")
            let userStatus = await client.targetStatus(targetKey: "target:user:7")
            XCTAssertEqual(roomStatus, .idle)
            XCTAssertEqual(userStatus, .idle)
            let reads = await store.loadCalls
            XCTAssertGreaterThanOrEqual(reads.count, 2)
            XCTAssertTrue(reads.allSatisfy { $0.principal == "user:42" })
            XCTAssertEqual(Set(reads.map(\.targetKey)), ["target:room:7", "target:user:7"])
        } catch {
            await client.shutdown()
            throw error
        }
        await client.shutdown()
        try gateway.finish()
    }

    func testSwiftAsyncStoreErrorReportsFailedTargetThroughRust() async throws {
        let gateway = try LoopbackGateway()
        defer { try? gateway.finish() }
        let store = MemoryCursorStore(failLoads: true)
        let client = try makeLoopbackClient(gateway, store: store)
        do {
            try await client.setPrincipalUserId(userId: 42)
            try await client.connect()
            // Per-target failures are reported as status + event; subscribe
            // still confirms the interest instead of throwing FfiError.Storage.
            let subscription = try await client.subscribe(
                targets: [TargetSub(targetKind: "room", targetId: 7)], watch: []
            )
            XCTAssertEqual(subscription.targets, ["target:room:7"])
            let status = await client.targetStatus(targetKey: "target:room:7")
            XCTAssertEqual(status, .failed)
            let reads = await store.loadCalls
            XCTAssertFalse(reads.isEmpty)
            XCTAssertTrue(reads.allSatisfy { $0.principal == "user:42" && $0.targetKey == "target:room:7" })
            XCTAssertTrue(gateway.targetChangesRequests.isEmpty, "A failed load must prevent journal RPCs")

            let received = expectation(description: "Rust reports the Swift async storage failure")
            let eventTask = Task {
                let event = await client.nextEvent()
                received.fulfill()
                return event
            }
            await fulfillment(of: [received], timeout: 3)
            // This generated binding does not forward Task cancellation.
            // Shutdown releases nextEvent even if the expected event is absent.
            await client.shutdown()
            let event = await eventTask.value
            if case .reconcileFailed(let targetKey, let message)? = event {
                XCTAssertEqual(targetKey, "target:room:7")
                XCTAssertTrue(message.contains("deliberate fixture failure"))
            } else {
                XCTFail("Expected a per-target reconciliation failure, got \(String(describing: event))")
            }
        } catch {
            await client.shutdown()
            throw error
        }
        await client.shutdown()
        try gateway.finish()
    }

    private func makeLoopbackClient(_ gateway: LoopbackGateway, store: MemoryCursorStore) throws -> TinkleClient {
        try TinkleClient(
            config: ClientConfigFfi(
                url: "ws://127.0.0.1:\(gateway.port)/syntra/v1/ws",
                allowInsecure: true,
                userAgent: "TinkleSDKLoopbackTests",
                grpcEndpoint: nil
            ),
            credential: "local-smoke-test",
            store: store
        )
    }
}

/// Conforms to the actual generated foreign-store protocol. Production apps
/// provide durable storage; this actor is intentionally test-only and ephemeral.
private actor MemoryCursorStore: FfiCursorStore {
    private var journals: [String: [String: Int64]] = [:]
    private let failLoads: Bool
    private(set) var loadCalls: [(principal: String, targetKey: String)] = []

    init(failLoads: Bool = false) { self.failLoads = failLoads }

    func loadJournal(principal: String, targetKey: String) async throws -> Int64? {
        // Force suspension so the foreign-future completion path is exercised.
        await Task.yield()
        loadCalls.append((principal, targetKey))
        if failLoads { throw FfiError.Storage("deliberate fixture failure") }
        return journals[principal]?[targetKey]
    }

    func saveJournal(principal: String, targetKey: String, acceptSeq: Int64) async throws {
        journals[principal, default: [:]][targetKey] = acceptSeq
    }

    func removeTarget(principal: String, targetKey: String) async throws {
        journals[principal]?[targetKey] = nil
    }

    func clearPrincipal(principal: String) async throws {
        journals[principal] = nil
    }
}
