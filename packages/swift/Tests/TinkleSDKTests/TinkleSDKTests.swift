import Darwin
import TinkleSDK
import XCTest

/// Tests the generated Swift API against the real Rust binary and a local gateway.
final class TinkleSDKTests: XCTestCase {
    /// Interrupted reads preserve a split JSON integer without truncating its Int64 value.
    func testControlReadRetriesInterruptionsAcrossJSONChunks() throws {
        let chunks = [Data("{\"after_accept_seq\":1099".utf8), Data("511627776}\n".utf8)]
        var calls = 0
        let response = try LoopbackGateway.readLine(fd: -1, op: "requests") { _, buffer, capacity, _ in
            defer { calls += 1 }
            if calls == 0 || calls == 2 {
                errno = EINTR
                return -1
            }
            let chunk = chunks[calls == 1 ? 0 : 1]
            XCTAssertLessThanOrEqual(chunk.count, capacity)
            chunk.withUnsafeBytes { buffer!.copyMemory(from: $0.baseAddress!, byteCount: chunk.count) }
            return chunk.count
        }
        let json = try JSONSerialization.jsonObject(with: response) as! [String: NSNumber]
        XCTAssertEqual(json["after_accept_seq"]?.int64Value, Int64(1) << 40)
        XCTAssertEqual(response, chunks[0] + chunks[1])
        XCTAssertEqual(calls, 4)
    }

    /// Interrupted partial sends resume at the first unsent byte.
    func testControlSendRetriesInterruptionsWithoutDuplicatingPartialWrites() throws {
        let bytes = Data("{\"op\":\"requests\"}\n".utf8)
        let results = [-1, 3, -1, 2, bytes.count - 5]
        var calls = 0
        var written = Data()
        try LoopbackGateway.sendAll(bytes, fd: -1, op: "requests") { _, buffer, count, _ in
            defer { calls += 1 }
            XCTAssertEqual(Data(bytes: buffer!, count: count), Data(bytes.dropFirst(written.count)))
            let result = results[calls]
            if result < 0 {
                errno = EINTR
            } else {
                written.append(contentsOf: UnsafeRawBufferPointer(start: buffer!, count: result))
            }
            return result
        }
        XCTAssertEqual(written, bytes)
        XCTAssertEqual(calls, results.count)
    }

    /// Partial-response failures retain distinct EOF, timeout, and system diagnostics.
    func testControlReadDiagnosesEOFTimeoutAndSystemErrorAfterPartialResponse() {
        for (result, code, expected) in [
            (0, Int32(0), "EOF before newline"),
            (-1, EAGAIN, "timeout"),
            (-1, ETIMEDOUT, "timeout"),
            (-1, ECONNRESET, "system error"),
        ] {
            var calls = 0
            XCTAssertThrowsError(try LoopbackGateway.readLine(fd: -1, op: "requests") { _, buffer, _, _ in
                defer { calls += 1 }
                if calls == 0 {
                    Data("abc".utf8).withUnsafeBytes { buffer!.copyMemory(from: $0.baseAddress!, byteCount: 3) }
                    return 3
                }
                errno = code
                return result
            }) { error in
                let message = String(describing: error)
                XCTAssertTrue(message.contains("control requests recv \(expected)"), message)
                XCTAssertTrue(message.contains("bytes=3"), message)
                if result < 0 { XCTAssertTrue(message.contains("errno=\(code)"), message) }
            }
            XCTAssertEqual(calls, 2)
        }
    }

    /// The control response limit includes its terminating newline.
    func testControlReadEnforcesCapIncludingNewline() throws {
        for length in [65536, 65537] {
            let bytes = Data(repeating: 120, count: length - 1) + Data([10])
            var offset = 0
            let recv: (Int32, UnsafeMutableRawPointer?, Int, Int32) -> Int = { _, buffer, capacity, _ in
                let count = min(capacity, bytes.count - offset)
                bytes.withUnsafeBytes { buffer!.copyMemory(from: $0.baseAddress!.advanced(by: offset), byteCount: count) }
                offset += count
                return count
            }
            if length == 65536 {
                XCTAssertEqual(try LoopbackGateway.readLine(fd: -1, op: "requests", recv: recv), bytes)
            } else {
                XCTAssertThrowsError(try LoopbackGateway.readLine(fd: -1, op: "requests", recv: recv)) { error in
                    let message = String(describing: error)
                    XCTAssertTrue(message.contains("control requests recv oversized response"), message)
                    XCTAssertTrue(message.contains("limit=65536 bytes=65536"), message)
                }
            }
            XCTAssertEqual(offset, 65536)
        }
    }

    /// Non-interruption failures stop sending instead of replaying a partial request.
    func testControlSendDoesNotRetryNonInterruptFailures() {
        for code in [EAGAIN, ETIMEDOUT, EPIPE] {
            var calls = 0
            XCTAssertThrowsError(try LoopbackGateway.sendAll(Data("request\n".utf8), fd: -1, op: "close") { _, _, _, _ in
                defer { calls += 1 }
                if calls == 0 { return 2 }
                errno = code
                return -1
            }) { error in
                let message = String(describing: error)
                let kind = code == EPIPE ? "system error" : "timeout"
                XCTAssertTrue(message.contains("control close send \(kind)"), message)
                XCTAssertTrue(message.contains("errno=\(code) bytes=2"), message)
            }
            XCTAssertEqual(calls, 2)
        }
        XCTAssertThrowsError(try LoopbackGateway.sendAll(Data([1]), fd: -1, op: "close", send: { _, _, _, _ in 0 })) { error in
            XCTAssertTrue(String(describing: error).contains("control close send made no progress bytes=0"))
        }
    }

    /// A failed Swift persistence callback cannot produce a durable enrollment receipt.
    func testEnrollmentCannotAdvanceBeforePlatformPersistenceAcknowledgement() async throws {
        let client = try TinkleClient(config: ClientConfigFfi(clientInfo: fixtureClientInfo(),
            host: "127.0.0.1", primaryPort: 9, fallbackPort: nil),
            trustSource: try TrustSourceFfi.bootstrap(trust: ServerTrustFfi(
                serverPublicKey: Data(repeating: 1, count: 32), serverKeyId: 7, scope: "fixture")),
            store: MemoryCursorStore())
        let secure = RejectingSecureStore()
        do {
            _ = try await client.prepareEnrollment(store: secure)
            XCTFail("An unacknowledged secure write must not return an operation receipt")
        } catch FfiError.Storage(let message) {
            XCTAssertTrue(message.contains("protected enrollment storage"))
            XCTAssertFalse(message.contains("host diagnostic"))
        }
        let calls = await secure.commits
        XCTAssertEqual(calls, 1)
        await client.shutdown()
    }

    /// Successive writes use exact CAS receipts, and a lost comparison never falls back.
    func testPrepareEnrollmentUsesConditionalSuccessorRevisionsWithoutLegacyFallback() async throws {
        let client = try TinkleClient(config: ClientConfigFfi(clientInfo: fixtureClientInfo(),
            host: "127.0.0.1", primaryPort: 9, fallbackPort: nil),
            trustSource: try TrustSourceFfi.bootstrap(trust: ServerTrustFfi(
                serverPublicKey: Data(repeating: 1, count: 32), serverKeyId: 7, scope: "fixture")),
            store: MemoryCursorStore())
        let secure = ConditionalSecureStore()
        do {
            let first = try await client.prepareEnrollment(store: secure)
            let second = try await client.prepareEnrollment(store: secure)
            XCTAssertNotEqual(first.operationId(), second.operationId())
            let loaded = try await client.loadEnrollmentOperation(store: secure, operationId: first.operationId())
            XCTAssertEqual(loaded.operationId(), first.operationId())
            let revisions = await secure.expectedRevisions
            XCTAssertEqual(revisions, ["conditional-fixture:0", "conditional-fixture:1"])
            let current = try await secure.namespaceSnapshot()
            let snapshot = try XCTUnwrap(current)
            XCTAssertEqual(snapshot.incarnation, "conditional-fixture")
            XCTAssertEqual(snapshot.revision, "conditional-fixture:2")
            XCTAssertTrue(snapshot.hasPending)
            XCTAssertFalse(snapshot.cleanupPending)
            XCTAssertNil(snapshot.active)
            await secure.rejectNextCommit()
            do {
                _ = try await client.prepareEnrollment(store: secure)
                XCTFail("A superseded conditional write must not return an operation receipt")
            } catch FfiError.Storage {
                // nil from the foreign callback is a failed CAS, not persistence.
            }
            let attempted = await secure.expectedRevisions
            let unconditional = await secure.unconditionalCommits
            let operations = await secure.operationIds
            XCTAssertEqual(attempted, ["conditional-fixture:0", "conditional-fixture:1", "conditional-fixture:2"])
            XCTAssertEqual(unconditional, 0)
            XCTAssertEqual(operations, Set([first.operationId(), second.operationId()]))
        } catch {
            await client.shutdown()
            throw error
        }
        await client.shutdown()
    }

    /// A denied foreign install permit must leave this client able to establish only Bootstrap.
    func testRestoreActiveEnrollmentRejectsDeniedInstallPermit() async throws {
        let gateway = try LoopbackGateway(enrollmentReview: true)
        defer { try? gateway.finish() }
        // Exact opaque bytes checked by review_swift_active_vector_matches_private_record_encoder_and_validates_returning
        // in crates/libtinkle/src/enrollment.rs; Swift must not recreate the private record format.
        let hex = Array(EnrollmentReviewVector.activeRecordHex)
        var bytes = Data(try stride(from: 0, to: hex.count, by: 2).map {
            try XCTUnwrap(UInt8(String(hex[$0..<$0 + 2]), radix: 16))
        })
        defer { bytes.resetBytes(in: 0..<bytes.count) }
        let secure = DenyingRestoreSecureStore(active: ProtectedEnrollmentRecord.fromSecureStorage(bytes: bytes))
        let cursors = MemoryCursorStore()
        let client = try makeLoopbackClient(gateway, store: cursors)
        do {
            let before = await client.sessionStatus()
            XCTAssertEqual(before.authorization, .unknown)
            XCTAssertEqual(before.network, .unavailable)
            do {
                _ = try await client.restoreActiveEnrollment(store: secure)
                XCTFail("A false beginRestore must not authorize ACTIVE restoration")
            } catch FfiError.Storage(let message) {
                XCTAssertTrue(message.contains("protected enrollment storage"))
            }
            let begins = await secure.begins
            XCTAssertEqual(begins.count, 1, "An earlier parsing/storage failure must not satisfy this regression")
            let begin = try XCTUnwrap(begins.first)
            XCTAssertEqual(begin.incarnation, "restore-denial-fixture")
            XCTAssertEqual(begin.revision, "restore-denial-fixture:1")
            XCTAssertTrue(begin.activeMatches)
            XCTAssertFalse(begin.permitId.isEmpty)
            let ends = await secure.endIds
            XCTAssertEqual(ends, [begin.permitId], "Rust must release the exact denied acquisition")
            let unexpected = await secure.unexpectedCalls
            XCTAssertTrue(unexpected.isEmpty, "Restore must not write, clear, or use legacy loads: \(unexpected)")
            let after = await client.sessionStatus()
            XCTAssertEqual(after.authorization, before.authorization)
            XCTAssertEqual(after.network, before.network)
            XCTAssertEqual(after.cleanup, before.cleanup)
            // Restoration made one independent Bootstrap retirement query; no main channel was installed.
            XCTAssertEqual(try gateway.admittedModes, ["bootstrap"])
            try await client.connect()
            // Await a real RPC, not a timing-dependent admission callback after connect().
            do {
                _ = try await client.sendCode(phoneNumber: "+14155550123", langCode: "en")
                XCTFail("The fixture does not implement verification delivery")
            } catch FfiError.Network {
                // Unimplemented RPCs map to Network; verified admission below is the handshake proof.
            }
            XCTAssertEqual(try gateway.admittedModes, ["bootstrap", "bootstrap"],
                "The post-denial main connection must authenticate as Bootstrap, never retained Returning")
            do {
                _ = try await client.subscribe(targets: [TargetSub(targetKind: "room", targetId: 7)], watch: [])
                XCTFail("Denied restoration must not install the retained user:42 principal")
            } catch FfiError.Sync(let message) {
                XCTAssertTrue(message.contains("authenticated principal unknown"))
            }
            let reads = await cursors.loadCalls
            XCTAssertTrue(reads.isEmpty, "An unknown principal must not read retained user journals")
        } catch {
            await client.shutdown()
            throw error
        }
        await client.shutdown()
        try gateway.finish()
    }

    /// Repeated initialization and Rust object construction cross the generated ABI safely.
    func testRuntimeInitializationAndRustStoreConstruction() {
        tinkleFfiInit()
        tinkleFfiInit()
        // Construction and release cross the ABI and check binding checksums.
        _ = InMemoryFfiStore()
    }

    /// Journal deletion is isolated by both principal and target.
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

    /// Native async calls preserve typed failures and shutdown releases event consumption.
    func testClientAsyncErrorsAndLifecycle() async throws {
        tinkleFfiInit()
        let store = MemoryCursorStore()
        // A refused native dial is retryable without changing channel trust.
        let client = try TinkleClient(
            config: ClientConfigFfi(
                clientInfo: fixtureClientInfo(),
                host: "127.0.0.1", primaryPort: 9, fallbackPort: nil
            ),
            trustSource: try TrustSourceFfi.bootstrap(trust: ServerTrustFfi(
                serverPublicKey: Data(repeating: 1, count: 32), serverKeyId: 7, scope: "test")),
            store: store
        )
        do {
            try await client.connect()
            XCTFail("A refused dial must fail")
        } catch FfiError.Retryable(let message) {
            XCTAssertTrue(message.contains("TCP unavailable"))
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

    /// Real journal RPCs use the principal-specific Int64 positions returned by Swift.
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
            XCTAssertEqual(try gateway.targetChangesRequests, [
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

    /// A suspended Swift storage failure becomes a Rust target failure and sync event.
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
            XCTAssertTrue(try gateway.targetChangesRequests.isEmpty, "A failed load must prevent journal RPCs")

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

    /// Builds an isolated native client with the gateway's exact Bootstrap pin and scope.
    private func makeLoopbackClient(_ gateway: LoopbackGateway, store: MemoryCursorStore) throws -> TinkleClient {
        try TinkleClient(
            config: ClientConfigFfi(
                clientInfo: fixtureClientInfo(),
                host: "127.0.0.1", primaryPort: gateway.port, fallbackPort: nil
            ),
            trustSource: try gateway.trustSource(),
            store: store
        )
    }
}

/// Restore-denial fixture only: it never grants exclusion or acknowledges a mutation.
private actor DenyingRestoreSecureStore: FfiSecureEnrollmentStore {
    private let active: ProtectedEnrollmentRecord
    private(set) var begins: [(incarnation: String, revision: String, activeMatches: Bool, permitId: String)] = []
    private(set) var endIds: [String] = []
    private(set) var unexpectedCalls: [String] = []

    /// Retains only the synthetic ACTIVE record, never a client or a permit owner.
    init(active: ProtectedEnrollmentRecord) { self.active = active }

    /// Supplies a stable ACTIVE namespace without pending operations or cleanup.
    func namespaceSnapshot() async throws -> ProtectedEnrollmentNamespace? {
        await Task.yield()
        return ProtectedEnrollmentNamespace(incarnation: "restore-denial-fixture", revision: "restore-denial-fixture:1",
            cleanupPending: false, active: active, hasPending: false)
    }
    /// Records the exact requested install binding but grants no exclusion.
    func beginRestore(incarnation: String, revision: String, active: ProtectedEnrollmentRecord, permitId: String) async throws -> Bool {
        await Task.yield()
        var expected = self.active.copyForSecureStorage()
        var received = active.copyForSecureStorage()
        defer {
            expected.resetBytes(in: 0..<expected.count)
            received.resetBytes(in: 0..<received.count)
        }
        begins.append((incarnation, revision, received == expected, permitId))
        return false
    }
    /// Records release after the foreign suspension resumes without creating a permit.
    func endRestore(permitId: String) async throws {
        await Task.yield()
        endIds.append(permitId)
    }
    /// Marks an unexpected mutation or legacy load before rejecting it.
    private func unsupported(_ method: String) -> FfiError {
        unexpectedCalls.append(method)
        return FfiError.Storage("restore-denial fixture does not support \(method)")
    }
    /// Restore denial must never request a conditional write.
    func commitCurrent(expectedRevision: String, kind: SecureEnrollmentWrite, operationId: String, record: ProtectedEnrollmentRecord) async throws -> String? {
        throw unsupported("commitCurrent")
    }
    /// Rejects an unexpected legacy write.
    func commit(kind: SecureEnrollmentWrite, operationId: String, record: ProtectedEnrollmentRecord) async throws { throw unsupported("commit") }
    /// Rejects an unexpected cleanup marker.
    func beginClear(expectedRevision: String) async throws -> Bool { throw unsupported("beginClear") }
    /// Rejects an unexpected cleanup transaction.
    func finishClear(expectedRevision: String) async throws -> Bool { throw unsupported("finishClear") }
    /// Rejects unconditional namespace clearing.
    func clear() async throws { throw unsupported("clear") }
    /// Rejects operation lookup on this ACTIVE-only restore fixture.
    func loadOperation(operationId: String) async throws -> ProtectedEnrollmentRecord? { throw unsupported("loadOperation") }
    /// Requires the atomic snapshot instead of a legacy ACTIVE read.
    func loadActive() async throws -> ProtectedEnrollmentRecord? { throw unsupported("loadActive") }
}

/// Conditional persistence callback fixture, not a confidential or durable store.
/// It cannot exclude namespace mutation during restore, so it grants no permit.
private actor ConditionalSecureStore: FfiSecureEnrollmentStore {
    private var revision = 0
    private var records: [String: ProtectedEnrollmentRecord] = [:]
    private var rejectCommit = false
    private(set) var expectedRevisions: [String] = []
    private(set) var unconditionalCommits = 0
    var operationIds: Set<String> { Set(records.keys) }

    /// Models another writer winning the next revision comparison.
    func rejectNextCommit() { rejectCommit = true }
    /// This create-only fixture cannot provide cross-writer restore exclusion.
    func beginRestore(incarnation: String, revision: String, active: ProtectedEnrollmentRecord, permitId: String) async throws -> Bool { false }
    /// No permit is held, so repeated release is harmless.
    func endRestore(permitId: String) async throws {}
    /// Reports the immutable incarnation and current operation revision atomically.
    func namespaceSnapshot() async throws -> ProtectedEnrollmentNamespace? {
        ProtectedEnrollmentNamespace(incarnation: "conditional-fixture", revision: "conditional-fixture:\(revision)",
            cleanupPending: false, active: nil, hasPending: !records.isEmpty)
    }
    /// Acknowledges only create-only operations under the exact current revision.
    func commitCurrent(expectedRevision: String, kind: SecureEnrollmentWrite, operationId: String, record: ProtectedEnrollmentRecord) async throws -> String? {
        await Task.yield()
        expectedRevisions.append(expectedRevision)
        guard expectedRevision == "conditional-fixture:\(revision)" else { return nil }
        if rejectCommit {
            rejectCommit = false
            revision += 1 // Another writer won the comparison; this record is not installed.
            return nil
        }
        guard case .operation = kind, records[operationId] == nil else {
            throw FfiError.Storage("fixture supports create-only operations")
        }
        records[operationId] = record
        revision += 1
        return "conditional-fixture:\(revision)"
    }
    /// This fixture never acknowledges cleanup markers.
    func beginClear(expectedRevision: String) async throws -> Bool { throw FfiError.Storage("clearing unsupported") }
    /// This fixture never acknowledges credential erasure.
    func finishClear(expectedRevision: String) async throws -> Bool { throw FfiError.Storage("clearing unsupported") }
    /// Rejects unconditional clearing of the conditional namespace.
    func clear() async throws { throw FfiError.Storage("clearing unsupported") }
    /// Detects any prohibited fallback from conditional to legacy persistence.
    func commit(kind: SecureEnrollmentWrite, operationId: String, record: ProtectedEnrollmentRecord) async throws {
        unconditionalCommits += 1
        throw FfiError.Storage("conditional fixture must not use legacy commit")
    }
    /// Returns the exact record retained for one acknowledged operation.
    func loadOperation(operationId: String) async throws -> ProtectedEnrollmentRecord? { records[operationId] }
    /// Preparation alone never creates an ACTIVE slot.
    func loadActive() async throws -> ProtectedEnrollmentRecord? { nil }
}

/// Failure-only fixture: it deliberately never acknowledges secure persistence.
private actor RejectingSecureStore: FfiSecureEnrollmentStore {
    /// Refuses restore exclusion rather than emulating unsupported host guarantees.
    func beginRestore(incarnation: String, revision: String, active: ProtectedEnrollmentRecord, permitId: String) async throws -> Bool {
        throw FfiError.Storage("restore exclusion unsupported by this failure-only fixture")
    }
    /// This failure-only host cannot acknowledge permit release.
    func endRestore(permitId: String) async throws { throw FfiError.Storage("restore exclusion unsupported") }
    /// Selects the legacy write path so its acknowledgement failure can be tested.
    func namespaceSnapshot() async throws -> ProtectedEnrollmentNamespace? { nil }
    /// Explicitly refuses conditional persistence.
    func commitCurrent(expectedRevision: String, kind: SecureEnrollmentWrite, operationId: String, record: ProtectedEnrollmentRecord) async throws -> String? {
        throw FfiError.Storage("conditional storage unsupported by this failure-only fixture")
    }
    /// Rejects unsupported conditional cleanup markers.
    func beginClear(expectedRevision: String) async throws -> Bool { throw FfiError.Storage("conditional storage unsupported") }
    /// Rejects unsupported conditional erasure.
    func finishClear(expectedRevision: String) async throws -> Bool { throw FfiError.Storage("conditional storage unsupported") }
    /// Never claims a durable full-namespace deletion.
    func clear() async throws { throw FfiError.Storage("secure clearing is not acknowledged by this failure-only fixture") }
    private(set) var commits = 0
    /// Suspends, erases its temporary copy, and returns a diagnostic Rust must sanitize.
    func commit(kind: SecureEnrollmentWrite, operationId: String, record: ProtectedEnrollmentRecord) async throws {
        await Task.yield()
        commits += 1
        var bytes = record.copyForSecureStorage()
        bytes.resetBytes(in: 0..<bytes.count)
        throw FfiError.Storage("host diagnostic must be sanitized")
    }
    /// Failed persistence leaves no recoverable operation.
    func loadOperation(operationId: String) async throws -> ProtectedEnrollmentRecord? { nil }
    /// Failed persistence leaves no ACTIVE record.
    func loadActive() async throws -> ProtectedEnrollmentRecord? { nil }
}

/// Conforms to the actual generated foreign-store protocol. Production apps
/// provide durable storage; this actor is intentionally test-only and ephemeral.
private actor MemoryCursorStore: FfiCursorStore {
    private var journals: [String: [String: Int64]] = [:]
    private let failLoads: Bool
    private(set) var loadCalls: [(principal: String, targetKey: String)] = []

    /// Optionally enables deterministic suspended read failures for Rust error tests.
    init(failLoads: Bool = false) { self.failLoads = failLoads }

    /// Records the calling principal and returns its target position after suspension.
    func loadJournal(principal: String, targetKey: String) async throws -> Int64? {
        // Force suspension so the foreign-future completion path is exercised.
        await Task.yield()
        loadCalls.append((principal, targetKey))
        if failLoads { throw FfiError.Storage("deliberate fixture failure") }
        return journals[principal]?[targetKey]
    }

    /// Stores an exact Int64 position within the principal's isolated target map.
    func saveJournal(principal: String, targetKey: String, acceptSeq: Int64) async throws {
        journals[principal, default: [:]][targetKey] = acceptSeq
    }

    /// Removes only one target owned by the supplied principal.
    func removeTarget(principal: String, targetKey: String) async throws {
        journals[principal]?[targetKey] = nil
    }

    /// Clears one principal without changing any other principal's positions.
    func clearPrincipal(principal: String) async throws {
        journals[principal] = nil
    }
}
