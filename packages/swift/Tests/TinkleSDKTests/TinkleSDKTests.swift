import Darwin
@testable import TinkleSDK
import CryptoKit
import XCTest

/// Real AVFoundation decoding plus the current Rust-to-Swift media preparation bridge.
final class AVFoundationVideoDecoderTests: XCTestCase {
    override func setUp() async throws {
        try await super.setUp()
        // Cold simulator media-service startup can exceed the decoder's 10s ceiling.
        // Warm the real codec via the separate 120s preparation contract, never widen decode limits.
        let input = try VideoFileFixture()
        defer { input.remove() }
        _ = try await prepareNativeMedia(path: input.url.path, kind: .video,
            preparer: AVFoundationMediaPreparer())
    }

    /// Encoded streams stay byte-identical; measured sample count is not a header estimate.
    func testActualDecodeOrientationColorAndBounds() async throws {
        let input = try VideoFileFixture()
        defer { input.remove() }
        let original = try Data(contentsOf: input.url)
        let result = try await AVFoundationVideoDecoder().decode(input: input, limits: videoLimits())
        XCTAssertEqual(result.evidence, .nativeDecoded)
        XCTAssertEqual(result.frameCount, 4)
        XCTAssertEqual(result.durationMs, 400)
        XCTAssertEqual(result.width, 32)
        XCTAssertEqual(result.height, 64)
        XCTAssertEqual(result.hasAlpha, false)
        XCTAssertEqual(result.bitDepth, 8)
        let thumbnail = try XCTUnwrap(result.thumbnail)
        XCTAssertEqual(thumbnail.width, 8)
        XCTAssertEqual(thumbnail.height, 16)
        XCTAssertEqual(thumbnail.rgba.count, 8 * 16 * 4)
        // The independent FFmpeg display matrix rotates the original red-left,
        // blue-right image counterclockwise: blue on top, red on the bottom.
        let top = 4 * (2 * 8 + 4)
        let bottom = 4 * (13 * 8 + 4)
        XCTAssertGreaterThan(thumbnail.rgba[top + 2], 220)
        XCTAssertLessThan(thumbnail.rgba[top], 40)
        XCTAssertGreaterThan(thumbnail.rgba[bottom], 220)
        XCTAssertLessThan(thumbnail.rgba[bottom + 2], 40)
        XCTAssertEqual(thumbnail.rgba[top + 3], 255)
        XCTAssertEqual(try Data(contentsOf: input.url), original)
    }

    /// A full decode is mandatory: a frame or pixel budget cannot become qualified metadata.
    func testBudgetAndCancellationAreTyped() async throws {
        let input = try VideoFileFixture()
        defer { input.remove() }
        var frames = videoLimits()
        frames.maxFrames = 3
        var pixels = videoLimits()
        pixels.maxPixels = 1
        var bytes = videoLimits()
        bytes.maxInputBytes = 1
        var deadline = videoLimits()
        deadline.deadlineMs = 0
        for limits in [frames, pixels, bytes, deadline] {
            do {
                _ = try await AVFoundationVideoDecoder().decode(input: input, limits: limits)
                XCTFail("Exceeded limits must not yield nativeDecoded")
            } catch let error as FfiMediaError { XCTAssertEqual(error, .Budget) }
        }
        input.cancellation.cancel()
        do {
            _ = try await AVFoundationVideoDecoder().decode(input: input, limits: videoLimits())
            XCTFail("Cancelled input must not decode")
        } catch let error as FfiMediaError { XCTAssertEqual(error, .Cancelled) }
    }

    /// The native decoder never opens a caller-supplied remote URL or unsupported container.
    func testUnsupportedInputFailsWithoutChangingSource() async throws {
        let input = try VideoFileFixture(bytes: Data("not an MP4 video".utf8))
        defer { input.remove() }
        do {
            _ = try await AVFoundationVideoDecoder().decode(input: input, limits: videoLimits())
            XCTFail("Malformed input must not qualify")
        } catch let error as FfiMediaError { XCTAssertEqual(error, .Unsupported) }
        XCTAssertEqual(try Data(contentsOf: input.url), Data("not an MP4 video".utf8))
    }

    /// Declared HDR and high-depth H.264 profiles are not silently flattened to RGBA8.
    func testUnsupportedColorAndProfileFailClosed() async throws {
        let color = try XCTUnwrap(videoFixtureBytes.range(of: Data("nclx".utf8)))
        var hdr = videoFixtureBytes
        hdr[color.upperBound + 3] = 16 // SMPTE ST 2084 (PQ) transfer characteristic.
        let avc = try XCTUnwrap(videoFixtureBytes.range(of: Data("avcC".utf8)))
        var high10 = videoFixtureBytes
        high10[avc.upperBound + 1] = 110 // H.264 High10, never an 8-bit qualification.
        for bytes in [hdr, high10] {
            let input = try VideoFileFixture(bytes: bytes)
            defer { input.remove() }
            do {
                _ = try await AVFoundationVideoDecoder().decode(input: input, limits: videoLimits())
                XCTFail("Unsupported color/precision must not qualify")
            } catch let error as FfiMediaError { XCTAssertEqual(error, .Unsupported) }
            XCTAssertEqual(try Data(contentsOf: input.url), bytes)
        }
    }

    /// Container headers can remain valid while the encoded frames cannot decode.
    func testDamagedEncodedPayloadCannotQualifyFromHeaders() async throws {
        let mdat = try XCTUnwrap(videoFixtureBytes.range(of: Data("mdat".utf8)))
        let moov = try XCTUnwrap(videoFixtureBytes.range(of: Data("moov".utf8)))
        var bytes = videoFixtureBytes
        bytes.replaceSubrange(mdat.upperBound..<(moov.lowerBound - 4),
            with: repeatElement(UInt8(0), count: moov.lowerBound - 4 - mdat.upperBound))
        let input = try VideoFileFixture(bytes: bytes)
        defer { input.remove() }
        do {
            _ = try await AVFoundationVideoDecoder().decode(input: input, limits: videoLimits())
            XCTFail("A header probe cannot establish nativeDecoded")
        } catch let error as FfiMediaError { XCTAssertEqual(error, .Codec) }
    }

    /// Swift's cancellation flag is observed by the adapter without claiming protocol cancellation.
    func testAlreadyCancelledSwiftTaskStopsNativeWork() async throws {
        let input = try VideoFileFixture()
        defer { input.remove() }
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await AVFoundationVideoDecoder().decode(input: input, limits: videoLimits())
        }
        do { _ = try await task.value; XCTFail("Cancelled Swift task must stop the adapter") }
        catch let error as FfiMediaError { XCTAssertEqual(error, .Cancelled) }
    }

    /// Cancellation cannot free native-worker admission while a synchronous read is parked.
    func testAdmissionRemainsHeldThroughActualWorkerCompletion() async throws {
        let first = try ParkedVideoInput()
        let second = try ParkedVideoInput()
        defer { first.fixture.remove(); second.fixture.remove() }
        let decoder = AVFoundationVideoDecoder()
        let tasks = [first, second].map { input in
            Task { try await decoder.decode(input: input, limits: videoLimits()) }
        }
        defer { first.resume.signal(); second.resume.signal() }
        await fulfillment(of: [first.entered, second.entered], timeout: 30)
        for task in tasks { task.cancel() }
        do {
            _ = try await decoder.decode(input: first.fixture, limits: videoLimits())
            XCTFail("Cancelled waiters must not release still-running native workers")
        } catch let error as FfiMediaError { XCTAssertEqual(error, .Busy) }
        first.resume.signal()
        second.resume.signal()
        for task in tasks {
            do { _ = try await task.value; XCTFail("The worker must observe cancellation after unblocking") }
            catch let error as FfiMediaError { XCTAssertEqual(error, .Cancelled) }
        }
        // Admission is reusable only after the two native closures really return.
        let result = try await decoder.decode(input: first.fixture, limits: videoLimits())
        XCTAssertEqual(result.frameCount, 4)
    }

    /// Normal sending now requires encoding, not the legacy decoder-only registration.
    /// Real Rust creates the frozen lease and projects a typed callback failure before upload.
    func testRustCallsActualPreparerWithFrozenSource() async throws {
        let fixture = try VideoFileFixture()
        defer { fixture.remove() }
        let client = try await videoClient()
        let recorder = MediaCallbackRecorder()
        do {
            try client.configureAVFoundationMediaPreparer()
            try client.configureNativeMediaPreparer(preparer: recorder)
            do {
                _ = try await client.sendingPrepareAttachment(key: "video-native-decode",
                    path: fixture.url.path, kind: .video, control: TransferControl())
                XCTFail("Recorder intentionally prevents any upload")
            } catch FfiSendError.Media(let error) { XCTAssertEqual(error, .Unsupported) }
            let recorded = await recorder.result
            XCTAssertEqual(recorded?.frameCount, 4)
            XCTAssertEqual(recorded?.width, 32)
            XCTAssertEqual(recorded?.height, 64)
            let path = await recorder.path
            XCTAssertNotEqual(path, fixture.url.path)
            let hash = await recorder.hash
            XCTAssertEqual(hash, fixture.sourceSha256())
            XCTAssertEqual(try Data(contentsOf: fixture.url), fixture.bytes)
            try client.clearNativeMediaPreparer()
        } catch {
            await client.shutdown()
            throw error
        }
        await client.shutdown()
    }

    /// Dropping Rust's foreign future cancels the Swift task, but a suspended callback
    /// must retain its read lease until it resumes and finishes native teardown.
    func testCancellationRetainsSuspendedCallbackLeaseUntilCompletion() async throws {
        let fixture = try VideoFileFixture()
        defer { fixture.remove() }
        let client = try await videoClient()
        let callback = SuspendedMediaCallback()
        let control = TransferControl()
        try client.configureNativeMediaPreparer(preparer: callback)
        let pending = Task {
            try await client.sendingPrepareAttachment(key: "video-cancel-lease",
                path: fixture.url.path, kind: .video, control: control)
        }
        // Bounded barrier, not a timing assumption about native callback entry.
        for _ in 0..<500 {
            if await callback.path != nil { break }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        guard let path = await callback.path else {
            control.cancel()
            await callback.resume()
            await client.shutdown()
            // Surface an early typed failure rather than hiding it behind a barrier timeout.
            _ = try await pending.value
            XCTFail("Rust did not invoke media preparation callback")
            return
        }
        control.cancel()
        do { _ = try await pending.value; XCTFail("Cancellation must stop preparation") }
        catch FfiSendError.Media(let error) { XCTAssertEqual(error, .Cancelled) }
        // Remove configuration/client ownership while the invocation remains parked.
        try client.clearNativeMediaPreparer()
        await client.shutdown()
        XCTAssertTrue(FileManager.default.fileExists(atPath: path))
        await callback.resume()
        for _ in 0..<500 {
            if await callback.finished { break }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        let finished = await callback.finished
        XCTAssertTrue(finished)
        for _ in 0..<500 {
            if !FileManager.default.fileExists(atPath: path) { break }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTAssertFalse(FileManager.default.fileExists(atPath: path))
    }

    /// A real offline SQLite client is sufficient; these tests never start a gateway.
    private func videoClient() async throws -> TinkleClient {
        let storage = try await SqliteStorage.openAsync(path: ":memory:")
        let client = try TinkleClient.newSqlite(config: ClientConfigFfi(clientInfo: fixtureClientInfo(),
            host: "127.0.0.1", primaryPort: 9, fallbackPort: nil),
            trustSource: try TrustSourceFfi.bootstrap(trust: ServerTrustFfi(
                serverPublicKey: Data(repeating: 1, count: 32), serverKeyId: 7, scope: "video-fixture")), storage: storage)
        try await client.setPrincipalUserId(userId: 42)
        return client
    }
}

/// Smaller thumbnail budget exercises scaling as well as the independent rotation fixture.
private func videoLimits() -> FfiVideoDecodeLimits {
    FfiVideoDecodeLimits(maxInputBytes: 256 * 1024 * 1024, maxPixels: 16 * 1024 * 1024,
        maxFrames: 18000, maxDurationMs: 600000, thumbnailEdge: 16, deadlineMs: 10000)
}

/// Immutable local input for focused native tests, never lowered across UniFFI.
private final class VideoFileFixture: FfiVideoInputProtocol, Sendable {
    let url: URL
    let bytes: Data
    let cancellation = Progress(totalUnitCount: 1)
    init(bytes: Data = videoFixtureBytes) throws {
        self.bytes = bytes
        url = FileManager.default.temporaryDirectory.appendingPathComponent("video-\(UUID().uuidString)")
        try bytes.write(to: url)
    }
    func sourcePath() -> String {
        XCTAssertFalse(Thread.isMainThread, "Native file/codec work must not run on the UI thread")
        return url.path
    }
    func sourceSha256() -> Data { Data(SHA256.hash(data: bytes)) }
    func sizeBytes() -> UInt64 { UInt64(bytes.count) }
    func isCancelled() -> Bool { cancellation.isCancelled }
    func remove() { try? FileManager.default.removeItem(at: url) }
}

/// A synchronous native-input barrier; all mutable synchronization lives in system primitives.
private final class ParkedVideoInput: FfiVideoInputProtocol, Sendable {
    let fixture: VideoFileFixture
    let entered = XCTestExpectation(description: "Native worker owns admission")
    let resume = DispatchSemaphore(value: 0)
    init() throws { fixture = try VideoFileFixture() }
    func sourcePath() -> String {
        entered.fulfill()
        XCTAssertEqual(resume.wait(timeout: .now() + 60), .success)
        return fixture.sourcePath()
    }
    func sourceSha256() -> Data { fixture.sourceSha256() }
    func sizeBytes() -> UInt64 { fixture.sizeBytes() }
    func isCancelled() -> Bool { fixture.isCancelled() }
}

/// Captures facts from the production encoder; the typed sentinel prevents network upload.
private actor MediaCallbackRecorder: FfiPlatformMediaPreparer {
    private(set) var result: FfiNativeMediaOutput?
    private(set) var path: String?
    private(set) var hash: Data?
    func prepare(input: FfiNativeMediaInput) async throws -> FfiNativeMediaOutput {
        path = input.sourcePath()
        hash = Data(SHA256.hash(data: try Data(contentsOf: URL(fileURLWithPath: input.sourcePath()))))
        result = try await AVFoundationMediaPreparer().prepare(input: input)
        throw FfiMediaError.Unsupported
    }
}

/// A barrier-controlled callback proves invocation ownership after cancellation and shutdown.
private actor SuspendedMediaCallback: FfiPlatformMediaPreparer {
    private(set) var path: String?
    private(set) var finished = false
    private var continuation: CheckedContinuation<Void, Never>?
    private var released = false
    func prepare(input: FfiNativeMediaInput) async throws -> FfiNativeMediaOutput {
        path = input.sourcePath()
        if !released { await withCheckedContinuation { continuation = $0 } }
        defer { finished = true }
        return try await AVFoundationMediaPreparer().prepare(input: input)
    }
    func resume() {
        released = true
        continuation?.resume()
        continuation = nil
    }
}

/// Host-generated FFmpeg/libx264 Baseline, 64x32, four 100ms frames, explicit BT.709,
/// red left/blue right, MP4 display_rotation=90. No executable is needed on iOS.
/// This tiny inline fixture travels through the package's existing source catalog.
let videoFixtureBytes = Data(base64Encoded: """
AAAAIGZ0eXBpc29tAAACAGlzb21pc28yYXZjMW1wNDEAAAAIZnJlZQAAAsZtZGF0AAACcQYF//9t3EXpvebZSLeWLNgg2SPu73gyNjQgLSBjb3JlIDE2NSByMzIyMiBiMzU2MDVhIC0gSC4yNjQvTVBFRy00IEFWQyBjb2RlYyAtIENvcHlsZWZ0IDIwMDMtMjAyNSAtIGh0dHA6Ly93d3cudmlkZW9sYW4ub3JnL3gyNjQuaHRtbCAtIG9wdGlvbnM6IGNhYmFjPTAgcmVmPTMgZGVibG9jaz0xOjA6MCBhbmFseXNlPTB4MToweDExMSBtZT1oZXggc3VibWU9NyBwc3k9MSBwc3lfcmQ9MS4wMDowLjAwIG1peGVkX3JlZj0xIG1lX3JhbmdlPTE2IGNocm9tYV9tZT0xIHRyZWxsaXM9MSA4eDhkY3Q9MCBjcW09MCBkZWFkem9uZT0yMSwxMSBmYXN0X3Bza2lwPTEgY2hyb21hX3FwX29mZnNldD0tMiB0aHJlYWRzPTEgbG9va2FoZWFkX3RocmVhZHM9MSBzbGljZWRfdGhyZWFkcz0wIG5yPTAgZGVjaW1hdGU9MSBpbnRlcmxhY2VkPTAgYmx1cmF5X2NvbXBhdD0wIGNvbnN0cmFpbmVkX2ludHJhPTAgYmZyYW1lcz0wIHdlaWdodHA9MCBrZXlpbnQ9MjUwIGtleWludF9taW49MTAgc2NlbmVjdXQ9NDAgaW50cmFfcmVmcmVzaD0wIHJjX2xvb2thaGVhZD00MCByYz1jcmYgbWJ0cmVlPTEgY3JmPTIzLjAgcWNvbXA9MC42MCBxcG1pbj0wIHFwbWF4PTY5IHFwc3RlcD00IGlwX3JhdGlvPTEuNDAgYXE9MToxLjAwAIAAAAAnZYiED/EYoAAm+xwABHBjgACrlJxGKAAJeccAAXWI4AAssydddPXgAAAABkGaOB/iYAAAAAZBmlQHeJgAAAAGQZpgN8TAAAADSW1vb3YAAABsbXZoZAAAAAAAAAAAAAAAAAAAA+gAAAGQAAEAAAEAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAJzdHJhawAAAFx0a2hkAAAAAwAAAAAAAAAAAAAAAQAAAAAAAAGQAAAAAAAAAAAAAAAAAAAAAAAAAAD//wAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAQAAAAABAAAAAIAAAAAAAJGVkdHMAAAAcZWxzdAAAAAAAAAABAAABkAAAAAAAAQAAAAAB621kaWEAAAAgbWRoZAAAAAAAAAAAAAAAAAAAKAAAABAAVcQAAAAAAC1oZGxyAAAAAAAAAAB2aWRlAAAAAAAAAAAAAAAAVmlkZW9IYW5kbGVyAAAAAZZtaW5mAAAAFHZtaGQAAAABAAAAAAAAAAAAAAAkZGluZgAAABxkcmVmAAAAAAAAAAEAAAAMdXJsIAAAAAEAAAFWc3RibAAAAM5zdHNkAAAAAAAAAAEAAAC+YXZjMQAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAABAACAASAAAAEgAAAAAAAAAARVMYXZjNjIuMjguMTAyIGxpYngyNjQAAAAAAAAAAAAAABj//wAAADFhdmNDAULACv/hABlnQsAK2QRbAWoCAgKAAAADAIAAAAoHiRMkAQAFaMuDyyAAAAATY29scm5jbHgAAQABAAEAAAAAEHBhc3AAAAABAAAAAQAAABRidHJ0AAAAAAAANtgAADbYAAAAGHN0dHMAAAAAAAAAAQAAAAQAAAQAAAAAFHN0c3MAAAAAAAAAAQAAAAEAAAAcc3RzYwAAAAAAAAABAAAAAQAAAAQAAAABAAAAJHN0c3oAAAAAAAAAAAAAAAQAAAKgAAAACgAAAAoAAAAKAAAAFHN0Y28AAAAAAAAAAQAAADAAAABidWR0YQAAAFptZXRhAAAAAAAAACFoZGxyAAAAAAAAAABtZGlyYXBwbAAAAAAAAAAAAAAAAC1pbHN0AAAAJal0b28AAAAdZGF0YQAAAAEAAAAATGF2ZjYyLjEyLjEwMg==
""")!

/// Tests the generated Swift API against the real Rust binary and a local gateway.
final class TinkleSDKTests: XCTestCase {
    /// Explicit stop releases the consumer; Swift Task cancellation is not forwarded to Rust.
    func testAccountStateSubscriptionRetainsSignOutAndUnblocksWaiter() async throws {
        let client = try TinkleClient(config: ClientConfigFfi(clientInfo: fixtureClientInfo(),
            host: "127.0.0.1", primaryPort: 9, fallbackPort: nil),
            trustSource: try TrustSourceFfi.bootstrap(trust: ServerTrustFfi(
                serverPublicKey: Data(repeating: 1, count: 32), serverKeyId: 7, scope: "fixture")),
            store: MemoryCursorStore())
        let stream = try await client.subscribeSessionState()
        let first = await stream.next()
        let initial = try XCTUnwrap(first)
        XCTAssertNil(initial.sessions)
        XCTAssertEqual(initial.status.authorization, .unknown)
        do {
            _ = try await client.subscribeSessionState()
            XCTFail("A second app consumer must be rejected")
        } catch FfiError.Sync { }
        let cancelled = Task { await stream.next() }
        cancelled.cancel()
        // Pinned UniFFI Swift does not propagate Task cancellation to Rust.
        // There is no public Rust waiter-registration barrier: next may start
        // before or after stop. This checks termination, not a parked-waiter race.
        stream.stop()
        let cancelledResult = await cancelled.value
        XCTAssertNil(cancelledResult)
        let terminalStream = try await client.subscribeSessionState()
        _ = try await client.logout()
        let result = await terminalStream.next()
        let terminal = try XCTUnwrap(result)
        XCTAssertEqual(terminal.reason, .selfLogout)
        XCTAssertEqual(terminal.status.authorization, .signedOut)
        XCTAssertEqual(terminal.status.cleanup, .complete)
        terminalStream.stop()
        let stopped = await terminalStream.next()
        XCTAssertNil(stopped)
        let late = try await client.subscribeSessionState()
        let retained = await late.next()
        XCTAssertEqual(retained, terminal)
        let waiting = Task { await late.next() }
        await client.shutdown()
        let ended = await waiting.value
        XCTAssertNil(ended)
    }

    /// Bounded presentation observation; shutdown, not foreign Task cancellation, joins the reader.
    private actor EffectObservation {
        private var first: Data?
        private var count = 0
        func record(_ data: Data) { if first == nil { first = data }; count = min(count + 1, 2) }
        func snapshot() -> (Data?, Int) { (first, count) }
    }

    /// Current native enum/byte ownership delivers one validated Returning presentation hint to Swift.
    /// This observes SDK events, not on-screen animation or production recipient authorization.
    func testReactionEffectCrossesCurrentNativeEnumAsProtobuf() async throws {
        let gateway=try LoopbackGateway(sessionReview:true)
        defer {try? gateway.finish()}
        let client=try await makeSessionReviewClient(gateway)
        let observation=EffectObservation()
        let reader=Task {
            while let event=await client.nextEvent() {
                if case .reactionEffect(let payload)=event {await observation.record(payload)}
            }
        }
        do {
            _ = try await client.start(targets:[TargetSub(targetKind:"user",targetId:7)],watch:[])
            let before=try gateway.targetChangesRequests.count
            try gateway.sendReactionEffectTwice()
            let effectDeadline=Date().addingTimeInterval(2)
            while await observation.snapshot().1 == 0 && Date()<effectDeadline {try await Task.sleep(nanoseconds:1_000_000)}
            let snapshot=await observation.snapshot()
            let data=try XCTUnwrap(snapshot.0)
            XCTAssertNotNil(data.range(of:Data("33333333-3333-4333-8333-333333333333".utf8)))
            try await Task.sleep(nanoseconds:200_000_000)
            let count=await observation.snapshot().1
            XCTAssertEqual(count,1,"the same actor/operation escaped twice at the SDK boundary")
            let deadline=Date().addingTimeInterval(2)
            while try gateway.targetChangesRequests.count <= before && Date()<deadline {try await Task.sleep(nanoseconds:1_000_000)}
            XCTAssertGreaterThan(try gateway.targetChangesRequests.count,before)
            XCTAssertEqual(try gateway.admittedModes,["returning"])
        } catch {await client.shutdown();await reader.value;throw error}
        await client.shutdown()
        await reader.value
    }

    /// Synthetic encrypted Returning replies exercise native result projection, not production account authority.
    func testConfirmSessionProjectsSyntheticReturningResponse() async throws {
        let gateway = try LoopbackGateway(sessionReview: true)
        defer { try? gateway.finish() }
        let client = try await makeSessionReviewClient(gateway)
        do {
            let id = "78e5c289-656b-4428-a091-2dddf6828465"
            let session = try await client.confirmSession(sessionId: id)
            XCTAssertEqual(session, AccountSession(
                id: id, deviceModel: "projection-device", platform: "android-fixture",
                systemVersion: "36-fixture", appVersion: "9.8.7-fixture", appBuild: "00042",
                createdDate: Int64.max - 1, activeDate: Int64.max, ip: "192.0.2.42",
                country: "ZZ", region: "projection-region", current: false, unconfirmed: false))
            XCTAssertEqual(try gateway.confirmSessionRequests, [id])
            XCTAssertEqual(try gateway.admittedModes, ["returning"])
        } catch {
            await client.shutdown()
            throw error
        }
        await client.shutdown()
        try gateway.finish()
    }

    /// A received PermissionDenied preserves its exact RPC status and non-retry guidance.
    func testConfirmSessionProjectsSyntheticPermissionDenied() async throws {
        let gateway = try LoopbackGateway(sessionReview: true)
        defer { try? gateway.finish() }
        let client = try await makeSessionReviewClient(gateway)
        do {
            let id = "96007910-fd5b-4ef2-bf0c-2e4ae1d44b1b"
            do {
                _ = try await client.confirmSession(sessionId: id)
                XCTFail("A fixture denial must not produce a confirmed session")
            } catch FfiError.Rpc(let code, let rawCode, let diagnostic, let unknownOutcome, let recovery) {
                XCTAssertEqual(code, 7)
                XCTAssertNil(rawCode)
                XCTAssertEqual(diagnostic, "synthetic fixture confirmation denied")
                XCTAssertFalse(unknownOutcome)
                XCTAssertEqual(recovery, .doNotRetry)
            }
            XCTAssertEqual(try gateway.confirmSessionRequests, [id], "The mutation must not be replayed")
            XCTAssertEqual(try gateway.admittedModes, ["returning"])
        } catch {
            await client.shutdown()
            throw error
        }
        await client.shutdown()
        try gateway.finish()
    }

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
        let secure = DenyingRestoreSecureStore(active: try fixtureActiveEnrollmentRecord())
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
            } catch FfiError.Rpc(let code, let rawCode, let diagnostic, let unknownOutcome, let recovery) {
                XCTAssertEqual(code, 12) // Unimplemented, not a local transport failure.
                XCTAssertNil(rawCode)
                XCTAssertEqual(diagnostic, "enrollment request rejected")
                XCTAssertFalse(unknownOutcome)
                XCTAssertEqual(recovery, .doNotRetry)
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
            _ = try await client.sendMessage(targetKind: "room", targetId: 0, text: "", idempotencyKey: 1)
            XCTFail("A zero target ID must fail validation")
        } catch FfiError.Codec(let message) {
            XCTAssertTrue(message.contains("target id must be positive"))
        }
        do {
            _ = try await client.sendMessage(targetKind: "room", targetId: 1, text: "offline", idempotencyKey: 1)
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

    /// Restores only the synthetic fixture vector through real callbacks before opening its Returning socket.
    private func makeSessionReviewClient(_ gateway: LoopbackGateway) async throws -> TinkleClient {
        let client = try makeLoopbackClient(gateway, store: MemoryCursorStore())
        do {
            let secure = ProjectionSecureStore(active: try fixtureActiveEnrollmentRecord())
            _ = try await client.restoreActiveEnrollment(store: secure)
            try await client.connect()
            return client
        } catch {
            await client.shutdown()
            throw error
        }
    }
}

/// Single-owner, immutable synthetic legacy store for projection tests only.
/// It grants no restore permits or persistence acknowledgements and models no production secure storage.
private actor ProjectionSecureStore: FfiSecureEnrollmentStore {
    private let active: ProtectedEnrollmentRecord

    /// Retains the existing Rust-validated synthetic vector, not a client or callback owner.
    init(active: ProtectedEnrollmentRecord) { self.active = active }
    /// Selects the legacy read-only path; this isolated fixture has no concurrent namespace writers.
    func namespaceSnapshot() async throws -> ProtectedEnrollmentNamespace? { nil }
    /// Suspends before supplying the immutable synthetic ACTIVE record to Rust.
    func loadActive() async throws -> ProtectedEnrollmentRecord? {
        await Task.yield()
        return active
    }
    /// This legacy fixture cannot grant cross-writer restore exclusion.
    func beginRestore(incarnation: String, revision: String, active: ProtectedEnrollmentRecord, permitId: String) async throws -> Bool {
        throw FfiError.Storage("projection fixture does not grant restore permits")
    }
    /// No conditional permit may be acquired or released on the legacy path.
    func endRestore(permitId: String) async throws { throw FfiError.Storage("unexpected projection permit release") }
    /// Rejects operation lookup rather than pretending to own an enrollment workflow.
    func loadOperation(operationId: String) async throws -> ProtectedEnrollmentRecord? { throw unsupported() }
    /// Never acknowledges a legacy write.
    func commit(kind: SecureEnrollmentWrite, operationId: String, record: ProtectedEnrollmentRecord) async throws { throw unsupported() }
    /// Never acknowledges a conditional write.
    func commitCurrent(expectedRevision: String, kind: SecureEnrollmentWrite, operationId: String, record: ProtectedEnrollmentRecord) async throws -> String? { throw unsupported() }
    /// Never acknowledges a cleanup marker.
    func beginClear(expectedRevision: String) async throws -> Bool { throw unsupported() }
    /// Never acknowledges conditional erasure.
    func finishClear(expectedRevision: String) async throws -> Bool { throw unsupported() }
    /// Never acknowledges namespace deletion.
    func clear() async throws { throw unsupported() }
    /// Fail closed if a projection-only test unexpectedly attempts persistence or cleanup.
    private func unsupported() -> FfiError { FfiError.Storage("projection fixture is read-only") }
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
