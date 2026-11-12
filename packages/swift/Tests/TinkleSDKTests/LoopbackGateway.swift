import Darwin
import Foundation
import TinkleSDK

/// Supplies complete, nonsecret client labels for native fixture connections.
func fixtureClientInfo() -> ClientInfoFfi {
    ClientInfoFfi(deviceModel: "fixture-device", platform: "ios", osVersion: "18",
        appVersion: "1", appBuild: "0001", options: ClientOptionsFfi(agent: "", deviceId: "",
            installId: "", acceptLanguage: "", timeZone: "", userAgent: ""))
}

/// Loads Rust-validated opaque synthetic ACTIVE bytes without recreating the private record format.
func fixtureActiveEnrollmentRecord() throws -> ProtectedEnrollmentRecord {
    let hex = Array(EnrollmentReviewVector.activeRecordHex)
    var bytes = Data(try stride(from: 0, to: hex.count, by: 2).map {
        guard let byte = UInt8(String(hex[$0..<$0 + 2]), radix: 16) else {
            throw FixtureError("invalid synthetic ACTIVE vector")
        }
        return byte
    })
    defer { bytes.resetBytes(in: 0..<bytes.count) }
    return ProtectedEnrollmentRecord.fromSecureStorage(bytes: bytes)
}

/// Controls the host-only Rust fixture. All application encryption, framing and
/// handshakes live in the published Syntra SDK used by that fixture.
final class LoopbackGateway: @unchecked Sendable {
    private let id: Int64
    private var stopped = false
    let port: UInt16
    let pin: Data
    private let scope: String

    struct TargetChangesRequest: Equatable {
        let targetKind: String
        let targetId: Int64
        let afterAcceptSeq: Int64
    }

    /// Defaults to Bootstrap; session review explicitly selects synthetic Returning for projection, not authority proof.
    init(enrollmentReview: Bool = false, sessionReview: Bool = false) throws {
        guard !(enrollmentReview && sessionReview) else {
            throw FixtureError("restore-denial and Returning projection modes must be separate")
        }
        let response = try Self.control(["op": "create", "deletions": [],
            "enrollment_review": enrollmentReview, "session_review": sessionReview])
        id = (response["id"] as! NSNumber).int64Value
        port = (response["port"] as! NSNumber).uint16Value
        pin = Data((response["pin"] as! [NSNumber]).map(\.uint8Value))
        scope = enrollmentReview || sessionReview ? "fixture" : "tinkle-mobile-fixture"
    }

    /// Recorded by Syntra's verified-identity callback, never inferred from RPCs.
    var admittedModes: [String] {
        get throws {
            let response = try Self.control(["op": "requests", "id": id])
            guard let modes = response["admissions"] as? [String] else {
                throw FixtureError("fixture response lacks authenticated admissions")
            }
            return modes
        }
    }

    /// Returns journal positions decoded from actual encrypted application requests.
    var targetChangesRequests: [TargetChangesRequest] {
        get throws {
            let response = try Self.control(["op": "requests", "id": id])
            return (response["requests"] as! [[String: Any]]).map {
                TargetChangesRequest(targetKind: $0["target_kind"] as! String,
                    targetId: ($0["target_id"] as! NSNumber).int64Value,
                    afterAcceptSeq: ($0["after_accept_seq"] as! NSNumber).int64Value)
            }
        }
    }

    /// Returns session identifiers decoded by the Rust handler from encrypted ConfirmSession requests.
    var confirmSessionRequests: [String] {
        get throws {
            let response = try Self.control(["op": "requests", "id": id])
            guard let ids = response["confirmations"] as? [String] else {
                throw FixtureError("fixture response lacks confirmation requests")
            }
            return ids
        }
    }

    /// Creates a fresh one-use Bootstrap source pinned to this gateway.
    func trustSource() throws -> TrustSourceFfi {
        try TrustSourceFfi.bootstrap(trust: ServerTrustFfi(
            serverPublicKey: pin, serverKeyId: 7, scope: scope))
    }

    /// Sends one fixed synthetic operation twice through the gateway's actual encrypted host.
    func sendReactionEffectTwice() throws {
        let response=try Self.control(["op":"reaction_effect","id":id])
        guard (response["notified"] as? NSNumber)?.intValue == 2 else {throw FixtureError("effect fixture has no live host")}
    }

    /// Idempotently stops the gateway and waits for its accepted transports to drain.
    func finish() throws {
        if !stopped {
            _ = try Self.control(["op": "close", "id": id])
            stopped = true
        }
    }

    /// Performs one bounded loopback control exchange outside the SDK transport.
    private static func control(_ request: [String: Any]) throws -> [String: Any] {
        let op = request["op"] as? String ?? "unknown"
        let fd = Darwin.socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { throw FixtureError("control \(op) socket failed errno=\(errno) bytes=0") }
        defer { Darwin.close(fd) }
        var timeout = timeval(tv_sec: 5, tv_usec: 0)
        guard setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size)) == 0 else {
            throw FixtureError("control \(op) SO_RCVTIMEO failed errno=\(errno) bytes=0")
        }
        guard setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size)) == 0 else {
            throw FixtureError("control \(op) SO_SNDTIMEO failed errno=\(errno) bytes=0")
        }
        var address = sockaddr_in()
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        address.sin_family = sa_family_t(AF_INET)
        address.sin_port = UInt16(48080).bigEndian
        address.sin_addr.s_addr = inet_addr("127.0.0.1")
        let connected = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard connected == 0 else { throw FixtureError("control \(op) connect failed errno=\(errno) bytes=0; start cargo run --locked -p libtinkle --example native-fixture before mobile tests") }
        let bytes = try JSONSerialization.data(withJSONObject: request) + Data([10])
        try sendAll(bytes, fd: fd, op: op)
        let response = try readLine(fd: fd, op: op)
        return try JSONSerialization.jsonObject(with: response) as! [String: Any]
    }

    /// Completes partial writes, retrying only interruptions without replaying bytes.
    static func sendAll(_ bytes: Data, fd: Int32, op: String,
        send: (Int32, UnsafeRawPointer?, Int, Int32) -> Int = Darwin.send) throws {
        var offset = 0
        while offset < bytes.count {
            let sent = bytes.withUnsafeBytes { send(fd, $0.baseAddress!.advanced(by: offset), bytes.count - offset, 0) }
            if sent < 0 {
                let code = errno
                if code == EINTR { continue }
                throw ioError(op: op, action: "send", code: code, bytes: offset)
            }
            guard sent > 0 else { throw FixtureError("control \(op) send made no progress bytes=\(offset)") }
            offset += sent
        }
    }

    /// Reads one capped newline-terminated response, with deterministic I/O injection for tests.
    static func readLine(fd: Int32, op: String,
        recv: (Int32, UnsafeMutableRawPointer?, Int, Int32) -> Int = Darwin.recv) throws -> Data {
        var response = Data()
        var buffer = [UInt8](repeating: 0, count: 4096)
        while response.count < 65536 {
            let capacity = min(buffer.count, 65536 - response.count)
            let received = buffer.withUnsafeMutableBytes { recv(fd, $0.baseAddress, capacity, 0) }
            if received < 0 {
                let code = errno
                if code == EINTR { continue }
                throw ioError(op: op, action: "recv", code: code, bytes: response.count)
            }
            guard received > 0 else { throw FixtureError("control \(op) recv EOF before newline bytes=\(response.count)") }
            if let newline = buffer[..<received].firstIndex(of: 10) {
                response.append(contentsOf: buffer[...newline])
                return response
            }
            response.append(contentsOf: buffer[..<received])
        }
        throw FixtureError("control \(op) recv oversized response limit=65536 bytes=\(response.count)")
    }

    /// Formats transport diagnostics without including request or response contents.
    private static func ioError(op: String, action: String, code: Int32, bytes: Int) -> FixtureError {
        let kind = code == EAGAIN || code == EWOULDBLOCK || code == ETIMEDOUT ? "timeout" : "system error"
        return FixtureError("control \(op) \(action) \(kind) errno=\(code) bytes=\(bytes)")
    }
}

private struct FixtureError: Error, CustomStringConvertible {
    let message: String
    /// Retains a control-channel diagnostic for XCTest failure reporting.
    init(_ message: String) { self.message = message }
    var description: String { message }
}
