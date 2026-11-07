import Darwin
import Foundation
import TinkleSDK

func fixtureClientInfo() -> ClientInfoFfi {
    ClientInfoFfi(deviceModel: "fixture-device", platform: "ios", osVersion: "18",
        appVersion: "1", appBuild: "0001", options: ClientOptionsFfi(agent: "", deviceId: "",
            installId: "", acceptLanguage: "", timeZone: "", userAgent: ""))
}

/// Controls the host-only Rust fixture. All application encryption, framing and
/// handshakes live in the published Syntra SDK used by that fixture.
final class LoopbackGateway: @unchecked Sendable {
    private let id: Int64
    private var stopped = false
    let port: UInt16
    let pin: Data

    struct TargetChangesRequest: Equatable {
        let targetKind: String
        let targetId: Int64
        let afterAcceptSeq: Int64
    }

    init() throws {
        let response = try Self.control(["op": "create", "deletions": []])
        id = (response["id"] as! NSNumber).int64Value
        port = (response["port"] as! NSNumber).uint16Value
        pin = Data((response["pin"] as! [NSNumber]).map(\.uint8Value))
    }

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

    func trustSource() throws -> TrustSourceFfi {
        try TrustSourceFfi.bootstrap(trust: ServerTrustFfi(
            serverPublicKey: pin, serverKeyId: 7, scope: "tinkle-mobile-fixture"))
    }

    func finish() throws {
        if !stopped {
            _ = try Self.control(["op": "close", "id": id])
            stopped = true
        }
    }

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

    private static func ioError(op: String, action: String, code: Int32, bytes: Int) -> FixtureError {
        let kind = code == EAGAIN || code == EWOULDBLOCK || code == ETIMEDOUT ? "timeout" : "system error"
        return FixtureError("control \(op) \(action) \(kind) errno=\(code) bytes=\(bytes)")
    }
}

private struct FixtureError: Error, CustomStringConvertible {
    let message: String
    init(_ message: String) { self.message = message }
    var description: String { message }
}
