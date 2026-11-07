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
        let fd = Darwin.socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { throw FixtureError("control socket failed") }
        defer { Darwin.close(fd) }
        var timeout = timeval(tv_sec: 5, tv_usec: 0)
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
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
        guard connected == 0 else { throw FixtureError("start cargo run --locked -p libtinkle --example native-fixture before mobile tests") }
        let bytes = try JSONSerialization.data(withJSONObject: request) + Data([10])
        var offset = 0
        while offset < bytes.count {
            let sent = bytes.withUnsafeBytes { Darwin.send(fd, $0.baseAddress!.advanced(by: offset), bytes.count - offset, 0) }
            guard sent > 0 else { throw FixtureError("control send failed") }
            offset += sent
        }
        var response = Data()
        while response.last != 10 {
            var byte: UInt8 = 0
            guard Darwin.recv(fd, &byte, 1, 0) == 1, response.count < 65536 else { throw FixtureError("control response failed") }
            response.append(byte)
        }
        return try JSONSerialization.jsonObject(with: response) as! [String: Any]
    }
}

private struct FixtureError: Error {
    let message: String
    init(_ message: String) { self.message = message }
}
