import CryptoKit
import Darwin
import Foundation

/// One-client test fixture, mirroring libtinkle's client_integration gateway.
/// Implements only WebSocket upgrade, Syntra hello/ready, and GetTargetChanges.
/// Field IDs match syntra.v1.Frame and tinkle.v1.GetTargetChangesResponse. This is not
/// a transport implementation for application use.
final class LoopbackGateway: @unchecked Sendable {
    private let lock = NSLock()
    private let worker = DispatchGroup()
    private let listener: Int32
    private var connection: Int32 = -1
    private var stopped = false
    private var failure: Error?
    private var requests: [TargetChangesRequest] = []
    let port: UInt16

    struct TargetChangesRequest: Equatable {
        let targetKind: String
        let targetId: Int64
        let afterAcceptSeq: Int64
    }

    var targetChangesRequests: [TargetChangesRequest] {
        lock.lock()
        defer { lock.unlock() }
        return requests
    }

    init() throws {
        let fd = Darwin.socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { throw FixtureError("socket failed") }
        do {
            var address = sockaddr_in()
            address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
            address.sin_family = sa_family_t(AF_INET)
            address.sin_addr.s_addr = inet_addr("127.0.0.1")
            let bound = withUnsafePointer(to: &address) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    Darwin.bind(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
                }
            }
            guard bound == 0, Darwin.listen(fd, 1) == 0 else { throw FixtureError("bind/listen failed") }
            var size = socklen_t(MemoryLayout<sockaddr_in>.size)
            let named = withUnsafeMutablePointer(to: &address) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { getsockname(fd, $0, &size) }
            }
            guard named == 0 else { throw FixtureError("getsockname failed") }
            port = UInt16(bigEndian: address.sin_port)
            listener = fd
        } catch {
            Darwin.close(fd)
            throw error
        }
        worker.enter()
        DispatchQueue(label: "tinkle-swift-smoke-gateway").async {
            defer { self.worker.leave() }
            do {
                try self.serve()
            } catch {
                self.lock.lock()
                if !self.stopped { self.failure = error }
                self.lock.unlock()
            }
        }
    }

    func finish() throws {
        lock.lock()
        if !stopped {
            stopped = true
            Darwin.shutdown(listener, SHUT_RDWR)
            Darwin.close(listener)
            if connection >= 0 { Darwin.shutdown(connection, SHUT_RDWR) }
        }
        lock.unlock()
        guard worker.wait(timeout: .now() + 3) == .success else { throw FixtureError("gateway did not stop") }
        lock.lock()
        let error = failure
        lock.unlock()
        if let error { throw error }
    }

    private func serve() throws {
        let fd = Darwin.accept(listener, nil, nil)
        guard fd >= 0 else { throw FixtureError("accept failed") }
        lock.lock()
        if stopped {
            Darwin.close(fd)
            lock.unlock()
            return
        }
        connection = fd
        lock.unlock()
        defer {
            lock.lock()
            connection = -1
            Darwin.close(fd)
            lock.unlock()
        }
        var timeout = timeval(tv_sec: 20, tv_usec: 0)
        var noSignal: Int32 = 1
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &noSignal, socklen_t(MemoryLayout<Int32>.size))
        var request: [UInt8] = []
        while !request.suffix(4).elementsEqual([13, 10, 13, 10]) {
            guard request.count < 16_384 else { throw FixtureError("HTTP header too long") }
            request += try read(fd, 1)
        }
        let headers = String(decoding: request, as: UTF8.self).components(separatedBy: "\r\n")
        guard let keyLine = headers.first(where: { $0.lowercased().hasPrefix("sec-websocket-key:") }),
              let separator = keyLine.firstIndex(of: ":") else { throw FixtureError("missing WebSocket key") }
        let key = keyLine[keyLine.index(after: separator)...].trimmingCharacters(in: .whitespaces)
        let digest = Insecure.SHA1.hash(data: Data((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").utf8))
        let accept = Data(digest).base64EncodedString()
        try write(fd, Array(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n" +
            "Connection: Upgrade\r\nSec-WebSocket-Protocol: syntra.v1\r\n" +
            "Sec-WebSocket-Accept: \(accept)\r\n\r\n").utf8))
        guard let helloBody = try readBinary(fd) else {
            throw FixtureError("missing Syntra hello")
        }
        let helloFrame = try fields(helloBody)
        guard helloFrame[1]?.number == 1, (helloFrame[2]?.number ?? 0) == 0,
              let hello = helloFrame[10]?.bytes else { throw FixtureError("invalid Syntra hello frame") }
        let greeting = try fields(hello)
        guard greeting[1]?.number == 1, greeting[2]?.number == 1,
              let credential = greeting[3]?.bytes, !credential.isEmpty else {
            throw FixtureError("invalid Syntra hello")
        }
        try send(fd, number(1, 1) + bytes(11, bytes(1, Array("swift-smoke".utf8))))
        while let body = try readBinary(fd) {
            let frame = try fields(body)
            guard let request = frame[12]?.bytes else { continue }
            let rpc = try fields(request)
            guard frame[1]?.number == 1,
                  rpc[1]?.bytes == Array("/tinkle.v1.MessagesService/GetTargetChanges".utf8),
                  let id = frame[2]?.number, id > 0 else { throw FixtureError("unexpected fixture RPC") }
            let payload = try fields(rpc[2]?.bytes ?? [])
            let target = try fields(payload[1]?.bytes ?? [])
            guard (target[1] != nil) != (target[2] != nil),
                  let targetId = target[1]?.number ?? target[2]?.number,
                  targetId > 0, targetId <= UInt64(Int64.max),
                  let limit = payload[3]?.number, (1...500).contains(limit) else {
                throw FixtureError("invalid target changes request")
            }
            let afterAcceptSeq = payload[2]?.number ?? 0
            guard afterAcceptSeq <= UInt64(Int64.max) else { throw FixtureError("negative acceptance position") }
            let decoded = TargetChangesRequest(
                targetKind: target[1] != nil ? "user" : "room",
                targetId: Int64(targetId), afterAcceptSeq: Int64(afterAcceptSeq)
            )
            lock.lock()
            requests.append(decoded)
            lock.unlock()
            // Empty page: explicitly caught up, with the request's cursor.
            // Omitting optional more_available would mean unknown, not false.
            let response = number(2, 0) + number(3, afterAcceptSeq)
            try send(fd, number(1, 1) + number(2, id) + bytes(13, bytes(1, response)))
        }
    }

    private func read(_ fd: Int32, _ count: Int, allowEOF: Bool = false) throws -> [UInt8] {
        var result = [UInt8](repeating: 0, count: count)
        var offset = 0
        while offset < count {
            let received = result.withUnsafeMutableBytes {
                Darwin.recv(fd, $0.baseAddress!.advanced(by: offset), count - offset, 0)
            }
            if received < 0 && errno == EINTR { continue }
            if received == 0 && allowEOF && offset == 0 { return [] }
            guard received > 0 else { throw FixtureError("socket read failed/EOF") }
            offset += received
        }
        return result
    }

    private func write(_ fd: Int32, _ bytes: [UInt8]) throws {
        var offset = 0
        while offset < bytes.count {
            let sent = bytes.withUnsafeBytes {
                Darwin.send(fd, $0.baseAddress!.advanced(by: offset), bytes.count - offset, 0)
            }
            if sent < 0 && errno == EINTR { continue }
            guard sent > 0 else { throw FixtureError("socket write failed") }
            offset += sent
        }
    }

    private func readBinary(_ fd: Int32) throws -> [UInt8]? {
        while true {
            let first = try read(fd, 1, allowEOF: true)
            if first.isEmpty { return nil }
            guard first[0] & 0x80 != 0 else { throw FixtureError("fragmented fixture frame") }
            let second = try read(fd, 1)[0]
            var length = UInt64(second & 0x7f)
            if length == 126 || length == 127 {
                let size = length == 126 ? 2 : 8
                length = try read(fd, size).reduce(0) { ($0 << 8) | UInt64($1) }
            }
            guard length <= 1_048_576 else { throw FixtureError("oversized fixture frame") }
            let mask = second & 0x80 != 0 ? try read(fd, 4) : []
            var body = try read(fd, Int(length))
            if !mask.isEmpty {
                for index in body.indices { body[index] ^= mask[index % 4] }
            }
            switch first[0] & 0x0f {
            case 2: return body
            case 8: try send(fd, body, opcode: 8); return nil
            case 9: try send(fd, body, opcode: 10)
            case 10: continue
            default: throw FixtureError("unexpected fixture opcode")
            }
        }
    }

    private func send(_ fd: Int32, _ body: [UInt8], opcode: UInt8 = 2) throws {
        guard body.count < 126 else { throw FixtureError("fixture response too long") }
        try write(fd, [0x80 | opcode, UInt8(body.count)] + body)
    }

    private func varint(_ value: UInt64) -> [UInt8] {
        var remaining = value
        var result: [UInt8] = []
        repeat {
            let byte = UInt8(remaining & 127)
            remaining >>= 7
            result.append(byte | (remaining == 0 ? 0 : 128))
        } while remaining != 0
        return result
    }

    private func number(_ field: Int, _ value: UInt64) -> [UInt8] {
        varint(UInt64(field * 8)) + varint(value)
    }

    private func bytes(_ field: Int, _ value: [UInt8]) -> [UInt8] {
        varint(UInt64(field * 8 + 2)) + varint(UInt64(value.count)) + value
    }

    private enum Value {
        case integer(UInt64), buffer([UInt8])
        var number: UInt64? { if case .integer(let value) = self { return value }; return nil }
        var bytes: [UInt8]? { if case .buffer(let value) = self { return value }; return nil }
    }

    private func fields(_ message: [UInt8]) throws -> [Int: Value] {
        var offset = 0
        func next() throws -> UInt64 {
            var result: UInt64 = 0
            for shift in stride(from: 0, through: 63, by: 7) {
                guard offset < message.count else { throw FixtureError("truncated varint") }
                let byte = message[offset]
                offset += 1
                guard shift < 63 || byte <= 1 else { throw FixtureError("varint overflow") }
                result |= UInt64(byte & 127) << shift
                if byte & 128 == 0 { return result }
            }
            throw FixtureError("invalid varint")
        }
        var result: [Int: Value] = [:]
        while offset < message.count {
            let key = try next()
            guard key >> 3 > 0, key >> 3 <= UInt64(Int.max) else { throw FixtureError("invalid field") }
            switch key & 7 {
            case 0: result[Int(key >> 3)] = .integer(try next())
            case 2:
                let length = try next()
                guard length <= UInt64(message.count - offset) else { throw FixtureError("truncated bytes") }
                result[Int(key >> 3)] = .buffer(Array(message[offset..<(offset + Int(length))]))
                offset += Int(length)
            default: throw FixtureError("unsupported fixture field")
            }
        }
        return result
    }

    private struct FixtureError: Error, CustomStringConvertible {
        let description: String
        init(_ description: String) { self.description = description }
    }
}
