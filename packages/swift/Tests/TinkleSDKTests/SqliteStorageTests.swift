import Foundation
import TinkleSDK
import XCTest

/// Uses the shipped Rust SQLite implementation, not a Swift mock or in-memory DB.
final class SqliteStorageTests: XCTestCase {
    func testNativeSqlitePersistenceAndPrincipalIsolation() async throws {
        tinkleFfiInit()
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("TinkleSDKTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = directory.appendingPathComponent("cache.sqlite3")
        // tinkle.v1.User { data { id: 42 } } and Chat { chat_id: 7 }.
        // Both writes go through Rust's protobuf validation and SQLite transactions.
        let userProto = Data([0x12, 0x02, 0x08, 0x2a])
        let chatProto = Data([0x08, 0x07])

        try await writeCache(path: database.path, userProto: userProto, chatProto: chatProto)
        XCTAssertTrue(FileManager.default.fileExists(atPath: database.path))
        let header = try Data(contentsOf: database).prefix(16)
        XCTAssertEqual(Data(header), Data("SQLite format 3\0".utf8))

        // Each helper releases its Rust handles before the next open: this must
        // survive reopening a real file, not just reading the writer's memory.
        try await verifyAndClearCache(path: database.path, userProto: userProto, chatProto: chatProto)
        let reopened = try await SqliteStorage.openAsync(path: database.path)
        let clearedUser = try await reopened.getUserProto(principal: "user:42", userId: 42)
        let clearedChat = try await reopened.getChatProto(principal: "user:42", chatId: 7)
        let retainedUser = try await reopened.getUserProto(principal: "user:99", userId: 42)
        let retainedChat = try await reopened.getChatProto(principal: "user:99", chatId: 7)
        XCTAssertNil(clearedUser)
        XCTAssertNil(clearedChat)
        XCTAssertEqual(retainedUser, userProto)
        XCTAssertEqual(retainedChat, chatProto)
    }

    private func writeCache(path: String, userProto: Data, chatProto: Data) async throws {
        let storage = try await SqliteStorage.openAsync(path: path)
        for principal in ["user:42", "user:99"] {
            try await storage.saveUserProto(principal: principal, userId: 42, userProto: userProto)
            try await storage.saveChatProto(principal: principal, chatId: 7, chatProto: chatProto)
        }
        // Exercise the native SQLite client constructor with the same handle.
        let client = try TinkleClient.newSqlite(
            config: ClientConfigFfi(
                url: "ws://127.0.0.1:9/syntra/v1/ws", allowInsecure: true,
                userAgent: "TinkleSDKSqliteTests", grpcEndpoint: nil
            ),
            credential: "local-smoke-test",
            storage: storage
        )
        try await client.setPrincipalUserId(userId: 42)
        await client.shutdown()
    }

    private func verifyAndClearCache(path: String, userProto: Data, chatProto: Data) async throws {
        let storage = try await SqliteStorage.openAsync(path: path)
        for principal in ["user:42", "user:99"] {
            let user = try await storage.getUserProto(principal: principal, userId: 42)
            let chat = try await storage.getChatProto(principal: principal, chatId: 7)
            XCTAssertEqual(user, userProto)
            XCTAssertEqual(chat, chatProto)
        }
        let absentUser = try await storage.getUserProto(principal: "user:123", userId: 42)
        let absentChat = try await storage.getChatProto(principal: "user:123", chatId: 7)
        let journal = try await storage.loadJournal(principal: "user:42", targetKey: "target:room:7")
        let messages = try await storage.getMessages(
            principal: "user:42", conversationKey: "target:room:7", limit: 10, beforeId: nil
        )
        let dialogs = try await storage.listDialogs(principal: "user:42", limit: 10)
        XCTAssertNil(absentUser)
        XCTAssertNil(absentChat)
        XCTAssertNil(journal)
        XCTAssertTrue(messages.isEmpty)
        XCTAssertTrue(dialogs.isEmpty)

        do {
            try await storage.saveUserProto(principal: "user:42", userId: 99, userProto: userProto)
            XCTFail("A mismatched embedded user ID must be rejected")
        } catch FfiError.Codec(let message) {
            XCTAssertTrue(message.contains("id mismatch"))
        }
        // The client in writeCache has shut down before deleting its principal.
        try await storage.clearPrincipal(principal: "user:42")
    }
}
