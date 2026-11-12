import AVFoundation
import CoreImage
import CryptoKit
import Foundation
@testable import TinkleSDK
import XCTest

/// The normal package helper, real encoders, durable Rust manifest and adoption use one current FFI.
final class NativeSendingTests: XCTestCase {
    /// No connection is opened: successful normalization must persist before upload reports failure.
    private func prepare(_ client: TinkleClient, _ key: String, _ path: String, _ kind: FfiMediaKind) async throws -> FfiAttachmentManifest {
        do {
            _ = try await client.sendingPrepareAttachment(key: key, path: path, kind: kind, control: TransferControl())
            XCTFail("Offline upload unexpectedly completed")
        } catch FfiSendError.Rpc(let error) {
            guard case .Sync = error else { throw error }
        }
        let manifest = try await client.sendingAttachmentManifest(key: key)
        return try XCTUnwrap(manifest)
    }

    /// Exercise normal voice/audio/video/image paths plus retained native adoption and codec-free retry.
    func testNormalNativeSendingAndAdoption() async throws {
        tinkleFfiInit()
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: false)
        defer { try? FileManager.default.removeItem(at: directory) }
        let storage = try await SqliteStorage.openAsync(path: directory.appendingPathComponent("cache.sqlite").path)
        let client = try TinkleClient.newSqlite(config: ClientConfigFfi(clientInfo: fixtureClientInfo(), host: "127.0.0.1", primaryPort: 1, fallbackPort: nil),
            trustSource: TrustSourceFfi.bootstrap(trust: ServerTrustFfi(serverPublicKey: Data(repeating: 7, count: 32), serverKeyId: 7, scope: "native-sending-test")), storage: storage)
        try await client.setPrincipalUserId(userId: 42)
        try client.configureAVFoundationMediaPreparer()
        let pcm = directory.appendingPathComponent("voice.pcm")
        var samples = Data()
        for i in 0..<19_200 {
            var sample = Int16(sin(Double(i) * 2 * .pi * 440 / 48000) * 16000).littleEndian
            withUnsafeBytes(of: &sample) { samples.append(contentsOf: $0) }
        }
        try samples.write(to: pcm)
        let voice = try await prepare(client, "voice", pcm.path, .voice(sampleRate: 48000, channels: 1, bitsPerSample: 16))
        XCTAssertEqual(voice.mimeType, "audio/mp4")
        XCTAssertEqual(voice.nativeFacts?.audioSamples, 19_200)
        let audio = try await prepare(client, "audio", voice.artifactPath, .audio)
        XCTAssertEqual(audio.nativeFacts?.audioChannels, 1)
        let video = directory.appendingPathComponent("source.mp4")
        try videoFixtureBytes.write(to: video)
        let movie = try await prepare(client, "video", video.path, .video)
        XCTAssertEqual(movie.mimeType, "video/mp4")
        XCTAssertEqual(movie.nativeFacts?.frameCount, 4)
        let png = directory.appendingPathComponent("source.png")
        let image = CIImage(color: CIColor(red: 0.2, green: 0.4, blue: 0.6, alpha: 0.5)).cropped(to: CGRect(x: 0, y: 0, width: 32, height: 16))
        try CIContext().writePNGRepresentation(of: image, to: png, format: .RGBA8, colorSpace: CGColorSpace(name: CGColorSpace.sRGB)!)
        let photo = try await prepare(client, "image", png.path, .image)
        XCTAssertEqual(photo.nativeFacts?.width, 32)
        let artifact = try await prepareNativeMedia(path: video.path, kind: .video, preparer: AVFoundationMediaPreparer())
        do {
            _ = try await client.sendingAdoptNativeAttachment(key: "adopt", sourcePath: video.path, prepared: artifact, control: TransferControl())
            XCTFail("Offline adoption upload unexpectedly completed")
        } catch FfiSendError.Rpc(let error) {
            guard case .Sync = error else { throw error }
        }
        let adopted = try await client.sendingAttachmentManifest(key: "adopt")
        XCTAssertEqual(adopted?.outputSha256, artifact.outputSha256())
        XCTAssertEqual(adopted?.preparationProfile, artifact.recipe())
        XCTAssertEqual(adopted?.nativeFacts?.frameCount, artifact.facts().frameCount)
        try client.clearNativeMediaPreparer()
        try FileManager.default.removeItem(at: pcm)
        let retry = try await prepare(client, "voice", pcm.path, .voice(sampleRate: 48000, channels: 1, bitsPerSample: 16))
        XCTAssertEqual(retry.artifactPath, voice.artifactPath)
        XCTAssertEqual(retry.outputSha256, voice.outputSha256)
        await client.shutdown()
        try await storage.clearPrincipal(principal: "user:42")
        XCTAssertFalse(FileManager.default.fileExists(atPath: voice.artifactPath))
    }
}
