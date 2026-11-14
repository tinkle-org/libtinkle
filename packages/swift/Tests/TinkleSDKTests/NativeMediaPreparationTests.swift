import AVFoundation
import CoreImage
import Foundation
import Darwin
import CryptoKit
#if !TINKLE_MEDIA_STANDALONE
@testable import TinkleSDK
import XCTest

final class NativeMediaPreparationTests: XCTestCase {
    func testRealNativeEncodingAndOwnedArtifacts() async throws {
        try await NativeMediaFixtures.run()
    }
}
#else
/// The same fixture can run as a simulator executable without staging unrelated consumer tests.
@main struct NativeMediaStandalone {
    static func main() async throws {
        setbuf(stdout, nil)
        try await NativeMediaFixtures.run()
        print("PASS native fixture; codec-only:", ProcessInfo.processInfo.environment["TINKLE_MEDIA_CODEC_ONLY"] == "1",
            "image-only:", ProcessInfo.processInfo.environment["TINKLE_MEDIA_IMAGE_ONLY"] == "1")
    }
}
#endif

/// Actual encode fixtures; no mocked codec, decoder capability flag, or metadata-only success.
private enum NativeMediaFixtures {
    /// Codec-only probes do not qualify a staged Rust SDK; the normal test uses the real FFI owner.
    private static let codecOnly = ProcessInfo.processInfo.environment["TINKLE_MEDIA_CODEC_ONLY"] == "1"
    private struct Artifact {
        let owner: FfiPreparedNativeMedia?
        let url: URL
        let measured: FfiNativeMediaOutput
        let mime: String
        let sourceDigest: Data
        let outputDigest: Data
        func path() -> String { url.path }
        func facts() -> FfiNativeMediaOutput { measured }
        func mimeType() -> String { mime }
        func sourceSha256() -> Data { sourceDigest }
        func outputSha256() -> Data { outputDigest }
        func waveform() -> Data { owner?.waveform() ?? Data() }
        func recipe() -> String { owner?.recipe() ?? "native-codec-only" }
    }
    private final class Input: FfiNativeMediaInputProtocol, @unchecked Sendable {
        let source: URL, output: URL, purpose: FfiNativeMediaKind, size: UInt64
        init(_ source: URL, _ output: URL, _ purpose: FfiNativeMediaKind) throws {
            self.source = source; self.output = output; self.purpose = purpose
            size = UInt64(try source.resourceValues(forKeys: [.fileSizeKey]).fileSize!)
        }
        func sourcePath() -> String { source.path }
        func outputPath() -> String { output.path }
        func sizeBytes() -> UInt64 { size }
        func kind() -> FfiNativeMediaKind { purpose }
        func isCancelled() -> Bool { false }
        func maxOutputBytes() -> UInt64 { if case .image = purpose { return 10_000_000 }; return 256 * 1024 * 1024 }
        func maxDurationMs() -> UInt64 { 600_000 }
        func maxFrames() -> UInt32 { 18_000 }
        func deadlineMs() -> UInt32 { 120_000 }
    }
    private static func digest(_ url: URL) throws -> Data {
        let file = try FileHandle(forReadingFrom: url)
        defer { try? file.close() }
        var hash = SHA256()
        while let bytes = try file.read(upToCount: 65536), !bytes.isEmpty { hash.update(data: bytes) }
        return Data(hash.finalize())
    }
    private static func prepare(path: String, kind: FfiNativeMediaKind, preparer: AVFoundationMediaPreparer) async throws -> Artifact {
        if !codecOnly {
            let value = try await prepareNativeMedia(path: path, kind: kind, preparer: preparer)
            // The fixture's independent native readers need a typed read alias; keep the real
            // Rust owner and exact inode until validation ends. The fixture directory owns cleanup.
            let suffix = value.mimeType() == "image/png" ? ".png" : ".mp4"
            let alias = URL(fileURLWithPath: path).deletingLastPathComponent().appendingPathComponent("read-\(UUID().uuidString)\(suffix)")
            try FileManager.default.linkItem(atPath: value.path(), toPath: alias.path)
            return Artifact(owner: value, url: alias, measured: value.facts(), mime: value.mimeType(),
                sourceDigest: value.sourceSha256(), outputDigest: value.outputSha256())
        }
        let source = URL(fileURLWithPath: path)
        let directory = source.deletingLastPathComponent()
        let output = directory.appendingPathComponent("encoded-\(UUID().uuidString).mp4")
        let facts = try await preparer.prepareLocal(input: Input(source, output, kind))
        let mime: String
        switch kind { case .image: mime = "image/png"; case .video: mime = "video/mp4"; default: mime = "audio/mp4" }
        if let retained = ProcessInfo.processInfo.environment["TINKLE_NATIVE_CODEC_ARTIFACT_DIR"] {
            if !FileManager.default.fileExists(atPath: retained) { try FileManager.default.createDirectory(atPath: retained, withIntermediateDirectories: false) }
            let name: String
            switch kind { case .voice: name = "voice"; case .audio: name = "audio"; case .video: name = "video"; case .image: name = facts.width > 32 ? "image-large" : "image" }
            let target = URL(fileURLWithPath: retained).appendingPathComponent(name + ".mp4")
            try FileManager.default.copyItem(at: output, to: target)
        }
        return Artifact(owner: nil, url: output, measured: facts, mime: mime,
            sourceDigest: try digest(source), outputDigest: try digest(output))
    }
    static func run() async throws {
        tinkleFfiInit()
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("native-media-tests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: false)
        defer { try? FileManager.default.removeItem(at: directory) }
        let preparer = AVFoundationMediaPreparer()
        if ProcessInfo.processInfo.environment["TINKLE_MEDIA_IMAGE_ONLY"] == "1" {
            let path = ProcessInfo.processInfo.environment["TINKLE_MEDIA_100MP_FIXTURE_PATH"]!
            var before = rusage(); getrusage(RUSAGE_SELF, &before)
            let result = try await prepare(path: path, kind: .image, preparer: preparer)
            var after = rusage(); getrusage(RUSAGE_SELF, &after)
            precondition(UInt64(result.facts().width) * UInt64(result.facts().height) <= 16 * 1024 * 1024)
            print("PASS isolated 100MP image", result.facts().width, result.facts().height,
                "Darwin peak resident bytes before/after", before.ru_maxrss, after.ru_maxrss)
            return
        }
        let pcm = directory.appendingPathComponent("voice.pcm")
        var bytes = Data()
        for index in 0..<19_200 {
            var sample = Int16(sin(Double(index) * 2 * .pi * 440 / 48000) * 16000).littleEndian
            withUnsafeBytes(of: &sample) { bytes.append(contentsOf: $0) }
        }
        try bytes.write(to: pcm)
        print("fixture: PCM16 voice encoding")
        let voice = try await prepare(path: pcm.path,
            kind: .voice(sampleRate: 48000, channels: 1, bitsPerSample: 16), preparer: preparer)
        defer { withExtendedLifetime(voice) {} }
        precondition(voice.mimeType() == "audio/mp4")
        precondition(voice.facts().audioSamples == 19_200 && voice.facts().audioChannels == 1)
        precondition(voice.facts().durationMs == 400 && (codecOnly || voice.waveform().count == 100))
        try checkSine(URL(fileURLWithPath: voice.path()))
        let retainedSource = try Data(contentsOf: pcm)
        precondition(retainedSource == bytes)
        let firstHash = voice.outputSha256()
        precondition(voice.outputSha256() == firstHash)
        let imported = try await prepare(path: voice.path(), kind: .audio, preparer: preparer)
        precondition(imported.facts().width == 0 && imported.facts().audioChannels == 1 && imported.waveform().isEmpty)
        try checkSine(URL(fileURLWithPath: imported.path()))
        let cancelledOutput = directory.appendingPathComponent("cancelled.mp4")
        let cancelled = Task { try await preparer.prepareLocal(input: Input(pcm, cancelledOutput,
            .voice(sampleRate: 48000, channels: 1, bitsPerSample: 16))) }
        cancelled.cancel()
        do { _ = try await cancelled.value; preconditionFailure("cancelled work completed") }
        catch FfiMediaError.Cancelled { }
        precondition(!FileManager.default.fileExists(atPath: cancelledOutput.path))

        let source = directory.appendingPathComponent("source.mp4")
        print("fixture: 4K video source encoding")
        try await video(source)
        let videoAsset = AVURLAsset(url: source)
        let audioAsset = AVURLAsset(url: URL(fileURLWithPath: voice.path()))
        let composition = AVMutableComposition()
        let videoTrack = composition.addMutableTrack(withMediaType: .video, preferredTrackID: kCMPersistentTrackID_Invalid)!
        let originalTrack = videoAsset.tracks(withMediaType: .video)[0]
        try videoTrack.insertTimeRange(CMTimeRange(start: .zero, duration: CMTime(value: 4, timescale: 10)), of: originalTrack, at: .zero)
        videoTrack.preferredTransform = originalTrack.preferredTransform
        let audioTrack = composition.addMutableTrack(withMediaType: .audio, preferredTrackID: kCMPersistentTrackID_Invalid)!
        try audioTrack.insertTimeRange(CMTimeRange(start: .zero, duration: CMTime(value: 4, timescale: 10)), of: audioAsset.tracks(withMediaType: .audio)[0], at: .zero)
        let mixed = directory.appendingPathComponent("mixed.mov")
        let export = AVAssetExportSession(asset: composition, presetName: AVAssetExportPresetPassthrough)!
        export.outputURL = mixed; export.outputFileType = .mov
        await withCheckedContinuation { continuation in export.exportAsynchronously { continuation.resume() } }
        guard export.status == .completed else { throw FfiMediaError.Codec }
        print("fixture: portrait H264 + AAC normalization")
        let encoded = try await prepare(path: mixed.path, kind: .video, preparer: preparer)
        defer { withExtendedLifetime(encoded) {} }
        let facts = encoded.facts()
        precondition(facts.width == 1080 && facts.height == 1920 && facts.frameCount == 4)
        precondition(facts.audioChannels == 1 && facts.audioSampleRate == 48000 && facts.audioSamples > 0)
        try checkSine(URL(fileURLWithPath: encoded.path()))
        precondition(encoded.sourceSha256() != encoded.outputSha256())
        // Explicit override permits codec-only probing against an older bootstrap artifact.
        let expectedRecipe = codecOnly ? "native-codec-only" : "tinkle-native-media-v2"
        precondition(encoded.recipe() == expectedRecipe)
        print("fixture: Rust artifact recipe", encoded.recipe())
        precondition(FileManager.default.fileExists(atPath: encoded.path()))

        // A static alpha image goes through real ImageIO PNG encoding, not an implicit document path.
        let png = directory.appendingPathComponent("source.png")
        print("fixture: alpha image normalization")
        let image = CIImage(color: CIColor(red: 0.2, green: 0.4, blue: 0.6, alpha: 0.5)).cropped(to: CGRect(x: 0, y: 0, width: 32, height: 16))
        try CIContext().writePNGRepresentation(of: image, to: png, format: .RGBA8, colorSpace: CGColorSpace(name: CGColorSpace.sRGB)!)
        let preparedImage = try await prepare(path: png.path, kind: .image, preparer: preparer)
        precondition(preparedImage.facts().width == 32 && preparedImage.facts().height == 16)
        if let path = ProcessInfo.processInfo.environment["TINKLE_MEDIA_100MP_FIXTURE_PATH"] {
            print("fixture: 100-Mpixel ImageIO downsampling")
            var before = rusage()
            getrusage(RUSAGE_SELF, &before)
            let large = try await prepare(path: path, kind: .image, preparer: preparer)
            let facts = large.facts()
            precondition(facts.width == facts.height && UInt64(facts.width) * UInt64(facts.height) <= 16 * 1024 * 1024)
            let size = try URL(fileURLWithPath: large.path()).resourceValues(forKeys: [.fileSizeKey]).fileSize!
            precondition(size > 0 && size <= 10_000_000)
            print("fixture: 100-Mpixel output", facts.width, facts.height, size)
            var after = rusage()
            getrusage(RUSAGE_SELF, &after)
            print("fixture: Darwin peak resident bytes before/after 100MP", before.ru_maxrss, after.ru_maxrss)
        }
    }

    /// Real decoded waveform integrity: AAC must retain the fixture's energy and 440-Hz tone.
    private static func checkSine(_ url: URL) throws {
        // AVFoundation needs a container suffix for the SDK's extensionless standalone tempfile.
        // Read the same inode; never transcode or substitute fixture bytes during verification.
        let alias = url.deletingLastPathComponent().appendingPathComponent("verify-\(UUID().uuidString).mp4")
        try FileManager.default.linkItem(at: url, to: alias)
        defer { try? FileManager.default.removeItem(at: alias) }
        let asset = AVURLAsset(url: alias)
        defer { asset.cancelLoading() }
        let reader = try AVAssetReader(asset: asset)
        defer { reader.cancelReading() }
        let output = AVAssetReaderTrackOutput(track: asset.tracks(withMediaType: .audio)[0], outputSettings: [
            AVFormatIDKey: kAudioFormatLinearPCM, AVLinearPCMBitDepthKey: 16,
            AVLinearPCMIsFloatKey: false, AVLinearPCMIsNonInterleaved: false])
        reader.add(output)
        precondition(reader.startReading())
        var count = 0, crossings = 0, previous: Int16 = 0
        var squares: Int64 = 0
        while let sample = output.copyNextSampleBuffer() {
            guard let block = CMSampleBufferGetDataBuffer(sample) else { throw FfiMediaError.Codec }
            let length = CMBlockBufferGetDataLength(block)
            var bytes = [Int16](repeating: 0, count: min(length / 2, 1024))
            for offset in Swift.stride(from: 0, to: length, by: 2048) {
                let size = min(2048, length - offset)
                let status = bytes.withUnsafeMutableBytes { CMBlockBufferCopyDataBytes(block, atOffset: offset, dataLength: size, destination: $0.baseAddress!) }
                precondition(status == kCMBlockBufferNoErr && size % 2 == 0)
                for value in bytes.prefix(size / 2) {
                    let value = Int16(littleEndian: value)
                    squares += Int64(value) * Int64(value)
                    if (value < 0) != (previous < 0) { crossings += 1 }
                    previous = value; count += 1
                }
            }
        }
        precondition(reader.status == .completed && count >= 19_200 && count <= 21_248)
        let rms = sqrt(Double(squares) / Double(count))
        let frequency = Double(crossings) * 48000 / (2 * Double(count))
        precondition((8000...14000).contains(rms) && (420...460).contains(frequency))
        print("fixture: decoded AAC tone", count, rms, frequency)
    }

    /// Four 4K source frames with a portrait display transform; real platform H.264 source bytes.
    private static func video(_ url: URL) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            DispatchQueue.global(qos: .utility).async {
                do {
                    let writer = try AVAssetWriter(outputURL: url, fileType: .mp4)
                    let input = AVAssetWriterInput(mediaType: .video, outputSettings: [AVVideoCodecKey: AVVideoCodecType.h264,
                        AVVideoWidthKey: 3840, AVVideoHeightKey: 2160, AVVideoCompressionPropertiesKey: [
                            AVVideoProfileLevelKey: AVVideoProfileLevelH264HighAutoLevel, AVVideoAllowFrameReorderingKey: false,
                            AVVideoExpectedSourceFrameRateKey: 10,
                        ]])
                    input.transform = CGAffineTransform(a: 0, b: 1, c: -1, d: 0, tx: 2160, ty: 0)
                    let adaptor = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput: input, sourcePixelBufferAttributes: [
                        kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA,
                        kCVPixelBufferWidthKey as String: 3840, kCVPixelBufferHeightKey as String: 2160,
                    ])
                    writer.add(input)
                    guard writer.startWriting() else { throw FfiMediaError.Codec }
                    writer.startSession(atSourceTime: .zero)
                    let context = CIContext(options: [.cacheIntermediates: false])
                    for index in 0..<4 {
                        while !input.isReadyForMoreMediaData { Thread.sleep(forTimeInterval: 0.002) }
                        var buffer: CVPixelBuffer?
                        guard let pool = adaptor.pixelBufferPool, CVPixelBufferPoolCreatePixelBuffer(nil, pool, &buffer) == kCVReturnSuccess,
                              let buffer else { throw FfiMediaError.Codec }
                        context.render(CIImage(color: CIColor(red: 0.7, green: 0.2, blue: 0.1)).cropped(to: CGRect(x: 0, y: 0, width: 3840, height: 2160)), to: buffer)
                        guard adaptor.append(buffer, withPresentationTime: CMTime(value: Int64(index), timescale: 10)) else { throw FfiMediaError.Codec }
                    }
                    input.markAsFinished(); writer.endSession(atSourceTime: CMTime(value: 4, timescale: 10))
                    writer.finishWriting { continuation.resume(with: writer.status == .completed ? .success(()) : .failure(FfiMediaError.Codec)) }
                } catch { continuation.resume(throwing: error) }
            }
        }
    }
}
