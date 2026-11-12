#if os(iOS) && canImport(AVFoundation)
import AVFoundation
import CoreImage
import ImageIO
import UniformTypeIdentifiers
import Foundation
import CryptoKit

/// Real LOCAL H.264/AAC-LC encoding. No client, network, upload, or publication ownership.
/// Cooperative cancellation retains the Rust lease until readers, writer and finalization join.
public final class AVFoundationMediaPreparer: FfiPlatformMediaPreparer, Sendable {
    public init() {}
    public func prepare(input: FfiNativeMediaInput) async throws -> FfiNativeMediaOutput {
        try await prepareLocal(input: input)
    }

    /// Protocol entry allows native fixtures without fabricated or borrowed Rust handles.
    internal func prepareLocal(input: FfiNativeMediaInputProtocol) async throws -> FfiNativeMediaOutput {
        guard await VideoWorkerAdmission.shared.acquire() else { throw FfiMediaError.Busy }
        let progress = Progress(totalUnitCount: 1)
        let deadline = ProcessInfo.processInfo.systemUptime + Double(input.deadlineMs()) / 1000
        do {
            let result: FfiNativeMediaOutput = try await withTaskCancellationHandler(operation: {
                if Task.isCancelled { progress.cancel() }
                return try await withCheckedThrowingContinuation { continuation in
                    DispatchQueue.global(qos: .utility).async {
                        let result = withExtendedLifetime(input) {
                            Result { try autoreleasepool {
                                try NativeMediaWriter.run(input, progress, deadline)
                            } }
                        }
                        continuation.resume(with: result)
                    }
                }
            }, onCancel: { progress.cancel() })
            await VideoWorkerAdmission.shared.release()
            return result
        } catch {
            await VideoWorkerAdmission.shared.release()
            throw error
        }
    }
}

/// Thread-confined reader/writer implementation; all paths originate in the retained SDK lease.
private enum NativeMediaWriter {
    static func run(_ input: FfiNativeMediaInputProtocol, _ progress: Progress, _ deadline: Double) throws -> FfiNativeMediaOutput {
        let check = {
            if progress.isCancelled || input.isCancelled() { throw FfiMediaError.Cancelled }
            if ProcessInfo.processInfo.systemUptime >= deadline { throw FfiMediaError.Budget }
        }
        try check()
        let source = URL(fileURLWithPath: input.sourcePath())
        let destination = URL(fileURLWithPath: input.outputPath())
        let files = FileManager.default
        guard source.path.hasPrefix("/"), destination.path.hasPrefix("/"), source != destination,
              !files.fileExists(atPath: destination.path) else { throw FfiMediaError.Invalid }
        let stage = destination.deletingLastPathComponent().appendingPathComponent("encoding.mp4")
        defer { try? files.removeItem(at: stage) }
        let result: FfiNativeMediaOutput
        switch input.kind() {
        case .image: result = try image(source, stage, input, check)
        case .voice(let sampleRate, let channels, let bitsPerSample):
            guard bitsPerSample == 16, (1...2).contains(channels), (8000...48000).contains(sampleRate),
                  input.sizeBytes() > 0, input.sizeBytes() % (UInt64(channels) * 2) == 0,
                  input.sizeBytes() <= UInt64(UInt32.max) - 36 else { throw FfiMediaError.Unsupported }
            // The bounded PCM wrapper is local only and deleted after native teardown.
            let wave = destination.deletingLastPathComponent().appendingPathComponent("voice.wav")
            defer { try? files.removeItem(at: wave) }
            try wrapPCM(source, wave, sampleRate, channels, UInt32(input.sizeBytes()), check)
            result = try movie(wave, stage, input, false, check)
        case .audio: result = try movie(source, stage, input, false, check)
        case .video: result = try movie(source, stage, input, true, check)
        }
        try check()
        try files.moveItem(at: stage, to: destination)
        return result
    }

    /// ImageIO downsamples before raster allocation, including 100-Mpixel imports. Rust handles
    /// animation before invoking this static adapter. Two bounded rasters may coexist on verification.
    private static func image(_ source: URL, _ destination: URL, _ input: FfiNativeMediaInputProtocol,
                              _ check: () throws -> Void) throws -> FfiNativeMediaOutput {
        guard let source = CGImageSourceCreateWithURL(source as CFURL, [kCGImageSourceShouldCache: false] as CFDictionary),
              CGImageSourceGetCount(source) == 1,
              let properties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any],
               let w = properties[kCGImagePropertyPixelWidth] as? UInt32,
               let h = properties[kCGImagePropertyPixelHeight] as? UInt32, w > 0, h > 0 else { throw FfiMediaError.Unsupported }
        guard max(w, h) <= 100_000, UInt64(w) * UInt64(h) <= 100_000_000 else { throw FfiMediaError.Budget }
        let orientation = properties[kCGImagePropertyOrientation] as? Int ?? 1
        let swap = (5...8).contains(orientation)
        let width = swap ? h : w, height = swap ? w : h
        for attempt: UInt32 in 0...64 {
            try check()
            let size = try mediaImageDimensions(width: width, height: height, attempt: attempt)
            if UInt64(size.width) * UInt64(size.height) > 16 * 1024 * 1024 { continue }
            let options: [CFString: Any] = [
                kCGImageSourceCreateThumbnailFromImageAlways: true,
                kCGImageSourceCreateThumbnailWithTransform: true,
                kCGImageSourceThumbnailMaxPixelSize: max(size.width, size.height),
                kCGImageSourceShouldCacheImmediately: true,
            ]
            let length: Int = try autoreleasepool {
                guard let image = CGImageSourceCreateThumbnailAtIndex(source, 0, options as CFDictionary),
                      image.width == Int(size.width), image.height == Int(size.height),
                      image.bitsPerComponent == 8 else { throw FfiMediaError.Unsupported }
                let stage = destination.deletingLastPathComponent().appendingPathComponent("image.png")
                defer { try? FileManager.default.removeItem(at: stage) }
                let sink = try PngFileSink(stage, input.maxOutputBytes())
                defer { try? sink.file.close() }
                var callbacks = CGDataConsumerCallbacks(putBytes: { info, bytes, count in
                    guard let info else { return 0 }
                    return Unmanaged<PngFileSink>.fromOpaque(info).takeUnretainedValue().write(bytes, count)
                }, releaseConsumer: nil)
                guard let consumer = CGDataConsumer(info: Unmanaged.passUnretained(sink).toOpaque(), cbks: &callbacks),
                      let writer = CGImageDestinationCreateWithDataConsumer(consumer, UTType.png.identifier as CFString, 1, nil)
                else { throw FfiMediaError.Codec }
                CGImageDestinationAddImage(writer, image, nil)
                let finalized = CGImageDestinationFinalize(writer)
                if sink.exceeded { return Int(input.maxOutputBytes()) + 1 }
                guard finalized, !sink.failed else { throw FfiMediaError.Codec }
                try check()
                let length = try stage.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0
                if length > 0, UInt64(length) <= input.maxOutputBytes() {
                    // Decode the exact output before returning; PNG is lossless on these final pixels.
                    guard let verify = CGImageSourceCreateWithURL(stage as CFURL, nil),
                          let decoded = CGImageSourceCreateImageAtIndex(verify, 0, [kCGImageSourceShouldCacheImmediately: true] as CFDictionary),
                           decoded.width == image.width, decoded.height == image.height,
                           try pixelDigest(decoded, check) == pixelDigest(image, check) else { throw FfiMediaError.Codec }
                    try FileManager.default.moveItem(at: stage, to: destination)
                }
                return length
            }
            if length > 0, UInt64(length) <= input.maxOutputBytes() {
                return FfiNativeMediaOutput(width: size.width, height: size.height, frameCount: 1,
                    durationMs: 0, audioChannels: 0, audioSampleRate: 0, audioSamples: 0)
            }
        }
        throw FfiMediaError.Budget
    }

    /// Compare rendered RGBA including alpha in 32-row strips, not a third full-raster Data copy.
    /// Premultiplication is explicit; the PNG encoder must preserve these final display pixels.
    private static func pixelDigest(_ image: CGImage, _ check: () throws -> Void) throws -> SHA256.Digest {
        let rows = min(32, image.height), stride = image.width * 4
        guard let context = CGContext(data: nil, width: image.width, height: rows, bitsPerComponent: 8,
            bytesPerRow: stride, space: CGColorSpace(name: CGColorSpace.sRGB)!,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue), let data = context.data else { throw FfiMediaError.Codec }
        context.setBlendMode(.copy)
        var digest = SHA256()
        for y in Swift.stride(from: 0, to: image.height, by: rows) {
            try check()
            let count = min(rows, image.height - y)
            context.clear(CGRect(x: 0, y: 0, width: image.width, height: rows))
            context.draw(image, in: CGRect(x: 0, y: -y, width: image.width, height: image.height))
            digest.update(bufferPointer: UnsafeRawBufferPointer(start: data, count: stride * count))
        }
        return digest.finalize()
    }

    /// ImageIO output is bounded during writes; a failed attempt never fills an unbounded Data.
    private final class PngFileSink {
        let file: FileHandle
        let maximum: UInt64
        var written: UInt64 = 0
        var exceeded = false
        var failed = false
        init(_ url: URL, _ maximum: UInt64) throws {
            guard FileManager.default.createFile(atPath: url.path, contents: nil) else { throw FfiMediaError.Io }
            file = try FileHandle(forWritingTo: url); self.maximum = maximum
        }
        func write(_ bytes: UnsafeRawPointer, _ count: Int) -> Int {
            guard !failed, !exceeded, UInt64(count) <= maximum - written else { exceeded = true; return 0 }
            do { try file.write(contentsOf: Data(bytesNoCopy: UnsafeMutableRawPointer(mutating: bytes), count: count, deallocator: .none)) }
            catch { failed = true; return 0 }
            written += UInt64(count)
            return count
        }
    }

    /// Preserve every source presentation timestamp; only the pixel dimensions/codec change.
    private static func movie(_ source: URL, _ destination: URL, _ input: FfiNativeMediaInputProtocol,
                              _ wantsVideo: Bool, _ check: () throws -> Void) throws -> FfiNativeMediaOutput {
        // AVFoundation needs a suffix for extensionless SDK snapshots. An alias retains the frozen inode.
        let alias = destination.deletingLastPathComponent().appendingPathComponent("input.\(source.pathExtension.isEmpty ? "mp4" : source.pathExtension)")
        try FileManager.default.linkItem(at: source, to: alias)
        defer { try? FileManager.default.removeItem(at: alias) }
        let asset = AVURLAsset(url: alias, options: [AVURLAssetPreferPreciseDurationAndTimingKey: true,
            AVURLAssetReferenceRestrictionsKey: AVAssetReferenceRestrictions.forbidAll.rawValue])
        defer { asset.cancelLoading() }
        try check()
        guard !asset.hasProtectedContent, asset.isReadable else { throw FfiMediaError.Unsupported }
        let video = asset.tracks(withMediaType: .video)
        let audio = asset.tracks(withMediaType: .audio)
        guard video.count == (wantsVideo ? 1 : 0), audio.count <= 1,
              wantsVideo || audio.count == 1, asset.tracks.count == video.count + audio.count else { throw FfiMediaError.Unsupported }
        let duration = CMTimeGetSeconds(asset.duration)
        guard duration.isFinite, duration > 0, duration * 1000 <= Double(input.maxDurationMs()) else { throw FfiMediaError.Budget }
        let reader = try AVAssetReader(asset: asset)
        let writer = try AVAssetWriter(outputURL: destination, fileType: .mp4)
        defer { reader.cancelReading(); if writer.status == .writing { writer.cancelWriting() } }
        var videoOutput: AVAssetReaderTrackOutput?
        var videoInput: AVAssetWriterInput?
        var adaptor: AVAssetWriterInputPixelBufferAdaptor?
        var size = FfiMediaDimensions(width: 0, height: 0)
        var display = size
        var pts: [CMTime] = []
        if let track = video.first {
            guard !track.hasMediaCharacteristic(.containsHDRVideo), !track.hasMediaCharacteristic(.containsAlphaChannel),
                  !track.formatDescriptions.isEmpty, track.formatDescriptions.count <= 16 else { throw FfiMediaError.Unsupported }
            let limits = FfiVideoDecodeLimits(maxInputBytes: 256 * 1024 * 1024, maxPixels: 16 * 1024 * 1024,
                maxFrames: input.maxFrames(), maxDurationMs: input.maxDurationMs(), thumbnailEdge: 0, deadlineMs: input.deadlineMs())
            let formats = try track.formatDescriptions.map { try VideoReader.inspect($0 as! CMVideoFormatDescription, limits) }
            guard let format = formats.first, formats.allSatisfy({ $0.width == format.width && $0.height == format.height && $0.depth == 8 })
            else { throw FfiMediaError.Unsupported }
            let orientation = try VideoReader.orientation(track.preferredTransform)
            size = try mediaVideoDimensions(width: UInt32(format.width), height: UInt32(format.height))
            display = [5,6,7,8].contains(orientation) ? FfiMediaDimensions(width: size.height, height: size.width) : size
            let output = AVAssetReaderTrackOutput(track: track, outputSettings: [
                kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange])
            output.alwaysCopiesSampleData = false
            let settings: [String: Any] = [AVVideoCodecKey: AVVideoCodecType.h264, AVVideoWidthKey: size.width,
                AVVideoHeightKey: size.height, AVVideoCompressionPropertiesKey: [
                    AVVideoProfileLevelKey: AVVideoProfileLevelH264HighAutoLevel,
                    AVVideoAllowFrameReorderingKey: false, AVVideoAverageBitRateKey: 8_000_000,
                ]]
            let sink = AVAssetWriterInput(mediaType: .video, outputSettings: settings)
            sink.expectsMediaDataInRealTime = false
            let t = track.preferredTransform
            var transform = CGAffineTransform(a: t.a, b: t.b, c: t.c, d: t.d, tx: 0, ty: 0)
            let bounds = CGRect(x: 0, y: 0, width: Int(size.width), height: Int(size.height)).applying(transform)
            transform.tx = -bounds.minX; transform.ty = -bounds.minY
            sink.transform = transform
            guard reader.canAdd(output), writer.canAdd(sink) else { throw FfiMediaError.Unsupported }
            reader.add(output); writer.add(sink)
            videoOutput = output; videoInput = sink
            adaptor = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput: sink, sourcePixelBufferAttributes: [
                kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange,
                kCVPixelBufferWidthKey as String: size.width, kCVPixelBufferHeightKey as String: size.height,
                kCVPixelBufferIOSurfacePropertiesKey as String: [:],
            ])
        }
        var audioOutput: AVAssetReaderTrackOutput?
        var audioInput: AVAssetWriterInput?
        var channels: UInt8 = 0, rate: UInt32 = 0, samples: UInt64 = 0
        if let track = audio.first {
            guard track.formatDescriptions.count == 1,
                  let description = track.formatDescriptions.first,
                  let asbd = CMAudioFormatDescriptionGetStreamBasicDescription(description as! CMAudioFormatDescription)?.pointee,
                  (1...2).contains(asbd.mChannelsPerFrame), (8000...48000).contains(asbd.mSampleRate) else { throw FfiMediaError.Unsupported }
            channels = UInt8(asbd.mChannelsPerFrame); rate = UInt32(asbd.mSampleRate)
            let output = AVAssetReaderTrackOutput(track: track, outputSettings: [AVFormatIDKey: kAudioFormatLinearPCM,
                AVLinearPCMBitDepthKey: 16, AVLinearPCMIsFloatKey: false, AVLinearPCMIsNonInterleaved: false])
            output.alwaysCopiesSampleData = false
            let sink = AVAssetWriterInput(mediaType: .audio, outputSettings: [AVFormatIDKey: kAudioFormatMPEG4AAC,
                AVEncoderAudioQualityKey: AVAudioQuality.high.rawValue, AVSampleRateKey: rate,
                AVNumberOfChannelsKey: channels, AVEncoderBitRateKey: 128_000])
            sink.expectsMediaDataInRealTime = false
            guard reader.canAdd(output), writer.canAdd(sink) else { throw FfiMediaError.Unsupported }
            reader.add(output); writer.add(sink); audioOutput = output; audioInput = sink
        }
        guard writer.startWriting(), reader.startReading() else { throw FfiMediaError.Codec }
#if TINKLE_MEDIA_STANDALONE
        print("native fixture: reader and writer started")
#endif
        writer.startSession(atSourceTime: .zero)
        let context = CIContext(options: [.cacheIntermediates: false])
        defer { context.clearCaches() }
        var videoDone = videoInput == nil, audioDone = audioInput == nil
        while !videoDone || !audioDone {
            try check(); try quota(destination, input.maxOutputBytes())
            guard writer.status == .writing else { throw FfiMediaError.Codec }
            var advanced = false
            if !videoDone, let sink = videoInput, sink.isReadyForMoreMediaData, let output = videoOutput, let adaptor {
                try autoreleasepool {
                    if let sample = output.copyNextSampleBuffer() {
                        if CMSampleBufferGetNumSamples(sample) == 0 { return }
                        guard pts.count < input.maxFrames(), let source = CMSampleBufferGetImageBuffer(sample),
                              CVPixelBufferGetPixelFormatType(source) == kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange,
                              let pool = adaptor.pixelBufferPool else { throw FfiMediaError.Codec }
                        let time = CMSampleBufferGetPresentationTimeStamp(sample)
                        guard time.isNumeric, time >= .zero, pts.last.map({ time > $0 }) ?? true else { throw FfiMediaError.Unsupported }
                        var buffer: CVPixelBuffer?
                        let attributes = [kCVPixelBufferPoolAllocationThresholdKey: 3] as CFDictionary
                        guard CVPixelBufferPoolCreatePixelBufferWithAuxAttributes(nil, pool, attributes, &buffer) == kCVReturnSuccess,
                              let buffer else { throw FfiMediaError.Budget }
                        let image = CIImage(cvPixelBuffer: source)
                        let resized = image.transformed(by: CGAffineTransform(scaleX: CGFloat(size.width) / image.extent.width,
                            y: CGFloat(size.height) / image.extent.height))
                        context.render(resized, to: buffer)
                        guard adaptor.append(buffer, withPresentationTime: time) else { throw FfiMediaError.Codec }
                        pts.append(time)
                    } else { sink.markAsFinished(); videoDone = true }
                }
                advanced = true
            }
            if !audioDone, let sink = audioInput, sink.isReadyForMoreMediaData, let output = audioOutput {
                try autoreleasepool {
                    if let sample = output.copyNextSampleBuffer() {
                        let count = CMSampleBufferGetNumSamples(sample)
                        if count == 0 { return }
                        guard CMSampleBufferDataIsReady(sample), count > 0,
                              samples + UInt64(count) <= UInt64(rate) * input.maxDurationMs() / 1000,
                              sink.append(sample) else { throw FfiMediaError.Codec }
                        samples += UInt64(count)
                    } else { sink.markAsFinished(); audioDone = true }
                }
                advanced = true
            }
            if !advanced { Thread.sleep(forTimeInterval: 0.002) }
        }
#if TINKLE_MEDIA_STANDALONE
        print("native fixture: input drained", reader.status.rawValue, pts.count, samples)
#endif
        guard reader.status == .completed, !wantsVideo || !pts.isEmpty, channels == 0 || samples > 0 else { throw FfiMediaError.Codec }
        writer.endSession(atSourceTime: asset.duration)
        let finished = DispatchSemaphore(value: 0)
        writer.finishWriting { finished.signal() }
        var cancellationError: Error?
        while finished.wait(timeout: .now() + 0.01) != .success {
            if cancellationError == nil {
                do { try check(); try quota(destination, input.maxOutputBytes()) }
                catch { cancellationError = error; writer.cancelWriting() }
            }
        }
        if let cancellationError { throw cancellationError }
        try check(); try quota(destination, input.maxOutputBytes())
        guard writer.status == .completed else { throw FfiMediaError.Codec }
#if TINKLE_MEDIA_STANDALONE
        print("native fixture: writer completed")
#endif
        try verify(destination, pts, size, display, channels, rate, samples, check)
        return FfiNativeMediaOutput(width: display.width, height: display.height, frameCount: UInt32(pts.count),
            durationMs: UInt64(ceil(duration * 1000)), audioChannels: channels, audioSampleRate: rate, audioSamples: samples)
    }

    /// Decode every output frame and every audio sample. Compare video timestamps, not nominal FPS.
    private static func verify(_ url: URL, _ expected: [CMTime], _ encoded: FfiMediaDimensions,
                               _ display: FfiMediaDimensions, _ channels: UInt8, _ rate: UInt32,
                               _ samples: UInt64, _ check: () throws -> Void) throws {
        let asset = AVURLAsset(url: url, options: [AVURLAssetReferenceRestrictionsKey: AVAssetReferenceRestrictions.forbidAll.rawValue])
        defer { asset.cancelLoading() }
        let reader = try AVAssetReader(asset: asset)
        defer { reader.cancelReading() }
        var outputs: [(AVAssetReaderTrackOutput, Bool)] = []
        for track in asset.tracks {
            let video = track.mediaType == .video
#if TINKLE_MEDIA_STANDALONE
            if !video, let description = track.formatDescriptions.first,
               let asbd = CMAudioFormatDescriptionGetStreamBasicDescription(description as! CMAudioFormatDescription)?.pointee {
                print("native fixture: output audio format", asbd.mFormatID, asbd.mFormatFlags, asbd.mChannelsPerFrame, asbd.mSampleRate)
            }
#endif
            if video {
                guard !expected.isEmpty, track.formatDescriptions.count == 1,
                      let description = track.formatDescriptions.first,
                       CMFormatDescriptionGetMediaSubType(description as! CMFormatDescription) == kCMVideoCodecType_H264 else { throw FfiMediaError.Codec }
                let size = CMVideoFormatDescriptionGetDimensions(description as! CMVideoFormatDescription)
                let orientation = try VideoReader.orientation(track.preferredTransform)
                let swap = (5...8).contains(orientation)
                guard size.width == encoded.width, size.height == encoded.height,
                      UInt32(swap ? size.height : size.width) == display.width,
                      UInt32(swap ? size.width : size.height) == display.height else { throw FfiMediaError.Codec }
            } else {
                guard track.mediaType == .audio, channels > 0, let description = track.formatDescriptions.first,
                      let asbd = CMAudioFormatDescriptionGetStreamBasicDescription(description as! CMAudioFormatDescription)?.pointee,
                      asbd.mFormatID == kAudioFormatMPEG4AAC,
                      [0, UInt32(MPEG4ObjectID.AAC_LC.rawValue)].contains(asbd.mFormatFlags),
                      asbd.mChannelsPerFrame == channels, asbd.mSampleRate == Double(rate) else { throw FfiMediaError.Codec }
            }
            let output = AVAssetReaderTrackOutput(track: track, outputSettings: video
                ? [kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange]
                : [AVFormatIDKey: kAudioFormatLinearPCM, AVLinearPCMBitDepthKey: 16, AVLinearPCMIsFloatKey: false])
            output.alwaysCopiesSampleData = false
            guard reader.canAdd(output) else { throw FfiMediaError.Codec }
            reader.add(output); outputs.append((output, video))
        }
        guard outputs.count == (expected.isEmpty ? 0 : 1) + (channels > 0 ? 1 : 0), reader.startReading() else { throw FfiMediaError.Codec }
        var frames = 0, audioSamples: UInt64 = 0
        for (output, video) in outputs {
            while true {
                try check()
                let found = try autoreleasepool { () throws -> Bool in
                    guard let sample = output.copyNextSampleBuffer() else { return false }
                    let count = CMSampleBufferGetNumSamples(sample)
                    if count == 0 { return true }
                    guard CMSampleBufferDataIsReady(sample) else { throw FfiMediaError.Codec }
                    if video {
                        guard frames < expected.count, let buffer = CMSampleBufferGetImageBuffer(sample),
                               CVPixelBufferGetPixelFormatType(buffer) == kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange,
                               CVPixelBufferGetWidth(buffer) == encoded.width, CVPixelBufferGetHeight(buffer) == encoded.height,
                              abs(CMTimeGetSeconds(CMSampleBufferGetPresentationTimeStamp(sample) - expected[frames])) < 0.0001
                        else { throw FfiMediaError.Codec }
                        frames += 1
                    } else { audioSamples += UInt64(count) }
                    return true
                }
                if !found { break }
            }
        }
        // AAC priming/padding may add up to two access units; no missing content is accepted.
#if TINKLE_MEDIA_STANDALONE
        print("native fixture: output drained", reader.status.rawValue, frames, audioSamples, samples)
#endif
        guard reader.status == .completed, frames == expected.count,
              channels == 0 || (audioSamples >= samples && audioSamples - samples <= 2048) else { throw FfiMediaError.Codec }
    }

    private static func quota(_ url: URL, _ maximum: UInt64) throws {
        if let size = try? url.resourceValues(forKeys: [.fileSizeKey]).fileSize, UInt64(size) > maximum { throw FfiMediaError.Budget }
    }

    /// Streaming PCM16 WAVE wrapper; 64 KiB working buffer and no second full-size memory copy.
    private static func wrapPCM(_ source: URL, _ destination: URL, _ rate: UInt32, _ channels: UInt8,
                                _ size: UInt32, _ check: () throws -> Void) throws {
        var header = Data("RIFF".utf8)
        func word<T: FixedWidthInteger>(_ value: T) { var value = value.littleEndian; withUnsafeBytes(of: &value) { header.append(contentsOf: $0) } }
        word(size + 36); header.append(Data("WAVEfmt ".utf8)); word(UInt32(16)); word(UInt16(1)); word(UInt16(channels))
        word(rate); word(rate * UInt32(channels) * 2); word(UInt16(channels) * 2); word(UInt16(16)); header.append(Data("data".utf8)); word(size)
        guard FileManager.default.createFile(atPath: destination.path, contents: header) else { throw FfiMediaError.Io }
        let reader = try FileHandle(forReadingFrom: source), writer = try FileHandle(forWritingTo: destination)
        defer { try? reader.close(); try? writer.close() }
        try writer.seekToEnd()
        var copied: UInt64 = 0
        while true {
            try check()
            let data = try reader.read(upToCount: 65536) ?? Data()
            if data.isEmpty { break }
            copied += UInt64(data.count)
            guard copied <= size else { throw FfiMediaError.SourceChanged }
            try writer.write(contentsOf: data)
        }
        guard copied == size else { throw FfiMediaError.SourceChanged }
    }
}
#endif
