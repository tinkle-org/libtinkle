// Staging copies the matching canonical bindings/swift/TinkleSDK.swift into
// this directory in a disposable package. Keep that file generated and untracked
// in libtinkle; the published distribution includes its release-matched copy.
#if !os(iOS)
#error("TinkleSDK currently supports iOS devices and iOS simulators only.")
#endif

#if os(iOS) && canImport(AVFoundation)
import AVFoundation
import CoreImage
import Foundation

/// A bounded, local-file AVFoundation validator for the SDK's frozen MP4 read lease.
///
/// Install explicitly with `client.configureAVFoundationVideoDecoder()`. This does not
/// upload, enqueue, remux or encode anything: Rust retains the exact encoded source.
/// Supports one H.264 Baseline/Main/High (8-bit) or HEVC Main/Main10 video track and
/// optional AAC-LC audio. Other tracks, HDR, alpha, non-square pixels, and unsupported
/// profiles fail with `FfiMediaError.Unsupported`; callers may explicitly choose the
/// document preparation path instead. There is no automatic document/upload fallback.
///
/// Every video frame must decode before `.nativeDecoded` is returned. Frame counts
/// are measured by decoding; duration uses the complete compressed-sample timeline,
/// cross-checked against decoded frames, never nominal FPS or an asset-duration estimate.
/// The optional first-frame thumbnail is display-oriented sRGB RGBA8, not a full-source
/// representation. Unknown color metadata or >8-bit source precision omits it.
///
/// Native calls run on a utility dispatch queue, not MainActor or a Swift executor.
/// An extensionless Rust snapshot gets a private `.mp4` hard-link read alias (same
/// frozen inode, no copy/proxy); only that alias is removed after native teardown.
/// Cancellation/deadlines are cooperative between native calls, not hard preemption of
/// a codec call. The invocation retains its lease and worker admission until that call
/// returns and reader teardown finishes, even if Rust has already timed out/cancelled.
/// Additional conservative ceilings are 256 MiB input, 4096 per encoded dimension,
/// 4096×2160 pixels, 18,000 frames, 10 minutes, 320 thumbnail edge, and 10 seconds.
public final class AVFoundationVideoDecoder: FfiPlatformVideoDecoder, Sendable {
    /// Creates an adapter without starting work or retaining a client.
    public init() {}

    /// Validates the complete frozen source within the supplied per-invocation budgets.
    public func inspectAndDecode(input: FfiVideoInput, limits: FfiVideoDecodeLimits) async throws -> FfiDecodedVideo {
        try await decode(input: input, limits: limits)
    }

    /// Shared by the real callback and focused file fixtures; no FFI handles are fabricated.
    internal func decode(input: FfiVideoInputProtocol, limits: FfiVideoDecodeLimits) async throws -> FfiDecodedVideo {
        let deadline = ProcessInfo.processInfo.systemUptime + Double(min(limits.deadlineMs, 10000)) / 1000
        guard await VideoWorkerAdmission.shared.acquire() else { throw FfiMediaError.Busy }
        let cancellation = Progress(totalUnitCount: 1)
        do {
            let result: FfiDecodedVideo = try await withTaskCancellationHandler(operation: {
                if Task.isCancelled { cancellation.cancel() }
                return try await withCheckedThrowingContinuation { continuation in
                    DispatchQueue.global(qos: .utility).async {
                        // The closure, not the suspended Swift/Rust waiter, owns this lease.
                        let result = withExtendedLifetime(input) {
                            Result { try autoreleasepool {
                                try VideoReader.read(input: input, limits: limits,
                                    cancellation: cancellation, deadline: deadline)
                            } }
                        }
                        continuation.resume(with: result)
                    }
                }
            }, onCancel: { cancellation.cancel() })
            await VideoWorkerAdmission.shared.release()
            return result
        } catch {
            await VideoWorkerAdmission.shared.release()
            throw error
        }
    }
}

extension TinkleClient {
    /// Install the real encoder for normal `sendingPrepareAttachment` image/video/audio/voice
    /// calls. Documents remain raw. Retained preparations resume without invoking the encoder.
    public func configureAVFoundationMediaPreparer() throws {
        try configureNativeMediaPreparer(preparer: AVFoundationMediaPreparer())
    }

    /// Registers the real iOS decoder only; preparation and sending remain explicit.
    /// Existing work retains its selected adapter if `clearVideoDecoder()` is called.
    public func configureAVFoundationVideoDecoder() throws {
        try configureVideoDecoder(decoder: AVFoundationVideoDecoder())
    }
}

/// Admission covers actual native completion, including waiters cancelled by UniFFI.
internal actor VideoWorkerAdmission {
    static let shared = VideoWorkerAdmission()
    private var active = 0
    func acquire() -> Bool {
        guard active < 2 else { return false }
        active += 1
        return true
    }
    func release() { active -= 1 }
}

/// All AVFoundation objects are created, used and destroyed inside one worker closure.
internal enum VideoReader {
    /// Precision/color qualification is separate from requesting decoded output pixels.
    internal struct Format {
        let width: Int32
        let height: Int32
        let depth: UInt8
        let thumbnailSafe: Bool
    }

    /// Exact per-sample container timing, cross-checked against every decoded frame.
    private struct Timing {
        let count: UInt32
        let first: CMTime
        let last: CMTime
        let durationMs: UInt64
    }

    /// Stop checks never release a lease out from under a native call.
    private static func check(_ input: FfiVideoInputProtocol, _ progress: Progress, _ deadline: Double) throws {
        if progress.isCancelled || input.isCancelled() { throw FfiMediaError.Cancelled }
        if ProcessInfo.processInfo.systemUptime >= deadline { throw FfiMediaError.Budget }
    }

    /// Inspect a bounded MP4 header before opening AVFoundation; no URL/proxy input exists.
    private static func localURL(_ input: FfiVideoInputProtocol, _ limits: FfiVideoDecodeLimits) throws -> URL {
        guard input.sizeBytes() > 0, input.sizeBytes() <= min(limits.maxInputBytes, 256 * 1024 * 1024),
              limits.maxPixels > 0, limits.maxFrames > 0, limits.maxDurationMs > 0,
              input.sourceSha256().count == 32 else { throw FfiMediaError.Budget }
        let path = input.sourcePath()
        guard path.hasPrefix("/") else { throw FfiMediaError.Invalid }
        let url = URL(fileURLWithPath: path)
        do {
            let values = try url.resourceValues(forKeys: [.isRegularFileKey, .isSymbolicLinkKey, .fileSizeKey])
            guard values.isRegularFile == true, values.isSymbolicLink != true,
                  values.fileSize.map(UInt64.init) == input.sizeBytes() else { throw FfiMediaError.SourceChanged }
            let file = try FileHandle(forReadingFrom: url)
            defer { try? file.close() }
            let header = try file.read(upToCount: 12) ?? Data()
            guard header.count == 12, header[4..<8] == Data("ftyp".utf8),
                  ["isom", "iso2", "mp41", "mp42", "avc1"].contains(String(decoding: header[8..<12], as: UTF8.self))
            else { throw FfiMediaError.Unsupported }
            return url
        } catch let error as FfiMediaError { throw error }
        catch { throw FfiMediaError.Io }
    }

    /// Synchronous legacy property loading is intentional on the native queue (iOS 15).
    /// Forbid ALL external media references, including local aliases and remote URLs.
    static func read(input: FfiVideoInputProtocol, limits: FfiVideoDecodeLimits,
                     cancellation: Progress, deadline: Double) throws -> FfiDecodedVideo {
        try check(input, cancellation, deadline)
        let source = try localURL(input, limits)
        try check(input, cancellation, deadline)
        // iOS 15 has no public MIME override. AVFoundation needs a container suffix
        // even for a valid extensionless Rust snapshot. Keep the same frozen inode.
        let directory = source.deletingLastPathComponent()
            .appendingPathComponent(".tinkle-video-read-\(UUID().uuidString)", isDirectory: true)
        let alias = directory.appendingPathComponent("source.mp4")
        let files = FileManager()
        do {
            try files.createDirectory(at: directory, withIntermediateDirectories: false,
                attributes: [.posixPermissions: 0o700])
        } catch { throw FfiMediaError.Io }
        defer { try? files.removeItem(at: directory) }
        do { try files.linkItem(at: source, to: alias) }
        catch { throw FfiMediaError.Io }
        let asset = AVURLAsset(url: alias, options: [
            AVURLAssetPreferPreciseDurationAndTimingKey: true,
            AVURLAssetReferenceRestrictionsKey: AVAssetReferenceRestrictions.forbidAll.rawValue,
        ])
        defer { asset.cancelLoading() }
        guard !asset.hasProtectedContent, asset.isPlayable else { throw FfiMediaError.Unsupported }
        let tracks = asset.tracks
        guard !tracks.isEmpty, tracks.count <= 2 else { throw FfiMediaError.Unsupported }
        var video: AVAssetTrack?
        for track in tracks {
            try check(input, cancellation, deadline)
            guard track.isPlayable, !track.formatDescriptions.isEmpty,
                  track.formatDescriptions.count <= 16 else { throw FfiMediaError.Unsupported }
            if track.mediaType == .video {
                guard video == nil, !track.hasMediaCharacteristic(.containsHDRVideo),
                      !track.hasMediaCharacteristic(.containsAlphaChannel) else { throw FfiMediaError.Unsupported }
                video = track
            } else if track.mediaType == .audio {
                // Preserve audio packets; do not pretend an arbitrary audio codec is playable.
                for description in track.formatDescriptions {
                    let format = description as! CMAudioFormatDescription
                    guard let audio = CMAudioFormatDescriptionGetStreamBasicDescription(format)?.pointee,
                          audio.mFormatID == kAudioFormatMPEG4AAC,
                          audio.mFormatFlags == UInt32(MPEG4ObjectID.AAC_LC.rawValue), (1...2).contains(audio.mChannelsPerFrame),
                          audio.mSampleRate > 0, audio.mSampleRate <= 48000
                    else { throw FfiMediaError.Unsupported }
                }
            } else { throw FfiMediaError.Unsupported }
        }
        guard let track = video else { throw FfiMediaError.Unsupported }
        let formats = try track.formatDescriptions.map { try inspect($0 as! CMVideoFormatDescription, limits) }
        guard let format = formats.first, formats.allSatisfy({
            $0.width == format.width && $0.height == format.height && $0.depth == format.depth
        }) else { throw FfiMediaError.Unsupported }
        let orientation = try orientation(track.preferredTransform)
        let swap = [5, 6, 7, 8].contains(orientation)
        let width = UInt32(swap ? format.height : format.width)
        let height = UInt32(swap ? format.width : format.height)
        // Metadata duration can reject an over-budget input, but cannot qualify success.
        let estimate = CMTimeGetSeconds(asset.duration)
        guard estimate.isFinite, estimate > 0 else { throw FfiMediaError.Invalid }
        guard estimate * 1000 <= Double(min(limits.maxDurationMs, 600000)) else { throw FfiMediaError.Budget }
        try check(input, cancellation, deadline)
        let timing = try sampleTiming(asset: asset, track: track, input: input, limits: limits,
            cancellation: cancellation, deadline: deadline)
        let reader: AVAssetReader
        do { reader = try AVAssetReader(asset: asset) } catch { throw FfiMediaError.Codec }
        defer { reader.cancelReading() }
        let output = AVAssetReaderTrackOutput(track: track, outputSettings: [
            kCVPixelBufferPixelFormatTypeKey as String: format.depth == 8
                ? kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange : kCVPixelFormatType_420YpCbCr10BiPlanarVideoRange,
        ])
        output.alwaysCopiesSampleData = false
        guard reader.canAdd(output) else { throw FfiMediaError.Unsupported }
        reader.add(output)
        guard reader.startReading() else { throw FfiMediaError.Codec }
        var count: UInt32 = 0
        var buffers: UInt32 = 0
        var first: CMTime?
        var previous: CMTime?
        var thumbnail: FfiDecodedVideoFrame?
        while true {
            try check(input, cancellation, deadline)
            let found: Bool = try autoreleasepool {
                guard let sample = output.copyNextSampleBuffer() else { return false }
                try check(input, cancellation, deadline)
                buffers += 1
                guard buffers <= min(limits.maxFrames, 18000) * 2 + 64 else { throw FfiMediaError.Budget }
                // Reader control/drain markers contain no media samples or payload.
                if CMSampleBufferGetNumSamples(sample) == 0 && CMSampleBufferGetTotalSampleSize(sample) == 0 { return true }
                guard count < min(limits.maxFrames, 18000) else { throw FfiMediaError.Budget }
                guard CMSampleBufferDataIsReady(sample), CMSampleBufferGetNumSamples(sample) == 1,
                      let buffer = CMSampleBufferGetImageBuffer(sample),
                      CVPixelBufferGetWidth(buffer) == Int(format.width),
                      CVPixelBufferGetHeight(buffer) == Int(format.height),
                      CVPixelBufferGetPixelFormatType(buffer) == (format.depth == 8
                        ? kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange : kCVPixelFormatType_420YpCbCr10BiPlanarVideoRange)
                else { throw FfiMediaError.Codec }
                let pts = CMSampleBufferGetPresentationTimeStamp(sample)
                guard pts.isNumeric, pts >= .zero,
                      previous.map({ pts > $0 }) ?? true else { throw FfiMediaError.Unsupported }
                if first == nil { first = pts }
                guard pts >= timing.first, pts <= timing.last else { throw FfiMediaError.Codec }
                if count == 0 && formats.allSatisfy(\.thumbnailSafe) && limits.thumbnailEdge > 0 {
                    thumbnail = try makeThumbnail(buffer, orientation: orientation,
                        width: width, height: height, edge: min(limits.thumbnailEdge, 320))
                }
                count += 1
                previous = pts
                return true
            }
            if !found { break }
        }
        try check(input, cancellation, deadline)
        guard reader.status == .completed, count == timing.count,
              first == timing.first, previous == timing.last else { throw FfiMediaError.Codec }
        return FfiDecodedVideo(width: width, height: height, durationMs: timing.durationMs,
            frameCount: count, hasAlpha: false, bitDepth: format.depth,
            evidence: .nativeDecoded, thumbnail: thumbnail)
    }

    /// AVAssetReader may discard durations when decompressing. Read the exact compressed
    /// sample timeline (including the final duration) rather than inventing one from FPS.
    /// This pass alone NEVER qualifies nativeDecoded; the decode pass must finish and match.
    private static func sampleTiming(asset: AVAsset, track: AVAssetTrack,
        input: FfiVideoInputProtocol, limits: FfiVideoDecodeLimits,
        cancellation: Progress, deadline: Double) throws -> Timing {
        let reader: AVAssetReader
        do { reader = try AVAssetReader(asset: asset) } catch { throw FfiMediaError.Codec }
        defer { reader.cancelReading() }
        let output = AVAssetReaderTrackOutput(track: track, outputSettings: nil)
        output.alwaysCopiesSampleData = false
        guard reader.canAdd(output) else { throw FfiMediaError.Unsupported }
        reader.add(output)
        guard reader.startReading() else { throw FfiMediaError.Codec }
        var count: UInt32 = 0
        var buffers: UInt32 = 0
        var first: CMTime?
        var last = CMTime.zero
        var end = CMTime.zero
        while true {
            try check(input, cancellation, deadline)
            let found: Bool = try autoreleasepool {
                guard let sample = output.copyNextSampleBuffer() else { return false }
                try check(input, cancellation, deadline)
                buffers += 1
                guard buffers <= min(limits.maxFrames, 18000) * 2 + 64 else { throw FfiMediaError.Budget }
                // AVFoundation emits zero-sample decoder reset/drain markers around MP4 media.
                if CMSampleBufferGetNumSamples(sample) == 0 && CMSampleBufferGetTotalSampleSize(sample) == 0 { return true }
                guard count < min(limits.maxFrames, 18000) else { throw FfiMediaError.Budget }
                guard CMSampleBufferDataIsReady(sample), CMSampleBufferGetNumSamples(sample) == 1 else { throw FfiMediaError.Codec }
                let pts = CMSampleBufferGetPresentationTimeStamp(sample)
                let duration = CMSampleBufferGetDuration(sample)
                guard pts.isNumeric, pts >= .zero, duration.isNumeric, duration > .zero else { throw FfiMediaError.Unsupported }
                first = first.map { CMTimeMinimum($0, pts) } ?? pts
                last = CMTimeMaximum(last, pts)
                end = CMTimeMaximum(end, CMTimeAdd(pts, duration))
                let elapsed = CMTimeGetSeconds(CMTimeSubtract(end, first!)) * 1000
                guard elapsed.isFinite, elapsed <= Double(min(limits.maxDurationMs, 600000)) else { throw FfiMediaError.Budget }
                count += 1
                return true
            }
            if !found { break }
        }
        try check(input, cancellation, deadline)
        guard reader.status == .completed, count > 0, let first else { throw FfiMediaError.Codec }
        let duration = CMTimeConvertScale(CMTimeSubtract(end, first), timescale: 1000, method: .roundAwayFromZero)
        guard duration.isNumeric, duration.value > 0,
              UInt64(duration.value) <= min(limits.maxDurationMs, 600000) else { throw FfiMediaError.Budget }
        return Timing(count: count, first: first, last: last, durationMs: UInt64(duration.value))
    }

    /// Restrict the codec profile before allocating decoded surfaces. Never infer source
    /// precision from an 8-bit output request; unsupported high-depth profiles fail closed.
    internal static func inspect(_ description: CMVideoFormatDescription, _ limits: FfiVideoDecodeLimits) throws -> Format {
        let dimensions = CMVideoFormatDescriptionGetDimensions(description)
        guard dimensions.width > 0, dimensions.height > 0 else { throw FfiMediaError.Invalid }
        guard UInt64(dimensions.width) * UInt64(dimensions.height) <= min(limits.maxPixels, 4096 * 2160),
              dimensions.width <= 4096, dimensions.height <= 4096 else { throw FfiMediaError.Budget }
        guard let extensions = CMFormatDescriptionGetExtensions(description) else { throw FfiMediaError.Unsupported }
        let ext = extensions as NSDictionary
        guard (ext[kCMFormatDescriptionExtension_ContainsAlphaChannel] as? Bool) != true else { throw FfiMediaError.Unsupported }
        if let aspect = ext[kCMFormatDescriptionExtension_PixelAspectRatio] as? NSDictionary {
            guard let h = aspect[kCMFormatDescriptionKey_PixelAspectRatioHorizontalSpacing] as? NSNumber,
                  let v = aspect[kCMFormatDescriptionKey_PixelAspectRatioVerticalSpacing] as? NSNumber,
                  h == v else { throw FfiMediaError.Unsupported }
        }
        let aperture = CMVideoFormatDescriptionGetCleanAperture(description, originIsAtTopLeft: true)
        guard aperture == CGRect(x: 0, y: 0, width: Int(dimensions.width), height: Int(dimensions.height))
        else { throw FfiMediaError.Unsupported }
        let atoms = ext[kCMFormatDescriptionExtension_SampleDescriptionExtensionAtoms] as? NSDictionary
        guard ["dvcC", "dvvC", "dvwC"].allSatisfy({ atoms?[$0] == nil }) else { throw FfiMediaError.Unsupported }
        let depth: UInt8
        switch CMFormatDescriptionGetMediaSubType(description) {
        case kCMVideoCodecType_H264:
            guard let config = atoms?["avcC"] as? Data, config.count >= 7, config[0] == 1,
                  [66, 77, 100].contains(config[1]) else { throw FfiMediaError.Unsupported }
            var parameterSet: UnsafePointer<UInt8>?
            var size = 0
            var sets = 0
            guard CMVideoFormatDescriptionGetH264ParameterSetAtIndex(description,
                parameterSetIndex: 0, parameterSetPointerOut: &parameterSet,
                parameterSetSizeOut: &size, parameterSetCountOut: &sets, nalUnitHeaderLengthOut: nil) == noErr,
                  let parameterSet, size >= 4, sets <= 32, parameterSet[0] & 31 == 7,
                  parameterSet[1] == config[1] else { throw FfiMediaError.Unsupported }
            depth = 8
        case kCMVideoCodecType_HEVC:
            guard let config = atoms?["hvcC"] as? Data, config.count >= 23, config[0] == 1,
                  [1, 2].contains(config[1] & 31), config[16] & 3 == 1,
                  config[17] & 7 == config[18] & 7,
                  [0, 2].contains(config[17] & 7) else { throw FfiMediaError.Unsupported }
            depth = 8 + (config[17] & 7)
        default: throw FfiMediaError.Unsupported
        }
        if let bits = ext[kCMFormatDescriptionExtension_BitsPerComponent] as? NSNumber, bits.intValue != Int(depth) {
            throw FfiMediaError.Unsupported
        }
        let transfer = ext[kCMFormatDescriptionExtension_TransferFunction] as? String
        let primaries = ext[kCMFormatDescriptionExtension_ColorPrimaries] as? String
        let matrix = ext[kCMFormatDescriptionExtension_YCbCrMatrix] as? String
        // No HDR tone mapping or wide-gamut clipping is hidden behind the RGBA8 API.
        guard transfer == nil || transfer == kCMFormatDescriptionTransferFunction_ITU_R_709_2 as String,
              primaries == nil || primaries == kCMFormatDescriptionColorPrimaries_ITU_R_709_2 as String,
              matrix == nil || matrix == kCMFormatDescriptionYCbCrMatrix_ITU_R_709_2 as String
        else { throw FfiMediaError.Unsupported }
        return Format(width: dimensions.width, height: dimensions.height, depth: depth,
            thumbnailSafe: depth == 8 && transfer != nil && primaries != nil && matrix != nil)
    }

    /// Only orthogonal, unit-scale display transforms are qualified (including mirrors).
    internal static func orientation(_ t: CGAffineTransform) throws -> Int32 {
        let matrices: [[CGFloat]] = [[1,0,0,1], [-1,0,0,1], [-1,0,0,-1], [1,0,0,-1],
                                    [0,1,1,0], [0,1,-1,0], [0,-1,-1,0], [0,-1,1,0]]
        guard t.tx.isFinite, t.ty.isFinite, let index = matrices.firstIndex(where: { matrix in
            zip(matrix, [t.a, t.b, t.c, t.d]).allSatisfy { abs($0 - $1) < 0.00001 }
        }) else { throw FfiMediaError.Unsupported }
        return Int32(index + 1)
    }

    /// Core Image performs the declared YCbCr/transfer conversion and orientation, with
    /// explicit sRGB output. Software rendering avoids custom shader/color assumptions.
    private static func makeThumbnail(_ buffer: CVPixelBuffer, orientation: Int32,
                                      width: UInt32, height: UInt32, edge: UInt32) throws -> FfiDecodedVideoFrame {
        let scale = min(1, Double(edge) / Double(max(width, height)))
        let w = max(1, Int(floor(Double(width) * scale)))
        let h = max(1, Int(floor(Double(height) * scale)))
        var image = CIImage(cvPixelBuffer: buffer).oriented(forExifOrientation: orientation)
        image = image.transformed(by: CGAffineTransform(translationX: -image.extent.minX, y: -image.extent.minY))
        image = image.transformed(by: CGAffineTransform(scaleX: CGFloat(w) / image.extent.width, y: CGFloat(h) / image.extent.height))
        let bounds = CGRect(x: 0, y: 0, width: w, height: h)
        guard let srgb = CGColorSpace(name: CGColorSpace.sRGB),
              let linear = CGColorSpace(name: CGColorSpace.extendedLinearSRGB) else { throw FfiMediaError.Codec }
        let context = CIContext(options: [.useSoftwareRenderer: true, .cacheIntermediates: false, .workingColorSpace: linear])
        defer { context.clearCaches() }
        guard let cgImage = context.createCGImage(image, from: bounds, format: .RGBA8, colorSpace: srgb)
        else { throw FfiMediaError.Codec }
        var bytes = Data(count: w * h * 4)
        try bytes.withUnsafeMutableBytes { raw in
            guard let bitmap = CGContext(data: raw.baseAddress, width: w, height: h,
                bitsPerComponent: 8, bytesPerRow: w * 4, space: srgb,
                bitmapInfo: CGBitmapInfo.byteOrder32Big.rawValue | CGImageAlphaInfo.premultipliedLast.rawValue)
            else { throw FfiMediaError.Codec }
            bitmap.draw(cgImage, in: bounds)
        }
        return FfiDecodedVideoFrame(width: UInt32(w), height: UInt32(h), rgba: bytes)
    }
}
#endif
