// swift-tools-version: 5.9
import Foundation
import PackageDescription

private struct SDKRelease: Decodable {
    struct Artifact: Decodable {
        let filename: String
        let url: String
        let sha256: String
    }
    struct Artifacts: Decodable {
        let ios: Artifact
    }
    let schema_version: Int
    let version: String
    let source_revision: String
    let source_repository: String
    let source_tag: String
    let artifacts: Artifacts
}

private let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
private let localPath = "Artifacts/TinkleSDKFFI.xcframework"
private let localURL = root.appendingPathComponent(localPath)
private let forceLocal = ProcessInfo.processInfo.environment["TINKLE_SDK_LOCAL"] == "1"
private var isDirectory: ObjCBool = false
private let hasLocalArtifact = FileManager.default.fileExists(atPath: localURL.path, isDirectory: &isDirectory) && isDirectory.boolValue

private let ffi: Target = {
    if forceLocal || hasLocalArtifact {
        guard hasLocalArtifact else {
            fatalError("TINKLE_SDK_LOCAL=1 requires Artifacts/TinkleSDKFFI.xcframework. Stage a matching native candidate into a disposable package copy; see README.md.")
        }
        return .binaryTarget(name: "TinkleSDKFFI", path: localPath)
    }
    let metadata = root.appendingPathComponent("sdk-artifact.json")
    guard FileManager.default.fileExists(atPath: metadata.path) else {
        fatalError("Missing staged XCFramework or sdk-artifact.json. packages/swift contains handwritten package sources; stage matching native artifacts and generated bindings into a disposable package copy. See README.md.")
    }
    do {
        let release = try JSONDecoder().decode(SDKRelease.self, from: Data(contentsOf: metadata))
        let artifact = release.artifacts.ios
        guard release.schema_version == 1,
              release.source_repository == "tinkle-org/libtinkle",
              release.source_tag == "tinkle-ffi-v\(release.version)",
              release.source_revision.range(of: "^[a-f0-9]{40}$", options: .regularExpression) != nil,
              artifact.filename == "TinkleSDKFFI.xcframework.zip",
              artifact.url == "https://github.com/tinkle-org/libtinkle/releases/download/\(release.source_tag)/\(artifact.filename)",
              artifact.sha256.range(of: "^[a-f0-9]{64}$", options: .regularExpression) != nil else {
            fatalError("Invalid sdk-artifact.json: expected schema 1 core-release provenance, an exact source SHA, and the canonical GitHub release XCFramework URL with SHA-256.")
        }
        return .binaryTarget(name: "TinkleSDKFFI", url: artifact.url, checksum: artifact.sha256)
    } catch {
        fatalError("Cannot read sdk-artifact.json: \(error)")
    }
}()

private let generatedSwift = root.appendingPathComponent("Sources/TinkleSDK/TinkleSDK.swift")
guard FileManager.default.fileExists(atPath: generatedSwift.path) else {
    fatalError("Missing staged Sources/TinkleSDK/TinkleSDK.swift. The canonical generated source is bindings/swift/TinkleSDK.swift in libtinkle; stage bindings from the same candidate as the XCFramework. See README.md.")
}

let package = Package(
    name: "TinkleSDK",
    platforms: [.iOS(.v15)],
    products: [.library(name: "TinkleSDK", targets: ["TinkleSDK"])],
    targets: [
        ffi,
        .target(
            name: "TinkleSDK",
            dependencies: ["TinkleSDKFFI"],
            linkerSettings: [
                // cargo rustc -- --print=native-static-libs (all iOS slices).
                // System, c and m are linked by the Apple toolchain itself.
                .linkedLibrary("iconv"),
            ]
        ),
        .testTarget(name: "TinkleSDKTests", dependencies: ["TinkleSDK"]),
    ],
    swiftLanguageVersions: [.v5]
)
