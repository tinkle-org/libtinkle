# TinkleSDK for iOS

Headless Swift bindings for the [Tinkle Rust messaging engine](https://github.com/tinkle-org/libtinkle).
The Swift package product and module are **`TinkleSDK`**. Its internal C FFI
module is **`TinkleSDKFFI`**. Requires iOS 15 or later and Swift tools 5.9 or later.
The release XCFramework contains arm64 devices, arm64 simulators, and x86_64 simulators.

## Canonical package sources

`packages/swift` in libtinkle owns the handwritten package manifest, bootstrap source,
and XCTest fixtures. The generated Swift source of truth is
`bindings/swift/TinkleSDK.swift` at the repository root, alongside its C header
and modulemap. There is deliberately no tracked generated binding, native
artifact, or release manifest in this directory.

Native consumer validation stages a disposable copy of this package. Central
automation (`scripts/sdk-ci.py stage --candidate`) supplies
`Sources/TinkleSDK/TinkleSDK.swift`, `sdk-artifact.json`, and the exact candidate
`Artifacts/TinkleSDKFFI.xcframework`. The bindings and native library must come
from the same candidate; a previously built sibling SDK artifact is not a
substitute. Package resolution fails clearly until these inputs are staged.

The distribution repository is
[`tinkle-org/tinkle-sdk-swift`](https://github.com/tinkle-org/tinkle-sdk-swift).
After a published package tag exists, add that URL in Xcode's **Add Package
Dependencies** and link the `TinkleSDK` product. This nested source directory
is used through a staged local package; it is not the root of a remote SwiftPM
dependency on libtinkle.

## Local development and simulator checks

Use libtinkle's pinned Rust toolchain, lockfile, generated bindings, and
`ios-release` profile. Xcode and its iOS SDKs are required. Build native
artifacts from the repository root using the core packaging scripts:

```sh
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
bash scripts/generate-bindings.sh --write
bash scripts/build-ios.sh
bash scripts/verify-ios.sh target/sdk/TinkleSDKFFI.xcframework.zip
```

`SDK_OUTPUT_DIR` overrides `<libtinkle>/target/sdk`; Cargo output paths respect
`CARGO_TARGET_DIR`. If deployment settings change, use a fresh Cargo target
directory to avoid stale native objects. Native packaging verifies that no
object's deployment minimum exceeds iOS 15.

Stage the package with the central consumer tooling, then run these commands
**inside the disposable package copy**:

```sh
TINKLE_SDK_LOCAL=1 swift package dump-package
xcrun simctl list devices available
# Choose an installed simulator UUID and a fresh result-bundle path.
TINKLE_SDK_LOCAL=1 xcodebuild test -scheme TinkleSDK \
  -destination 'platform=iOS Simulator,id=<installed-simulator-uuid>' \
  -derivedDataPath build/DerivedData -resultBundlePath build/Smoke.xcresult \
  IPHONEOS_DEPLOYMENT_TARGET=15.0 CODE_SIGNING_ALLOWED=NO
```

`Package.swift` automatically uses a local XCFramework when present;
`TINKLE_SDK_LOCAL=1` explicitly requires it. A plain host `swift test` targets
macOS, which this package does not support. The generated module keeps SwiftPM's
default nonisolated context and compiles in Swift 5 language mode.

The XCTest suite initializes Rust, constructs native stores and clients, checks
typed async errors and shutdown, and runs a loopback WebSocket/Syntra gateway.
The gateway decodes actual `GetTargetChanges` requests to verify that Rust awaits
Swift's async journal-store callbacks and uses principal- and target-specific
`Int64` acceptance positions. Storage failures are checked through Rust target
status and sync events. A separate smoke test writes protobuf cache data through
the real Rust-owned SQLite API, releases and reopens the database, verifies
principal isolation, and checks that clearing one principal persists across
another reopen. No external messaging server is required.

Compile/typecheck checks alone do not prove native linkage or callbacks. A
simulator test result covers that simulator's runtime and architecture. Xcode
27 cannot run an iOS 15 simulator; minimum-OS and physical-device runtime proof
require separate compatible runtime/device lanes.

## Application API

```swift
import TinkleSDK

// Once during application startup (idempotent).
tinkleFfiInit()

// The async constructor moves database opening and migrations off the UI thread.
let storage = try await SqliteStorage.openAsync(path: databasePath)
let client = try TinkleClient.newSqlite(
    config: ClientConfigFfi(
        url: gatewayURL, allowInsecure: false,
        userAgent: "MyApp", grpcEndpoint: nil
    ),
    credential: sessionToken,
    storage: storage
)
try await client.setPrincipalUserId(userId: authenticatedUserID)
try await client.start(targets: [TargetSub(targetKind: "room", targetId: roomID)], watch: [])
// Consume nextEvent() and read the shared cache in response to sync events.
// Call shutdown() when this client's owner ends its lifecycle.
```

Alternatively, pass an application-owned `FfiCursorStore` to the throwing
`TinkleClient(config:credential:store:)` constructor. Implement async
`loadJournal`, `saveJournal`, `removeTarget`, and `clearPrincipal`, with durable
positions isolated by principal and target key (`target:room:<id>` or
`target:user:<id>`). The actor-backed test implementation is ephemeral.
Use `FfiError` for typed failures. Stop the client before clearing its SQLite
principal so live in-memory positions cannot race the deletion. No SwiftUI or
application lifecycle policy is included in the SDK.

## Release metadata

A distributed package includes `sdk-artifact.json`, copied unchanged from the
core release's schema-1 `sdk-release.json`, plus that release's generated Swift
binding. The manifest validates `source_repository: "tinkle-org/libtinkle"`,
`source_tag: "tinkle-ffi-v<version>"`, an exact source SHA, the canonical GitHub
XCFramework URL, and its SHA-256 before declaring a remote binary target.
SwiftPM verifies the ZIP checksum. Central release automation supplies these
files and publishes the downstream package.

Validate remote resolution without a local `Artifacts/` directory, since local
artifacts take precedence. Keep the published XCFramework ZIP unchanged after
its checksum is recorded. Historical evidence from the sibling repository is
not runtime evidence for the current FFI revision.

## License

The included `LICENSE` is copied from the original Swift SDK and matches the
Rust repository's license file.
