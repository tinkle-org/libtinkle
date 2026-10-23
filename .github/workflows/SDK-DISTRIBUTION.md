# Native SDK automation contract

## Release chain

1. Release-plz retains the workspace's `publish = false` git-only policy. Its
   `releases` action output is a JSON array of `{package_name, tag, version, prs}`
   ([official output schema](https://release-plz.dev/docs/github/output)). Only
   `package_name == "tinkle-ffi"` starts the reusable `native-sdk.yml` workflow.
   This is an explicit `needs`/`workflow_call` dependency: it does not depend on
   `GITHUB_TOKEN`-created tags generating another event.
2. Resolve the existing `tinkle-ffi-v<version>` tag to an exact 40-character SHA.
   Require its revision to be on protected `main`, and match the version from
   `cargo metadata --locked --no-deps --format-version 1`. Platform jobs checkout
   that SHA, with checkout credentials disabled. Rust is **1.99.1** and all four
   UniFFI runtime/generator packages must resolve to **0.32.2**.
3. Build once per platform with `scripts/build-ios.sh` and
   `scripts/build-android.sh`. Both honor `SDK_OUTPUT_DIR` (default `target/sdk`),
   use `ios-release` / `android-release`, and enforce generated-binding parity
   using `generate-bindings.sh --check [library]`. Platform build/ELF checks are
   owned by those scripts. CI does not replace their linker flags.
4. Assemble `sdk-release.json` from the four actual ZIP files. Use Actions
   artifacts to transport the unchanged bytes between jobs, then test consumer
   packages from those ZIPs. No native binary is committed to a repository.
5. Only after the Swift and Kotlin consumer jobs pass, upload those same bytes
   and the manifest to the **existing** core GitHub Release. Never create/move
   source tags, recreate releases, use `--clobber`, or silently replace an asset.
   A retry verifies GitHub's SHA-256 digest, or downloads and hashes the existing
   asset if the API does not expose a digest. A difference fails the release.
6. An optional fine-grained `SDK_DISTRIBUTION_TOKEN` opens one release PR in each
   SDK repository. Its absence explicitly fails the promotion jobs with
   `BLOCKED`; the native validation jobs and core artifact publication do not
   need that token. Successful publication alone is not successful promotion.
7. Review and merge each SDK PR. The SDK release workflow verifies the published
   core manifest, generated binding, merged same-repository release branch,
   restricted changed-file set, main ancestry, and tag/version before creating
   a lightweight `v<version>` tag at the merge commit. It never auto-merges.
   Existing tags must already point to that exact commit. The Kotlin workflow
   explicitly depends on this authorization to warm/test the JitPack coordinate;
   it also does not rely on a token-created tag triggering another workflow.
   JitPack's downloaded final AAR is then tested by the sample consumer on both
   page-size lanes, with its digest recorded and checked before/after execution.
   An automation-only Gradle init task resolves the real published coordinate,
   compares that AAR's digest, and requires transitive Android JNA/coroutines
   from its POM before the runtime proof.

GitHub Releases are the durable distribution store. Actions artifacts have a
14-day retention and are only CI transport/evidence. An expired validation-only
run can be rebuilt, but replacing a published artifact requires a new version.
The schema-1 `sdk-release.json` deliberately keeps exactly four required
artifacts. Producer debug outputs are retained durably alongside it as a
separate sidecar: `android-native-debug-symbols.zip`,
`android-build-metadata.json`, and their checksums in `sdk-debug-assets.json`.
The sidecar ties each shipped `.so` (per-ABI SHA-256/size) to the producer's
`android-build-metadata.json` build IDs, and publishing fails closed on any
partial sidecar instead of silently discarding symbols. iOS dSYMs are not
produced by the current platform automation; if platform agents add them, extend
the sidecar rather than widening the fixed four-artifact schema.

## Manifest and archives

The canonical manifest has exactly these fields:

```json
{
  "schema_version": 1,
  "version": "1.2.3",
  "source_revision": "0123456789abcdef0123456789abcdef01234567",
  "source_repository": "tinklehq/libtinkle",
  "source_tag": "tinkle-ffi-v1.2.3",
  "artifacts": {
    "ios": {"filename": "TinkleSDKFFI.xcframework.zip", "url": "https://github.com/tinklehq/libtinkle/releases/download/tinkle-ffi-v1.2.3/TinkleSDKFFI.xcframework.zip", "sha256": "<64 lowercase hex characters>"},
    "swift_bindings": {"filename": "swift-bindings.zip", "url": "https://github.com/tinklehq/libtinkle/releases/download/tinkle-ffi-v1.2.3/swift-bindings.zip", "sha256": "<64 lowercase hex characters>"},
    "android": {"filename": "android-native-libraries.zip", "url": "https://github.com/tinklehq/libtinkle/releases/download/tinkle-ffi-v1.2.3/android-native-libraries.zip", "sha256": "<64 lowercase hex characters>"},
    "kotlin_bindings": {"filename": "kotlin-bindings.zip", "url": "https://github.com/tinklehq/libtinkle/releases/download/tinkle-ffi-v1.2.3/kotlin-bindings.zip", "sha256": "<64 lowercase hex characters>"}
  }
}
```

The example hashes are placeholders, not usable release metadata. The helper
derives real values from files and Cargo; no workflow input supplies a version.
All four URLs must exactly match the source repository, tag, and artifact name;
HTTP, credentials in URLs, alternate hosts/repositories, query strings, and
fragments are rejected. Downloads follow HTTPS redirects only to GitHub's known
release asset hosts. Checksums are verified before extraction.

Archives reject traversal, absolute/Windows paths, symlinks, special files,
duplicate/case-colliding paths, file/directory collisions, encryption, excessive
entry counts, and excessive compressed/expanded sizes. Limits are 2 GiB per ZIP,
4 GiB expanded, and 20,000 entries. Binding layouts are:

* Swift ZIP: exactly `TinkleSDK.swift`, `TinkleSDKFFI.h`, `module.modulemap` at root.
* Kotlin ZIP: `com/tinkle/sdk/tinkle_ffi.kt` at root (directory entries allowed).
* XCFramework ZIP: `TinkleSDKFFI.xcframework/` at root.
* Android ZIP: the layout enforced by `build-android.sh` and the SDK Gradle
  unpack task; all supported ABI `.so` files come from the same core SHA.

## CLI

Run from the libtinkle root with Python 3.11+:

```sh
python3 scripts/sdk-release.py build-manifest --artifacts target/sdk
python3 scripts/sdk-release.py build-debug-assets --manifest target/sdk/sdk-release.json --artifacts target/sdk
python3 scripts/sdk-release.py verify --manifest target/sdk/sdk-release.json --artifacts target/sdk
python3 scripts/sdk-release.py verify --manifest target/sdk/sdk-release.json --artifacts target/sdk --debug-assets
# Fetch and verify an existing release (no token required for public assets):
python3 scripts/sdk-release.py verify --manifest sdk-release.json --artifacts /tmp/sdk-artifacts --download
# Local-only promotion, no git/network mutation:
python3 scripts/sdk-release.py promote --manifest target/sdk/sdk-release.json --artifacts target/sdk --sdk swift --destination ../tinkle-sdk-swift
python3 scripts/sdk-release.py promote --manifest target/sdk/sdk-release.json --artifacts target/sdk --sdk kotlin --destination ../tinkle-sdk-kotlin
# Explicit remote PR operation, called only by the promotion jobs:
python3 scripts/sdk-release.py promote --manifest target/sdk/sdk-release.json --artifacts target/sdk --sdk swift --open-pr
```

Promotion writes only the full manifest as `sdk-artifact.json` and:

* Swift: `Sources/TinkleSDK/TinkleSDK.swift`
* Kotlin: `src/main/kotlin/com/tinkle/sdk/tinkle_ffi.kt`

Handwritten adapters, package manifests, Gradle files, tests, and native files
are outside promotion's write set. Same metadata is a no-op, including file
timestamps; a version's existing provenance cannot be rewritten. Remote retries
reuse `sdk-release/<version>` and its PR, compare existing proposed files, and
refuse force pushes or unrelated branch edits. A changed base requires human
resolution if it makes the existing proposal inconsistent. Argument arrays are
used for subprocesses; neither shell evaluation nor credential-bearing URLs are
used by the helper.

## Consumer/build integration contracts

`scripts/sdk-ci.py` is automation-only and shared by core and downstream CI.
It stages exact packaged artifacts. In ordinary SDK CI it **compares** checked-in
generated bindings with the published ZIP; only core candidate testing and
explicit initial bootstrap copy a generated binding into a disposable checkout.

Swift package integration:

* Use `Artifacts/TinkleSDKFFI.xcframework` when present, otherwise use the URL and
  checksum in `sdk-artifact.json`. Local staging never changes the ZIP bytes.
* Scheme/test target: `TinkleSDK` / `TinkleSDKTests`. The test must import
  `TinkleSDK` and make a deterministic generated binding call, triggering UniFFI
  contract/checksum validation.
* Release Xcode **27.1**; PR/validation Xcode **26.6** on hosted `macos-26`;
  deployment target **iOS 15.0**. The helper
  chooses an actually available simulator and records its runtime/device ID.
  This is current-runtime proof only. These Xcode versions cannot run iOS 15; minimum-OS
  device/runtime proof needs separate evidence. No CI result claims otherwise.

Kotlin package integration:

* Root Android library outputs `build/outputs/aar/*-release.aar` and supports
  `assembleRelease` / `testReleaseUnitTest` with its checked-in Gradle wrapper.
* Local CI override: `TINKLE_SDK_ARTIFACT_DIR` is the absolute directory holding
  the exact Android/binding ZIPs, and `TINKLE_SDK_ANDROID_SHA256` /
  `TINKLE_SDK_KOTLIN_BINDINGS_SHA256` provide their computed checksums. The SDK
  validates these together, including before a first manifest exists.
  Published/JitPack builds instead use the committed `sdk-artifact.json`.
* `sample` has `connectedDebugAndroidTest`. With
  `-PtinkleSdkTestAar=/absolute/path/to/release.aar` (also provided as
  `TINKLE_SDK_TEST_AAR`) it consumes that final file AAR, never the source library
  project or a jniLibs directory. The helper records and rechecks the AAR digest.
* The instrumentation test calls the adapter initializer, explicitly invokes
  `uniffiEnsureInitialized()` for UniFFI 0.32.2, and makes a stable generated API
  call. Alignment checks must include all bundled JNA/native libraries.
* R8 stays enabled for both the sample app and its instrumentation APK. The SDK
  AAR ships its JNA native/reflection keep rules plus narrow exclusions for the
  four unavailable desktop-only AWT types referenced by JNA's optional helpers.
  These follow [JNA's Android guidance](https://github.com/java-native-access/jna/blob/5.18.1/www/FrequentlyAskedQuestions.md#jna-on-android),
  not blanket missing-class suppression. AndroidX Test's Error Prone annotation
  types are supplied on `androidTestCompileOnly`, so R8 can resolve the
  CLASS-retained references without shipping Java compiler APIs in the APK.
* minSdk **24**, compileSdk **36**, sample targetSdk **36**; read actual producer
  plugin/Kotlin/JNA versions from the SDK's Gradle configuration. CI does not
  override them. CI uses JDK 17; change the lane if the checked-in tuple requires
  another JDK.
* Android setup installs the producer's NDK **30.0.16248370**, platform 36, and
  build-tools 36.0.0 explicitly. Final AAR and APK ELF checks call the producer's
  `verify-android.sh`; the APK also passes `zipalign -c -P 16 -v 4`. This includes
  dependency `.so` files such as JNA in the final consumer APK.
* Runtime lanes: API 24/default/x86_64/4096-byte pages (minimum floor),
  API 36/google_apis/x86_64/4096-byte pages (current runtime), and API
  36/google_apis_ps16k/x86_64/16384-byte pages (current 16 KB runtime). The API
  level, primary ABI, and application page size are asserted; a mismatch fails
  the lane. An unavailable 16 KB image fails/blocks the lane, never falls back
  to 4 KB. Authoritative Google SDK indexes list
  `system-images;android-36;google_apis;x86_64` (rev 7),
  `system-images;android-36;google_apis_ps16k;x86_64` (rev 7), and
  `system-images;android-24;default;x86_64` (rev 8). The
  `reactivecircus/android-emulator-runner` lane provisions these images; a
  missing runner/image is blocked evidence, not a pass. ARM runtime execution
  and physical device proof are not claimed by x86_64 CI.
* Runtime page size is measured with `getconf PAGE_SIZE` (or `PAGESIZE`). API 24,
  which may lack `getconf`, uses the same `AT_PAGESZ` value from the device-side
  `/proc/self/auxv`, transported via binary-safe `adb exec-out`. The fallback
  parser is restricted to the matrix's native little-endian x86_64 utilities.
  A successful numeric result never falls back just because it differs from the
  expected lane size. `/proc/self/smaps` is not used as page-size proof: Android's
  [x86_64 16 KB simulation](https://source.android.com/docs/core/architecture/16kb-page-size/getting-started-cf-x86-64-pgagnostic)
  can expose 16 KB application pages while mappings still use 4 KB kernel pages.
* JitPack coordinate: `com.github.tinklehq:tinkle-sdk-kotlin:vX.Y.Z`. JitPack
  downloads the pinned Rust bundle and packages it; it must never build Rust.

## Bootstrap and operator handoff

No initial `sdk-artifact.json` or fake release tag is added. Before the first
core release, SDK CI reports native validation as **not run**. Use SDK
`workflow_dispatch` with `source_ref` to explicitly select the core revision
containing the matching build/automation changes. The ref is resolved to a SHA
before building. For the current stacked bootstrap, an operator may choose
`feat/native-sdk-distribution`; it is not baked into the permanent dependency.
Once metadata exists, SDK CI exclusively uses its `source_revision`.

Core PR and validation-only runs default to the full SDK commit SHAs checked in
at `scripts/sdk-consumers.json`, not the downstream `main` branches. These are
test dependencies: update them deliberately alongside core/consumer contract
changes, including after the bootstrap PRs merge. An SDK bootstrap need not be
merged to validate a core PR. Optional `swift_ref` and `kotlin_ref` dispatch/call
overrides must be full lowercase commit SHAs; mutable branches/tags are rejected.
For a manual validation run, use `publish=false` and an exact `source_ref`.

The `source` job resolves each consumer commit and inspects its immutable Git
tree before any native builds. Both the package scaffolding and the build/test
sources must exist, including the Kotlin `buildSrc` implementation and tests.
Missing files fail with their SDK/revision and complete missing-file list; no
placeholder manifests, skipped tests, or `continue-on-error` are used. Candidate
staging repeats the scaffold check before reading artifacts. Do not use a dirty
local checkout as proof: untracked/ignored files are absent from CI. In
particular, Kotlin's package directory `com/tinkle/build` must not be lost to a
blanket `**/build/` ignore rule.

Publishing does not use these validation pins. It resolves each SDK's `main`
once to an immutable SHA and requires the same complete scaffold. Release input
overrides may be empty or `main` only. Before publication, merge the matching
producer/consumer contracts to main. All PR validation remains read-only, and
the existing protected-main/tag, artifact checksum, and consumer-test gates
remain mandatory for publication.

This follows the explicit multi-repository `repository`/`ref` checkout pattern
in [actions/checkout](https://github.com/actions/checkout#checkout-multiple-repos-side-by-side).
[Ferrostar](https://stadiamaps.github.io/ferrostar/dev-env-setup.html) avoids
cross-repository bootstrap ordering by keeping platform packages in its
monorepo. We retain separate SDK repositories and instead version their CI
dependency revisions explicitly. GitHub's [inputs context](https://docs.github.com/en/actions/reference/workflows-and-actions/contexts#inputs-context)
exists for manual/reusable workflows, not ordinary PR events; input defaults
alone cannot configure a PR's downstream dependencies.

PR/validation builds explicitly select Xcode 26.6, present on the
[hosted image used by the failing runs](https://github.com/actions/runner-images/blob/macos-26-arm64/20261013.0424/images/macos/macos-26-arm64-Readme.md).
Publication retains the Xcode 27.1 requirement. Both producer and Swift consumer
use the same mode-specific version and verify `xcodebuild -version` under the
selected `DEVELOPER_DIR` before exporting it for later steps. `Xcode.app` is
accepted only if its actual version exactly matches the pin. An unavailable or
wrong version is a hard failure, not a fallback to an arbitrary runner default.
Validation under 26.6 does not claim release qualification under 27.1.

Main/operator follow-up (not performed by these changes):

1. Review/merge the native scripts, generated bindings, toolchain/profile/lockfile
   updates, and the two SDK package implementations alongside this automation.
   In particular confirm the Gradle local-artifact/AAR-consumer inputs above.
2. Make the public SDK repositories and their `main` branches available, and
   protect core/SDK main and release-tag patterns against unauthorized writes.
   Core publishing explicitly checks that main is protected; CI never changes
   protection settings.
3. Provision a runner with Xcode 27.1 at the documented path and both required
   Android emulator images. Treat an absent runner/image as blocked evidence.
4. If automatic cross-repository PRs are desired, provision a fine-grained PAT
   with Contents read/write and Pull requests read/write **only** for the two
   SDK repositories, stored in core as `SDK_DISTRIBUTION_TOKEN`. Core release
   uploads use its job-scoped `GITHUB_TOKEN`. No GitHub App or registry/signing
   secret is required. Verify organization policy permits Actions PR creation.
5. Run explicit bootstrap validation and then a real release. Review all job
   results, promotion PRs, and JitPack resolution. Local Python/actionlint success
   does not establish simulator, device, GitHub release, or token-gated success.

Dependabot checks Cargo and Actions weekly and groups the three Syntra crates in
one update PR. The Buf registry (`sparse+https://buf.build/gen/cargo/public/`,
`[registries.buf]` in `.cargo/config.toml`) is public and anonymous:
`GET /gen/cargo/public/config.json` returns HTTP 200 with
`{"dl": ..., "api": ..., "auth-required": false}`, so no token is invented and
none is configured. The `buf-public` entry is still declared under the top-level
`registries:` key (type `cargo-registry`, `registry: buf`, same URL, no token)
because Dependabot update jobs only get network egress to declared registry
hosts; `.cargo/config.toml` alone does not grant it. This matches the
Dependabot schema (`type` + `url` required, `token` optional) and the private
registries doc (declare the registry even for anonymous access). Scope the
declaration to the Cargo update block via `registries: [buf-public]`. Add a
Gradle entry in core only when core actually gains a Gradle manifest.

Rust/MSRV lane split: the pinned-Rust lane runs clippy/test with
`--all-features`, covering the optional `cli` generator feature. The MSRV lane
resolves the single declared workspace MSRV, pre-fetches with the modern Cargo,
then runs `cargo +<msrv> check --locked --offline --workspace --all-targets`
with default features only (optional CLI stays disabled). This is intentional:
the MSRV guarantee covers the default build; the optional CLI path is proven by
the pinned-toolchain `--all-features` lane.

## Action pins

Resolved independently with `gh api repos/<owner>/<repo>/commits/<tag>`:

| Action | Version | Commit |
| --- | --- | --- |
| actions/checkout | v7.0.1 | `3d3c42e5aac5ba805825da76410c181273ba90b1` |
| actions/upload-artifact | v7.0.2 | `cf430e030ddbb5b0abf93d22962f4752f3646cd9` |
| actions/download-artifact | v8.0.2 | `9000827ccba6bdab643e8b6fd33ac0654aef8333` |
| actions/setup-java | v6.1.0 | `b843ebf9b913146b38da2eaa7aeddf76074ae1ad` |
| gradle/actions (setup-gradle) | v6.4.0 | `3f5f9adaf7d9fecd50b5935e54106014257a94e6` |
| reactivecircus/android-emulator-runner | v2.38.0 | `a421e43855164a8197daf9d8d40fe71c6996bb0d` |
| Swatinem/rust-cache | v2.9.3 | `d1527c8f67ef9af357290475d4af4b0fab8f12e0` |
| release-plz/action | v0.5.144 | `8b5bf233ecb5779f63508db7cd050e948ee77cc2` |

Upload v7 and download v8 use the compatible artifact service; uploaded ZIPs are
ordinary files inside the transport archive, preserving their checksummed bytes.
All third-party workflow action references are full immutable commit SHAs.
