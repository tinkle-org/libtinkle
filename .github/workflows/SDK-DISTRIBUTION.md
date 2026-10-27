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
5. For authorized publication, `native-sdk.yml` calls reusable `ci.yml` through
   `rust-checks`, passing `source_sha: needs.source.outputs.sha` to run the same
   required Rust checks at the exact resolved source. `publish` directly needs
   `source`, `rust-checks`, `manifest`, `swift-consumer`, and `kotlin-consumer`;
   only after they succeed does it upload those same bytes and the manifest to
   the **existing** core GitHub Release. `scripts/sdk-attestation.py` is deleted;
   there is no separate historical push/main Actions API polling guarantee.
   Never create/move source tags, recreate releases, use `--clobber`, or silently
   replace an asset.
   A retry verifies GitHub's SHA-256 digest, or downloads and hashes the existing
   asset if the API does not expose a digest. A difference fails the release.
6. An optional fine-grained `SDK_DISTRIBUTION_TOKEN` opens one release PR in each
   SDK repository. Its absence explicitly fails the promotion jobs with
   `BLOCKED`; the native validation jobs and core artifact publication do not
   need that token. Successful publication alone is not successful promotion.
7. Review and merge each SDK PR. In the local downstream templates, `ci.yml`
   directly calls reusable `release.yml` after successful consumer validation
   at that SDK event SHA (including Kotlin build-logic and all matrix lanes).
   A main push changing `sdk-artifact.json` can create a lightweight
   `v<version>` tag at that exact merge commit; an external `v*` push verifies
   its existing tag through the same guard. The `sdk-tag` guard verifies the
   published core manifest, full canonical package snapshot/binding/inventory,
   merged same-repository release branch,
   restricted changed-file set, main ancestry, and tag/version. It never
   auto-merges. These inactive templates still require separate downstream
   installation (see below).
   Existing tags must already point to that exact commit. The Kotlin workflow
   explicitly depends on this authorization to warm/test the JitPack coordinate;
   it also does not rely on a token-created tag triggering another workflow.
   JitPack's downloaded final AAR is then tested by the sample consumer on all three
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
  "source_repository": "tinkle-org/libtinkle",
  "source_tag": "tinkle-ffi-v1.2.3",
  "artifacts": {
    "ios": {"filename": "TinkleSDKFFI.xcframework.zip", "url": "https://github.com/tinkle-org/libtinkle/releases/download/tinkle-ffi-v1.2.3/TinkleSDKFFI.xcframework.zip", "sha256": "<64 lowercase hex characters>"},
    "swift_bindings": {"filename": "swift-bindings.zip", "url": "https://github.com/tinkle-org/libtinkle/releases/download/tinkle-ffi-v1.2.3/swift-bindings.zip", "sha256": "<64 lowercase hex characters>"},
    "android": {"filename": "android-native-libraries.zip", "url": "https://github.com/tinkle-org/libtinkle/releases/download/tinkle-ffi-v1.2.3/android-native-libraries.zip", "sha256": "<64 lowercase hex characters>"},
    "kotlin_bindings": {"filename": "kotlin-bindings.zip", "url": "https://github.com/tinkle-org/libtinkle/releases/download/tinkle-ffi-v1.2.3/kotlin-bindings.zip", "sha256": "<64 lowercase hex characters>"}
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

Promotion writes the **full canonical package source snapshot** selected by
`packages/packages.json` at the manifest's exact `source_revision`, including
handwritten adapters, package/Gradle manifests, build logic, and tests. It also
writes the full manifest as `sdk-artifact.json`, the managed source inventory as
`sdk-source-files.json`, and the checksum-verified generated binding:

* Swift: `Sources/TinkleSDK/TinkleSDK.swift`
* Kotlin: `src/main/kotlin/com/tinkle/sdk/tinkle_ffi.kt`

Canonical git file modes are preserved (`100644`/`100755`, including executable
wrappers). Removed package paths are deleted only when the previous inventory
matches the previous immutable core catalog and provenance; an arbitrary
inventory cannot authorize deletion. A legacy checkout without an inventory
does not grant ownership of unrelated files. Native binaries and `.github`
workflow/settings files remain outside the write set. The catalog deliberately
excludes `.github`; workflow installation is a separate manual review operation.
Matching bytes, modes, and managed absences are a no-op, including file
timestamps; a version's existing provenance cannot be rewritten. Remote retries
reuse `sdk-release/<version>` and its PR, compare existing proposed files, and
refuse force pushes or unrelated branch edits. A changed base requires human
resolution if it makes the existing proposal inconsistent. Argument arrays are
used for subprocesses; neither shell evaluation nor credential-bearing URLs are
used by the helper.

`open_pr` in `scripts/sdk-release.py` is intentionally retained: a published version's
proposal branch is immutable, never force-updated. Closed PRs are rejected;
merged PRs are reverified at their historical merge SHA. The default branch
rewrites in `peter-evans/create-pull-request`, including force-with-lease, are
not equivalent to this policy; promotion must not become mutable auto-update.

## Consumer/build integration contracts

`scripts/sdk-ci.py` is automation-only. In core CI it syncs the local
`packages/` trees into a disposable directory, then stages exact packaged
artifacts. In ordinary SDK CI it **compares** checked-in
generated bindings with the published ZIP; only core candidate testing and
explicit initial bootstrap copy a generated binding into a disposable checkout.
The `sync-package` subcommand is the only writer of package sources into the
test directory; `stage` never silently heals an incomplete checkout.

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
* Before Android assembly, `sdk-ci.py android-build` runs independent JVM
  `fixture-tests` and `buildSrc` tests. The wire fixture supports journal paging
  and the subsequent SQLite `GetHistory` catch-up; an empty terminal history
  response does not advance the acceptance cursor. The fixture's Android Base64
  shim lives only in the JVM test project, never in consumer APKs or the SDK AAR.
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
  The checked-in tuple is Kotlin **2.3.0** and AGP **8.13.2**, which bundles
  [R8 8.13.19 with Kotlin 2.3 support](https://developer.android.com/build/releases/past-releases/agp-8-13-0-release-notes).
* SHA-pinned, Node 24 `android-actions/setup-android` installs the producer's
  NDK **30.0.16248370**, platform 36, and build-tools 36.0.0 explicitly, and
  `ANDROID_NDK_HOME` is persisted through `GITHUB_ENV` to pin later tools.
  Final AAR and APK ELF checks call the producer's
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
* JitPack coordinate: `com.github.tinkle-org:tinkle-sdk-kotlin:vX.Y.Z`. JitPack
  downloads the pinned Rust bundle and packages it; it must never build Rust.

## Bootstrap and operator handoff

No initial `sdk-artifact.json` or fake release tag is added. Before the first
core release, native validation runs against the local `packages/` tree only.
Use `workflow_dispatch` with `source_ref` to explicitly select the core revision
containing the matching build/automation changes. The ref is resolved to a SHA
before building. Once metadata exists, downstream release flows exclusively use
its `source_revision`.

Core PR and validation-only runs use the `packages/swift` and `packages/kotlin`
trees at the tested core SHA — the same commit that owns the Rust API,
generated bindings, and native wrappers. This follows the
[`packages/`](https://github.com/reallyme/crypto) monorepo pattern and
[Ferrostar](https://stadiamaps.github.io/ferrostar/dev-env-setup.html): a
breaking FFI change updates Rust, bindings, wrappers, and tests in one PR, and
CI validates that exact combination. `packages/packages.json` is the sole
package-source catalog. `scripts/sdk-consumers.json`, `pin-consumers`, and the
remote migration path are removed entirely; there are no `swift_ref`/`kotlin_ref`
overrides. For a manual validation run, use `publish=false` and an exact
`source_ref`. `scripts/sdk-ci.py` remains responsible for package staging and
packaging/runtime checks.

Core `native-sdk.yml` also runs on every **main push** in validation mode, using
that exact main SHA and Xcode 26.6. PR and main-push native runs exercise local
package snapshots without publishing assets or opening promotion PRs. Publishing
is a distinct existing-tag/release-plz or explicit authorized operation: it uses
Xcode 27.1, verifies the source tag/protected-main ancestry, and rechecks the
immutable SHA through `rust-checks` in its own publication job graph. Standard
PR/main Rust CI runs separately once per event; validation-only native runs do
not call it again. Publication requires its own Rust and consumer dependencies,
without polling a separate main-push run. Core Rust/native concurrency is
commit/source-specific for non-PR runs and PR-ref-specific for PRs, with
`cancel-in-progress` enabled only for PRs.

Core consumer jobs upload explicit diagnostic paths with `if: always()`:
Swift's `ci-results.xcresult`, and Kotlin's JVM/build-logic results,
instrumentation XML/HTML, R8 mappings, APKs, release AAR and digest record, and
device exception logcat. Logcat is bounded to 2,000 entries and restricted to
`System.err` warnings and `AndroidRuntime` errors; it is collected before emulator
shutdown. Instrumentation prints the complete cause/suppressed chain and the
helper surfaces XML failures in the job log. Collection is best-effort and never
replaces the instrumentation exit status. Reports are untrusted test data, not
release evidence; no environment dumps, Gradle caches, or credentials are uploaded.

The `source` job validates the local package catalog (`packages/packages.json`)
before any native builds. Both the package scaffolding and the build/test
sources must exist, including the Kotlin `buildSrc` implementation and tests.
Missing files fail with the complete missing-file list; no placeholder
manifests, skipped tests, or `continue-on-error` are used. `sync-package`
copies the catalog files into a disposable directory, and candidate staging
repeats the scaffold check before reading artifacts. Do not use a dirty
local checkout as proof: `sync-package` prefers committed bytes, and
untracked/ignored files are absent from CI. In particular, Kotlin's package
directory `com/tinkle/build` must not be lost to a blanket `**/build/`
ignore rule.

Publishing snapshots the canonical `packages/` trees at the release SHA into
the downstream SDK repositories. Before publication, merge the matching
producer/consumer contracts to main. All PR validation remains read-only, and
the existing protected-main/tag, artifact checksum, and consumer-test gates
remain mandatory for publication.

PR/validation builds explicitly select Xcode 26.6 with SHA-pinned, Node 24
`maxim-lobanov/setup-xcode`. Xcode 26.6 is present on the
[hosted image used by the failing runs](https://github.com/actions/runner-images/blob/macos-26-arm64/20261013.0424/images/macos/macos-26-arm64-Readme.md).
Publication retains the Xcode 27.1 requirement. Both producer and Swift consumer
use the same mode-specific version and assert the exact `xcodebuild -version`
after setup. The maintained setup actions replace the removed `select-xcode`
and `android-setup` helper commands. An unavailable or wrong version is a hard
failure, not a fallback to an arbitrary runner default.
Validation under 26.6 does not claim release qualification under 27.1.

### Downstream workflow templates and direct release dependencies

Reviewable sources live outside the active workflow directory in
[`../sdk-distribution/`](../sdk-distribution/README.md): Swift and Kotlin each
have `ci.yml` and `release.yml`. All four templates are inactive and use
`tinkle-org` identities throughout. Install each CI/release pair together in a
separate human-reviewed downstream PR; neither belongs in catalog snapshots.
Swift CI uses the pinned setup action for 26.6; the release template separately
qualifies with exactly 27.1 and fails if unavailable. Consumer and release core
checkouts use `fetch-depth: 0` so
`package_snapshot` can read release and previous-inventory catalogs from history.
Allowlisted failure reports are diagnostic, untrusted data, never release
authorization inputs. See the README for upload scope and manual installation.

Downstream CI owns PR, main-push, `v*`-push, and explicit bootstrap validation.
Its `source` job outputs `ready`, `sdk_sha`, `core_sha`, `bootstrap`, and `release`.
On main, `release` detects a provenance-file change against the commit's first
parent, not the whole push range; `v*` pushes always request verification and
fail if metadata is absent. The release call directly needs `source` and
consumer(s), plus Kotlin `build-logic`, and requires an own-repository main/`v*`
push with `ready=true`, `bootstrap=false`, and `release=true`.

`release.yml` is `workflow_call`-only and receives immutable `source_sha` and
`sdk_sha`. It checks their manifest/checkout identity and protected core-main
ancestry before checking out and executing core helpers. `sdk-tag --create`
is restricted to main pushes with checkout SHA equal to the event SHA; external
tags are verification-only. Both retain merged scoped release-PR, checksum,
full-snapshot, ancestry, and tag-target guards. Tag authorization re-fetches
the selected PR detail because the commit-to-PR association omits
`changed_files`, and permits only promotion-authorized removals from the
merge's first parent with `removed` status proven absent. Kotlin's three JitPack runtime
lanes directly need `authorize`, because `GITHUB_TOKEN`-created tags do not
spawn push workflows. See the template README for installation and local checks.

Main/operator follow-up (not performed by these changes):

1. Review/merge the native scripts, generated bindings, toolchain/profile/lockfile
   updates, and the two SDK package implementations alongside this automation.
   In particular confirm the Gradle local-artifact/AAR-consumer inputs above.
2. Make the public SDK repositories and their `main` branches available, and
   protect core/SDK main and release-tag patterns against unauthorized writes.
   Core publishing explicitly checks that main is protected. No remote rulesets
   or repository settings have been changed by this local refactor.
3. Provision a runner with exactly Xcode 27.1 and all required
   Android emulator images. Treat an absent runner/image as blocked evidence.
4. If automatic cross-repository PRs are desired, provision a fine-grained PAT
   with Contents read/write and Pull requests read/write **only** for the two
   SDK repositories, stored in core as `SDK_DISTRIBUTION_TOKEN`. Core release
   uploads use its job-scoped `GITHUB_TOKEN`. No GitHub App or registry/signing
   secret is required. Verify organization policy permits Actions PR creation.
   Do not grant Workflows write or add workflow templates to package snapshots.
   Apply downstream workflow updates only through separate human-reviewed PRs,
   installing each CI/release pair together with the dependencies above.
5. Run explicit bootstrap validation and then a real release. Review all job
   results, promotion PRs, and JitPack resolution. Local Python/actionlint success
   does not establish simulator, device, GitHub release, or token-gated success.

Local validation from the libtinkle root:

```sh
python3 -B -m unittest discover -s tests -p 'test_*.py' -v
actionlint
```

With `actionlint` installed, `test_sdk_distribution_templates.py` also lints
each inactive CI/release pair in a temporary installed `.github/workflows/`
layout, where the local reusable-workflow path resolves correctly. These local
checks do not establish hosted CI or device execution.

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
| maxim-lobanov/setup-xcode | v1 (Node 24) | `ed7a3b1fda3918c0306d1b724322adc0b8cc0a90` |
| android-actions/setup-android | v4 (Node 24) | `be39fa834029ff78f1a44aa3bb0819b8fc2bd8fd` |
| gradle/actions (setup-gradle) | v6.4.0 | `3f5f9adaf7d9fecd50b5935e54106014257a94e6` |
| reactivecircus/android-emulator-runner | v2.38.0 | `a421e43855164a8197daf9d8d40fe71c6996bb0d` |
| Swatinem/rust-cache | v2.9.3 | `d1527c8f67ef9af357290475d4af4b0fab8f12e0` |
| release-plz/action | v0.5.144 | `8b5bf233ecb5779f63508db7cd050e948ee77cc2` |

Upload v7 and download v8 use the compatible artifact service; uploaded ZIPs are
ordinary files inside the transport archive, preserving their checksummed bytes.
All third-party workflow action references are full immutable commit SHAs.
