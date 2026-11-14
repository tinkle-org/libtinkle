# Platform integration

## iOS and Android

`TinkleClient` wraps the native Rust client on a shared process-wide Tokio
runtime. Both platforms use encrypted TCP with exact rc.5 SDK pins;
[dependency pinning](../PROTO_PINNING.md#sdk-pins-and-qualification)
owns crate-specific release links and qualification requirements.
Configuration is `ClientConfigFfi(host, primary_port, fallback_port)`;
production defaults in Rust are 443/80. Both ports have identical trust checks.
The configuration also requires `ClientInfoFfi`: all five device/app labels and
the entire immutable optional snapshot.

Provision `ServerTrustFfi(server_public_key, server_key_id, scope)` independently
of DNS/address selection. Construct `TrustSourceFfi.bootstrap(trust)` explicitly
for bootstrap. Supply `FfiSecureEnrollmentStore` over protected platform storage,
then use `prepareEnrollment` → `signIn`/`resumeEnrollment` → explicit
`activateEnrollment`. The stored receipt is returned only after the platform
acknowledges durable candidate storage. Activation uses Bootstrap; validated
ACTIVE storage precedes fresh Returning. Existing sessions use
`restoreActiveEnrollment`. Foreign byte copies remain host-owned and must be
erased. The complete callback and recovery contract is in `device-enrollment.md`.

`TinkleClient.logout` follows the
[high-level Logout contract](device-enrollment.md#local-sign-out-and-remote-revocation):
one nonrenewable entry-time budget limits remote eligibility, while acknowledged
local durable clearing may take longer. Do not impose a three-second cutoff on
host invalidation or equate an unknown remote result with rollback. Incomplete
transport join leaves cleanup `Failed` even after protected storage was
invalidated; retry finishes retained work without remote replay. The
[native verification lanes](../PROTO_PINNING.md#sdk-pins-and-qualification) and
installed platform tests cover different boundaries.

The application surface retains `start`, `connect`, `subscribe`, typed/raw calls,
`loadHistory`, `loadDialogs`, `getMessages`, `listMessageChanges`, file transfers,
`nextEvent`, `reconcileNow`, `targetCursor`, `targetStatus`, `trackedTargets`,
`suspend`, `resume`, `stop` and `shutdown`. Store callbacks remain asynchronous.
Swift/Kotlin keep structured authentication and retryable error variants.
Direct Rust calls support `CancelToken`; explicit cancellation-token bridging
across UniFFI remains separate work.

`crates/tinkle-ffi/src/account.rs` exports `subscribeSessionState()` as an
`AccountStateSubscription`, plus `sessionStatus()`, `getSessions()`,
`confirmSession(sessionId)`, `revokeSession(sessionId)`, and
`revokeOtherSessions()`. The generated surface is in
`bindings/swift/TinkleSDK.swift` and `bindings/kotlin/`.

Only one account-state consumer may be active. Its `next()` returns the
initial/latest `AccountState`; `stop()` releases the slot and unblocks pending
reads with nil/null without signing out. The subscription does not retain the
Client or a foreign callback. `AccountState.sessions` is optional: nil/null means
unavailable/unfetched, while an empty collection is a fetched empty list.
`SelfLogout` and `SessionRetired` remain distinct through cleanup retries.

The SDK owns automatic session refresh. Prompt only for `unconfirmed && !current`
when the current session is confirmed; use targeted `revokeSession` for “No”,
not `revokeOtherSessions`. See
[session review and state](device-enrollment.md#session-review-and-state) for the
server gate and unknown mutation outcomes.

For background execution, suspend the foreground socket. Push wakes drive
bounded authoritative fetch/reconciliation through the existing durable job and
scope-fencing pipeline. Application cache and cursors survive process death;
channel state and in-flight requests do not.

Check and regenerate bindings from the original workspace against the
[published schema pin](../PROTO_PINNING.md#current-published-schema-pin):

```sh
bash scripts/generate-bindings.sh --check
bash scripts/generate-bindings.sh --write
```

`--check` verifies parity; `--write` updates checked-in Swift/Kotlin bindings.
Required CI uses the published Cargo pin without Buf or schema-candidate
generation. Optional future-schema development is described in
[device enrollment](device-enrollment.md#published-schema-checks).

## Native consumer fixtures

Run `python3 scripts/with-native-fixture.py <consumer command...>` from this
repository. It builds a host-only Rust example, never substitutes packaged SDK
artifacts, starts its loopback control socket and stops it after the consumer
exits. The default control port remains 48080 for Swift/Kotlin native-distribution
consumers. Swift simulator fixtures control it at `127.0.0.1`; Android emulator
fixtures use the host-loopback alias `10.0.2.2`.

For host tests, pass `--port 0` to request an OS-assigned control port and avoid
fixed-port collisions in Linux's ephemeral range. The wrapper reads the fixture's
actual bound port and passes it to the consumer in
`TINKLE_NATIVE_FIXTURE_CONTROL_PORT`. `crates/libtinkle/tests/mobile_fixture.rs`
reads that variable, falling back to 48080 when unset. Swift/Kotlin fixtures use
fixed port 48080, so omit `--port` for those consumers.

The harness allocates independent encrypted app listeners, generates server
pins through the SDK, serves schema-encoded journal/history pages and exposes
request logs for assertions. Swift/Kotlin fixtures implement only JSON harness
control. All application channel proof, encryption and RPC framing use the
published SDK. Rust integration tests exercise transport/trust boundaries and
the full application cache/history contracts.

```sh
python3 scripts/with-native-fixture.py --port 0 cargo test --locked -p libtinkle --test mobile_fixture -- --ignored
```

`packages/swift/Tests/TinkleSDKTests/TinkleSDKTests.swift` exercises retained
account state, synthetic encrypted Returning confirmation projection, and typed
confirmation denial through Rust and UniFFI. These fixture replies test the
native bridge, not production account authority. Use the
[Swift simulator lane](../packages/swift/README.md#local-development-and-simulator-checks)
for the package's build and runtime commands.

The Kotlin initializer, generated binding, and sample compilation are separate
from installed Android runtime testing. The account-state smoke in
`packages/kotlin/sample/src/main/kotlin/com/tinkle/sdk/smoke/SmokeChecks.kt` uses
the asynchronous `FfiCursorStore` adapter and JNI-backed subscription methods.
Run the [installed Android lane](../packages/kotlin/README.md#verification) with
the configured NDK, system image, and emulator or device; missing runtime
prerequisites do not turn compilation into an execution result.

A simulator/device result covers only its tested runtime and architecture.
Minimum-OS behavior, physical devices, protected-store durability/security, and
release artifacts require their own scoped evidence; no fixture or compilation
result alone qualifies a release.

## Browser

Browser networking is unsupported. `libtinkle-web` is a portable schema-check
crate with no native runtime dependency. `network_supported()` returns false;
`tinkle_connect()` returns an explicit unsupported error. It accepts no network
credentials. `validate_connectivity` and `tinkle-schema` validation remain useful
without a connection.

```sh
cargo check --locked -p libtinkle-web -p tinkle-schema --target wasm32-unknown-unknown
```
