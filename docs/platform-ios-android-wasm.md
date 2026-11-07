# Platform integration

## iOS and Android

`TinkleClient` wraps the native Rust client on a shared process-wide Tokio
runtime. Both platforms use encrypted TCP from the exact published Syntra rc.3
SDK. Configuration is `ClientConfigFfi(host, primary_port, fallback_port)`;
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

The application surface retains `start`, `connect`, `subscribe`, typed/raw calls,
`loadHistory`, `loadDialogs`, `getMessages`, `getTargetChanges`, file transfers,
`nextEvent`, `reconcileNow`, `targetCursor`, `targetStatus`, `trackedTargets`,
`suspend`, `resume`, `stop` and `shutdown`. Store callbacks remain asynchronous.
Swift/Kotlin keep structured authentication and retryable error variants.
Direct Rust calls support `CancelToken`; explicit cancellation-token bridging
across UniFFI remains separate work.

For background execution, suspend the foreground socket. Push wakes drive
bounded authoritative fetch/reconciliation through the existing durable job and
scope-fencing pipeline. Application cache and cursors survive process death;
channel state and in-flight requests do not.

## Native consumer fixtures

Run `python3 scripts/with-native-fixture.py <consumer command...>` from this
repository. It builds a host-only Rust example, never substitutes packaged SDK
artifacts, starts its loopback control socket at port 48080 and stops it after
the consumer exits. Swift simulator fixtures control it at `127.0.0.1`; Android
emulator fixtures use the host-loopback alias `10.0.2.2`.

The harness allocates independent encrypted app listeners, generates server
pins through the SDK, serves schema-encoded journal/history pages and exposes
request logs for assertions. Swift/Kotlin fixtures implement only JSON harness
control. All application channel proof, encryption and RPC framing use the
published SDK. Rust integration tests exercise transport/trust boundaries and
the full application cache/history contracts.

## Browser

Browser networking is unsupported. `libtinkle-web` is a portable schema-check
crate with no native runtime dependency. `network_supported()` returns false;
`tinkle_connect()` returns an explicit unsupported error. It accepts no network
credentials. `validate_connectivity` and `tinkle-schema` validation remain useful
without a connection.

```sh
./scripts/test-schema-candidate.sh --output /absolute/temp/candidate
./scripts/test-schema-candidate.sh --output /absolute/temp/candidate -- \
  cargo check --locked -p tinkle-schema -p libtinkle-web --target wasm32-unknown-unknown
./scripts/test-schema-candidate.sh --output /absolute/temp/candidate --bindings
```
