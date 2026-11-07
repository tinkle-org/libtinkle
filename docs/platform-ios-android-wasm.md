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

Check and regenerate bindings from the original workspace against the published
BSR prost artifact `0.5.0-20261107080548-dc41c2199c76.2`, including schema #57's
enrollment-disposition contract:

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
artifacts, starts its loopback control socket at port 48080 and stops it after
the consumer exits. Swift simulator fixtures control it at `127.0.0.1`; Android
emulator fixtures use the host-loopback alias `10.0.2.2`.

The harness allocates independent encrypted app listeners, generates server
pins through the SDK, serves schema-encoded journal/history pages and exposes
request logs for assertions. Swift/Kotlin fixtures implement only JSON harness
control. All application channel proof, encryption and RPC framing use the
published SDK. Rust integration tests exercise transport/trust boundaries and
the full application cache/history contracts.

```sh
python3 scripts/with-native-fixture.py cargo test --locked -p libtinkle --test mobile_fixture -- --ignored
```

## Browser

Browser networking is unsupported. `libtinkle-web` is a portable schema-check
crate with no native runtime dependency. `network_supported()` returns false;
`tinkle_connect()` returns an explicit unsupported error. It accepts no network
credentials. `validate_connectivity` and `tinkle-schema` validation remain useful
without a connection.

```sh
cargo check --locked -p libtinkle-web -p tinkle-schema --target wasm32-unknown-unknown
```
