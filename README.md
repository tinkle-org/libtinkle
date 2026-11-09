# libtinkle

Native Rust client runtime for Tinkle, with Swift/Kotlin bindings. Application
RPCs, history, cache hydration, journal reconciliation and file transfers use
encrypted Syntra TCP. The SDK uses exact rc.4 pins for
`syntra-core`, `syntra-runtime`, and TCP-only `syntra-net`; see
[dependency pins, upstream release links, and scoped paired qualification](PROTO_PINNING.md#sdk-pins-and-qualification).
`Cargo.toml` / `Cargo.lock` own the installed pins.

## Native configuration

```rust,no_run
use libtinkle::{Client, ClientConfig, ClientInfo, InMemoryCursorStore, ServerTrust, TrustSource};

// Provision these public trust values through a trusted application channel.
let trust = ServerTrust {
    server_public_key: provisioned_pin,
    server_key_id: provisioned_key_id,
    scope: provisioned_scope,
};
let client = Client::new(
    ClientConfig::new("gateway.example.com", ClientInfo::new(
        device_model, platform, os_version, app_version, app_build, Default::default())?),
    TrustSource::Bootstrap(trust),
    InMemoryCursorStore::new(),
);
client.connect().await?;
```

`ClientConfig` fields:

| Field | Default / meaning |
|---|---|
| `host: String` | Bare hostname or IP, required; no URL |
| `client_info: ClientInfo` | All five device/app labels plus the entire immutable optional snapshot |
| `primary_port: u16` | `443` |
| `fallback_port: Option<u16>` | `Some(80)` |
| `connect_timeout: Duration` | 10 seconds per dial, within one 15-second establishment budget |
| `connection: syntra_runtime::ConnectionConfig` | Published runtime defaults |
| `reconnect: ReconnectPolicy` | Supervisor backoff: 500 ms base, ×2, 30-second cap, 10 attempts |

Both ports use the same host, pin, scope, key ID and mode. Only an initial TCP dial
failure permits trying the fallback port. Once a socket connects, any channel,
authentication, framing, timeout or I/O failure parks automatic reconnect. There is no
HTTP/TLS wrapper or alternate network transport. Custom ports use the same
encryption and trust checks as the defaults.

Socket selection is retained across reconnects and suspend/resume. The TCP-only
SDK does not expose its private post-ready failure category, so opaque channel
loss cannot authorize trying another port. Explicit `replace_trust_source`
resets port selection; callers can then establish afresh under that source.

## Trust and enrollment

- `TrustSource::Bootstrap(ServerTrust)` explicitly selects Bootstrap with an
  empty Core credential and no authorization key.
- `TrustSource::Returning { trust, enrollment }` requires validated ACTIVE
  material. The normal flow obtains it through activation or protected-store
  restoration. `Enrollment::from_active_status` rejects STAGED status.
- `Enrollment` is not Clone, redacts Debug and zeroizes on drop. Syntra receives
  a fresh zeroizing key owner for each establishment. Neither the application
  cache nor cursor storage persists keys. Hosts load/store the returned tuple
  in their protected platform secret store, bound to the provisioned trust.
- `replace_trust_source(next).await` retires the connection and installs a raw
  source only with identical `ServerTrust` (pin, key ID, and scope). It clears
  the application principal and retained targets, watches, and pending catch-up
  state; raw credentials do not establish a verified user identity. A different
  deployment requires a new client with a separate cache/cursor namespace.
  Normal enrollment/rotation uses the typed coordinator described below.
  Rejected Returning proof never changes to Bootstrap automatically.

The typed `Auth` facade supports SendCode, SignIn, ResumeEnrollment,
RotateEnrollment and ActivateEnrollment. Prepare a durable operation before
SignIn/rotation; persist the delivered candidate through the acknowledged secure
store; activate explicitly over pinned Bootstrap; then establish fresh Returning.
The first public unary call on every connection is InitConnect-wrapped. See
[device enrollment](docs/device-enrollment.md) for the exact staged API, storage
contract, recovery path and runnable checks.

Schema #57 merged at `2b15bb6db2e282f8f962742a800cab1f15ab54c5` and includes the
enrollment-disposition contract in the published BSR prost artifact
`0.5.0-20261107080548-dc41c2199c76.2`. Release qualification uses the published
Cargo pin and ordinary `--locked` checks in this workspace, without Buf or a
schema-candidate dependency in required CI. See [public pins](PROTO_PINNING.md).

`client.account().get_sessions()`, `revoke_session(session_id)` and
`revoke_other_sessions()` expose device-session management. UniFFI exports the
same names directly on `TinkleClient`. These calls never replay a mutation or
interpret a denied request as proof of retirement.

`logout()` fences authorization before awaiting anything, then joins retained
cleanup. Dropping a waiter leaves cleanup running. Retrying a local failure
preserves the original cache principal and cumulative remote confirmation, with
no repeated remote mutation. `session_status()` distinguishes transport
availability, unknown authorization, signed-out state and cleanup progress.
Restart-safe cleanup additionally requires a conditional secure-store host and
`cleanup == Complete`; legacy hosts report `restart_safe_store == false`.

Conditional ACTIVE restoration observes permanent retirement before attempting
Returning. Independent foreground entry and a fixed 60-second foreground timer
also observe; Returning failures never create or accelerate that work. Each
status-only encrypted connection shares the selected route/trust and has its own
five-second absolute budget. Exact current RETIRED evidence starts local-only
cleanup; unknown/error outcomes retain credentials. Expired records supply
structural lookup metadata only and cannot install a Returning key.

## Application lifecycle and synchronization

Use `start(targets)` for supervised operation, or manual `connect`, `subscribe`
and `reconnect`. `reconnect` performs one attempt; the existing supervisor alone
owns automatic retry scheduling. Trust/configuration and unverified establishment failures park
automatic reconnect until an explicit trust replacement. `suspend` closes the
connection and suppresses dialing; `resume` permits the supervisor to reconnect.
`stop`/`shutdown` release live state while keeping durable application state.

Typed/raw request interfaces, `load_history`, `load_dialogs`, `get_messages`,
`get_target_changes`, file transfers and push-driven synchronization share the
native channel. The application coordinator retains paged history continuation,
per-target journals, cache/cursor atomic commits, scope fencing, deduplication
and bounded catch-up. Consume `next_event()` and inspect `target_cursor`,
`target_status` and `tracked_targets`. Unsafe mutations are never auto-replayed.
See [reconnect/sync](docs/reconnect-sync.md) and [mobile lifecycle](docs/mobile-lifecycle.md).

## Swift and Kotlin

`ClientConfigFfi { client_info, host, primary_port, fallback_port }` uses bare TCP addresses.
`ServerTrustFfi { server_public_key, server_key_id, scope }` carries public trust.
`TrustSourceFfi.bootstrap(trust)` produces the one-use provisioning source.
`FfiSecureEnrollmentStore` supplies acknowledged atomic platform persistence;
opaque `EnrollmentOperation` and `StoredEnrollment` objects guard the staged
flow. Activation or `restoreActiveEnrollment` installs Returning only from
validated ACTIVE records. Hosts must erase their foreign secret byte copies.

Check binding parity with `bash scripts/generate-bindings.sh --check`; regenerate
with `bash scripts/generate-bindings.sh --write` against the published schema pin.
See [platform integration](docs/platform-ios-android-wasm.md).

## Logout

`logout().await` fences the Client permanently, erases SDK-owned key/capability
cells, joins liveness work, and clears every bound protected namespace through
the acknowledged `SecureEnrollmentStore::clear` callback. Late enrollment
responses cannot revive it. `Ok(true)` confirms remote revocation; `Ok(false)`
means remote revocation is unconfirmed but local durable clearing succeeded.
Cleanup errors leave `is_signed_out()` true and are never a durable-logout ack.
Resolve failed host invalidation before allowing restoration after restart.
Use a new Client for deliberate sign-in. The
[high-level Logout contract](docs/device-enrollment.md#local-sign-out-and-remote-revocation)
requires a single remote-eligibility budget of up to three seconds, captured before fencing
or lifecycle waits, retained without renewal on join/retry. Local durable clearing
is independent and may outlast three seconds. The
[scoped paired result](PROTO_PINNING.md#sdk-pins-and-qualification) does not imply
a global three-second wall-clock guarantee for `logout()`.
Raw calls retain their selected timeout; see the
[request-budget integration contract](docs/architecture.md#request-budget-integration-contract).

## Portable schema checks

Browser networking is unsupported. `libtinkle-web` has no dependency on the
native client. It exports `network_supported() == false`, an explicit unsupported
`tinkle_connect()`, and portable connectivity schema validation. Portable schema
checks remain available in `tinkle-schema`.

## Checks

```sh
cargo test --locked --workspace --all-features --verbose
cargo clippy --locked --workspace --all-targets --all-features --no-deps -- -D warnings
cargo fmt --all -- --check
bash scripts/generate-bindings.sh --check
cargo check --locked -p libtinkle-web -p tinkle-schema --target wasm32-unknown-unknown
```

`scripts/test-schema-candidate.sh` is optional tooling for future-schema
development, not release qualification. See [public pinning](PROTO_PINNING.md).

Rust integration fixtures use the published SDK for encrypted native TCP,
including trust failures and configurable primary/fallback ports. Swift/Kotlin
consumer tests use the host-only `native-fixture` example, also built on that
SDK. `scripts/with-native-fixture.py <consumer command...>` starts it on the host
and cleans it up. Its separate loopback JSON control socket allocates fixtures
and reads application request logs; it is test harness code only.

For host tests, use `--port 0` to avoid fixed-port collisions in Linux's ephemeral
range. The wrapper passes the OS-assigned control port to the consumer through
`TINKLE_NATIVE_FIXTURE_CONTROL_PORT`, which `crates/libtinkle/tests/mobile_fixture.rs`
reads. Omit `--port` for Swift/Kotlin native-distribution consumers, which retain
the fixed default 48080; see [native consumer fixtures](docs/platform-ios-android-wasm.md#native-consumer-fixtures).

```sh
python3 scripts/with-native-fixture.py --port 0 cargo test --locked -p libtinkle --test mobile_fixture -- --ignored
```

The native examples `send-message` and `subscribe` require `TINKLE_HOST`,
`TINKLE_SERVER_PIN` (64 hex characters), `TINKLE_SERVER_KEY_ID`, `TINKLE_SCOPE`,
all five `TINKLE_DEVICE_MODEL` / `TINKLE_PLATFORM` / `TINKLE_OS_VERSION` /
`TINKLE_APP_VERSION` / `TINKLE_APP_BUILD`, and `TINKLE_SECURE_STORE_HELPER`.
Optional `TINKLE_PORT` accepts a nonzero TCP port (1–65535); an explicit port
disables fallback. Without it, the examples use primary 443 and fallback 80,
available only after an initial primary dial failure before socket selection.
The `TINKLE_SECURE_STORE_HELPER` executable receives `load-active` and returns the SDK's opaque
record from protected platform storage on stdout. It must also implement `clear`,
returning exit status zero only after full durable namespace invalidation. These
examples do not perform enrollment writes. Production hosts use the Rust or FFI
callback contract directly.
