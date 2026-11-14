# Native integration

## UniFFI C ABI

`crates/tinkle-ffi` adapts the Rust client to UniFFI `0.31.2`.
The repository provides source; consumers build libraries exposing UniFFI
functions and metadata.
Objects cross as opaque reference-counted handles; records and errors use
UniFFI serialization. Async calls return future
handles with poll, completion, cancellation, and release functions. Foreign
traits use registered callback tables. Consumer generators supply their own
declarations and language runtime.

Consumers own language binding generation, initialization, handle/buffer release,
callback thread dispatch, and cancellation integration. Pin the exact source
revision and a compatible UniFFI generator/runtime. Generate from an unstripped
library built at that revision before packaging or stripping it. Consumer
builds select the library type, targets, linker settings, and native dependencies.
Keep unwind containment enabled when relying on UniFFI's panic-to-error handling.
Generate bindings and build the library together so their serialized types and
calling conventions agree.

Rust-side adapter tests run with:

```sh
cargo test --locked -p tinkle-ffi
```

These tests call the Rust adapters directly. Consumers verify their generated
bindings, native linking, handle/buffer ownership, async operations, and foreign
callbacks through the actual language boundary.

## Host contracts

Call the exported `tinkle_ffi_init` at process startup. It is idempotent and
creates the process-wide Tokio runtime. `TinkleClient` wraps the same Rust core
for application RPCs, history, journal reconciliation, transfers, and push wakes.

Provision `ServerTrustFfi` independently of address selection. Configuration
requires `ClientInfoFfi` and uses bare TCP addresses with default ports 443/80.
Both ports share the same trust checks. Bootstrap is explicit; Returning is
installed only through activation or validated protected-store restoration.

Supply `FfiSecureEnrollmentStore` over confidential durable platform storage.
Acknowledging a callback confirms the specified atomic transaction, not an
asynchronous enqueue. Host copies of secret bytes must be erased. See
[device enrollment](device-enrollment.md) for the complete storage, restoration,
retirement-observation, and cleanup contracts.

Use `SqliteStorage` for shared cache/cursor commits, or supply `FfiCursorStore`.
Authorization keys must not enter the application database. Hosts supply
`FfiPlatformMediaPreparer` for platform codecs and retain each invocation's
input/output leases until native work actually completes. These implementations
and their platform security/codec qualification belong to the consumer.

The account-state subscription and synchronization-event stream are independent.
Stop subscriptions and release handles when their owners finish. Logout fences
authorization immediately and joins retained cleanup; cancelling a foreign waiter
does not cancel independently owned logout cleanup.

## Consumer qualification

Consumers own target support, minimum OS versions, native library compilation,
export checks, packaging, debug symbols, and simulator/device execution. Platform
storage security, codec behavior, and production-server acceptance require their
own evidence. Rust tests in this repository do not establish those results.

## Rust transport fixtures

`scripts/with-native-fixture.py` builds a host-only Rust fixture using the
published Syntra SDK, starts its loopback control socket, and stops it when the
consumer command exits. Use an OS-assigned control port for host tests:

```sh
python3 scripts/with-native-fixture.py --port 0 cargo test --locked \
  -p libtinkle --test mobile_fixture -- --ignored
```

The wrapper passes the allocated port in `TINKLE_NATIVE_FIXTURE_CONTROL_PORT`.
The default control port is 48080. The control socket allocates encrypted
listeners and inspects fixture request logs; application proof, encryption, and
RPC framing use the published SDK. Fixture replies are test authority only.

## Browser

Browser networking is unsupported. `libtinkle-web` provides portable schema
checks without a native runtime dependency. `network_supported()` returns false
and `tinkle_connect()` returns an explicit unsupported error.

```sh
cargo check --locked -p libtinkle-web -p tinkle-schema --target wasm32-unknown-unknown
```
