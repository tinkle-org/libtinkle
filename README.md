# libtinkle — Tinkle client runtime over Syntra

A reusable Rust client runtime for the Tinkle `client-api` over Syntra,
consuming Syntra protocol types from the published crates.io crates
(`syntra-core`, `syntra-runtime`, `syntra-ws`) and BSR-generated
`tinkle-proto` application messages, implementing application behavior in
this SDK (generated bindings alone are not an SDK).

The same logical client runtime is reusable across iOS, Android, and
WebAssembly/browser via thin platform adapters. No gRPC client is
required for standard Syntra application calls. Protocol and application
boundaries remain distinct.

## Workspace audit (Phase 1)

This directory was empty on 2026-10-20; no prior Cargo workspace,
transport dependencies, protocol crates, native bindings, iOS/Android
integration, WASM configuration, or test architecture existed. The layout
below follows the existing naming and architecture conventions of the
sibling repositories (`syntra/`, `tinkle-proto/`):

```text
libtinkle/
  Cargo.toml                  workspace (workspace-owned crates publish=false; git tags + GitHub releases only; syntra-core/runtime/ws from crates.io)
  .cargo/config.toml          public BSR Cargo index (no token for public modules)
  crates/tinkle-schema/       PRIVATE Tinkle bindings + realtime validation (publish=false)
  crates/libtinkle/           PUBLIC Rust API: Client, Credential, CursorStore, typed facades
  crates/tinkle-ffi/          PUBLIC UniFFI: TinkleClient object for Swift/Kotlin
  crates/libtinkle-web/       PUBLIC wasm-bindgen wrapper for browsers
  docs/                       architecture, platforms, lifecycle, security
  PROTO_PINNING.md            BSR crate versions + upgrade
```

Apps never depend on the Syntra crates (`syntra-core`/`syntra-runtime`/`syntra-ws`
from crates.io) or `tinkle-schema` directly — same rule as TDLib, where
MTProto internals never leak and clients speak only the stable `Client` /
UniFFI / web surface.

## Quick start

```rust
let client = libtinkle::Client::new(
    libtinkle::ClientConfig::new("wss://gateway.example.com/syntra/v1/ws"),
    libtinkle::Credential::from_session_token(&token),
    libtinkle::InMemoryCursorStore::new(),
);
client.connect().await?;
let resp = client.messages().send_message(req).await?;
```

Run the examples against the Tinkle test gateway:

```sh
TINKLE_URL=wss://gateway.test.example.com/syntra/v1/ws \
TINKLE_TOKEN=<session-token> \
cargo run -p libtinkle --example send-message -- 7 "hello"

TINKLE_URL=wss://gateway.test.example.com/syntra/v1/ws \
TINKLE_TOKEN=<session-token> \
cargo run -p libtinkle --example subscribe
```

## Protocol layers (Phase 2)

Syntra comes from published crates.io crates (`syntra-core`,
`syntra-runtime`, `syntra-ws`, all `=1.0.0-rc.1`); the local prototype
crates were removed. `syntra-core` implements each layer separately, per
the specifications in `../syntra/specifications/`:

1. WSS transport adapter — `syntra-ws::native` (tokio-tungstenite +
   rustls TLS 1.3) and `syntra-ws::wasm` (browser WebSocket, same
   one-message-one-Frame rule).
2. Syntra binary Frame codec — `syntra-core::codec` (prost, 1 MiB bound,
   reserved 16/17 rejection, empty-payload semantics).
3. Connection state machine — `syntra-core::state` (exactly six states,
   no shortcuts, no re-handshake on one transport).
4. ClientHello/ServerReady handshake — `syntra-core::handshake`
   (version overlap, 5 s deadline, opaque `connection_id`).
5. Request ID allocation — `syntra-core::ids` (nonzero u64, outstanding
   uniqueness, fresh space per connection).
6. Pending RPC correlation — `syntra-core::pending` (bounded, duplicate
   rejection, unknown-result ignore, transport-loss drain).
7. Request timeouts — per-RPC budget (`timeout_ms==0` selects bounded
   host default), initiator timer ≤ budget, `DEADLINE_EXCEEDED`.
8. Cancellation — best-effort `Cancel{}`, completion wins, unknown ids
   ignored; `call_cancellable` with `CancellationToken`.
9. Error translation — 17-code registry, unknown future codes map to
   `UNKNOWN` with numeric preserved, branch on code not text.
10. Notification reception — `request_id==0`, never completes an RPC,
    bounded queue, pre-ACTIVE ignored with diagnostic.
11. Reconnection — fresh handshake + fresh ids, `UNAVAILABLE` with unknown
    outcome, bounded backoff, explicit resubscribe.
12. Tinkle event decoding — `tinkle-schema::realtime` (single-decoder
    `RealtimeNotification`, complete-batch validation, cursor tracking).

The generic Syntra implementation is independent of Tinkle types wherever
practical; `syntra-core` never imports `tinkle-schema`. Syntra internals
use prost 0.13.5 while the Tinkle boundary (`tinkle-schema`, `libtinkle`)
uses prost 0.14; payload bytes cross as opaque `Vec<u8>`, so the two prost
versions never mix at the type level.

## Public client API (Phase 3)

`libtinkle::Client` provides `connect`, `call_raw`, typed facades
(`messages()`, `auth()`, `realtime()`), and `subscribe`/`next_event`.
Method bindings (`tinkle-schema::methods::ALL_METHODS`, 169 entries) are
verified against the BSR service definitions backing the prost crates
— no manual duplication of all signatures. Representative
typed helpers (`SendMessage`, `GetHistory`, `GetDialogs`, `SubscribeBatched`)
demonstrate the encode → `RpcRequest` → `request_id` → send → await
`RpcResult` → decode → typed-error path; all other methods use the same
generic `call_typed` path.

## Concurrency (Phase 4)

Multiple concurrent requests with out-of-order completion (correlated by
`request_id` only), bounded outstanding (default 64, minimum 16),
per-request cancellation tokens, per-request timeouts, graceful shutdown,
and unknown-outcome reporting (`UNAVAILABLE`/`DEADLINE_EXCEEDED` with
`is_unknown_outcome()`). Unsafe mutations are never auto-retried; retry
requires idempotency or an application operation key (`random_id` for
sends) with fresh transport correlation and bounded backoff.

## Realtime (Phase 5)

`SubscribeBatched`/`UnsubscribeBatched` dispatcher identifiers (not gRPC),
`RealtimeNotification` single-decoder validation, complete-batch grammar
(1–4 ordered events), `(stream, cursor)` whole-batch dedup, gap/stale
handling (stop, retain checkpoint, never skip), reset signals (head
diagnostic only), reconnect re-authentication + explicit resubscribe, and
durable cursor persistence through the injected `CursorStore` trait
(`InMemoryCursorStore` default; SQLite/Keychain/DataStore in production).
Persistent user state lives outside the generic Syntra core.

## Platforms (Phase 6)

See `docs/platform-ios-android-wasm.md`. Native uses tokio +
tokio-tungstenite + rustls; browser uses `ws_stream_wasm` with identical
protocol logic. No browser-only APIs in native code; no `Authorization`
headers on browser WebSockets — authentication uses the approved Syntra
handshake. Async runtime and cancellation cross platform boundaries via
`tokio` (native) / `wasm-bindgen-futures` + `CancellationToken` (all).

## Security (Phase 7)

See `docs/security.md`. `wss://` required in production; `ws://` is
loopback/isolated-dev only with explicit opt-in. Credentials are redacted
from `Debug`, zeroized on drop, never logged (generated debug output can
leak them), never in URLs/query/headers, never in panic reports or event
callbacks, never cloned beyond the handshake. No Signal cryptography is
implemented; E2EE ciphertext, if added later, is constructed at the
application layer before entering Syntra.

## Mobile lifecycle (Phase 8)

See `docs/mobile-lifecycle.md`. Foreground `connect`, background
`suspend` (graceful close, keep cursors), `reconnect`/`resume` with
backoff + resubscribe, `rotate_credential` on token expiry, durable store
survives process termination/app restart. No permanent background
WebSocket is assumed; push notifications trigger `sync_after_wake`
(reconnect + resubscribe from persisted cursors).

## Testing (Phase 9)

```sh
cargo test --workspace
cargo test -p libtinkle --test client_integration -- --nocapture
```

- `tinkle-schema/tests/realtime_conformance.rs` — batch grammar, UUID,
  position order, cursor/floor/reset vectors.
- `libtinkle/tests/client_integration.rs` — live in-process mock gateway:
  handshake/auth, concurrent out-of-order RPCs, cancellation, notification
  interleaving, reconnect + cursor recovery, errors, malformed-frame
  tolerance. Syntra protocol conformance lives upstream in the published
  crates and is validated here transitively through these integration tests.
- Platform compilation: native `cargo check --workspace`; wasm via
  `cargo check -p libtinkle-web --target wasm32-unknown-unknown` (requires the
  target + `ws_stream_wasm`; native never touches browser APIs; the
  crates.io `syntra-ws` dependency builds transitively).

## Acceptance

- Rust SDK calls Tinkle RPCs over Syntra (typed + raw paths tested).
- Same logical runtime across iOS/Android/WASM (platform adapters wrap
  this core; see docs).
- No gRPC client for standard Syntra calls.
- Protocol/application boundaries distinct (`syntra-core` ↔ `tinkle-schema`).
- Reconnect + sync safe and documented (`docs/reconnect-sync.md`).

## Deliverables

Runtime (`syntra-core`, `syntra-ws`, `tinkle-schema`, `libtinkle`), transport
adapters, typed client + 169 method descriptors, tests (50 cases),
platform/lifecycle/security docs, and runnable examples (`send-message`,
`subscribe`).
