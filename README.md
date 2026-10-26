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
UniFFI / web surface. Two deliberate exceptions: cancellation
(`Client::call_raw_cancellable` takes `&syntra_runtime::CancelToken`,
re-exported as `libtinkle::CancelToken`, so apps need not add a direct
`syntra-runtime` dependency), and `TinkleError::Syntra` transparently
wraps `syntra_core::error::SyntraError` (with `ErrorCode` re-exported as
`libtinkle::ErrorCode`, so apps need not add a direct `syntra-core`
dependency).

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
   ignored; `call_cancellable` with `CancelToken`.
9. Error translation — 17-code registry, unknown future codes map to
   `UNKNOWN` with numeric preserved, branch on code not text.
10. Notification reception — `request_id==0`, never completes an RPC,
    bounded queue, pre-ACTIVE ignored with diagnostic.
11. Reconnection — fresh handshake + fresh ids, `UNAVAILABLE` with unknown
    outcome, bounded backoff, explicit resubscribe.
12. Tinkle event decoding — `tinkle-schema::sync` single-decoder
    journal validation (`validate_changes_page`: `accept_seq`
    strictly ascending within a page, `last_accept_seq` must cover the
    page, edited messages carry bodies with matching ids, removals carry
    none, empty pages only with explicit completion), span-aware dedup
    (`accept_seq` equal tokens fall back to `edit_date`), gap/stale rules,
    floor/head checks.

The generic Syntra implementation is independent of Tinkle types wherever
practical; `syntra-core` never imports `tinkle-schema`. Syntra internals
use prost 0.13.5 while the Tinkle boundary (`tinkle-schema`, `libtinkle`)
uses prost 0.14; payload bytes cross as opaque `Vec<u8>`, so the two prost
versions never mix at the type level.

## Public client API (Phase 3)

`libtinkle::Client` provides supervised `start` (connect + subscribe +
supervisor owning recovery/reconnect) plus manual `connect` / `subscribe` /
`reconnect` primitives, `call_raw`, typed facades (`messages()`, `auth()`,
`updates()`), demand-driven `next_event`, snapshot
`load_history` / `load_dialogs`, reconciliation RPCs
(`get_messages` / `get_target_changes`), and `target_cursor` /
`target_status` / `tracked_targets` queries. Method bindings
(`tinkle-schema::methods::ALL_METHODS`) are verified against the BSR
service definitions backing the prost crates — no manual duplication of
all signatures. Representative typed helpers (`SendMessage`, `GetHistory`,
`GetDialogs`, `GetMessages`, `GetTargetChanges`) demonstrate the encode →
`RpcRequest` → `request_id` → send → await `RpcResult` → decode →
typed-error path; all other methods use the same generic `call_typed` path.

Demand-driven contract: consume `next_event()`; refresh on
`ConversationChanged` / `MessagesChanged` (journal pages are applied and
checkpointed atomically by the coordinator — receipt alone needs no host
action). Poll `target_cursor` for authoritative UI progress and
`target_status` (`Idle`/`Reconciling`/`Failed`) for demand state instead of
inferring it from events. History/dialog snapshots live outside the journal
and never move accept positions. Recovery reconciles per-target
`GetTargetChanges` journals over Syntra by default (gRPC opt-in), with
authoritative `more_available`, per-target `ReconcileFailed` parking, and
`reconcile_now()` retry. Opaque notification hints and reconnects drive
reconciliation; there is no replay cursor and no rebuild path.

UniFFI (`tinkle-ffi::TinkleClient`) and browser (`libtinkle-web::
TinkleWebClient`) expose the same flow: `start`/`subscribe`, `next_event`,
`get_messages`/`get_target_changes`, `load_history`/`load_dialogs` (with
cache/network policy), `target_cursor`/`target_status`/`tracked_targets`,
 `reconcile_now`, `suspend`/`resume`; teardown is `stop`/`shutdown` on
 `tinkle-ffi` while the browser wrapper exposes `shutdown`, not `stop`.
Structured errors preserve codes across the boundary (`Auth`, `Retryable`
— never collapsed to a network string).

## Concurrency (Phase 4)

Multiple concurrent requests with out-of-order completion (correlated by
`request_id` only), bounded outstanding (default 64, minimum 16),
per-request cancellation tokens, per-request timeouts, graceful shutdown,
and unknown-outcome reporting (`UNAVAILABLE`/`DEADLINE_EXCEEDED` with
`is_unknown_outcome()`). Unsafe mutations are never auto-retried; retry
requires idempotency or an application operation key (`random_id` for
sends) with fresh transport correlation and bounded backoff.

## Realtime (Phase 5)

`ObserveConnectivity` over Syntra (not gRPC),
`ConnectivityChanged` single-decoder validation, per-conversation
`accept_seq` dedup (absent ⇒ order by date only), journal paging with
`more_available` completion, `GetMessages` hydration (chunked ≤100,
`edit_date` freshness), opaque-hint-driven reconciliation (sweep tracked
targets, single-flight per target — never skip), per-target failure
parking with retry, reconnect re-authentication + explicit resubscribe,
supervisor-owned recovery with per-target demand + backoff, and durable
accept-position persistence through the injected `CursorStore` trait
(`InMemoryCursorStore` default; SQLite/Keychain/DataStore in production,
IndexedDB stub on web). Persistent user state lives outside the generic
Syntra core.

## Platforms (Phase 6)

See `docs/platform-ios-android-wasm.md`. Native uses tokio +
tokio-tungstenite + rustls; browser uses `ws_stream_wasm` with identical
protocol logic. No browser-only APIs in native code; no `Authorization`
headers on browser WebSockets — authentication uses the approved Syntra
handshake. Async runtime and cancellation cross platform boundaries via
`tokio` (native) / `wasm-bindgen-futures` + `syntra_runtime::CancelToken`
(re-exported as `libtinkle::CancelToken`) on all targets. Mobile
bridging: Swift `Task.cancel()` / Kotlin coroutine cancellation maps to
`CancelToken::cancel()` — the platform adapter owns a `CancelToken` per
cancellable call and invokes `cancel()` when the Swift task or Kotlin
coroutine is cancelled, which sends best-effort `Cancel{}` for the
allocated request id (completion wins races).

## Security (Phase 7)

See `docs/security.md`. `wss://` required in production; `ws://` is
loopback/isolated-dev only with explicit opt-in. Credentials are redacted
from `Debug`, zeroized on drop, never logged (generated debug output can
leak them), never in URLs/query/headers, never in panic reports or event
callbacks, never cloned beyond the handshake. No Signal cryptography is
implemented; E2EE ciphertext, if added later, is constructed at the
application layer before entering Syntra.

## Mobile lifecycle (Phase 8)

See `docs/mobile-lifecycle.md`. Supervised `start(rooms, watch)` (or
foreground `connect` + `subscribe` on the manual path), background
`suspend` (graceful close, keep cursors; supervisor blocked until
`resume`), `reconnect`/`resume` with backoff + resubscribe,
`rotate_credential` on token expiry, durable store survives process
 termination/app restart. No permanent background WebSocket is assumed;
 push notifications trigger `PushManager::process_push` (reconnect +
 resubscribe from persisted accept positions). Hosts drive `next_event` and
poll `target_cursor`/`target_status`; `read_state_pending` on dialogs
means refresh via `load_dialogs(HydrateCache)`.

## Testing (Phase 9)

```sh
cargo test --workspace
cargo test -p libtinkle --test client_integration -- --nocapture
```

- `tinkle-schema/tests/sync_conformance.rs` — journal page grammar,
  accept_seq decisions, fetch chunking, body freshness, target keys,
  connectivity-only method inventory.
- `libtinkle/tests/client_integration.rs` — live in-process mock gateway:
  handshake/auth, concurrent out-of-order RPCs, cancellation, opaque-hint
  reconciliation, journal paging + `more_available` tri-state, history
  healing of journal skips, reconnect + journal recovery, errors,
  malformed-frame tolerance. Syntra protocol conformance lives upstream in the published
  crates; these integration tests cover only the Syntra paths exercised by
  libtinkle, not the complete upstream suite.
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
