# Architecture

Ownership follows `syntra/specifications/architecture.md` (SYN-ARCH-010–013):
each layer has exactly one owner and must not absorb another's duties.

```text
Tinkle application          methods, payload schemas, auth policy,
                            subscription interests, cursors, E2EE (yours)
       ↓
libtinkle client            typed dispatch, subscriptions, reconnect,
                            backoff, cursor persistence, gRPC catch-up (this SDK)
       ↓
tinkle-schema                  prost + tonic bindings + sync
                             validation (no transport)
       ↓
Syntra RPC / Events         correlation, deadlines, cancel, notifications
       ↓
syntra-core                 envelope, handshake, sessions, errors,
                            versions (no application semantics)
       ↓
syntra-ws                   WSS/WS adapter: Upgrade, subprotocol,
                            binary framing, Ping/Pong, close codes
       ↓
Network                     tokio-tungstenite + rustls (native) /
                            browser WebSocket (wasm)
```

## Crates

- `syntra-core`: `Frame` codec, `SNTR-WIRE-*`/`SNTR-SER-*` validation,
  `SNTR-HS-001` six-state machine, version negotiation, `request_id`
  allocation, bounded pending correlation, per-RPC deadlines,
  best-effort cancel, typed errors, notification routing. Never parses
  Tinkle bytes, never decrypts ciphertext. Runtime-agnostic validation
  plus a `runtime-tokio` connection engine.
- `syntra-ws`: `SNTR-WS-V1` / `SNTR-WSS-H1-V1` adapters. One binary
  message = one `Frame`, no length prefix, binary only, `syntra.v1`
  subprotocol, `/syntra/v1/ws` path, Ping/Pong liveness, close-code
  mapping, TLS 1.3 verification (native) / `ws_stream_wasm` (browser).
- `tinkle-schema`: `tinkle.v1` prost + tonic types, 179 canonical method identifiers
  (`/tinkle.v1.<Service>/<Method>`), per-target journal validation
  (`validate_changes_page`: strict `accept_seq` ascent, `last_accept_seq`
  cursor discipline, edited-carries-body vs deleted/revoked-identity-only),
  `accept_seq` ranking (`accept_seq_decision`; absent tokens never vote),
  body freshness (`fresher_body` by `edit_date`), request batching
  (`chunk_message_ids`, `MAX_MESSAGE_IDS_PER_REQUEST`,
  `MAX_TARGET_CHANGES_LIMIT`), and canonical target keys/constructors
  (`target_key_for`, `target_from_parts`). `MessagesService`:
  `GetTargetChanges` (journal pages) and `GetMessages` (body hydration) as
  unary RPCs; `UpdatesService`: `ObserveConnectivity` (ephemeral presence).
- `libtinkle`: `Client` (supervised `start`/`stop` plus manual
  `connect`/`subscribe`/`reconnect`/`suspend`/`resume`, `next_event`,
  `get_messages`/`get_target_changes`, `reconcile_now`,
  `target_cursor`/`target_status`/`tracked_targets`,
  `load_history`/`load_dialogs`), `Credential` (redacted + zeroized),
  `CursorStore` injection, backoff policy, typed facades (`messages()`,
  `auth()`, `updates()`), optional direct-unary `grpc_endpoint`,
  structured `TinkleError` with code-based mapping. This is the ONLY
  public Rust API surface (plus `SyncEvent`/`SyncManager`/
  `ReconcileStatus` for explicit-store apps and `SqliteCursorStore` on
  native); internal modules are `pub(crate)` with re-exports providing
  the public paths.
- `tinkle-ffi` (public FFI): UniFFI `TinkleClient` object (`start`,
  `subscribe`, `next_event`, `get_messages`/`get_target_changes`,
  `reconcile_now`, `target_cursor`/`target_status`/`tracked_targets`,
  `load_history`/`load_dialogs` with `FfiLoadPolicy`, `suspend`/
  `resume`/`is_suspended`, `is_connected`/`pending_len`, `stop`/`shutdown`)
  + structured `FfiError` (`Auth`, `Retryable` alongside `Network`/`Sync`/
  `Codec`/`Storage`/…) + `ClientConfigFfi` (fail-fast `grpc_endpoint`
  validation for the direct-unary transport) + `FfiCursorStore` foreign
  trait over a shared process-wide Tokio runtime. Swift/Kotlin implement
  storage natively (or share native SQLite via `SqliteStorage`); no Syntra
  type crosses FFI.
- `libtinkle-web` (public web): `wasm-bindgen` `TinkleWebClient` over
  `libtinkle` (`connect`/`start`/`subscribe`/`next_event`/
  `get_messages`/`get_target_changes`/`reconcile_now`/`target_cursor`/
  `target_status`/`tracked_targets`/`load_history`/`load_dialogs` +
  lifecycle) with an `IndexedDB CursorStore` stub (in-memory today, TODO
  real IndexedDB); transport resolves to the `syntra-ws` wasm adapter.
  Compile-verified; browser runtime smoke awaits the `spawn_local` pump
  adaptation (documented, not claimed).

## Visibility (TDLib rule)

Workspace-owned crates (`tinkle-schema`, `libtinkle`, `tinkle-ffi`,
`libtinkle-web`) are `publish = false` implementation details, and the
whole workspace is git-only distributed (crates.io rejects our
buf-registry BSR dependencies). `syntra-core`/`syntra-runtime`/`syntra-ws`
are crates.io dependencies, not workspace crates covered by that statement.
Apps depend only on `libtinkle` (Rust),
`tinkle-ffi` (Swift/Kotlin via UniFFI), or `libtinkle-web` (browser).
`Frame`, `Connection`, `RpcRequest`/`RpcResult`, `request_id`,
`WsConfig`/`WsHandle`, `ALL_METHODS`, and validators never appear in
public signatures. Two deliberate exceptions: `Client::call_raw_cancellable`
publicly takes `&syntra_runtime::CancelToken` (re-exported as
`libtinkle::CancelToken`, so apps need not add a direct `syntra-runtime`
dependency — `syntra-runtime` remains a public dependency of `libtinkle`
for cancellable calls), and `TinkleError::Syntra` transparently wraps
`syntra_core::error::SyntraError` (mapped to `FfiError` at the FFI
boundary; `TinkleError::code()` returns `syntra_core::error::ErrorCode`,
re-exported as `libtinkle::ErrorCode` so apps need not add a direct
`syntra-core` dependency).

Syntra comes from crates.io (`syntra-core`/`syntra-runtime`/`syntra-ws`
`=1.0.0-rc.1`); the local `crates/syntra-core` and `crates/syntra-ws`
prototypes were removed.

## Data flow

Send: typed request → prost bytes → `RpcRequest{method, payload,
timeout_ms}` → `Frame{version, request_id≠0, request}` → WS binary →
host. Receive: WS binary → `Frame` → validate → `result` completes the
pending RPC by id (notifications interleave without completing) →
prost-decode typed response or map `RpcError{code,message}`.

Sync: `subscribe(Vec<Target>)` registers interests (not reconciliation) →
notification hints and reconnects drive per-target `GetTargetChanges`
journal pages → validate (`validate_changes_page`) → `accept_seq` dedup →
hydrate via `GetMessages` (chunked, `edit_date` freshness) → commit cache +
accept positions atomically (native SQLite path) and emit
`ConversationChanged`/`MessagesChanged`. `more_available` is authoritative
for page completion; failures park per-target (`ReconcileFailed` +
`Failed` status, retried via `reconcile_now`). Demand surfaces via
`target_status`/`target_cursor`/`tracked_targets`, not event inference.
Connectivity is ephemeral. There are no replay cursors, no
`max_replay_distance`, and no rebuild path.
