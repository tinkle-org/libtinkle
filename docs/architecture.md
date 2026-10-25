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
- `tinkle-schema`: `tinkle.v1` prost + tonic types, 172 canonical method identifiers
  (`/tinkle.v1.<Service>/<Method>`), `Update` single-decoder validation,
  `(stream, cursor)` dedup, gap/stale/rebuild rules, `position_order`, floor/head checks.
  `UpdatesService`: `ReadUpdates` (gRPC catch-up), `FollowUpdates` and
  `ObserveConnectivity` (Syntra follow binding).
- `libtinkle`: `Client` (connect/call/subscribe/reconnect/suspend),
  `Credential` (redacted + zeroized), `CursorStore` injection,
  backoff policy, typed facades (`messages()`, `auth()`, `updates()`).
  This is the ONLY public Rust API surface (plus `SyncEvent`/`SyncManager`
  for explicit-store apps and `SqliteCursorStore` on native); internal
  modules are `pub(crate)` with re-exports providing the public paths.
- `tinkle-ffi` (public FFI): UniFFI `TinkleClient` object + `FfiError` +
  `FfiCursorStore` foreign trait over a shared process-wide Tokio runtime.
  Swift/Kotlin implement storage natively; no Syntra type crosses FFI.
- `libtinkle-web` (public web): `wasm-bindgen` wrapper over `libtinkle`
  for browsers; transport resolves to the `syntra-ws` wasm adapter.

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

Sync: `FollowUpdates` confirms interests (not replay) →
`Notification{payload=FollowUpdatesResponse}` → validate single `Update` →
apply data, then `acknowledge(stream, cursor)` persists via `CursorStore`.
Connectivity is ephemeral; rebuild head is diagnostic only.
