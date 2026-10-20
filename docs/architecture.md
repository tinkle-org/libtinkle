# Architecture

Ownership follows `syntra/specifications/architecture.md` (SYN-ARCH-010–013):
each layer has exactly one owner and must not absorb another's duties.

```text
Tinkle application          methods, payload schemas, auth policy,
                            subscription interests, cursors, E2EE (yours)
       ↓
libtinkle client            typed dispatch, subscriptions, reconnect,
                            backoff, cursor persistence (this SDK)
       ↓
tinkle-schema                  prost bindings + realtime batch/cursor
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
- `tinkle-schema`: `tinkle.v1` prost types, 169 canonical method identifiers
  (`/tinkle.v1.<Service>/<Method>`), `RealtimeNotification`
  single-decoder validation, complete-batch grammar, `(stream, cursor)`
  dedup, gap/stale/reset rules, `position_order`, floor/head checks.
- `libtinkle`: `Client` (connect/call/subscribe/reconnect/suspend),
  `Credential` (redacted + zeroized), `CursorStore` injection,
  backoff policy, typed facades (`messages()`, `auth()` + crate-internal
  realtime dispatch). This is the ONLY public Rust API; internal modules
  are `pub(crate)`, `SubscriptionManager` never leaves the crate.
- `tinkle-ffi` (public FFI): UniFFI `TinkleClient` object + `FfiError` +
  `FfiCursorStore` foreign trait over a shared process-wide Tokio runtime.
  Swift/Kotlin implement storage natively; no Syntra type crosses FFI.
- `libtinkle-web` (public web): `wasm-bindgen` wrapper over `libtinkle`
  for browsers; transport resolves to the `syntra-ws` wasm adapter.

## Visibility (TDLib rule)

`syntra-core`, `syntra-ws`, `tinkle-schema` are `publish = false`
implementation details. Apps depend only on `libtinkle` (Rust),
`tinkle-ffi` (Swift/Kotlin via UniFFI), or `libtinkle-web` (browser).
`Frame`, `Connection`, `RpcRequest`/`RpcResult`, `request_id`,
`WsConfig`/`WsHandle`, `ALL_METHODS`, and validators never appear in
public signatures; `TinkleError::Syntra` is mapped to `FfiError` at the
FFI boundary.

## Data flow

Send: typed request → prost bytes → `RpcRequest{method, payload,
timeout_ms}` → `Frame{version, request_id≠0, request}` → WS binary →
host. Receive: WS binary → `Frame` → validate → `result` completes the
pending RPC by id (notifications interleave without completing) →
prost-decode typed response or map `RpcError{code,message}`.

Realtime: `SubscribeBatched` confirms interests (not replay) →
`Notification{payload=RealtimeNotification}` → validate complete batch →
atomically commit effects + `final_cursor` → persist via `CursorStore`.
Presence is ephemeral; reset head is diagnostic only.
