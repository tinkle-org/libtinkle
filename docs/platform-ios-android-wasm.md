# Platform integration: iOS, Android, WebAssembly/browser

The same logical client runtime (`libtinkle::Client` + `syntra-core` +
`tinkle-schema`) is reused on all targets. Platform code wraps the async
core; it never reimplements framing, handshake, correlation, or cursor
logic. Syntra comes from crates.io (`syntra-core`/`syntra-runtime`/
`syntra-ws` `=1.0.0-rc.1`); the local prototype crates were removed.

## Runtime compatibility review (before choosing crates)

- Core validation (`frame`, `method`, `state`, `ids`, `handshake`,
  `sync`) uses `prost` + `std` only — portable to iOS, Android
  (via NDK), and `wasm32-unknown-unknown`.
- Connection engine uses `tokio::sync` + `tokio::time`. Native targets
  run full `tokio` (multi-thread or current-thread). Browser targets run
  `wasm-bindgen-futures::spawn_local` with the same state machine;
  timeouts use `wasmtimer` semantics (the `Connection` budget logic is
  clock-abstract; only the timer driver differs).
- WebSocket: native uses `tokio-tungstenite` (real sockets, masking,
  Ping/Pong, rustls). Browser uses `ws_stream_wasm` (DOM `WebSocket`,
  no raw sockets, no custom masking). Both enforce one-binary-message =
  one-`Frame`, binary only, size bounds, subprotocol checks.
- No browser-only APIs (`window`, `document`, `fetch`, `localStorage`)
  appear in native code (`syntra-ws::native`). No raw-socket or
  `std::net` blocking calls appear in `wasm.rs`. `#[cfg(target_arch =
  "wasm32")]` gates the two adapters.

## iOS (Swift)

- Expose `Client` via UniFFI (`tinkle-ffi::TinkleClient`): `start`,
  `connect`, `subscribe`, `sendMessage` (with `targetKind`/`targetId`),
  `loadHistory`/`loadDialogs` (with `FfiLoadPolicy`), `nextEvent`,
  `getMessages`/`getTargetChanges`, `reconcileNow`, `targetCursor`/
  `targetStatus`/`trackedTargets`, `suspend`/`resume`/`isSuspended`,
  `isConnected`/`pendingLen`, `stop`/`shutdown`, `setPrincipalUserId`
  (throwing). Structured errors preserve codes (`Auth`, `Retryable`).
  Async via `uniffi` async + Swift `async/await`. Targets use `Target`
  naming (`targetKind` `"user"`/`"room"`/`"chat"` + `targetId`); the
  numeric `offsetPeerId` dialog vocabulary is unchanged.
- Runtime: embed `tokio` current-thread inside the UniFFI scaffolding
  (one runtime per `Client`, driven by Swift's executor; never block the
  main thread). Cancellation: direct Rust `libtinkle` callers use
  `syntra_runtime::CancelToken` (re-exported as `libtinkle::CancelToken`),
  where `cancel()` sends best-effort `Cancel{}` for the allocated request
  id (completion wins races). At the shipped `tinkle-ffi` UniFFI boundary
  no tokens cross FFI — dropping the Swift `Task`/foreign future cancels
  the in-flight Rust future. Bridging `Task.cancel()` to an owned
  `CancelToken::cancel()` per cancellable call remains a planned change
  and is not yet in the facade.
- Storage: implement `CursorStore` over Core Data/SQLite (GRDB) or
  Keychain-backed SQLite; persist atomically with message indexes.
- Push: APNs (including VoIP for calls) wakes the app; parse with
  `parsePushPayload`, then drive `handlePushWake(envelope, timeoutSecs)`
  for the durable push flow (independent connection, authoritative
  fetch, shared-ingest commit, bounded reconcile). For foreground
  refreshes use `reconnect` + `openPushNotification` (navigate via
  `pushTargetKey`), and call `pushRecoverStaleJobs` at startup to
  requeue leases leaked by a killed extension. Do not hold a background
  WebSocket; iOS suspends it.
- TLS: rustls native roots + system roots; ATS requires `wss://` (no
  `ws://` exceptions in production).

## Android (Kotlin)

- Expose via UniFFI (`tinkle-ffi::TinkleClient`): same surface as iOS
  (`start`, `loadHistory`/`loadDialogs`, `getMessages`/`getTargetChanges`,
  `targetStatus`, structured errors, throwing setters — see above),
  Kotlin coroutines bridge `async` calls. One `tokio` runtime per process
  (shared).
  Cancellation: direct Rust `libtinkle` callers use `CancelToken::cancel()`
  for best-effort `Cancel{}`. At the shipped `tinkle-ffi` UniFFI boundary
  no tokens cross FFI — dropping the coroutine's `Job`/foreign future
  cancels the in-flight Rust future. Bridging `Job` cancellation to an
  owned `CancelToken` per cancellable call remains a planned change and is
  not yet in the facade.
- Storage: `CursorStore` over Room/SQLite + EncryptedSharedPreferences
  for the session token (never in cleartext prefs).
- Push: FCM data messages trigger `parsePushPayload` +
  `handlePushWake(envelope, timeoutSecs)` (same durable flow as iOS).
  Respect Doze; use `WorkManager` for deferred sync, not a foreground
  socket. Foreground refreshes go through `reconnect` +
  `openPushNotification`; call `pushRecoverStaleJobs` at startup.
- TLS: rustls native roots; Network Security Config pins `wss://` only.

## WebAssembly/browser

- Build: `cargo build --target wasm32-unknown-unknown` with
  `wasm-bindgen` + `wasm-bindgen-futures`. Transport uses
  `ws_stream_wasm::WsMeta::connect(url, [SUBPROTOCOL])`.
- Browsers forbid arbitrary `Authorization` headers on WebSockets;
  authentication uses the approved Syntra handshake
  (`ClientHello.credential` → `ServerReady`), never headers or query.
- `Origin` is defense-in-depth (server allowlist), never authentication.
- Storage: `IndexedDbCursorStore` stub today (in-memory map with
  principal/stream isolation; TODO real `indexed-db-futures` persistence),
  or `localStorage` (cursors only, never secrets). Secrets live in memory
  (and `CryptoKey` unwrapped per session where needed).
- Surface: `TinkleWebClient` (`new`, `connect`/`start`/`subscribe`,
  `next_event` as JSON (`conversation_changed`/`messages_changed`/
  `conversation_removed`/`reconcile_failed`/`connectivity`/
  `checkpoint_failed`), `get_messages`/`get_target_changes` returning
  `Uint8Array` protobuf bytes, `reconcile_now`, `target_cursor`/
  `target_status`/`tracked_targets`, `load_history`/`load_dialogs`
  returning `Uint8Array` protobuf bytes, `suspend`/`resume`/`reconnect`/
  `shutdown`) via `wasm-bindgen-futures`. Same demand-driven contract as
  native: refresh on `conversation_changed`/`messages_changed`, retry via
  `reconcile_now` on `reconcile_failed`; poll `target_cursor`/
  `target_status` for authoritative progress. History/dialog targets use
  `targetKind`/`targetId` naming.
- Example (target surface; runnable natively today, pending the
  `spawn_local` adaptation on `wasm32` — `start` returns an
  unsupported-runtime error in the browser until then):

```js
const client = new TinkleWebClient(url, token);
await client.set_principal_user_id(42n);
const confirmation = JSON.parse(await client.start('[{"target_kind":"room","target_id":7}]', '[]'));
const evtJson = await client.next_event(); // JSON string or undefined
await client.reconcile_now();
const historyBytes = await client.load_history("room", 7n, 50, true); // Uint8Array
const changesBytes = await client.get_target_changes("room", 7n, 0n, 100); // Uint8Array
```

## Compilation checks

```sh
cargo check -p tinkle-schema -p libtinkle -p tinkle-ffi   # native, passes (syntra-* build transitively)
cargo check -p tinkle-schema --target wasm32-unknown-unknown           # pure app, passes
cargo check --target wasm32-unknown-unknown -p libtinkle-web -p libtinkle  # wrapper + core, passes
```

WASM is compile-verified only (no browser runtime smoke claimed):
`syntra-ws` compiles against the published browser transport
(`ws_stream_wasm` via `ws_stream_wasm::WsMeta::connect`,
`wasm-bindgen-futures`, `getrandom` with `js` support, tokio sync-only
shims); the `#[cfg(target_arch = "wasm32")]` boundary already guarantees
native builds never touch browser APIs and vice versa. `Client::connect`
still drives its notification pump with `tokio::spawn`, which panics
without an active Tokio runtime on the browser executor — the
`spawn_local` adaptation is required before a browser connection can be
established, so `TinkleWebClient::connect`/`start` return an
unsupported-runtime error on `wasm32` instead of claiming success.
Browser smoke tests remain separate.
