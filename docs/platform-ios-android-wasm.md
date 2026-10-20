# Platform integration: iOS, Android, WebAssembly/browser

The same logical client runtime (`libtinkle::Client` + `syntra-core` +
`tinkle-schema`) is reused on all targets. Platform code wraps the async
core; it never reimplements framing, handshake, correlation, or cursor
logic.

## Runtime compatibility review (before choosing crates)

- Core validation (`frame`, `method`, `state`, `ids`, `handshake`,
  `realtime`) uses `prost` + `std` only — portable to iOS, Android
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

- Expose `Client` via UniFFI (`uniffi-rs`): `connect`, `sendMessage`,
  `getHistory`, `subscribe`, `nextEvent` (callback/stream), `suspend`,
  `resume`, `close`. Async via `uniffi` async + Swift `async/await`.
- Runtime: embed `tokio` current-thread inside the UniFFI scaffolding
  (one runtime per `Client`, driven by Swift's executor; never block the
  main thread). Cancellation via `CancellationToken` bridged to Swift
  `Task.cancel()`.
- Storage: implement `CursorStore` over Core Data/SQLite (GRDB) or
  Keychain-backed SQLite; persist atomically with message indexes.
- Push: APNs (including VoIP for calls) wakes the app; call
  `sync_after_wake` (reconnect + resubscribe from persisted cursors).
  Do not hold a background WebSocket; iOS suspends it.
- TLS: rustls native roots + system roots; ATS requires `wss://` (no
  `ws://` exceptions in production).

## Android (Kotlin)

- Expose via UniFFI or `jni-rs`: same surface as iOS, Kotlin coroutines
  bridge `async` calls. One `tokio` runtime per process (shared).
- Storage: `CursorStore` over Room/SQLite + EncryptedSharedPreferences
  for the session token (never in cleartext prefs).
- Push: FCM data messages trigger `sync_after_wake`. Respect Doze;
  use `WorkManager` for deferred sync, not a foreground socket.
- TLS: rustls native roots; Network Security Config pins `wss://` only.

## WebAssembly/browser

- Build: `cargo build --target wasm32-unknown-unknown` with
  `wasm-bindgen` + `wasm-bindgen-futures`. Transport uses
  `ws_stream_wasm::WsMeta::connect(url, [SUBPROTOCOL])`.
- Browsers forbid arbitrary `Authorization` headers on WebSockets;
  authentication uses the approved Syntra handshake
  (`ClientHello.credential` → `ServerReady`), never headers or query.
- `Origin` is defense-in-depth (server allowlist), never authentication.
- Storage: `CursorStore` over IndexedDB (`indexed-db-futures`) or
  `localStorage` (cursors only, never secrets). Secrets live in memory
  (and `CryptoKey` unwrapped per session where needed).
- Example (pseudo):

```rust
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub async fn tinkle_connect(url: String, token: String) -> Result<JsValue, JsValue> {
    let client = Client::new(ClientConfig::new(url), Credential::from_session_token(&token), Arc::new(IdbCursorStore::new()));
    client.connect().await.map_err(|e| e.to_string().into())?;
    Ok(JsValue::NULL)
}
```

## Compilation checks (verified 2026-10-20)

```sh
cargo check -p syntra-core -p tinkle-schema -p syntra-ws -p libtinkle   # native, passes
cargo check -p syntra-core --no-default-features --target wasm32-unknown-unknown  # pure core, passes
cargo check -p tinkle-schema --target wasm32-unknown-unknown           # pure app, passes
```

Full `syntra-ws` / `libtinkle` wasm builds require the browser transport
wiring (`ws_stream_wasm`, `wasm-bindgen-futures`, `getrandom_backend="wasm_js"`,
tokio sync-only shims) and are tracked as follow-up; the `#[cfg(target_arch
= "wasm32")]` boundary already guarantees native builds never touch browser
APIs and vice versa. `wasm.rs` currently returns a descriptive error until
`ws_stream_wasm` is wired.
