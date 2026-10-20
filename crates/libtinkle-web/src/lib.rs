//! `libtinkle-web`: wasm-bindgen wrapper over `libtinkle` for browsers.
//!
//! This crate is the browser entry point. It reuses the same async core
//! (`libtinkle` → `syntra-core` + `syntra-ws`) as native; it never
//! reimplements protocol behavior. WASM transport goes through
//! `syntra-ws::wasm` (`ws_stream_wasm`, browser `WebSocket`), not
//! `tokio-tungstenite` (native-only, gated to `not(wasm32)`).
//!
//! Follow-ups (not in this proof):
//! - `IndexedDB CursorStore`: implement `libtinkle::CursorStore` over
//!   IndexedDB (e.g. via `rexie`/`idb`) for durable per-principal cursors,
//!   replacing `InMemoryCursorStore` used here. Persist `final_cursor` only
//!   after batch effects commit; isolate by authenticated principal.
//! - Client registry: keep connected `Client`s in a global map (e.g.
//!   `HashMap<client_id, Client>`) so JS can call/subscribe/close across
//!   invocations instead of one-shot connect.
//! - `ws_stream_wasm` wiring: complete `syntra-ws::wasm::connect`
//!   (`WsMeta::connect`, binary-only pumps via `wasm-bindgen-futures`).

use libtinkle::{Client, ClientConfig, Credential, InMemoryCursorStore};
use wasm_bindgen::prelude::*;

/// Minimal proof: construct a `Client` with an in-memory cursor store,
/// run the Syntra handshake over the browser WebSocket, and resolve.
///
/// On wasm32-unknown-unknown `syntra-ws::wasm::connect` is currently a stub
/// returning a descriptive `TransportError` at runtime — that is expected
/// until `ws_stream_wasm` wiring lands. This function must still COMPILE for
/// wasm32-unknown-unknown; runtime failure is acceptable for now.
///
/// `url` must be `wss://host/syntra/v1/ws` (production) or an explicitly
/// allowed insecure dev URL. `token` is presented only as
/// `ClientHello.credential`, never in the URL.
#[wasm_bindgen]
pub async fn tinkle_connect(url: String, token: String) -> Result<(), JsValue> {
    let config = ClientConfig::new(url);
    let credential = Credential::from_session_token(&token);
    // Ephemeral store for the proof. Production web must inject an
    // IndexedDB-backed CursorStore (see module docs).
    let store = InMemoryCursorStore::new();
    let client = Client::new(config, credential, store);
    client
        .connect()
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    // Proof only: connection established (or stub error propagated above).
    // Intentionally drop here; follow-up will retain the client for
    // subsequent RPC/subscribe calls.
    Ok(())
}
