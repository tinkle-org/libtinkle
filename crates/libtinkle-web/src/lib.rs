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
//!
//! Transport note: the published `syntra-ws =1.0.0-rc.1` `wasm` adapter is
//! fully implemented (`ws_stream_wasm::WsMeta::connect`, binary-only pumps
//! via `wasm-bindgen-futures::spawn_local`, handshake). WASM is still
//! compile-verified only for `libtinkle`: `Client::connect` spawns its
//! notification pump with `tokio::spawn` (`crates/libtinkle/src/client.rs`),
//! which has no browser runtime driver yet (browser needs `spawn_local`
//! adaptation). Browser runtime smoke tests remain separate.

use libtinkle::{Client, ClientConfig, Credential, InMemoryCursorStore};
use wasm_bindgen::prelude::*;

/// Minimal proof: construct a `Client` with an in-memory cursor store,
/// run the Syntra handshake over the browser WebSocket, and resolve.
///
/// Transport uses the published `syntra-ws::wasm::connect`
/// (`ws_stream_wasm::WsMeta::connect`, fully implemented in
/// `syntra-ws =1.0.0-rc.1`). This function must COMPILE for
/// wasm32-unknown-unknown; browser runtime acceptance is NOT established
/// here because `libtinkle::Client` still drives its notification pump
/// with `tokio::spawn` instead of `spawn_local` — browser runtime smoke
/// tests remain separate.
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
    // Proof only: connection established.
    // Intentionally drop here; follow-up will retain the client for
    // subsequent RPC/subscribe calls.
    Ok(())
}
