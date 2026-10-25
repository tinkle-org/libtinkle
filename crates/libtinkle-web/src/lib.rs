//! `libtinkle-web`: wasm-bindgen wrapper over `libtinkle` for browsers.
//!
//! This crate is the browser entry point. It reuses the same async core
//! (`libtinkle` → `syntra-core` + `syntra-ws`) as native; it never
//! reimplements protocol behavior. WASM transport goes through
//! `syntra-ws::wasm` (`ws_stream_wasm`, browser `WebSocket`), not
//! `tokio-tungstenite` (native-only, gated to `not(wasm32)`).
//!
//! Follow-ups (not in this proof):
//! - `spawn_local` pump: adapt `libtinkle::Client::connect` to drive its
//!   notification pump with `wasm-bindgen-futures::spawn_local` on
//!   `wasm32` (currently `tokio::spawn`, which panics without an active
//!   Tokio runtime). Until then WASM remains compile-verified only.
//! - `IndexedDB CursorStore`: implement `libtinkle::CursorStore` over
//!   IndexedDB for durable per-principal cursors. Persist only after
//!   application effects commit (see `SyncManager::acknowledge`); isolate by authenticated principal.
//! - Client registry: keep connected `Client`s in a global map (e.g.
//!   `HashMap<client_id, Client>`) so JS can call/subscribe/close across
//!   invocations instead of one-shot connect.
//!
//! Transport note: the published `syntra-ws =1.0.0-rc.1` `wasm` adapter is
//! fully implemented (`ws_stream_wasm::WsMeta::connect`, binary-only pumps
//! via `wasm-bindgen-futures::spawn_local`, handshake). WASM is still
//! compile-verified only for `libtinkle`: `Client::connect` spawns its
//! notification pump with `tokio::spawn` (`crates/libtinkle/src/client.rs`),
//! which panics without an active Tokio runtime on the browser executor
//! (browser needs `spawn_local` adaptation). Browser runtime smoke tests
//! remain separate.

#[cfg(not(target_arch = "wasm32"))]
use libtinkle::{Client, ClientConfig, Credential, InMemoryCursorStore};
use wasm_bindgen::prelude::*;

/// Compile-only proof: construct a `Client` with an in-memory cursor store.
/// Does NOT establish a browser connection yet.
///
/// Transport uses the published `syntra-ws::wasm::connect`
/// (`ws_stream_wasm::WsMeta::connect`, fully implemented in
/// `syntra-ws =1.0.0-rc.1`). This function must COMPILE for
/// wasm32-unknown-unknown; browser runtime acceptance is NOT established
/// here because `libtinkle::Client` still drives its notification pump
/// with `tokio::spawn` instead of `spawn_local`, which panics without an
/// active Tokio runtime while the wasm-bindgen entry point runs on the
/// browser executor — browser runtime smoke tests remain separate.
///
/// On `wasm32` this returns an unsupported-runtime error instead of
/// claiming success (and instead of calling `Client::connect`, which would
/// panic). Native builds retain the one-shot connect path for testing.
///
/// `url` must be `wss://host/syntra/v1/ws` (production) or an explicitly
/// allowed insecure dev URL. `token` is presented only as
/// `ClientHello.credential`, never in the URL.
#[wasm_bindgen]
pub async fn tinkle_connect(url: String, token: String) -> Result<(), JsValue> {
    #[cfg(target_arch = "wasm32")]
    {
        let _ = (url, token);
        return Err(JsValue::from_str(
            "unsupported runtime: libtinkle::Client::connect uses tokio::spawn for its \
             notification pump, which panics without an active Tokio runtime; \
             browser needs spawn_local adaptation before a connection can be established",
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
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
        // Proof only: connection established (native test path).
        // Intentionally drop here; follow-up will retain the client for
        // subsequent RPC/subscribe calls.
        Ok(())
    }
}
