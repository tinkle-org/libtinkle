//! Browser/WASM adapter (wasm32 only).
//!
//! Uses `ws_stream_wasm` (browser `WebSocket`) with the same
//! one-binary-message-one-Frame rule. No `Authorization` header is set
//! (browsers forbid arbitrary WS headers); authentication uses the
//! approved Syntra handshake (`ClientHello.credential` → `ServerReady`).
//!
//! This module compiles only for `wasm32-unknown-unknown`. Native builds
//! use `native.rs` and never touch browser APIs.

use syntra_core::{Connection, ConnectionConfig};

use crate::config::WsConfig;
use crate::error::TransportError;

pub struct WsHandle {
    pub conn: Connection,
    pub connection_id: String,
    pub version: u32,
}

pub async fn connect(
    _cfg: WsConfig,
    _credential: Vec<u8>,
    _conn_cfg: ConnectionConfig,
) -> Result<WsHandle, TransportError> {
    // Implemented when the wasm target is enabled:
    // 1. `WsMeta::connect(url, [SUBPROTOCOL])` to `ENDPOINT_PATH`.
    // 2. Binary-only send/recv; reject text/empty/oversize as in native.rs.
    // 3. `conn.handshake(credential)` over the established socket.
    // 4. Spawn `wasm-bindgen-futures` pumps for read/write.
    Err(TransportError::Connect(
        "wasm adapter: build for wasm32-unknown-unknown with ws_stream_wasm".to_string(),
    ))
}
