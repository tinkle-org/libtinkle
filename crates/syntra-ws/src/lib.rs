//! `syntra-ws`: WSS/WS transport adapters for Syntra v1.
//!
//! Profiles: `SNTR-WS-V1` (plaintext, restricted-development only:
//! loopback or isolated test networks, never public traffic) and
//! `SNTR-WSS-H1-V1` (TLS 1.3 over HTTP/1.1 Upgrade, production-capable).
//!
//! Mapping (normative): exactly one complete binary WebSocket message
//! (opcode 0x2, reassembled across fragments) contains exactly one
//! serialized Syntra `Frame`. No TCP length prefix inside WS messages.
//! Binary only; text (0x1) is rejected (close 1003). Empty binary is
//! rejected (`INVALID_ARGUMENT`). Subprotocol token is `syntra.v1`.
//! Endpoint path is `/syntra/v1/ws`. Credentials travel only in
//! `ClientHello.credential`, never in URLs/headers/query.
//!
//! Native implementation uses `tokio-tungstenite` + `rustls` (TLS 1.3,
//! native roots, full chain + hostname verification, fail closed, no
//! plaintext fallback). Browser implementation (wasm32) uses
//! `ws_stream_wasm` with the same one-message-one-Frame rule; see
//! `wasm.rs`. No browser-only APIs are used in native code.

pub mod config;
pub mod error;

#[cfg(not(target_arch = "wasm32"))]
pub mod native;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

pub use config::{Endpoint, WsConfig};
pub use error::TransportError;

#[cfg(not(target_arch = "wasm32"))]
pub use native::{connect, WsHandle};

#[cfg(target_arch = "wasm32")]
pub use wasm::{connect, WsHandle};

/// WebSocket subprotocol token (WS §1C, WSS §1C).
pub const SUBPROTOCOL: &str = "syntra.v1";
/// Canonical endpoint path (WS §1B).
pub const ENDPOINT_PATH: &str = "/syntra/v1/ws";
