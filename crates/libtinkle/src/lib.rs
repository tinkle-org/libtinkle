//! `libtinkle`: Tinkle client runtime over Syntra.
//!
//! The same logical client runtime is reusable across iOS, Android,
//! and WebAssembly/browser. Platform adapters (FFI, UniFFI, wasm-bindgen)
//! wrap this async core; they never reimplement protocol behavior.
//!
//! Boundaries:
//! - `syntra-core` owns framing, handshake, correlation, errors.
//! - `syntra-ws` owns WSS/WS byte transport.
//! - `tinkle-schema` owns Tinkle payload schemas + realtime validation.
//! - This crate owns connection lifecycle, typed RPC dispatch,
//!   subscriptions, cursors, reconnect, and backoff.
//!
//! No gRPC client is required for standard Syntra application calls.
//! Ciphertext, if E2EE is integrated later, is constructed at the
//! application encryption layer before entering Syntra.

pub(crate) mod client;
pub(crate) mod config;
pub(crate) mod cursor;
pub(crate) mod errors;
pub(crate) mod reconnect;
pub(crate) mod services;
pub(crate) mod subscription;

pub use client::Client;
pub use config::{ClientConfig, Credential, ReconnectPolicy};
pub use cursor::{CursorStore, InMemoryCursorStore};
pub use errors::TinkleError;
pub use services::{Auth, Messages, Realtime};
pub use subscription::RealtimeEvent;
/// Re-exported cancellation token for [`Client::call_raw_cancellable`].
///
/// `Client::call_raw_cancellable` takes `&syntra_runtime::CancelToken`;
/// apps can name the type through this re-export instead of depending on
/// `syntra-runtime` directly.
pub use syntra_runtime::CancelToken;
