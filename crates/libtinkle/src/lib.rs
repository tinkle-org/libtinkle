//! `libtinkle`: Tinkle client runtime over Syntra + gRPC catch-up.
//!
//! The same logical client runtime is reusable across iOS, Android,
//! and WebAssembly/browser. Platform adapters (FFI, UniFFI, wasm-bindgen)
//! wrap this async core; they never reimplement protocol behavior.
//!
//! Boundaries:
//! - `syntra-core` owns framing, handshake, correlation, errors.
//! - `syntra-ws` owns WSS/WS byte transport.
//! - `tinkle-schema` owns Tinkle payload schemas + sync validation.
//! - This crate owns connection lifecycle, typed RPC dispatch,
//!   sync coordination, cursors, reconnect, and backoff.
//!
//! Catch-up uses gRPC `ReadUpdates` on native (unavailable on wasm32,
//! where `grpc` returns an error); ongoing delivery uses Syntra
//! notifications carrying `FollowUpdatesResponse` bytes.

#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod cache_projector;
pub(crate) mod client;
pub(crate) mod config;
pub(crate) mod cursor;
pub(crate) mod errors;
pub(crate) mod grpc;
pub(crate) mod reconnect;
pub(crate) mod services;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod sqlite_store;
pub(crate) mod sync;

pub use client::{Client, SubscribeResult};
pub use config::{ClientConfig, Credential, ReconnectPolicy};
pub use cursor::{CursorStore, InMemoryCursorStore};
pub use errors::{RecoveryDecision, TinkleError};
pub use services::{Auth, Messages, Updates};
#[cfg(not(target_arch = "wasm32"))]
pub use sqlite_store::SqliteCursorStore;
pub use sync::{SyncEvent, SyncManager};
/// Re-exported error code for [`TinkleError::code`].
pub use syntra_core::error::ErrorCode;
/// Re-exported cancellation token.
pub use syntra_runtime::CancelToken;
/// Re-exported native storage so public signatures naming `Storage`
/// resolve via `libtinkle` without adding a direct `tinkle-storage`
/// dependency.
#[cfg(not(target_arch = "wasm32"))]
pub use tinkle_storage::Storage;
