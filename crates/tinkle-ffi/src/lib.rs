//! `tinkle-ffi`: UniFFI facade over `libtinkle` for iOS (Swift) and Android (Kotlin).
//!
//! This crate is a thin boundary adapter. It owns no protocol behavior:
//! framing, handshake, RPC correlation, sync validation, cursors, and
//! reconnect all live in `libtinkle` (and below). The facade only maps
//! between FFI-safe types (owned `String`/`Vec`/records/enums) and the
//! async `libtinkle::Client` API.
//!
//! Rules at this boundary:
//! - All method params are owned (`String`, `Vec`, records, `Arc<...>`).
//!   No `&str`, no borrowed slices.
//! - No Syntra, prost, or `tinkle-schema` types in exported signatures.
//!   (`tinkle-schema` is used internally to build the single write path.)
//! - No cancellation tokens cross FFI. Dropping the foreign future/suspend
//!   task cancels the Rust future, which is then dropped.
//! - Async methods run under Tokio (`async_runtime = "tokio"`); the host
//!   app must call `tinkle_ffi_init()` once at startup so `libtinkle`'s
//!   background tasks (`tokio::spawn`) have a process-wide runtime.

uniffi::setup_scaffolding!();

pub mod client;
pub mod config;
pub mod error;
pub mod runtime;
pub mod sqlite;
pub mod store;

pub use client::{FfiSubscribeResponse, FfiSyncEvent, RoomSub, TinkleClient};
pub use config::ClientConfigFfi;
pub use error::FfiError;
pub use runtime::tinkle_ffi_init;
pub use sqlite::{FfiDialog, FfiMessage, SqliteStorage};
pub use store::{FfiCursorStore, InMemoryFfiStore};
