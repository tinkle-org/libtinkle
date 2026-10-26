//! `libtinkle`: Tinkle client runtime over Syntra + gRPC fallback.
//!
//! The same logical client runtime is reusable across iOS, Android,
//! and WebAssembly/browser. Platform adapters (FFI, UniFFI, wasm-bindgen)
//! wrap this async core; they never reimplement protocol behavior.
//!
//! Boundaries:
//! - `syntra-core` owns framing, handshake, correlation, errors.
//! - `syntra-ws` owns WSS/WS byte transport.
//! - `tinkle-schema` owns Tinkle payload schemas + journal validation.
//! - This crate owns connection lifecycle, typed RPC dispatch,
//!   reconcile coordination, journal cursors, reconnect, and backoff.
//!
//! Reconciliation model: per-conversation mutation journals
//! (`MessagesService/GetTargetChanges` pages validated by
//! `tinkle-schema::sync`, bodies fetched by `GetMessages` in chunks of at
//! most 100) commit atomically with their journal cursor via
//! `tinkle-storage::Storage::apply_journal_page_fenced`. Notification frames
//! are opaque hints that kick per-target reconciles; connectivity
//! observations (`UpdatesService/ObserveConnectivity`) are ephemeral and
//! never advance a cursor. History windows (`GetHistory.more_available`:
//! `Some(true)` more, `Some(false)` done, `None` legacy window-edge
//! heuristic) heal journal skips without moving the cursor.

#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod cache_projector;
pub(crate) mod client;
pub(crate) mod config;
pub(crate) mod cursor;
pub(crate) mod errors;
pub mod files;
pub(crate) mod grpc;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod push;
pub(crate) mod reconnect;
pub(crate) mod services;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod sqlite_store;
pub(crate) mod sync;

pub use client::Client;
pub use config::{ClientConfig, Credential, ReconnectPolicy};
pub use cursor::{CursorStore, InMemoryCursorStore};
pub use errors::{RecoveryDecision, TinkleError};
pub use files::{
    DownloadManager, FileManager, UploadManager, UploadParams, FALLBACK_MAX_DOWNLOAD_PART_BYTES,
    SHARED_FILE_SCOPE,
};
#[cfg(not(target_arch = "wasm32"))]
pub use push::{
    parse_push_payload, push_envelope_target_key, push_job_id, PushEnvelope, PushManager,
    PushOutcome, PushTokenKind, PUSH_ACCOUNT_JOB_KIND, PUSH_CLAIM_LEASE_SECS,
    PUSH_DEFAULT_BUDGET_SECS, PUSH_ENVELOPE_VERSION, PUSH_JOB_KIND, PUSH_MAX_MESSAGE_IDS,
};
pub use services::{Auth, Files, Messages, Updates};
#[cfg(not(target_arch = "wasm32"))]
pub use sqlite_store::SqliteCursorStore;
pub use sync::{ReconcileStatus, SyncEvent, SyncManager};
/// Re-exported error code for [`TinkleError::code`].
pub use syntra_core::error::ErrorCode;
/// Re-exported cancellation token.
pub use syntra_runtime::CancelToken;
/// Re-exported native storage so public signatures naming `Storage`
/// resolve via `libtinkle` without adding a direct `tinkle-storage`
/// dependency.
#[cfg(not(target_arch = "wasm32"))]
pub use tinkle_storage::Storage;
