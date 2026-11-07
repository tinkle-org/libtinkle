//! `libtinkle`: native Tinkle client runtime over encrypted Syntra TCP.
//!
//! Native platform adapters wrap this async core. Browser networking is
//! unsupported; portable schema checks live in `libtinkle-web`.
//!
//! Boundaries:
//! - `syntra-core` owns framing, handshake, correlation, errors.
//! - `syntra-net` owns encrypted TCP byte transport.
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

pub(crate) mod authorization;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod cache_projector;
pub(crate) mod client;
pub(crate) mod client_info;
pub(crate) mod config;
pub(crate) mod cursor;
pub(crate) mod enrollment;
pub(crate) mod errors;
pub(crate) mod files;
pub(crate) mod liveness;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod logout;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod push;
pub(crate) mod reconnect;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod retirement;
pub(crate) mod services;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod session;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod sqlite_store;
pub(crate) mod sync;

pub use client::Client;
pub use client_info::{ClientInfo, ClientOptions};
pub use config::{ClientConfig, Enrollment, ReconnectPolicy, ServerTrust, TrustSource};
pub use cursor::{CursorStore, InMemoryCursorStore};
pub use enrollment::{
    DurableOperation, EnrollmentNamespaceSnapshot, PersistedEnrollment, SecretBlob,
    SecureEnrollmentStore, SecureStoreError, SecureWrite, SignInDetails, SignInOutcome,
};
pub use errors::{RecoveryDecision, TinkleError};
pub use files::{
    sha256, DownloadManager, FileManager, UploadManager, UploadParams,
    FALLBACK_MAX_DOWNLOAD_PART_BYTES, SHARED_FILE_SCOPE,
};
#[cfg(not(target_arch = "wasm32"))]
pub use push::{
    parse_push_payload, push_envelope_target_key, push_job_id, PushEnvelope, PushManager,
    PushOutcome, PushTokenKind, PUSH_ACCOUNT_JOB_KIND, PUSH_CLAIM_LEASE_SECS,
    PUSH_DEFAULT_BUDGET_SECS, PUSH_ENVELOPE_VERSION, PUSH_JOB_KIND, PUSH_MAX_MESSAGE_IDS,
};
#[cfg(not(target_arch = "wasm32"))]
pub use retirement::{
    EnrollmentObservation, RETIREMENT_OBSERVATION_BUDGET, RETIREMENT_OBSERVATION_INTERVAL,
};
pub use services::{Account, Auth, Files, Messages, Updates};
#[cfg(not(target_arch = "wasm32"))]
pub use session::{SessionAuthorization, SessionCleanup, SessionNetwork, SessionStatus};
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
