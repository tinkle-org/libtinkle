//! High-level typed client: `connect → subscribe → reconcile`.
//!
//! Conceptual use:
//! ```ignore
//! client.connect().await?;
//! client.set_principal_user_id(42).await;
//! client.subscribe(vec![target]).await?;
//! while let Some(evt) = client.next_event().await {
//!     // ConversationChanged / MessagesChanged / Connectivity ...
//! }
//! ```
//!
//! The SDK serializes the public request protobuf, constructs the Syntra
//! `RpcRequest`, allocates a nonzero `request_id`, sends through the
//! active connection, awaits the matching `RpcResult`, decodes the typed
//! response, and returns typed errors. Journal paging (`GetTargetChanges`)
//! and body fetches (`GetMessages`) run over the same Syntra connection by
//! default (dedicated gRPC is opt-in via `grpc_endpoint`); ongoing delivery
//! is opaque `Notification` hints that kick per-target reconciliation.
//! Never auto-retries unsafe mutations without an application idempotency
//! policy.

#[cfg(not(target_arch = "wasm32"))]
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};
#[cfg(not(target_arch = "wasm32"))]
use zeroize::Zeroizing;

use syntra_runtime::{CancelToken, Initiator};

use crate::config::{ClientConfig, Credential};
use crate::cursor::CursorStore;
use crate::errors::{RecoveryDecision, TinkleError};
use crate::reconnect::{backoff_sleep, should_retry};
use crate::services::{Auth, Files, Messages, Updates};
use crate::sync::{ReconcileStatus, RecoverySignal, SyncEvent, SyncManager, UNKNOWN_PRINCIPAL};

struct Live {
    handle: syntra_ws::WsHandle,
    connection_id: String,
    version: u32,
    sync_mgr: Arc<SyncManager>,
    pump: tokio::task::JoinHandle<()>,
}

#[derive(Default)]
struct SupState {
    handle: Option<tokio::task::JoinHandle<()>>,
    shutdown: Option<oneshot::Sender<()>>,
    epoch: u64,
    #[cfg(not(target_arch = "wasm32"))]
    last_result: Option<()>,
    #[cfg(not(target_arch = "wasm32"))]
    last_reestablish: Option<std::time::Instant>,
}

/// Per-target reconcile demand tracked by the supervisor.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
struct TargetDemand {
    failures: u32,
    next_due: std::time::Instant,
}

#[cfg(not(target_arch = "wasm32"))]
impl TargetDemand {
    fn fresh() -> Self {
        Self {
            failures: 0,
            next_due: std::time::Instant::now(),
        }
    }

    fn backoff_for(failures: u32) -> std::time::Duration {
        let shift = failures.min(5);
        std::time::Duration::from_secs(2u64.saturating_mul(1 << shift).min(60))
    }
}

/// Sync interests carried across reconnects: tracked conversation targets.
#[derive(Debug, Default, Clone)]
struct Interests {
    targets: Vec<tinkle_schema::proto::Target>,
    subscribed: bool,
}

/// Resume state for one unfinished new-message catch-up: the `GetHistory`
/// offset the next run continues from plus the top of the contiguous range
/// covered so far. `edge: None` means start at the newest window.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CatchupResume {
    edge: Option<i64>,
    cover_top: Option<i64>,
}

/// Outcome of one fenced catch-up commit that the reconcile loop must
/// classify: fence loss propagates as scope-changed, anything else persists
/// demand with the next offset.
#[cfg(not(target_arch = "wasm32"))]
enum CatchupCommitError {
    /// Fence lost under the commit lock: drop the stale page, no effects.
    FenceLost,
    /// Commit (or pre-commit read) failed: persist demand and retry later.
    Failed(TinkleError),
}

/// Grouped scope inputs for one fenced catch-up commit (clippy
/// `too_many_arguments`: fence fields travel together).
#[cfg(not(target_arch = "wasm32"))]
struct CatchupCommitCtx<'a> {
    mgr: &'a SyncManager,
    fence_principal: &'a str,
    fence_generation: u64,
    target_key: &'a str,
}

/// Typed Tinkle client over Syntra + gRPC fallback.
///
/// Cheap to clone (shares connection + cursors). The same logical runtime
/// is reusable across iOS/Android/WASM via thin platform adapters.
#[derive(Clone)]
pub struct Client {
    config: ClientConfig,
    credential: Arc<RwLock<Credential>>,
    store: Arc<dyn CursorStore>,
    #[cfg(not(target_arch = "wasm32"))]
    sqlite: Option<Arc<tinkle_storage::Storage>>,
    live: Arc<RwLock<Option<Live>>>,
    principal: Arc<RwLock<String>>,
    interests: Arc<tokio::sync::Mutex<Interests>>,
    /// Connectivity watch set retained across reconnects: `ObserveConnectivity`
    /// is per-connection, so every new connection must reissue the stored set.
    watch: Arc<tokio::sync::Mutex<Vec<i64>>>,
    /// Serializes `subscribe` bodies: concurrent subscribes must not
    /// interleave tracking + kicking.
    sub_lock: Arc<tokio::sync::Mutex<()>>,
    /// Serializes the check-dial-install sequence in `connect`.
    connect_lock: Arc<tokio::sync::Mutex<()>>,
    /// Recovery-signal sender installed by the supervisor.
    signal_tx: Arc<tokio::sync::Mutex<Option<mpsc::Sender<RecoverySignal>>>>,
    /// Transport liveness as observed by the notification pump.
    transport_ok: Arc<AtomicBool>,
    sup_state: Arc<tokio::sync::Mutex<SupState>>,
    start_lock: Arc<tokio::sync::Mutex<()>>,
    drain_mgrs: Arc<tokio::sync::Mutex<Vec<Arc<SyncManager>>>>,
    suspended: Arc<AtomicBool>,
    /// In-memory fallback for unfinished new-message catch-ups when the
    /// `background_sync_jobs` table is unavailable (pre-v4 databases).
    /// Durable jobs win when the table exists; this map keeps demand alive
    /// either way so the next reconcile retries. The value is the resume
    /// edge: the `GetHistory` offset the next run must continue from
    /// (`None` = start at the newest window). A stored edge is only ever
    /// replaced by a deeper one or cleared at the confirmed boundary.
    #[cfg(not(target_arch = "wasm32"))]
    catchup_pending: Arc<tokio::sync::Mutex<HashMap<String, CatchupResume>>>,
}

fn target_key_of(target: &tinkle_schema::proto::Target) -> Result<String, TinkleError> {
    match &target.kind {
        Some(tinkle_schema::proto::target::Kind::UserId(id)) => {
            if *id <= 0 {
                return Err(TinkleError::Codec("target id must be positive".into()));
            }
            Ok(format!("target:user:{id}"))
        }
        Some(tinkle_schema::proto::target::Kind::ChatId(id)) => {
            if *id <= 0 {
                return Err(TinkleError::Codec("target id must be positive".into()));
            }
            Ok(format!("target:room:{id}"))
        }
        None => Err(TinkleError::Codec("target kind missing".into())),
    }
}

fn parse_target_key(target_key: &str) -> Result<tinkle_schema::proto::Target, TinkleError> {
    let rest = target_key.strip_prefix("target:").ok_or_else(|| {
        TinkleError::Codec("target_key must be target:user:<id> or target:room:<id>".into())
    })?;
    let (kind, id_str) = rest.split_once(':').ok_or_else(|| {
        TinkleError::Codec("target_key must be target:user:<id> or target:room:<id>".into())
    })?;
    let id: i64 = id_str
        .parse()
        .map_err(|_| TinkleError::Codec("target_key id must be positive".into()))?;
    if id <= 0 {
        return Err(TinkleError::Codec("target_key id must be positive".into()));
    }
    tinkle_schema::sync::target_from_parts(kind, id).map_err(TinkleError::SyncError)
}

impl Client {
    pub fn new(config: ClientConfig, credential: Credential, store: Arc<dyn CursorStore>) -> Self {
        Self {
            config,
            credential: Arc::new(RwLock::new(credential)),
            store,
            #[cfg(not(target_arch = "wasm32"))]
            sqlite: None,
            live: Arc::new(RwLock::new(None)),
            principal: Arc::new(RwLock::new(UNKNOWN_PRINCIPAL.to_string())),
            interests: Arc::new(tokio::sync::Mutex::new(Interests::default())),
            watch: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            sub_lock: Arc::new(tokio::sync::Mutex::new(())),
            connect_lock: Arc::new(tokio::sync::Mutex::new(())),
            signal_tx: Arc::new(tokio::sync::Mutex::new(None)),
            transport_ok: Arc::new(AtomicBool::new(false)),
            sup_state: Arc::new(tokio::sync::Mutex::new(SupState::default())),
            start_lock: Arc::new(tokio::sync::Mutex::new(())),
            drain_mgrs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            suspended: Arc::new(AtomicBool::new(false)),
            #[cfg(not(target_arch = "wasm32"))]
            catchup_pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_sqlite(
        config: ClientConfig,
        credential: Credential,
        storage: Arc<tinkle_storage::Storage>,
    ) -> Self {
        let store: Arc<dyn CursorStore> =
            Arc::new(crate::sqlite_store::SqliteCursorStore::new(storage.clone()));
        Self {
            config,
            credential: Arc::new(RwLock::new(credential)),
            store,
            sqlite: Some(storage),
            live: Arc::new(RwLock::new(None)),
            principal: Arc::new(RwLock::new(UNKNOWN_PRINCIPAL.to_string())),
            interests: Arc::new(tokio::sync::Mutex::new(Interests::default())),
            watch: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            sub_lock: Arc::new(tokio::sync::Mutex::new(())),
            connect_lock: Arc::new(tokio::sync::Mutex::new(())),
            signal_tx: Arc::new(tokio::sync::Mutex::new(None)),
            transport_ok: Arc::new(AtomicBool::new(false)),
            sup_state: Arc::new(tokio::sync::Mutex::new(SupState::default())),
            start_lock: Arc::new(tokio::sync::Mutex::new(())),
            drain_mgrs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            suspended: Arc::new(AtomicBool::new(false)),
            catchup_pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn sqlite_storage(&self) -> Option<Arc<tinkle_storage::Storage>> {
        self.sqlite.clone()
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub async fn is_connected(&self) -> bool {
        self.live.read().await.is_some() && self.transport_ok.load(Ordering::SeqCst)
    }

    pub async fn principal(&self) -> String {
        self.principal.read().await.clone()
    }

    pub async fn set_principal_user_id(&self, user_id: i64) {
        if user_id <= 0 {
            return;
        }
        let p = format!("user:{user_id}");
        // Serialize with in-flight commits: hold the checkpoint lock across
        // the demand reset, both principal updates, and hydration, so an
        // ingest cannot commit between them under a half-switched scope.
        // Lock order is checkpoint-then-state everywhere (see the journal
        // commit path); no path takes them in the reverse order.
        let mgr = self.live_mgr().await.ok();
        let _guard = match mgr.as_ref() {
            Some(m) => Some(m.checkpoint_save_lock().await),
            None => None,
        };
        // Target keys are not principal-scoped, so a stale resume edge from
        // the previous principal would resume the wrong cache. Clear on
        // principal change, before hydrating this principal's durable rows.
        // (Unchanged principal keeps live edges; hydration only fills gaps.)
        #[cfg(not(target_arch = "wasm32"))]
        let changed = *self.principal.read().await != p;
        #[cfg(not(target_arch = "wasm32"))]
        if changed {
            self.catchup_pending.lock().await.clear();
        }
        *self.principal.write().await = p.clone();
        if let Some(m) = mgr.as_deref() {
            m.set_principal(p.clone()).await;
        }
        // Rehydrate resume edges persisted by earlier runs (including
        // pre-restart runs): durable catch-up rows outlive the in-memory
        // map. Best-effort; a miss just starts the next run newest.
        // Native-only (SQLite resume state does not exist on wasm).
        #[cfg(not(target_arch = "wasm32"))]
        self.hydrate_catchup_resume_edges(&p).await;
    }

    /// Retained connectivity watch set. `ObserveConnectivity` is
    /// per-connection, so every new connection must reissue the stored set.
    /// Merges (union + dedupe); empty input keeps the existing set.
    /// When connected, observes the merged set immediately.
    pub async fn set_watch(&self, watch: Vec<i64>) -> Result<(), TinkleError> {
        for w in &watch {
            if *w <= 0 {
                return Err(TinkleError::Codec("watch user_id must be positive".into()));
            }
        }
        let merged = {
            let mut g = self.watch.lock().await;
            for w in watch {
                if !g.contains(&w) {
                    g.push(w);
                }
            }
            g.clone()
        };
        if merged.is_empty() || !self.is_connected().await {
            return Ok(());
        }
        let req = tinkle_schema::proto::ObserveConnectivityRequest { user_ids: merged };
        self.updates().observe_connectivity(req).await?;
        Ok(())
    }

    /// Current retained watch set (for inspection/testing).
    pub async fn watch_list(&self) -> Vec<i64> {
        self.watch.lock().await.clone()
    }

    async fn reobserve_stored_watch(&self) -> Result<(), TinkleError> {
        let watch = self.watch.lock().await.clone();
        if watch.is_empty() {
            return Ok(());
        }
        let req = tinkle_schema::proto::ObserveConnectivityRequest { user_ids: watch };
        self.updates().observe_connectivity(req).await?;
        Ok(())
    }

    /// Push-path fence snapshot: current principal plus the live reconcile
    /// generation when a connection exists (`None` on a cold push
    /// extension with no `live` yet; principal equality still fences).
    /// Native-only: the push wake pipeline (`push.rs`) is not compiled
    /// for wasm.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn push_fence_snapshot(&self) -> (String, Option<u64>) {
        let principal = self.principal.read().await.clone();
        let generation = match self.live.read().await.as_ref() {
            Some(l) => Some(l.sync_mgr.generation().await),
            None => None,
        };
        (principal, generation)
    }

    /// Push-path fence check: principal must still match, and a live
    /// generation snapshot must still match the live scope. Native-only
    /// (see `push_fence_snapshot`).
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn push_fence_check(&self, principal: &str, generation: Option<u64>) -> bool {
        if *self.principal.read().await != principal {
            return false;
        }
        let Some(g) = generation else {
            return true;
        };
        // Live tore down mid-push: principal equality above is the
        // fence; a teardown alone is not a logout/switch.
        let live = self.live.read().await;
        let Some(l) = live.as_ref() else {
            return true;
        };
        l.sync_mgr.check_fence(principal, g).await
    }

    /// Logout-scoped local reset: principal back to unknown, tracked
    /// interests and watch set cleared, in-memory catch-up demand dropped,
    /// and the live reconcile scope fenced via `set_principal`. Durable rows
    /// are wiped separately via `Storage::clear_principal`. Native-only:
    /// called from the push module's `logout`.
    ///
    /// Holds `checkpoint_save_lock` (when a live scope exists) across the
    /// whole reset so an in-flight ingest cannot commit between the
    /// principal write and the generation bump.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn reset_session_for_logout(&self) {
        let mgr = self.live_mgr().await.ok();
        let _guard = match mgr.as_ref() {
            Some(m) => Some(m.checkpoint_save_lock().await),
            None => None,
        };
        self.reset_session_locked(mgr.as_deref()).await;
    }

    /// In-memory logout reset; the caller must hold `checkpoint_save_lock`
    /// when a live scope exists (see `reset_session_for_logout` and
    /// `logout_wipe_and_reset`).
    #[cfg(not(target_arch = "wasm32"))]
    async fn reset_session_locked(&self, mgr: Option<&SyncManager>) {
        *self.principal.write().await = UNKNOWN_PRINCIPAL.to_string();
        self.interests.lock().await.targets.clear();
        self.interests.lock().await.subscribed = false;
        self.watch.lock().await.clear();
        self.catchup_pending.lock().await.clear();
        if let Some(m) = mgr {
            m.set_principal(UNKNOWN_PRINCIPAL.to_string()).await;
        }
    }

    /// Logout-scoped durable wipe + in-memory reset under a single
    /// `checkpoint_save_lock` hold: the `clear_principal` row wipe and the
    /// generation-bumping reset are atomic w.r.t. commits, so an in-flight
    /// ingest can neither commit between them nor land after the wipe
    /// under the old scope. Native-only: called from the push module's
    /// `logout` instead of wiping inline.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn logout_wipe_and_reset(&self, principal: &str) -> Result<(), TinkleError> {
        let mgr = self.live_mgr().await.ok();
        let _guard = match mgr.as_ref() {
            Some(m) => Some(m.checkpoint_save_lock().await),
            None => None,
        };
        // Capture the wipe result but never let it bypass the reset: the
        // in-memory scope (principal, generation fence, tracked state) must
        // come down even when the durable wipe fails, or a half-logged-out
        // client keeps reconciling under a dead principal.
        let mut wipe_result: Result<(), TinkleError> = Ok(());
        if principal != UNKNOWN_PRINCIPAL {
            if let Some(storage) = self.sqlite_storage() {
                let p = principal.to_string();
                wipe_result = tokio::task::spawn_blocking(move || storage.clear_principal(&p))
                    .await
                    .map_err(|e| TinkleError::CursorStore(format!("logout wipe task failed: {e}")))
                    .and_then(|r| r.map_err(|e| TinkleError::CursorStore(e.to_string())));
            }
        }
        self.reset_session_locked(mgr.as_deref()).await;
        wipe_result
    }

    pub async fn connect(&self) -> Result<(), TinkleError> {
        let _connect_guard = self.connect_lock.lock().await;
        if self.is_connected().await {
            return Ok(());
        }
        if self.live.read().await.is_some() {
            self.teardown_connection().await;
        }
        let ws_cfg = self.config.ws_config();
        let credential_bytes = self.credential.read().await.expose_secret().to_vec();
        let handle = syntra_ws::connect(ws_cfg, credential_bytes, self.config.connection.clone())
            .await
            .map_err(|e| {
                TinkleError::Syntra(syntra_core::error::SyntraError::Transport(e.to_string()))
            })?;

        let conn = handle.connection().clone();
        let connection_id = handle.connection_id.clone();
        let version = handle.version;

        let principal = self.principal.read().await.clone();
        let sync_mgr = SyncManager::new(principal);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(sqlite) = self.sqlite.clone() {
            sync_mgr.set_sqlite_storage(sqlite).await;
        }
        // Re-install tracked targets so the fresh pump demands correctly.
        {
            let targets = self.interests.lock().await.targets.clone();
            if !targets.is_empty() {
                sync_mgr.track_targets(targets).await;
            }
        }
        if let Some(tx) = self.signal_tx.lock().await.clone() {
            sync_mgr.set_signal_tx(tx).await;
        }

        // Notification pump: opaque hints only. A frame that decodes as a
        // plausible connectivity observation (positive user_id) is emitted
        // as an ephemeral event and nothing else; every other frame —
        // decode failures and implausible observations included — is a
        // reconcile hint. Accepted observations never double-trigger a
        // reconcile sweep.
        let pump_conn = conn.clone();
        let pump_mgr = sync_mgr.clone();
        let pump_ok = self.transport_ok.clone();
        let pump = tokio::spawn(async move {
            while let Some(payload) = pump_conn.next_notification().await {
                match tinkle_schema::sync::validate_connectivity_notification(&payload) {
                    Ok(obs) if obs.user_id > 0 => {
                        pump_mgr.emit_connectivity(obs).await;
                    }
                    _ => {
                        pump_mgr.handle_hint(&payload).await;
                    }
                }
            }
            pump_ok.store(false, Ordering::SeqCst);
            pump_mgr.emit_signal(RecoverySignal::PumpDead).await;
        });

        let mut live_w = self.live.write().await;
        if live_w.is_some() {
            drop(live_w);
            handle.close().await;
            return Ok(());
        }
        *live_w = Some(Live {
            handle,
            connection_id,
            version,
            sync_mgr,
            pump,
        });
        drop(live_w);
        self.transport_ok.store(true, Ordering::SeqCst);
        // Reissue the retained watch set on the fresh connection.
        // `ObserveConnectivity` is per-connection; without this a reconnect
        // silently drops presence observations. Presence is optional: a
        // terminal watch rejection (Fail) is logged and the healthy
        // connection kept — only errors requiring recovery tear it down.
        if let Err(e) = self.reobserve_stored_watch().await {
            match e.recovery_decision() {
                RecoveryDecision::Fail => {
                    tracing::warn!(error = %e, "stored watch rejected; keeping connection");
                }
                _ => {
                    self.teardown_connection().await;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    async fn live_conn(&self) -> Result<Initiator, TinkleError> {
        self.live
            .read()
            .await
            .as_ref()
            .map(|l| l.handle.connection().clone())
            .ok_or_else(|| TinkleError::NotConnected("call connect() first".into()))
    }

    async fn live_mgr(&self) -> Result<Arc<SyncManager>, TinkleError> {
        self.live
            .read()
            .await
            .as_ref()
            .map(|l| l.sync_mgr.clone())
            .ok_or_else(|| TinkleError::NotConnected("call connect() first".into()))
    }

    pub async fn call_raw(
        &self,
        method: &str,
        payload: Vec<u8>,
        timeout_ms: u32,
    ) -> Result<Vec<u8>, TinkleError> {
        let conn = self.live_conn().await?;
        conn.call(method, payload, timeout_ms)
            .await
            .map_err(TinkleError::Syntra)
    }

    pub async fn call_raw_cancellable(
        &self,
        method: &str,
        payload: Vec<u8>,
        timeout_ms: u32,
        token: &CancelToken,
    ) -> Result<Vec<u8>, TinkleError> {
        let conn = self.live_conn().await?;
        conn.call_cancellable(method, payload, timeout_ms, token)
            .await
            .map_err(TinkleError::Syntra)
    }

    pub fn messages(&self) -> Messages<'_> {
        Messages::new(self)
    }

    pub fn auth(&self) -> Auth<'_> {
        Auth::new(self)
    }

    pub fn updates(&self) -> Updates<'_> {
        Updates::new(self)
    }

    pub fn files(&self) -> Files<'_> {
        Files::new(self)
    }

    /// Rust-owned file identity + eviction manager.
    ///
    /// Canonical `(scope, file_id)` identity with shared records; honors
    /// `pinned` cache rows and active readers, refcounts via storage.
    /// Without SQLite storage the manager still tracks active readers and
    /// reports zero refcounts (wasm / memory-only clients).
    pub fn file_manager(&self) -> crate::files::FileManager {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(s) = self.sqlite.clone() {
            return crate::files::FileManager::with_storage(s);
        }
        crate::files::FileManager::new()
    }

    /// Rust-owned upload manager (`BeginUpload` → `UploadPart` →
    /// `CompleteUpload` with PR #54 status-decision semantics).
    pub fn upload_manager(&self) -> crate::files::UploadManager {
        crate::files::UploadManager::new(self.clone())
    }

    /// Rust-owned download manager (`GetFileInfo` → `DownloadPart` ranges
    /// with size + SHA-256 verify and atomic publish).
    pub fn download_manager(&self) -> crate::files::DownloadManager {
        crate::files::DownloadManager::new(self.clone())
    }

    /// Upload bytes (minimal pass-through used by FFI/Web): returns the
    /// committed `FileInfo`. Idempotency retries preserve the EXACT original
    /// `BeginUploadRequest` (incl. `Option` presence).
    pub async fn upload_bytes(
        &self,
        params: crate::files::UploadParams,
        data: Vec<u8>,
    ) -> Result<tinkle_schema::proto::FileInfo, TinkleError> {
        self.upload_manager().upload_bytes(params, data).await
    }

    /// Download bytes (minimal pass-through used by FFI/Web): verifies
    /// size + SHA-256. `cancel` is per-consumer (isolated from other
    /// consumers of the same file).
    pub async fn download_bytes(
        &self,
        file_id: i64,
        cancel: Option<CancelToken>,
    ) -> Result<Vec<u8>, TinkleError> {
        self.download_manager()
            .download_bytes(file_id, cancel)
            .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn validated_grpc_endpoint(&self) -> Result<String, TinkleError> {
        let endpoint = self
            .config
            .grpc_endpoint
            .clone()
            .ok_or_else(|| TinkleError::GrpcConfig("grpc_endpoint not configured".into()))?;
        ClientConfig::validate_grpc_endpoint(&endpoint, self.config.allow_insecure)?;
        Ok(endpoint)
    }

    /// Fetch specific messages, chunking to `MAX_MESSAGE_IDS_PER_REQUEST`
    /// internally. Syntra is primary; on retryable Syntra failures with a
    /// configured `grpc_endpoint` (native only), falls back to dedicated
    /// gRPC per chunk.
    pub async fn get_messages(
        &self,
        target: tinkle_schema::proto::Target,
        message_ids: Vec<i64>,
    ) -> Result<tinkle_schema::proto::GetMessagesResponse, TinkleError> {
        if target.kind.is_none() {
            return Err(TinkleError::Codec("target kind missing".into()));
        }
        for id in &message_ids {
            if *id <= 0 {
                return Err(TinkleError::Codec("message_ids must be positive".into()));
            }
        }
        if message_ids.is_empty() {
            return Ok(tinkle_schema::proto::GetMessagesResponse {
                messages: vec![],
                users: vec![],
                chats: vec![],
            });
        }
        let chunks = tinkle_schema::sync::chunk_message_ids(&message_ids);
        let mut messages = Vec::new();
        let mut users = Vec::new();
        let mut chats = Vec::new();
        for chunk in chunks {
            let req = tinkle_schema::proto::GetMessagesRequest {
                target: Some(target),
                message_ids: chunk,
            };
            let resp = self.get_messages_single(req).await?;
            messages.extend(resp.messages);
            users.extend(resp.users);
            chats.extend(resp.chats);
        }
        Ok(tinkle_schema::proto::GetMessagesResponse {
            messages,
            users,
            chats,
        })
    }

    async fn get_messages_single(
        &self,
        req: tinkle_schema::proto::GetMessagesRequest,
    ) -> Result<tinkle_schema::proto::GetMessagesResponse, TinkleError> {
        match self.messages().get_messages(req.clone()).await {
            Ok(r) => Ok(r),
            Err(e) => {
                #[cfg(not(target_arch = "wasm32"))]
                let retryable = matches!(e.recovery_decision(), RecoveryDecision::Retry);
                #[cfg(not(target_arch = "wasm32"))]
                if retryable {
                    if let Ok(endpoint) = self.validated_grpc_endpoint() {
                        let token: Zeroizing<Vec<u8>> =
                            Zeroizing::new(self.credential.read().await.expose_secret().to_vec());
                        match crate::grpc::get_messages_grpc(
                            &endpoint,
                            token.as_slice(),
                            req,
                            30_000,
                        )
                        .await
                        {
                            Ok(r) => return Ok(r),
                            Err(_ge) => {}
                        }
                    }
                }
                Err(e)
            }
        }
    }

    /// List retained mutations strictly after one accept position.
    /// Syntra primary with the same gRPC fallback as `get_messages`.
    pub async fn get_target_changes(
        &self,
        target: tinkle_schema::proto::Target,
        after_accept_seq: i64,
        limit: i32,
    ) -> Result<tinkle_schema::proto::GetTargetChangesResponse, TinkleError> {
        if target.kind.is_none() {
            return Err(TinkleError::Codec("target kind missing".into()));
        }
        if after_accept_seq < 0 {
            return Err(TinkleError::Codec(
                "after_accept_seq must be non-negative".into(),
            ));
        }
        let limit = if limit == 0 {
            tinkle_schema::sync::MAX_TARGET_CHANGES_LIMIT
        } else {
            limit.clamp(1, tinkle_schema::sync::MAX_TARGET_CHANGES_LIMIT)
        };
        let req = tinkle_schema::proto::GetTargetChangesRequest {
            target: Some(target),
            after_accept_seq,
            limit,
        };
        match self.messages().get_target_changes(req).await {
            Ok(r) => Ok(r),
            Err(e) => {
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(e.recovery_decision(), RecoveryDecision::Retry) {
                    if let Ok(endpoint) = self.validated_grpc_endpoint() {
                        let token: Zeroizing<Vec<u8>> =
                            Zeroizing::new(self.credential.read().await.expose_secret().to_vec());
                        if let Ok(r) = crate::grpc::get_target_changes_grpc(
                            &endpoint,
                            token.as_slice(),
                            req,
                            30_000,
                        )
                        .await
                        {
                            return Ok(r);
                        }
                    }
                }
                Err(e)
            }
        }
    }

    /// Files: Syntra primary with the same gRPC fallback as
    /// `get_messages`/`get_target_changes` (native only). Input validation
    /// lives in the `Files` facade; this wrapper only adds the fallback.
    pub async fn begin_upload(
        &self,
        req: tinkle_schema::proto::BeginUploadRequest,
    ) -> Result<tinkle_schema::proto::BeginUploadResponse, TinkleError> {
        match self.files().begin_upload(req.clone()).await {
            Ok(r) => Ok(r),
            Err(e) => {
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(e.recovery_decision(), RecoveryDecision::Retry) {
                    if let Ok(endpoint) = self.validated_grpc_endpoint() {
                        let token: Zeroizing<Vec<u8>> =
                            Zeroizing::new(self.credential.read().await.expose_secret().to_vec());
                        if let Ok(r) =
                            crate::grpc::begin_upload_grpc(&endpoint, token.as_slice(), req, 30_000)
                                .await
                        {
                            return Ok(r);
                        }
                    }
                }
                Err(e)
            }
        }
    }

    pub async fn upload_part(
        &self,
        req: tinkle_schema::proto::UploadPartRequest,
    ) -> Result<tinkle_schema::proto::UploadPartResponse, TinkleError> {
        // Clone-free fast path: without a validated gRPC endpoint no
        // fallback is possible, so move the request (part bytes can be
        // large) instead of cloning it for a retry that cannot happen.
        #[cfg(not(target_arch = "wasm32"))]
        if self.validated_grpc_endpoint().is_err() {
            return self.files().upload_part(req).await;
        }
        #[cfg(target_arch = "wasm32")]
        {
            return self.files().upload_part(req).await;
        }
        match self.files().upload_part(req.clone()).await {
            Ok(r) => Ok(r),
            Err(e) => {
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(e.recovery_decision(), RecoveryDecision::Retry) {
                    if let Ok(endpoint) = self.validated_grpc_endpoint() {
                        let token: Zeroizing<Vec<u8>> =
                            Zeroizing::new(self.credential.read().await.expose_secret().to_vec());
                        if let Ok(r) =
                            crate::grpc::upload_part_grpc(&endpoint, token.as_slice(), req, 30_000)
                                .await
                        {
                            return Ok(r);
                        }
                    }
                }
                Err(e)
            }
        }
    }

    pub async fn get_upload_status(
        &self,
        req: tinkle_schema::proto::GetUploadStatusRequest,
    ) -> Result<tinkle_schema::proto::GetUploadStatusResponse, TinkleError> {
        match self.files().get_upload_status(req.clone()).await {
            Ok(r) => Ok(r),
            Err(e) => {
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(e.recovery_decision(), RecoveryDecision::Retry) {
                    if let Ok(endpoint) = self.validated_grpc_endpoint() {
                        let token: Zeroizing<Vec<u8>> =
                            Zeroizing::new(self.credential.read().await.expose_secret().to_vec());
                        if let Ok(r) = crate::grpc::get_upload_status_grpc(
                            &endpoint,
                            token.as_slice(),
                            req,
                            30_000,
                        )
                        .await
                        {
                            return Ok(r);
                        }
                    }
                }
                Err(e)
            }
        }
    }

    pub async fn complete_upload(
        &self,
        req: tinkle_schema::proto::CompleteUploadRequest,
    ) -> Result<tinkle_schema::proto::CompleteUploadResponse, TinkleError> {
        match self.files().complete_upload(req.clone()).await {
            Ok(r) => Ok(r),
            Err(e) => {
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(e.recovery_decision(), RecoveryDecision::Retry) {
                    if let Ok(endpoint) = self.validated_grpc_endpoint() {
                        let token: Zeroizing<Vec<u8>> =
                            Zeroizing::new(self.credential.read().await.expose_secret().to_vec());
                        if let Ok(r) = crate::grpc::complete_upload_grpc(
                            &endpoint,
                            token.as_slice(),
                            req,
                            30_000,
                        )
                        .await
                        {
                            return Ok(r);
                        }
                    }
                }
                Err(e)
            }
        }
    }

    pub async fn abort_upload(
        &self,
        req: tinkle_schema::proto::AbortUploadRequest,
    ) -> Result<tinkle_schema::proto::AbortUploadResponse, TinkleError> {
        match self.files().abort_upload(req.clone()).await {
            Ok(r) => Ok(r),
            Err(e) => {
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(e.recovery_decision(), RecoveryDecision::Retry) {
                    if let Ok(endpoint) = self.validated_grpc_endpoint() {
                        let token: Zeroizing<Vec<u8>> =
                            Zeroizing::new(self.credential.read().await.expose_secret().to_vec());
                        if let Ok(r) =
                            crate::grpc::abort_upload_grpc(&endpoint, token.as_slice(), req, 30_000)
                                .await
                        {
                            return Ok(r);
                        }
                    }
                }
                Err(e)
            }
        }
    }

    pub async fn get_file_info(
        &self,
        req: tinkle_schema::proto::GetFileInfoRequest,
    ) -> Result<tinkle_schema::proto::GetFileInfoResponse, TinkleError> {
        match self.files().get_file_info(req).await {
            Ok(r) => Ok(r),
            Err(e) => {
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(e.recovery_decision(), RecoveryDecision::Retry) {
                    if let Ok(endpoint) = self.validated_grpc_endpoint() {
                        let token: Zeroizing<Vec<u8>> =
                            Zeroizing::new(self.credential.read().await.expose_secret().to_vec());
                        if let Ok(r) = crate::grpc::get_file_info_grpc(
                            &endpoint,
                            token.as_slice(),
                            req,
                            30_000,
                        )
                        .await
                        {
                            return Ok(r);
                        }
                    }
                }
                Err(e)
            }
        }
    }

    pub async fn download_part(
        &self,
        req: tinkle_schema::proto::DownloadPartRequest,
    ) -> Result<tinkle_schema::proto::DownloadPartResponse, TinkleError> {
        match self.files().download_part(req).await {
            Ok(r) => Ok(r),
            Err(e) => {
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(e.recovery_decision(), RecoveryDecision::Retry) {
                    if let Ok(endpoint) = self.validated_grpc_endpoint() {
                        let token: Zeroizing<Vec<u8>> =
                            Zeroizing::new(self.credential.read().await.expose_secret().to_vec());
                        if let Ok(r) = crate::grpc::download_part_grpc(
                            &endpoint,
                            token.as_slice(),
                            req,
                            30_000,
                        )
                        .await
                        {
                            return Ok(r);
                        }
                    }
                }
                Err(e)
            }
        }
    }

    /// Reconcile one conversation: page `GetTargetChanges` from its journal
    /// cursor, validate each page, hydrate edited bodies via chunked
    /// `GetMessages`, project via `cache_projector`, and atomically commit
    /// via `apply_journal_page_fenced`. Returns the applied-change count.
    ///
    /// Room history-scope rule: dropping a target's journal cursor (via
    /// `remove_target`) makes its next reconcile start at 0. When the
    /// retained journal then skips past the dropped position (first change
    /// `accept_seq > after + 1`), this heals with a best-effort `GetHistory`
    /// fallback that repopulates the offline cache without moving the
    /// journal cursor; the current page still jumps the cursor forward over
    /// the gap.
    ///
    /// Implicit-TooLong mapping: the contract has no explicit "retained
    /// mutations too old" code, so a journal skip (`first.accept_seq >
    /// after + 1`) is that signal. History repair is the recovery path: the
    /// cursor jumps forward over the gap and best-effort paged history
    /// (`reconcile_new_messages`, plus `heal_via_history` for the skipped
    /// range) repopulates what the journal can no longer disclose. No
    /// schema change; cursor-jump plus best-effort paged history is the
    /// accepted terminal behavior.
    pub async fn reconcile_target(&self, target_key: &str) -> Result<u64, TinkleError> {
        let target = parse_target_key(target_key)?;
        let mgr = self.live_mgr().await?;
        let principal = self.principal().await;
        if principal == UNKNOWN_PRINCIPAL {
            return Err(TinkleError::Subscription(
                "authenticated principal unknown; call set_principal_user_id after sign_in".into(),
            ));
        }
        let Some(run_generation) = mgr.begin_reconcile(target_key).await else {
            return Ok(0);
        };
        let fence_principal = principal.clone();
        let fence_generation = run_generation;
        let result = self
            .reconcile_target_inner(&mgr, &fence_principal, fence_generation, target_key, target)
            .await;
        match &result {
            Ok(_) => mgr.end_reconcile(target_key, run_generation, false).await,
            Err(_) => {
                mgr.end_reconcile(target_key, run_generation, true).await;
                if let Err(e) = &result {
                    let _ = mgr
                        .emit_event(SyncEvent::ReconcileFailed {
                            target_key: target_key.to_string(),
                            error: e.to_string(),
                        })
                        .await;
                    mgr.emit_signal(RecoverySignal::ReconcileFailed {
                        target_key: target_key.to_string(),
                    })
                    .await;
                }
            }
        }
        result
    }

    async fn reconcile_target_inner(
        &self,
        mgr: &Arc<SyncManager>,
        fence_principal: &str,
        fence_generation: u64,
        target_key: &str,
        target: tinkle_schema::proto::Target,
    ) -> Result<u64, TinkleError> {
        let mut after: i64 = self
            .store
            .load_journal(fence_principal, target_key)
            .await?
            .unwrap_or(0);
        let mut applied: u64 = 0;
        let mut unknown_repage_done = false;
        // Deletions/revocations seen in this run. Delayed body fetches
        // (edited hydration now, history catch-up later) must never
        // resurrect these ids.
        let mut run_removals: HashSet<i64> = HashSet::new();
        loop {
            if !mgr.check_fence(fence_principal, fence_generation).await {
                return Err(TinkleError::Sync(format!(
                    "reconcile scope changed for {target_key}; dropping stale pages"
                )));
            }
            let mut attempt = 0u32;
            let resp = loop {
                match self
                    .get_target_changes(
                        target,
                        after,
                        tinkle_schema::sync::MAX_TARGET_CHANGES_LIMIT,
                    )
                    .await
                {
                    Ok(r) => break r,
                    Err(e) => match e.recovery_decision() {
                        RecoveryDecision::Retry if attempt < 2 => {
                            attempt += 1;
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (1 << attempt),
                            ))
                            .await;
                            continue;
                        }
                        _ => return Err(e),
                    },
                }
            };
            let page = tinkle_schema::sync::validate_changes_page(after, &resp)
                .map_err(TinkleError::SyncError)?;
            if page.changes.is_empty() {
                match page.more {
                    tinkle_schema::sync::PageMore::No => break,
                    tinkle_schema::sync::PageMore::Unknown if !unknown_repage_done => {
                        unknown_repage_done = true;
                        after = page.last_accept_seq;
                        continue;
                    }
                    _ => break,
                }
            }
            // Journal skip → best-effort history heal (does not move cursor).
            if let Some(first) = page.changes.first() {
                if first.accept_seq > after.saturating_add(1) {
                    self.heal_via_history(&target, target_key).await;
                }
            }
            // Hydrate + project (native SQLite cache). Wasm reconciles
            // cursors + events only; message bodies arrive via snapshots.
            // Removals from this page mask delayed fetches in the same run.
            #[cfg(not(target_arch = "wasm32"))]
            {
                for c in &page.changes {
                    if matches!(
                        tinkle_schema::proto::TargetChangeKind::try_from(c.kind),
                        Ok(tinkle_schema::proto::TargetChangeKind::Deleted
                            | tinkle_schema::proto::TargetChangeKind::Revoked)
                    ) {
                        run_removals.insert(c.message_id);
                    }
                }
            }
            // Journal pages batch up to 500 changes. Dialog touches are
            // coalesced per peer by `cache_projector`, but a page can still
            // exceed the SQLite per-commit batch limits (notably 256
            // dialogs), so commit in shards with intermediate cursors. Each
            // shard advances the cursor to its last change, so a crash
            // replays from the last committed shard.
            const JOURNAL_COMMIT_CHANGE_BUDGET: usize = 200;
            #[cfg(target_arch = "wasm32")]
            let affected: Vec<i64> = page.changes.iter().map(|c| c.message_id).collect();
            #[cfg(not(target_arch = "wasm32"))]
            let mut affected: Vec<i64> = Vec::with_capacity(page.changes.len());
            // Set when the native shard loop below reports per-shard, so the
            // page-level ConversationChanged stays shard-less-only.
            // (Mutability is cfg-gated: wasm has no shards to report.)
            #[cfg(target_arch = "wasm32")]
            let shard_reported = false;
            #[cfg(not(target_arch = "wasm32"))]
            let mut shard_reported = false;
            let new_cursor = page.last_accept_seq;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(sqlite) = mgr.sqlite_clone().await {
                let shards: Vec<&[tinkle_schema::proto::TargetChange]> =
                    page.changes.chunks(JOURNAL_COMMIT_CHANGE_BUDGET).collect();
                for chunk in shards {
                    let Some(last) = chunk.last() else {
                        continue;
                    };
                    let shard_cursor = last.accept_seq;
                    let sub_page = tinkle_schema::sync::ValidatedChangesPage {
                        changes: chunk.to_vec(),
                        last_accept_seq: shard_cursor,
                        more: page.more,
                    };
                    let (ops, mut shard_affected) = self
                        .build_page_ops(target_key, target, &sub_page, &run_removals)
                        .await?;
                    let _guard = mgr.checkpoint_save_lock().await;
                    if !mgr.check_fence(fence_principal, fence_generation).await {
                        return Err(TinkleError::Sync(format!(
                            "reconcile scope changed for {target_key}; dropping stale pages"
                        )));
                    }
                    let gen = {
                        let sqlite_task = sqlite.clone();
                        let p = fence_principal.to_string();
                        let t = target_key.to_string();
                        tokio::task::spawn_blocking(move || sqlite_task.stream_state(&p, &t))
                            .await
                            .map_err(|e| {
                                TinkleError::CursorStore(format!("stream_state task failed: {e}"))
                            })?
                            .map_err(|e| TinkleError::CursorStore(e.to_string()))?
                            .generation
                    };
                    let apply = {
                        let sqlite_task = sqlite.clone();
                        let p = fence_principal.to_string();
                        let t = target_key.to_string();
                        let ops_task = ops;
                        tokio::task::spawn_blocking(move || {
                            sqlite_task.apply_journal_page_fenced(
                                &p,
                                &t,
                                &ops_task,
                                shard_cursor,
                                gen,
                            )
                        })
                        .await
                        .map_err(|e| {
                            TinkleError::CursorStore(format!("journal apply task failed: {e}"))
                        })?
                    };
                    if let Err(e) = apply {
                        use tinkle_storage::StorageError as SE;
                        match &e {
                            SE::GenerationMismatch { .. } | SE::ScopeMismatch { .. } => {
                                return Err(TinkleError::Sync(e.to_string()));
                            }
                            _ => {
                                let _ = mgr
                                    .emit_event(SyncEvent::CheckpointFailed {
                                        target_key: target_key.to_string(),
                                        accept_seq: shard_cursor,
                                        error: e.to_string(),
                                    })
                                    .await;
                                mgr.emit_signal(RecoverySignal::CheckpointFailed {
                                    target_key: target_key.to_string(),
                                    accept_seq: shard_cursor,
                                })
                                .await;
                                return Err(TinkleError::CheckpointFailed {
                                    target_key: target_key.to_string(),
                                    accept_seq: shard_cursor,
                                    error: e.to_string(),
                                });
                            }
                        }
                    }
                    affected.append(&mut shard_affected);
                    // Report each shard as it commits: a later shard (or
                    // page) can still fail, and already-committed progress
                    // must be reported rather than lost with it. The
                    // aggregates below then only fire for shard-less paths
                    // (`affected` stays empty here).
                    if !affected.is_empty() {
                        shard_reported = true;
                        let _ = mgr
                            .emit_event(SyncEvent::ConversationChanged {
                                target_key: target_key.to_string(),
                                applied_accept_seq: shard_cursor,
                            })
                            .await;
                        let _ = mgr
                            .emit_event(SyncEvent::MessagesChanged {
                                target_key: target_key.to_string(),
                                message_ids: std::mem::take(&mut affected),
                            })
                            .await;
                    }
                }
            } else {
                let _guard = mgr.checkpoint_save_lock().await;
                if let Err(e) = self
                    .store
                    .save_journal(fence_principal, target_key, new_cursor)
                    .await
                {
                    let _ = mgr
                        .emit_event(SyncEvent::CheckpointFailed {
                            target_key: target_key.to_string(),
                            accept_seq: new_cursor,
                            error: e.to_string(),
                        })
                        .await;
                    return Err(e);
                }
                affected = page.changes.iter().map(|c| c.message_id).collect();
            }
            #[cfg(target_arch = "wasm32")]
            {
                let _guard = mgr.checkpoint_save_lock().await;
                if let Err(e) = self
                    .store
                    .save_journal(fence_principal, target_key, new_cursor)
                    .await
                {
                    let _ = mgr
                        .emit_event(SyncEvent::CheckpointFailed {
                            target_key: target_key.to_string(),
                            accept_seq: new_cursor,
                            error: e.to_string(),
                        })
                        .await;
                    return Err(e);
                }
            }
            applied += page.changes.len() as u64;
            after = new_cursor;
            {
                // Native shards already reported per-shard above; the
                // page-level ConversationChanged is for shard-less paths
                // only (wasm, native cursor-only) to avoid duplicates.
                if !shard_reported {
                    let _ = mgr
                        .emit_event(SyncEvent::ConversationChanged {
                            target_key: target_key.to_string(),
                            applied_accept_seq: new_cursor,
                        })
                        .await;
                }
                if !affected.is_empty() {
                    let _ = mgr
                        .emit_event(SyncEvent::MessagesChanged {
                            target_key: target_key.to_string(),
                            message_ids: affected,
                        })
                        .await;
                }
            }
            match page.more {
                tinkle_schema::sync::PageMore::Yes => continue,
                tinkle_schema::sync::PageMore::No => break,
                tinkle_schema::sync::PageMore::Unknown if !unknown_repage_done => {
                    unknown_repage_done = true;
                    continue;
                }
                _ => break,
            }
        }
        // Journal pages cover edits/removals only, never new messages.
        // Catch up on bodies the journal cannot disclose: page history
        // windows backward to the server-confirmed boundary, diff each
        // window against the cache, and ingest the missing ids.
        // Best-effort: failures persist a retry job, never fail the run
        // (fence loss still propagates like the journal loop above).
        #[cfg(not(target_arch = "wasm32"))]
        {
            if self.sqlite.is_some() {
                let caught = self
                    .reconcile_new_messages(
                        mgr,
                        fence_principal,
                        fence_generation,
                        &target,
                        target_key,
                        &run_removals,
                    )
                    .await?;
                applied += caught;
            }
        }
        Ok(applied)
    }

    /// Shared message ingest: project bodies via `cache_projector` and
    /// commit without moving the journal cursor (same-cursor commit).
    ///
    /// Used by the history HydrateCache path (`load_history`), the
    /// new-message catch-up (`reconcile_new_messages`), and the future push
    /// path. Freshness (`accept_seq` / `edit_date`) travels on the row and
    /// is enforced by storage dedup; users/chats commit with scoped
    /// provenance in the same fenced transaction. Message ids in `exclude`
    /// (deletions from the same run) are never ingested, and existence is
    /// re-checked against the cache immediately before commit so a delayed
    /// fetch cannot resurrect a deleted row.
    ///
    /// Native commits via `apply_journal_page_fenced` at the current cursor;
    /// wasm has no SQLite cache and only ensures the journal row exists.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn ingest_messages(
        &self,
        principal: &str,
        target_key: &str,
        messages: &[tinkle_schema::proto::Message],
        users: &[tinkle_schema::proto::User],
        chats: &[tinkle_schema::proto::Chat],
        exclude: &HashSet<i64>,
    ) -> Result<Vec<i64>, TinkleError> {
        let storage = match self.sqlite.clone() {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        if messages.is_empty() && users.is_empty() && chats.is_empty() {
            return Ok(Vec::new());
        }
        // Project bodies, skipping same-run removals. File refs ride
        // alongside messages/entities in the same fenced transaction.
        let mut ops =
            crate::cache_projector::project_messages_to_ops(target_key, messages, exclude)
                .map_err(TinkleError::Sync)?;
        // Entity hydration with scoped provenance.
        crate::cache_projector::merge_ops(
            &mut ops,
            crate::cache_projector::project_entities_to_ops(users, chats),
        );
        // Journal pages require file origins == target_key; entity file
        // refs arrive with a global-pin placeholder ("") and are rebased
        // here so the same shared (scope,file_id) record commits atomically.
        crate::cache_projector::rebase_file_origins(&mut ops, target_key);
        if ops.messages.is_empty()
            && ops.users.is_empty()
            && ops.chats.is_empty()
            && ops.files.is_empty()
            && ops.message_files.is_empty()
            && ops.entity_files.is_empty()
            && ops.replace_entity_files.is_empty()
        {
            return Ok(Vec::new());
        }
        // Re-check before commit: drop any message id that is now excluded
        // or that a concurrent removal already deleted. The cache has no
        // tombstones, so the run-removal set is authoritative; the
        // pre-commit id diff below is the second gate.
        let want_ids: Vec<i64> = ops.messages.iter().map(|m| m.message_id).collect();
        if want_ids.is_empty() && ops.users.is_empty() && ops.chats.is_empty() {
            return Ok(Vec::new());
        }
        let current: i64 = self
            .store
            .load_journal(principal, target_key)
            .await
            .map_err(|e| TinkleError::CursorStore(e.to_string()))?
            .unwrap_or(0);
        let gen: u64 = {
            let sqlite_task = storage.clone();
            let p = principal.to_string();
            let t = target_key.to_string();
            match tokio::task::spawn_blocking(move || sqlite_task.stream_state(&p, &t)).await {
                Ok(Ok(st)) => st.generation,
                Ok(Err(e)) => return Err(TinkleError::CursorStore(e.to_string())),
                Err(e) => {
                    return Err(TinkleError::CursorStore(format!(
                        "stream_state task failed: {e}"
                    )));
                }
            }
        };
        // Second gate before commit: re-apply the exclusion set in case
        // it grew after projection (concurrent removal in the same run).
        // Existence itself is re-checked by callers via GetMessages omission
        // (omitted ids are never passed here); the cache keeps no
        // tombstones, so the run-removal set is authoritative.
        ops.messages.retain(|m| !exclude.contains(&m.message_id));
        if ops.messages.is_empty() && ops.users.is_empty() && ops.chats.is_empty() {
            return Ok(Vec::new());
        }
        let tk = target_key.to_string();
        let pr = principal.to_string();
        let result = tokio::task::spawn_blocking(move || {
            storage.apply_journal_page_fenced(&pr, &tk, &ops, current, gen)
        })
        .await;
        match result {
            Ok(Ok(())) => Ok(want_ids
                .into_iter()
                .filter(|id| !exclude.contains(id))
                .collect()),
            Ok(Err(e)) => {
                tracing::debug!(error = %e, "ingest_messages commit failed");
                Err(TinkleError::CursorStore(e.to_string()))
            }
            Err(e) => Err(TinkleError::CursorStore(format!(
                "ingest commit task failed: {e}"
            ))),
        }
    }

    /// Wasm ingest: no SQLite cache; validate ids and ensure the journal
    /// row exists without moving the cursor.
    #[cfg(target_arch = "wasm32")]
    async fn ingest_messages(
        &self,
        principal: &str,
        target_key: &str,
        messages: &[tinkle_schema::proto::Message],
        _users: &[tinkle_schema::proto::User],
        _chats: &[tinkle_schema::proto::Chat],
        exclude: &HashSet<i64>,
    ) -> Result<Vec<i64>, TinkleError> {
        let mut out = Vec::new();
        for m in messages {
            if m.message_id <= 0 || exclude.contains(&m.message_id) {
                continue;
            }
            out.push(m.message_id);
        }
        // Same-cursor journal touch: never advances the cursor.
        let current: i64 = self
            .store
            .load_journal(principal, target_key)
            .await
            .map_err(|e| TinkleError::CursorStore(e.to_string()))?
            .unwrap_or(0);
        self.store
            .save_journal(principal, target_key, current)
            .await
            .map_err(|e| TinkleError::CursorStore(e.to_string()))?;
        Ok(out)
    }

    /// Native-only page hydration + projection: fetch edited bodies via
    /// chunked `GetMessages`, project the validated page into `CacheOps`,
    /// and collect affected message ids for the `MessagesChanged` event.
    ///
    /// Bodies whose ids are in `run_removals` (deletions/revocations from
    /// the same reconcile run) are skipped so a delayed fetch never
    /// resurrects a deleted row. Entity hydration shares
    /// `cache_projector::project_entities_to_ops` with `ingest_messages`.
    #[cfg(not(target_arch = "wasm32"))]
    async fn build_page_ops(
        &self,
        target_key: &str,
        target: tinkle_schema::proto::Target,
        page: &tinkle_schema::sync::ValidatedChangesPage,
        run_removals: &HashSet<i64>,
    ) -> Result<(tinkle_storage::CacheOps, Vec<i64>), TinkleError> {
        // Hydrate edited bodies via chunked GetMessages, skipping ids
        // already known-deleted in this run.
        let edited_ids: Vec<i64> = page
            .changes
            .iter()
            .filter(|c| {
                tinkle_schema::proto::TargetChangeKind::try_from(c.kind)
                    .map(|k| k == tinkle_schema::proto::TargetChangeKind::Edited)
                    .unwrap_or(false)
            })
            .map(|c| c.message_id)
            .filter(|id| !run_removals.contains(id))
            .collect();
        let mut fetched: HashMap<i64, tinkle_schema::proto::Message> = HashMap::new();
        let mut fetched_users = Vec::new();
        let mut fetched_chats = Vec::new();
        if !edited_ids.is_empty() {
            match self.get_messages(target, edited_ids).await {
                Ok(r) => {
                    for m in r.messages {
                        // Second gate: a body that arrived for a deleted id
                        // (server race) is dropped before projection.
                        if run_removals.contains(&m.message_id) {
                            continue;
                        }
                        fetched.insert(m.message_id, m);
                    }
                    fetched_users = r.users;
                    fetched_chats = r.chats;
                }
                Err(e) => {
                    // Hydration is best-effort: fall back to journal bodies.
                    tracing::debug!(error = %e, "edited hydration failed, using journal bodies");
                }
            }
        }
        let mut ops = tinkle_storage::CacheOps::default();
        let mut affected: Vec<i64> = Vec::with_capacity(page.changes.len());
        for change in &page.changes {
            affected.push(change.message_id);
            let kind =
                tinkle_schema::proto::TargetChangeKind::try_from(change.kind).map_err(|_| {
                    TinkleError::Sync(format!("unknown target change kind {}", change.kind))
                })?;
            match kind {
                tinkle_schema::proto::TargetChangeKind::Edited => {
                    if run_removals.contains(&change.message_id) {
                        continue;
                    }
                    let body = fetched
                        .get(&change.message_id)
                        .or(change.message.as_ref())
                        .ok_or_else(|| {
                            TinkleError::Sync(format!(
                                "edited change for message {} must carry a body",
                                change.message_id
                            ))
                        })?;
                    let extra = crate::cache_projector::project_message_to_ops(target_key, body)
                        .map_err(TinkleError::Sync)?;
                    crate::cache_projector::merge_ops(&mut ops, extra);
                }
                tinkle_schema::proto::TargetChangeKind::Deleted
                | tinkle_schema::proto::TargetChangeKind::Revoked => {
                    let extra = crate::cache_projector::project_change_to_ops(target_key, change)
                        .map_err(TinkleError::Sync)?;
                    crate::cache_projector::merge_ops(&mut ops, extra);
                }
                tinkle_schema::proto::TargetChangeKind::Unspecified => {
                    return Err(TinkleError::Sync(format!(
                        "change for message {} has unspecified kind",
                        change.message_id
                    )));
                }
            }
        }
        // Entity hydration from GetMessages (scoped provenance, shared with
        // ingest_messages). File refs ride alongside; rebase placeholder
        // origins to the page target.
        crate::cache_projector::merge_ops(
            &mut ops,
            crate::cache_projector::project_entities_to_ops(&fetched_users, &fetched_chats),
        );
        crate::cache_projector::rebase_file_origins(&mut ops, target_key);
        Ok((ops, affected))
    }

    /// New-message catch-up for one target, paged to the server-confirmed
    /// boundary.
    ///
    /// `GetTargetChanges` discloses edits/removals only, never new
    /// messages. After journal paging, page `GetHistory` windows backward
    /// (newest first) to the server-confirmed end, diff each window's ids
    /// against the offline cache, re-check the missing ids via
    /// `GetMessages` (omitted ids carry no information and are treated as
    /// deleted), and ingest the survivors through the shared
    /// `ingest_messages` helper (same-cursor commit, never moving the
    /// journal cursor). Ids in `run_removals` are skipped up front and
    /// re-checked before commit. An all-deleted/all-omitted window carries
    /// no ingestible bodies but must NOT end the loop: paging continues to
    /// the boundary.
    ///
    /// Paging mirrors `load_history`: `more_available == Some(true)`
    /// continues at `offset_id_offset` (else the window's smallest
    /// `message_id`); `Some(false)` stops at the confirmed boundary; `None`
    /// applies the legacy rule (empty or non-full window stops, else
    /// continue at the edge). A no-progress guard breaks the loop when the
    /// window edge fails to move backward while `more` still says
    /// continue.
    ///
    /// Each run fetches a bounded number of windows
    /// (`MAX_CATCHUP_WINDOWS_PER_RUN`); when the budget is spent with more
    /// work pending, the next offset is persisted (in-memory resume edge
    /// plus the durable job payload) so the supervisor sweep and routine
    /// hints resume from that edge instead of restarting at the newest
    /// window. Restarting would starve the tail under the bound and re-page
    /// covered windows every run. A resumed run first covers post-resume
    /// arrivals with newest-first windows until overlap with cached
    /// coverage (or the edge), then jumps to the edge.
    ///
    /// Every commit goes through `ingest_catchup_window`: the checkpoint
    /// lock is held, the run fence is re-checked under it, and the commit
    /// lands under `fence_principal`. Fence loss drops the stale page with
    /// a scope-changed error; any other failure persists demand with the
    /// next offset and returns the count so far.
    ///
    /// Best-effort: returns the total ingested count (`Ok(0)` when caught
    /// up). `clear_catchup_demand` runs only on a clean reach of the
    /// boundary; any `GetHistory`/`GetMessages`/ingest error, no-progress
    /// break, or spent budget calls `persist_catchup_demand` and returns the
    /// count so far. Fence loss returns a `TinkleError::Sync`
    /// scope-changed error like the journal loop (the one failure that
    /// propagates instead of persisting demand).
    #[cfg(not(target_arch = "wasm32"))]
    async fn reconcile_new_messages(
        &self,
        mgr: &SyncManager,
        fence_principal: &str,
        fence_generation: u64,
        target: &tinkle_schema::proto::Target,
        target_key: &str,
        run_removals: &HashSet<i64>,
    ) -> Result<u64, TinkleError> {
        // History windows cap at 100 (`MAX_TARGET_CHANGES_LIMIT` is for the
        // journal only).
        const HISTORY_CATCHUP_LIMIT: i32 = 100;
        // Windows fetched per run. Exhaustion across runs chains through
        // persisted demand + resume edge (see below), so one run never
        // pages forever against a live server; the sweep re-runs while
        // demand persists. Mirrors `load_history`'s 10-page cap.
        const MAX_CATCHUP_WINDOWS_PER_RUN: u32 = 10;
        // The run commits under the reconcile scope it started in; there is
        // no function-start principal snapshot. A scope change mid-run
        // fails the loop top (or the under-lock re-check before each
        // commit) with a scope-changed error instead of ingesting under a
        // stale scope.
        if fence_principal == UNKNOWN_PRINCIPAL {
            return Ok(0);
        }
        let storage = match self.sqlite.clone() {
            Some(s) => s,
            None => return Ok(0),
        };
        // Resume state from an earlier bounded/failed run: the `GetHistory`
        // offset the next run continues from plus the top of the contiguous
        // range covered so far. Consulted here so both the supervisor sweep
        // and routine hints (both funnel into `reconcile_target`) resume
        // instead of restarting at the newest window.
        let resume: CatchupResume = self
            .catchup_pending
            .lock()
            .await
            .get(target_key)
            .copied()
            .unwrap_or_default();
        // While set, the run is covering post-resume arrivals: after each
        // newest-first window it stops only on proof of connection to the
        // previously covered range (the window reaches the stored cover top,
        // or passes the edge) and jumps to the edge instead of re-paging
        // already-covered windows. Without this, arrivals between runs
        // newer than the edge would be skipped by the jump.
        let mut covering_new = resume.edge.is_some();
        // Connection threshold for the covering phase: the top of the
        // previously covered contiguous range. Falls back to the edge for
        // rows written before the top was persisted.
        let cover_top = resume.cover_top.or(resume.edge);
        // Diff baseline, loaded once; extended with ingested ids as windows
        // commit so overlapping windows never re-request the same bodies.
        let mut cached: HashSet<i64> = {
            let st = storage.clone();
            let p = fence_principal.to_string();
            let t = target_key.to_string();
            match tokio::task::spawn_blocking(move || st.cached_message_ids(&p, &t)).await {
                Ok(Ok(ids)) => ids.into_iter().collect(),
                _ => HashSet::new(),
            }
        };
        let mut total: u64 = 0;
        let mut offset_id: Option<i64> = None;
        // Edge of the previous window (smallest message id seen, else the
        // server offset). Paging moves backward, so each new edge must be
        // strictly smaller; anything else while `more` says continue is a
        // stalled server cursor.
        let mut prev_edge: Option<i64> = None;
        let mut windows: u32 = 0;
        let mut run_top: Option<i64> = None;
        // Per-run scope for every fenced operation below (commits and
        // demand bookkeeping share one fence context).
        let ctx = CatchupCommitCtx {
            mgr,
            fence_principal,
            fence_generation,
            target_key,
        };
        loop {
            if !mgr.check_fence(fence_principal, fence_generation).await {
                return Err(TinkleError::Sync(format!(
                    "reconcile scope changed for {target_key}; dropping stale pages"
                )));
            }
            if windows >= MAX_CATCHUP_WINDOWS_PER_RUN {
                // Per-run budget spent with more work pending: persist the
                // next offset so the sweep resumes from this edge.
                tracing::debug!(
                    target_key,
                    windows,
                    "new-message catch-up window budget spent"
                );
                self.persist_catchup_demand(&ctx, offset_id, run_top).await;
                return Ok(total);
            }
            let history_req = tinkle_schema::proto::GetHistoryRequest {
                target: Some(*target),
                offset_id,
                offset_date: None,
                add_offset: 0,
                limit: HISTORY_CATCHUP_LIMIT,
                max_id: None,
                min_id: None,
            };
            let history = match self.messages().get_history(history_req).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!(target_key, error = %e, "new-message catch-up history failed");
                    self.persist_catchup_demand(&ctx, offset_id, run_top).await;
                    return Ok(total);
                }
            };
            windows += 1;
            let more = history.more_available;
            let offset = history.offset_id_offset;
            let empty = history.messages.is_empty();
            let full = history.messages.len() as i32 >= HISTORY_CATCHUP_LIMIT;
            // Edge for the legacy fallback and the no-progress guard:
            // smallest id in this window.
            let min_id = history.messages.iter().map(|m| m.message_id).min();
            // Track the newest id seen: windows page strictly backward, so
            // the max across fetched windows is this run's coverage top.
            run_top = std::cmp::max(run_top, history.messages.iter().map(|m| m.message_id).max());
            if empty {
                // Still upsert any entities the window carried.
                if !history.users.is_empty() || !history.chats.is_empty() {
                    let no_exclude: HashSet<i64> = HashSet::new();
                    match self
                        .ingest_catchup_window(
                            &ctx,
                            &[],
                            &history.users,
                            &history.chats,
                            &no_exclude,
                        )
                        .await
                    {
                        Ok(_) => {}
                        Err(CatchupCommitError::FenceLost) => {
                            return Err(TinkleError::Sync(format!(
                                "reconcile scope changed for {target_key}; dropping stale pages"
                            )));
                        }
                        Err(CatchupCommitError::Failed(e)) => {
                            tracing::debug!(target_key, error = %e, "new-message catch-up ingest failed");
                            self.persist_catchup_demand(&ctx, offset_id, run_top).await;
                            return Ok(total);
                        }
                    }
                }
            } else {
                let mut missing: Vec<i64> = history
                    .messages
                    .iter()
                    .map(|m| m.message_id)
                    .filter(|id| *id > 0 && !cached.contains(id) && !run_removals.contains(id))
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                missing.sort_unstable();
                if !missing.is_empty() {
                    // Re-check existence via GetMessages: omitted ids are
                    // deleted / invisible and must not be ingested.
                    let rechecked = match self.get_messages(*target, missing.clone()).await {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::debug!(target_key, error = %e, "new-message catch-up fetch failed");
                            self.persist_catchup_demand(&ctx, offset_id, run_top).await;
                            return Ok(total);
                        }
                    };
                    let live_ids: HashSet<i64> =
                        rechecked.messages.iter().map(|m| m.message_id).collect();
                    // Keep history bodies for ids the server confirmed, so
                    // edit_date / accept_seq from either source survive;
                    // prefer the GetMessages copy when both exist (it is
                    // the fresher read).
                    let mut by_id: HashMap<i64, tinkle_schema::proto::Message> = HashMap::new();
                    for m in history.messages {
                        if live_ids.contains(&m.message_id) && !run_removals.contains(&m.message_id)
                        {
                            by_id.insert(m.message_id, m);
                        }
                    }
                    for m in rechecked.messages {
                        if !run_removals.contains(&m.message_id) {
                            by_id.insert(m.message_id, m);
                        }
                    }
                    // An all-deleted/all-omitted window ingests nothing but
                    // continues paging below; it never clears demand here.
                    if !by_id.is_empty() {
                        let bodies: Vec<tinkle_schema::proto::Message> =
                            by_id.into_values().collect();
                        // Merge entity hydration from both reads.
                        let mut users = history.users;
                        users.extend(rechecked.users);
                        let mut chats = history.chats;
                        chats.extend(rechecked.chats);
                        match self
                            .ingest_catchup_window(&ctx, &bodies, &users, &chats, run_removals)
                            .await
                        {
                            Ok(ids) => {
                                for id in &ids {
                                    cached.insert(*id);
                                }
                                total += ids.len() as u64;
                            }
                            Err(CatchupCommitError::FenceLost) => {
                                return Err(TinkleError::Sync(format!(
                                    "reconcile scope changed for {target_key}; dropping stale pages"
                                )));
                            }
                            Err(CatchupCommitError::Failed(e)) => {
                                tracing::debug!(target_key, error = %e, "new-message catch-up ingest failed");
                                self.persist_catchup_demand(&ctx, offset_id, run_top).await;
                                return Ok(total);
                            }
                        }
                    }
                }
            }
            // Resume fast-forward: while covering post-resume arrivals, stop
            // descending only on proof of connection to the previously
            // covered range — the window reaches the stored cover top, or
            // passes the edge — and jump to the edge instead of re-paging
            // already-covered windows. A merely overlapping cached id proves
            // nothing about contiguity (healed windows, scrolled-back history,
            // and seeds plant scattered ids below the edge), so overlap alone
            // must neither end coverage nor trigger the jump. Routine-hint
            // runs funnel through here too, so they resume from the edge
            // rather than being restricted to the newest window while older
            // history may still be uncached.
            if covering_new {
                let reached_top = matches!((min_id, cover_top), (Some(m), Some(t)) if m <= t);
                let past_edge = matches!((min_id, resume.edge), (Some(m), Some(e)) if m <= e);
                if reached_top || past_edge {
                    covering_new = false;
                    if !past_edge {
                        // Strictly above the stored edge: jump to it. The
                        // next loop-top bound check still applies; the
                        // no-progress guard recalibrates from the edge.
                        offset_id = resume.edge;
                        prev_edge = None;
                        continue;
                    }
                    // Reached or passed the edge in transit: the stored edge
                    // is behind us, so keep normal paging from here.
                }
                // Otherwise this window sits strictly above the covered
                // range: keep covering with the normal advancement below.
            }
            // Paging decision mirrors `load_history`.
            match more {
                Some(true) => {
                    let next = match offset {
                        Some(off) => Some(off),
                        None => min_id,
                    };
                    let Some(next_edge) = next else {
                        // Server claims more but supplies no cursor.
                        tracing::debug!(
                            target_key,
                            "new-message catch-up stalled: more_available without edge"
                        );
                        self.persist_catchup_demand(&ctx, offset_id, run_top).await;
                        return Ok(total);
                    };
                    if let Some(prev) = prev_edge {
                        if next_edge >= prev {
                            tracing::debug!(
                                target_key,
                                prev,
                                next_edge,
                                "new-message catch-up stalled: edge did not advance"
                            );
                            self.persist_catchup_demand(&ctx, offset_id, run_top).await;
                            return Ok(total);
                        }
                    }
                    prev_edge = Some(next_edge);
                    offset_id = Some(next_edge);
                    continue;
                }
                Some(false) => {
                    self.clear_catchup_demand(&ctx).await;
                    return Ok(total);
                }
                None => {
                    if empty || !full {
                        self.clear_catchup_demand(&ctx).await;
                        return Ok(total);
                    }
                    let Some(edge) = min_id else {
                        self.clear_catchup_demand(&ctx).await;
                        return Ok(total);
                    };
                    if let Some(prev) = prev_edge {
                        if edge >= prev {
                            tracing::debug!(
                                target_key,
                                prev,
                                edge,
                                "new-message catch-up stalled: edge did not advance"
                            );
                            self.persist_catchup_demand(&ctx, offset_id, run_top).await;
                            return Ok(total);
                        }
                    }
                    prev_edge = Some(edge);
                    offset_id = Some(edge);
                    continue;
                }
            }
        }
    }

    /// One fenced catch-up commit: hold `checkpoint_save_lock`, re-check
    /// the run fence under it with the preserved fence generation, then
    /// commit under `fence_principal` (never a function-start snapshot).
    /// Mirrors the journal shard commit path, so ingestion cannot race a
    /// logout or principal switch that lands between the check and the
    /// commit. Fence loss is `FenceLost` (caller drops the stale page with
    /// a scope-changed error and no effects); anything else is `Failed`
    /// (caller persists demand with the next offset).
    #[cfg(not(target_arch = "wasm32"))]
    async fn ingest_catchup_window(
        &self,
        ctx: &CatchupCommitCtx<'_>,
        messages: &[tinkle_schema::proto::Message],
        users: &[tinkle_schema::proto::User],
        chats: &[tinkle_schema::proto::Chat],
        exclude: &HashSet<i64>,
    ) -> Result<Vec<i64>, CatchupCommitError> {
        let _guard = ctx.mgr.checkpoint_save_lock().await;
        if !ctx
            .mgr
            .check_fence(ctx.fence_principal, ctx.fence_generation)
            .await
        {
            return Err(CatchupCommitError::FenceLost);
        }
        self.ingest_messages(
            ctx.fence_principal,
            ctx.target_key,
            messages,
            users,
            chats,
            exclude,
        )
        .await
        .map_err(CatchupCommitError::Failed)
    }

    /// Persist an unfinished catch-up: durable `background_sync_jobs` row
    /// when the table exists, plus the in-memory demand entry. Never fails
    /// the run (a skipped persist just retries from the retained edge).
    /// `next_offset` is the `GetHistory` offset this run stopped at and
    /// `run_top` the newest id it observed. The stored edge only ever moves
    /// deeper: it is replaced solely by a deeper offset, so a persist from
    /// the covering phase (whose offsets sit above the edge) can never
    /// clobber it with a shallower one. The cover top is claimed from the
    /// current run only when nothing is stored yet, and retained
    /// afterwards, so the next run can prove connection to the contiguous
    /// range. A legacy row (edge without a proven top) keeps an empty top
    /// so resume pages through the edge instead of jumping over
    /// potentially uncached arrivals. Both travel in-memory and in the
    /// durable payload.
    /// Holds `checkpoint_save_lock` and re-checks the fence first: a stale
    /// run (logout/principal switch mid-flight) skips the write instead of
    /// recording demand under the wrong scope. Durable writes go under
    /// `fence_principal`, never a function-start snapshot.
    #[cfg(not(target_arch = "wasm32"))]
    async fn persist_catchup_demand(
        &self,
        ctx: &CatchupCommitCtx<'_>,
        next_offset: Option<i64>,
        run_top: Option<i64>,
    ) {
        let _guard = ctx.mgr.checkpoint_save_lock().await;
        if !ctx
            .mgr
            .check_fence(ctx.fence_principal, ctx.fence_generation)
            .await
        {
            return;
        }
        let resume = {
            let mut pending = self.catchup_pending.lock().await;
            let stored = pending.get(ctx.target_key).copied().unwrap_or_default();
            // The edge only ever moves deeper: a covering-phase persist
            // (whose offsets sit above the edge) can never clobber it with
            // a shallower one. The established cover top is retained; the
            // current run's top is claimed only when nothing is stored.
            let edge = match (stored.edge, next_offset) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, b) => b,
            };
            let cover_top = match (stored.edge, stored.cover_top, run_top) {
                // Retained top describes the contiguous range already proven;
                // never replace it mid-chain.
                (_, Some(t), _) => Some(t),
                // Legacy row (edge without a proven top): keep it empty so the
                // resume fallback (top = edge) pages through the edge instead
                // of jumping over potentially uncached arrivals.
                (Some(_), None, _) => None,
                // Fresh claim: the current run's top starts the proven range.
                (None, None, c) => c,
            };
            let resume = CatchupResume {
                edge,
                cover_top: if edge.is_some() { cover_top } else { None },
            };
            pending.insert(ctx.target_key.to_string(), resume);
            resume
        };
        let storage = match self.sqlite.clone() {
            Some(s) => s,
            _ => return,
        };
        let principal = ctx.fence_principal.to_string();
        let target_key = ctx.target_key.to_string();
        let job_id = format!("catchup:{target_key}");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let job = tinkle_storage::BackgroundSyncJob {
            job_id: job_id.clone(),
            kind: "new_messages".to_string(),
            target_key,
            state: "pending".to_string(),
            payload: Self::encode_resume_state(resume),
            attempts: 0,
            next_due: now,
            claimed_by: None,
            updated_at: now,
        };
        let result = tokio::task::spawn_blocking(move || {
            storage.enqueue_background_sync_job(&principal, &job)
        })
        .await;
        match result {
            // Ok(true) = durable row written; Ok(false) = table missing,
            // in-memory demand above is the retry path.
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::debug!(target_key = ctx.target_key, error = %e, "catch-up job persist failed");
            }
            Err(e) => {
                tracing::debug!(target_key = ctx.target_key, error = %e, "catch-up job task failed");
            }
        }
    }

    /// Clear a finished catch-up from both the durable table (when present)
    /// and the in-memory map. Missing-table is not an error. Holds
    /// `checkpoint_save_lock` and re-checks the fence first: a stale run
    /// that reaches the boundary after a scope change leaves the live
    /// scope's demand alone instead of clearing it.
    #[cfg(not(target_arch = "wasm32"))]
    async fn clear_catchup_demand(&self, ctx: &CatchupCommitCtx<'_>) {
        let _guard = ctx.mgr.checkpoint_save_lock().await;
        if !ctx
            .mgr
            .check_fence(ctx.fence_principal, ctx.fence_generation)
            .await
        {
            return;
        }
        {
            let mut pending = self.catchup_pending.lock().await;
            pending.remove(ctx.target_key);
        }
        let storage = match self.sqlite.clone() {
            Some(s) => s,
            _ => return,
        };
        let principal = ctx.fence_principal.to_string();
        let job_id = format!("catchup:{}", ctx.target_key);
        let _ = tokio::task::spawn_blocking(move || {
            storage.delete_background_sync_job(&principal, &job_id)
        })
        .await;
    }

    /// Encode a catch-up resume state for `BackgroundSyncJob.payload`.
    /// A missing edge encodes as empty (demand starting newest); the top
    /// rides along only when an edge is present. Older single-edge rows
    /// (`resume_offset=N` without a top) still decode.
    #[cfg(not(target_arch = "wasm32"))]
    fn encode_resume_state(resume: CatchupResume) -> Vec<u8> {
        match (resume.edge, resume.cover_top) {
            (Some(edge), Some(top)) => format!("resume_offset={edge};cover_top={top}").into_bytes(),
            (Some(edge), None) => format!("resume_offset={edge}").into_bytes(),
            (None, _) => Vec::new(),
        }
    }

    /// Decode a catch-up resume state; garbage (or a non-positive edge)
    /// means start newest.
    #[cfg(not(target_arch = "wasm32"))]
    fn decode_resume_state(payload: &[u8]) -> CatchupResume {
        let text = match std::str::from_utf8(payload) {
            Ok(t) => t,
            Err(_) => return CatchupResume::default(),
        };
        let mut edge = None;
        let mut cover_top = None;
        for part in text.split(';') {
            let Some((k, v)) = part.split_once('=') else {
                continue;
            };
            let Ok(n) = v.parse::<i64>() else {
                continue;
            };
            if n <= 0 {
                continue;
            }
            match k {
                "resume_offset" => edge = Some(n),
                "cover_top" => cover_top = Some(n),
                _ => {}
            }
        }
        // A top without an edge is meaningless (same invariant as persist).
        let cover_top = if edge.is_some() { cover_top } else { None };
        CatchupResume { edge, cover_top }
    }

    /// Load durable catch-up resume edges for one principal into the
    /// in-memory map (pre-restart runs persisted them via
    /// `persist_catchup_demand`). Runs at login; the sweep then resumes
    /// from these edges. Best-effort: a missing table or no rows just
    /// means no stored resume state. Only fills gaps: a live in-memory
    /// edge (same process, deeper run) always wins over the durable row.
    #[cfg(not(target_arch = "wasm32"))]
    async fn hydrate_catchup_resume_edges(&self, principal: &str) {
        let storage = match self.sqlite.clone() {
            Some(s) => s,
            None => return,
        };
        let p = principal.to_string();
        let jobs = match tokio::task::spawn_blocking(move || {
            storage.list_background_sync_jobs(&p, Some("pending"))
        })
        .await
        {
            Ok(Ok(j)) => j,
            _ => return,
        };
        if jobs.is_empty() {
            return;
        }
        let mut pending = self.catchup_pending.lock().await;
        for job in jobs {
            if job.kind != "new_messages" {
                continue;
            }
            pending
                .entry(job.target_key)
                .or_insert_with(|| Self::decode_resume_state(&job.payload));
        }
    }

    /// Best-effort history heal for a journal skip: fetch the latest history
    /// window and repopulate the offline cache without moving the journal
    /// cursor. Failures are logged, never fatal to the reconcile run.
    /// Uses the shared `ingest_messages` helper (same-cursor commit).
    async fn heal_via_history(&self, target: &tinkle_schema::proto::Target, target_key: &str) {
        let req = tinkle_schema::proto::GetHistoryRequest {
            target: Some(*target),
            offset_id: None,
            offset_date: None,
            add_offset: 0,
            limit: 100,
            max_id: None,
            min_id: None,
        };
        match self.messages().get_history(req).await {
            Ok(resp) => {
                #[cfg(not(target_arch = "wasm32"))]
                if self.sqlite.is_some() {
                    let principal = self.principal().await;
                    if principal != UNKNOWN_PRINCIPAL {
                        let empty = HashSet::new();
                        let _ = self
                            .ingest_messages(
                                &principal,
                                target_key,
                                &resp.messages,
                                &resp.users,
                                &resp.chats,
                                &empty,
                            )
                            .await;
                    }
                }
                tracing::debug!(
                    target_key,
                    messages = resp.messages.len(),
                    "history heal fetched"
                );
            }
            Err(e) => {
                tracing::debug!(target_key, error = %e, "history heal failed");
            }
        }
    }

    /// Reconcile every tracked target once. Returns after all runs settle;
    /// per-target failures surface as `ReconcileFailed` events, not as an
    /// early return.
    pub async fn reconcile_now(&self) -> Result<(), TinkleError> {
        let keys = self.tracked_targets().await;
        for key in keys {
            let _ = self.reconcile_target(&key).await;
        }
        Ok(())
    }

    /// Durable journal cursor for one target (`None` when never reconciled).
    /// Local read: works offline.
    pub async fn target_cursor(&self, target_key: &str) -> Option<i64> {
        let principal = self.principal().await;
        if principal == UNKNOWN_PRINCIPAL {
            return None;
        }
        self.store
            .load_journal(&principal, target_key)
            .await
            .ok()
            .flatten()
    }

    /// Per-target reconcile status. `None` when offline or untracked.
    pub async fn target_status(&self, target_key: &str) -> Option<ReconcileStatus> {
        match self.live_mgr().await {
            Ok(mgr) => mgr.target_status(target_key).await,
            Err(_) => None,
        }
    }

    /// Tracked conversation keys.
    pub async fn tracked_targets(&self) -> Vec<String> {
        self.interests
            .lock()
            .await
            .targets
            .iter()
            .filter_map(|t| target_key_of(t).ok())
            .collect()
    }

    /// Register tracked conversations and kick reconciliation.
    ///
    /// Flow: validate principal → track → demand each target. On the manual
    /// path this awaits one reconcile pass over all tracked targets so
    /// callers observe `ConversationChanged`/`MessagesChanged` without a
    /// supervisor; under `start` the supervisor also reschedules demand.
    pub async fn subscribe(
        &self,
        targets: Vec<tinkle_schema::proto::Target>,
    ) -> Result<(), TinkleError> {
        let _sub = self.sub_lock.lock().await;
        let mgr = self.live_mgr().await?;
        let principal = self.principal().await;
        if principal == UNKNOWN_PRINCIPAL {
            return Err(TinkleError::Subscription(
                "authenticated principal unknown; call set_principal_user_id after sign_in".into(),
            ));
        }
        for t in &targets {
            target_key_of(t)?;
        }
        mgr.track_targets(targets.clone()).await;
        {
            let mut g = self.interests.lock().await;
            // Merge: keep existing plus new (dedupe by key).
            let mut keys: HashSet<String> = g
                .targets
                .iter()
                .filter_map(|t| target_key_of(t).ok())
                .collect();
            for t in targets {
                if let Ok(k) = target_key_of(&t) {
                    if keys.insert(k) {
                        g.targets.push(t);
                    }
                }
            }
            g.subscribed = true;
        }
        // Kick: one pass now (manual path); supervisor reschedules demand.
        let keys = self.tracked_targets().await;
        for key in keys {
            if let Err(e) = self.reconcile_target(&key).await {
                tracing::debug!(target_key = %key, error = %e, "initial reconcile failed");
            }
        }
        Ok(())
    }

    /// Stop tracking conversations: drop interests, untrack in the live
    /// manager (emits `ConversationRemoved` per key so hosts drop cached
    /// rows), and drop journal cursors so a future re-subscribe starts at
    /// 0 with a history heal. Best-effort when offline (interests + stored
    /// cursors are still dropped; no event without a live manager).
    pub async fn unsubscribe(&self, target_keys: &[String]) -> Result<(), TinkleError> {
        let _sub = self.sub_lock.lock().await;
        {
            let mut g = self.interests.lock().await;
            g.targets.retain(|t| {
                target_key_of(t)
                    .map(|k| !target_keys.contains(&k))
                    .unwrap_or(true)
            });
            if g.targets.is_empty() {
                g.subscribed = false;
            }
        }
        if let Ok(mgr) = self.live_mgr().await {
            mgr.untrack_targets(target_keys).await;
        }
        let principal = self.principal().await;
        if principal != UNKNOWN_PRINCIPAL {
            for key in target_keys {
                let _ = self.store.remove_target(&principal, key).await;
            }
        }
        Ok(())
    }

    pub async fn next_event(&self) -> Option<SyncEvent> {
        loop {
            {
                let mut retained = self.drain_mgrs.lock().await;
                let i = 0;
                while i < retained.len() {
                    if let Some(evt) = retained[i].try_next_event().await {
                        return Some(evt);
                    }
                    retained.remove(i);
                }
            }
            let mgr = match self.live_mgr().await {
                Ok(m) => m,
                Err(_) => return None,
            };
            if let Some(evt) = mgr.try_next_event().await {
                return Some(evt);
            }
            match tokio::time::timeout(std::time::Duration::from_millis(200), mgr.next_event())
                .await
            {
                Ok(evt) => return evt,
                Err(_) => {
                    let current = self.live_mgr().await.ok();
                    let rotated = match current {
                        Some(cur) => !Arc::ptr_eq(&cur, &mgr),
                        None => true,
                    };
                    if rotated {
                        self.retain_manager(mgr).await;
                    }
                }
            }
        }
    }

    async fn retain_manager(&self, mgr: Arc<SyncManager>) {
        let mut retained = self.drain_mgrs.lock().await;
        if retained.iter().any(|m| Arc::ptr_eq(m, &mgr)) {
            return;
        }
        retained.push(mgr);
        while retained.len() > 2 {
            retained.remove(0);
        }
    }

    async fn teardown_connection(&self) {
        if let Some(live) = self.live.write().await.take() {
            live.handle.close().await;
            live.pump.abort();
            live.sync_mgr.on_disconnect().await;
            self.retain_manager(live.sync_mgr).await;
        }
        self.transport_ok.store(false, Ordering::SeqCst);
    }

    pub async fn close(&self) {
        self.stop().await;
    }

    pub async fn stop(&self) {
        self.stop_supervisor().await;
        self.teardown_connection().await;
    }

    async fn stop_supervisor(&self) {
        let (handle, shutdown) = {
            let mut sup = self.sup_state.lock().await;
            sup.epoch += 1;
            (sup.handle.take(), sup.shutdown.take())
        };
        if let Some(tx) = shutdown {
            let _ = tx.send(());
        }
        if let Some(h) = handle {
            h.abort();
        }
        *self.signal_tx.lock().await = None;
    }

    pub async fn suspend(&self) {
        self.suspended.store(true, Ordering::SeqCst);
        self.teardown_connection().await;
    }

    pub async fn resume(&self) {
        self.suspended.store(false, Ordering::SeqCst);
    }

    pub async fn is_suspended(&self) -> bool {
        self.suspended.load(Ordering::SeqCst)
    }

    pub async fn reconnect(&self) -> Result<(), TinkleError> {
        if self.suspended.load(Ordering::SeqCst) {
            return Err(TinkleError::NotConnected(
                "suspended; reconnect suppressed".into(),
            ));
        }
        self.teardown_connection().await;
        let policy = self.config.reconnect.clone();
        let mut attempt: u32 = 0;
        loop {
            if self.suspended.load(Ordering::SeqCst) {
                self.teardown_connection().await;
                return Err(TinkleError::NotConnected(
                    "suspended during reconnect".into(),
                ));
            }
            match self.connect().await {
                Ok(()) => {
                    if self.suspended.load(Ordering::SeqCst) {
                        self.teardown_connection().await;
                        return Err(TinkleError::NotConnected("suspended after connect".into()));
                    }
                    let (subscribed, targets) = {
                        let g = self.interests.lock().await;
                        (g.subscribed, g.targets.clone())
                    };
                    if subscribed {
                        if let Err(e) = self.subscribe(targets).await {
                            self.teardown_connection().await;
                            return Err(e);
                        }
                    }
                    return Ok(());
                }
                Err(e) => {
                    if self.suspended.load(Ordering::SeqCst) {
                        self.teardown_connection().await;
                        return Err(TinkleError::NotConnected(
                            "suspended during reconnect".into(),
                        ));
                    }
                    if !should_retry(&policy, attempt) {
                        return Err(TinkleError::ReconnectExhausted { attempts: attempt });
                    }
                    tracing::debug!(attempt, error = %e, "reconnect backoff");
                    backoff_sleep(&policy, attempt).await;
                    attempt += 1;
                }
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn start(
        &self,
        targets: Vec<tinkle_schema::proto::Target>,
    ) -> Result<(), TinkleError> {
        let _guard = self.start_lock.lock().await;
        {
            let sup = self.sup_state.lock().await;
            if sup.handle.is_some() {
                return Ok(());
            }
        }
        let epoch = self.sup_state.lock().await.epoch;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (sig_tx, sig_rx) = mpsc::channel(64);
        self.suspended.store(false, Ordering::SeqCst);
        *self.signal_tx.lock().await = Some(sig_tx);
        let result = self.establish(targets).await;
        match result {
            Ok(()) => {
                let mut sup = self.sup_state.lock().await;
                if sup.epoch != epoch || sup.handle.is_some() {
                    return Err(TinkleError::NotConnected("stopped during start".into()));
                }
                let task_client = self.clone();
                let handle =
                    tokio::spawn(async move { task_client.supervise(sig_rx, shutdown_rx).await });
                sup.handle = Some(handle);
                sup.shutdown = Some(shutdown_tx);
                sup.last_result = Some(());
                Ok(())
            }
            Err(e) => {
                *self.signal_tx.lock().await = None;
                self.teardown_connection().await;
                Err(e)
            }
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn start(
        &self,
        _targets: Vec<tinkle_schema::proto::Target>,
    ) -> Result<(), TinkleError> {
        let _guard = self.start_lock.lock().await;
        Err(TinkleError::NotConnected(
            "supervised lifecycle unavailable on wasm; use connect/subscribe".into(),
        ))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn establish(
        &self,
        targets: Vec<tinkle_schema::proto::Target>,
    ) -> Result<(), TinkleError> {
        let epoch = self.sup_state.lock().await.epoch;
        if !self.is_connected().await {
            self.connect().await?;
            if self.sup_state.lock().await.epoch != epoch {
                self.teardown_connection().await;
                return Err(TinkleError::NotConnected("stopped during start".into()));
            }
        }
        if let Some(tx) = self.signal_tx.lock().await.clone() {
            self.live_mgr().await?.set_signal_tx(tx).await;
        }
        let result = self.subscribe(targets).await;
        if self.sup_state.lock().await.epoch != epoch {
            self.teardown_connection().await;
            return Err(TinkleError::NotConnected("stopped during start".into()));
        }
        result
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn supervise(
        self,
        mut sig_rx: mpsc::Receiver<RecoverySignal>,
        mut shutdown: oneshot::Receiver<()>,
    ) {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut demand: HashMap<String, TargetDemand> = HashMap::new();
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = tick.tick() => {
                    if self.suspended.load(Ordering::SeqCst) {
                        continue;
                    }
                    if !self.is_connected().await {
                        self.reestablish().await;
                    }
                    if self.is_connected().await {
                        self.recover_due(&mut demand).await;
                        // Bounded `try_send` signals can drop under pressure
                        // without ever entering `demand`. Sweep tracked +
                        // dirty + catch-up markers so a dropped `Demand` is
                        // retried instead of stalling until the next hint.
                        self.sweep_tracked(&mut demand).await;
                    }
                }
                Some(signal) = sig_rx.recv() => {
                    match signal {
                        RecoverySignal::PumpDead => {
                            self.reestablish().await;
                        }
                        RecoverySignal::Demand { target_key }
                        | RecoverySignal::ReconcileFailed { target_key } => {
                            demand
                                .entry(target_key.clone())
                                .or_insert_with(TargetDemand::fresh);
                            self.recover_one(&target_key, &mut demand).await;
                        }
                        RecoverySignal::CheckpointFailed {
                            target_key,
                            accept_seq,
                        } => {
                            tracing::debug!(
                                target_key = %target_key,
                                accept_seq,
                                "checkpoint failed; rescheduling reconcile"
                            );
                            demand
                                .entry(target_key.clone())
                                .or_insert_with(TargetDemand::fresh);
                            self.recover_one(&target_key, &mut demand).await;
                        }
                    }
                }
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn recover_due(&self, demand: &mut HashMap<String, TargetDemand>) {
        // Drop demand for untracked targets first: stale queued demand must
        // not recreate reconciliation state after `unsubscribe`. Runs
        // already in flight still complete (single-flight is untouched).
        if let Ok(mgr) = self.live_mgr().await {
            let tracked: HashSet<String> = mgr.tracked_keys().await.into_iter().collect();
            demand.retain(|k, _| tracked.contains(k));
        }
        let now = std::time::Instant::now();
        let due: Vec<String> = demand
            .iter()
            .filter(|(_, d)| now >= d.next_due)
            .map(|(s, _)| s.clone())
            .collect();
        for target_key in due {
            self.recover_one(&target_key, demand).await;
        }
    }

    /// Periodic safety net for best-effort `Demand` signals: the pump and
    /// `end_reconcile` use a bounded 64-slot `try_send` that drops under
    /// pressure without entering `demand`. Sweep tracked + dirty +
    /// catch-up markers and (re)queue anything that looks unreconciled so
    /// a dropped signal is retried on the next tick instead of stalling.
    #[cfg(not(target_arch = "wasm32"))]
    async fn sweep_tracked(&self, demand: &mut HashMap<String, TargetDemand>) {
        let mgr = match self.live_mgr().await {
            Ok(m) => m,
            Err(_) => return,
        };
        let keys = mgr.tracked_keys().await;
        // Prune demand for untracked targets before anything else: stale
        // queued demand must not recreate reconciliation state after
        // `unsubscribe`. Already-active runs still complete (single-flight
        // entries in `in_flight` are untouched).
        let tracked: HashSet<String> = keys.iter().cloned().collect();
        demand.retain(|k, _| tracked.contains(k));
        if keys.is_empty() {
            return;
        }
        let dirty: HashSet<String> = mgr.dirty_keys().await.into_iter().collect();
        let pending_catchup: HashSet<String> =
            self.catchup_pending.lock().await.keys().cloned().collect();
        let mut added = Vec::new();
        for key in keys {
            if demand.contains_key(&key) {
                continue;
            }
            if mgr.is_reconciling(&key).await {
                continue;
            }
            let failed = matches!(
                mgr.target_status(&key).await,
                Some(crate::sync::ReconcileStatus::Failed)
            );
            if failed || dirty.contains(&key) || pending_catchup.contains(&key) {
                demand
                    .entry(key.clone())
                    .or_insert_with(TargetDemand::fresh);
                added.push(key);
            }
        }
        // `recover_due` already ran this tick; drive only the entries this
        // sweep (re)queued (fresh entries are immediately due).
        for target_key in added {
            self.recover_one(&target_key, demand).await;
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn reestablish(&self) {
        if self.suspended.load(Ordering::SeqCst) {
            return;
        }
        {
            let mut sup = self.sup_state.lock().await;
            let now = std::time::Instant::now();
            if let Some(last) = sup.last_reestablish {
                if now.duration_since(last) < std::time::Duration::from_secs(10) {
                    return;
                }
            }
            sup.last_reestablish = Some(now);
        }
        if self.is_connected().await {
            return;
        }
        tracing::debug!("supervisor re-establishing connection");
        if let Err(e) = self.reconnect().await {
            tracing::debug!(error = %e, "supervisor re-establish failed, will retry");
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn recover_one(&self, target_key: &str, demand: &mut HashMap<String, TargetDemand>) {
        let now = std::time::Instant::now();
        if let Some(d) = demand.get(target_key) {
            if now < d.next_due {
                return;
            }
        }
        let mgr = match self.live_mgr().await {
            Ok(m) => m,
            Err(_) => return,
        };
        // A queued signal can outlive `unsubscribe`: never reconcile a
        // target the live manager no longer tracks. Drop its demand so it
        // cannot restore removed state.
        if !mgr.tracked_keys().await.iter().any(|k| k == target_key) {
            demand.remove(target_key);
            return;
        }
        if mgr.is_reconciling(target_key).await {
            if let Some(d) = demand.get_mut(target_key) {
                d.next_due = now + std::time::Duration::from_secs(5);
            }
            return;
        }
        let mut ok = false;
        for _ in 0..3 {
            match self.reconcile_target(target_key).await {
                Ok(_) => {
                    ok = true;
                    break;
                }
                Err(e) => match e.recovery_decision() {
                    RecoveryDecision::Retry => {
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        continue;
                    }
                    _ => break,
                },
            }
        }
        if ok {
            demand.remove(target_key);
        } else {
            let failures = demand.get(target_key).map(|d| d.failures).unwrap_or(0) + 1;
            demand.insert(
                target_key.to_string(),
                TargetDemand {
                    failures,
                    next_due: now + TargetDemand::backoff_for(failures),
                },
            );
        }
    }

    /// Fetch message history AND hydrate the offline cache (native only).
    ///
    /// History paging follows `more_available`: `Some(true)` means more,
    /// `Some(false)` means done, `None` falls back to the legacy
    /// window-edge heuristic (continue while the window came back full,
    /// advancing `offset_id` to the window edge; `count` stays
    /// informational). Ingestion upserts authoritative users/chats plus
    /// history message bodies (same-cursor journal commit, never advancing
    /// the journal cursor). Best-effort: storage failures are logged and the
    /// fetched pages are still returned as one combined response.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn load_history(
        &self,
        req: tinkle_schema::proto::GetHistoryRequest,
    ) -> Result<tinkle_schema::proto::GetHistoryResponse, TinkleError> {
        if req.target.is_none() {
            return Err(TinkleError::Codec(
                "GetHistoryRequest.target missing".into(),
            ));
        }
        let limit = if req.limit <= 0 {
            20
        } else {
            req.limit.min(100)
        };
        let mut cur = req;
        cur.limit = limit;
        let mut all_messages = Vec::new();
        let mut all_users = Vec::new();
        let mut all_chats = Vec::new();
        for _ in 0..10 {
            let resp = self.messages().get_history(cur).await?;
            let more = resp.more_available;
            let offset = resp.offset_id_offset;
            all_users.extend(resp.users);
            all_chats.extend(resp.chats);
            let empty = resp.messages.is_empty();
            let full = resp.messages.len() as i32 >= limit;
            // Edge for the legacy fallback: smallest id in this window.
            let min_id = resp.messages.iter().map(|m| m.message_id).min();
            all_messages.extend(resp.messages);
            match more {
                Some(true) => {
                    if let Some(off) = offset {
                        cur.offset_id = Some(off);
                    } else if let Some(min) = min_id {
                        cur.offset_id = Some(min);
                    } else {
                        break;
                    }
                    continue;
                }
                Some(false) => break,
                None => {
                    if empty || !full {
                        break;
                    }
                    if let Some(min) = min_id {
                        cur.offset_id = Some(min);
                        continue;
                    }
                    break;
                }
            }
        }
        // Dedupe users/chats by id for the combined response.
        {
            use std::collections::HashMap;
            let mut seen_u = HashMap::new();
            for u in all_users {
                let id = match u.user_type.as_ref() {
                    Some(tinkle_schema::proto::user::UserType::Data(d)) => d.id,
                    Some(tinkle_schema::proto::user::UserType::Empty(e)) => e.id,
                    None => 0,
                };
                if id > 0 {
                    seen_u.insert(id, u);
                }
            }
            all_users = seen_u.into_values().collect();
            let mut seen_c = HashMap::new();
            for c in all_chats {
                if c.chat_id > 0 {
                    seen_c.insert(c.chat_id, c);
                }
            }
            all_chats = seen_c.into_values().collect();
        }
        // HydrateCache via the shared ingest helper (same-cursor commit,
        // never moves the journal cursor). Single call covers messages +
        // entities, replacing the old split ingest paths.
        if let Some(t) = req.target.as_ref() {
            if let Ok(key) = target_key_of(t) {
                let principal = self.principal().await;
                if principal != UNKNOWN_PRINCIPAL {
                    let empty = HashSet::new();
                    let _ = self
                        .ingest_messages(
                            &principal,
                            &key,
                            &all_messages,
                            &all_users,
                            &all_chats,
                            &empty,
                        )
                        .await;
                }
            }
        }
        Ok(tinkle_schema::proto::GetHistoryResponse {
            messages: all_messages,
            users: all_users,
            chats: all_chats,
            count: 0,
            offset_id_offset: None,
            more_available: Some(false),
        })
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn load_history(
        &self,
        req: tinkle_schema::proto::GetHistoryRequest,
    ) -> Result<tinkle_schema::proto::GetHistoryResponse, TinkleError> {
        if req.target.is_none() {
            return Err(TinkleError::Codec(
                "GetHistoryRequest.target missing".into(),
            ));
        }
        let limit = if req.limit <= 0 {
            20
        } else {
            req.limit.min(100)
        };
        let mut cur = req;
        cur.limit = limit;
        let mut all_messages = Vec::new();
        let mut all_users = Vec::new();
        let mut all_chats = Vec::new();
        for _ in 0..10 {
            let resp = self.messages().get_history(cur).await?;
            let more = resp.more_available;
            let offset = resp.offset_id_offset;
            all_users.extend(resp.users);
            all_chats.extend(resp.chats);
            let empty = resp.messages.is_empty();
            let full = resp.messages.len() as i32 >= limit;
            let min_id = resp.messages.iter().map(|m| m.message_id).min();
            all_messages.extend(resp.messages);
            match more {
                Some(true) => {
                    if let Some(off) = offset {
                        cur.offset_id = Some(off);
                    } else if let Some(min) = min_id {
                        cur.offset_id = Some(min);
                    } else {
                        break;
                    }
                    continue;
                }
                Some(false) => break,
                None => {
                    if empty || !full {
                        break;
                    }
                    if let Some(min) = min_id {
                        cur.offset_id = Some(min);
                        continue;
                    }
                    break;
                }
            }
        }
        Ok(tinkle_schema::proto::GetHistoryResponse {
            messages: all_messages,
            users: all_users,
            chats: all_chats,
            count: 0,
            offset_id_offset: None,
            more_available: Some(false),
        })
    }

    /// Fetch dialogs AND hydrate the offline cache (native only).
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn load_dialogs(
        &self,
        req: tinkle_schema::proto::GetDialogsRequest,
    ) -> Result<tinkle_schema::proto::GetDialogsResponse, TinkleError> {
        let resp = self.messages().get_dialogs(req).await?;
        self.ingest_snapshot_entities(&resp.users, &resp.chats)
            .await;
        self.ingest_snapshot_dialogs(&resp.dialogs).await;
        Ok(resp)
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn load_dialogs(
        &self,
        req: tinkle_schema::proto::GetDialogsRequest,
    ) -> Result<tinkle_schema::proto::GetDialogsResponse, TinkleError> {
        self.messages().get_dialogs(req).await
    }

    /// Dialog-list entity hydration (global pins). Shares entity encoding
    /// with the message ingest path via `cache_projector`; dialogs
    /// themselves stay global (no single target_key to scope to).
    #[cfg(not(target_arch = "wasm32"))]
    async fn ingest_snapshot_entities(
        &self,
        users: &[tinkle_schema::proto::User],
        chats: &[tinkle_schema::proto::Chat],
    ) {
        let (principal, storage) = match (self.principal().await, self.sqlite.clone()) {
            (p, Some(s)) if p != UNKNOWN_PRINCIPAL => (p, s),
            _ => return,
        };
        if users.is_empty() && chats.is_empty() {
            return;
        }
        // Shared encoding with ingest_messages (scoped path uses the same
        // helpers, then commits with scoped provenance; here the commit is
        // the global-pin upsert used by dialog snapshots). File refs share
        // records (scope "") and persist as global pins alongside entities.
        let ops = crate::cache_projector::project_entities_to_ops(users, chats);
        if ops.users.is_empty()
            && ops.chats.is_empty()
            && ops.files.is_empty()
            && ops.entity_files.is_empty()
            && ops.replace_entity_files.is_empty()
        {
            return;
        }
        let result = tokio::task::spawn_blocking(move || {
            for row in &ops.users {
                if let Err(e) = storage.upsert_user(&principal, row) {
                    tracing::debug!(error = %e, "history user ingest failed");
                }
            }
            for row in &ops.chats {
                if let Err(e) = storage.upsert_chat(&principal, row) {
                    tracing::debug!(error = %e, "history chat ingest failed");
                }
            }
            // Global-pin file metadata + entity refs (avatars). First-wins
            // metadata, replace-per-entity refs — same shared-record path as
            // journal (stale avatar refs are collected, not leaked).
            for row in &ops.files {
                if let Err(e) = storage.upsert_file(&principal, row) {
                    tracing::debug!(error = %e, "history file ingest failed");
                }
            }
            {
                use std::collections::HashMap;
                let mut by_entity: HashMap<(&str, i64), Vec<&tinkle_storage::EntityFileRef>> =
                    HashMap::new();
                for r in &ops.entity_files {
                    by_entity
                        .entry((r.kind.as_str(), r.entity_id))
                        .or_default()
                        .push(r);
                }
                for (kind, entity_id) in &ops.replace_entity_files {
                    let refs: Vec<tinkle_storage::EntityFileRef> = by_entity
                        .get(&(kind.as_str(), *entity_id))
                        .map(|v| v.iter().map(|r| (*r).clone()).collect())
                        .unwrap_or_default();
                    if let Err(e) =
                        storage.replace_entity_file_refs(&principal, kind, *entity_id, &refs)
                    {
                        tracing::debug!(error = %e, "history entity-file ingest failed");
                    }
                }
            }
        })
        .await;
        if result.is_err() {
            tracing::debug!("history entity ingest task failed");
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn ingest_snapshot_dialogs(&self, dialogs: &[tinkle_schema::proto::Dialog]) {
        use prost::Message as _;
        let (principal, storage) = match (self.principal().await, self.sqlite.clone()) {
            (p, Some(s)) if p != UNKNOWN_PRINCIPAL => (p, s),
            _ => return,
        };
        if dialogs.is_empty() {
            return;
        }
        let mut rows = Vec::with_capacity(dialogs.len());
        for d in dialogs {
            let (peer_kind, peer_id) = match d.target.as_ref().and_then(|t| t.kind.as_ref()) {
                Some(tinkle_schema::proto::target::Kind::ChatId(id)) => ("room".to_string(), *id),
                Some(tinkle_schema::proto::target::Kind::UserId(id)) => ("user".to_string(), *id),
                None => continue,
            };
            if peer_id <= 0 {
                continue;
            }
            let mut proto = Vec::new();
            if d.encode(&mut proto).is_err() {
                continue;
            }
            // Authoritative snapshot: global pin.
            // Derive origin from target for scoping; dialogs list itself is
            // global, so pin globally (clears pending on write).
            let origin = String::new();
            let _ = peer_kind.clone();
            rows.push(tinkle_storage::DialogRow {
                peer_kind,
                peer_id,
                top_message: d.top_message,
                unread_count: d.unread_count,
                read_inbox_max_id: d.read_inbox_max_id,
                read_outbox_max_id: d.read_outbox_max_id,
                pinned: d.pinned,
                proto,
                origin_stream: origin,
                read_state_pending: false,
            });
        }
        if rows.is_empty() {
            return;
        }
        let result = tokio::task::spawn_blocking(move || {
            for row in &rows {
                if let Err(e) = storage.upsert_dialog_authoritative(&principal, row, true) {
                    tracing::debug!(error = %e, "dialog snapshot ingest failed");
                }
            }
        })
        .await;
        if result.is_err() {
            tracing::debug!("dialog snapshot ingest task failed");
        }
    }

    pub async fn rotate_credential(&self, next: Credential) -> Result<(), TinkleError> {
        let was_supervised = self.sup_state.lock().await.handle.is_some();
        let had_interests = self.interests.lock().await.subscribed;
        self.teardown_connection().await;
        *self.credential.write().await = next;
        if was_supervised || had_interests {
            self.reconnect().await
        } else {
            Ok(())
        }
    }

    pub async fn pending_len(&self) -> usize {
        match self.live.read().await.as_ref() {
            Some(l) => l.handle.connection().pending_len(),
            None => 0,
        }
    }

    pub async fn connection_id(&self) -> Option<String> {
        self.live
            .read()
            .await
            .as_ref()
            .map(|l| l.connection_id.clone())
    }

    pub async fn version(&self) -> Option<u32> {
        self.live.read().await.as_ref().map(|l| l.version)
    }
}

#[cfg(test)]
mod tests {
    use super::TargetDemand;

    #[test]
    fn demand_backoff_grows_and_caps() {
        assert_eq!(
            TargetDemand::backoff_for(0),
            std::time::Duration::from_secs(2)
        );
        assert_eq!(
            TargetDemand::backoff_for(1),
            std::time::Duration::from_secs(4)
        );
        assert_eq!(
            TargetDemand::backoff_for(100),
            std::time::Duration::from_secs(60)
        );
        assert!(TargetDemand::fresh().next_due <= std::time::Instant::now());
    }

    #[test]
    fn resume_state_codec_round_trips() {
        use super::{CatchupResume, Client};
        let full = CatchupResume {
            edge: Some(151),
            cover_top: Some(250),
        };
        assert_eq!(
            Client::decode_resume_state(&Client::encode_resume_state(full)),
            full
        );
        // Edge without a top (older single-edge rows) still decodes.
        let edge_only = Client::decode_resume_state(b"resume_offset=151");
        assert_eq!(edge_only.edge, Some(151));
        assert_eq!(edge_only.cover_top, None);
        // No edge encodes as empty; garbage decodes as fresh demand.
        assert!(Client::encode_resume_state(CatchupResume::default()).is_empty());
        assert_eq!(Client::decode_resume_state(&[]), CatchupResume::default());
        assert_eq!(
            Client::decode_resume_state(b"resume_offset=0"),
            CatchupResume::default()
        );
        assert_eq!(
            Client::decode_resume_state(b"garbage"),
            CatchupResume::default()
        );
        assert_eq!(
            Client::decode_resume_state(b"resume_offset=-5;cover_top=10"),
            CatchupResume::default()
        );
    }
}
