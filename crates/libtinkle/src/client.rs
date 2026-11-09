//! High-level typed client: `connect → subscribe → reconcile`.
//!
//! Conceptual use:
//! ```ignore
//! client.restore_active_enrollment(secure_store).await?;
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
//! and body fetches (`GetMessages`) run over the same encrypted TCP connection;
//! ongoing delivery
//! is opaque `Notification` hints that kick per-target reconciliation.
//! Never auto-retries unsafe mutations without an application idempotency
//! policy.
//! ACTIVE Returning sockets own one five-second application KeepAlive loop;
//! Bootstrap sockets never issue that authenticated RPC. The live lifecycle
//! cancels/joins the loop and the existing supervisor alone reconnects.

use prost::Message;
#[cfg(not(target_arch = "wasm32"))]
use std::collections::HashMap;
use std::collections::HashSet;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};
use zeroize::Zeroizing;

use syntra_runtime::{CancelToken, Initiator};

use crate::config::{ClientConfig, TrustSource};
use crate::cursor::CursorStore;
use crate::errors::{RecoveryDecision, TinkleError};
use crate::liveness::{AbortTask, ClientOwner, LiveGeneration, LivenessTask};
use crate::reconnect::should_retry;
use crate::services::{Account, Auth, Files, Messages, Updates};
use crate::sync::{ReconcileStatus, RecoverySignal, SyncEvent, SyncManager, UNKNOWN_PRINCIPAL};

struct Live {
    handle: syntra_net::tcp::ClientHandle,
    connection_id: String,
    version: u32,
    sync_mgr: Arc<SyncManager>,
    pump: AbortTask,
    initialized: Arc<Initialization>,
    watch_restore: Option<AbortTask>,
    generation: Arc<LiveGeneration>,
    liveness: Option<LivenessTask>,
}

/// One connection's snapshot gate and non-awaiting restoration wakeup.
#[derive(Default)]
struct Initialization {
    ready: tokio::sync::Mutex<bool>,
    watch: tokio::sync::Notify,
    dispatched_deadline: std::sync::Mutex<Option<Arc<syntra_runtime::elapsed::ElapsedDeadline>>>,
    changed: tokio::sync::Notify,
}

/// An initialization owner may be retained without polling by its caller. Keep
/// its actual dispatched cutoff observable independently of that caller.
struct InitializationAttempt<'a> {
    initialized: &'a Initialization,
    deadline: Arc<syntra_runtime::elapsed::ElapsedDeadline>,
}

impl Drop for InitializationAttempt<'_> {
    /// Withdraw only this initializer's cutoff and wake queued heartbeat admission.
    fn drop(&mut self) {
        let mut current = self
            .initialized
            .dispatched_deadline
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if current
            .as_ref()
            .is_some_and(|deadline| Arc::ptr_eq(deadline, &self.deadline))
        {
            *current = None;
        }
        self.initialized.changed.notify_one();
    }
}

/// Distinguish local admission deferral from an RPC handed to Syntra.
#[derive(Default)]
struct CallProgress {
    dispatched: AtomicBool,
    deferred: AtomicBool,
}

impl Initialization {
    /// Serialize snapshot installation; ready unary calls release this gate before dispatch.
    async fn lock(&self) -> tokio::sync::MutexGuard<'_, bool> {
        self.ready.lock().await
    }
}

/// Route selection travels with the establishment gate, including Bootstrap children.
#[derive(Default)]
pub(crate) struct ConnectionGate {
    lock: tokio::sync::Mutex<()>,
    selected_port: AtomicU16,
}

impl ConnectionGate {
    /// Serialize dial, installation, and scope transitions across parent and provisioning clients.
    pub(crate) async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.lock.lock().await
    }
}

#[derive(Default)]
struct SupState {
    handle: Option<tokio::task::JoinHandle<()>>,
    shutdown: Option<oneshot::Sender<()>>,
    epoch: u64,
    /// Only a later suspend vetoes resume; supervisor stop is not suspension.
    suspension_epoch: u64,
    resume_cancel: Option<CancelToken>,
    #[cfg(not(target_arch = "wasm32"))]
    last_result: Option<()>,
    #[cfg(not(target_arch = "wasm32"))]
    next_reestablish: Option<std::time::Instant>,
    reconnect_failures: u32,
    reconnect_blocked: bool,
    /// Latest admitted attempt, including reconnects using unchanged credentials.
    reconnect_attempt: Option<Arc<()>>,
}

/// Policy bookkeeping belongs to one attempt, not just one credential source.
struct ReconnectFence {
    source_revision: u64,
    attempt: Arc<()>,
    generation: Option<Arc<LiveGeneration>>,
}

/// Join ownership for the single retained foreground transition.
struct ResumeTask {
    suspension_epoch: u64,
    task: tokio::task::JoinHandle<()>,
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
    /// Make newly observed target demand eligible for an immediate recovery attempt.
    fn fresh() -> Self {
        Self {
            failures: 0,
            next_due: std::time::Instant::now(),
        }
    }

    /// Double the target retry delay from two seconds, saturating at sixty seconds.
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
    /// In-flight covering descent position: the `GetHistory` offset the
    /// next covering run continues from. Set only while a covering chain
    /// is open (budget spent or failure mid-chain); cleared when the chain
    /// connects to `cover_top`, which widens the top instead.
    covering: Option<i64>,
    /// Newest id observed when the covering chain started: the value the
    /// `cover_top` widens to once the chain connects.
    covering_top: Option<i64>,
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

/// Grouped payload refs for one ingest commit (clippy
/// `too_many_arguments`: commit fields travel together). Native-only:
/// wasm has no SQLite commit to parameterize.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct IngestBatch<'a> {
    pub messages: &'a [tinkle_schema::proto::Message],
    pub users: &'a [tinkle_schema::proto::User],
    pub chats: &'a [tinkle_schema::proto::Chat],
    pub exclude: &'a HashSet<i64>,
}

/// Grouped scope inputs for one fenced catch-up commit (clippy
/// `too_many_arguments`: fence fields travel together).
#[cfg(not(target_arch = "wasm32"))]
struct CatchupCommitCtx<'a> {
    mgr: &'a SyncManager,
    fence_principal: &'a str,
    fence_generation: u64,
    target_key: &'a str,
    epoch: u64,
}

/// Typed Tinkle client over encrypted native Syntra TCP.
///
/// Cheap to clone (shares connection + cursors). The same logical runtime
/// is reusable across native iOS/Android via thin platform adapters.
#[derive(Clone)]
pub struct Client {
    pub(crate) authorization: Arc<crate::authorization::Authorization>,
    /// Retains cleanup ownership independently of any host waiter.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) logout_cleanup: Arc<crate::logout::LogoutCleanup>,
    /// Independent foreground observation; never driven by Returning failures.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) retirement: Arc<crate::retirement::RetirementObserver>,
    /// A genuine endpoint proof rejection parks every use of this configuration.
    pub(crate) endpoint_rejected: Arc<AtomicBool>,
    /// Private Bootstrap children share the parent's proof-rejection fence.
    pub(crate) provisioning_context: Option<(Arc<crate::retirement::RetirementObserver>, u64)>,
    /// Private generation-bound revocation caller, never an admission bypass
    /// for arbitrary application methods after local sign-out.
    logout_only: bool,
    owner: Arc<ClientOwner>,
    /// Heartbeats and reconnect restoration address only their captured generation.
    call_generation: Option<Arc<LiveGeneration>>,
    config: ClientConfig,
    pub(crate) trust_source: Arc<RwLock<TrustSource>>,
    pub(crate) enrollment_lock: Arc<tokio::sync::Mutex<()>>,
    store: Arc<dyn CursorStore>,
    #[cfg(not(target_arch = "wasm32"))]
    sqlite: Option<Arc<tinkle_storage::Storage>>,
    live: Arc<RwLock<Option<Live>>>,
    /// Retain task owners across cancellation of an in-progress teardown, so a
    /// later stop/reconnect can still join every retired liveness task.
    retiring: Arc<tokio::sync::Mutex<Vec<Live>>>,
    principal: Arc<RwLock<String>>,
    interests: Arc<tokio::sync::Mutex<Interests>>,
    /// Connectivity watch set retained across reconnects: `ObserveConnectivity`
    /// is per-connection, so every new connection must reissue the stored set.
    watch: Arc<tokio::sync::Mutex<Vec<i64>>>,
    /// Serializes `subscribe` bodies: concurrent subscribes must not
    /// interleave tracking + kicking.
    sub_lock: Arc<tokio::sync::Mutex<()>>,
    /// Serializes the check-dial-install sequence in `connect`.
    pub(crate) connect_lock: Arc<ConnectionGate>,
    /// Recovery-signal sender installed by the supervisor.
    signal_tx: Arc<tokio::sync::Mutex<Option<mpsc::Sender<RecoverySignal>>>>,
    /// Transport liveness as observed by the notification pump.
    transport_ok: Arc<AtomicBool>,
    sup_state: Arc<tokio::sync::Mutex<SupState>>,
    start_lock: Arc<tokio::sync::Mutex<()>>,
    drain_mgrs: Arc<tokio::sync::Mutex<Vec<Arc<SyncManager>>>>,
    suspended: Arc<AtomicBool>,
    /// Retain the resume transition even when its foreign/Rust waiter is dropped.
    resume_task: Arc<tokio::sync::Mutex<Option<ResumeTask>>>,
    /// Client-level commit gate: every native ingest holds this lock
    /// across its epoch check + write. Blocking writers retain owned guards
    /// even after their async waiter is cancelled. Logout/principal switches
    /// drain the gate before wiping/changing scope, including when no live
    /// manager remains to fence pending work.
    #[cfg(not(target_arch = "wasm32"))]
    checkpoint_save: Arc<tokio::sync::Mutex<()>>,
    /// Client-level reconcile epoch, shared by every `SyncManager`
    /// instance (a fresh one is built per connection). Bumped on logout
    /// and principal change; commits capture it at run start and re-check
    /// under the checkpoint lock.
    #[cfg(not(target_arch = "wasm32"))]
    reconcile_epoch: Arc<AtomicU64>,
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

/// Validate a positive peer identity before constructing its canonical journal key.
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

/// Reject malformed journal keys before converting their peer kind and positive ID to a target.
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
    /// Irreversibly revoke local admission and invalidate native reconcile work before cleanup awaits.
    pub(crate) fn begin_signed_out(&self) {
        self.authorization.sign_out();
        self.suspended.store(true, Ordering::SeqCst);
        #[cfg(not(target_arch = "wasm32"))]
        self.reconcile_epoch.fetch_add(1, Ordering::SeqCst);
    }

    /// Local sign-out is irreversible for this coordinator, including after a
    /// protected-store error. A new Client is required for deliberate sign-in.
    pub fn is_signed_out(&self) -> bool {
        self.authorization.is_signed_out()
    }

    /// The sole post-fence RPC is best-effort revocation on the captured,
    /// already-authenticated channel. It never dials or falls back to Bootstrap.
    pub(crate) async fn retire_signed_out_transport(
        &self,
        revoke_budget: Option<&syntra_runtime::elapsed::ElapsedDeadline>,
    ) -> (String, bool) {
        self.suspended.store(true, Ordering::SeqCst);
        self.stop_supervisor().await;
        let _connection = self.connect_lock.lock().await;
        let principal = self.principal.read().await.clone();
        {
            let mut source = self.trust_source.write().await;
            let trust = match &*source {
                TrustSource::Bootstrap(trust)
                | TrustSource::SignedOut(trust)
                | TrustSource::Returning { trust, .. } => trust.clone(),
            };
            *source = TrustSource::SignedOut(trust);
        }
        let generation = {
            let mut live = self.live.write().await;
            if let Some(live) = live.as_mut() {
                if let Some(task) = live.liveness.as_mut() {
                    task.shutdown().await;
                }
                if let Some(task) = live.watch_restore.as_mut() {
                    task.shutdown().await;
                }
                if live.generation.returning && live.generation.open.load(Ordering::SeqCst) {
                    Some(live.generation.clone())
                } else {
                    None
                }
            } else {
                None
            }
        };
        let confirmed = if let Some((generation, budget)) = generation.zip(revoke_budget) {
            let mut caller = self.background_clone();
            caller.call_generation = Some(generation);
            caller.logout_only = true;
            let result = caller
                .call_initialized_with_budget(
                    tinkle_schema::methods::accountservice::LOGOUT,
                    Zeroizing::new(tinkle_schema::proto::LogoutRequest {}.encode_to_vec()),
                    budget,
                    &CancelToken::new(),
                    None,
                    None,
                )
                .await;
            result.is_ok_and(|bytes| {
                tinkle_schema::proto::LogoutResponse::decode(bytes.as_slice())
                    .is_ok_and(|response| response.logged_out)
            })
        } else {
            false
        };
        self.sup_state.lock().await.reconnect_blocked = true;
        self.teardown_connection().await;
        (principal, confirmed)
    }

    /// Background work shares runtime state but never keeps host ownership
    /// alive. The original owner's cancellation token is retained separately.
    pub(crate) fn background_clone(&self) -> Self {
        let mut caller = self.clone();
        caller.owner = Arc::new(ClientOwner::default());
        caller
    }

    /// Background owners observe host lifetime without retaining the host anchor.
    pub(crate) fn owner_gone(&self) -> CancelToken {
        self.owner.gone.clone()
    }

    /// Settle only this failed socket generation. Never join our own liveness
    /// task here; lifecycle teardown retains and joins it after this returns.
    pub(crate) async fn fail_liveness(
        &self,
        generation: &Arc<LiveGeneration>,
        error: &TinkleError,
    ) {
        // Logout owns the final best-effort revocation and channel retirement.
        // A locally fenced heartbeat must not close that healthy channel first.
        if self.authorization.is_signed_out() {
            return;
        }
        // Stop admission immediately, even if a lifecycle owner currently holds
        // the connection lock. This fence cannot invalidate a replacement live.
        generation.open.store(false, Ordering::SeqCst);
        generation.closed.cancel();
        let connection = self.connect_lock.lock().await;
        let mut current = self.live.write().await;
        let Some(live) = current
            .as_mut()
            .filter(|live| Arc::ptr_eq(generation, &live.generation))
        else {
            return;
        };
        let blocked = !matches!(error.recovery_decision(), RecoveryDecision::Retry);
        self.sup_state.lock().await.reconnect_blocked |= blocked;
        generation.open.store(false, Ordering::SeqCst);
        generation.closed.cancel();
        self.transport_ok.store(false, Ordering::SeqCst);
        live.handle.suspend();
        let _ = live
            .handle
            .shutdown_transport(std::time::Duration::from_secs(1))
            .await;
        generation.classified.store(true, Ordering::SeqCst);
        let manager = live.sync_mgr.clone();
        drop(current);
        drop(connection);
        tracing::debug!(code = ?error.code(), blocked, "Returning liveness failed; connection retired");
        manager.emit_signal(RecoverySignal::PumpDead).await;
    }

    /// Construct a disconnected client using caller-provided trust and principal-scoped cursors.
    pub fn new(
        config: ClientConfig,
        trust_source: TrustSource,
        store: Arc<dyn CursorStore>,
    ) -> Self {
        let authorization = crate::authorization::Authorization::new(&trust_source);
        Self {
            authorization,
            #[cfg(not(target_arch = "wasm32"))]
            logout_cleanup: Arc::new(crate::logout::LogoutCleanup::default()),
            #[cfg(not(target_arch = "wasm32"))]
            retirement: Arc::new(crate::retirement::RetirementObserver::default()),
            endpoint_rejected: Arc::new(AtomicBool::new(false)),
            provisioning_context: None,
            logout_only: false,
            owner: Arc::new(ClientOwner::default()),
            call_generation: None,
            config,
            trust_source: Arc::new(RwLock::new(trust_source)),
            enrollment_lock: Arc::new(tokio::sync::Mutex::new(())),
            store,
            #[cfg(not(target_arch = "wasm32"))]
            sqlite: None,
            live: Arc::new(RwLock::new(None)),
            retiring: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            principal: Arc::new(RwLock::new(UNKNOWN_PRINCIPAL.to_string())),
            interests: Arc::new(tokio::sync::Mutex::new(Interests::default())),
            watch: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            sub_lock: Arc::new(tokio::sync::Mutex::new(())),
            connect_lock: Arc::new(ConnectionGate::default()),
            signal_tx: Arc::new(tokio::sync::Mutex::new(None)),
            transport_ok: Arc::new(AtomicBool::new(false)),
            sup_state: Arc::new(tokio::sync::Mutex::new(SupState::default())),
            start_lock: Arc::new(tokio::sync::Mutex::new(())),
            drain_mgrs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            suspended: Arc::new(AtomicBool::new(false)),
            resume_task: Arc::new(tokio::sync::Mutex::new(None)),
            #[cfg(not(target_arch = "wasm32"))]
            checkpoint_save: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(not(target_arch = "wasm32"))]
            reconcile_epoch: Arc::new(AtomicU64::new(0)),
            #[cfg(not(target_arch = "wasm32"))]
            catchup_pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Share one native SQLite store between journal checkpoints and offline cache projection.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_sqlite(
        config: ClientConfig,
        trust_source: TrustSource,
        storage: Arc<tinkle_storage::Storage>,
    ) -> Self {
        let authorization = crate::authorization::Authorization::new(&trust_source);
        let store: Arc<dyn CursorStore> =
            Arc::new(crate::sqlite_store::SqliteCursorStore::new(storage.clone()));
        Self {
            authorization,
            logout_cleanup: Arc::new(crate::logout::LogoutCleanup::default()),
            retirement: Arc::new(crate::retirement::RetirementObserver::default()),
            endpoint_rejected: Arc::new(AtomicBool::new(false)),
            provisioning_context: None,
            logout_only: false,
            owner: Arc::new(ClientOwner::default()),
            call_generation: None,
            config,
            trust_source: Arc::new(RwLock::new(trust_source)),
            enrollment_lock: Arc::new(tokio::sync::Mutex::new(())),
            store,
            sqlite: Some(storage),
            live: Arc::new(RwLock::new(None)),
            retiring: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            principal: Arc::new(RwLock::new(UNKNOWN_PRINCIPAL.to_string())),
            interests: Arc::new(tokio::sync::Mutex::new(Interests::default())),
            watch: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            sub_lock: Arc::new(tokio::sync::Mutex::new(())),
            connect_lock: Arc::new(ConnectionGate::default()),
            signal_tx: Arc::new(tokio::sync::Mutex::new(None)),
            transport_ok: Arc::new(AtomicBool::new(false)),
            sup_state: Arc::new(tokio::sync::Mutex::new(SupState::default())),
            start_lock: Arc::new(tokio::sync::Mutex::new(())),
            drain_mgrs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            suspended: Arc::new(AtomicBool::new(false)),
            resume_task: Arc::new(tokio::sync::Mutex::new(None)),
            #[cfg(not(target_arch = "wasm32"))]
            checkpoint_save: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(not(target_arch = "wasm32"))]
            reconcile_epoch: Arc::new(AtomicU64::new(0)),
            catchup_pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Return the shared native cache, or None for a cursor-only client.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn sqlite_storage(&self) -> Option<Arc<tinkle_storage::Storage>> {
        self.sqlite.clone()
    }

    /// Current client-level reconcile epoch (native only).
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn reconcile_epoch(&self) -> u64 {
        self.reconcile_epoch.load(Ordering::SeqCst)
    }

    /// Wasm stub: no SQLite commit gate exists on wasm, so there is no
    /// epoch to invalidate; callers only thread the value into native-only
    /// paths.
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn reconcile_epoch(&self) -> u64 {
        0
    }

    /// Inspect the immutable endpoint, client snapshot, and runtime policy supplied at construction.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Report local transport availability, not server authorization or presence renewal.
    pub async fn is_connected(&self) -> bool {
        if self.authorization.is_signed_out() {
            return false;
        }
        self.live
            .read()
            .await
            .as_ref()
            .is_some_and(|live| live.generation.open.load(Ordering::SeqCst))
            && self.transport_ok.load(Ordering::SeqCst)
    }

    /// Return the application cache principal, hiding the retired scope after local sign-out.
    pub async fn principal(&self) -> String {
        if self.authorization.is_signed_out() {
            return UNKNOWN_PRINCIPAL.into();
        }
        self.principal.read().await.clone()
    }

    /// Switch cache scope for a positive user ID, retiring any prior Returning account context.
    pub async fn set_principal_user_id(&self, user_id: i64) {
        if user_id <= 0 || self.authorization.is_signed_out() {
            return;
        }
        let _connection = self.connect_lock.lock().await;
        if self.authorization.is_signed_out() {
            return;
        }
        if *self.principal.read().await != format!("user:{user_id}") {
            self.retirement.invalidate();
            let returning = self
                .live
                .read()
                .await
                .as_ref()
                .is_some_and(|live| live.generation.returning);
            if returning {
                self.sup_state.lock().await.reconnect_blocked = true;
                self.teardown_connection().await;
            }
        }
        self.set_principal_locked(user_id).await;
    }

    /// Caller serializes connection retirement before changing cache scope.
    async fn set_principal_locked(&self, user_id: i64) {
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
        //
        // The client checkpoint lock is taken on every call — before
        // reading the current principal — and `changed` is computed under
        // it, so the check cannot race another principal writer and no
        // ingest can slip between the check and the epoch invalidation.
        // From here through both principal writes, an in-flight ingest
        // either completes before the switch or fails the epoch check: it
        // can never observe the new epoch with the old principal. The
        // guard releases after `self.principal` and `set_principal` have
        // both landed. Epoch bump and demand clear stay changed-only.
        #[cfg(not(target_arch = "wasm32"))]
        let _client_guard = self.checkpoint_save.lock().await;
        if self.authorization.is_signed_out() {
            return;
        }
        #[cfg(not(target_arch = "wasm32"))]
        let changed = *self.principal.read().await != p;
        #[cfg(not(target_arch = "wasm32"))]
        if changed {
            self.reconcile_epoch.fetch_add(1, Ordering::SeqCst);
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
    /// When connected and initialized, observes the merged set immediately.
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
        let (_, initialized) = self.live_conn().await?;
        if !*initialized.lock().await {
            return Ok(());
        }
        let req = tinkle_schema::proto::ObserveConnectivityRequest { user_ids: merged };
        if let Err(error) = self.updates().observe_connectivity(req).await {
            if matches!(error.recovery_decision(), RecoveryDecision::Retry) {
                initialized.watch.notify_one();
            }
            return Err(error);
        }
        Ok(())
    }

    /// Current retained watch set (for inspection/testing).
    pub async fn watch_list(&self) -> Vec<i64> {
        self.watch.lock().await.clone()
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
    /// Lock order: the manager checkpoint lock first (when a live scope
    /// exists), then the client checkpoint lock — the same order every
    /// commit path uses — both held across the epoch bump and the reset,
    /// so an ingest can neither commit between the epoch change and the
    /// cleanup nor after it.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn reset_session_for_logout(&self) {
        let _connection = self.connect_lock.lock().await;
        self.sup_state.lock().await.reconnect_blocked = true;
        self.teardown_connection().await;
        let mgr = self.live_mgr().await.ok();
        let _guard = match mgr.as_ref() {
            Some(m) => Some(m.checkpoint_save_lock().await),
            None => None,
        };
        let _client_guard = Arc::new(self.checkpoint_save.clone().lock_owned().await);
        self.reconcile_epoch.fetch_add(1, Ordering::SeqCst);
        self.reset_session_locked(mgr.as_deref()).await;
    }

    /// In-memory scope reset; the caller must hold `checkpoint_save_lock`
    /// when a live scope exists (see `reset_session_for_logout` and
    /// `logout_wipe_and_reset`).
    async fn reset_session_locked(&self, mgr: Option<&SyncManager>) {
        *self.principal.write().await = UNKNOWN_PRINCIPAL.to_string();
        *self.interests.lock().await = Interests::default();
        self.watch.lock().await.clear();
        #[cfg(not(target_arch = "wasm32"))]
        self.catchup_pending.lock().await.clear();
        if let Some(m) = mgr {
            m.set_principal(UNKNOWN_PRINCIPAL.to_string()).await;
        }
    }

    /// Logout-scoped durable wipe + in-memory reset. Lock order: the
    /// manager checkpoint lock first (when a live scope exists), then the
    /// client checkpoint lock — the same order every commit path uses —
    /// both held across the epoch bump, the `clear_principal` row wipe,
    /// and the generation-bumping reset, so an in-flight ingest can
    /// neither commit between them nor land after the wipe under the old
    /// scope. With no live manager, the client lock alone still drains
    /// push-path ingests and invalidates their epoch. Native-only:
    /// called from the push module's `logout` instead of wiping inline.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn logout_wipe_and_reset(&self, principal: &str) -> Result<(), TinkleError> {
        let _connection = self.connect_lock.lock().await;
        self.sup_state.lock().await.reconnect_blocked = true;
        self.teardown_connection().await;
        let mgr = self.live_mgr().await.ok();
        let _guard = match mgr.as_ref() {
            Some(m) => Some(m.checkpoint_save_lock().await),
            None => None,
        };
        let _client_guard = Arc::new(self.checkpoint_save.clone().lock_owned().await);
        self.reconcile_epoch.fetch_add(1, Ordering::SeqCst);
        // Capture the wipe result but never let it bypass the reset: the
        // in-memory scope (principal, generation fence, tracked state) must
        // come down even when the durable wipe fails, or a half-logged-out
        // client keeps reconciling under a dead principal.
        let mut wipe_result: Result<(), TinkleError> = Ok(());
        if principal != UNKNOWN_PRINCIPAL {
            if let Some(storage) = self.sqlite_storage() {
                let p = principal.to_string();
                let commit_guard = _client_guard.clone();
                wipe_result = tokio::task::spawn_blocking(move || {
                    let _guard = commit_guard;
                    storage.clear_principal(&p)
                })
                .await
                .map_err(|e| TinkleError::CursorStore(format!("logout wipe task failed: {e}")))
                .and_then(|r| r.map_err(|e| TinkleError::CursorStore(e.to_string())));
            }
            let store = self.store.clone();
            let p = principal.to_string();
            let commit_guard = _client_guard.clone();
            let cursor_result = tokio::spawn(async move {
                let _guard = commit_guard;
                store.clear_principal(&p).await
            })
            .await
            .map_err(|e| TinkleError::CursorStore(format!("cursor clear task failed: {e}")))
            .and_then(|result| result);
            wipe_result = wipe_result.and(cursor_result);
        }
        self.reset_session_locked(mgr.as_deref()).await;
        wipe_result
    }

    /// Establish one pinned connection unless already connected; never retry an application request.
    pub async fn connect(&self) -> Result<(), TinkleError> {
        let _connect_guard = self.connect_lock.lock().await;
        self.connect_locked().await
    }

    /// Race establishment against irreversible sign-out while the caller holds the connection gate.
    async fn connect_locked(&self) -> Result<(), TinkleError> {
        let epoch = self.authorization.generation()?;
        let revoked = self.authorization.revoked.clone();
        tokio::select! {
            biased;
            _ = revoked.cancelled() => Err(TinkleError::SignedOut),
            result = self.connect_generation(&epoch) => result,
        }
    }

    /// Install a fresh channel and its owned workers only if authorization and lifecycle still match.
    async fn connect_generation(
        &self,
        authorization: &crate::authorization::EnrollmentEpoch,
    ) -> Result<(), TinkleError> {
        self.check_provisioning_context()?;
        if self.endpoint_rejected.load(Ordering::SeqCst) {
            return Err(TinkleError::TrustRejected);
        }
        if self.retirement.is_restoring() {
            return Err(TinkleError::NotConnected(
                "status-first restoration is pending".into(),
            ));
        }
        if self.suspended.load(Ordering::SeqCst) {
            return Err(TinkleError::NotConnected(
                "suspended; connect suppressed".into(),
            ));
        }
        let epoch = self.sup_state.lock().await.epoch;
        if self.is_connected().await {
            return Ok(());
        }
        self.authorization.set_authentication_known(false);
        if self.live.read().await.is_some() {
            self.teardown_connection().await;
        }
        self.config.validate()?;
        // Hold one source through dial selection and establishment. Neither a
        // failed pin/proof nor a rejected Returning mode can trigger fallback.
        let source = self.trust_source.read().await;
        let returning = matches!(&*source, TrustSource::Returning { .. });
        let security = source.security()?;
        let budget = syntra_net::EstablishmentBudget::new(
            syntra_runtime::elapsed::ElapsedClock::new(),
            std::time::Duration::from_secs(15),
        )
        .map_err(TinkleError::from_network)?;
        let stream = self.dial_selected_tcp(&budget).await?;
        let handle = match syntra_net::tcp::connect_stream_with_budget(
            stream,
            security,
            Vec::new(),
            self.config.connection.clone(),
            Default::default(),
            budget,
        )
        .await
        {
            Ok(handle) => handle,
            Err(error) => {
                self.sup_state.lock().await.reconnect_blocked = true;
                let error = self.classify_establishment(error);
                if self.provisioning_context.is_some()
                    && matches!(error, TinkleError::EstablishmentFailed)
                {
                    // An unverified Bootstrap proof may conceal rejection. This
                    // shared flag is set while holding the parent's connect lock.
                    self.endpoint_rejected.store(true, Ordering::SeqCst);
                }
                return Err(error);
            }
        };
        drop(source);
        if self.suspended.load(Ordering::SeqCst) || self.sup_state.lock().await.epoch != epoch {
            handle.close().await;
            return Err(TinkleError::NotConnected(
                "connection retired during establishment".into(),
            ));
        }

        let conn = handle.conn.clone();
        let connection_id = handle.connection_id.clone();
        let version = handle.version;
        let generation = LiveGeneration::new(returning);

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
        let pump_generation = generation.clone();
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
            pump_generation.closed.cancel();
            if pump_generation.open.swap(false, Ordering::SeqCst) {
                pump_ok.store(false, Ordering::SeqCst);
                // Returning failures are classified by its bounded in-flight
                // KeepAlive (if any) before the supervisor may reconnect.
                if !pump_generation.returning {
                    pump_mgr.emit_signal(RecoverySignal::PumpDead).await;
                }
            }
        });

        let mut live_w = self.live.write().await;
        if live_w.is_some() {
            drop(live_w);
            handle.close().await;
            return Ok(());
        }
        let initialized = Arc::new(Initialization::default());
        let watch_restore = self.spawn_watch_restore(conn, initialized.clone(), generation.clone());
        let live = Live {
            handle,
            connection_id,
            version,
            sync_mgr,
            pump: AbortTask::new(pump),
            initialized,
            watch_restore: Some(watch_restore),
            generation: generation.clone(),
            liveness: None,
        };
        self.authorization
            .with_generation(authorization.generation, || {
                *live_w = Some(live);
                self.transport_ok.store(true, Ordering::SeqCst);
                if returning {
                    let owner_gone = self.owner.gone.clone();
                    let mut caller = self.background_clone();
                    caller.call_generation = Some(generation.clone());
                    live_w.as_mut().expect("installed live generation").liveness =
                        Some(LivenessTask::spawn(caller, generation, owner_gone));
                }
            })?;
        drop(live_w);
        self.authorization.set_authentication_known(returning);
        // Reissue the retained watch set on the fresh connection.
        // `ObserveConnectivity` is per-connection; without this a reconnect
        // silently drops presence observations. Presence is optional: a
        // terminal watch rejection (Fail) is logged and the healthy
        // connection kept — only errors requiring recovery tear it down.
        // Retained observations are restored only after the first unary call
        // installs this connection's immutable client snapshot.
        Ok(())
    }

    /// Share the chosen TCP route with short-lived status observations. Callers
    /// hold connect_lock through dial/establishment; only an initial dial failure
    /// may choose the configured fallback before any socket has been selected.
    pub(crate) async fn dial_selected_tcp(
        &self,
        budget: &syntra_net::EstablishmentBudget,
    ) -> Result<tokio::net::TcpStream, TinkleError> {
        if self.endpoint_rejected.load(Ordering::SeqCst) {
            return Err(TinkleError::TrustRejected);
        }
        self.config.validate()?;
        let selected = match self.connect_lock.selected_port.load(Ordering::SeqCst) {
            0 => None,
            port => Some(port),
        };
        let dial = |port| {
            tokio::time::timeout(
                self.config.connect_timeout,
                tokio::net::TcpStream::connect((self.config.host.as_str(), port)),
            )
        };
        let primary = selected.unwrap_or(self.config.primary_port);
        let (stream, port) = match budget
            .run(dial(primary))
            .await
            .map_err(TinkleError::from_network)?
        {
            Ok(Ok(stream)) => (stream, primary),
            _ => match self.config.fallback_port.filter(|_| selected.is_none()) {
                Some(port) => (
                    budget
                        .run(dial(port))
                        .await
                        .map_err(TinkleError::from_network)?
                        .map_err(|_| TinkleError::NetworkUnavailable)?
                        .map_err(|_| TinkleError::NetworkUnavailable)?,
                    port,
                ),
                None => return Err(TinkleError::NetworkUnavailable),
            },
        };
        self.connect_lock
            .selected_port
            .store(port, Ordering::SeqCst);
        stream
            .set_nodelay(true)
            .map_err(|_| TinkleError::NetworkUnavailable)?;
        Ok(stream)
    }

    /// A typed endpoint proof/protocol rejection parks both connection users.
    /// Opaque EOF/timeout or application authorization errors cannot reset routes.
    pub(crate) fn classify_establishment(&self, error: syntra_net::Error) -> TinkleError {
        let error = TinkleError::from_establishment(error);
        if matches!(error, TinkleError::TrustRejected) {
            self.endpoint_rejected.store(true, Ordering::SeqCst);
            self.authorization.set_authentication_known(false);
        }
        error
    }

    /// Resolve an open channel, rejecting a background caller pinned to a superseded generation.
    async fn live_conn(&self) -> Result<(Initiator, Arc<Initialization>), TinkleError> {
        self.live
            .read()
            .await
            .as_ref()
            .filter(|live| {
                live.generation.open.load(Ordering::SeqCst)
                    && self
                        .call_generation
                        .as_ref()
                        .is_none_or(|generation| Arc::ptr_eq(generation, &live.generation))
            })
            .map(|l| (l.handle.conn.clone(), l.initialized.clone()))
            .ok_or_else(|| TinkleError::NotConnected("call connect() first".into()))
    }

    /// Resolve the reconcile manager without letting generation-bound restoration adopt a successor.
    async fn live_mgr(&self) -> Result<Arc<SyncManager>, TinkleError> {
        self.live
            .read()
            .await
            .as_ref()
            .filter(|live| {
                self.call_generation
                    .as_ref()
                    .is_none_or(|generation| Arc::ptr_eq(generation, &live.generation))
            })
            .map(|l| l.sync_mgr.clone())
            .ok_or_else(|| TinkleError::NotConnected("call connect() first".into()))
    }

    /// Send opaque application bytes once, including first-call initialization in the caller budget.
    pub async fn call_raw(
        &self,
        method: &str,
        payload: Vec<u8>,
        timeout_ms: u32,
    ) -> Result<Vec<u8>, TinkleError> {
        self.call_initialized(method, payload, timeout_ms, &CancelToken::new())
            .await
    }

    /// Apply cooperative cancellation to one raw call without replaying an uncertain remote effect.
    pub async fn call_raw_cancellable(
        &self,
        method: &str,
        payload: Vec<u8>,
        timeout_ms: u32,
        token: &CancelToken,
    ) -> Result<Vec<u8>, TinkleError> {
        self.call_initialized(method, payload, timeout_ms, token)
            .await
    }

    /// Serialize only first-call initialization; ordinary ready calls remain concurrent.
    async fn call_initialized(
        &self,
        method: &str,
        payload: Vec<u8>,
        timeout_ms: u32,
        token: &CancelToken,
    ) -> Result<Vec<u8>, TinkleError> {
        let payload = Zeroizing::new(payload);
        self.check_call_admission(method)?;
        let budget = self.call_budget(timeout_ms)?;
        self.call_initialized_with_budget(method, payload, &budget, token, None, None)
            .await
    }

    /// Resolve default/requested and deployment limits once, before any local wait.
    pub(crate) fn call_budget(
        &self,
        timeout_ms: u32,
    ) -> Result<syntra_runtime::elapsed::ElapsedDeadline, TinkleError> {
        let duration = if timeout_ms == 0 {
            self.config.connection.default_timeout
        } else {
            std::time::Duration::from_millis(timeout_ms as u64)
        };
        syntra_runtime::elapsed::ElapsedClock::new()
            .deadline_after(duration.min(self.config.connection.max_rpc_timeout))
            .map_err(|_| Self::call_budget_elapsed())
    }

    /// Fail closed on exhaustion or clock loss, never falling back to SDK default zero.
    fn remaining_timeout_ms(
        budget: &syntra_runtime::elapsed::ElapsedDeadline,
    ) -> Result<u32, TinkleError> {
        let remaining = budget
            .remaining()
            .map_err(|_| Self::call_budget_elapsed())?;
        let millis = remaining.as_millis().min(u128::from(u32::MAX)) as u32;
        if millis == 0 {
            return Err(Self::call_budget_elapsed());
        }
        Ok(millis)
    }

    /// One sanitized error for a spent or unusable local call budget.
    fn call_budget_elapsed() -> TinkleError {
        TinkleError::Syntra(syntra_core::SyntraError::DeadlineExceeded(
            "initialization/call budget elapsed".into(),
        ))
    }

    /// Reuse the caller's original cutoff, including retained logout lifecycle time.
    async fn call_initialized_with_budget(
        &self,
        method: &str,
        payload: Zeroizing<Vec<u8>>,
        budget: &syntra_runtime::elapsed::ElapsedDeadline,
        token: &CancelToken,
        progress: Option<&CallProgress>,
        ready: Option<tokio::sync::MutexGuard<'_, bool>>,
    ) -> Result<Vec<u8>, TinkleError> {
        self.check_call_admission_progress(method, progress)?;
        // Preserve the conservative outer pre-poll timeout; do not post-check an
        // SDK-selected response with ElapsedDeadline::run.
        let result = tokio::select! {
            biased;
            _ = self.authorization.revoked.cancelled(), if !self.logout_only => Err(TinkleError::SignedOut),
            _ = budget.expired() => Err(Self::call_budget_elapsed()),
            result = self.call_initialized_inner(method, payload, budget, token, progress, ready) => result,
            _ = token.cancelled() => Err(TinkleError::Syntra(syntra_core::SyntraError::Cancelled("call cancelled".into()))),
        };
        if let Err(error) = &result {
            self.observe_denial(error);
        }
        result
    }

    /// Admit the private logout caller only on its captured Returning generation.
    fn check_call_admission(&self, method: &str) -> Result<(), TinkleError> {
        self.check_call_admission_progress(method, None)
    }

    /// Mark restoration-only deferral separately from trust, sign-out, and transport failures.
    fn check_call_admission_progress(
        &self,
        method: &str,
        progress: Option<&CallProgress>,
    ) -> Result<(), TinkleError> {
        self.check_provisioning_context()?;
        if self.endpoint_rejected.load(Ordering::SeqCst) {
            return Err(TinkleError::TrustRejected);
        }
        if self.retirement.is_restoring() {
            if let Some(progress) = progress {
                progress.deferred.store(true, Ordering::SeqCst);
            }
            return Err(TinkleError::NotConnected(
                "restoration install is pending".into(),
            ));
        }
        if self.logout_only {
            if method == tinkle_schema::methods::accountservice::LOGOUT
                && self.call_generation.as_ref().is_some_and(|generation| {
                    generation.returning && generation.open.load(Ordering::SeqCst)
                })
            {
                return Ok(());
            }
            return Err(TinkleError::SignedOut);
        }
        self.authorization.generation().map(|_| ())
    }

    /// Any application denial withdraws authorization knowledge, never credentials.
    pub(crate) fn observe_denial(&self, error: &TinkleError) {
        if matches!(
            error.code(),
            syntra_core::ErrorCode::Unauthenticated | syntra_core::ErrorCode::PermissionDenied
        ) {
            self.authorization.set_authentication_known(false);
        }
    }

    /// A child admitted under an older configuration cannot park or use a
    /// configuration that the host has explicitly recovered in the meantime.
    fn check_provisioning_context(&self) -> Result<(), TinkleError> {
        if self
            .provisioning_context
            .as_ref()
            .is_some_and(|(parent, revision)| parent.endpoint_revision() != *revision)
        {
            return Err(TinkleError::Configuration(
                "provisioning configuration was superseded".into(),
            ));
        }
        Ok(())
    }

    /// Recheck authorization after waiting for connection and initialization locks.
    async fn call_initialized_inner(
        &self,
        method: &str,
        payload: Zeroizing<Vec<u8>>,
        budget: &syntra_runtime::elapsed::ElapsedDeadline,
        token: &CancelToken,
        progress: Option<&CallProgress>,
        ready: Option<tokio::sync::MutexGuard<'_, bool>>,
    ) -> Result<Vec<u8>, TinkleError> {
        let (conn, initialized) = self.live_conn().await?;
        self.check_call_admission_progress(method, progress)?;
        if !method.starts_with("/tinkle.v1.") {
            let encoded = payload.to_vec();
            let timeout_ms = Self::remaining_timeout_ms(budget)?;
            if let Some(progress) = progress {
                progress.dispatched.store(true, Ordering::SeqCst);
            }
            return conn
                .call_cancellable(method, encoded, timeout_ms, token)
                .await
                .map_err(Into::into);
        }
        if method == "/tinkle.v1.ConnectionService/InitConnect" {
            return Err(TinkleError::Configuration(
                "InitConnect is owned by the client snapshot".into(),
            ));
        }
        let mut ready = match ready {
            Some(ready) => ready,
            None => initialized.lock().await,
        };
        self.check_call_admission_progress(method, progress)?;
        if *ready {
            drop(ready);
            let encoded = payload.to_vec();
            let timeout_ms = Self::remaining_timeout_ms(budget)?;
            if let Some(progress) = progress {
                progress.dispatched.store(true, Ordering::SeqCst);
            }
            return conn
                .call_cancellable(method, encoded, timeout_ms, token)
                .await
                .map_err(Into::into);
        }
        if method == tinkle_schema::sync::OBSERVE_METHOD {
            return Err(TinkleError::Configuration(
                "initialize with a public unary call before observing connectivity".into(),
            ));
        }
        let mut request = tinkle_schema::proto::InitConnectRequest {
            client: Some(self.config.client_info.proto()),
            method: method.into(),
            payload: payload.as_slice().to_vec().into(),
        };
        let encoded = request.encode_to_vec();
        crate::enrollment::wipe_bytes(std::mem::take(&mut request.payload));
        let timeout_ms = Self::remaining_timeout_ms(budget)?;
        let attempt = InitializationAttempt {
            initialized: &initialized,
            deadline: Arc::new(budget.clone()),
        };
        *initialized
            .dispatched_deadline
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(attempt.deadline.clone());
        initialized.changed.notify_one();
        if let Some(progress) = progress {
            progress.dispatched.store(true, Ordering::SeqCst);
        }
        let response = Zeroizing::new(
            conn.call_cancellable(
                "/tinkle.v1.ConnectionService/InitConnect",
                encoded,
                timeout_ms,
                token,
            )
            .await?,
        );
        let mut decoded = tinkle_schema::proto::InitConnectResponse::default();
        let result = decoded.merge(response.as_slice());
        if result.is_err() {
            crate::enrollment::wipe_bytes(std::mem::take(&mut decoded.payload));
            return Err(TinkleError::Codec("invalid initialization response".into()));
        }
        let output = Zeroizing::new(decoded.payload.to_vec());
        crate::enrollment::wipe_bytes(std::mem::take(&mut decoded.payload));
        *ready = true;
        drop(attempt);
        drop(ready);
        // This synchronous wake cannot turn a confirmed response into a local timeout.
        initialized.watch.notify_one();
        Ok(output.to_vec())
    }

    /// Retain FIFO admission behind bounded initialization instead of repeatedly
    /// timing out and requeueing. The initializer keeps its original deadline;
    /// the heartbeat's five-second budget starts only after local admission.
    pub(crate) async fn call_keep_alive(
        &self,
        payload: Vec<u8>,
    ) -> Option<Result<Vec<u8>, TinkleError>> {
        let progress = CallProgress::default();
        let method = crate::liveness::KEEP_ALIVE_METHOD;
        if let Err(error) = self.check_call_admission_progress(method, Some(&progress)) {
            return if progress.deferred.load(Ordering::SeqCst) {
                None
            } else {
                Some(Err(error))
            };
        }
        let (_, initialized) = match self.live_conn().await {
            Ok(live) => live,
            Err(error) => return Some(Err(error)),
        };
        let queued = initialized.lock();
        tokio::pin!(queued);
        let ready = loop {
            let deadline = initialized
                .dispatched_deadline
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            tokio::select! { biased;
                _ = self.authorization.revoked.cancelled() => return Some(Err(TinkleError::SignedOut)),
                _ = async {
                    match &self.call_generation {
                        Some(generation) => generation.closed.cancelled().await,
                        None => std::future::pending::<()>().await,
                    }
                } => return Some(Err(TinkleError::NetworkUnavailable)),
                ready = &mut queued => break ready,
                _ = initialized.changed.notified() => {},
                _ = async {
                    match &deadline {
                        Some(deadline) => deadline.expired().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    let current = initialized.dispatched_deadline.lock().unwrap_or_else(|e| e.into_inner());
                    if current.as_ref().zip(deadline.as_ref()).is_some_and(|(current, expected)| Arc::ptr_eq(current, expected)) {
                        // A dispatched initializer exceeded its own deadline. This
                        // is not a timeout inferred merely from mutex contention.
                        return Some(Err(Self::call_budget_elapsed()));
                    }
                },
            }
        };
        let budget = match self.call_budget(5_000) {
            Ok(budget) => budget,
            Err(error) => return Some(Err(error)),
        };
        let result = self
            .call_initialized_with_budget(
                crate::liveness::KEEP_ALIVE_METHOD,
                Zeroizing::new(payload),
                &budget,
                &CancelToken::new(),
                Some(&progress),
                Some(ready),
            )
            .await;
        if !progress.dispatched.load(Ordering::SeqCst)
            && (progress.deferred.load(Ordering::SeqCst)
                || result
                    .as_ref()
                    .is_err_and(|error| error.code() == syntra_core::ErrorCode::DeadlineExceeded))
        {
            None
        } else {
            Some(result)
        }
    }

    /// Exactly one connection-owned observation worker, independent of heartbeat success.
    fn spawn_watch_restore(
        &self,
        conn: Initiator,
        initialized: Arc<Initialization>,
        generation: Arc<LiveGeneration>,
    ) -> AbortTask {
        let watch = self.watch.clone();
        let revoked = self.authorization.revoked.clone();
        let owner_gone = self.owner.gone.clone();
        AbortTask::new(tokio::spawn(async move {
            let restore = async {
                loop {
                    initialized.watch.notified().await;
                    let mut delay = 2;
                    loop {
                        let user_ids = watch.lock().await.clone();
                        if user_ids.is_empty() {
                            break;
                        }
                        let request = tinkle_schema::proto::ObserveConnectivityRequest { user_ids };
                        match conn
                            .call(
                                tinkle_schema::sync::OBSERVE_METHOD,
                                request.encode_to_vec(),
                                30_000,
                            )
                            .await
                        {
                            Ok(_) => break,
                            Err(error) => {
                                let error = TinkleError::from(error);
                                tracing::debug!(code = ?error.code(), "retained connectivity observation rejected");
                                if !matches!(error.recovery_decision(), RecoveryDecision::Retry) {
                                    break;
                                }
                            }
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                        delay = (delay * 2).min(60);
                    }
                }
            };
            tokio::select! { biased;
                _ = revoked.cancelled() => {},
                _ = owner_gone.cancelled() => {},
                _ = generation.closed.cancelled() => {},
                _ = restore => {},
            }
        }))
    }

    /// Bind typed message operations to this client's current admission and channel.
    pub fn messages(&self) -> Messages<'_> {
        Messages::new(self)
    }

    /// Bind enrollment operations to this client's protected-store coordinator.
    pub fn auth(&self) -> Auth<'_> {
        Auth::new(self)
    }

    /// Authenticated session listing and single-attempt device revocation.
    pub fn account(&self) -> Account<'_> {
        Account::new(self)
    }

    /// Bind ephemeral connectivity observations to this client's active channel.
    pub fn updates(&self) -> Updates<'_> {
        Updates::new(self)
    }

    /// Bind validated file RPCs to this client without creating a separate transport.
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

    /// Fetch specific messages, chunking to `MAX_MESSAGE_IDS_PER_REQUEST`
    /// internally, using the active Syntra connection for every chunk.
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

    /// Dispatch one already-chunked body fetch through the typed validation boundary.
    async fn get_messages_single(
        &self,
        req: tinkle_schema::proto::GetMessagesRequest,
    ) -> Result<tinkle_schema::proto::GetMessagesResponse, TinkleError> {
        self.messages().get_messages(req).await
    }

    /// List retained mutations strictly after one accept position.
    /// Uses the active Syntra connection.
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
        self.messages().get_target_changes(req).await
    }

    /// Begin an upload over the active Syntra connection. Input validation
    /// lives in the `Files` facade.
    pub async fn begin_upload(
        &self,
        req: tinkle_schema::proto::BeginUploadRequest,
    ) -> Result<tinkle_schema::proto::BeginUploadResponse, TinkleError> {
        self.files().begin_upload(req).await
    }

    /// Upload one validated part using the active channel and the facade's size checks.
    pub async fn upload_part(
        &self,
        req: tinkle_schema::proto::UploadPartRequest,
    ) -> Result<tinkle_schema::proto::UploadPartResponse, TinkleError> {
        self.files().upload_part(req).await
    }

    /// Query upload progress without inferring that an unconfirmed mutation was rolled back.
    pub async fn get_upload_status(
        &self,
        req: tinkle_schema::proto::GetUploadStatusRequest,
    ) -> Result<tinkle_schema::proto::GetUploadStatusResponse, TinkleError> {
        self.files().get_upload_status(req).await
    }

    /// Submit upload completion once through the typed file service.
    pub async fn complete_upload(
        &self,
        req: tinkle_schema::proto::CompleteUploadRequest,
    ) -> Result<tinkle_schema::proto::CompleteUploadResponse, TinkleError> {
        self.files().complete_upload(req).await
    }

    /// Request server-side upload cancellation; local future cancellation alone is not this RPC.
    pub async fn abort_upload(
        &self,
        req: tinkle_schema::proto::AbortUploadRequest,
    ) -> Result<tinkle_schema::proto::AbortUploadResponse, TinkleError> {
        self.files().abort_upload(req).await
    }

    /// Fetch file metadata through the facade's identity validation.
    pub async fn get_file_info(
        &self,
        req: tinkle_schema::proto::GetFileInfoRequest,
    ) -> Result<tinkle_schema::proto::GetFileInfoResponse, TinkleError> {
        self.files().get_file_info(req).await
    }

    /// Fetch one bounded file range through the active encrypted connection.
    pub async fn download_part(
        &self,
        req: tinkle_schema::proto::DownloadPartRequest,
    ) -> Result<tinkle_schema::proto::DownloadPartResponse, TinkleError> {
        self.files().download_part(req).await
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

    /// Page and commit one fenced target journal, then repair missing bodies without cursor forgery.
    async fn reconcile_target_inner(
        &self,
        mgr: &Arc<SyncManager>,
        fence_principal: &str,
        fence_generation: u64,
        target_key: &str,
        target: tinkle_schema::proto::Target,
    ) -> Result<u64, TinkleError> {
        let run_epoch = self.reconcile_epoch();
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
                    self.heal_via_history(
                        mgr,
                        fence_principal,
                        fence_generation,
                        run_epoch,
                        &target,
                        target_key,
                    )
                    .await;
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
                    // Client-level commit gate (same epoch check as
                    // ingest_messages): a logout bump waits on this lock,
                    // so it invalidates the shard before it writes.
                    let _commit_guard = Arc::new(self.checkpoint_save.clone().lock_owned().await);
                    if self.reconcile_epoch() != run_epoch {
                        return Err(TinkleError::Sync(format!(
                            "reconcile epoch changed for {target_key}; dropping stale pages"
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
                        let commit_guard = _commit_guard.clone();
                        tokio::task::spawn_blocking(move || {
                            let _guard = commit_guard;
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
                        CatchupCommitCtx {
                            mgr,
                            fence_principal,
                            fence_generation,
                            target_key,
                            epoch: run_epoch,
                        },
                        &target,
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
        batch: IngestBatch<'_>,
        run_epoch: u64,
    ) -> Result<Vec<i64>, TinkleError> {
        let IngestBatch {
            messages,
            users,
            chats,
            exclude,
        } = batch;
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
        // Client-level commit gate: serialize ingests process-wide and
        // reject stale epochs (logout/principal switch) so a commit can
        // never land during or after a wipe. Check + write share one hold.
        let _commit_guard = Arc::new(self.checkpoint_save.clone().lock_owned().await);
        if self.reconcile_epoch() != run_epoch {
            return Err(TinkleError::Sync(
                "reconcile epoch changed; dropping stale ingest".into(),
            ));
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
        let commit_guard = _commit_guard.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _guard = commit_guard;
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
        ctx: CatchupCommitCtx<'_>,
        target: &tinkle_schema::proto::Target,
        run_removals: &HashSet<i64>,
    ) -> Result<u64, TinkleError> {
        let CatchupCommitCtx {
            mgr,
            fence_principal,
            fence_generation,
            target_key,
            epoch: run_epoch,
        } = ctx;
        // Rebuild the per-run scope: every fenced operation below (commits
        // and demand bookkeeping) shares this one fence context.
        let ctx = CatchupCommitCtx {
            mgr,
            fence_principal,
            fence_generation,
            target_key,
            epoch: run_epoch,
        };
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
        let mut covering_new = resume.edge.is_some() || resume.covering.is_some();
        // Connection threshold for the covering phase: the top of the
        // previously covered contiguous range. Falls back to the edge for
        // rows written before the top was persisted.
        let cover_top = resume.cover_top.or(resume.edge);
        // Top of the covering chain (the newest id when it started). A
        // resumed chain carries it in the stored state; a fresh chain
        // claims it from the first window's max id below.
        let mut chain_top: Option<i64> = resume.covering_top;
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
        // A stored covering cursor resumes the descent mid-chain instead
        // of restarting at the newest window (which would starve the tail:
        // chains longer than the per-run budget would never connect).
        let mut offset_id: Option<i64> = resume.covering;
        // Edge of the previous window (smallest message id seen, else the
        // server offset). Paging moves backward, so each new edge must be
        // strictly smaller; anything else while `more` says continue is a
        // stalled server cursor.
        let mut prev_edge: Option<i64> = None;
        let mut windows: u32 = 0;
        let mut run_top: Option<i64> = None;
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
                self.persist_catchup_demand(&ctx, offset_id, run_top, covering_new, chain_top)
                    .await;
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
                    self.persist_catchup_demand(&ctx, offset_id, run_top, covering_new, chain_top)
                        .await;
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
            // The covering chain's top is claimed from the first window.
            if covering_new {
                chain_top = chain_top.or(run_top);
            }
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
                            self.persist_catchup_demand(
                                &ctx,
                                offset_id,
                                run_top,
                                covering_new,
                                chain_top,
                            )
                            .await;
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
                            self.persist_catchup_demand(
                                &ctx,
                                offset_id,
                                run_top,
                                covering_new,
                                chain_top,
                            )
                            .await;
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
                                self.persist_catchup_demand(
                                    &ctx,
                                    offset_id,
                                    run_top,
                                    covering_new,
                                    chain_top,
                                )
                                .await;
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
                        self.persist_catchup_demand(
                            &ctx,
                            offset_id,
                            run_top,
                            covering_new,
                            chain_top,
                        )
                        .await;
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
                            self.persist_catchup_demand(
                                &ctx,
                                offset_id,
                                run_top,
                                covering_new,
                                chain_top,
                            )
                            .await;
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
                            self.persist_catchup_demand(
                                &ctx,
                                offset_id,
                                run_top,
                                covering_new,
                                chain_top,
                            )
                            .await;
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
            IngestBatch {
                messages,
                users,
                chats,
                exclude,
            },
            ctx.epoch,
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
    /// Holds the manager checkpoint lock, then the client checkpoint lock
    /// (manager-to-client order holds everywhere), and re-checks both the
    /// run fence and the client epoch first: a stale run (logout/principal
    /// switch mid-flight) skips the write instead of recording demand
    /// under the wrong scope. Durable writes go under
    /// `fence_principal`, never a function-start snapshot.
    /// `covering_open` marks an in-flight covering chain (`covering_new`
    /// at the persist point): the current offset is stored as the covering
    /// cursor so the next run resumes mid-chain instead of restarting at
    /// the newest window — chains longer than the per-run budget would
    /// otherwise never connect and the tail below the edge would starve.
    /// When the chain closed this run (`!covering_open` with a claimed
    /// `chain_top`), the covering cursor is cleared and `cover_top` widens
    /// to the chain's starting top.
    #[cfg(not(target_arch = "wasm32"))]
    async fn persist_catchup_demand(
        &self,
        ctx: &CatchupCommitCtx<'_>,
        next_offset: Option<i64>,
        run_top: Option<i64>,
        covering_open: bool,
        chain_top: Option<i64>,
    ) {
        let _guard = ctx.mgr.checkpoint_save_lock().await;
        if !ctx
            .mgr
            .check_fence(ctx.fence_principal, ctx.fence_generation)
            .await
        {
            return;
        }
        // Client checkpoint lock second (manager-to-client order holds
        // everywhere), then re-check the client epoch: its scope is the
        // whole process, while manager generations are per-connection, so
        // a stale run can pass a dead manager's fence check yet must not
        // write demand after a logout/switch bumped the epoch.
        let _client_guard = Arc::new(self.checkpoint_save.clone().lock_owned().await);
        if self.reconcile_epoch() != ctx.epoch {
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
            let cover_top = match (covering_open, chain_top) {
                // The covering chain connected this run: clear the cursor
                // and widen the proven top to the chain's starting top.
                (false, Some(chain)) => Some(match stored.cover_top {
                    Some(t) => t.max(chain),
                    None => chain,
                }),
                // Chain still open or none: legacy retention rules below.
                _ => {
                    match (stored.edge, stored.cover_top, run_top) {
                        // Retained top describes the contiguous range already
                        // proven; never replace it mid-chain.
                        (_, Some(t), _) => Some(t),
                        // Legacy row (edge without a proven top): keep it empty
                        // so the resume fallback (top = edge) pages through the
                        // edge instead of jumping over potentially uncached
                        // arrivals.
                        (Some(_), None, _) => None,
                        // Fresh claim: the current run's top starts the proven
                        // range.
                        (None, None, c) => c,
                    }
                }
            };
            // Covering cursor: only while the chain is open and a resume
            // offset exists; a chain broken without a cursor restarts
            // newest, so its top claim is dropped too.
            let covering = if covering_open && next_offset.is_some() && edge.is_some() {
                next_offset
            } else {
                None
            };
            let covering_top = covering.and_then(|_| stored.covering_top.or(chain_top));
            let resume = CatchupResume {
                edge,
                cover_top: if edge.is_some() { cover_top } else { None },
                covering,
                covering_top,
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
        let commit_guard = _client_guard.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _guard = commit_guard;
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
    /// and the in-memory map. Missing-table is not an error. Holds the
    /// manager checkpoint lock, then the client checkpoint lock
    /// (manager-to-client order holds everywhere), and re-checks both the
    /// run fence and the client epoch first: a stale run that reaches the
    /// boundary after a scope change leaves the live scope's demand alone
    /// instead of clearing it.
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
        // See persist_catchup_demand: the epoch check covers scope changes
        // the per-connection manager fence cannot see.
        let _client_guard = Arc::new(self.checkpoint_save.clone().lock_owned().await);
        if self.reconcile_epoch() != ctx.epoch {
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
        let commit_guard = _client_guard.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let _guard = commit_guard;
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
        let Some(edge) = resume.edge else {
            return Vec::new();
        };
        let mut out = format!("resume_offset={edge}");
        if let Some(t) = resume.cover_top {
            out.push_str(&format!(";cover_top={t}"));
        }
        if let Some(c) = resume.covering {
            out.push_str(&format!(";covering={c}"));
        }
        if let Some(t) = resume.covering_top {
            out.push_str(&format!(";covering_top={t}"));
        }
        out.into_bytes()
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
        let mut covering = None;
        let mut covering_top = None;
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
                "covering" => covering = Some(n),
                "covering_top" => covering_top = Some(n),
                _ => {}
            }
        }
        // Fields without their precondition are meaningless (same
        // invariants as persist): top and covering require an edge, and a
        // covering top requires the cursor.
        let with_edge = edge.is_some();
        cover_top = if with_edge { cover_top } else { None };
        covering = if with_edge { covering } else { None };
        covering_top = if covering.is_some() {
            covering_top
        } else {
            None
        };
        CatchupResume {
            edge,
            cover_top,
            covering,
            covering_top,
        }
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
    #[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
    async fn heal_via_history(
        &self,
        mgr: &SyncManager,
        fence_principal: &str,
        fence_generation: u64,
        run_epoch: u64,
        target: &tinkle_schema::proto::Target,
        target_key: &str,
    ) {
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
                    // Fence the heal: hold the manager checkpoint lock,
                    // re-check the run fence, and commit under the fenced
                    // principal + client epoch so a heal started before
                    // logout cannot write afterward. Losses are logged and
                    // dropped; the heal stays best-effort.
                    let _guard = mgr.checkpoint_save_lock().await;
                    if mgr.check_fence(fence_principal, fence_generation).await
                        && self.reconcile_epoch() == run_epoch
                    {
                        let empty = HashSet::new();
                        let _ = self
                            .ingest_messages(
                                fence_principal,
                                target_key,
                                IngestBatch {
                                    messages: &resp.messages,
                                    users: &resp.users,
                                    chats: &resp.chats,
                                    exclude: &empty,
                                },
                                run_epoch,
                            )
                            .await;
                    } else {
                        tracing::debug!(target_key, "history heal fenced; dropping page");
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
        let epoch = self.reconcile_epoch();
        let _sub = self.sub_lock.lock().await;
        let scope = self.connect_lock.lock().await;
        if self.reconcile_epoch() != epoch || self.is_signed_out() {
            return Err(TinkleError::Sync("subscription scope changed".into()));
        }
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
        {
            let mut g = self.interests.lock().await;
            if self.reconcile_epoch() != epoch || self.is_signed_out() {
                return Err(TinkleError::Sync("subscription scope changed".into()));
            }
            mgr.track_targets(targets.clone()).await;
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
        drop(scope);
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

    /// Drain retained managers before waiting on the current one, following connection replacement.
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

    /// Keep at most two distinct retired event sources available for subsequent draining.
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

    /// Fence the live channel and join retired workers, retaining ownership if teardown is cancelled.
    async fn teardown_connection(&self) {
        let mut retiring = self.retiring.lock().await;
        {
            let mut live = self.live.write().await;
            if let Some(live) = live.take() {
                live.generation.open.store(false, Ordering::SeqCst);
                live.generation.closed.cancel();
                self.transport_ok.store(false, Ordering::SeqCst);
                retiring.push(live);
            }
        }
        while let Some(live) = retiring.last_mut() {
            if let Some(task) = live.liveness.as_mut() {
                task.shutdown().await;
            }
            if let Some(task) = live.watch_restore.as_mut() {
                task.shutdown().await;
            }
            live.pump.shutdown().await;
            live.handle.suspend();
            let _ = live
                .handle
                .shutdown_transport(std::time::Duration::from_secs(1))
                .await;
            live.sync_mgr.on_disconnect().await;
            self.retain_manager(live.sync_mgr.clone()).await;
            retiring.pop();
        }
    }

    /// Stop transport and supervised work without signing out or erasing protected credentials.
    pub async fn close(&self) {
        self.stop().await;
    }

    /// Stop observation, supervisor, and transport before draining native commits outside the dial gate.
    pub async fn stop(&self) {
        self.pause_retirement_schedule().await;
        self.stop_supervisor().await;
        {
            let _connect = self.connect_lock.lock().await;
            self.teardown_connection().await;
        }
        // Explicit stop may drain writes, but must not hold up logout's
        // transport retirement/protected-store invalidation via connect_lock.
        #[cfg(not(target_arch = "wasm32"))]
        let _commits = self.checkpoint_save.lock().await;
    }

    /// Join the async supervisor without blocking protected-store invalidation on cache writers.
    async fn stop_supervisor(&self) {
        // Keep the aborted join owner in shared state if this waiter is dropped.
        let mut sup = self.sup_state.lock().await;
        sup.epoch = sup.epoch.wrapping_add(1);
        if let Some(tx) = sup.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(h) = sup.handle.as_mut() {
            h.abort();
            let _ = h.await;
        }
        sup.handle = None;
        drop(sup);
        // Cache draining belongs to wipe/reset, after protected-store invalidation.
        // Aborting this owner cannot abort its already-started blocking writes.
        *self.signal_tx.lock().await = None;
    }

    /// Make suspension authoritative over older resume work, then retire observation and transport.
    pub async fn suspend(&self) {
        {
            let mut sup = self.sup_state.lock().await;
            sup.epoch = sup.epoch.wrapping_add(1);
            sup.suspension_epoch = sup.suspension_epoch.wrapping_add(1);
            if let Some(cancel) = sup.resume_cancel.take() {
                cancel.cancel();
            }
            self.suspended.store(true, Ordering::SeqCst);
        }
        self.pause_retirement_schedule().await;
        let _connect = self.connect_lock.lock().await;
        self.teardown_connection().await;
    }

    /// Join or retain a foreground transition so waiter cancellation cannot strand local suspension.
    pub async fn resume(&self) {
        if self.authorization.is_signed_out() {
            return;
        }
        // Admission precedes the owner queue: a later suspend supersedes queued
        // requests as well as the transition currently running for this epoch.
        let epoch = self.sup_state.lock().await.suspension_epoch;
        let mut owner = self.resume_task.lock().await;
        if self.sup_state.lock().await.suspension_epoch != epoch {
            return;
        }
        if let Some(current) = owner.as_mut() {
            let join_current = !current.task.is_finished() && epoch == current.suspension_epoch;
            let _ = (&mut current.task).await;
            *owner = None;
            if join_current {
                return;
            }
        }
        if self.is_signed_out() {
            return;
        }
        let mut sup = self.sup_state.lock().await;
        if sup.suspension_epoch != epoch {
            return;
        }
        let cancelled = CancelToken::new();
        sup.resume_cancel = Some(cancelled.clone());
        self.suspended.store(true, Ordering::SeqCst);
        let caller = self.background_clone();
        let owner_gone = self.owner_gone();
        // Spawn before yielding after the suspension write. The shared join owner
        // survives waiter cancellation; the worker never locks resume_task.
        *owner = Some(ResumeTask {
            suspension_epoch: epoch,
            task: tokio::spawn(async move {
                caller.resume_owned(epoch, cancelled, owner_gone).await;
            }),
        });
        drop(sup);
        if let Some(current) = owner.as_mut() {
            let _ = (&mut current.task).await;
        }
        *owner = None;
    }

    /// One retained foreground transition. A later suspend cancels entry and
    /// fences completion without abandoning teardown's retained task owners.
    async fn resume_owned(
        &self,
        suspension_epoch: u64,
        cancelled: CancelToken,
        owner_gone: CancelToken,
    ) {
        // A foreground/wake call never carries timers or an unverified old
        // socket across the platform suspension boundary.
        let connection = tokio::select! { biased;
            _ = cancelled.cancelled() => return,
            _ = owner_gone.cancelled() => return,
            _ = self.authorization.revoked.cancelled() => return,
            connection = self.connect_lock.lock() => connection,
        };
        self.teardown_connection().await;
        drop(connection);
        tokio::select! { biased;
            _ = cancelled.cancelled() => return,
            _ = owner_gone.cancelled() => return,
            _ = self.authorization.revoked.cancelled() => return,
            _ = self.foreground_retirement() => {},
        }
        let sup = self.sup_state.lock().await;
        if !self.is_signed_out() && sup.suspension_epoch == suspension_epoch {
            self.suspended.store(false, Ordering::SeqCst);
        }
    }

    /// Inspect the local admission gate used by lifecycle and background workers.
    pub async fn is_suspended(&self) -> bool {
        self.suspended.load(Ordering::SeqCst)
    }

    /// Make one explicit replacement attempt and restore interests without replaying application RPCs.
    pub async fn reconnect(&self) -> Result<(), TinkleError> {
        self.reconnect_once(false, &mut None).await
    }

    /// Fence restoration and retry bookkeeping to this attempt's source and installed generation.
    async fn reconnect_once(
        &self,
        automatic: bool,
        fence: &mut Option<ReconnectFence>,
    ) -> Result<(), TinkleError> {
        self.authorization.generation()?;
        if self.suspended.load(Ordering::SeqCst) {
            return Err(TinkleError::NotConnected(
                "suspended; reconnect suppressed".into(),
            ));
        }
        let connection = self.connect_lock.lock().await;
        // Policy may become terminal while this attempt waits behind account
        // retirement/activation. A queued supervisor request cannot unpark it.
        if automatic && self.sup_state.lock().await.reconnect_blocked {
            return Err(TinkleError::Configuration(
                "automatic reconnect is parked".into(),
            ));
        }
        // Resume may wake the supervisor while a manual dial already owns this
        // gate. Its queued loss observation must not retire that healthy result.
        if automatic && self.is_connected().await {
            return Ok(());
        }
        let attempt = Arc::new(());
        self.sup_state.lock().await.reconnect_attempt = Some(attempt.clone());
        let source_revision = self.retirement.revision();
        *fence = Some(ReconnectFence {
            source_revision,
            attempt: attempt.clone(),
            generation: None,
        });
        self.teardown_connection().await;
        // One attempt only. The supervisor schedules subsequent attempts.
        self.connect_locked().await?;
        let generation = self
            .live
            .read()
            .await
            .as_ref()
            .map(|live| live.generation.clone());
        if let Some(fence) = fence.as_mut() {
            fence.generation = generation.clone();
        }
        drop(connection);
        if self.suspended.load(Ordering::SeqCst) {
            self.teardown_generation(generation.as_ref()).await;
            return Err(TinkleError::NotConnected("suspended after connect".into()));
        }
        let (subscribed, targets) = {
            let g = self.interests.lock().await;
            (g.subscribed, g.targets.clone())
        };
        if subscribed {
            let mut caller = self.clone();
            caller.call_generation = generation.clone();
            if let Err(e) = caller.subscribe(targets).await {
                self.teardown_generation(generation.as_ref()).await;
                return Err(e);
            }
        }
        if !automatic {
            let _connection = self.connect_lock.lock().await;
            if self.retirement.revision() == source_revision
                && self.live.read().await.as_ref().is_some_and(|live| {
                    generation
                        .as_ref()
                        .is_some_and(|expected| Arc::ptr_eq(expected, &live.generation))
                })
            {
                let mut sup = self.sup_state.lock().await;
                if !sup.reconnect_blocked
                    && sup
                        .reconnect_attempt
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(current, &attempt))
                {
                    sup.reconnect_failures = 0;
                    sup.next_reestablish = None;
                }
            }
        }
        Ok(())
    }

    /// Delayed restoration failure may retire only the connection this attempt created.
    async fn teardown_generation(&self, expected: Option<&Arc<LiveGeneration>>) {
        let _connection = self.connect_lock.lock().await;
        let matches = self.live.read().await.as_ref().is_some_and(|live| {
            expected.is_some_and(|generation| Arc::ptr_eq(generation, &live.generation))
        });
        if matches {
            self.teardown_connection().await;
        }
    }

    /// Establish retained interests and one supervisor after the foreground retirement barrier.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn start(
        &self,
        targets: Vec<tinkle_schema::proto::Target>,
    ) -> Result<(), TinkleError> {
        self.authorization.generation()?;
        let _guard = self.start_lock.lock().await;
        if self.suspended.load(Ordering::SeqCst) {
            self.resume().await;
        }
        self.foreground_retirement().await?;
        {
            let sup = self.sup_state.lock().await;
            if sup.handle.is_some() {
                return Ok(());
            }
        }
        let epoch = self.sup_state.lock().await.epoch;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (sig_tx, sig_rx) = mpsc::channel(64);
        *self.signal_tx.lock().await = Some(sig_tx);
        let result = self.establish(targets).await;
        match result {
            Ok(()) => {
                let mut sup = self.sup_state.lock().await;
                if sup.epoch != epoch || sup.handle.is_some() {
                    return Err(TinkleError::NotConnected("stopped during start".into()));
                }
                let task_client = self.background_clone();
                let owner_gone = self.owner.gone.clone();
                let handle = tokio::spawn(async move {
                    tokio::select! {
                        biased;
                        _ = owner_gone.cancelled() => { task_client.teardown_connection().await; },
                        _ = task_client.clone().supervise(sig_rx, shutdown_rx) => {},
                    }
                });
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

    /// Reject supervised startup on unsupported wasm runtimes without attempting native I/O.
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

    /// Connect and subscribe for startup, rejecting completion after a supervisor epoch change.
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
        // A Bootstrap OTP flow can supervise transport without claiming an
        // application principal or subscribing to account data.
        if targets.is_empty() && self.principal().await == UNKNOWN_PRINCIPAL {
            return Ok(());
        }
        let result = self.subscribe(targets).await;
        if self.sup_state.lock().await.epoch != epoch {
            self.teardown_connection().await;
            return Err(TinkleError::NotConnected("stopped during start".into()));
        }
        result
    }

    /// Drive bounded recovery signals and periodic demand sweeps through the sole reconnect scheduler.
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

    /// Prune untracked demand and retry only targets whose per-target backoff has elapsed.
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

    /// Attempt recovery only after liveness classification, backoff, and terminal parking allow it.
    #[cfg(not(target_arch = "wasm32"))]
    async fn reestablish(&self) {
        if self.authorization.is_signed_out() {
            return;
        }
        if self.suspended.load(Ordering::SeqCst) {
            return;
        }
        if self.live.read().await.as_ref().is_some_and(|live| {
            live.generation.returning
                && !live.generation.open.load(Ordering::SeqCst)
                && !live.generation.classified.load(Ordering::SeqCst)
        }) {
            return;
        }
        {
            let sup = self.sup_state.lock().await;
            let now = std::time::Instant::now();
            if sup.reconnect_blocked
                || sup.next_reestablish.is_some_and(|due| now < due)
                || !should_retry(&self.config.reconnect, sup.reconnect_failures)
            {
                return;
            }
        }
        if self.is_connected().await {
            return;
        }
        tracing::debug!("supervisor re-establishing connection");
        let mut fence = None;
        let result = self.reconnect_once(true, &mut fence).await;
        if let Some(fence) = fence {
            self.apply_reconnect_result(&fence, result).await;
        }
    }

    /// A completed predecessor attempt cannot park or charge a replacement source.
    #[cfg(not(target_arch = "wasm32"))]
    async fn apply_reconnect_result(
        &self,
        fence: &ReconnectFence,
        result: Result<(), TinkleError>,
    ) {
        let _connection = self.connect_lock.lock().await;
        if self.retirement.revision() != fence.source_revision {
            return;
        }
        if self.live.read().await.as_ref().is_some_and(|live| {
            fence
                .generation
                .as_ref()
                .is_none_or(|expected| !Arc::ptr_eq(expected, &live.generation))
        }) {
            return;
        }
        let mut sup = self.sup_state.lock().await;
        if !sup
            .reconnect_attempt
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &fence.attempt))
        {
            return;
        }
        match result {
            Ok(()) => {
                sup.reconnect_failures = 0;
                sup.next_reestablish = None;
            }
            Err(e) => {
                sup.reconnect_blocked |= !matches!(e.recovery_decision(), RecoveryDecision::Retry);
                sup.next_reestablish = Some(
                    std::time::Instant::now()
                        + self.config.reconnect.delay_for(sup.reconnect_failures),
                );
                sup.reconnect_failures = sup.reconnect_failures.saturating_add(1);
                tracing::debug!(error = %e, blocked = sup.reconnect_blocked, "supervisor re-establish failed");
            }
        }
    }

    /// Retry a tracked, non-running target in bounded attempts, retaining demand on failure.
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
        let run_epoch = self.reconcile_epoch();
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
                            IngestBatch {
                                messages: &all_messages,
                                users: &all_users,
                                chats: &all_chats,
                                exclude: &empty,
                            },
                            run_epoch,
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

    /// Aggregate bounded history pages on the non-SQLite path without advancing journal positions.
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
        let epoch = self.reconcile_epoch();
        let principal = self.principal().await;
        let resp = self.messages().get_dialogs(req).await?;
        self.ingest_snapshot_entities(&principal, epoch, &resp.users, &resp.chats)
            .await;
        self.ingest_snapshot_dialogs(&principal, epoch, &resp.dialogs)
            .await;
        Ok(resp)
    }

    /// Return the typed dialog snapshot without native cache hydration.
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
        principal: &str,
        epoch: u64,
        users: &[tinkle_schema::proto::User],
        chats: &[tinkle_schema::proto::Chat],
    ) {
        let (principal, storage) = match (principal, self.sqlite.clone()) {
            (p, Some(s)) if p != UNKNOWN_PRINCIPAL => (p.to_string(), s),
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
        let commit_guard = self.checkpoint_save.clone().lock_owned().await;
        let current_epoch = self.reconcile_epoch.clone();
        let authorization = self.authorization.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _guard = commit_guard;
            if current_epoch.load(Ordering::SeqCst) != epoch || authorization.is_signed_out() {
                return;
            }
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

    /// Globally pin dialog rows only while the captured epoch remains valid at the blocking write.
    #[cfg(not(target_arch = "wasm32"))]
    async fn ingest_snapshot_dialogs(
        &self,
        principal: &str,
        epoch: u64,
        dialogs: &[tinkle_schema::proto::Dialog],
    ) {
        use prost::Message as _;
        let (principal, storage) = match (principal, self.sqlite.clone()) {
            (p, Some(s)) if p != UNKNOWN_PRINCIPAL => (p.to_string(), s),
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
        let commit_guard = self.checkpoint_save.clone().lock_owned().await;
        let current_epoch = self.reconcile_epoch.clone();
        let authorization = self.authorization.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _guard = commit_guard;
            if current_epoch.load(Ordering::SeqCst) != epoch || authorization.is_signed_out() {
                return;
            }
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

    /// Replace host-supplied channel trust/enrollment, retiring the old channel.
    /// Raw credentials withdraw the previous application principal and interests.
    /// A different deployment requires a new client with a separate cache namespace;
    /// verified enrollment installation supplies its own validated principal.
    pub async fn replace_trust_source(&self, next: TrustSource) -> Result<(), TinkleError> {
        let authorization = self.authorization.generation()?;
        next.security()?;
        let _connect = self.connect_lock.lock().await;
        authorization.check(self)?;
        let next_trust = match &next {
            TrustSource::Bootstrap(trust)
            | TrustSource::SignedOut(trust)
            | TrustSource::Returning { trust, .. } => trust,
        };
        // Cache keys contain a user id, not a deployment identity. A different
        // trust domain needs a new Client with its own cache/cursor namespace.
        if *next_trust != self.enrollment_trust().await {
            return Err(TinkleError::Configuration(
                "a different deployment requires a new client and cache namespace".into(),
            ));
        }
        self.retirement.invalidate();
        let was_supervised = self.sup_state.lock().await.handle.is_some();
        self.teardown_connection().await;
        {
            #[cfg(not(target_arch = "wasm32"))]
            let _cache = self.checkpoint_save.lock().await;
            authorization.check(self)?;
            #[cfg(not(target_arch = "wasm32"))]
            self.reconcile_epoch.fetch_add(1, Ordering::SeqCst);
            // Raw credentials carry no verified user id; never inherit the old account.
            self.reset_session_locked(None).await;
        }
        if let TrustSource::Returning { enrollment, .. } = &next {
            self.authorization
                .register_source(authorization.generation, enrollment.secret())?;
        }
        let mut source = self.trust_source.write().await;
        self.authorization
            .with_generation(authorization.generation, || *source = next)?;
        drop(source);
        self.retirement.reset_endpoint(&self.endpoint_rejected);
        {
            let mut sup = self.sup_state.lock().await;
            sup.reconnect_blocked = false;
            sup.reconnect_failures = 0;
            sup.next_reestablish = None;
            self.connect_lock.selected_port.store(0, Ordering::SeqCst);
        }
        drop(_connect);
        if was_supervised {
            self.reconnect().await
        } else {
            Ok(())
        }
    }

    /// Inspect configured channel mode without treating retained credentials as current admission.
    pub(crate) async fn has_returning_source(&self) -> bool {
        matches!(
            &*self.trust_source.read().await,
            TrustSource::Returning { .. }
        )
    }

    /// A verified activation withdraws the previous tuple even if subsequent
    /// local persistence fails. Park recovery and erase the old in-memory key.
    pub(crate) async fn retire_for_activation(
        &self,
        trust: &crate::ServerTrust,
        authorization: &crate::authorization::EnrollmentEpoch,
    ) -> Result<(), TinkleError> {
        let _guard = self.connect_lock.lock().await;
        authorization.check(self)?;
        if *trust != self.enrollment_trust().await {
            return Err(TinkleError::Configuration(
                "enrollment trust changed during activation".into(),
            ));
        }
        self.sup_state.lock().await.reconnect_blocked = true;
        self.retirement.invalidate();
        self.teardown_connection().await;
        let mut source = self.trust_source.write().await;
        self.authorization
            .with_generation(authorization.generation, || {
                *source = TrustSource::Bootstrap(trust.clone())
            })?;
        Ok(())
    }

    /// Install an ACTIVE record with the old transport retired before changing
    /// application scope. No Bootstrap connection ever carries the new principal.
    pub(crate) async fn install_active_source(
        &self,
        source: TrustSource,
        user_id: i64,
        authorization: &crate::authorization::EnrollmentEpoch,
        observation: Option<(
            tinkle_schema::proto::DeviceEnrollmentStatus,
            Arc<dyn crate::SecureEnrollmentStore>,
        )>,
        restoration: &mut crate::enrollment::RestorePermit,
    ) -> Result<(), TinkleError> {
        let guard = self.connect_lock.lock().await;
        authorization.check(self)?;
        if restoration
            .binding
            .as_ref()
            .is_some_and(|binding| !self.retirement.is_current(binding))
        {
            return Err(TinkleError::Configuration(
                "restored enrollment was superseded".into(),
            ));
        }
        let proposed_trust = match &source {
            TrustSource::Bootstrap(trust)
            | TrustSource::SignedOut(trust)
            | TrustSource::Returning { trust, .. } => trust.clone(),
        };
        if proposed_trust != self.enrollment_trust().await {
            return Err(TinkleError::Configuration(
                "enrollment trust changed during activation".into(),
            ));
        }
        self.teardown_connection().await;
        #[cfg(not(target_arch = "wasm32"))]
        if *self.principal.read().await != format!("user:{user_id}") {
            let _cache = self.checkpoint_save.lock().await;
            authorization.check(self)?;
            self.reconcile_epoch.fetch_add(1, Ordering::SeqCst);
            self.reset_session_locked(None).await;
        }
        if let TrustSource::Returning { enrollment, .. } = &source {
            self.authorization
                .register_source(authorization.generation, enrollment.secret())?;
        }
        let mut current_source = self.trust_source.write().await;
        self.authorization
            .with_generation(authorization.generation, || *current_source = source)?;
        if let (TrustSource::Returning { enrollment, .. }, Some((status, _))) =
            (&*current_source, &observation)
        {
            restoration.installed = Some((enrollment.secret(), status.clone()));
        }
        drop(current_source);
        self.retirement.invalidate();
        if let Some((status, store)) = observation {
            let conditional = self.authorization.store_revision(store.as_ref()).is_some();
            restoration.binding = Some(self.retirement.bind(
                proposed_trust.clone(),
                &status,
                store,
                false,
                conditional,
            ));
        }
        self.set_principal_locked(user_id).await;
        authorization.check(self)?;
        {
            let mut sup = self.sup_state.lock().await;
            sup.reconnect_blocked = self.endpoint_rejected.load(Ordering::SeqCst);
            sup.reconnect_failures = 0;
            sup.next_reestablish = None;
        }
        drop(guard);
        if !self.is_suspended().await {
            self.arm_retirement_schedule().await;
        }
        if let Err(error) = self.reconnect().await {
            tracing::debug!(code = ?error.code(), "ACTIVE enrollment installed; reconnect failed");
        }
        Ok(())
    }

    /// Withdraw only this failed install before releasing its protected-store permit.
    pub(crate) async fn clear_failed_restore(
        &self,
        binding: &Arc<crate::retirement::RetainedEnrollment>,
        installed: Option<&(
            Arc<crate::authorization::SecretCell>,
            tinkle_schema::proto::DeviceEnrollmentStatus,
        )>,
    ) {
        let _connection = self.connect_lock.lock().await;
        if let Some((secret, status)) = installed {
            // Lifecycle stop may revise metadata without replacing this source.
            // A successor with the same public tuple still owns a different cell.
            let source = self.trust_source.read().await;
            if !matches!(&*source, TrustSource::Returning { enrollment, .. }
                if Arc::ptr_eq(&enrollment.secret(), secret))
            {
                return;
            }
            drop(source);
            self.retirement.fence_restore(&binding.trust, status);
            self.authorization.set_authentication_known(false);
            self.sup_state.lock().await.reconnect_blocked = true;
            self.teardown_connection().await;
            let mut source = self.trust_source.write().await;
            if let TrustSource::Returning { trust, .. } = &*source {
                *source = TrustSource::Bootstrap(trust.clone());
            }
        } else {
            self.retirement.clear_restoring_binding(binding);
            return;
        }
        self.retirement.invalidate();
    }

    /// Inspect current runtime correlation demand, returning zero when no channel is retained.
    pub async fn pending_len(&self) -> usize {
        match self.live.read().await.as_ref() {
            Some(l) => l.handle.conn.pending_len(),
            None => 0,
        }
    }

    /// Inspect the retained channel identity; its presence alone does not attest an open transport.
    pub async fn connection_id(&self) -> Option<String> {
        self.live
            .read()
            .await
            .as_ref()
            .map(|l| l.connection_id.clone())
    }

    /// Inspect the retained channel's negotiated Syntra version, not application schema compatibility.
    pub async fn version(&self) -> Option<u32> {
        self.live.read().await.as_ref().map(|l| l.version)
    }
}

#[cfg(test)]
mod tests {
    use super::TargetDemand;

    /// Observe actual SDK-admitted budgets, without the legacy wire fixture adapter.
    #[derive(Default)]
    struct BudgetHandler(
        std::sync::Mutex<Vec<(String, std::time::Duration)>>,
        std::sync::atomic::AtomicBool,
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::atomic::AtomicBool,
        std::sync::Mutex<Option<syntra_core::ErrorCode>>,
    );

    impl syntra_runtime::Handler for BudgetHandler {
        /// Let the fixture handler, rather than transport authorization, choose test denial codes.
        fn authorize(
            &self,
            _: &syntra_runtime::Principal,
            _: &str,
        ) -> Result<(), syntra_runtime::AuthError> {
            Ok(())
        }

        /// Reject the legacy handler path so budget tests cannot omit the SDK request context.
        fn handle(
            &self,
            _: String,
            _: Vec<u8>,
            _: syntra_runtime::CancelToken,
        ) -> syntra_runtime::HandlerFut {
            panic!("budget fixture requires the published request context")
        }

        /// Record admitted budgets and exercise held, rejected, or echoed application responses.
        fn handle_with_context(
            &self,
            method: String,
            payload: Vec<u8>,
            _: syntra_runtime::CancelToken,
            lease: syntra_runtime::ExecutionLease,
            context: syntra_runtime::terminal::RequestContext,
        ) -> syntra_runtime::HandlerFut {
            use prost::Message;
            let wrapped = method == "/tinkle.v1.ConnectionService/InitConnect";
            let (method, payload) = if wrapped {
                let request =
                    tinkle_schema::proto::InitConnectRequest::decode(payload.as_slice()).unwrap();
                (request.method, request.payload.to_vec())
            } else {
                (method, payload)
            };
            self.0
                .lock()
                .unwrap()
                .push((method.clone(), context.budget().remaining().unwrap()));
            let unavailable = method == tinkle_schema::sync::OBSERVE_METHOD
                && self.1.swap(false, std::sync::atomic::Ordering::SeqCst);
            let hold = self.4.load(std::sync::atomic::Ordering::SeqCst);
            let entered = self.2.clone();
            let released = self.3.clone();
            let denial = *self.5.lock().unwrap();
            Box::pin(async move {
                let _lease = lease;
                entered.notify_one();
                if hold {
                    released.notified().await;
                }
                if unavailable {
                    return syntra_runtime::HandlerOutcome::error(
                        syntra_core::ErrorCode::Unavailable,
                        "retry observation",
                    );
                }
                if let Some(code) = denial {
                    return syntra_runtime::HandlerOutcome::error(code, "denied");
                }
                let payload = if method.ends_with("/Logout") {
                    tinkle_schema::proto::LogoutResponse { logged_out: true }.encode_to_vec()
                } else if method == crate::liveness::KEEP_ALIVE_METHOD {
                    let request =
                        tinkle_schema::proto::KeepAliveRequest::decode(payload.as_slice()).unwrap();
                    tinkle_schema::proto::KeepAliveResponse {
                        sequence: request.sequence,
                    }
                    .encode_to_vec()
                } else {
                    Vec::new()
                };
                syntra_runtime::HandlerOutcome::Payload(if wrapped {
                    tinkle_schema::proto::InitConnectResponse {
                        payload: payload.into(),
                    }
                    .encode_to_vec()
                } else {
                    payload
                })
            })
        }
    }

    /// Fixed Returning key for a private, encrypted TCP budget fixture.
    struct BudgetKeys;
    impl syntra_core::secure_channel::KeyStore for BudgetKeys {
        /// Supply the fixed synthetic key used by this fixture's Returning channel.
        fn lookup(
            &self,
            _: &syntra_core::secure_channel::Context,
        ) -> Result<syntra_core::secure_channel::Secret32, syntra_core::secure_channel::Error>
        {
            Ok(syntra_core::secure_channel::Secret32::new([9; 32]))
        }
    }

    /// Connect one real published-SDK peer; tests may hold private lifecycle/init locks.
    async fn budget_fixture(
        returning: bool,
        default: std::time::Duration,
        max: std::time::Duration,
    ) -> (
        super::Client,
        std::sync::Arc<BudgetHandler>,
        tokio::task::JoinHandle<()>,
    ) {
        budget_fixture_connections(returning, default, max, 1).await
    }

    /// Accept a fixed number of encrypted generations to make reconnect tests self-terminating.
    async fn budget_fixture_connections(
        returning: bool,
        default: std::time::Duration,
        max: std::time::Duration,
        connections: usize,
    ) -> (
        super::Client,
        std::sync::Arc<BudgetHandler>,
        tokio::task::JoinHandle<()>,
    ) {
        use super::*;
        use syntra_core::secure_channel::{self, Carrier, Mode};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (key, pin) = secure_channel::generate_server_key().unwrap();
        let trust = crate::ServerTrust {
            server_public_key: pin,
            server_key_id: 7,
            scope: "budget-fixture".into(),
        };
        let info = crate::ClientInfo::new(
            "device".into(),
            "test".into(),
            "1".into(),
            "1".into(),
            "1".into(),
            Default::default(),
        )
        .unwrap();
        let mut config = ClientConfig::new("127.0.0.1", info);
        config.primary_port = listener.local_addr().unwrap().port();
        config.fallback_port = None;
        config.connection.default_timeout = default;
        config.connection.max_rpc_timeout = max;
        let source = if returning {
            TrustSource::Returning {
                trust,
                enrollment: crate::Enrollment::new([9; 32], [8; 8], 1).unwrap(),
            }
        } else {
            TrustSource::Bootstrap(trust)
        };
        let client = Client::new(config, source, crate::InMemoryCursorStore::new());
        let handler = Arc::new(BudgetHandler::default());
        let recorded = handler.clone();
        let server = tokio::spawn(async move {
            let security = secure_channel::ServerConfig {
                mode: if returning {
                    Mode::Returning
                } else {
                    Mode::Bootstrap
                },
                carrier: Carrier::Tcp,
                server_key_id: 7,
                scope: b"budget-fixture".to_vec(),
                private_key: key,
                store: Some(Arc::new(BudgetKeys)),
            };
            let factory: Arc<dyn syntra_net::AuthenticatorFactory> =
                Arc::new(
                    |_: &secure_channel::VerifiedIdentity| -> Result<
                        Arc<dyn syntra_runtime::Authenticator>,
                        syntra_runtime::AuthError,
                    > { Ok(Arc::new(syntra_runtime::AllowAnonymous)) },
                );
            for _ in 0..connections {
                let (stream, _) = listener.accept().await.unwrap();
                let handle = syntra_net::tcp::accept_stream(
                    stream,
                    &security,
                    factory.clone(),
                    recorded.clone(),
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap();
                while !handle.is_closed() {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                handle.close().await;
            }
        });
        client.connect().await.unwrap();
        (client, handler, server)
    }

    /// A confirmed result cannot be lost waiting for optional restoration locks.
    #[tokio::test]
    async fn review_confirmed_unary_never_waits_for_watch_or_live_locks() {
        use super::*;
        use std::time::Duration;
        for block_live in [false, true] {
            let (client, handler, server) =
                budget_fixture(false, Duration::from_secs(2), Duration::from_secs(2)).await;
            handler.4.store(true, Ordering::SeqCst);
            let watch = client.watch.lock().await;
            let caller = client.clone();
            let call = tokio::spawn(async move {
                caller
                    .call_raw("/tinkle.v1.AuthService/SendCode", Vec::new(), 1_000)
                    .await
            });
            handler.2.notified().await;
            let live = if block_live {
                Some(client.live.write().await)
            } else {
                None
            };
            handler.3.notify_one();
            tokio::time::timeout(Duration::from_millis(500), call)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            drop(live);
            drop(watch);
            client.close().await;
            server.await.unwrap();
        }
    }

    /// Keep optional observation demand alive through Unavailable despite a healthy heartbeat.
    #[tokio::test]
    async fn review_observation_retries_while_keep_alive_succeeds() {
        use super::*;
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(true, Duration::from_secs(2), Duration::from_secs(10)).await;
        handler.1.store(true, Ordering::SeqCst);
        client.set_watch(vec![42]).await.unwrap();
        client
            .call_raw("/tinkle.v1.AuthService/SendCode", Vec::new(), 1_000)
            .await
            .unwrap();
        let mut heartbeat = client.clone();
        heartbeat.call_generation = Some(
            client
                .live
                .read()
                .await
                .as_ref()
                .unwrap()
                .generation
                .clone(),
        );
        heartbeat
            .call_keep_alive(tinkle_schema::proto::KeepAliveRequest { sequence: 1 }.encode_to_vec())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let count = handler
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(method, _)| method == tinkle_schema::sync::OBSERVE_METHOD)
                    .count();
                if count >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(client.watch_list().await, vec![42]);
        assert!(client.is_connected().await);
        client.close().await;
        server.await.unwrap();
    }

    /// Hold local admission past the RPC ceiling without retiring the open generation.
    #[tokio::test]
    async fn review_keep_alive_local_contention_is_not_connection_failure() {
        use super::*;
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(true, Duration::from_secs(2), Duration::from_millis(40)).await;
        let (_, initialized) = client.live_conn().await.unwrap();
        let lock = initialized.lock().await;
        let generation = client
            .live
            .read()
            .await
            .as_ref()
            .unwrap()
            .generation
            .clone();
        let mut heartbeat = client.clone();
        heartbeat.call_generation = Some(generation.clone());
        let waiting = heartbeat.call_keep_alive(
            tinkle_schema::proto::KeepAliveRequest { sequence: 1 }.encode_to_vec(),
        );
        tokio::pin!(waiting);
        assert!(futures_util::poll!(&mut waiting).is_pending());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(futures_util::poll!(&mut waiting).is_pending());
        assert!(handler.0.lock().unwrap().is_empty());
        assert!(generation.open.load(Ordering::SeqCst));
        assert!(client.is_connected().await);
        drop(lock);
        assert!(waiting.await.unwrap().is_ok());
        assert!(heartbeat
            .call_keep_alive(tinkle_schema::proto::KeepAliveRequest { sequence: 2 }.encode_to_vec())
            .await
            .unwrap()
            .is_ok());
        client.close().await;
        server.await.unwrap();
    }

    /// Ensure a queued heartbeat keeps its place ahead of later unary initialization attempts.
    #[tokio::test]
    async fn followup_heartbeat_keeps_fifo_place_across_local_timeout_windows() {
        use super::*;
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(true, Duration::from_secs(1), Duration::from_millis(100)).await;
        let (_, initialized) = client.live_conn().await.unwrap();
        let lock = initialized.lock().await;
        let heartbeat = client.call_keep_alive(
            tinkle_schema::proto::KeepAliveRequest { sequence: 1 }.encode_to_vec(),
        );
        tokio::pin!(heartbeat);
        assert!(futures_util::poll!(&mut heartbeat).is_pending());
        // Queue an ordinary call behind the heartbeat, then hold local admission
        // longer than its eventual RPC budget without treating that as peer loss.
        let unary = client.call_raw("/tinkle.v1.AuthService/SendCode", vec![], 0);
        tokio::pin!(unary);
        assert!(futures_util::poll!(&mut unary).is_pending());
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(futures_util::poll!(&mut heartbeat).is_pending());
        assert!(client.is_connected().await);
        drop(lock);
        heartbeat.await.unwrap().unwrap();
        assert_eq!(
            handler.0.lock().unwrap()[0].0,
            crate::liveness::KEEP_ALIVE_METHOD
        );
        assert!(unary.await.is_err());
        client.close().await;
        server.await.unwrap();
    }

    /// Distinguish a dispatched heartbeat timeout from the initializer timeout that preceded it.
    #[tokio::test]
    async fn followup_stalled_initializer_does_not_mask_dispatched_heartbeat_failure() {
        use super::*;
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(true, Duration::from_secs(1), Duration::from_millis(100)).await;
        handler.4.store(true, Ordering::SeqCst);
        let caller = client.clone();
        let unary = tokio::spawn(async move {
            caller
                .call_raw("/tinkle.v1.AuthService/SendCode", vec![], 0)
                .await
        });
        handler.2.notified().await;
        let heartbeat = client.call_keep_alive(
            tinkle_schema::proto::KeepAliveRequest { sequence: 1 }.encode_to_vec(),
        );
        tokio::pin!(heartbeat);
        assert_eq!(
            unary.await.unwrap().unwrap_err().code(),
            syntra_core::ErrorCode::DeadlineExceeded
        );
        let error = heartbeat.await.unwrap().unwrap_err();
        assert_eq!(error.code(), syntra_core::ErrorCode::DeadlineExceeded);
        assert_eq!(handler.0.lock().unwrap().len(), 2);
        let generation = client
            .live
            .read()
            .await
            .as_ref()
            .unwrap()
            .generation
            .clone();
        client.fail_liveness(&generation, &error).await;
        assert!(!client.is_connected().await);
        client.close().await;
        server.await.unwrap();
    }

    /// Observe a dispatched initializer's expiry even when its caller stops polling the held future.
    #[tokio::test]
    async fn followup_unpolled_dispatched_initializer_is_detected_by_its_own_cutoff() {
        use super::*;
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(true, Duration::from_secs(1), Duration::from_millis(100)).await;
        handler.4.store(true, Ordering::SeqCst);
        {
            let unary = client.call_raw("/tinkle.v1.AuthService/SendCode", vec![], 0);
            tokio::pin!(unary);
            tokio::select! {
                _ = &mut unary => panic!("fixture must hold initialization"),
                _ = handler.2.notified() => {},
            }
            // Retain, but deliberately do not poll, the initializer again. Its
            // mutex remains held even when the dispatched request has expired.
            tokio::time::sleep(Duration::from_millis(150)).await;
            let error = client
                .call_keep_alive(
                    tinkle_schema::proto::KeepAliveRequest { sequence: 1 }.encode_to_vec(),
                )
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code(), syntra_core::ErrorCode::DeadlineExceeded);
            assert_eq!(handler.0.lock().unwrap().len(), 1, "no initializer replay");
        }
        client.close().await;
        server.await.unwrap();
    }

    /// Build synthetic reference metadata for local admission tests, not a usable ACTIVE receipt.
    fn followup_status() -> tinkle_schema::proto::DeviceEnrollmentStatus {
        tinkle_schema::proto::DeviceEnrollmentStatus {
            session_id: "12345678-1234-4234-8234-123456789abc".into(),
            handle: vec![8; 8].into(),
            enrollment_epoch: 1,
            user_id: 1,
            account_auth_epoch: 1,
            scope: 1,
            state: 2,
            ..Default::default()
        }
    }

    /// Cover restoration starting both before heartbeat entry and during its initialization wait.
    #[tokio::test]
    async fn followup_restoration_pending_defers_heartbeat_before_and_after_wait() {
        use super::*;
        use std::time::Duration;
        for after_wait in [false, true] {
            let (client, handler, server) =
                budget_fixture(true, Duration::from_secs(1), Duration::from_secs(1)).await;
            let (_, initialized) = client.live_conn().await.unwrap();
            let lock = initialized.lock().await;
            let heartbeat = client.call_keep_alive(
                tinkle_schema::proto::KeepAliveRequest { sequence: 1 }.encode_to_vec(),
            );
            tokio::pin!(heartbeat);
            if after_wait {
                assert!(futures_util::poll!(&mut heartbeat).is_pending());
            }
            client.retirement.bind(
                client.enrollment_trust().await,
                &followup_status(),
                Arc::new(BudgetStore::default()),
                true,
                false,
            );
            drop(lock);
            assert!(heartbeat.await.is_none());
            assert!(handler.0.lock().unwrap().is_empty());
            assert!(client.is_connected().await);
            client.retirement.invalidate();
            client
                .call_keep_alive(
                    tinkle_schema::proto::KeepAliveRequest { sequence: 2 }.encode_to_vec(),
                )
                .await
                .unwrap()
                .unwrap();
            client.close().await;
            server.await.unwrap();
        }
    }

    /// Resume actual periodic probing after a restoration barrier without replacing the channel.
    #[tokio::test]
    async fn followup_liveness_loop_survives_restoration_admission_barrier() {
        use super::*;
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(true, Duration::from_secs(1), Duration::from_secs(1)).await;
        client.retirement.bind(
            client.enrollment_trust().await,
            &followup_status(),
            Arc::new(BudgetStore::default()),
            true,
            false,
        );
        tokio::task::yield_now().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::time::resume();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(client.is_connected().await);
        assert!(handler.0.lock().unwrap().is_empty());
        client.retirement.invalidate();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(1), async {
            while handler.0.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            handler.0.lock().unwrap()[0].0,
            crate::liveness::KEEP_ALIVE_METHOD
        );
        assert!(client.is_connected().await);
        client.close().await;
        server.await.unwrap();
    }

    /// Apply both denial codes across raw/typed and initialized/wrapped dispatch without key erasure.
    #[tokio::test]
    async fn followup_raw_and_typed_denials_withdraw_auth_without_retiring_credentials() {
        use std::time::Duration;
        for code in [
            syntra_core::ErrorCode::PermissionDenied,
            syntra_core::ErrorCode::Unauthenticated,
        ] {
            for ready in [false, true] {
                for raw in [false, true] {
                    let (client, handler, server) =
                        budget_fixture(true, Duration::from_secs(1), Duration::from_secs(1)).await;
                    let (_, initialized) = client.live_conn().await.unwrap();
                    *initialized.lock().await = ready;
                    *handler.5.lock().unwrap() = Some(code);
                    let result = if raw {
                        client.call_raw("/example.Echo/Call", vec![], 0).await
                    } else {
                        client
                            .messages()
                            .get_history(Default::default())
                            .await
                            .map(|_| vec![])
                    };
                    assert_eq!(result.unwrap_err().code(), code);
                    assert!(!client.authorization.authentication_known());
                    if code == syntra_core::ErrorCode::PermissionDenied {
                        assert!(client.is_connected().await);
                    }
                    assert!(!client.is_signed_out());
                    assert!(client.has_returning_source().await);
                    assert_eq!(handler.0.lock().unwrap().len(), 1);
                    client.close().await;
                    server.await.unwrap();
                }
            }
        }
    }

    /// Release an older resume after suspend has advanced its fence and retain suspension.
    #[tokio::test]
    async fn review_resume_cannot_undo_later_suspend() {
        use std::time::Duration;
        let (client, _, server) =
            budget_fixture(false, Duration::from_secs(2), Duration::from_secs(2)).await;
        let gate = client.connect_lock.lock().await;
        let resume = client.resume();
        let suspend = client.suspend();
        tokio::pin!(resume, suspend);
        assert!(futures_util::poll!(&mut resume).is_pending());
        assert!(futures_util::poll!(&mut suspend).is_pending());
        drop(gate);
        tokio::join!(resume, suspend);
        assert!(client.is_suspended().await);
        client.close().await;
        server.await.unwrap();
    }

    /// A resume queued before suspend must not adopt the newer epoch after its predecessor finishes.
    #[tokio::test]
    async fn followup_queued_resume_cannot_undo_later_suspend() {
        use std::time::Duration;
        let (client, _, server) =
            budget_fixture(false, Duration::from_secs(1), Duration::from_secs(1)).await;
        let retiring = client.retiring.lock().await;
        let first = client.resume();
        let queued = client.resume();
        let suspend = client.suspend();
        tokio::pin!(first, queued, suspend);
        assert!(futures_util::poll!(&mut first).is_pending());
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.connect_lock.lock.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // A holds resume_task and waits in teardown; B has already entered resume.
        assert!(futures_util::poll!(&mut queued).is_pending());
        assert!(futures_util::poll!(&mut suspend).is_pending());
        drop(retiring);
        first.await;
        suspend.await;
        queued.await;
        assert!(client.is_suspended().await);
        assert!(!client.is_connected().await);
        assert!(client.sup_state.lock().await.resume_cancel.is_none());
        // Only a new request admitted after suspension may enter foreground again.
        client.resume().await;
        assert!(!client.is_suspended().await);
        client.close().await;
        server.await.unwrap();
    }

    /// Keep stop distinct from suspension so a concurrent resume still permits a later connect.
    #[tokio::test]
    async fn review_resume_concurrent_stop_does_not_require_another_resume() {
        use std::time::Duration;
        let (client, _, server) =
            budget_fixture_connections(false, Duration::from_secs(2), Duration::from_secs(2), 2)
                .await;
        let gate = client.connect_lock.lock().await;
        let resume = client.resume();
        let stop = client.stop();
        tokio::pin!(resume, stop);
        assert!(futures_util::poll!(&mut resume).is_pending());
        assert!(futures_util::poll!(&mut stop).is_pending());
        drop(gate);
        tokio::join!(resume, stop);
        assert!(!client.is_signed_out());
        assert!(!client.is_suspended().await);
        client.connect().await.unwrap();
        assert!(client.is_connected().await);
        client.close().await;
        server.await.unwrap();
    }

    /// Allow foreground completion to clear suspension after an unrelated supervisor epoch change.
    #[tokio::test]
    async fn review_resume_foreground_wait_ignores_supervisor_only_epoch_changes() {
        let client = crate::enrollment::review_tests::client(false);
        let resume = client.resume();
        tokio::pin!(resume);
        // No connection or locks: resume has reached its retained foreground entry.
        assert!(futures_util::poll!(&mut resume).is_pending());
        client.stop_supervisor().await;
        resume.await;
        assert!(!client.is_signed_out());
        assert!(!client.is_suspended().await);
        client.close().await;
    }

    /// Ensure retirement's supervisor stop cannot let resume reopen a signed-out client.
    #[tokio::test]
    async fn review_resume_retirement_cleanup_keeps_signed_out_suspended() {
        use super::*;
        let client = crate::enrollment::review_tests::client(false);
        let supervisor_epoch = client.sup_state.lock().await.epoch;
        let resume = client.resume();
        tokio::pin!(resume);
        assert!(futures_util::poll!(&mut resume).is_pending());
        // Exercise the retained cleanup completion that foreground retirement joins.
        let cleanup = client
            .logout_cleanup
            .join_retirement(client.background_clone(), "user:42".into());
        let (_, result) = tokio::join!(resume, cleanup);
        assert!(!result.unwrap());
        assert_ne!(client.sup_state.lock().await.epoch, supervisor_epoch);
        assert!(client.is_signed_out());
        assert!(client.is_suspended().await);
        assert!(matches!(
            client.connect().await,
            Err(TinkleError::SignedOut)
        ));
        client.close().await;
    }

    /// Join the retained transition directly without starting another explicit resume.
    async fn followup_drain_resume(client: &super::Client) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut owner = client.resume_task.lock().await;
            if let Some(current) = owner.as_mut() {
                (&mut current.task).await.unwrap();
            }
            *owner = None;
        })
        .await
        .unwrap();
    }

    /// Drop a teardown waiter, then verify completion and later suspend/logout authority separately.
    #[tokio::test]
    async fn followup_cancelled_resume_finishes_teardown_without_overriding_suspend_or_logout() {
        use std::time::Duration;
        for later in [0, 1, 2] {
            let (client, _, server) =
                budget_fixture(false, Duration::from_secs(1), Duration::from_secs(1)).await;
            let retiring = client.retiring.lock().await;
            {
                let resume = client.resume();
                tokio::pin!(resume);
                assert!(futures_util::poll!(&mut resume).is_pending());
                tokio::task::yield_now().await;
                assert!(client.is_suspended().await);
            }
            let subsequent = async {
                match later {
                    1 => client.suspend().await,
                    2 => {
                        client.logout().await.unwrap();
                    }
                    _ => {}
                }
            };
            tokio::pin!(subsequent);
            if later != 0 {
                assert!(futures_util::poll!(&mut subsequent).is_pending());
            }
            drop(retiring);
            subsequent.await;
            followup_drain_resume(&client).await;
            assert_eq!(client.is_suspended().await, later != 0);
            assert_eq!(client.is_signed_out(), later == 2);
            client.close().await;
            server.await.unwrap();
        }
    }

    #[derive(Default)]
    struct ForegroundBarrierStore {
        entered: tokio::sync::Notify,
        released: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl crate::SecureEnrollmentStore for ForegroundBarrierStore {
        /// Hold foreground observation at its protected read, then return no conditional namespace.
        async fn namespace_snapshot(
            &self,
        ) -> Result<Option<crate::EnrollmentNamespaceSnapshot>, crate::SecureStoreError> {
            self.entered.notify_one();
            self.released.notified().await;
            Ok(None)
        }
        /// Acknowledge clearing this metadata-only fixture, which retains no protected records.
        async fn clear(&self) -> Result<(), crate::SecureStoreError> {
            Ok(())
        }
        /// Accept unused writes without persisting credentials in this foreground-only fixture.
        async fn commit(
            &self,
            _: crate::SecureWrite,
            _: &str,
            _: &crate::SecretBlob,
        ) -> Result<(), crate::SecureStoreError> {
            Ok(())
        }
        /// Keep operation recovery absent so the fixture tests only foreground read ownership.
        async fn load_operation(
            &self,
            _: &str,
        ) -> Result<Option<crate::SecretBlob>, crate::SecureStoreError> {
            Ok(None)
        }
        /// Return no active key; the test binding supplies observation metadata only.
        async fn load_active(&self) -> Result<Option<crate::SecretBlob>, crate::SecureStoreError> {
            Ok(None)
        }
    }

    /// Release a held protected read after cancelling resume's waiter and observe its final state.
    #[tokio::test]
    async fn followup_cancelled_resume_finishes_foreground_entry() {
        use super::*;
        use std::time::Duration;
        let (client, _, server) =
            budget_fixture(false, Duration::from_secs(1), Duration::from_secs(1)).await;
        let store = Arc::new(ForegroundBarrierStore::default());
        client.retirement.bind(
            client.enrollment_trust().await,
            &followup_status(),
            store.clone(),
            false,
            true,
        );
        let caller = client.clone();
        let waiter = tokio::spawn(async move { caller.resume().await });
        tokio::time::timeout(Duration::from_secs(1), store.entered.notified())
            .await
            .unwrap();
        assert!(client.is_suspended().await);
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        store.released.notify_one();
        followup_drain_resume(&client).await;
        assert!(!client.is_suspended().await);
        assert!(!client.is_signed_out());
        client.close().await;
        server.await.unwrap();
    }

    /// Change principal behind sub_lock and require rejection before any manager or interest mutation.
    #[tokio::test]
    async fn followup_subscription_scope_rejection_has_no_tracking_effects() {
        use super::*;
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(false, Duration::from_secs(1), Duration::from_secs(1)).await;
        client.set_principal_user_id(1).await;
        let manager = client.live_mgr().await.unwrap();
        let lock = client.sub_lock.lock().await;
        let subscribe = client.subscribe(vec![tinkle_schema::proto::Target {
            kind: Some(tinkle_schema::proto::target::Kind::UserId(42)),
        }]);
        tokio::pin!(subscribe);
        assert!(futures_util::poll!(&mut subscribe).is_pending());
        client.set_principal_user_id(2).await;
        drop(lock);
        assert!(matches!(subscribe.await, Err(TinkleError::Sync(_))));
        assert!(manager.tracked_keys().await.is_empty());
        assert!(client.tracked_targets().await.is_empty());
        assert!(!client.interests.lock().await.subscribed);
        assert!(handler.0.lock().unwrap().is_empty());
        client.close().await;
        server.await.unwrap();
    }

    /// Recheck queued loss demand after a manual dial wins, avoiding a redundant replacement.
    #[tokio::test]
    async fn followup_queued_supervisor_does_not_replace_completed_manual_dial() {
        use std::time::Duration;
        let (client, _, server) =
            budget_fixture_connections(false, Duration::from_secs(1), Duration::from_secs(1), 2)
                .await;
        client.close().await;
        let connection = client.connect_lock.lock().await;
        let manual = client.reconnect();
        let automatic = client.reestablish();
        tokio::pin!(manual, automatic);
        assert!(futures_util::poll!(&mut manual).is_pending());
        assert!(futures_util::poll!(&mut automatic).is_pending());
        drop(connection);
        tokio::time::timeout(Duration::from_secs(2), async {
            let (result, ()) = tokio::join!(manual, automatic);
            result.unwrap();
        })
        .await
        .unwrap();
        assert!(client.is_connected().await);
        assert!(!client.sup_state.lock().await.reconnect_blocked);
        client.close().await;
        server.await.unwrap();
    }

    #[derive(Default)]
    struct FollowupCursorStore {
        inner: crate::InMemoryCursorStore,
        fail: std::sync::atomic::AtomicBool,
        clears: std::sync::Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl crate::CursorStore for FollowupCursorStore {
        /// Read the isolated in-memory journal used to verify per-principal clearing.
        async fn load_journal(
            &self,
            principal: &str,
            target: &str,
        ) -> Result<Option<i64>, crate::TinkleError> {
            self.inner.load_journal(principal, target).await
        }
        /// Seed independent principal checkpoints for logout isolation assertions.
        async fn save_journal(
            &self,
            principal: &str,
            target: &str,
            seq: i64,
        ) -> Result<(), crate::TinkleError> {
            self.inner.save_journal(principal, target, seq).await
        }
        /// Preserve ordinary target removal while this fixture injects failures only on clear.
        async fn remove_target(
            &self,
            principal: &str,
            target: &str,
        ) -> Result<(), crate::TinkleError> {
            self.inner.remove_target(principal, target).await
        }
        /// Record the requested principal and optionally fail before erasing its checkpoints.
        async fn clear_principal(&self, principal: &str) -> Result<(), crate::TinkleError> {
            self.clears.lock().unwrap().push(principal.to_string());
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(crate::TinkleError::CursorStore("clear failed".into()));
            }
            self.inner.clear_principal(principal).await
        }
    }

    /// Retry failed injected-store clearing under the original scope, preserving another account.
    #[tokio::test]
    async fn followup_logout_clears_injected_cursors_and_retries_original_principal() {
        use super::*;
        use std::time::Duration;
        for with_sqlite in [false, true] {
            let (mut client, _, server) =
                budget_fixture(false, Duration::from_secs(1), Duration::from_secs(1)).await;
            let store = Arc::new(FollowupCursorStore::default());
            client.store = store.clone();
            store
                .save_journal("user:1", "target:user:42", 9)
                .await
                .unwrap();
            store
                .save_journal("user:2", "target:user:42", 10)
                .await
                .unwrap();
            if with_sqlite {
                client.sqlite = Some(Arc::new(tinkle_storage::Storage::open_in_memory().unwrap()));
            }
            client.set_principal_user_id(1).await;
            store.fail.store(true, Ordering::SeqCst);
            assert!(matches!(
                client.logout().await,
                Err(TinkleError::CursorStore(_))
            ));
            assert_eq!(
                client.logout_cleanup.status(),
                crate::SessionCleanup::Failed
            );
            assert!(client.is_signed_out());
            assert_eq!(client.principal().await, UNKNOWN_PRINCIPAL);
            assert_eq!(
                store
                    .load_journal("user:1", "target:user:42")
                    .await
                    .unwrap(),
                Some(9)
            );
            store.fail.store(false, Ordering::SeqCst);
            assert!(!client.logout().await.unwrap());
            assert_eq!(
                client.logout_cleanup.status(),
                crate::SessionCleanup::Complete
            );
            assert_eq!(*store.clears.lock().unwrap(), ["user:1", "user:1"]);
            assert_eq!(
                store
                    .load_journal("user:1", "target:user:42")
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                store
                    .load_journal("user:2", "target:user:42")
                    .await
                    .unwrap(),
                Some(10)
            );
            client.close().await;
            server.await.unwrap();
        }
    }

    /// Clear exhausted manual retry counters on success without overriding terminal parking.
    #[tokio::test]
    async fn review_manual_reconnect_resets_only_nonterminal_backoff() {
        use std::time::Duration;
        for blocked in [false, true] {
            let (client, _, server) = budget_fixture_connections(
                false,
                Duration::from_secs(2),
                Duration::from_secs(2),
                2,
            )
            .await;
            {
                let mut sup = client.sup_state.lock().await;
                sup.reconnect_failures = 10;
                sup.next_reestablish = Some(std::time::Instant::now() + Duration::from_secs(60));
                sup.reconnect_blocked = blocked;
            }
            let old = client
                .live
                .read()
                .await
                .as_ref()
                .unwrap()
                .generation
                .clone();
            client.reconnect().await.unwrap();
            let sup = client.sup_state.lock().await;
            assert_eq!(sup.reconnect_blocked, blocked);
            assert_eq!(sup.reconnect_failures, if blocked { 10 } else { 0 });
            assert_eq!(sup.next_reestablish.is_some(), blocked);
            drop(sup);
            client.teardown_generation(Some(&old)).await;
            assert!(
                client.is_connected().await,
                "stale cleanup closed the replacement"
            );
            client.close().await;
            server.await.unwrap();
        }
    }

    /// Drive both attempts to the held subscription gate, then release A before B.
    #[tokio::test]
    async fn review_same_source_overlap_cannot_park_manual_replacement() {
        use super::*;
        use std::time::Duration;
        let (client, _, server) =
            budget_fixture_connections(false, Duration::from_secs(2), Duration::from_secs(2), 4)
                .await;
        client.set_principal_user_id(1).await;
        client.close().await;
        client.interests.lock().await.subscribed = true;
        let source_revision = client.retirement.revision();
        let subscription = client.sub_lock.lock().await;
        let automatic = client.reestablish();
        tokio::pin!(automatic);
        let generation_a = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                assert!(futures_util::poll!(&mut automatic).is_pending());
                if let Some(live) = client.live.read().await.as_ref() {
                    break live.generation.clone();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::task::yield_now().await;
        assert!(futures_util::poll!(&mut automatic).is_pending());

        let manual = client.reconnect();
        tokio::pin!(manual);
        let generation_b = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                assert!(futures_util::poll!(&mut manual).is_pending());
                if let Some(live) = client.live.read().await.as_ref() {
                    if !Arc::ptr_eq(&generation_a, &live.generation) {
                        break live.generation.clone();
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::task::yield_now().await;
        assert!(futures_util::poll!(&mut manual).is_pending());
        drop(subscription);
        let (_, result) = tokio::join!(automatic, manual);
        result.unwrap();
        assert_eq!(client.retirement.revision(), source_revision);
        {
            let sup = client.sup_state.lock().await;
            assert!(
                !sup.reconnect_blocked,
                "superseded attempt parked generation B"
            );
            assert_eq!(sup.reconnect_failures, 0);
            assert!(sup.next_reestablish.is_none());
        }
        assert!(client.is_connected().await);
        {
            let _connection = client.connect_lock.lock().await;
            client.teardown_connection().await;
        }
        client.reestablish().await;
        assert!(
            client.is_connected().await,
            "generation B loss must remain retryable"
        );
        assert!(!Arc::ptr_eq(
            &generation_b,
            &client.live.read().await.as_ref().unwrap().generation,
        ));
        client.close().await;
        server.await.unwrap();
    }

    /// First logout must clear credentials before waiting for a cancelled SQLite writer.
    #[tokio::test]
    async fn review_first_logout_invalidates_store_before_draining_cancelled_sqlite() {
        use super::*;
        use std::time::Duration;
        let (mut client, _, server) =
            budget_fixture(true, Duration::from_secs(2), Duration::from_secs(2)).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("review-first-logout.db");
        let storage = Arc::new(tinkle_storage::Storage::open(path.to_str().unwrap()).unwrap());
        client.sqlite = Some(storage.clone());
        client.set_principal_user_id(1).await;
        let store = Arc::new(BudgetStore::default());
        client
            .authorization
            .generation()
            .unwrap()
            .store(&client, store.clone(), None, None)
            .unwrap();
        let blocker = rusqlite::Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let chats = [tinkle_schema::proto::Chat {
            chat_id: 42,
            ..Default::default()
        }];
        {
            let write =
                client.ingest_snapshot_entities("user:1", client.reconcile_epoch(), &[], &chats);
            tokio::pin!(write);
            assert!(futures_util::poll!(&mut write).is_pending());
        }
        assert!(client.checkpoint_save.try_lock().is_err());
        let caller = client.clone();
        let first = tokio::spawn(async move { caller.logout().await });
        // No preseeded cleanup principal: exercise the initial transport-retirement path.
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .expect("first logout waited for SQLite before protected-store invalidation");
        assert_eq!(store.calls.load(Ordering::SeqCst), 1);
        assert!(client.is_signed_out());
        assert!(!first.is_finished());
        store.released.notify_one();
        first.abort();
        let _ = first.await;
        blocker.execute_batch("COMMIT").unwrap();
        // Setting the cache principal retired the unbound Returning fixture;
        // offline logout still owes the same ordered local invalidation.
        assert!(!client.logout().await.unwrap());
        assert_eq!(store.calls.load(Ordering::SeqCst), 1);
        assert!(storage.get_chat("user:1", 42).unwrap().is_none());
        assert_eq!(client.principal().await, UNKNOWN_PRINCIPAL);
        server.await.unwrap();
    }

    /// Refuse provisioning fallback after the parent selected a primary that later became unavailable.
    #[tokio::test]
    async fn review_provisioning_inherits_selected_primary_without_fallback() {
        use super::*;
        use std::time::Duration;
        let (mut parent, _, server) =
            budget_fixture(false, Duration::from_secs(2), Duration::from_secs(2)).await;
        let fallback = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        parent.config.fallback_port = Some(fallback.local_addr().unwrap().port());
        let trust = parent.enrollment_trust().await;
        parent.close().await;
        server.await.unwrap();
        let epoch = parent.authorization.generation().unwrap();
        assert!(matches!(
            parent.provisioning_client(&trust, &epoch).await,
            Err(TinkleError::NetworkUnavailable)
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(40), fallback.accept())
                .await
                .is_err()
        );
    }

    /// Withdraw inherited account interests on raw replacement and reject cross-deployment cache reuse.
    #[tokio::test]
    async fn review_raw_replacement_withdraws_unknown_account_and_rejects_cross_deployment_cache() {
        use super::*;
        use std::time::Duration;
        let (client, _, server) =
            budget_fixture(false, Duration::from_secs(2), Duration::from_secs(2)).await;
        client.set_principal_user_id(1).await;
        client.watch.lock().await.push(42);
        *client.interests.lock().await = Interests {
            targets: vec![tinkle_schema::proto::Target {
                kind: Some(tinkle_schema::proto::target::Kind::UserId(42)),
            }],
            subscribed: true,
        };
        let trust = client.enrollment_trust().await;
        client
            .replace_trust_source(TrustSource::Returning {
                trust: trust.clone(),
                enrollment: crate::Enrollment::new([9; 32], [8; 8], 2).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(client.principal().await, UNKNOWN_PRINCIPAL);
        assert!(client.watch_list().await.is_empty());
        assert!(client.tracked_targets().await.is_empty());
        assert!(!client.interests.lock().await.subscribed);
        let mut other = trust;
        other.scope = "different-deployment".into();
        assert!(matches!(
            client
                .replace_trust_source(TrustSource::Bootstrap(other))
                .await,
            Err(TinkleError::Configuration(_))
        ));
        client.close().await;
        server.await.unwrap();
    }

    /// Discard predecessor success/failure bookkeeping after a source replacement.
    #[tokio::test]
    async fn review_old_reconnect_result_cannot_charge_or_park_replacement() {
        use super::*;
        use std::time::Duration;
        let (client, _, server) =
            budget_fixture(false, Duration::from_secs(2), Duration::from_secs(2)).await;
        let old = ReconnectFence {
            source_revision: client.retirement.revision(),
            attempt: Arc::new(()),
            generation: None,
        };
        client.sup_state.lock().await.reconnect_attempt = Some(old.attempt.clone());
        let trust = client.enrollment_trust().await;
        client
            .replace_trust_source(TrustSource::Bootstrap(trust))
            .await
            .unwrap();
        client
            .apply_reconnect_result(&old, Err(TinkleError::TrustRejected))
            .await;
        {
            let mut sup = client.sup_state.lock().await;
            assert!(!sup.reconnect_blocked);
            assert_eq!(sup.reconnect_failures, 0);
            assert!(sup.next_reestablish.is_none());
            sup.reconnect_failures = 7;
            sup.next_reestablish = Some(std::time::Instant::now() + Duration::from_secs(60));
        }
        client.apply_reconnect_result(&old, Ok(())).await;
        assert_eq!(client.sup_state.lock().await.reconnect_failures, 7);
        // A failed manual attempt keeps the prior failure count/deadline unchanged.
        server.await.unwrap();
        assert!(client.reconnect().await.is_err());
        let sup = client.sup_state.lock().await;
        assert_eq!(sup.reconnect_failures, 7);
        assert!(sup.next_reestablish.is_some());
        assert!(!sup.reconnect_blocked);
        drop(sup);
        client.close().await;
    }

    /// Cancel a real SQLite writer while it waits behind another transaction.
    /// The blocking closure, not its dropped async waiter, must own the commit gate.
    #[tokio::test]
    async fn review_cancelled_snapshot_writers_drain_before_wipe_and_reject_stale_pages() {
        use super::*;
        for entities in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("review-cache.db");
            let storage = Arc::new(tinkle_storage::Storage::open(path.to_str().unwrap()).unwrap());
            let info = crate::ClientInfo::new(
                "device".into(),
                "test".into(),
                "1".into(),
                "1".into(),
                "1".into(),
                Default::default(),
            )
            .unwrap();
            let trust = crate::ServerTrust {
                server_public_key: [1; 32],
                server_key_id: 7,
                scope: "review".into(),
            };
            let client = Client::new_sqlite(
                ClientConfig::new("127.0.0.1", info),
                TrustSource::Bootstrap(trust),
                storage.clone(),
            );
            client.set_principal_user_id(1).await;
            let epoch = client.reconcile_epoch();
            let chats = vec![tinkle_schema::proto::Chat {
                chat_id: 42,
                ..Default::default()
            }];
            let dialogs = vec![tinkle_schema::proto::Dialog {
                target: Some(tinkle_schema::proto::Target {
                    kind: Some(tinkle_schema::proto::target::Kind::UserId(42)),
                }),
                ..Default::default()
            }];
            let blocker = rusqlite::Connection::open(&path).unwrap();
            blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
            {
                let write = async {
                    if entities {
                        client
                            .ingest_snapshot_entities("user:1", epoch, &[], &chats)
                            .await;
                    } else {
                        client
                            .ingest_snapshot_dialogs("user:1", epoch, &dialogs)
                            .await;
                    }
                };
                tokio::pin!(write);
                assert!(futures_util::poll!(&mut write).is_pending());
            }
            assert!(
                client.checkpoint_save.try_lock().is_err(),
                "cancelled SQLite task lost its gate"
            );
            client.stop_supervisor().await;
            let wipe = client.logout_wipe_and_reset("user:1");
            tokio::pin!(wipe);
            assert!(futures_util::poll!(&mut wipe).is_pending());
            blocker.execute_batch("COMMIT").unwrap();
            wipe.await.unwrap();
            // Old fetched data cannot recreate rows, even after the logout waiter completed.
            client
                .ingest_snapshot_entities("user:1", epoch, &[], &chats)
                .await;
            client
                .ingest_snapshot_dialogs("user:1", epoch, &dialogs)
                .await;
            assert!(storage.get_chat("user:1", 42).unwrap().is_none());
            assert!(storage.list_dialogs("user:1", 10).unwrap().is_empty());

            blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
            {
                let wipe = client.logout_wipe_and_reset("user:1");
                tokio::pin!(wipe);
                assert!(futures_util::poll!(&mut wipe).is_pending());
            }
            assert!(
                client.checkpoint_save.try_lock().is_err(),
                "cancelled wipe released exclusion before SQLite finished"
            );
            blocker.execute_batch("COMMIT").unwrap();
            let _drained = client.checkpoint_save.lock().await;
        }
    }

    /// Every dispatch branch hands off only the remainder after a contended lock.
    #[tokio::test]
    async fn call_budget_charges_wait_before_sdk_dispatch() {
        use std::time::Duration;
        for requested in [0, 600] {
            for (ready, method) in [
                (false, "/tinkle.v1.AuthService/SendCode"),
                (true, "/tinkle.v1.AuthService/SendCode"),
                (false, "/example.Echo/Echo"),
            ] {
                let (client, handler, server) =
                    budget_fixture(false, Duration::from_millis(600), Duration::from_secs(2)).await;
                let (_, initialized) = client.live_conn().await.unwrap();
                *initialized.lock().await = ready;
                let init_guard = initialized.lock().await;
                let live_guard = if method.starts_with("/tinkle.v1.") {
                    None
                } else {
                    Some(client.live.write().await)
                };
                let call = client.call_raw(method, Vec::new(), requested);
                tokio::pin!(call);
                assert!(futures_util::poll!(&mut call).is_pending());
                tokio::time::sleep(Duration::from_millis(250)).await;
                drop(live_guard);
                drop(init_guard);
                call.await.unwrap();
                let remaining = handler.0.lock().unwrap()[0].1;
                assert!(
                    remaining < Duration::from_millis(450),
                    "SDK received renewed budget: {remaining:?}"
                );
                assert!(remaining > Duration::ZERO);
                client.close().await;
                server.await.unwrap();
            }
        }
    }

    /// The configured maximum bounds the outer wait for explicit and default budgets.
    #[tokio::test]
    async fn call_budget_maximum_includes_initialization_wait() {
        use std::time::Duration;
        for requested in [0, 1_000] {
            let (client, handler, server) =
                budget_fixture(false, Duration::from_secs(1), Duration::from_millis(40)).await;
            let (_, initialized) = client.live_conn().await.unwrap();
            let guard = initialized.lock().await;
            let call = client.call_raw("/tinkle.v1.AuthService/SendCode", Vec::new(), requested);
            tokio::pin!(call);
            assert!(futures_util::poll!(&mut call).is_pending());
            tokio::time::sleep(Duration::from_millis(80)).await;
            assert!(
                matches!(futures_util::poll!(&mut call), std::task::Poll::Ready(Err(error)) if error.code() == syntra_core::ErrorCode::DeadlineExceeded),
                "configured maximum did not expire queued call"
            );
            drop(guard);
            assert!(handler.0.lock().unwrap().is_empty());
            client.close().await;
            server.await.unwrap();
        }
    }

    /// Fractional milliseconds must expire locally instead of becoming SDK default zero.
    #[tokio::test]
    async fn call_budget_submillisecond_never_dispatches() {
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(false, Duration::from_micros(900), Duration::from_secs(1)).await;
        let error = client
            .call_raw("/example.Echo/Echo", Vec::new(), 0)
            .await
            .unwrap_err();
        assert_eq!(error.code(), syntra_core::ErrorCode::DeadlineExceeded);
        assert!(
            handler.0.lock().unwrap().is_empty(),
            "sub-ms budget was handed to SDK as default zero"
        );
        client.close().await;
        server.await.unwrap();
    }

    /// Handoff floors fractional milliseconds and rejects expiry or clock loss.
    #[tokio::test(start_paused = true)]
    async fn call_budget_floor_and_clock_loss_never_use_default_zero() {
        use super::Client;
        use std::time::Duration;
        let clock = syntra_runtime::elapsed::ElapsedClock::lifecycle_driven();
        let budget = clock.deadline_after(Duration::from_micros(2_750)).unwrap();
        assert_eq!(Client::remaining_timeout_ms(&budget).unwrap(), 2);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(Client::remaining_timeout_ms(&budget).unwrap(), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(
            Client::remaining_timeout_ms(&budget).unwrap_err().code(),
            syntra_core::ErrorCode::DeadlineExceeded
        );
        tokio::time::advance(Duration::from_micros(750)).await;
        assert_eq!(
            Client::remaining_timeout_ms(&budget).unwrap_err().code(),
            syntra_core::ErrorCode::DeadlineExceeded
        );
        let budget = clock.deadline_after(Duration::from_secs(1)).unwrap();
        clock.invalidate();
        assert_eq!(
            Client::remaining_timeout_ms(&budget).unwrap_err().code(),
            syntra_core::ErrorCode::DeadlineExceeded
        );
    }

    /// Raw Logout obeys the caller's requested budget rather than the high-level cap.
    #[tokio::test]
    async fn call_budget_raw_logout_keeps_requested_timeout() {
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(true, Duration::from_secs(30), Duration::from_secs(120)).await;
        client
            .call_raw(
                tinkle_schema::methods::accountservice::LOGOUT,
                Vec::new(),
                4_000,
            )
            .await
            .unwrap();
        assert!(handler.0.lock().unwrap()[0].1 > Duration::from_secs(3));
        assert!(!client.is_signed_out());
        client.close().await;
        server.await.unwrap();
    }

    /// Protected clearing can fail or wait independently of the remote budget.
    #[derive(Default)]
    struct BudgetStore {
        fail: std::sync::atomic::AtomicBool,
        calls: std::sync::atomic::AtomicUsize,
        entered: tokio::sync::Notify,
        released: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl crate::SecureEnrollmentStore for BudgetStore {
        /// Block clear acknowledgement until released, optionally returning a sanitized store failure.
        async fn clear(&self) -> Result<(), crate::SecureStoreError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.entered.notify_one();
            self.released.notified().await;
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                Err(crate::SecureStoreError)
            } else {
                Ok(())
            }
        }
        /// Accept unused writes without adding persistence delays to logout budget tests.
        async fn commit(
            &self,
            _: crate::SecureWrite,
            _: &str,
            _: &crate::SecretBlob,
        ) -> Result<(), crate::SecureStoreError> {
            Ok(())
        }
        /// Expose no recoverable operation because these tests bind the store directly.
        async fn load_operation(
            &self,
            _: &str,
        ) -> Result<Option<crate::SecretBlob>, crate::SecureStoreError> {
            Ok(None)
        }
        /// Expose no active record; Returning material comes from the encrypted fixture setup.
        async fn load_active(&self) -> Result<Option<crate::SecretBlob>, crate::SecureStoreError> {
            Ok(None)
        }
    }

    /// A partial lifecycle wait shrinks the original, configuration-clamped logout cap.
    #[tokio::test]
    async fn logout_budget_charges_lifecycle_wait_and_configured_maximum() {
        use super::*;
        use std::time::Duration;
        for max_ms in [120_000, 600] {
            for block_initialization in [false, true] {
                let (client, handler, server) =
                    budget_fixture(true, Duration::from_secs(30), Duration::from_millis(max_ms))
                        .await;
                let store = Arc::new(BudgetStore::default());
                client
                    .authorization
                    .generation()
                    .unwrap()
                    .store(&client, store.clone(), None, None)
                    .unwrap();
                let (_, initialized) = client.live_conn().await.unwrap();
                let connection_guard = if block_initialization {
                    None
                } else {
                    Some(client.connect_lock.lock().await)
                };
                let init_guard = if block_initialization {
                    Some(initialized.lock().await)
                } else {
                    None
                };
                let exiting = client.clone();
                let waiter = tokio::spawn(async move { exiting.logout().await });
                while !client.is_signed_out() {
                    tokio::task::yield_now().await;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
                drop(connection_guard);
                drop(init_guard);
                store.entered.notified().await;
                store.released.notify_one();
                assert!(waiter.await.unwrap().unwrap());
                let remaining = handler.0.lock().unwrap()[0].1;
                assert!(
                    remaining < Duration::from_millis(max_ms.min(3_000) - 150),
                    "logout renewed its budget: {remaining:?}"
                );
                server.await.unwrap();
            }
        }
    }

    /// Lifecycle time consumes the first logout budget; local retries cannot renew it.
    #[tokio::test]
    async fn logout_budget_exhausted_by_lifecycle_still_clears_without_replay() {
        use super::*;
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(true, Duration::from_secs(30), Duration::from_secs(120)).await;
        let store = Arc::new(BudgetStore::default());
        client
            .authorization
            .generation()
            .unwrap()
            .store(&client, store.clone(), None, None)
            .unwrap();
        store.fail.store(true, Ordering::SeqCst);
        let guard = client.connect_lock.lock().await;
        let exiting = client.clone();
        let waiter = tokio::spawn(async move { exiting.logout().await });
        while !client.is_signed_out() {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(3_100)).await;
        let joining = client.clone();
        let joiner = tokio::spawn(async move { joining.logout().await });
        tokio::task::yield_now().await;
        drop(guard);
        store.entered.notified().await;
        assert!(
            !waiter.is_finished(),
            "logout must await durable clear acknowledgement"
        );
        store.released.notify_one();
        assert!(matches!(
            waiter.await.unwrap(),
            Err(TinkleError::SecureStore)
        ));
        assert!(matches!(
            joiner.await.unwrap(),
            Err(TinkleError::SecureStore)
        ));
        store.fail.store(false, Ordering::SeqCst);
        store.released.notify_one();
        assert!(
            !client.logout().await.unwrap(),
            "expired remote budget cannot confirm revocation"
        );
        assert_eq!(store.calls.load(Ordering::SeqCst), 2);
        assert!(
            handler.0.lock().unwrap().is_empty(),
            "expired logout was dispatched or replayed"
        );
        server.await.unwrap();
    }

    /// Remote expiry and a dropped waiter cannot abandon mandatory local clearing.
    #[tokio::test]
    async fn logout_budget_dropped_waiter_clear_outlives_remote_cutoff() {
        use super::*;
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(true, Duration::from_secs(30), Duration::from_secs(120)).await;
        let store = Arc::new(BudgetStore::default());
        client
            .authorization
            .generation()
            .unwrap()
            .store(&client, store.clone(), None, None)
            .unwrap();
        let exiting = client.clone();
        let waiter = tokio::spawn(async move { exiting.logout().await });
        store.entered.notified().await;
        waiter.abort();
        let _ = waiter.await;
        tokio::time::sleep(Duration::from_millis(3_100)).await;
        assert_eq!(
            client.logout_cleanup.status(),
            crate::SessionCleanup::Pending
        );
        store.released.notify_one();
        assert!(client.logout().await.unwrap());
        assert_eq!(store.calls.load(Ordering::SeqCst), 1);
        assert_eq!(handler.0.lock().unwrap().len(), 1);
        server.await.unwrap();
    }

    /// A signed-out wrapped response cannot wait for optional watch restoration.
    #[tokio::test]
    async fn logout_budget_confirmation_does_not_wait_for_watch_lock() {
        use super::*;
        use std::time::Duration;
        let (client, handler, server) =
            budget_fixture(true, Duration::from_secs(30), Duration::from_secs(120)).await;
        let store = Arc::new(BudgetStore::default());
        client
            .authorization
            .generation()
            .unwrap()
            .store(&client, store.clone(), None, None)
            .unwrap();
        let watch = client.watch.lock().await;
        let exiting = client.clone();
        let waiter = tokio::spawn(async move { exiting.logout().await });
        // Keep the optional lock beyond the remote cutoff; mandatory reset waits too.
        tokio::time::sleep(Duration::from_millis(3_100)).await;
        assert!(!waiter.is_finished());
        drop(watch);
        store.entered.notified().await;
        store.released.notify_one();
        assert!(
            waiter.await.unwrap().unwrap(),
            "successful wrapped Logout lost confirmation on optional watch lock"
        );
        assert_eq!(handler.0.lock().unwrap().len(), 1);
        server.await.unwrap();
    }

    /// A queued old provisioner cannot use or reject an explicitly recovered configuration.
    #[tokio::test]
    async fn provisioning_context_is_fenced_by_explicit_recovery() {
        use super::*;
        let info = crate::ClientInfo::new(
            "device".into(),
            "test".into(),
            "1".into(),
            "1".into(),
            "1".into(),
            Default::default(),
        )
        .unwrap();
        let trust = crate::ServerTrust {
            server_public_key: [1; 32],
            server_key_id: 7,
            scope: "test".into(),
        };
        let config = ClientConfig::new("127.0.0.1", info);
        let parent = Client::new(
            config.clone(),
            TrustSource::Bootstrap(trust.clone()),
            crate::InMemoryCursorStore::new(),
        );
        let mut child = Client::new(
            config,
            TrustSource::Bootstrap(trust),
            crate::InMemoryCursorStore::new(),
        );
        child.endpoint_rejected = parent.endpoint_rejected.clone();
        child.connect_lock = parent.connect_lock.clone();
        child.provisioning_context = Some((
            parent.retirement.clone(),
            parent.retirement.endpoint_revision(),
        ));
        assert!(child.check_provisioning_context().is_ok());
        parent.retirement.reset_endpoint(&parent.endpoint_rejected);
        assert!(matches!(
            child.connect().await,
            Err(TinkleError::Configuration(_))
        ));
        assert!(matches!(
            child.check_call_admission(tinkle_schema::methods::authservice::RESUME_ENROLLMENT),
            Err(TinkleError::Configuration(_))
        ));
        assert!(!parent.endpoint_rejected.load(Ordering::SeqCst));
    }

    /// Check the initial target delay, exponential growth, cap, and immediately due fresh demand.
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

    /// Preserve current resume fields while accepting legacy rows and discarding orphaned metadata.
    #[test]
    fn resume_state_codec_round_trips() {
        use super::{CatchupResume, Client};
        // Full v3 row: edge + top + open covering chain.
        let full = CatchupResume {
            edge: Some(151),
            cover_top: Some(250),
            covering: Some(301),
            covering_top: Some(1300),
        };
        assert_eq!(
            Client::decode_resume_state(&Client::encode_resume_state(full)),
            full
        );
        // v2 row (edge + top, no covering) still decodes; the covering
        // top is dropped without its cursor.
        let v2 = Client::decode_resume_state(b"resume_offset=151;cover_top=250");
        assert_eq!(v2.edge, Some(151));
        assert_eq!(v2.cover_top, Some(250));
        assert_eq!(v2.covering, None);
        assert_eq!(v2.covering_top, None);
        let orphan_top = Client::decode_resume_state(b"resume_offset=151;covering_top=1300");
        assert_eq!(orphan_top.covering_top, None);
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
        // A covering cursor without an edge is meaningless.
        assert_eq!(
            Client::decode_resume_state(b"covering=301"),
            CatchupResume::default()
        );
    }
}
