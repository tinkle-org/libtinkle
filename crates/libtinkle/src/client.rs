//! High-level typed client: `connect → call → sync → reconnect`.
//!
//! Conceptual use:
//! ```ignore
//! client.connect().await?;
//! client.subscribe(vec![], vec![]).await?;
//! while let Some(evt) = client.next_event().await {
//!     if let SyncEvent::UpdateReady { stream, cursor, update } = evt {
//!         // apply update + required refreshes, then:
//!         client.acknowledge(&stream, cursor).await?;
//!     }
//! }
//! ```
//!
//! The SDK serializes the public request protobuf, constructs the Syntra
//! `RpcRequest`, allocates a nonzero `request_id`, sends through the
//! active connection, awaits the matching `RpcResult`, decodes the typed
//! response, and returns typed errors. Catch-up uses actual gRPC
//! `ReadUpdates`; ongoing delivery uses Syntra `FollowUpdates` /
//! `ObserveConnectivity` notifications. Never auto-retries unsafe
//! mutations without an application idempotency policy.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, RwLock};
use zeroize::Zeroizing;

use syntra_runtime::{CancelToken, Initiator};

use crate::config::{ClientConfig, Credential};
use crate::cursor::CursorStore;
use crate::errors::{RecoveryDecision, TinkleError};
use crate::reconnect::{backoff_sleep, should_retry};
use crate::services::{Auth, Messages, Updates};
use crate::sync::{Interests, RecoverySignal, SyncEvent, SyncManager, UNKNOWN_PRINCIPAL};
use crate::ErrorCode;
use tinkle_schema::sync::SyncError;

struct Live {
    handle: syntra_ws::WsHandle,
    connection_id: String,
    version: u32,
    sync_mgr: Arc<SyncManager>,
    pump: tokio::task::JoinHandle<()>,
}

/// Supervisor state: exactly one monitor task per logical client.
/// `epoch` guards handle installation against a concurrent `stop()`;
/// `last_result` reflects the initial `start()` establishment.
#[derive(Default)]
struct SupState {
    handle: Option<tokio::task::JoinHandle<()>>,
    shutdown: Option<oneshot::Sender<()>>,
    epoch: u64,
    last_result: Option<SubscribeResult>,
    last_reestablish: Option<std::time::Instant>,
}

/// Result of establishing sync interests.
#[derive(Debug, Clone)]
pub struct SubscribeResult {
    pub stream: String,
    pub after_cursor: u64,
    pub head: Option<u64>,
}

/// Typed Tinkle client over Syntra + gRPC catch-up.
///
/// Cheap to clone (shares connection + cursors). The same logical runtime
/// is reusable across iOS/Android/WASM via thin platform adapters.
#[derive(Clone)]
pub struct Client {
    config: ClientConfig,
    credential: Credential,
    store: Arc<dyn CursorStore>,
    #[cfg(not(target_arch = "wasm32"))]
    sqlite: Option<Arc<tinkle_storage::Storage>>,
    live: Arc<RwLock<Option<Live>>>,
    principal: Arc<RwLock<String>>,
    interests: Arc<tokio::sync::Mutex<Interests>>,
    /// Follows established on the current connection, by resolved stream.
    /// Connection-scoped: cleared whenever the socket dies (`close`), so a
    /// new connection always re-establishes. Insert-before-request gives
    /// at-most-once issuance per connection; the marker is removed if the
    /// RPC fails so retries re-issue.
    follows: Arc<tokio::sync::Mutex<HashSet<String>>>,
    /// Serializes `subscribe` bodies: concurrent subscribes must not issue
    /// duplicate follows or interleave hydration.
    sub_lock: Arc<tokio::sync::Mutex<()>>,
    /// Recovery-signal sender installed by the supervisor. `connect()`
    /// wires a clone into each new `SyncManager` and pump, so every
    /// re-established connection reports to the same supervisor.
    signal_tx: Arc<tokio::sync::Mutex<Option<mpsc::Sender<RecoverySignal>>>>,
    /// Transport liveness as observed by the notification pump. Set on
    /// connect, cleared when the pump exits. `is_connected` reflects this,
    /// not just the presence of a `Live` handle.
    transport_ok: Arc<AtomicBool>,
    /// Supervisor lifecycle (handle, shutdown, epoch, last result).
    sup_state: Arc<tokio::sync::Mutex<SupState>>,
    /// Serializes `start()` so concurrent callers share one supervisor.
    start_lock: Arc<tokio::sync::Mutex<()>>,
}

impl Client {
    pub fn new(config: ClientConfig, credential: Credential, store: Arc<dyn CursorStore>) -> Self {
        Self {
            config,
            credential,
            store,
            #[cfg(not(target_arch = "wasm32"))]
            sqlite: None,
            live: Arc::new(RwLock::new(None)),
            principal: Arc::new(RwLock::new(UNKNOWN_PRINCIPAL.to_string())),
            interests: Arc::new(tokio::sync::Mutex::new(Interests::default())),
            follows: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            sub_lock: Arc::new(tokio::sync::Mutex::new(())),
            signal_tx: Arc::new(tokio::sync::Mutex::new(None)),
            transport_ok: Arc::new(AtomicBool::new(false)),
            sup_state: Arc::new(tokio::sync::Mutex::new(SupState::default())),
            start_lock: Arc::new(tokio::sync::Mutex::new(())),
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
            credential,
            store,
            sqlite: Some(storage),
            live: Arc::new(RwLock::new(None)),
            principal: Arc::new(RwLock::new(UNKNOWN_PRINCIPAL.to_string())),
            interests: Arc::new(tokio::sync::Mutex::new(Interests::default())),
            follows: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            sub_lock: Arc::new(tokio::sync::Mutex::new(())),
            signal_tx: Arc::new(tokio::sync::Mutex::new(None)),
            transport_ok: Arc::new(AtomicBool::new(false)),
            sup_state: Arc::new(tokio::sync::Mutex::new(SupState::default())),
            start_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn sqlite_storage(&self) -> Option<Arc<tinkle_storage::Storage>> {
        self.sqlite.clone()
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// True only when a live connection exists AND its notification
    /// pump has not exited. A dead pump with a stale handle reports false.
    pub async fn is_connected(&self) -> bool {
        self.live.read().await.is_some() && self.transport_ok.load(Ordering::SeqCst)
    }

    pub async fn principal(&self) -> String {
        self.principal.read().await.clone()
    }

    /// Set the authenticated principal from a known user id.
    /// Called automatically after `sign_in`; apps with an existing
    /// session token should call it after resolving their own id.
    ///
    /// Changing the principal resets live sync state (cursors, pending,
    /// delivered) via `SyncManager::set_principal`, so resubscribe
    /// afterwards to re-establish follows from persisted cursors.
    /// Setting the same principal again is a no-op.
    pub async fn set_principal_user_id(&self, user_id: i64) {
        if user_id <= 0 {
            return;
        }
        let p = format!("user:{user_id}");
        *self.principal.write().await = p.clone();
        if let Ok(mgr) = self.live_mgr().await {
            mgr.set_principal(p).await;
        }
    }

    /// Foreground connect: WSS Upgrade → Syntra handshake → ACTIVE.
    /// No Syntra byte is sent before TLS + 101 (WSS §1F).
    pub async fn connect(&self) -> Result<(), TinkleError> {
        if self.is_connected().await {
            return Ok(());
        }
        // Stale handle without a live pump: tear down before redialing so
        // a dead transport is never mistaken for a connection.
        if self.live.read().await.is_some() {
            self.teardown_connection().await;
        }
        let ws_cfg = self.config.ws_config();
        let credential_bytes = self.credential.expose_secret().to_vec();
        let handle = syntra_ws::connect(ws_cfg, credential_bytes, self.config.connection.clone())
            .await
            .map_err(|e| {
                TinkleError::Syntra(syntra_core::error::SyntraError::Transport(e.to_string()))
            })?;

        let conn = handle.connection().clone();
        let connection_id = handle.connection_id.clone();
        let version = handle.version;

        let principal = self.principal.read().await.clone();
        let sync_mgr = SyncManager::new(principal, self.store.clone());
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(sqlite) = self.sqlite.clone() {
            sync_mgr.set_sqlite_storage(sqlite).await;
        }
        // Supervisor reporting, when supervised: every re-established
        // connection reports to the same supervisor.
        if let Some(tx) = self.signal_tx.lock().await.clone() {
            sync_mgr.set_signal_tx(tx).await;
        }

        // Notification pump: Initiator → validate → cursors → app events.
        // Interleaved notifications never complete an RPC (core routes them
        // here, not to pending waiters). Natural exit means the transport
        // died: mark it and hint the supervisor (the watchdog covers a
        // missed hint).
        let pump_conn = conn.clone();
        let pump_mgr = sync_mgr.clone();
        let pump_ok = self.transport_ok.clone();
        let pump = tokio::spawn(async move {
            while let Some(payload) = pump_conn.next_notification().await {
                if let Err(e) = pump_mgr.handle_payload(&payload).await {
                    tracing::debug!(error = %e, "sync notification handling reported gap/rebuild");
                }
            }
            pump_ok.store(false, Ordering::SeqCst);
            pump_mgr.emit_signal(RecoverySignal::PumpDead).await;
        });

        *self.live.write().await = Some(Live {
            handle,
            connection_id,
            version,
            sync_mgr,
            pump,
        });
        self.transport_ok.store(true, Ordering::SeqCst);
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

    /// Raw RPC: opaque bytes in/out.
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

    /// Cancellable RPC.
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

    /// Typed service facades (method bindings from BSR descriptors).
    pub fn messages(&self) -> Messages<'_> {
        Messages::new(self)
    }

    pub fn auth(&self) -> Auth<'_> {
        Auth::new(self)
    }

    pub fn updates(&self) -> Updates<'_> {
        Updates::new(self)
    }

    /// gRPC catch-up for one stream. Requires `grpc_endpoint` in config.
    pub(crate) async fn read_updates_grpc(
        &self,
        req: tinkle_schema::proto::ReadUpdatesRequest,
    ) -> Result<tinkle_schema::proto::ReadUpdatesResponse, TinkleError> {
        let endpoint = self.validated_grpc_endpoint()?;
        // Zeroized on drop; never logged, never placed in URLs.
        let token: Zeroizing<Vec<u8>> = Zeroizing::new(self.credential.expose_secret().to_vec());
        crate::grpc::read_updates_grpc(&endpoint, token.as_slice(), req, 30_000).await
    }

    /// Fetch the configured gRPC endpoint, failing fast with
    /// `GrpcConfig` (FailedPrecondition) on missing/blank/scheme/https
    /// violations instead of a retried `Grpc` (Unavailable) at page time.
    fn validated_grpc_endpoint(&self) -> Result<String, TinkleError> {
        let endpoint = self.config.grpc_endpoint.clone().ok_or_else(|| {
            TinkleError::GrpcConfig("grpc_endpoint not configured".into())
        })?;
        ClientConfig::validate_grpc_endpoint(&endpoint, self.config.allow_insecure)?;
        Ok(endpoint)
    }

    /// Catch up one stream over gRPC with paginated recovery.
    ///
    /// Pages `ReadUpdates` from `after`, validates floor/head, feeds each
    /// update through the shared pipeline (buffered replay on gaps), and
    /// retries transient failures.
    pub async fn catch_up(
        &self,
        stream: &str,
        after: u64,
        limit: i32,
    ) -> Result<tinkle_schema::proto::ReadUpdatesResponse, TinkleError> {
        self.recover_stream(stream, after, limit).await
    }

    async fn recover_stream(
        &self,
        stream: &str,
        mut after: u64,
        limit: i32,
    ) -> Result<tinkle_schema::proto::ReadUpdatesResponse, TinkleError> {
        let mgr = self.live_mgr().await?;
        let limit = limit.clamp(1, 500);
        let _ = mgr.hydrate_from_store(&[stream.to_string()]).await;
        if !mgr.begin_recovery(stream).await {
            return Err(TinkleError::Sync(format!(
                "concurrent recovery already in progress on {stream}"
            )));
        }
        let mut last_resp: Option<tinkle_schema::proto::ReadUpdatesResponse> = None;
        let mut pages = 0u32;
        let mut no_progress_pages = 0u32;
        let result: Result<tinkle_schema::proto::ReadUpdatesResponse, TinkleError> = async {
            let endpoint = self.validated_grpc_endpoint()?;
            // Zeroized on drop; never logged, never placed in URLs.
            let token: Zeroizing<Vec<u8>> =
                Zeroizing::new(self.credential.expose_secret().to_vec());
            // Fail fast on malformed credentials before opening the
            // connection, not after the 10s connect timeout.
            let _ = crate::grpc::bearer(token.as_slice())?;
            // One TCP/TLS/H2 connection per recovery run, shared by all
            // pages — not one handshake per page.
            #[cfg(not(target_arch = "wasm32"))]
            let channel = crate::grpc::connect_channel(&endpoint, 30_000).await?;
            loop {
                if pages >= 20 {
                    break;
                }
                pages += 1;
                let req = tinkle_schema::proto::ReadUpdatesRequest {
                    stream: stream.to_string(),
                    after_cursor: after,
                    limit,
                };
                let mut attempt = 0u32;
                    let resp = loop {
                        #[cfg(not(target_arch = "wasm32"))]
                        let page = crate::grpc::read_updates_on_channel(
                            &channel,
                            token.as_slice(),
                            req.clone(),
                        )
                        .await;
                        #[cfg(target_arch = "wasm32")]
                        let page = self.read_updates_grpc(req.clone()).await;
                        match page {
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
                                RecoveryDecision::Rebuild => {
                                    // Expired/unbridgeable history: stop
                                    // paging and enter the rebuild path
                                    // instead of retrying a window that can
                                    // never succeed.
                                    mgr.handle_expired(stream, None).await;
                                    return Err(TinkleError::SyncError(
                                        SyncError::RebuildRequired {
                                            stream: stream.to_string(),
                                            head: None,
                                        },
                                    ));
                                }
                                _ => return Err(e),
                            },
                        }
                    };
                mgr.record_bounds(stream, resp.replay_floor, Some(resp.head))
                    .await;
                if after < resp.replay_floor {
                    mgr.handle_expired(stream, Some(resp.head)).await;
                    return Err(TinkleError::SyncError(SyncError::RebuildRequired {
                        stream: stream.to_string(),
                        head: Some(resp.head),
                    }));
                }
                if after > resp.head {
                    // Our cursor is beyond everything retained: history past
                    // head is unrecoverable. Never re-anchor to the head
                    // silently; enter the rebuild path so the stream
                    // reconstructs from an authoritative boundary.
                    mgr.handle_expired(stream, Some(resp.head)).await;
                    return Err(TinkleError::SyncError(SyncError::RebuildRequired {
                        stream: stream.to_string(),
                        head: Some(resp.head),
                    }));
                }
                if resp.updates.is_empty() {
                    last_resp = Some(resp);
                    break;
                }
                let mut progressed = false;
                for u in &resp.updates {
                    if u.stream != stream {
                        return Err(TinkleError::Sync(format!(
                            "stream mismatch in page: got {} want {stream}",
                            u.stream
                        )));
                    }
                    if u.cursor <= after {
                        continue;
                    }
                    match mgr.handle_update(u.clone()).await {
                        Ok(()) => {
                            if u.cursor > after {
                                after = u.cursor;
                                progressed = true;
                            }
                        }
                        Err(e) => {
                            if matches!(e, TinkleError::SyncError(SyncError::Gap { .. })) {
                                break;
                            }
                            return Err(e);
                        }
                    }
                }
                last_resp = Some(resp.clone());
                if !progressed {
                    no_progress_pages += 1;
                    if no_progress_pages >= 3 {
                        break;
                    }
                } else {
                    no_progress_pages = 0;
                }
                if (resp.updates.len() as i32) < limit || after >= resp.head {
                    break;
                }
            }
            match last_resp {
                None => Err(TinkleError::Grpc("empty recovery: no pages".into())),
                Some(resp) => {
                    if after < resp.head {
                        Err(TinkleError::Sync(format!(
                            "incomplete recovery on {stream}: after {after} < head {} after {pages} pages",
                            resp.head
                        )))
                    } else {
                        Ok(resp)
                    }
                }
            }
        }
        .await;
        match &result {
            Ok(_) => mgr.end_recovery(stream, true).await,
            Err(_) => mgr.end_recovery(stream, false).await,
        }
        result
    }

    /// Subscribe with durable cursor recovery.
    ///
    /// Flow: resolve principal (probe-as-follow: exactly one own-stream
    /// follow per connection) → hydrate → gRPC catch-up for own + rooms →
    /// start Syntra follows (skipping already-established) + connectivity
    /// observations. Returns the own-stream confirmation; replay itself
    /// arrives as `UpdateReady` events requiring explicit `acknowledge`.
    ///
    /// Partial room failures do not abort established follows. Rooms that
    /// fail (after the scoped-continuation fallback) are reported in the
    /// returned error while interests are still persisted, so a retry or
    /// reconnect reconciles without issuing duplicate follows.
    pub async fn subscribe(
        &self,
        rooms: Vec<(i64, u64)>,
        watch_user_ids: Vec<i64>,
    ) -> Result<SubscribeResult, TinkleError> {
        let _sub = self.sub_lock.lock().await;
        let mgr = self.live_mgr().await?;
        // Exactly-once own-stream follow per connection: when the principal
        // is unknown the probe doubles as the follow (see
        // `ProbeFollowResolver`); otherwise the explicit follow below is the
        // only one.
        ProbeFollowResolver.resolve(self).await?;
        let principal = self.principal().await;
        let own_stream = if principal.starts_with("user:") {
            principal.clone()
        } else {
            format!("user:{}", principal_user_suffix(&principal))
        };
        let mut streams = vec![own_stream.clone()];
        for (room_id, _) in &rooms {
            streams.push(format!("room:{room_id}"));
        }
        let _ = mgr.hydrate_from_store(&streams).await;

        let own_cursor = self.store.load(&principal, &own_stream).await?.unwrap_or(0);
        let mut effective_rooms = Vec::with_capacity(rooms.len());
        for (room_id, after) in &rooms {
            let stream = format!("room:{room_id}");
            let stored = self.store.load(&principal, &stream).await?.unwrap_or(0);
            effective_rooms.push((*room_id, (*after).max(stored)));
        }

        // Best-effort gRPC catch-up for own stream when configured.
        // Failures do not block follow establishment; gaps surface as events.
        let mut head: Option<u64> = None;
        if self.config.grpc_endpoint.is_some() {
            match self.catch_up(&own_stream, own_cursor, 100).await {
                Ok(resp) => {
                    head = Some(resp.head);
                }
                Err(e) => {
                    tracing::debug!(error = %e, "own-stream catch-up failed, continuing with follow");
                }
            }
            for (room_id, after) in &effective_rooms {
                let stream = format!("room:{room_id}");
                let _ = self.catch_up(&stream, *after, 100).await;
            }
        }

        // Start follows for own + rooms. Each follow resumes from the
        // post-catch-up cursor, covering commits between the gRPC page and
        // subscription establishment without replaying already-delivered
        // updates as duplicates.
        let own_follow_cursor = own_cursor.max(mgr.effective_cursor(&own_stream).await);
        match self.follow_once(&own_stream, own_follow_cursor).await? {
            Some(own_resp) => {
                if !own_resp.stream.is_empty() && own_resp.stream != own_stream {
                    // Principal rotation: gateway resolved a different own stream.
                    self.follows.lock().await.remove(&own_stream);
                    self.follows.lock().await.insert(own_resp.stream.clone());
                    *self.principal.write().await = own_resp.stream.clone();
                    mgr.set_principal(own_resp.stream.clone()).await;
                }
                head = head.or(own_resp.head);
            }
            None => {
                // Probe-as-follow already established the own stream on this
                // connection; re-requesting would be rejected as a duplicate.
            }
        }
        let mut failed_rooms: Vec<(i64, String)> = Vec::new();
        for (room_id, after) in &effective_rooms {
            let stream = format!("room:{room_id}");
            let follow_cursor = (*after).max(mgr.effective_cursor(&stream).await);
            match self.follow_once(&stream, follow_cursor).await {
                Ok(_) => {}
                Err(e) if e.code() == ErrorCode::FailedPrecondition && follow_cursor != 0 => {
                    // Scoped-continuation rejection: retry once from the
                    // origin. The stored checkpoint is untouched; the dedup
                    // pipeline makes the replay idempotent. If the retry is
                    // rejected too, the room stays degraded.
                    tracing::debug!(%stream, error = %e, "room resume rejected, retrying from origin");
                    if let Err(e2) = self.follow_once(&stream, 0).await {
                        failed_rooms.push((*room_id, e2.to_string()));
                    }
                }
                Err(e) => failed_rooms.push((*room_id, e.to_string())),
            }
        }
        if !watch_user_ids.is_empty() {
            let req = tinkle_schema::proto::ObserveConnectivityRequest {
                user_ids: watch_user_ids.clone(),
            };
            self.updates().observe_connectivity(req).await?;
        }

        mgr.on_subscribed(effective_rooms.clone(), watch_user_ids.clone())
            .await;
        *self.interests.lock().await = Interests {
            rooms: effective_rooms,
            watch_user_ids,
            subscribed: true,
        };
        if !failed_rooms.is_empty() {
            return Err(TinkleError::Subscription(format!(
                "room follows failed (degraded; established follows kept, retry reconciles): {failed_rooms:?}"
            )));
        }
        Ok(SubscribeResult {
            stream: self.principal().await,
            after_cursor: own_follow_cursor,
            head,
        })
    }

    /// Issue a follow RPC at most once per connection. Returns `Ok(None)`
    /// when the stream was already established earlier on this connection
    /// (skip, do not re-request). A failed RPC removes the marker so a
    /// retry re-issues.
    async fn follow_once(
        &self,
        stream: &str,
        after_cursor: u64,
    ) -> Result<Option<tinkle_schema::proto::FollowUpdatesResponse>, TinkleError> {
        if !self.follows.lock().await.insert(stream.to_string()) {
            return Ok(None);
        }
        let req = tinkle_schema::proto::FollowUpdatesRequest {
            stream: stream.to_string(),
            after_cursor,
        };
        match self.updates().follow(req).await {
            Ok(resp) => Ok(Some(resp)),
            Err(e) => {
                self.follows.lock().await.remove(stream);
                Err(e)
            }
        }
    }

    /// Next sync event. `UpdateReady` must be followed by `acknowledge`
    /// after successful application.
    pub async fn next_event(&self) -> Option<SyncEvent> {
        let mgr = self.live_mgr().await.ok()?;
        mgr.next_event().await
    }

    /// Persist the checkpoint after applying the update.
    pub async fn acknowledge(&self, stream: &str, cursor: u64) -> Result<(), TinkleError> {
        let mgr = self.live_mgr().await?;
        mgr.acknowledge(stream, cursor).await
    }

    /// Connection teardown without touching the supervisor. Follow
    /// markers die with the socket: the next connection must re-establish
    /// every follow from persisted cursors.
    async fn teardown_connection(&self) {
        if let Some(live) = self.live.write().await.take() {
            live.handle.close().await;
            live.pump.abort();
            live.sync_mgr.on_disconnect().await;
        }
        self.follows.lock().await.clear();
        self.transport_ok.store(false, Ordering::SeqCst);
    }

    /// Graceful close. Also stops the sync supervisor when running; use
    /// `reconnect` (or `start` again) to resume. Equivalent to `stop`.
    pub async fn close(&self) {
        self.stop().await;
    }

    /// Stop the sync supervisor (if running) and tear down the connection.
    /// Idempotent. Supervised apps call this instead of `close`.
    pub async fn stop(&self) {
        self.stop_supervisor().await;
        self.teardown_connection().await;
    }

    /// Abort the monitor task and detach recovery reporting. Bumps the
    /// epoch so a `start()` that is concurrently establishing cannot
    /// install a supervisor for the stopped generation.
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

    /// Background suspension (mobile).
    pub async fn suspend(&self) {
        self.teardown_connection().await;
    }

    /// Reconnect + re-handshake + explicit resubscribe from persisted cursors.
    pub async fn reconnect(&self) -> Result<(), TinkleError> {
        self.teardown_connection().await;
        let policy = self.config.reconnect.clone();
        let mut attempt: u32 = 0;
        loop {
            match self.connect().await {
                Ok(()) => {
                    let (subscribed, rooms, watch) = {
                        let g = self.interests.lock().await;
                        (g.subscribed, g.rooms.clone(), g.watch_user_ids.clone())
                    };
                    if subscribed {
                        let previous = self.principal().await;
                        if previous == UNKNOWN_PRINCIPAL {
                            if let Err(e) = self.subscribe(rooms, watch).await {
                                self.teardown_connection().await;
                                return Err(e);
                            }
                        } else {
                            let original_rooms = rooms.clone();
                            let watch_clone = watch.clone();
                            let resp = match self.subscribe(rooms, watch).await {
                                Ok(r) => r,
                                Err(e) => {
                                    self.teardown_connection().await;
                                    return Err(e);
                                }
                            };
                            if resp.stream != previous {
                                let zeroed = original_rooms
                                    .into_iter()
                                    .map(|(id, _)| (id, 0))
                                    .collect::<Vec<_>>();
                                if let Err(e) = self.subscribe(zeroed, watch_clone).await {
                                    self.teardown_connection().await;
                                    return Err(e);
                                }
                            }
                        }
                    }
                    return Ok(());
                }
                Err(e) => {
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

    /// Supervised lifecycle: establish the session, restore interests, and
    /// start the sync supervisor, which owns automatic recovery and
    /// reconnection from here on. The host only consumes `next_event()` and
    /// acknowledges/applies updates; gaps, dead transports, and rebuilds
    /// are handled inside the SDK.
    ///
    /// Idempotent: concurrent callers share the single supervisor and get
    /// the initial establishment result. Call `stop()` (or `close()`) to
    /// shut down; `connect`/`subscribe`/`reconnect` remain as documented
    /// low-level primitives for manual control.
    pub async fn start(
        &self,
        rooms: Vec<(i64, u64)>,
        watch_user_ids: Vec<i64>,
    ) -> Result<SubscribeResult, TinkleError> {
        let _guard = self.start_lock.lock().await;
        {
            let sup = self.sup_state.lock().await;
            if sup.handle.is_some() {
                return sup.last_result.clone().ok_or_else(|| {
                    TinkleError::NotConnected("supervisor running without result".into())
                });
            }
        }
        let epoch = self.sup_state.lock().await.epoch;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (sig_tx, sig_rx) = mpsc::channel(64);
        *self.signal_tx.lock().await = Some(sig_tx);
        let result = self.establish(rooms, watch_user_ids).await;
        match result {
            Ok(res) => {
                let mut sup = self.sup_state.lock().await;
                if sup.epoch != epoch || sup.handle.is_some() {
                    // A concurrent stop() won the race: do not install a
                    // supervisor for the stopped generation.
                    return Err(TinkleError::NotConnected("stopped during start".into()));
                }
                let task_client = self.clone();
                let handle =
                    tokio::spawn(async move { task_client.supervise(sig_rx, shutdown_rx).await });
                sup.handle = Some(handle);
                sup.shutdown = Some(shutdown_tx);
                sup.last_result = Some(res.clone());
                Ok(res)
            }
            Err(e) => {
                // Leave no unsupervised half-established connection behind.
                *self.signal_tx.lock().await = None;
                self.teardown_connection().await;
                Err(e)
            }
        }
    }

    /// Connect (if needed) and subscribe. Signals are wired by `connect()`
    /// from `signal_tx`, so supervised and manual paths share this.
    async fn establish(
        &self,
        rooms: Vec<(i64, u64)>,
        watch_user_ids: Vec<i64>,
    ) -> Result<SubscribeResult, TinkleError> {
        if !self.is_connected().await {
            self.connect().await?;
        }
        self.subscribe(rooms, watch_user_ids).await
    }

    /// Supervisor monitor: exactly one per client. Reacts to pump death
    /// and recovery hints; the interval tick is a backstop for missed
    /// hints. Returns only on shutdown.
    async fn supervise(
        self,
        mut sig_rx: mpsc::Receiver<RecoverySignal>,
        mut shutdown: oneshot::Receiver<()>,
    ) {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Per-stream (consecutive failures, last attempt): bounds hammering
        // a persistently failing stream while allowing eventual recovery.
        let mut degraded: HashMap<String, (u32, std::time::Instant)> = HashMap::new();
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = tick.tick() => {
                    if !self.is_connected().await {
                        self.reestablish().await;
                        degraded.clear();
                    }
                }
                Some(signal) = sig_rx.recv() => {
                    match signal {
                        RecoverySignal::PumpDead => {
                            self.reestablish().await;
                            degraded.clear();
                        }
                        RecoverySignal::Gap { stream }
                        | RecoverySignal::CheckpointFailed { stream } => {
                            self.recover_one(&stream, &mut degraded).await;
                        }
                        RecoverySignal::Rebuild { stream } => {
                            // Expiry already reset local state and fenced the
                            // cache; reconstruct from the origin through the
                            // standard recovery path. Persistent failure
                            // leaves RebuildRequired in place.
                            self.recover_one(&stream, &mut degraded).await;
                        }
                    }
                }
            }
        }
    }

    /// Reconnect + resubscribe from persisted interests, with a cooldown so
    /// overlapping triggers do not pile up (reconnect backs off internally).
    async fn reestablish(&self) {
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

    /// Bounded recovery for one stream from its durable applied cursor.
    /// Success clears the degraded record; anything else leaves stream
    /// state (Gap/RebuildRequired events already emitted) for the host.
    async fn recover_one(
        &self,
        stream: &str,
        degraded: &mut HashMap<String, (u32, std::time::Instant)>,
    ) {
        let now = std::time::Instant::now();
        if let Some((count, last)) = degraded.get(stream) {
            if *count >= 3 && now.duration_since(*last) < std::time::Duration::from_secs(30) {
                return;
            }
        }
        let mgr = match self.live_mgr().await {
            Ok(m) => m,
            // No connection: the watchdog owns re-establishment; the next
            // signal after reconnect retries recovery.
            Err(_) => return,
        };
        let after = mgr.applied_cursor(stream).await;
        let mut ok = false;
        for _ in 0..3 {
            match self.recover_stream(stream, after, 200).await {
                Ok(_) => {
                    ok = true;
                    break;
                }
                Err(e) => match e.recovery_decision() {
                    RecoveryDecision::Retry => {
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        continue;
                    }
                    // Rebuild/Auth/Fail: state already reflects the outcome
                    // (RebuildRequired event or surfaced error); do not loop.
                    _ => break,
                },
            }
        }
        if ok {
            degraded.remove(stream);
        } else {
            let count = degraded.get(stream).map(|(c, _)| *c).unwrap_or(0) + 1;
            degraded.insert(stream.to_string(), (count, now));
        }
    }

    /// Best-effort effective cursor for `stream` (`applied` plus volatile
    /// delivered progress). `None` when offline. Authoritative for UI
    /// progress: informational events may drop under pressure, cursors do
    /// not.
    pub async fn current_cursor(&self, stream: &str) -> Option<u64> {
        match self.live_mgr().await {
            Ok(mgr) => Some(mgr.effective_cursor(stream).await),
            Err(_) => None,
        }
    }

    pub async fn rotate_credential(&mut self, next: Credential) -> Result<(), TinkleError> {
        self.close().await;
        self.credential = next;
        self.connect().await
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

/// Resolves the authenticated principal without creating a duplicate
/// subscription. Swap point for a future dedicated identity RPC: replace
/// the strategy, keep the exactly-once-follow contract.
#[async_trait]
trait PrincipalResolver: Send + Sync {
    async fn resolve(&self, client: &Client) -> Result<String, TinkleError>;
}

/// Probe strategy: the empty-stream follow request doubles as the
/// own-stream follow. The gateway resolves the caller's stream from
/// authentication and echoes the concrete `user:<id>`; that follow stays
/// active, so no second own-stream follow is ever issued on the connection.
/// `SyncManager::resolve_principal` preserves the retained suffix that may
/// already be replaying through the pump.
struct ProbeFollowResolver;

#[async_trait]
impl PrincipalResolver for ProbeFollowResolver {
    async fn resolve(&self, client: &Client) -> Result<String, TinkleError> {
        if client.principal().await != UNKNOWN_PRINCIPAL {
            return Ok(client.principal().await);
        }
        let mgr = client.live_mgr().await?;
        let probe = tinkle_schema::proto::FollowUpdatesRequest {
            stream: String::new(),
            after_cursor: 0,
        };
        match client.updates().follow(probe).await {
            Ok(resp) if !resp.stream.is_empty() => {
                let resolved = resp.stream.clone();
                *client.principal.write().await = resolved.clone();
                mgr.resolve_principal(resolved.clone()).await;
                client.follows.lock().await.insert(resolved.clone());
                Ok(resolved)
            }
            Ok(_) => {
                // Gateway without empty-stream resolution: principal must
                // have been set via `sign_in` or `set_principal_user_id`.
                // Fail closed rather than checkpointing under `unknown`.
                Err(TinkleError::Subscription(
                    "authenticated principal unknown; call set_principal_user_id after sign_in"
                        .into(),
                ))
            }
            Err(e) => Err(e),
        }
    }
}

fn principal_user_suffix(principal: &str) -> String {
    principal.strip_prefix("user:").unwrap_or("0").to_string()
}
