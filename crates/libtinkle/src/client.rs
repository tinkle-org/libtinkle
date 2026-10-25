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

use std::sync::Arc;

use tokio::sync::RwLock;

use syntra_runtime::{CancelToken, Initiator};

use crate::config::{ClientConfig, Credential};
use crate::cursor::CursorStore;
use crate::errors::TinkleError;
use crate::reconnect::{backoff_sleep, should_retry};
use crate::services::{Auth, Messages, Updates};
use crate::sync::{Interests, SyncEvent, SyncManager};

struct Live {
    handle: syntra_ws::WsHandle,
    connection_id: String,
    version: u32,
    sync_mgr: Arc<SyncManager>,
    pump: tokio::task::JoinHandle<()>,
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
            principal: Arc::new(RwLock::new("unknown".to_string())),
            interests: Arc::new(tokio::sync::Mutex::new(Interests::default())),
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
            principal: Arc::new(RwLock::new("unknown".to_string())),
            interests: Arc::new(tokio::sync::Mutex::new(Interests::default())),
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
        self.live.read().await.is_some()
    }

    pub async fn principal(&self) -> String {
        self.principal.read().await.clone()
    }

    /// Set the authenticated principal from a known user id.
    /// Called automatically after `sign_in`; apps with an existing
    /// session token should call it after resolving their own id.
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

        // Notification pump: Initiator → validate → cursors → app events.
        // Interleaved notifications never complete an RPC (core routes them
        // here, not to pending waiters).
        let pump_conn = conn.clone();
        let pump_mgr = sync_mgr.clone();
        let pump = tokio::spawn(async move {
            while let Some(payload) = pump_conn.next_notification().await {
                if let Err(e) = pump_mgr.handle_payload(&payload).await {
                    tracing::debug!(error = %e, "sync notification handling reported gap/rebuild");
                }
            }
        });

        *self.live.write().await = Some(Live {
            handle,
            connection_id,
            version,
            sync_mgr,
            pump,
        });
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

    pub(crate) fn updates(&self) -> Updates<'_> {
        Updates::new(self)
    }

    /// gRPC catch-up for one stream. Requires `grpc_endpoint` in config.
    pub(crate) async fn read_updates_grpc(
        &self,
        req: tinkle_schema::proto::ReadUpdatesRequest,
    ) -> Result<tinkle_schema::proto::ReadUpdatesResponse, TinkleError> {
        let endpoint = self
            .config
            .grpc_endpoint
            .clone()
            .ok_or_else(|| TinkleError::Grpc("grpc_endpoint not configured".into()))?;
        let lower = endpoint.to_ascii_lowercase();
        if self.config.allow_insecure {
            if !(lower.starts_with("https://") || lower.starts_with("http://")) {
                return Err(TinkleError::Grpc(
                    "grpc_endpoint must include http:// or https:// scheme".into(),
                ));
            }
        } else if !lower.starts_with("https://") {
            return Err(TinkleError::Grpc(
                "grpc_endpoint must use https:// unless allow_insecure".into(),
            ));
        }
        let token = self.credential.expose_secret().to_vec();
        crate::grpc::read_updates_grpc(&endpoint, &token, req, 30_000).await
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
        let _ = mgr.begin_recovery(stream).await;
        let mut last_resp: Option<tinkle_schema::proto::ReadUpdatesResponse> = None;
        let mut pages = 0u32;
        let mut no_progress_pages = 0u32;
        let result: Result<tinkle_schema::proto::ReadUpdatesResponse, TinkleError> = async {
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
                    match self.read_updates_grpc(req.clone()).await {
                        Ok(r) => break r,
                        Err(_) if attempt < 2 => {
                            attempt += 1;
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (1 << attempt),
                            ))
                            .await;
                            continue;
                        }
                        Err(e) => return Err(e),
                    }
                };
                mgr.record_bounds(stream, resp.replay_floor, Some(resp.head))
                    .await;
                if after < resp.replay_floor {
                    mgr.handle_expired(stream, Some(resp.head)).await;
                    return Err(TinkleError::Sync(format!(
                        "replay expired on {stream}: after {after} < floor {}",
                        resp.replay_floor
                    )));
                }
                if after > resp.head {
                    return Err(TinkleError::Sync(format!(
                        "after {after} > head {} on {stream}",
                        resp.head
                    )));
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
                            let msg = e.to_string();
                            if msg.contains("gap") || msg.contains("Gap") {
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
            last_resp.ok_or_else(|| TinkleError::Grpc("empty recovery: no pages".into()))
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
    /// Flow: learn principal (own-stream probe) → hydrate → gRPC catch-up
    /// for own + rooms → start Syntra follows + connectivity observations.
    /// Returns the own-stream confirmation; replay itself arrives as
    /// `UpdateReady` events requiring explicit `acknowledge`.
    pub async fn subscribe(
        &self,
        rooms: Vec<(i64, u64)>,
        watch_user_ids: Vec<i64>,
    ) -> Result<SubscribeResult, TinkleError> {
        let mgr = self.live_mgr().await?;
        if self.principal().await == "unknown" {
            // Probe the authenticated own stream via the follow binding:
            // an empty stream selects the caller's own user stream and the
            // gateway echoes the concrete `user:<id>`.
            let probe = tinkle_schema::proto::FollowUpdatesRequest {
                stream: String::new(),
                after_cursor: 0,
            };
            match self.updates().follow(probe).await {
                Ok(resp) if !resp.stream.is_empty() => {
                    let authenticated = resp.stream.clone();
                    *self.principal.write().await = authenticated.clone();
                    mgr.set_principal(authenticated).await;
                }
                _ => {
                    // Gateway without empty-stream resolution: principal
                    // must have been set via `sign_in` or
                    // `set_principal_user_id`. Fail closed rather than
                    // checkpointing under `unknown`.
                    return Err(TinkleError::Subscription(
                        "authenticated principal unknown; call set_principal_user_id after sign_in"
                            .into(),
                    ));
                }
            }
        }
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

        // Start follows for own + rooms. Each follow replays retained
        // suffix after the supplied cursor, covering commits between the
        // gRPC page and subscription establishment.
        let follow_own = tinkle_schema::proto::FollowUpdatesRequest {
            stream: own_stream.clone(),
            after_cursor: own_cursor,
        };
        let own_resp = self.updates().follow(follow_own).await?;
        if !own_resp.stream.is_empty() && own_resp.stream != own_stream {
            // Principal rotation: gateway resolved a different own stream.
            *self.principal.write().await = own_resp.stream.clone();
            mgr.set_principal(own_resp.stream.clone()).await;
        }
        for (room_id, after) in &effective_rooms {
            let req = tinkle_schema::proto::FollowUpdatesRequest {
                stream: format!("room:{room_id}"),
                after_cursor: *after,
            };
            // Room follow failures surface immediately; own follow already
            // established, so caller can retry rooms without losing own.
            let _ = self.updates().follow(req).await?;
        }
        if !watch_user_ids.is_empty() {
            let req = tinkle_schema::proto::ObserveConnectivityRequest {
                user_ids: watch_user_ids.clone(),
            };
            let _ = self.updates().observe_connectivity(req).await;
        }

        mgr.on_subscribed(effective_rooms.clone(), watch_user_ids.clone())
            .await;
        *self.interests.lock().await = Interests {
            rooms: effective_rooms,
            watch_user_ids,
            subscribed: true,
        };
        Ok(SubscribeResult {
            stream: own_resp.stream.clone(),
            after_cursor: own_cursor,
            head: head.or(own_resp.head),
        })
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

    /// Graceful close.
    pub async fn close(&self) {
        if let Some(live) = self.live.write().await.take() {
            live.handle.close().await;
            live.pump.abort();
            live.sync_mgr.on_disconnect().await;
        }
    }

    /// Background suspension (mobile).
    pub async fn suspend(&self) {
        self.close().await;
    }

    /// Reconnect + re-handshake + explicit resubscribe from persisted cursors.
    pub async fn reconnect(&self) -> Result<(), TinkleError> {
        self.close().await;
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
                        if previous == "unknown" {
                            if let Err(e) = self.subscribe(rooms, watch).await {
                                self.close().await;
                                return Err(e);
                            }
                        } else {
                            let original_rooms = rooms.clone();
                            let watch_clone = watch.clone();
                            let resp = match self.subscribe(rooms, watch).await {
                                Ok(r) => r,
                                Err(e) => {
                                    self.close().await;
                                    return Err(e);
                                }
                            };
                            if resp.stream != previous {
                                let zeroed = original_rooms
                                    .into_iter()
                                    .map(|(id, _)| (id, 0))
                                    .collect::<Vec<_>>();
                                if let Err(e) = self.subscribe(zeroed, watch_clone).await {
                                    self.close().await;
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

fn principal_user_suffix(principal: &str) -> String {
    principal.strip_prefix("user:").unwrap_or("0").to_string()
}
