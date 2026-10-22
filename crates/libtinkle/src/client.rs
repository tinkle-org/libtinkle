//! High-level typed client: `connect → call → subscribe → reconnect`.
//!
//! Conceptual use:
//! ```ignore
//! client.connect().await?;
//! let resp = client.messages().send_message(req).await?;
//! ```
//!
//! The SDK serializes the public request protobuf, constructs the Syntra
//! `RpcRequest`, allocates a nonzero `request_id`, sends through the
//! active connection, awaits the matching `RpcResult`, decodes the typed
//! response, and returns typed errors. Never auto-retries unsafe
//! mutations without an application idempotency policy.

use std::sync::Arc;

use tokio::sync::RwLock;

use syntra_runtime::{CancelToken, Initiator};

use crate::config::{ClientConfig, Credential};
use crate::cursor::CursorStore;
use crate::errors::TinkleError;
use crate::reconnect::{backoff_sleep, should_retry};
use crate::services::{Auth, Messages, Realtime};
use crate::subscription::{Interests, RealtimeEvent, SubscriptionManager};

struct Live {
    handle: syntra_ws::WsHandle,
    connection_id: String,
    version: u32,
    sub_mgr: Arc<SubscriptionManager>,
    pump: tokio::task::JoinHandle<()>,
}

/// Typed Tinkle client over Syntra.
///
/// Cheap to clone (shares connection + cursors). The same logical runtime
/// is reusable across iOS/Android/WASM via thin platform adapters.
#[derive(Clone)]
pub struct Client {
    config: ClientConfig,
    credential: Credential,
    store: Arc<dyn CursorStore>,
    live: Arc<RwLock<Option<Live>>>,
    /// Authenticated principal label for cursor scope, e.g. `user:123`.
    /// Set after first successful subscribe; used for durable isolation.
    principal: Arc<RwLock<String>>,
    /// Subscription interests that survive reconnect (Live + its
    /// SubscriptionManager are discarded on close).
    interests: Arc<tokio::sync::Mutex<Interests>>,
}

impl Client {
    pub fn new(config: ClientConfig, credential: Credential, store: Arc<dyn CursorStore>) -> Self {
        Self {
            config,
            credential,
            store,
            live: Arc::new(RwLock::new(None)),
            principal: Arc::new(RwLock::new("unknown".to_string())),
            interests: Arc::new(tokio::sync::Mutex::new(Interests::default())),
        }
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

    /// Selected Syntra version (`Frame.version` post-handshake).
    pub async fn version(&self) -> Option<u32> {
        self.live.read().await.as_ref().map(|l| l.version)
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

        // WsHandle owns WS background tasks; Live owns the handle and
        // explicit close() consumes it via WsHandle::close.
        let principal = self.principal.read().await.clone();
        let sub_mgr = SubscriptionManager::new(principal, self.store.clone());

        // Notification pump: Initiator → validate → cursors → app events.
        // Interleaved notifications never complete an RPC (core routes them
        // here, not to pending waiters).
        let pump_conn = conn.clone();
        let pump_mgr = sub_mgr.clone();
        let pump = tokio::spawn(async move {
            while let Some(payload) = pump_conn.next_notification().await {
                if let Err(e) = pump_mgr.handle_payload(&payload).await {
                    tracing::debug!(error = %e, "notification handling reported gap/reset");
                }
            }
        });

        *self.live.write().await = Some(Live {
            handle,
            connection_id,
            version,
            sub_mgr,
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

    async fn live_mgr(&self) -> Result<Arc<SubscriptionManager>, TinkleError> {
        self.live
            .read()
            .await
            .as_ref()
            .map(|l| l.sub_mgr.clone())
            .ok_or_else(|| TinkleError::NotConnected("call connect() first".into()))
    }

    /// Raw RPC: opaque bytes in/out. Validates method shape, allocates a
    /// nonzero id, enforces bounded outstanding, awaits the matching
    /// terminal result. Supports concurrent out-of-order completion.
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

    /// Cancellable RPC: `token.cancel()` sends best-effort `Cancel{}` for
    /// the allocated id and returns `CANCELLED`. Completion wins races.
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

    /// Typed service facades (method bindings from verified descriptors).
    pub fn messages(&self) -> Messages<'_> {
        Messages::new(self)
    }

    pub fn auth(&self) -> Auth<'_> {
        Auth::new(self)
    }

    pub(crate) fn realtime(&self) -> Realtime<'_> {
        Realtime::new(self)
    }

    /// Subscribe with durable cursor recovery. Hydrates memory from the
    /// store, calls `SubscribeBatched`, records interests, and returns the
    /// response (which confirms interests, not replay completion).
    pub async fn subscribe(
        &self,
        rooms: Vec<(i64, u64)>,
        watch_user_ids: Vec<i64>,
    ) -> Result<tinkle_schema::proto::SubscribeResponse, TinkleError> {
        let mgr = self.live_mgr().await?;
        // Hydrate own + room streams. Own stream label is derived after
        // first subscribe; before that use persisted principal label.
        let principal = self.principal().await;
        let mut streams = vec![format!("user:{}", principal_user_suffix(&principal))];
        for (room_id, _) in &rooms {
            streams.push(format!("room:{room_id}"));
        }
        // Best-effort hydrate; absent means zero (never start-live).
        let _ = mgr.hydrate_from_store(&streams).await;

        // Build request with per-stream after cursors.
        // Own cursor: load durable for the own stream if known.
        let own_stream = streams.first().cloned().unwrap_or_default();
        let own_cursor = self.store.load(&principal, &own_stream).await?.unwrap_or(0);
        let req_rooms: Vec<tinkle_schema::proto::RealtimeRoomSubscription> = rooms
            .iter()
            .map(
                |(room_id, after)| tinkle_schema::proto::RealtimeRoomSubscription {
                    room_id: *room_id,
                    after_cursor: if *after == 0 { None } else { Some(*after) },
                },
            )
            .collect();

        let req = tinkle_schema::proto::SubscribeRequest {
            own_cursor: if own_cursor == 0 {
                None
            } else {
                Some(own_cursor)
            },
            rooms: req_rooms,
            watch_user_ids: watch_user_ids.clone(),
        };
        let resp = self.realtime().subscribe_batched(req).await?;
        // `stream` is user:<verified-user-id> from authenticated identity.
        *self.principal.write().await = resp.stream.clone();
        mgr.on_subscribed(rooms.clone(), watch_user_ids.clone())
            .await;
        *self.interests.lock().await = Interests {
            rooms,
            watch_user_ids,
            subscribed: true,
        };
        Ok(resp)
    }

    /// Next realtime app event (batch/presence/gap/reset).
    pub async fn next_event(&self) -> Option<RealtimeEvent> {
        let mgr = self.live_mgr().await.ok()?;
        mgr.next_event().await
    }

    /// Graceful close: stop new RPCs, allow close grace, teardown.
    /// In-flight RPCs get `UNAVAILABLE`/`CANCELLED`. State is discarded
    /// except durable cursors + diagnostics.
    pub async fn close(&self) {
        if let Some(live) = self.live.write().await.take() {
            live.handle.close().await;
            live.pump.abort();
            live.sub_mgr.on_disconnect().await;
        }
    }

    /// Background suspension (mobile): graceful close, keep durable
    /// cursors. No permanent background WebSocket is assumed.
    pub async fn suspend(&self) {
        self.close().await;
    }

    /// App restart / network change / wake: reconnect + re-handshake +
    /// explicit resubscribe from persisted cursors. Push notifications
    /// trigger this after wake (see docs/mobile-lifecycle.md).
    pub async fn reconnect(&self) -> Result<(), TinkleError> {
        self.close().await;
        let policy = self.config.reconnect.clone();
        let mut attempt: u32 = 0;
        loop {
            match self.connect().await {
                Ok(()) => {
                    // Re-establish Client-level interests explicitly; id
                    // space is fresh and Live was discarded on close.
                    // Check `subscribed` (not list emptiness) so the own-user
                    // stream is restored even when rooms/watch are empty.
                    let (subscribed, rooms, watch) = {
                        let g = self.interests.lock().await;
                        (g.subscribed, g.rooms.clone(), g.watch_user_ids.clone())
                    };
                    if subscribed {
                        // Propagate resubscribe failures: reconnect succeeds
                        // only once subscriptions are active again.
                        self.subscribe(rooms, watch).await?;
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

    /// Token expiry rotation: replace credential and re-handshake on a new
    /// transport (re-auth on the same connection is forbidden by default).
    // TODO(runtime-acceptance-credential): UNAUTHENTICATED triggers a single
    // rotation + reconnect; acceptance must assert no hot re-auth loop.
    pub async fn rotate_credential(&mut self, next: Credential) -> Result<(), TinkleError> {
        self.close().await;
        self.credential = next;
        self.connect().await
    }

    /// Observability: outstanding RPC count.
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
}

fn principal_user_suffix(principal: &str) -> String {
    // principal labels are `user:<id>` after first subscribe; before that
    // the label is "unknown" and the own stream hydrates as zero.
    principal.strip_prefix("user:").unwrap_or("0").to_string()
}
