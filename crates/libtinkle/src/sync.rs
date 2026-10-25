//! Application sync + notification decoding + cursor tracking.
//!
//! Follows are established via generic RPC after ACTIVE
//! (`/tinkle.v1.UpdatesService/FollowUpdates`). Delivery is `notification`
//! frames (`request_id == 0`) carrying `FollowUpdatesResponse` or
//! `ObserveConnectivityResponse` bytes. Catch-up uses actual gRPC
//! `ReadUpdates`. This module validates updates, tracks per-stream
//! cursors, detects duplicates/gaps, surfaces rebuilds, and persists
//! durable checkpoints through [`crate::CursorStore`] only after the
//! application has applied the data.
//!
//! Persistent user application state lives here, outside the generic
//! Syntra core. Connectivity observations are ephemeral and cleared on
//! disconnect.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tracing::debug;

use tinkle_schema::{
    proto::{ConnectivityChanged, ObserveConnectivityResponse, Update},
    sync::{decide_cursor, CursorDecision, StreamCursors, ValidatedFollow},
};

use crate::cursor::CursorStore;
use crate::errors::TinkleError;

/// App-visible sync events.
#[derive(Debug, Clone)]
pub enum SyncEvent {
    /// One durable update validated and ready for application.
    /// The app must apply `update` (plus required refreshes/hints) and
    /// then call [`SyncManager::acknowledge`] to persist the checkpoint.
    /// Receipt alone never advances the checkpoint.
    UpdateReady {
        stream: String,
        cursor: u64,
        update: Box<Update>,
    },
    /// Update applied and checkpoint committed.
    Applied { stream: String, cursor: u64 },
    /// Duplicate delivery ignored (no reapply, no advance).
    Duplicate { stream: String, cursor: u64 },
    /// Gap: missing update; progress stopped, old checkpoint retained.
    Gap {
        stream: String,
        expected: u64,
        got: u64,
    },
    /// Stale/rollback overlap ignored.
    Stale {
        stream: String,
        applied: u64,
        got: u64,
    },
    /// Ephemeral connectivity observation (never checkpointed).
    Connectivity(ConnectivityChanged),
    /// Progress-only heartbeat; never advances the checkpoint.
    Progress { stream: String, head: Option<u64> },
    /// Rebuild: one stream sequence terminated. Recovery requires a proven
    /// consistent snapshot/checkpoint; head is diagnostic only.
    RebuildRequired { stream: String, head: Option<u64> },
    /// Durable checkpoint save failed. Memory remains at the previous
    /// committed checkpoint; the update must be re-applied after recovery.
    CheckpointFailed {
        stream: String,
        cursor: u64,
        error: String,
    },
}

/// Per-stream recovery status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum StreamStatus {
    #[default]
    Initializing,
    Recovering,
    Live,
    RebuildRequired,
}

/// Principal-scoped cursor state guarded by a single synchronization
/// barrier. `principal` and `cursors` must always be observed and mutated
/// together. Store I/O stays outside the lock so a slow store never blocks
/// the notification pump.
#[derive(Debug, Default)]
struct PrincipalState {
    principal: String,
    cursors: StreamCursors,
    /// Last emitted `UpdateReady` per stream, independent of the persisted
    /// applied checkpoint. Lets consecutive unacknowledged updates advance
    /// while `acknowledge` still persists only applied cursors.
    delivered: HashMap<String, u64>,
    /// Buffered live updates arriving ahead of the applied cursor, ordered
    /// by cursor. Replayed through the same validation/application path
    /// after recovery fills the gap.
    pending: HashMap<String, BTreeMap<u64, Update>>,
    status: HashMap<String, StreamStatus>,
    heads: HashMap<String, Option<u64>>,
}

/// Manages one principal sync interests + cursors (crate-internal;
/// apps observe [`SyncEvent`] via [`crate::Client::next_event`]).
pub struct SyncManager {
    state: Mutex<PrincipalState>,
    store: Arc<dyn CursorStore>,
    #[cfg(not(target_arch = "wasm32"))]
    sqlite: Mutex<Option<Arc<tinkle_storage::Storage>>>,
    event_tx: mpsc::Sender<SyncEvent>,
    event_rx: Mutex<mpsc::Receiver<SyncEvent>>,
    interests: Mutex<Interests>,
}

/// Sync interests carried across reconnects.

#[derive(Debug, Default, Clone)]
pub(crate) struct Interests {
    pub(crate) rooms: Vec<(i64, u64)>,
    pub(crate) watch_user_ids: Vec<i64>,
    pub(crate) subscribed: bool,
}

impl SyncManager {
    pub fn new(principal: impl Into<String>, store: Arc<dyn CursorStore>) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(256);
        Arc::new(Self {
            state: Mutex::new(PrincipalState {
                principal: principal.into(),
                cursors: StreamCursors::new(),
                delivered: HashMap::new(),
                pending: HashMap::new(),
                status: HashMap::new(),
                heads: HashMap::new(),
            }),
            store,
            #[cfg(not(target_arch = "wasm32"))]
            sqlite: Mutex::new(None),
            event_tx: tx,
            event_rx: Mutex::new(rx),
            interests: Mutex::new(Interests::default()),
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn set_sqlite_storage(&self, storage: Arc<tinkle_storage::Storage>) {
        *self.sqlite.lock().await = Some(storage);
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn sqlite_clone(&self) -> Option<Arc<tinkle_storage::Storage>> {
        self.sqlite.lock().await.clone()
    }

    pub(crate) async fn begin_recovery(&self, stream: &str) -> bool {
        let mut state = self.state.lock().await;
        match state.status.get(stream) {
            Some(StreamStatus::Recovering) => false,
            _ => {
                state
                    .status
                    .insert(stream.to_string(), StreamStatus::Recovering);
                true
            }
        }
    }

    pub(crate) async fn end_recovery(&self, stream: &str, live: bool) {
        let mut state = self.state.lock().await;
        state.status.insert(
            stream.to_string(),
            if live {
                StreamStatus::Live
            } else {
                StreamStatus::Initializing
            },
        );
    }

    pub(crate) async fn record_bounds(&self, stream: &str, floor: u64, head: Option<u64>) {
        {
            let mut state = self.state.lock().await;
            state.cursors.set_floor(stream, floor);
            state.heads.insert(stream.to_string(), head);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(sqlite) = self.sqlite_clone().await {
            // Best-effort; recovery validation uses in-memory bounds first.
            let _ = sqlite.set_replay_bounds(
                &self.state.lock().await.principal.clone(),
                stream,
                floor,
                head,
            );
        }
    }

    pub(crate) async fn set_principal(&self, principal: String) {
        let mut state = self.state.lock().await;
        if state.principal == principal {
            return;
        }
        state.principal = principal;
        state.cursors = StreamCursors::new();
        state.delivered.clear();
        state.pending.clear();
        state.status.clear();
        state.heads.clear();
    }

    pub(crate) async fn handle_expired(&self, stream: &str, head: Option<u64>) {
        let principal = {
            let mut state = self.state.lock().await;
            state.cursors.on_reset(stream);
            state.delivered.remove(stream);
            state.pending.remove(stream);
            state
                .status
                .insert(stream.to_string(), StreamStatus::RebuildRequired);
            state.heads.insert(stream.to_string(), head);
            state.principal.clone()
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(sqlite) = self.sqlite.lock().await.clone() {
            let _ = sqlite.mark_rebuild(&principal, stream);
        }
        let _ = self.store.remove_stream(&principal, stream).await;
        let _ = self
            .event_tx
            .send(SyncEvent::RebuildRequired {
                stream: stream.to_string(),
                head,
            })
            .await;
    }

    pub(crate) async fn on_subscribed(&self, rooms: Vec<(i64, u64)>, watch_user_ids: Vec<i64>) {
        let mut g = self.interests.lock().await;
        g.rooms = rooms;
        g.watch_user_ids = watch_user_ids;
        g.subscribed = true;
    }

    pub(crate) async fn hydrate_from_store(&self, streams: &[String]) -> Result<(), TinkleError> {
        let principal = self.state.lock().await.principal.clone();
        let mut loaded: Vec<(String, u64)> = Vec::with_capacity(streams.len());
        for s in streams {
            let v = self.store.load(&principal, s).await?.unwrap_or(0);
            loaded.push((s.clone(), v));
        }
        let mut state = self.state.lock().await;
        if state.principal != principal {
            return Ok(());
        }
        for (s, v) in loaded {
            if v > state.cursors.get(&s) {
                state.cursors.hydrate(&s, v);
            } else if v == 0 && state.cursors.get(&s) == 0 {
                state.cursors.hydrate(&s, 0);
            }
            let applied = state.cursors.get(&s);
            // Delivered tracks volatile emits ahead of the durable cursor.
            // Never move it backward; if it lags the hydrated checkpoint,
            // re-anchor it so the next expected cursor is applied + 1.
            match state.delivered.get(&s).copied() {
                Some(d) if d >= applied => {}
                _ => {
                    if applied == 0 {
                        state.delivered.remove(&s);
                    } else {
                        state.delivered.insert(s.clone(), applied);
                    }
                }
            }
            // Preserve buffered live updates above the hydrated cursor so
            // valid gaps buffered during recovery survive hydration.
            if let Some(pending) = state.pending.get_mut(&s) {
                pending.retain(|cursor, _| *cursor > applied);
                if pending.is_empty() {
                    state.pending.remove(&s);
                }
            }
            // Bounds are re-established per recovery page; stale heads must
            // not fence. Clear only hydrated streams, leave others alone.
            state.heads.remove(&s);
        }
        Ok(())
    }

    /// Handle one raw follow `Notification.payload` carrying exactly one
    /// `FollowUpdatesResponse`. Validates before any effects. `Update`
    /// results emit `UpdateReady` without persisting; the app must call
    /// [`Self::acknowledge`] after applying.
    pub(crate) async fn handle_payload(&self, payload: &[u8]) -> Result<(), TinkleError> {
        // Try connectivity first: `ObserveConnectivityResponse` and
        // `FollowUpdatesResponse` are distinct top-level messages. The
        // gateway routes them unambiguously; a payload that decodes as
        // connectivity is never a durable update.
        if let Ok(obs) = Self::try_decode_connectivity(payload) {
            let _ = self.event_tx.try_send(SyncEvent::Connectivity(obs));
            return Ok(());
        }
        let follow = tinkle_schema::sync::validate_follow_response(payload)?;
        match follow {
            ValidatedFollow::Update(v) => self.handle_validated_update(v.update).await,
            ValidatedFollow::Rebuild { stream, head } => {
                let principal = {
                    let mut state = self.state.lock().await;
                    state.cursors.on_reset(&stream);
                    state.delivered.remove(&stream);
                    state.pending.remove(&stream);
                    state
                        .status
                        .insert(stream.clone(), StreamStatus::RebuildRequired);
                    state.heads.insert(stream.clone(), head);
                    state.principal.clone()
                };
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(sqlite) = self.sqlite.lock().await.clone() {
                    let _ = sqlite.mark_rebuild(&principal, &stream);
                }
                let _ = self.store.remove_stream(&principal, &stream).await;
                let _ = self
                    .event_tx
                    .send(SyncEvent::RebuildRequired { stream, head })
                    .await;
                Ok(())
            }
            ValidatedFollow::Progress { stream, head } => {
                let _ = self.event_tx.try_send(SyncEvent::Progress { stream, head });
                Ok(())
            }
        }
    }

    fn try_decode_connectivity(payload: &[u8]) -> Result<ConnectivityChanged, ()> {
        use prost::Message as _;
        let resp = ObserveConnectivityResponse::decode(payload).map_err(|_| ())?;
        // Connectivity responses always carry an update; follow responses
        // never decode to a valid connectivity shape with a positive user.
        let u = resp.update.ok_or(())?;
        if u.user_id <= 0 {
            return Err(());
        }
        Ok(u)
    }

    /// Shared pipeline for gRPC `ReadUpdates` pages and live follows.
    /// Validates, classifies by `(stream, max(applied, delivered), cursor)`,
    /// and emits `UpdateReady` for the next contiguous update without
    /// persisting.
    pub async fn handle_update(&self, update: Update) -> Result<(), TinkleError> {
        let v = tinkle_schema::sync::validate_update(&update)?;
        self.handle_validated_update(v.update).await
    }

    async fn handle_validated_update(&self, update: Update) -> Result<(), TinkleError> {
        // Process one update plus any buffered contiguous chain. Gaps are
        // buffered (bounded) and replayed through this same path, so live
        // arrivals during recovery are not dropped.
        let mut current = Some(update);
        let mut first_err: Option<TinkleError> = None;
        while let Some(u) = current.take() {
            match self.step_once(u).await {
                Ok(next) => {
                    current = next;
                    if current.is_none() {
                        break;
                    }
                }
                Err(e) => {
                    first_err = Some(e);
                    break;
                }
            }
        }
        if let Some(e) = first_err {
            Err(e)
        } else {
            Ok(())
        }
    }

    #[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
    async fn step_once(&self, update: Update) -> Result<Option<Update>, TinkleError> {
        let stream = update.stream.clone();
        let cursor = update.cursor;
        enum Kind {
            Duplicate,
            Gap { expected: u64 },
            Stale { applied: u64 },
            Next { principal: String, applied: u64 },
        }
        let kind = {
            let mut state = self.state.lock().await;
            let applied = state.cursors.get(&stream);
            let delivered = state.delivered.get(&stream).copied().unwrap_or(0);
            let effective = applied.max(delivered);
            match decide_cursor(effective, cursor) {
                CursorDecision::Duplicate => Kind::Duplicate,
                CursorDecision::Gap => {
                    let expected = effective.saturating_add(1);
                    // Bounded buffer: recovery refetches on overflow.
                    let pending = state.pending.entry(stream.clone()).or_default();
                    if pending.len() < 512 {
                        pending.insert(cursor, update.clone());
                    }
                    Kind::Gap { expected }
                }
                CursorDecision::Stale => Kind::Stale { applied },
                CursorDecision::Next => {
                    // Explicit mode advances delivered here; SQLite mode
                    // advances after the atomic DB commit below.
                    #[cfg(not(target_arch = "wasm32"))]
                    let has_sqlite = self.sqlite.lock().await.is_some();
                    #[cfg(target_arch = "wasm32")]
                    let has_sqlite = false;
                    if !has_sqlite {
                        state.delivered.insert(stream.clone(), cursor);
                    }
                    Kind::Next {
                        principal: state.principal.clone(),
                        applied,
                    }
                }
            }
        };
        match kind {
            Kind::Duplicate => {
                let _ = self
                    .event_tx
                    .send(SyncEvent::Duplicate {
                        stream: stream.clone(),
                        cursor,
                    })
                    .await;
                Ok(None)
            }
            Kind::Stale { applied } => {
                let _ = self
                    .event_tx
                    .send(SyncEvent::Stale {
                        stream: stream.clone(),
                        applied,
                        got: cursor,
                    })
                    .await;
                Ok(None)
            }
            Kind::Gap { expected } => {
                let _ = self
                    .event_tx
                    .send(SyncEvent::Gap {
                        stream: stream.clone(),
                        expected,
                        got: cursor,
                    })
                    .await;
                Err(TinkleError::from(tinkle_schema::sync::SyncError::Gap {
                    stream: stream.clone(),
                    expected,
                    got: cursor,
                }))
            }
            Kind::Next { principal, applied } => {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(sqlite) = self.sqlite_clone().await {
                    // Automatic mode: project + commit cache and checkpoint
                    // atomically, then publish Applied. No explicit ack needed.
                    let ops = crate::cache_projector::project_update_to_ops(&principal, &update)
                        .map_err(TinkleError::Sync)?;
                    if let Err(e) =
                        sqlite.apply_sync_batch(&principal, &stream, cursor, applied, &ops)
                    {
                        // Scope moved under us (concurrent apply) or rebuild:
                        // hydrate in-memory cursor and surface for recovery.
                        if matches!(e, tinkle_storage::StorageError::ScopeMismatch { .. }) {
                            if let Ok(Some(current)) = sqlite.load_checkpoint(&principal, &stream) {
                                let mut state = self.state.lock().await;
                                if state.principal == principal {
                                    state.cursors.hydrate(&stream, current);
                                    let delivered_prev =
                                        state.delivered.get(&stream).copied().unwrap_or(0);
                                    state
                                        .delivered
                                        .insert(stream.clone(), current.max(delivered_prev));
                                }
                            }
                            return Err(TinkleError::Sync(e.to_string()));
                        }
                        let error = e.to_string();
                        let _ = self
                            .event_tx
                            .send(SyncEvent::CheckpointFailed {
                                stream: stream.clone(),
                                cursor,
                                error,
                            })
                            .await;
                        return Err(TinkleError::CursorStore(format!(
                            "atomic apply failed: {e}"
                        )));
                    }
                    {
                        let mut state = self.state.lock().await;
                        if state.principal == principal {
                            let _ = state.cursors.commit(&stream, cursor);
                            state.delivered.insert(stream.clone(), cursor);
                        }
                    }
                    // Persist through the CursorStore path as well when it is
                    // SQLite-backed this is a duplicate idempotent save; when
                    // it is foreign/InMemory it keeps those stores coherent.
                    // Duplicate saves are harmless (same cursor).
                    let _ = self.store.save(&principal, &stream, cursor).await;
                    let _ = self
                        .event_tx
                        .send(SyncEvent::Applied {
                            stream: stream.clone(),
                            cursor,
                        })
                        .await;
                    return Ok(self.pop_buffered_next(&stream).await);
                }
                debug!(%stream, cursor, "update ready, awaiting application");
                let _ = self
                    .event_tx
                    .send(SyncEvent::UpdateReady {
                        stream: stream.clone(),
                        cursor,
                        update: Box::new(update),
                    })
                    .await;
                Ok(self.pop_buffered_next(&stream).await)
            }
        }
    }

    async fn pop_buffered_next(&self, stream: &str) -> Option<Update> {
        let mut state = self.state.lock().await;
        let applied = state.cursors.get(stream);
        let delivered = state.delivered.get(stream).copied().unwrap_or(0);
        let expected = applied.max(delivered).saturating_add(1);
        state
            .pending
            .get_mut(stream)
            .and_then(|m| m.remove(&expected))
    }

    /// Persist the checkpoint after the app has applied the update and its
    /// required refreshes. Must be called with the exact `cursor` from
    /// `UpdateReady`; out-of-order or duplicate acks never move state
    /// backward.
    pub async fn acknowledge(&self, stream: &str, cursor: u64) -> Result<(), TinkleError> {
        let (principal, previous, decision) = {
            let mut state = self.state.lock().await;
            let prev = state.cursors.get(stream);
            // Duplicate ack after a successful commit is harmless.
            if cursor == prev {
                return Ok(());
            }
            match state.cursors.decision(stream, cursor) {
                CursorDecision::Next => {
                    state.cursors.commit(stream, cursor)?;
                    (state.principal.clone(), prev, true)
                }
                _ => {
                    let applied = state.cursors.get(stream);
                    if cursor > applied {
                        let expected = applied.saturating_add(1);
                        let stream_owned = stream.to_string();
                        drop(state);
                        return Err(TinkleError::Sync(
                            tinkle_schema::sync::SyncError::Gap {
                                stream: stream_owned,
                                expected,
                                got: cursor,
                            }
                            .to_string(),
                        ));
                    }
                    drop(state);
                    if cursor < applied {
                        let _ = self
                            .event_tx
                            .send(SyncEvent::Stale {
                                stream: stream.to_string(),
                                applied,
                                got: cursor,
                            })
                            .await;
                    }
                    return Ok(());
                }
            }
        };
        let _ = (previous, decision);
        if let Err(e) = self.store.save(&principal, stream, cursor).await {
            let error = e.to_string();
            let mut state = self.state.lock().await;
            if state.principal == principal && state.cursors.get(stream) == cursor {
                state.cursors.hydrate(stream, previous);
            }
            drop(state);
            let _ = self
                .event_tx
                .send(SyncEvent::CheckpointFailed {
                    stream: stream.to_string(),
                    cursor,
                    error,
                })
                .await;
            return Err(TinkleError::CursorStore(format!(
                "acknowledge save failed: {e}"
            )));
        }
        let _ = self
            .event_tx
            .send(SyncEvent::Applied {
                stream: stream.to_string(),
                cursor,
            })
            .await;
        Ok(())
    }

    pub async fn next_event(&self) -> Option<SyncEvent> {
        self.event_rx.lock().await.recv().await
    }

    pub(crate) async fn on_disconnect(&self) {
        self.interests.lock().await.subscribed = false;
        let mut state = self.state.lock().await;
        state.delivered.clear();
        state.pending.clear();
        state.status.clear();
        state.heads.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex as StdMutex};

    use async_trait::async_trait;

    struct MemStore {
        inner: StdMutex<HashMap<(String, String), u64>>,
    }

    impl MemStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: StdMutex::new(HashMap::new()),
            })
        }
    }

    #[async_trait]
    impl CursorStore for MemStore {
        async fn load(&self, p: &str, s: &str) -> Result<Option<u64>, TinkleError> {
            Ok(self
                .inner
                .lock()
                .unwrap()
                .get(&(p.to_string(), s.to_string()))
                .copied())
        }
        async fn save(&self, p: &str, s: &str, c: u64) -> Result<(), TinkleError> {
            self.inner
                .lock()
                .unwrap()
                .insert((p.to_string(), s.to_string()), c);
            Ok(())
        }
        async fn remove_stream(&self, p: &str, s: &str) -> Result<(), TinkleError> {
            self.inner
                .lock()
                .unwrap()
                .remove(&(p.to_string(), s.to_string()));
            Ok(())
        }
        async fn clear_principal(&self, p: &str) -> Result<(), TinkleError> {
            self.inner.lock().unwrap().retain(|(pp, _), _| pp != p);
            Ok(())
        }
    }

    fn omitted(stream: &str, cursor: u64) -> Update {
        Update {
            event_id: String::new(),
            stream: stream.into(),
            cursor,
            occurred_at: None,
            source_seq: None,
            source_revision: None,
            payload: Some(tinkle_schema::proto::update::Payload::ContentOmitted(
                tinkle_schema::proto::ContentOmitted {},
            )),
        }
    }

    #[tokio::test]
    async fn update_ready_requires_acknowledge() {
        let store = MemStore::new();
        let mgr = SyncManager::new("user:1", store.clone());
        mgr.handle_update(omitted("user:1", 1)).await.unwrap();
        // Checkpoint not yet persisted.
        assert_eq!(store.load("user:1", "user:1").await.unwrap(), None);
        let evt = mgr.next_event().await.unwrap();
        assert!(matches!(evt, SyncEvent::UpdateReady { cursor: 1, .. }));
        mgr.acknowledge("user:1", 1).await.unwrap();
        assert_eq!(store.load("user:1", "user:1").await.unwrap(), Some(1));
        // Duplicate ack is harmless.
        mgr.acknowledge("user:1", 1).await.unwrap();
    }

    #[tokio::test]
    async fn gap_does_not_advance() {
        let store = MemStore::new();
        let mgr = SyncManager::new("user:1", store);
        assert!(mgr.handle_update(omitted("user:1", 2)).await.is_err());
        let evt = mgr.next_event().await.unwrap();
        assert!(matches!(
            evt,
            SyncEvent::Gap {
                expected: 1,
                got: 2,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn consecutive_updates_before_any_acknowledge() {
        let store = MemStore::new();
        let mgr = SyncManager::new("user:1", store.clone());
        for cursor in 1..=3 {
            mgr.handle_update(omitted("user:1", cursor)).await.unwrap();
        }
        for expected in 1..=3 {
            let evt = mgr.next_event().await.unwrap();
            assert!(
                matches!(evt, SyncEvent::UpdateReady { cursor, .. } if cursor == expected),
                "expected UpdateReady {expected}, got {evt:?}"
            );
        }
        assert_eq!(store.load("user:1", "user:1").await.unwrap(), None);
        for cursor in 1..=3 {
            mgr.acknowledge("user:1", cursor).await.unwrap();
            // Drain the Applied event so the channel stays empty.
            let evt = mgr.next_event().await.unwrap();
            assert!(matches!(evt, SyncEvent::Applied { cursor: c, .. } if c == cursor));
        }
        assert_eq!(store.load("user:1", "user:1").await.unwrap(), Some(3));
    }

    #[tokio::test]
    async fn acknowledge_skipped_cursor_returns_gap() {
        let store = MemStore::new();
        let mgr = SyncManager::new("user:1", store);
        let err = mgr.acknowledge("user:1", 2).await.unwrap_err();
        assert!(matches!(err, TinkleError::Sync(_)));
        assert!(err.to_string().contains("gap"));
    }
}
