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

/// Pre-authentication principal placeholder. Resolution from this value to
/// the authenticated `user:<id>` preserves in-flight per-stream progress
/// (see [`SyncManager::resolve_principal`]); any other principal change is
/// an account switch and resets sync state.
pub(crate) const UNKNOWN_PRINCIPAL: &str = "unknown";

/// Capacity of the app-visible event queue. Durable delivery events
/// (`UpdateReady`, `Gap`, `RebuildRequired`, `CheckpointFailed`) apply
/// backpressure when full; informational events (`Progress`, `Duplicate`,
/// `Stale`, `Connectivity`, `Applied`) are dropped instead. Durable state
/// is always queryable via cursors, so drops never lose sync progress.
const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Internal recovery signal from the sync pipeline to the connection
/// supervisor. Hints only: stream states persist in the manager, and the
/// supervisor watchdog covers lost signals. Never blocks the pump.
#[derive(Debug, Clone)]
pub(crate) enum RecoverySignal {
    /// Notification pump exited; the transport is presumed dead.
    PumpDead,
    /// Missing update; run bounded recovery from the durable cursor.
    Gap { stream: String },
    /// Stream sequence terminated; reconstruct from the origin.
    Rebuild { stream: String },
    /// Durable checkpoint save failed; retry recovery for the stream.
    CheckpointFailed { stream: String },
}

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
    /// Serializes durable checkpoint writes from concurrent `acknowledge`
    /// calls so commits persist in commit order and a newer cursor is never
    /// overwritten by an older one. Held only across the in-memory commit +
    /// store save; store I/O still stays outside `state` so the notification
    /// pump is never blocked.
    checkpoint_save: Mutex<()>,
    event_tx: mpsc::Sender<SyncEvent>,
    event_rx: Mutex<mpsc::Receiver<SyncEvent>>,
    interests: Mutex<Interests>,
    /// Best-effort recovery hints to the connection supervisor. `None` on
    /// the manual path (no supervisor); set by the supervisor at `start`.
    /// Emitted with `try_send`: a full or missing receiver never blocks
    /// the pump.
    signals: Mutex<Option<mpsc::Sender<RecoverySignal>>>,
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
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
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
            checkpoint_save: Mutex::new(()),
            event_tx: tx,
            event_rx: Mutex::new(rx),
            interests: Mutex::new(Interests::default()),
            signals: Mutex::new(None),
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

    pub(crate) async fn set_signal_tx(&self, tx: mpsc::Sender<RecoverySignal>) {
        *self.signals.lock().await = Some(tx);
    }

    /// Best-effort hint to the supervisor. Drops when no supervisor is
    /// attached or its queue is full; stream states persist, so the
    /// watchdog and subsequent signals cover the loss.
    pub(crate) async fn emit_signal(&self, signal: RecoverySignal) {
        if let Some(tx) = self.signals.lock().await.clone() {
            let _ = tx.try_send(signal);
        }
    }

    /// Durable applied cursor for `stream` (persisted checkpoint, not
    /// volatile delivered progress). Recovery resumes from here.
    pub(crate) async fn applied_cursor(&self, stream: &str) -> u64 {
        self.state.lock().await.cursors.get(stream)
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

    /// Last fully delivered cursor for `stream`: `max(applied, delivered)`.
    /// Catch-up advances `delivered` without persisting, so follows must
    /// resume from here — not from the pre-catch-up store cursor — to avoid
    /// replaying already-delivered updates as duplicates.
    pub(crate) async fn effective_cursor(&self, stream: &str) -> u64 {
        let state = self.state.lock().await;
        state
            .cursors
            .get(stream)
            .max(state.delivered.get(stream).copied().unwrap_or(0))
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

    /// Resolve the pre-authentication placeholder to the authenticated
    /// principal, preserving per-stream cursors, delivered progress, and
    /// buffered live updates. Updates validated before resolution (e.g. the
    /// retained suffix replaying on a probe-as-follow) stay valid and
    /// acknowledgeable after hydration raises the applied cursor.
    ///
    /// Any transition from a concrete principal is an account switch and
    /// keeps the hard-reset semantics of [`Self::set_principal`].
    pub(crate) async fn resolve_principal(&self, principal: String) {
        let from_known = {
            let state = self.state.lock().await;
            if state.principal == principal {
                return;
            }
            state.principal != UNKNOWN_PRINCIPAL
        };
        if from_known {
            self.set_principal(principal).await;
            return;
        }
        let mut state = self.state.lock().await;
        if state.principal == principal {
            return;
        }
        if state.principal != UNKNOWN_PRINCIPAL {
            drop(state);
            self.set_principal(principal).await;
            return;
        }
        state.principal = principal;
        state.status.clear();
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
        self.emit_signal(RecoverySignal::Rebuild {
            stream: stream.to_string(),
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
                    .send(SyncEvent::RebuildRequired {
                        stream: stream.clone(),
                        head,
                    })
                    .await;
                self.emit_signal(RecoverySignal::Rebuild { stream }).await;
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
                // Informational: droppable under pressure; cursors already
                // cover the position.
                let _ = self.event_tx.try_send(SyncEvent::Duplicate {
                    stream: stream.clone(),
                    cursor,
                });
                Ok(None)
            }
            Kind::Stale { applied } => {
                let _ = self.event_tx.try_send(SyncEvent::Stale {
                    stream: stream.clone(),
                    applied,
                    got: cursor,
                });
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
                self.emit_signal(RecoverySignal::Gap {
                    stream: stream.clone(),
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
                    let ops =
                        match crate::cache_projector::project_update_to_ops(&principal, &update) {
                            Ok(ops) => ops,
                            Err(projection) => {
                                // Sync validation accepts shapes the projector
                                // cannot persist (e.g. non-positive message_id).
                                // Without an event the update would be dropped
                                // silently and the checkpoint would stall; surface
                                // it for recovery like any other apply failure.
                                let _ = self
                                    .event_tx
                                    .send(SyncEvent::CheckpointFailed {
                                        stream: stream.clone(),
                                        cursor,
                                        error: projection.clone(),
                                    })
                                    .await;
                                self.emit_signal(RecoverySignal::CheckpointFailed {
                                    stream: stream.clone(),
                                })
                                .await;
                                return Err(TinkleError::Sync(projection));
                            }
                        };
                    // `Storage` is synchronous rusqlite behind a mutex; run
                    // it on the blocking pool so SQLite contention cannot
                    // stall the Tokio notification pump.
                    let batch_result = {
                        let sqlite_task = sqlite.clone();
                        let principal_task = principal.clone();
                        let stream_task = stream.clone();
                        tokio::task::spawn_blocking(move || {
                            sqlite_task.apply_sync_batch(
                                &principal_task,
                                &stream_task,
                                cursor,
                                applied,
                                &ops,
                            )
                        })
                        .await
                        .map_err(|e| {
                            TinkleError::CursorStore(format!("sync apply task failed: {e}"))
                        })?
                    };
                    if let Err(e) = batch_result {
                        // Scope moved under us (concurrent apply) or rebuild:
                        // hydrate in-memory cursor and surface for recovery.
                        if matches!(e, tinkle_storage::StorageError::ScopeMismatch { .. }) {
                            let current: Option<u64> = {
                                let sqlite_task = sqlite.clone();
                                let principal_task = principal.clone();
                                let stream_task = stream.clone();
                                tokio::task::spawn_blocking(move || {
                                    sqlite_task.load_checkpoint(&principal_task, &stream_task)
                                })
                                .await
                                .ok()
                                .and_then(|r| r.ok())
                                .flatten()
                            };
                            if let Some(current) = current {
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
                        self.emit_signal(RecoverySignal::CheckpointFailed {
                            stream: stream.clone(),
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
                    // Duplicate saves are harmless (same cursor). Applied is
                    // informational in auto mode — the atomic commit above is
                    // the durable truth — so it is droppable under pressure;
                    // hosts query cursors for authoritative progress.
                    let _ = self.store.save(&principal, &stream, cursor).await;
                    let _ = self.event_tx.try_send(SyncEvent::Applied {
                        stream: stream.clone(),
                        cursor,
                    });
                    // ReadStateInvalidated carries no cacheable body, so the
                    // atomic commit above advances the checkpoint without
                    // refreshing unread/read state. Surface the payload so FFI
                    // hosts can trigger a GetDialogs/read-state refresh;
                    // without this SQLite-mode clients would see only
                    // `Applied` and serve stale dialogs.
                    if matches!(
                        update.payload.as_ref(),
                        Some(tinkle_schema::proto::update::Payload::ReadStateInvalidated(
                            _
                        ))
                    ) {
                        let _ = self
                            .event_tx
                            .send(SyncEvent::UpdateReady {
                                stream: stream.clone(),
                                cursor,
                                update: Box::new(update.clone()),
                            })
                            .await;
                    }
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
        // Serialize checkpoint writes so concurrent acks persist in commit
        // order: without this, a newer cursor could be saved first and then
        // overwritten by an older one, causing replay on restart.
        let _save_guard = self.checkpoint_save.lock().await;
        let (principal, previous) = {
            let mut state = self.state.lock().await;
            let prev = state.cursors.get(stream);
            // Duplicate ack after a successful commit is harmless.
            if cursor == prev {
                return Ok(());
            }
            // The ack must reference an update that was actually delivered as
            // `UpdateReady`; otherwise the caller could advance the durable
            // checkpoint without ever applying the update.
            let delivered = state.delivered.get(stream).copied().unwrap_or(0);
            if cursor > delivered {
                let expected = prev.saturating_add(1);
                let stream_owned = stream.to_string();
                drop(state);
                return Err(TinkleError::SyncError(
                    tinkle_schema::sync::SyncError::Gap {
                        stream: stream_owned,
                        expected,
                        got: cursor,
                    },
                ));
            }
            match state.cursors.decision(stream, cursor) {
                CursorDecision::Next => {
                    state.cursors.commit(stream, cursor)?;
                    (state.principal.clone(), prev)
                }
                _ => {
                    let applied = state.cursors.get(stream);
                    if cursor > applied {
                        let expected = applied.saturating_add(1);
                        let stream_owned = stream.to_string();
                        drop(state);
                        return Err(TinkleError::SyncError(
                            tinkle_schema::sync::SyncError::Gap {
                                stream: stream_owned,
                                expected,
                                got: cursor,
                            },
                        ));
                    }
                    drop(state);
                    if cursor < applied {
                        let _ = self.event_tx.try_send(SyncEvent::Stale {
                            stream: stream.to_string(),
                            applied,
                            got: cursor,
                        });
                    }
                    return Ok(());
                }
            }
        };
        if let Err(e) = self.store.save(&principal, stream, cursor).await {
            let error = e.to_string();
            let mut state = self.state.lock().await;
            if state.principal == principal && state.cursors.get(stream) == cursor {
                state.cursors.hydrate(stream, previous);
                // Roll delivered back as well: the update must be retried,
                // and leaving delivered ahead would classify the redelivery
                // as duplicate/stale and suppress it.
                if previous == 0 {
                    state.delivered.remove(stream);
                } else {
                    state.delivered.insert(stream.to_string(), previous);
                }
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
            self.emit_signal(RecoverySignal::CheckpointFailed {
                stream: stream.to_string(),
            })
            .await;
            return Err(TinkleError::CursorStore(format!(
                "acknowledge save failed: {e}"
            )));
        }
        // The caller just acknowledged: Applied confirms what it already
        // knows, so it is droppable under pressure.
        let _ = self.event_tx.try_send(SyncEvent::Applied {
            stream: stream.to_string(),
            cursor,
        });
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
    async fn resolve_placeholder_preserves_inflight_progress() {
        // Probe-as-follow replays before the principal is known: progress
        // must survive resolution and stay acknowledgeable afterwards.
        let store = MemStore::new();
        let mgr = SyncManager::new(UNKNOWN_PRINCIPAL, store.clone());
        mgr.handle_update(omitted("user:42", 1)).await.unwrap();
        mgr.handle_update(omitted("user:42", 2)).await.unwrap();
        mgr.resolve_principal("user:42".to_string()).await;
        // Pre-resolution backlog still queued, cursor intact.
        assert_eq!(mgr.effective_cursor("user:42").await, 2);
        for expected in 1..=2u64 {
            let evt = mgr.next_event().await.unwrap();
            assert!(
                matches!(evt, SyncEvent::UpdateReady { cursor, .. } if cursor == expected),
                "expected UpdateReady {expected}, got {evt:?}"
            );
        }
        for expected in 1..=2u64 {
            mgr.acknowledge("user:42", expected).await.unwrap();
            let evt = mgr.next_event().await.unwrap();
            assert!(
                matches!(evt, SyncEvent::Applied { cursor, .. } if cursor == expected),
                "expected Applied {expected}, got {evt:?}"
            );
        }
        assert_eq!(store.load("user:42", "user:42").await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn resolve_after_hydrate_dedupes_applied_prefix() {
        // Cold start with a stored checkpoint: the retained suffix replays
        // from origin, hydration raises applied, already-applied cursors
        // classify stale while new ones stay acknowledgeable.
        let store = MemStore::new();
        store.save("user:42", "user:42", 1).await.unwrap();
        let mgr = SyncManager::new(UNKNOWN_PRINCIPAL, store.clone());
        mgr.handle_update(omitted("user:42", 1)).await.unwrap();
        mgr.handle_update(omitted("user:42", 2)).await.unwrap();
        mgr.resolve_principal("user:42".to_string()).await;
        mgr.hydrate_from_store(&["user:42".to_string()])
            .await
            .unwrap();
        assert_eq!(mgr.effective_cursor("user:42").await, 2);
        // Cursor 1 was already applied: ack is a harmless no-op...
        mgr.acknowledge("user:42", 1).await.unwrap();
        // ...while cursor 2 commits.
        mgr.acknowledge("user:42", 2).await.unwrap();
        assert_eq!(store.load("user:42", "user:42").await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn set_principal_resets_on_account_switch() {
        let store = MemStore::new();
        let mgr = SyncManager::new("user:1", store);
        mgr.handle_update(omitted("user:1", 1)).await.unwrap();
        mgr.set_principal("user:2".to_string()).await;
        assert_eq!(mgr.effective_cursor("user:1").await, 0);
        assert_eq!(mgr.effective_cursor("user:2").await, 0);
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
    async fn large_backlog_flows_with_concurrent_consumer() {
        // More updates than any bounded queue: with a draining consumer,
        // background catch-up must complete, never deadlock. The checkpoint
        // still advances only through explicit acknowledgment.
        const N: u64 = 1200;
        let store = MemStore::new();
        let mgr = SyncManager::new("user:1", store.clone());
        let drain = mgr.clone();
        let consumer = tokio::spawn(async move {
            let mut ready = 0u64;
            while ready < N {
                match drain.next_event().await {
                    Some(SyncEvent::UpdateReady { .. }) => ready += 1,
                    Some(_) => {}
                    None => break,
                }
            }
            ready
        });
        for cursor in 1..=N {
            mgr.handle_update(omitted("user:1", cursor)).await.unwrap();
        }
        let ready = tokio::time::timeout(std::time::Duration::from_secs(30), consumer)
            .await
            .expect("backlog must not stall")
            .unwrap();
        assert_eq!(ready, N);
        assert_eq!(store.load("user:1", "user:1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn acknowledge_skipped_cursor_returns_gap() {
        let store = MemStore::new();
        let mgr = SyncManager::new("user:1", store);
        let err = mgr.acknowledge("user:1", 2).await.unwrap_err();
        assert!(matches!(
            err,
            TinkleError::Sync(_) | TinkleError::SyncError(_)
        ));
        assert!(err.is_gap());
        assert!(err.to_string().to_ascii_lowercase().contains("gap"));
    }

    #[tokio::test]
    async fn acknowledge_without_delivery_is_rejected() {
        let store = MemStore::new();
        let mgr = SyncManager::new("user:1", store.clone());
        // No `UpdateReady` was ever delivered: the ack must not advance the
        // durable checkpoint without the update being applied.
        let err = mgr.acknowledge("user:1", 1).await.unwrap_err();
        assert!(err.is_gap());
        assert_eq!(store.load("user:1", "user:1").await.unwrap(), None);
    }

    struct FailingStore;

    #[async_trait]
    impl CursorStore for FailingStore {
        async fn load(&self, _p: &str, _s: &str) -> Result<Option<u64>, TinkleError> {
            Ok(None)
        }
        async fn save(&self, _p: &str, _s: &str, _c: u64) -> Result<(), TinkleError> {
            Err(TinkleError::CursorStore("boom".into()))
        }
        async fn remove_stream(&self, _p: &str, _s: &str) -> Result<(), TinkleError> {
            Ok(())
        }
        async fn clear_principal(&self, _p: &str) -> Result<(), TinkleError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn failed_ack_rolls_back_delivered_so_retry_redelivers() {
        let store = Arc::new(FailingStore);
        let mgr = SyncManager::new("user:1", store);
        mgr.handle_update(omitted("user:1", 1)).await.unwrap();
        assert!(matches!(
            mgr.next_event().await.unwrap(),
            SyncEvent::UpdateReady { cursor: 1, .. }
        ));
        assert!(mgr.acknowledge("user:1", 1).await.is_err());
        assert!(matches!(
            mgr.next_event().await.unwrap(),
            SyncEvent::CheckpointFailed { cursor: 1, .. }
        ));
        // Redelivery must surface `UpdateReady` again, not Duplicate/Stale.
        mgr.handle_update(omitted("user:1", 1)).await.unwrap();
        assert!(matches!(
            mgr.next_event().await.unwrap(),
            SyncEvent::UpdateReady { cursor: 1, .. }
        ));
    }

    struct SlowFirstStore {
        inner: StdMutex<HashMap<(String, String), u64>>,
    }

    #[async_trait]
    impl CursorStore for SlowFirstStore {
        async fn load(&self, p: &str, s: &str) -> Result<Option<u64>, TinkleError> {
            Ok(self
                .inner
                .lock()
                .unwrap()
                .get(&(p.to_string(), s.to_string()))
                .copied())
        }
        async fn save(&self, p: &str, s: &str, c: u64) -> Result<(), TinkleError> {
            if c == 1 {
                // Force the race: the older cursor's save lands last unless
                // checkpoint writes are serialized in commit order.
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            }
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

    #[tokio::test]
    async fn concurrent_acks_persist_newest_last() {
        let store = Arc::new(SlowFirstStore {
            inner: StdMutex::new(HashMap::new()),
        });
        let mgr = SyncManager::new("user:1", store.clone());
        mgr.handle_update(omitted("user:1", 1)).await.unwrap();
        mgr.handle_update(omitted("user:1", 2)).await.unwrap();
        assert!(matches!(
            mgr.next_event().await.unwrap(),
            SyncEvent::UpdateReady { cursor: 1, .. }
        ));
        assert!(matches!(
            mgr.next_event().await.unwrap(),
            SyncEvent::UpdateReady { cursor: 2, .. }
        ));
        let m1 = mgr.clone();
        let h1 = tokio::spawn(async move { m1.acknowledge("user:1", 1).await });
        // Let ack(1) commit and enter its slow save before ack(2) starts.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        mgr.acknowledge("user:1", 2).await.unwrap();
        h1.await.unwrap().unwrap();
        let mut applied = Vec::new();
        applied.push(mgr.next_event().await.unwrap());
        applied.push(mgr.next_event().await.unwrap());
        assert!(
            applied
                .iter()
                .all(|e| matches!(e, SyncEvent::Applied { .. })),
            "expected two Applied events, got {applied:?}"
        );
        assert_eq!(store.load("user:1", "user:1").await.unwrap(), Some(2));
    }

    fn message_refresh(stream: &str, cursor: u64, message_id: i64) -> Update {
        use tinkle_schema::proto::{
            peer_input::Target, update::Payload as P, Message, MessageContext, MessageRefresh,
            PeerInput,
        };
        Update {
            event_id: "11111111-1111-4111-8111-111111111111".into(),
            stream: stream.into(),
            cursor,
            occurred_at: None,
            source_seq: None,
            source_revision: None,
            payload: Some(P::MessageRefresh(MessageRefresh {
                message: Some(Message {
                    message_id,
                    peer: Some(PeerInput {
                        target: Some(Target::ChatId(7)),
                    }),
                    from_id: 42,
                    date: None,
                    message: "hello".into(),
                    media: None,
                    entities: vec![],
                    reply_to: None,
                    edit_date: None,
                    reactions: None,
                    replies: None,
                    pinned: false,
                    ttl_period: 0,
                }),
                random_id: None,
                context: Some(MessageContext {
                    conversation_key: "room:7".into(),
                    order_key: tinkle_schema::sync::position_order(cursor, None),
                    resolve_user_id: None,
                    resolve_conversation: false,
                }),
            })),
        }
    }

    #[tokio::test]
    async fn sqlite_auto_apply_commits_cache_and_checkpoint() {
        let store = MemStore::new();
        let mgr = SyncManager::new("user:1", store);
        let sqlite = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
        mgr.set_sqlite_storage(sqlite.clone()).await;
        mgr.handle_update(message_refresh("room:7", 1, 10))
            .await
            .unwrap();
        let evt = mgr.next_event().await.unwrap();
        assert!(
            matches!(evt, SyncEvent::Applied { cursor: 1, .. }),
            "got {evt:?}"
        );
        assert_eq!(sqlite.load_checkpoint("user:1", "room:7").unwrap(), Some(1));
        let rows = sqlite.get_messages("user:1", "room:7", 10, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].message_id, 10);
    }

    #[tokio::test]
    async fn sqlite_projection_failure_surfaces_checkpoint_failed() {
        let store = MemStore::new();
        let mgr = SyncManager::new("user:1", store);
        let sqlite = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
        mgr.set_sqlite_storage(sqlite.clone()).await;
        // Passes sync validation but the projector rejects message_id 0.
        let err = mgr
            .handle_update(message_refresh("room:7", 1, 0))
            .await
            .unwrap_err();
        assert!(matches!(err, TinkleError::Sync(_)));
        let evt = mgr.next_event().await.unwrap();
        assert!(
            matches!(evt, SyncEvent::CheckpointFailed { cursor: 1, .. }),
            "got {evt:?}"
        );
        // Nothing committed, nothing cached: recovery can retry cleanly.
        assert_eq!(sqlite.load_checkpoint("user:1", "room:7").unwrap(), None);
        assert!(sqlite
            .get_messages("user:1", "room:7", 10, None)
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn rebuild_clears_native_cache_until_snapshot_recommits() {
        let store = MemStore::new();
        let mgr = SyncManager::new("user:1", store);
        let sqlite = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
        mgr.set_sqlite_storage(sqlite.clone()).await;
        mgr.handle_update(message_refresh("room:7", 1, 10))
            .await
            .unwrap();
        assert!(matches!(
            mgr.next_event().await.unwrap(),
            SyncEvent::Applied { .. }
        ));
        assert_eq!(
            sqlite
                .get_messages("user:1", "room:7", 10, None)
                .unwrap()
                .len(),
            1
        );
        mgr.handle_expired("room:7", None).await;
        assert!(matches!(
            mgr.next_event().await.unwrap(),
            SyncEvent::RebuildRequired { .. }
        ));
        // Stale rows from discarded history are not readable offline.
        assert!(sqlite
            .get_messages("user:1", "room:7", 10, None)
            .unwrap()
            .is_empty());
        assert!(sqlite.list_dialogs("user:1", 10).unwrap().is_empty());
        assert_eq!(sqlite.load_checkpoint("user:1", "room:7").unwrap(), None);
    }
}
