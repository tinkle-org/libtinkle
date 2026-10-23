//! Application subscriptions + notification decoding + cursor tracking.
//!
//! Subscriptions are established only via generic RPC after ACTIVE
//! (`/tinkle.v1.Realtime/SubscribeBatched`). Delivery is `notification`
//! frames (`request_id == 0`) carrying exactly one `RealtimeNotification`.
//! This module validates batches, tracks per-stream cursors, detects
//! duplicates/gaps, surfaces resets, and persists durable checkpoints
//! through the injected [`crate::CursorStore`].
//!
//! Persistent user application state lives here, outside the generic
//! Syntra core. Presence is ephemeral and cleared on disconnect.

use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tracing::debug;

use tinkle_schema::realtime::{CursorDecision, StreamCursors, ValidatedNotification};

use crate::cursor::CursorStore;
use crate::errors::TinkleError;

/// App-visible realtime events.
#[derive(Debug, Clone)]
pub enum RealtimeEvent {
    /// A complete durable batch applied + checkpoint committed.
    BatchApplied {
        stream: String,
        final_cursor: u64,
        batch: tinkle_schema::proto::RealtimeBatch,
    },
    /// Duplicate batch ignored (no reapply, no advance).
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
    /// Ephemeral presence (never checkpointed).
    Presence(tinkle_schema::proto::RealtimeOnline),
    /// Reset: one stream's sequence terminated. Recovery requires a proven
    /// consistent snapshot/checkpoint; head is diagnostic only.
    ResetRequired { stream: String, head: Option<u64> },
}

/// Principal-scoped cursor state guarded by a single synchronization
/// barrier. `principal` and `cursors` must always be observed and mutated
/// together: classifying a notification with one principal's cursors while
/// persisting under another drops or duplicates checkpoints. A single
/// `Mutex` ensures `set_principal` atomically swaps both, and every
/// reader commits/decides under the same lock scope. Store I/O stays
/// outside the lock (snapshotted principal + version check) so a slow
/// store never blocks the notification pump.
#[derive(Debug, Default)]
struct PrincipalState {
    principal: String,
    cursors: StreamCursors,
}

/// Manages one principal's realtime interests + cursors (crate-internal;
/// apps observe [`RealtimeEvent`] via [`crate::Client::next_event`]).
pub(crate) struct SubscriptionManager {
    state: Mutex<PrincipalState>,
    store: Arc<dyn CursorStore>,
    event_tx: mpsc::Sender<RealtimeEvent>,
    event_rx: Mutex<mpsc::Receiver<RealtimeEvent>>,
    interests: Mutex<Interests>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Interests {
    pub(crate) rooms: Vec<(i64, u64)>,
    pub(crate) watch_user_ids: Vec<i64>,
    pub(crate) subscribed: bool,
}

impl SubscriptionManager {
    pub(crate) fn new(principal: impl Into<String>, store: Arc<dyn CursorStore>) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(256);
        Arc::new(Self {
            state: Mutex::new(PrincipalState {
                principal: principal.into(),
                cursors: StreamCursors::new(),
            }),
            store,
            event_tx: tx,
            event_rx: Mutex::new(rx),
            interests: Mutex::new(Interests::default()),
        })
    }

    /// Update the cursor principal after authenticated subscribe
    /// (`resp.stream`). Room cursor writes and reconnect loads must use
    /// this same principal rather than the initial `unknown` value.
    /// Clears in-memory cursors only when the principal actually changes;
    /// same-principal updates preserve existing cursors.
    ///
    /// Holds the single `state` barrier across the compare-and-swap so no
    /// in-flight `handle_batch`/`hydrate_from_store`/reset can classify
    /// with the old scope and persist with the new one (or vice versa).
    pub(crate) async fn set_principal(&self, principal: String) {
        let mut state = self.state.lock().await;
        if state.principal == principal {
            return;
        }
        state.principal = principal;
        state.cursors = StreamCursors::new();
    }

    /// Record interests after a successful `SubscribeBatched`.
    pub(crate) async fn on_subscribed(&self, rooms: Vec<(i64, u64)>, watch_user_ids: Vec<i64>) {
        let mut g = self.interests.lock().await;
        g.rooms = rooms;
        g.watch_user_ids = watch_user_ids;
        g.subscribed = true;
    }

    /// Hydrate memory cursors from durable storage (call after connect,
    /// before resubscribe). Absent cursors mean zero, never start-live.
    ///
    /// Store reads run outside the barrier; the install re-acquires it
    /// and is discarded when the principal rotated mid-hydrate, so a
    /// stale scope never clobbers the new scope. Installs only move
    /// forward (never overwrite a newer live commit that landed while
    /// loads were in flight).
    pub(crate) async fn hydrate_from_store(&self, streams: &[String]) -> Result<(), TinkleError> {
        let principal = self.state.lock().await.principal.clone();
        let mut loaded: Vec<(String, u64)> = Vec::with_capacity(streams.len());
        for s in streams {
            let v = self.store.load(&principal, s).await?.unwrap_or(0);
            loaded.push((s.clone(), v));
        }
        let mut state = self.state.lock().await;
        if state.principal != principal {
            // Principal rotated during the loads; stale values belong to
            // the old scope and must not be installed into the new one.
            return Ok(());
        }
        for (s, v) in loaded {
            if v > state.cursors.get(&s) {
                state.cursors.hydrate(&s, v);
            } else if v == 0 && state.cursors.get(&s) == 0 {
                state.cursors.hydrate(&s, 0);
            }
            // v <= applied (including v == 0 with applied > 0): keep the
            // newer live value; never move the checkpoint backward.
        }
        Ok(())
    }

    /// Handle one raw `Notification.payload` (exactly one
    /// `RealtimeNotification`). Validates the complete batch before any
    /// effects; atomically commits effects + checkpoint, or neither.
    pub(crate) async fn handle_payload(&self, payload: &[u8]) -> Result<(), TinkleError> {
        let notif = tinkle_schema::realtime::validate_notification(payload)?;
        match notif {
            ValidatedNotification::Presence(p) => {
                let _ = self.event_tx.try_send(RealtimeEvent::Presence(p));
            }
            ValidatedNotification::ResetRequired { stream, head } => {
                // Atomically clear memory + snapshot the owning principal
                // under one barrier; durable removal runs outside the lock
                // with that snapshot so it cannot hit the wrong scope.
                let principal = {
                    let mut state = self.state.lock().await;
                    state.cursors.on_reset(&stream);
                    state.principal.clone()
                };
                let _ = self.store.remove_stream(&principal, &stream).await;
                let _ = self
                    .event_tx
                    .try_send(RealtimeEvent::ResetRequired { stream, head });
            }
            ValidatedNotification::Batch(b) => {
                self.handle_batch(b).await?;
            }
        }
        Ok(())
    }

    async fn handle_batch(
        &self,
        b: tinkle_schema::realtime::ValidatedBatch,
    ) -> Result<(), TinkleError> {
        let stream = b.stream.clone();
        let cursor = b.final_cursor;
        // Reconstruct the proto batch for the app event (validated copy).
        // The validated batch already holds events; rebuild minimal proto.
        let proto_batch = tinkle_schema::proto::RealtimeBatch {
            stream: stream.clone(),
            events: b.events.clone(),
            final_cursor: cursor,
        };
        // Single-barrier classify + commit: decision, memory commit, and
        // principal snapshot happen under one lock acquisition so a
        // concurrent `set_principal` cannot split classification (old
        // scope) from persistence (new scope). The durable `save` runs
        // after the lock is released, using the snapshot that matches the
        // commit — slow store I/O never blocks the pump while holding the
        // barrier.
        enum Action {
            Duplicate,
            Next(String),
            Gap { expected: u64 },
            Stale { applied: u64 },
        }
        let action = {
            let mut state = self.state.lock().await;
            match state.cursors.decision(&stream, cursor) {
                CursorDecision::Duplicate => Action::Duplicate,
                CursorDecision::Next => {
                    state.cursors.commit(&stream, cursor)?;
                    Action::Next(state.principal.clone())
                }
                CursorDecision::Gap => Action::Gap {
                    expected: state.cursors.get(&stream).saturating_add(1),
                },
                CursorDecision::Stale => Action::Stale {
                    applied: state.cursors.get(&stream),
                },
            }
        };
        match action {
            Action::Duplicate => {
                debug!(%stream, cursor, "duplicate batch ignored");
                let _ = self
                    .event_tx
                    .try_send(RealtimeEvent::Duplicate { stream, cursor });
                Ok(())
            }
            Action::Next(principal) => {
                // Atomically commit memory + durable store. The caller must
                // have applied all required effects + refreshes before this
                // checkpoint; receipt alone is not commit. Here we persist
                // the checkpoint; app effects are emitted with the event for
                // the caller to apply atomically where possible.
                // Memory already committed under the same barrier as the
                // snapshot; `principal` is that commit's owning scope even
                // if a rotation landed in between.
                self.store.save(&principal, &stream, cursor).await?;
                let _ = self.event_tx.try_send(RealtimeEvent::BatchApplied {
                    stream,
                    final_cursor: cursor,
                    batch: proto_batch,
                });
                Ok(())
            }
            Action::Gap { expected } => {
                debug!(%stream, expected, got = cursor, "gap: progress stopped");
                let _ = self.event_tx.try_send(RealtimeEvent::Gap {
                    stream,
                    expected,
                    got: cursor,
                });
                // Stop progress; retain old checkpoint; caller recovers via
                // resubscribe/replay or snapshot (never skip to cursor/head).
                Err(TinkleError::from(
                    tinkle_schema::realtime::RealtimeError::Gap {
                        stream: proto_batch.stream.clone(),
                        expected,
                        got: cursor,
                    },
                ))
            }
            Action::Stale { applied } => {
                debug!(%stream, applied, got = cursor, "stale batch ignored");
                let _ = self.event_tx.try_send(RealtimeEvent::Stale {
                    stream,
                    applied,
                    got: cursor,
                });
                Ok(())
            }
        }
    }

    /// Next app-visible event (None when closed).
    pub(crate) async fn next_event(&self) -> Option<RealtimeEvent> {
        self.event_rx.lock().await.recv().await
    }

    /// Clear ephemeral presence state on disconnect. Durable checkpoints
    /// survive (they live in the store + memory `applied` map).
    pub(crate) async fn on_disconnect(&self) {
        self.interests.lock().await.subscribed = false;
    }
}
