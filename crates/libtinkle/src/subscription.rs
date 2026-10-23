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
    /// Durable checkpoint save failed after memory commit was rolled back.
    /// Memory is back at the durable checkpoint, so the next `subscribe()`
    /// re-requests from there and the replayed batch is classified `Next`.
    /// Apps should resubscribe promptly instead of waiting for the next
    /// notification (which would otherwise surface only as a `Gap`).
    CheckpointFailed {
        stream: String,
        cursor: u64,
        error: String,
    },
}

/// Principal-scoped cursor state guarded by a single synchronization
/// barrier. `principal` and `cursors` must always be observed and mutated
/// together: classifying a notification with one principal's cursors while
/// persisting under another drops or duplicates checkpoints. A single
/// `Mutex` ensures `set_principal` atomically swaps both, and every
/// reader commits/decides under the same lock scope. Store I/O stays
/// outside the lock (snapshotted principal) so a slow
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
            Next { principal: String, previous: u64 },
            Gap { expected: u64 },
            Stale { applied: u64 },
        }
        let action = {
            let mut state = self.state.lock().await;
            match state.cursors.decision(&stream, cursor) {
                CursorDecision::Duplicate => Action::Duplicate,
                CursorDecision::Next => {
                    let previous = state.cursors.get(&stream);
                    state.cursors.commit(&stream, cursor)?;
                    Action::Next {
                        principal: state.principal.clone(),
                        previous,
                    }
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
            Action::Next {
                principal,
                previous,
            } => {
                // Atomically commit memory + durable store. The caller must
                // have applied all required effects + refreshes before this
                // checkpoint; receipt alone is not commit. Here we persist
                // the checkpoint; app effects are emitted with the event for
                // the caller to apply atomically where possible.
                // Memory already committed under the same barrier as the
                // snapshot; `principal` is that commit's owning scope even
                // if a rotation landed in between.
                if let Err(e) = self.store.save(&principal, &stream, cursor).await {
                    let error = e.to_string();
                    // Roll memory back so the next `subscribe()` re-requests
                    // from the durable checkpoint and the replayed batch is
                    // classified `Next` (not `Duplicate`), allowing its
                    // effects to be applied. Only roll back when the scope
                    // and cursor are still ours; a concurrent rotation or a
                    // newer commit owns the state now and must be preserved.
                    let mut state = self.state.lock().await;
                    if state.principal == principal && state.cursors.get(&stream) == cursor {
                        state.cursors.hydrate(&stream, previous);
                    }
                    drop(state);
                    // Surface the failure to the app so it can resubscribe
                    // without waiting for the next notification (which would
                    // otherwise appear only as a `Gap`).
                    let _ = self.event_tx.try_send(RealtimeEvent::CheckpointFailed {
                        stream: stream.clone(),
                        cursor,
                        error,
                    });
                    return Err(e);
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;

    struct FailOnceStore {
        inner: std::sync::Mutex<HashMap<(String, String), u64>>,
        fail_next_save: AtomicBool,
    }

    impl FailOnceStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: std::sync::Mutex::new(HashMap::new()),
                fail_next_save: AtomicBool::new(true),
            })
        }
    }

    #[async_trait]
    impl CursorStore for FailOnceStore {
        async fn load(&self, principal: &str, stream: &str) -> Result<Option<u64>, TinkleError> {
            Ok(self
                .inner
                .lock()
                .unwrap()
                .get(&(principal.to_string(), stream.to_string()))
                .copied())
        }

        async fn save(
            &self,
            principal: &str,
            stream: &str,
            cursor: u64,
        ) -> Result<(), TinkleError> {
            if self.fail_next_save.swap(false, Ordering::SeqCst) {
                return Err(TinkleError::CursorStore("injected save failure".into()));
            }
            self.inner
                .lock()
                .unwrap()
                .insert((principal.to_string(), stream.to_string()), cursor);
            Ok(())
        }

        async fn remove_stream(&self, principal: &str, stream: &str) -> Result<(), TinkleError> {
            self.inner
                .lock()
                .unwrap()
                .remove(&(principal.to_string(), stream.to_string()));
            Ok(())
        }

        async fn clear_principal(&self, principal: &str) -> Result<(), TinkleError> {
            self.inner
                .lock()
                .unwrap()
                .retain(|(p, _), _| p != principal);
            Ok(())
        }
    }

    fn test_batch(stream: &str, cursor: u64) -> tinkle_schema::realtime::ValidatedBatch {
        use tinkle_schema::proto::{
            realtime_event::Kind as EK, RealtimeBatch, RealtimeChatPosition, RealtimeEvent,
            RealtimeEventType, RealtimeMessageSnapshot,
        };
        let order = tinkle_schema::realtime::position_order(cursor, None);
        let uuid = "11111111-1111-4111-8111-111111111111";
        let batch = RealtimeBatch {
            stream: stream.into(),
            events: vec![
                RealtimeEvent {
                    event_id: uuid.into(),
                    stream: stream.into(),
                    cursor,
                    occurred_at: None,
                    source_seq: None,
                    source_revision: None,
                    r#type: RealtimeEventType::MessageSnapshot as i32,
                    kind: Some(EK::MessageSnapshot(RealtimeMessageSnapshot {
                        message: None,
                        random_id: None,
                    })),
                },
                RealtimeEvent {
                    event_id: uuid.into(),
                    stream: stream.into(),
                    cursor,
                    occurred_at: None,
                    source_seq: None,
                    source_revision: None,
                    r#type: RealtimeEventType::ChatPosition as i32,
                    kind: Some(EK::ChatPosition(RealtimeChatPosition {
                        chat_key: "room:1".into(),
                        order,
                    })),
                },
            ],
            final_cursor: cursor,
        };
        tinkle_schema::realtime::validate_batch(&batch).expect("test batch must validate")
    }

    #[tokio::test]
    async fn save_failure_rolls_back_and_replay_applies() {
        let store = FailOnceStore::new();
        let mgr = SubscriptionManager::new("user:42", store.clone());
        let batch = test_batch("user:42", 1);

        // First attempt fails at the durable save.
        let err = mgr
            .handle_batch(batch.clone())
            .await
            .expect_err("save must fail once");
        assert!(matches!(err, TinkleError::CursorStore(_)));

        // Memory rolled back so the replay is Next, not Duplicate.
        assert_eq!(mgr.state.lock().await.cursors.get("user:42"), 0);
        assert_eq!(store.load("user:42", "user:42").await.unwrap(), None);

        // App-visible failure signal (not silent until the next Gap).
        match mgr.next_event().await.expect("CheckpointFailed event") {
            RealtimeEvent::CheckpointFailed { stream, cursor, .. } => {
                assert_eq!(stream, "user:42");
                assert_eq!(cursor, 1);
            }
            other => panic!("expected CheckpointFailed, got {other:?}"),
        }

        // Replayed batch applies and checkpoints.
        mgr.handle_batch(batch.clone())
            .await
            .expect("replay must succeed");
        match mgr.next_event().await.expect("BatchApplied event") {
            RealtimeEvent::BatchApplied {
                stream,
                final_cursor,
                ..
            } => {
                assert_eq!(stream, "user:42");
                assert_eq!(final_cursor, 1);
            }
            other => panic!("expected BatchApplied, got {other:?}"),
        }
        assert_eq!(mgr.state.lock().await.cursors.get("user:42"), 1);
        assert_eq!(store.load("user:42", "user:42").await.unwrap(), Some(1));
    }
}
