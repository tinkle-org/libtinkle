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

use tokio::sync::{mpsc, Mutex, RwLock};
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

/// Manages one principal's realtime interests + cursors (crate-internal;
/// apps observe [`RealtimeEvent`] via [`crate::Client::next_event`]).
pub(crate) struct SubscriptionManager {
    principal: RwLock<String>,
    store: Arc<dyn CursorStore>,
    cursors: Mutex<StreamCursors>,
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
            principal: RwLock::new(principal.into()),
            store,
            cursors: Mutex::new(StreamCursors::new()),
            event_tx: tx,
            event_rx: Mutex::new(rx),
            interests: Mutex::new(Interests::default()),
        })
    }

    /// Update the cursor principal after authenticated subscribe
    /// (`resp.stream`). Room cursor writes and reconnect loads must use
    /// this same principal rather than the initial `unknown` value.
    pub(crate) async fn set_principal(&self, principal: String) {
        *self.principal.write().await = principal;
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
    pub(crate) async fn hydrate_from_store(&self, streams: &[String]) -> Result<(), TinkleError> {
        let principal = self.principal.read().await.clone();
        let mut curs = self.cursors.lock().await;
        for s in streams {
            match self.store.load(&principal, s).await? {
                Some(v) => curs.hydrate(s, v),
                None => curs.hydrate(s, 0),
            }
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
                {
                    let mut c = self.cursors.lock().await;
                    c.on_reset(&stream);
                }
                let principal = self.principal.read().await.clone();
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
        let decision = self.cursors.lock().await.decision(&stream, cursor);
        match decision {
            CursorDecision::Duplicate => {
                debug!(%stream, cursor, "duplicate batch ignored");
                let _ = self
                    .event_tx
                    .try_send(RealtimeEvent::Duplicate { stream, cursor });
                Ok(())
            }
            CursorDecision::Next => {
                // Atomically commit memory + durable store. The caller must
                // have applied all required effects + refreshes before this
                // checkpoint; receipt alone is not commit. Here we persist
                // the checkpoint; app effects are emitted with the event for
                // the caller to apply atomically where possible.
                {
                    let mut c = self.cursors.lock().await;
                    c.commit(&stream, cursor)?;
                }
                let principal = self.principal.read().await.clone();
                self.store.save(&principal, &stream, cursor).await?;
                let _ = self.event_tx.try_send(RealtimeEvent::BatchApplied {
                    stream,
                    final_cursor: cursor,
                    batch: proto_batch,
                });
                Ok(())
            }
            CursorDecision::Gap => {
                let expected = self.cursors.lock().await.get(&stream).saturating_add(1);
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
            CursorDecision::Stale => {
                let applied = self.cursors.lock().await.get(&stream);
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
