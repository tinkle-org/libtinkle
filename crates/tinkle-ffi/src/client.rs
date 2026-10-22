//! UniFFI client object: the single entry point for Swift/Kotlin.
//!
//! [`TinkleClient`] wraps [`libtinkle::Client`]. All method params are
//! owned (`String`/`Vec`/records); no `&str`, no Syntra/prost types, and
//! no cancellation tokens cross the boundary. Dropping the foreign
//! future cancels the in-flight Rust future.
//!
//! Minimal surface first: one write path end-to-end
//! ([`TinkleClient::send_message`]), plus connect/subscribe/event-pump and
//! lifecycle. More write paths follow the same pattern.

use std::sync::Arc;

use crate::{ClientConfigFfi, FfiCursorStore, FfiError};

/// One room interest for [`TinkleClient::subscribe`].
///
/// A record (not a `(i64, u64)` tuple) because UniFFI has no tuple type.
#[derive(Debug, Clone, uniffi::Record)]
pub struct RoomSub {
    pub room_id: i64,
    pub after_cursor: u64,
}

/// Subscribe confirmation (FFI-safe mirror of the proto response).
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiSubscribeResponse {
    pub stream: String,
    pub after_cursor: u64,
    pub head: Option<u64>,
}

/// App-visible realtime events. Owned primitives only; the underlying
/// proto batch is summarized as an event count, never exposed.
#[derive(Debug, Clone, uniffi::Enum)]
pub enum FfiRealtimeEvent {
    BatchApplied {
        stream: String,
        final_cursor: u64,
        event_count: u32,
    },
    Duplicate {
        stream: String,
        cursor: u64,
    },
    Gap {
        stream: String,
        expected: u64,
        got: u64,
    },
    Stale {
        stream: String,
        applied: u64,
        got: u64,
    },
    Presence {
        user_id: i64,
        online: bool,
        known: bool,
    },
    ResetRequired {
        stream: String,
        head: Option<u64>,
    },
}

impl From<libtinkle::RealtimeEvent> for FfiRealtimeEvent {
    fn from(e: libtinkle::RealtimeEvent) -> Self {
        match e {
            libtinkle::RealtimeEvent::BatchApplied {
                stream,
                final_cursor,
                batch,
            } => Self::BatchApplied {
                stream,
                final_cursor,
                event_count: batch.events.len() as u32,
            },
            libtinkle::RealtimeEvent::Duplicate { stream, cursor } => {
                Self::Duplicate { stream, cursor }
            }
            libtinkle::RealtimeEvent::Gap {
                stream,
                expected,
                got,
            } => Self::Gap {
                stream,
                expected,
                got,
            },
            libtinkle::RealtimeEvent::Stale {
                stream,
                applied,
                got,
            } => Self::Stale {
                stream,
                applied,
                got,
            },
            libtinkle::RealtimeEvent::Presence(p) => Self::Presence {
                user_id: p.user_id,
                online: p.online,
                known: p.known,
            },
            libtinkle::RealtimeEvent::ResetRequired { stream, head } => {
                Self::ResetRequired { stream, head }
            }
        }
    }
}

/// UniFFI client object.
#[derive(uniffi::Object)]
pub struct TinkleClient {
    inner: libtinkle::Client,
}

#[uniffi::export(async_runtime = "tokio")]
impl TinkleClient {
    /// Create a client. `credential` is the opaque session token;
    /// `store` is the foreign (or in-memory) durable cursor store.
    #[uniffi::constructor]
    pub fn new(
        config: ClientConfigFfi,
        credential: String,
        store: Arc<dyn FfiCursorStore>,
    ) -> Arc<Self> {
        crate::runtime::init();
        let inner = libtinkle::Client::new(
            config.into(),
            libtinkle::Credential::from_session_token(&credential),
            Arc::new(crate::store::FfiStoreAdapter(store)),
        );
        Arc::new(Self { inner })
    }

    /// Foreground connect: WSS upgrade + Syntra handshake to ACTIVE.
    pub async fn connect(&self) -> Result<(), FfiError> {
        self.inner.connect().await.map_err(FfiError::from)
    }

    /// Minimal write path end-to-end: send a text message to a room.
    /// Returns the server-assigned message id.
    pub async fn send_message(
        &self,
        room_id: i64,
        text: String,
        random_id: u64,
    ) -> Result<u64, FfiError> {
        use tinkle_schema::proto::{peer_input, PeerInput, SendMessageRequest};

        if room_id == 0 {
            return Err(FfiError::Codec("room_id must be nonzero".to_string()));
        }
        let random_id = i64::try_from(random_id)
            .map_err(|_| FfiError::Codec("random_id out of i64 range".to_string()))?;
        let req = SendMessageRequest {
            peer: Some(PeerInput {
                target: Some(peer_input::Target::ChatId(room_id)),
            }),
            message: text,
            random_id,
            reply_to: None,
            silent: None,
            no_forward: None,
            clear_draft: None,
            entities: Vec::new(),
            schedule_date: None,
        };
        let resp = self
            .inner
            .messages()
            .send_message(req)
            .await
            .map_err(FfiError::from)?;
        let id = resp.message.map(|m| m.message_id).unwrap_or(0);
        u64::try_from(id)
            .map_err(|_| FfiError::Codec(format!("server returned bad message_id {id}")))
    }

    /// Subscribe with durable cursor recovery. Returns the confirmation
    /// (interests registered, not replay completion).
    // TODO(runtime-acceptance-resubscribe): acceptance must assert interests
    // survive reconnect (fix already hoisted interests to Client; watch ids
    // preserved). Test: subscribe, reconnect, verify resubscribe replays rooms+watch.
    pub async fn subscribe(
        &self,
        rooms: Vec<RoomSub>,
        watch: Vec<i64>,
    ) -> Result<FfiSubscribeResponse, FfiError> {
        let rooms: Vec<(i64, u64)> = rooms
            .into_iter()
            .map(|r| (r.room_id, r.after_cursor))
            .collect();
        let resp = self
            .inner
            .subscribe(rooms, watch)
            .await
            .map_err(FfiError::from)?;
        Ok(FfiSubscribeResponse {
            stream: resp.stream,
            after_cursor: resp.after_cursor,
            head: resp.head,
        })
    }

    /// Next realtime app event. `None` when the client is closed.
    pub async fn next_event(&self) -> Option<FfiRealtimeEvent> {
        self.inner.next_event().await.map(FfiRealtimeEvent::from)
    }

    /// Background suspension: graceful close, durable cursors kept.
    // TODO(runtime-acceptance-ios): Swift background/foreground acceptance —
    // assert suspend() on backgrounding (no background WS held), then
    // reconnect+resubscribe on foreground restores events without gaps.
    pub async fn suspend(&self) {
        self.inner.suspend().await;
    }

    /// Reconnect + re-handshake + explicit resubscribe from persisted
    /// cursors (network change / wake from push).
    // TODO(runtime-acceptance-android): FCM/Doze acceptance — wake triggers
    // fresh handshake, UNAVAILABLE surfaces unknown-outcome (never blindly
    // retried for non-idempotent ops), WorkManager defers sync under Doze.
    pub async fn reconnect(&self) -> Result<(), FfiError> {
        self.inner.reconnect().await.map_err(FfiError::from)
    }

    /// Graceful close: stop RPCs, tear down transport.
    pub async fn close(&self) {
        self.inner.close().await;
    }
}
