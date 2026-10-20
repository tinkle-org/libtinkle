//! Generic Syntra connection runtime (transport-independent).
//!
//! The connection owns the six-state machine, version selection,
//! pending-RPC correlation, per-request deadlines, cancellation, error
//! translation, and notification routing. A thin transport adapter
//! (TCP/TLS/HTTP/WS/WSS) moves serialized `Frame` bytes; this module
//! decides what the bytes mean.
//!
//! Design notes (spec mapping):
//! - One `Frame` per unit; one terminal `result` per `request_id`
//!   (`SNTR-WIRE-001`, `SNTR-RPC-002/006/030`).
//! - Nonzero unique ids among outstanding RPCs; reuse only after
//!   terminal result; fresh id space per connection (`SNTR-RPC-§1B`).
//! - Initiator timer ≤ requested budget; abandonment without `cancel`
//!   does not cancel the host (`SNTR-RPC-004/021`).
//! - `cancel` is advisory; completion wins races (`SNTR-RPC-005/031`).
//! - Transport loss maps to `UNAVAILABLE` with unknown outcome
//!   (`SNTR-REL-009`); never auto-retry unsafe mutations (`SNTR-REL-003`).
//! - Notifications never complete an RPC (`SNTR-RPC-020`, `SNTR-EVENT-005`).

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::debug;

use crate::error::{ErrorCode, SyntraError};
use crate::frame::{validate_frame, FrameKind, ValidationContext};
use crate::handshake::{HANDSHAKE_TIMEOUT_DEFAULT, MAX_VERSION, MIN_VERSION};
use crate::ids::RequestIdAllocator;
use crate::method::validate_method;
use crate::pending::{PendingOutcome, PendingTable};
use crate::proto::{frame::Body, Cancel, ClientHello, Frame, RpcRequest};
use crate::state::{ConnectionState, StateEvent};

/// Tunables. All bounds are documented per deployment (SNTR-REL-031).
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Max concurrent RPCs. Peers MUST support ≥16 (SNTR-RPC-008).
    pub max_outstanding: usize,
    /// Default call budget when `timeout_ms == 0` (bounded, documented).
    pub default_timeout: Duration,
    /// Handshake deadline (SNTR-HS-002/011).
    pub handshake_timeout: Duration,
    /// Outgoing notification channel capacity.
    pub notification_capacity: usize,
    /// Outgoing writer channel capacity (backpressure bound).
    pub writer_capacity: usize,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_outstanding: 64,
            default_timeout: Duration::from_secs(30),
            handshake_timeout: HANDSHAKE_TIMEOUT_DEFAULT,
            notification_capacity: 256,
            writer_capacity: 256,
        }
    }
}

/// Re-exported for callers that match on transport-loss mapping.
pub use crate::pending::PendingOutcome as PendingOutcomeReexport;

struct Inner {
    state: RwLock<ConnectionState>,
    version: RwLock<Option<u32>>,
    /// Our proposed max (stamped on hello); selected after ready.
    proposed_max: u32,
    pending: Arc<PendingTable>,
    ids: RequestIdAllocator,
    writer: mpsc::Sender<Vec<u8>>,
    notif_tx: mpsc::Sender<prost::bytes::Bytes>,
    handshake_tx: RwLock<Option<oneshot::Sender<Result<String, SyntraError>>>>,
    config: ConnectionConfig,
    closed: AtomicU64,
}

/// Transport-independent Syntra connection (initiator side).
///
/// A transport adapter constructs this with a `writer` channel (bytes the
/// adapter sends as one WS binary message / one length-prefixed TCP unit /
/// one HTTP body), drives inbound bytes via [`Connection::ingest`], and
/// forwards [`Connection::take_notifications`] to the application.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<Inner>,
    notif_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<prost::bytes::Bytes>>>,
}

impl Connection {
    /// Create a connection in `CONNECTED` with the given writer.
    /// The writer carries exactly one serialized `Frame` per item.
    pub fn new(writer: mpsc::Sender<Vec<u8>>, config: ConnectionConfig) -> Self {
        let (notif_tx, notif_rx) = mpsc::channel(config.notification_capacity);
        Self {
            inner: Arc::new(Inner {
                state: RwLock::new(ConnectionState::Connected),
                version: RwLock::new(None),
                proposed_max: MAX_VERSION,
                pending: PendingTable::new(config.max_outstanding),
                ids: RequestIdAllocator::new(),
                writer,
                notif_tx,
                handshake_tx: RwLock::new(None),
                config,
                closed: AtomicU64::new(0),
            }),
            notif_rx: Arc::new(tokio::sync::Mutex::new(notif_rx)),
        }
    }

    /// Current state (for diagnostics; not a wire value).
    pub async fn state(&self) -> ConnectionState {
        *self.inner.state.read().await
    }

    pub async fn negotiated_version(&self) -> Option<u32> {
        *self.inner.version.read().await
    }

    async fn ctx(&self) -> ValidationContext {
        let s = *self.inner.state.read().await;
        let v = *self.inner.version.read().await;
        ValidationContext {
            state: s,
            negotiated_version: v,
        }
    }

    // -- Handshake -------------------------------------------------------

    /// Send `hello` and await `ready` (or a terminal handshake error).
    ///
    /// `credential` is opaque secret bytes presented only in
    /// `ClientHello.credential`. Callers MUST NOT log it, MUST NOT place
    /// it in URLs, and MUST drop it after the handshake where possible.
    pub async fn handshake(&self, credential: Vec<u8>) -> Result<String, SyntraError> {
        // Guard: exactly one hello per transport (SNTR-HS-020/040).
        {
            let s = *self.inner.state.read().await;
            if s != ConnectionState::Connected {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::FailedPrecondition,
                    message: "handshake already started".to_string(),
                });
            }
        }
        crate::handshake::validate_hello_range(MIN_VERSION, MAX_VERSION)?;
        if credential.len() > crate::handshake::MAX_CREDENTIAL_BYTES {
            return Err(SyntraError::Protocol {
                code: ErrorCode::ResourceExhausted,
                message: "credential exceeds bound".to_string(),
            });
        }

        let hello = ClientHello {
            min_version: MIN_VERSION,
            max_version: MAX_VERSION,
            credential: credential.into(),
        };
        let frame = Frame {
            version: self.inner.proposed_max,
            request_id: 0,
            body: Some(Body::Hello(hello)),
        };
        // Validate locally before emitting.
        validate_frame(
            &frame,
            ValidationContext {
                state: ConnectionState::Connected,
                negotiated_version: None,
            },
        )?;

        let (tx, rx) = oneshot::channel();
        *self.inner.handshake_tx.write().await = Some(tx);
        // CONNECTED -> NEGOTIATING.
        self.transition(StateEvent::HelloSent).await?;

        let bytes = {
            use prost::Message;
            frame.encode_to_vec()
        };
        self.send_bytes(bytes).await?;

        // Await ready with the handshake deadline (SNTR-HS-011).
        let timeout = self.inner.config.handshake_timeout;
        let res = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| SyntraError::DeadlineExceeded("handshake timed out".to_string()))?
            .map_err(|_| SyntraError::Closed("handshake waiter dropped".to_string()))??;
        Ok(res)
    }

    async fn transition(&self, ev: StateEvent) -> Result<(), SyntraError> {
        let cur = *self.inner.state.read().await;
        let next = cur.transition(ev)?;
        *self.inner.state.write().await = next;
        debug!(from = ?cur, to = ?next, event = ?ev, "syntra state");
        Ok(())
    }

    async fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), SyntraError> {
        if self.inner.closed.load(Ordering::Relaxed) == 1 {
            return Err(SyntraError::Closed("connection closed".to_string()));
        }
        self.inner
            .writer
            .send(bytes)
            .await
            .map_err(|_| SyntraError::Closed("transport writer gone".to_string()))
    }

    // -- Inbound ---------------------------------------------------------

    /// Ingest one transport unit (exactly one serialized `Frame`).
    /// Called by the transport adapter's read loop.
    pub async fn ingest(&self, bytes: &[u8]) {
        let ctx = self.ctx().await;
        let frame = match crate::codec::decode_frame(bytes, ctx) {
            Ok(f) => f,
            Err(e) => {
                // Framing-layer failures before ACTIVE are terminal for the
                // connection; in ACTIVE they fail the affected unit (or the
                // connection if exhausted/misbehaving per policy).
                debug!(error = ?e, "inbound frame rejected");
                let st = self.state().await;
                if !st.is_active() {
                    self.fail_handshake(SyntraError::Protocol {
                        code: ErrorCode::InvalidArgument,
                        message: "malformed handshake frame".to_string(),
                    })
                    .await;
                    self.mark_closed().await;
                }
                return;
            }
        };
        self.dispatch(frame).await;
    }

    async fn dispatch(&self, frame: Frame) {
        let kind = match &frame.body {
            Some(Body::Hello(_)) => FrameKind::Hello,
            Some(Body::Ready(_)) => FrameKind::Ready,
            Some(Body::Request(_)) => FrameKind::Request,
            Some(Body::Result(_)) => FrameKind::Result,
            Some(Body::Cancel(_)) => FrameKind::Cancel,
            Some(Body::Notification(_)) => FrameKind::Notification,
            None => return,
        };
        match kind {
            FrameKind::Ready => self.on_ready(frame).await,
            FrameKind::Result => self.on_result(frame).await,
            FrameKind::Notification => self.on_notification(frame).await,
            // The initiator never expects hello/request/cancel inbound.
            // In ACTIVE this is a diagnostic; before ACTIVE it fails the
            // handshake (SNTR-HS-012).
            _ => {
                let st = self.state().await;
                debug!(?kind, state = ?st, "unexpected inbound frame");
                if !st.is_active() {
                    self.fail_handshake(SyntraError::Handshake {
                        code: ErrorCode::FailedPrecondition,
                        message: "unexpected message during handshake".to_string(),
                    })
                    .await;
                    self.mark_closed().await;
                }
            }
        }
    }

    async fn on_ready(&self, frame: Frame) {
        let ready = match frame.body {
            Some(Body::Ready(r)) => r,
            _ => return,
        };
        // Selected version is Frame.version (SNTR-HS-004/010).
        if frame.version == 0 {
            self.fail_handshake(SyntraError::Handshake {
                code: ErrorCode::InvalidArgument,
                message: "ready version must be nonzero".to_string(),
            })
            .await;
            return;
        }
        if let Err(e) = crate::handshake::validate_connection_id(&ready.connection_id) {
            self.fail_handshake(SyntraError::Handshake {
                code: ErrorCode::InvalidArgument,
                message: format!("bad connection_id: {e}"),
            })
            .await;
            return;
        }
        *self.inner.version.write().await = Some(frame.version);
        // NEGOTIATING -> AUTHENTICATING -> ACTIVE. Our initiator path
        // collapses version-accept + credential-accept into ready.
        let _ = self.transition(StateEvent::VersionAccepted).await;
        let _ = self.transition(StateEvent::ReadyReceived).await;
        if let Some(tx) = self.inner.handshake_tx.write().await.take() {
            let _ = tx.send(Ok(ready.connection_id));
        }
    }

    async fn on_result(&self, frame: Frame) {
        let result = match frame.body {
            Some(Body::Result(r)) => r,
            _ => return,
        };
        // Missing outcome was already rejected by validation; unknown ids
        // are ignored with a diagnostic (no error storm).
        self.inner.pending.complete(frame.request_id, result);
    }

    async fn on_notification(&self, frame: Frame) {
        let notif = match frame.body {
            Some(Body::Notification(n)) => n,
            _ => return,
        };
        // Notifications never complete an RPC. Pre-ACTIVE notifications
        // are ignored with a diagnostic (events.md default); the handshake
        // MUST NOT treat them as subscription establishment.
        if self.state().await != ConnectionState::Active {
            debug!("pre-ACTIVE notification ignored");
            return;
        }
        // Bounded channel: drop-or-error per stream policy, never unbounded
        // queueing (SNTR-REL-007). Here: try_send, drop with diagnostic.
        if self.inner.notif_tx.try_send(notif.payload).is_err() {
            debug!("notification queue full; dropping");
        }
    }

    async fn fail_handshake(&self, e: SyntraError) {
        if let Some(tx) = self.inner.handshake_tx.write().await.take() {
            let _ = tx.send(Err(e));
        }
    }

    // -- RPC -------------------------------------------------------------

    /// Invoke a method: serialize is the caller's job (opaque bytes),
    /// correlation/deadline/cancel are ours.
    ///
    /// - Validates method shape before sending (`INVALID_ARGUMENT`).
    /// - Allocates a nonzero id unique among outstanding RPCs.
    /// - Enforces bounded outstanding (`RESOURCE_EXHAUSTED`).
    /// - Sends one `request` Frame; awaits exactly one terminal `result`.
    /// - Interleaved notifications do not complete the RPC.
    /// - Local timeout abandons the waiter but does not cancel the host
    ///   without an explicit [`Connection::cancel`].
    pub async fn call(
        &self,
        method: &str,
        payload: Vec<u8>,
        timeout_ms: u32,
    ) -> Result<Vec<u8>, SyntraError> {
        if self.state().await != ConnectionState::Active {
            return Err(SyntraError::Protocol {
                code: ErrorCode::FailedPrecondition,
                message: "RPC requires ACTIVE connection".to_string(),
            });
        }
        validate_method(method)?;

        let version = self
            .negotiated_version()
            .await
            .ok_or(SyntraError::Protocol {
                code: ErrorCode::FailedPrecondition,
                message: "no negotiated version".to_string(),
            })?;

        // Allocate a fresh id (skip outstanding collisions on wrap).
        let id = {
            let mut id = self.inner.ids.next_candidate();
            for _ in 0..1024 {
                if !self.inner.pending.is_pending(id) && id != 0 {
                    break;
                }
                id = self.inner.ids.next_candidate();
            }
            if id == 0 || self.inner.pending.is_pending(id) {
                return Err(SyntraError::ResourceExhausted(
                    "request-id space exhausted".to_string(),
                ));
            }
            id
        };

        let budget = if timeout_ms == 0 {
            self.inner.config.default_timeout
        } else {
            Duration::from_millis(timeout_ms as u64)
        };

        let rx = self.inner.pending.insert(id, method.to_string())?;

        let req = RpcRequest {
            method: method.to_string(),
            payload: payload.into(),
            timeout_ms,
        };
        let frame = Frame {
            version,
            request_id: id,
            body: Some(Body::Request(req)),
        };
        validate_frame(&frame, ValidationContext::active(version))?;
        {
            use prost::Message;
            let bytes = frame.encode_to_vec();
            if bytes.len() > crate::codec::MAX_FRAME_BYTES {
                self.inner.pending.abandon(id);
                return Err(SyntraError::ResourceExhausted(
                    "request exceeds maximum envelope size".to_string(),
                ));
            }
            if let Err(e) = self.send_bytes(bytes).await {
                self.inner.pending.abandon(id);
                return Err(e);
            }
        }

        // Initiator timer ≤ requested budget (SNTR-RPC-004/§1F).
        let outcome = tokio::time::timeout(budget, rx)
            .await
            .map_err(|_| {
                // Local abandonment; host still owns the RPC until cancel
                // or its own deadline. Late result is ignored as unknown.
                self.inner.pending.abandon(id);
                SyntraError::DeadlineExceeded(format!("{method} timed out"))
            })?
            .map_err(|_| SyntraError::Closed("rpc waiter dropped".to_string()))?;

        match outcome {
            PendingOutcome::TransportLost => Err(SyntraError::Unavailable(format!(
                "{method} lost: transport closed (outcome unknown)"
            ))),
            PendingOutcome::Result(res) => match res.outcome {
                Some(crate::proto::rpc_result::Outcome::Payload(b)) => Ok(b.to_vec()),
                Some(crate::proto::rpc_result::Outcome::Error(e)) => {
                    let (code, raw) = crate::error::ErrorCode::from_i32_lossy(e.code);
                    Err(SyntraError::Rpc {
                        code,
                        raw_code: raw,
                        message: e.message,
                    })
                }
                None => Err(SyntraError::Protocol {
                    code: ErrorCode::InvalidArgument,
                    message: "result outcome missing".to_string(),
                }),
            },
        }
    }

    /// Cancellable RPC: same as [`Connection::call`] but aborts early when
    /// `cancel` fires. On cancellation this sends best-effort `Cancel{}` for
    /// the allocated id, abandons the waiter, and returns `CANCELLED`. The
    /// host still ends the RPC with exactly one terminal `result`
    /// (completion wins races); the late result is ignored as unknown.
    /// (`SNTR-RPC-005/021`.)
    pub async fn call_cancellable(
        &self,
        method: &str,
        payload: Vec<u8>,
        timeout_ms: u32,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<Vec<u8>, SyntraError> {
        if self.state().await != ConnectionState::Active {
            return Err(SyntraError::Protocol {
                code: ErrorCode::FailedPrecondition,
                message: "RPC requires ACTIVE connection".to_string(),
            });
        }
        validate_method(method)?;
        let version = self
            .negotiated_version()
            .await
            .ok_or(SyntraError::Protocol {
                code: ErrorCode::FailedPrecondition,
                message: "no negotiated version".to_string(),
            })?;
        let id = {
            let mut id = self.inner.ids.next_candidate();
            for _ in 0..1024 {
                if !self.inner.pending.is_pending(id) && id != 0 {
                    break;
                }
                id = self.inner.ids.next_candidate();
            }
            if id == 0 || self.inner.pending.is_pending(id) {
                return Err(SyntraError::ResourceExhausted(
                    "request-id space exhausted".to_string(),
                ));
            }
            id
        };
        let budget = if timeout_ms == 0 {
            self.inner.config.default_timeout
        } else {
            Duration::from_millis(timeout_ms as u64)
        };
        let rx = self.inner.pending.insert(id, method.to_string())?;
        let req = RpcRequest {
            method: method.to_string(),
            payload: payload.into(),
            timeout_ms,
        };
        let frame = Frame {
            version,
            request_id: id,
            body: Some(Body::Request(req)),
        };
        validate_frame(&frame, ValidationContext::active(version))?;
        {
            use prost::Message;
            let bytes = frame.encode_to_vec();
            if bytes.len() > crate::codec::MAX_FRAME_BYTES {
                self.inner.pending.abandon(id);
                return Err(SyntraError::ResourceExhausted(
                    "request exceeds maximum envelope size".to_string(),
                ));
            }
            if let Err(e) = self.send_bytes(bytes).await {
                self.inner.pending.abandon(id);
                return Err(e);
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => {
                let _ = self.cancel(id).await;
                self.inner.pending.abandon(id);
                Err(SyntraError::Cancelled(format!("{method} cancelled")))
            }
            out = tokio::time::timeout(budget, rx) => {
                let outcome = out
                    .map_err(|_| {
                        self.inner.pending.abandon(id);
                        SyntraError::DeadlineExceeded(format!("{method} timed out"))
                    })?
                    .map_err(|_| SyntraError::Closed("rpc waiter dropped".to_string()))?;
                match outcome {
                    PendingOutcome::TransportLost => Err(SyntraError::Unavailable(format!(
                        "{method} lost: transport closed (outcome unknown)"
                    ))),
                    PendingOutcome::Result(res) => match res.outcome {
                        Some(crate::proto::rpc_result::Outcome::Payload(b)) => Ok(b.to_vec()),
                        Some(crate::proto::rpc_result::Outcome::Error(e)) => {
                            let (code, raw) = crate::error::ErrorCode::from_i32_lossy(e.code);
                            Err(SyntraError::Rpc {
                                code,
                                raw_code: raw,
                                message: e.message,
                            })
                        }
                        None => Err(SyntraError::Protocol {
                            code: ErrorCode::InvalidArgument,
                            message: "result outcome missing".to_string(),
                        }),
                    },
                }
            }
        }
    }

    /// Number of outstanding RPCs (for backpressure observability).
    pub fn pending_len(&self) -> usize {
        self.inner.pending.len()
    }

    /// Best-effort cancellation (`SNTR-RPC-005`). Advisory only; the RPC
    /// still ends with exactly one terminal `result` (success wins races).
    /// Unknown/completed ids are ignored to tolerate races.
    pub async fn cancel(&self, request_id: u64) -> Result<(), SyntraError> {
        if request_id == 0 {
            return Err(SyntraError::Protocol {
                code: ErrorCode::InvalidArgument,
                message: "cancel requires nonzero request_id".to_string(),
            });
        }
        let version = match self.negotiated_version().await {
            Some(v) => v,
            None => return Ok(()), // no ACTIVE connection; nothing to cancel
        };
        let frame = Frame {
            version,
            request_id,
            body: Some(Body::Cancel(Cancel {})),
        };
        // Send best-effort; ignore transport errors for unknown ids.
        {
            use prost::Message;
            let bytes = frame.encode_to_vec();
            let _ = self.inner.writer.try_send(bytes);
        }
        Ok(())
    }

    /// Take the next notification payload (opaque application bytes).
    /// Returns None when the connection is closed and drained.
    pub async fn next_notification(&self) -> Option<Vec<u8>> {
        self.notif_rx.lock().await.recv().await.map(|b| b.to_vec())
    }

    /// Graceful close from ACTIVE: stop new RPCs, allow the transport's
    /// grace period, then teardown (SNTR-HS-006/013).
    pub async fn close(&self) {
        let st = self.state().await;
        if st == ConnectionState::Active {
            let _ = self.transition(StateEvent::GracefulClose).await;
        }
        self.mark_closed().await;
    }

    /// Report abrupt transport loss: `UNAVAILABLE` to all in-flight RPCs
    /// with unknown outcome (SNTR-REL-009), handshake waiters fail,
    /// state moves to CLOSED.
    pub async fn on_transport_lost(&self) {
        self.fail_handshake(SyntraError::Unavailable(
            "transport lost during handshake (outcome unknown)".to_string(),
        ))
        .await;
        self.inner.pending.drain_transport_lost();
        self.mark_closed().await;
    }

    async fn mark_closed(&self) {
        self.inner.closed.store(1, Ordering::Relaxed);
        let cur = *self.inner.state.read().await;
        match cur {
            ConnectionState::Closed => {}
            ConnectionState::Closing => {
                *self.inner.state.write().await = ConnectionState::Closed;
            }
            _ => {
                // Abrupt loss goes straight to CLOSED (SNTR-HS-§1B).
                *self.inner.state.write().await = ConnectionState::Closed;
            }
        }
        self.inner.pending.drain_transport_lost();
    }

    /// Test hook: inject a decoded inbound frame without bytes.
    #[cfg(test)]
    pub async fn inject_frame(&self, frame: Frame) {
        self.dispatch(frame).await;
    }
}

// Re-export RpcError conversion helpers for transport adapters.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::rpc_result::Outcome;
    use crate::proto::RpcError as ProtoError;
    use crate::proto::RpcResult;

    async fn test_conn() -> (Connection, mpsc::Receiver<Vec<u8>>) {
        let (wtx, wrx) = mpsc::channel(16);
        let c = Connection::new(wtx, ConnectionConfig::default());
        // Force ACTIVE for RPC tests.
        *c.inner.state.write().await = ConnectionState::Active;
        *c.inner.version.write().await = Some(1);
        (c, wrx)
    }

    #[tokio::test]
    async fn out_of_order_results_correlate_by_id() {
        let (c, mut wrx) = test_conn().await;
        let c_call1 = c.clone();
        let c_call2 = c.clone();
        let h1 = tokio::spawn(async move {
            c_call1
                .call("/example.Echo/Echo", b"A".to_vec(), 5000)
                .await
        });
        let h2 = tokio::spawn(async move {
            c_call2
                .call("/example.Echo/Echo", b"B".to_vec(), 5000)
                .await
        });
        // Collect the two request ids.
        let b1 = wrx.recv().await.unwrap();
        let b2 = wrx.recv().await.unwrap();
        use prost::Message;
        let f1 = Frame::decode(&*b1).unwrap();
        let f2 = Frame::decode(&*b2).unwrap();
        assert_ne!(f1.request_id, f2.request_id);
        // Complete second first (out-of-order legal; FIFO must not be assumed).
        c.inject_frame(Frame {
            version: 1,
            request_id: f2.request_id,
            body: Some(Body::Result(RpcResult {
                outcome: Some(Outcome::Payload(b"B".to_vec().into())),
            })),
        })
        .await;
        c.inject_frame(Frame {
            version: 1,
            request_id: f1.request_id,
            body: Some(Body::Result(RpcResult {
                outcome: Some(Outcome::Payload(b"A".to_vec().into())),
            })),
        })
        .await;
        assert_eq!(h1.await.unwrap().unwrap(), b"A");
        assert_eq!(h2.await.unwrap().unwrap(), b"B");
    }

    #[tokio::test]
    async fn unknown_id_result_ignored() {
        let (c, _wrx) = test_conn().await;
        c.inject_frame(Frame {
            version: 1,
            request_id: 9999,
            body: Some(Body::Result(RpcResult {
                outcome: Some(Outcome::Payload(vec![].into())),
            })),
        })
        .await;
        // No panic, no error storm.
        assert_eq!(c.inner.pending.len(), 0);
    }

    #[tokio::test]
    async fn transport_loss_maps_to_unavailable() {
        let (c, mut wrx) = test_conn().await;
        let h = tokio::spawn({
            let c = c.clone();
            async move { c.call("/example.Echo/Echo", b"x".to_vec(), 5000).await }
        });
        let _ = wrx.recv().await.unwrap();
        c.on_transport_lost().await;
        let err = h.await.unwrap().unwrap_err();
        assert!(err.is_unknown_outcome());
        assert!(matches!(err, SyntraError::Unavailable(_)));
    }

    #[tokio::test]
    async fn error_code_mapped() {
        let (c, mut wrx) = test_conn().await;
        let h = tokio::spawn({
            let c = c.clone();
            async move { c.call("/example.KV/Get", b"k".to_vec(), 5000).await }
        });
        let b = wrx.recv().await.unwrap();
        use prost::Message;
        let f = Frame::decode(&*b).unwrap();
        c.inject_frame(Frame {
            version: 1,
            request_id: f.request_id,
            body: Some(Body::Result(RpcResult {
                outcome: Some(Outcome::Error(ProtoError {
                    code: 5, // NOT_FOUND
                    message: "missing".into(),
                })),
            })),
        })
        .await;
        let err = h.await.unwrap().unwrap_err();
        assert_eq!(err.code(), ErrorCode::NotFound);
    }
}
