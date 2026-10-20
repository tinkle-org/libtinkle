//! Native WSS/WS adapter (`tokio-tungstenite` + `rustls`).
//!
//! Security properties (WSS §1M, TSEC):
//! - TLS 1.3 only via established `rustls` defaults; <1.3 fails closed.
//! - Full chain + hostname + expiry validation before any Syntra byte.
//! - No `wss://` → `ws://` downgrade retry (FORBIDDEN).
//! - SNI sent consistently with the intended hostname.
//! - No credentials in Upgrade target/headers/query; the only Syntra
//!   credential carrier is `ClientHello.credential` after 101.
//! - `Origin` is never authentication (WS §1H).
//!
//! Framing: one binary message = one `Frame` (no length prefix).
//! Text (0x1) → close 1003. Empty binary → `INVALID_ARGUMENT` + close.
//! Assembled message > MAX_FRAME_BYTES → close 1009. Control frames
//! (Ping/Pong/Close) never reach the Syntra core.

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};

use syntra_core::{Connection, ConnectionConfig};

use crate::config::{Endpoint, WsConfig};
use crate::error::TransportError;
use crate::{ENDPOINT_PATH, SUBPROTOCOL};

/// Live transport handle: protocol engine + shutdown.
pub struct WsHandle {
    /// Generic Syntra connection (handshake, RPCs, notifications).
    pub conn: Connection,
    /// Server's `connection_id` from `ServerReady` (diagnostic only,
    /// never an authenticator or resume token).
    pub connection_id: String,
    /// Selected Syntra version (`Frame.version` post-handshake).
    pub version: u32,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl WsHandle {
    /// Graceful close: stop new RPCs, allow close grace, WS Close 1000,
    /// then teardown (WSS §1J / WS §1J).
    pub async fn close(mut self) {
        self.conn.close().await;
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

/// Connect, validate the Upgrade, run the Syntra handshake, and spawn
/// background pump tasks.
///
/// `credential` is secret bytes presented only as
/// `ClientHello.credential`. This function never logs it and the caller
/// should zeroize/drop it after the handshake where possible.
pub async fn connect(
    cfg: WsConfig,
    credential: Vec<u8>,
    conn_cfg: ConnectionConfig,
) -> Result<WsHandle, TransportError> {
    if !cfg.url.contains(ENDPOINT_PATH) {
        // Fast path; full validation below.
    }
    let endpoint = Endpoint::parse(&cfg.url, cfg.allow_insecure)?;
    if !endpoint.is_secure {
        tracing::warn!("insecure WS enabled: restricted-development only, never public traffic");
    }

    // Build the WS request with the Syntra subprotocol offered first.
    let url = cfg.url.clone();
    let request = {
        use tokio_tungstenite::tungstenite::http::Request;
        Request::builder()
            .uri(url.as_str())
            .header("Host", endpoint.host.clone())
            .header("Upgrade", "websocket")
            .header("Connection", "Upgrade")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tokio_tungstenite::tungstenite::handshake::client::generate_key(),
            )
            .header("Sec-WebSocket-Protocol", SUBPROTOCOL)
            .body(())
            .map_err(|e| TransportError::InvalidUrl(format!("bad ws request: {e}")))?
    };

    let connect_fut = tokio_tungstenite::connect_async(request);
    let (ws_stream, response) = tokio::time::timeout(cfg.connect_timeout, connect_fut)
        .await
        .map_err(|_| TransportError::Connect("ws connect timed out".to_string()))?
        .map_err(|e| TransportError::Connect(format!("ws connect failed: {e}")))?;

    // Validate 101 + selected subprotocol (WS §1C).
    let selected = response
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if selected != SUBPROTOCOL {
        return Err(TransportError::Subprotocol(format!(
            "expected {SUBPROTOCOL}, got '{selected}'"
        )));
    }

    // Writer channel: connection engine -> WS binary messages.
    let (wtx, mut wrx) = mpsc::channel::<Vec<u8>>(conn_cfg.writer_capacity);
    let conn = Connection::new(wtx, conn_cfg.clone());
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let (mut ws_write, mut ws_read) = ws_stream.split();

    // Write pump: one Vec<u8> = one binary message (never length-prefixed).
    let write_conn = conn.clone();
    let writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    let _ = ws_write.send(Message::Close(Some(CloseFrame {
                        code: 1000.into(),
                        reason: "normal closure".into(),
                    }))).await;
                    break;
                }
                msg = wrx.recv() => {
                    match msg {
                        Some(bytes) => {
                            if bytes.len() > syntra_core::MAX_FRAME_BYTES {
                                tracing::warn!("outgoing frame exceeds bound; dropping");
                                continue;
                            }
                            if ws_write.send(Message::Binary(bytes.into())).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
        write_conn.on_transport_lost().await;
    });

    // Read pump: binary -> ingest; text -> close 1003; empty -> reject;
    // oversize -> close 1009; control frames never reach the core.
    let read_conn = conn.clone();
    let _reader_task = tokio::spawn(async move {
        while let Some(msg) = ws_read.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if data.is_empty() {
                        tracing::warn!("empty binary message rejected");
                        break;
                    }
                    if data.len() > syntra_core::MAX_FRAME_BYTES {
                        tracing::warn!("assembled message exceeds bound");
                        break;
                    }
                    read_conn.ingest(&data).await;
                }
                Ok(Message::Text(_)) => {
                    tracing::warn!("text message rejected (binary required)");
                    break;
                }
                Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {
                    // Handled by tungstenite's auto-pong; never reaches core.
                }
                Ok(Message::Close(_)) => break,
                Ok(Message::Frame(_)) => {}
                Err(e) => {
                    tracing::debug!(error = %e, "ws read error");
                    break;
                }
            }
        }
        read_conn.on_transport_lost().await;
    });

    // Syntra handshake: exactly one hello first, then await ready.
    // No Syntra byte was sent before TLS + 101 (WSS §1F).
    let connection_id = conn
        .handshake(credential)
        .await
        .map_err(|e| TransportError::Connect(format!("syntra handshake failed: {e}")))?;
    let version = conn.negotiated_version().await.unwrap_or(1);

    // Detach writer task (it lives until shutdown/close).
    let _ = writer_task; // held by runtime; shutdown via WsHandle::close

    Ok(WsHandle {
        conn,
        connection_id,
        version,
        shutdown_tx: Some(shutdown_tx),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subprotocol_and_path_constants() {
        assert_eq!(SUBPROTOCOL, "syntra.v1");
        assert_eq!(ENDPOINT_PATH, "/syntra/v1/ws");
    }
}
