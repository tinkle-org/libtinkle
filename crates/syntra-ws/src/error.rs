//! Transport-level errors (mapped to Syntra codes by the connection).

use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum TransportError {
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    #[error("insecure transport denied: {0}")]
    InsecureDenied(String),
    #[error("connect failed: {0}")]
    Connect(String),
    #[error("upgrade rejected: {0}")]
    UpgradeRejected(String),
    #[error("subprotocol mismatch: {0}")]
    Subprotocol(String),
    #[error("tls verification failed: {0}")]
    Tls(String),
    #[error("message too big")]
    TooBig,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("closed: {0}")]
    Closed(String),
    #[error("io: {0}")]
    Io(String),
}
