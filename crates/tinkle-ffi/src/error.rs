//! Flat FFI error surface.
//!
//! Foreign code sees variant names plus a Display message only
//! (`flat_error`). No Syntra, prost, or schema types cross the boundary;
//! [`FfiError`] is built from [`libtinkle::TinkleError`] via strings.

/// Errors surfaced to Swift/Kotlin.
#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
#[uniffi(flat_error)]
pub enum FfiError {
    #[error("network error: {0}")]
    Network(String),
    #[error("not connected: call connect() first")]
    NotConnected,
    #[error("operation timed out")]
    Timeout,
    #[error("operation was cancelled")]
    Cancelled,
    #[error("realtime error: {0}")]
    Realtime(String),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("cursor store error: {0}")]
    Storage(String),
    #[error("reconnect attempts exhausted")]
    ReconnectExhausted,
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<libtinkle::TinkleError> for FfiError {
    fn from(e: libtinkle::TinkleError) -> Self {
        match e {
            libtinkle::TinkleError::Syntra(s) => {
                // Classify without naming Syntra types: `code()` returns a
                // code whose Debug rendering is the stable variant name.
                let msg = s.to_string();
                match format!("{:?}", s.code()).as_str() {
                    "Cancelled" => Self::Cancelled,
                    "DeadlineExceeded" => Self::Timeout,
                    _ => Self::Network(msg),
                }
            }
            libtinkle::TinkleError::NotConnected(_) => Self::NotConnected,
            libtinkle::TinkleError::ReconnectExhausted { .. } => Self::ReconnectExhausted,
            libtinkle::TinkleError::Realtime(msg) => Self::Realtime(msg),
            libtinkle::TinkleError::Codec(msg) => Self::Codec(msg),
            libtinkle::TinkleError::CursorStore(msg) => Self::Storage(msg),
            // Subscription setup failures are realtime-domain failures.
            libtinkle::TinkleError::Subscription(msg) => Self::Realtime(msg),
        }
    }
}

impl From<FfiError> for libtinkle::TinkleError {
    /// Folds a foreign store failure back into the core error type. Every
    /// cursor-store operation failure is storage-domain by construction.
    fn from(e: FfiError) -> Self {
        Self::CursorStore(e.to_string())
    }
}
