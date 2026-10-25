//! Structured FFI error surface.
//!
//! Foreign sees named variants with message payloads (e.g. `Storage(String)`);
//! no Syntra, prost, or schema types cross the boundary. [`FfiError`] is built
//! from [`libtinkle::TinkleError`] via strings, preserving variant structure so
//! foreign cursor-store failures round-trip instead of panicking.

/// Errors surfaced to Swift/Kotlin.
///
/// This must not use `flat_error`: foreign `FfiCursorStore` implementations
/// return `FfiError`, and UniFFI cannot lift flat errors back into Rust
/// (`rustPanic("Can't lift flat errors")`). Keeping the message payloads
/// makes store failures round-trip instead of panicking.
#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum FfiError {
    #[error("network error: {0}")]
    Network(String),
    #[error("not connected: call connect() first")]
    NotConnected,
    #[error("operation timed out")]
    Timeout,
    #[error("operation was cancelled")]
    Cancelled,
    #[error("sync error: {0}")]
    Sync(String),
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
                let msg = s.to_string();
                // TODO(runtime-acceptance-errors): Unauthenticated currently maps to
                // Network; add FfiError::AuthExpired variant once mobile auth UX is
                // defined, plus a 17-code matrix test covering every ErrorCode.
                match s.code() {
                    syntra_core::ErrorCode::Cancelled => Self::Cancelled,
                    syntra_core::ErrorCode::DeadlineExceeded => Self::Timeout,
                    syntra_core::ErrorCode::Unspecified
                    | syntra_core::ErrorCode::Unknown
                    | syntra_core::ErrorCode::InvalidArgument
                    | syntra_core::ErrorCode::NotFound
                    | syntra_core::ErrorCode::AlreadyExists
                    | syntra_core::ErrorCode::PermissionDenied
                    | syntra_core::ErrorCode::ResourceExhausted
                    | syntra_core::ErrorCode::FailedPrecondition
                    | syntra_core::ErrorCode::Aborted
                    | syntra_core::ErrorCode::OutOfRange
                    | syntra_core::ErrorCode::Unimplemented
                    | syntra_core::ErrorCode::Internal
                    | syntra_core::ErrorCode::Unavailable
                    | syntra_core::ErrorCode::DataLoss
                    | syntra_core::ErrorCode::Unauthenticated => Self::Network(msg),
                }
            }
            libtinkle::TinkleError::NotConnected(_) => Self::NotConnected,
            libtinkle::TinkleError::ReconnectExhausted { .. } => Self::ReconnectExhausted,
            libtinkle::TinkleError::Sync(msg) => Self::Sync(msg),
            libtinkle::TinkleError::Grpc(msg) => Self::Network(msg),
            libtinkle::TinkleError::Codec(msg) => Self::Codec(msg),
            libtinkle::TinkleError::CursorStore(msg) => Self::Storage(msg),
            // Subscription setup failures are sync-domain failures.
            libtinkle::TinkleError::Subscription(msg) => Self::Sync(msg),
        }
    }
}

impl From<FfiError> for libtinkle::TinkleError {
    /// Folds a foreign store failure back into the core error type. Every
    /// cursor-store operation failure is storage-domain by construction.
    fn from(e: FfiError) -> Self {
        match e {
            FfiError::Storage(msg) => Self::CursorStore(msg),
            other => Self::CursorStore(other.to_string()),
        }
    }
}
