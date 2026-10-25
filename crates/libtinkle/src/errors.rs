//! Typed client errors. Clients branch on variants/codes, never on text.

use thiserror::Error;

use syntra_core::error::{ErrorCode, SyntraError};
use tinkle_schema::sync::SyncError;

#[derive(Debug, Error, Clone)]
pub enum TinkleError {
    #[error(transparent)]
    Syntra(#[from] SyntraError),
    #[error("sync: {0}")]
    Sync(String),
    #[error(transparent)]
    SyncError(#[from] SyncError),
    #[error("grpc: {0}")]
    Grpc(String),
    /// Permanent local gRPC setup failure: endpoint missing, blank,
    /// wrong scheme, or non-`https` without `allow_insecure`. Never
    /// retryable — fix the config, not the network.
    #[error("grpc config: {0}")]
    GrpcConfig(String),
    /// Malformed locally-supplied credential (non-ASCII, invalid header
    /// value). Never retryable — re-authenticate, don't back off.
    #[error("grpc credentials: {0}")]
    GrpcAuth(String),
    #[error("codec: {0}")]
    Codec(String),
    #[error("not connected: {0}")]
    NotConnected(String),
    #[error("subscription: {0}")]
    Subscription(String),
    #[error("cursor store: {0}")]
    CursorStore(String),
    #[error("reconnect exhausted after {attempts} attempts")]
    ReconnectExhausted { attempts: u32 },
}

impl TinkleError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Syntra(e) => e.code(),
            Self::Sync(_) | Self::SyncError(_) => ErrorCode::Internal,
            Self::Grpc(_) => ErrorCode::Unavailable,
            Self::GrpcConfig(_) => ErrorCode::FailedPrecondition,
            Self::GrpcAuth(_) => ErrorCode::InvalidArgument,
            Self::Codec(_) => ErrorCode::InvalidArgument,
            Self::NotConnected(_) => ErrorCode::Unavailable,
            Self::Subscription(_) => ErrorCode::FailedPrecondition,
            Self::CursorStore(_) => ErrorCode::Internal,
            Self::ReconnectExhausted { .. } => ErrorCode::Unavailable,
        }
    }

    pub fn is_unknown_outcome(&self) -> bool {
        match self {
            Self::Syntra(e) => e.is_unknown_outcome(),
            _ => false,
        }
    }
}

impl TinkleError {
    /// True when this error is a [`SyncError::Gap`], regardless of whether
    /// it is carried as typed [`SyncError`] or legacy `Sync(String)`.
    /// Prefer matching `TinkleError::SyncError(SyncError::Gap { .. })`
    /// directly; this helper exists for backward-compatible call sites.
    pub fn is_gap(&self) -> bool {
        match self {
            Self::SyncError(SyncError::Gap { .. }) => true,
            Self::Sync(msg) => {
                let lower = msg.to_ascii_lowercase();
                lower.contains("gap detected") || lower.contains("gap:")
            }
            _ => false,
        }
    }
}
