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
    #[error("grpc: {0}")]
    Grpc(String),
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
            Self::Sync(_) => ErrorCode::Internal,
            Self::Grpc(_) => ErrorCode::Unavailable,
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

impl From<SyncError> for TinkleError {
    fn from(e: SyncError) -> Self {
        Self::Sync(e.to_string())
    }
}
