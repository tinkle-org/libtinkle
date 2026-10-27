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
    /// gRPC status with its code preserved. Recovery decisions key on the
    /// code, never on message text.
    #[error("grpc status {code:?}: {message}")]
    GrpcStatus { code: ErrorCode, message: String },
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
    #[error("checkpoint failed for {target_key} at {accept_seq}: {error}")]
    CheckpointFailed {
        target_key: String,
        accept_seq: i64,
        error: String,
    },
    #[error("reconcile failed for {target_key}: {error}")]
    Reconcile { target_key: String, error: String },
    #[error("reconnect exhausted after {attempts} attempts")]
    ReconnectExhausted { attempts: u32 },
}

impl TinkleError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Syntra(e) => e.code(),
            Self::Sync(_) | Self::SyncError(_) => ErrorCode::Internal,
            Self::Grpc(_) => ErrorCode::Unavailable,
            Self::GrpcStatus { code, .. } => *code,
            Self::GrpcConfig(_) => ErrorCode::FailedPrecondition,
            Self::GrpcAuth(_) => ErrorCode::InvalidArgument,
            Self::Codec(_) => ErrorCode::InvalidArgument,
            Self::NotConnected(_) => ErrorCode::Unavailable,
            Self::Subscription(_) => ErrorCode::FailedPrecondition,
            Self::CursorStore(_) => ErrorCode::Internal,
            Self::CheckpointFailed { .. } => ErrorCode::Internal,
            Self::Reconcile { .. } => ErrorCode::Internal,
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
    /// Classify a reconcile-page failure. Transport-level `Grpc` failures
    /// (connect/read framing, no status) are transient by construction.
    /// Syntra RPC failures key on their code the same way: journal reads
    /// are idempotent, so unknown-outcome and transient codes retry.
    /// Everything else keys on the typed status code.
    pub fn recovery_decision(&self) -> RecoveryDecision {
        match self {
            Self::GrpcStatus { code, .. } => match code {
                ErrorCode::Unavailable
                | ErrorCode::DeadlineExceeded
                | ErrorCode::Aborted
                | ErrorCode::ResourceExhausted
                | ErrorCode::Internal
                | ErrorCode::Unknown
                | ErrorCode::DataLoss => RecoveryDecision::Retry,
                ErrorCode::OutOfRange => RecoveryDecision::Rebuild,
                ErrorCode::Unauthenticated | ErrorCode::PermissionDenied => RecoveryDecision::Auth,
                _ => RecoveryDecision::Fail,
            },
            Self::Syntra(e) => match e.code() {
                ErrorCode::Unavailable
                | ErrorCode::DeadlineExceeded
                | ErrorCode::ResourceExhausted
                | ErrorCode::Aborted
                | ErrorCode::Internal
                | ErrorCode::Unknown
                | ErrorCode::DataLoss => RecoveryDecision::Retry,
                ErrorCode::OutOfRange => RecoveryDecision::Rebuild,
                ErrorCode::Unauthenticated | ErrorCode::PermissionDenied => RecoveryDecision::Auth,
                _ => RecoveryDecision::Fail,
            },
            Self::Grpc(_) => RecoveryDecision::Retry,
            _ => RecoveryDecision::Fail,
        }
    }
}

/// Recovery decision for a failed reconcile page. Keys on typed codes;
/// message text never drives recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryDecision {
    /// Transient failure: back off and retry the page.
    Retry,
    /// Retention gap: the journal no longer covers the cursor (e.g.
    /// `OutOfRange`). The run fails without moving the cursor, parks the
    /// target in `Failed` with a `ReconcileFailed` event, and retries via
    /// `reconcile_now`/supervisor backoff. Skips observed on the next
    /// successful page heal best-effort via `GetHistory` without moving
    /// the cursor; there is no cursor reset or snapshot fencing.
    Rebuild,
    /// Authentication/authorization failure: re-authenticate, do not retry.
    Auth,
    /// Anything else: surface to the caller without retrying.
    Fail,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(code: ErrorCode) -> TinkleError {
        TinkleError::GrpcStatus {
            code,
            message: "injected".into(),
        }
    }

    #[test]
    fn classifier_keys_on_code_not_message() {
        for code in [
            ErrorCode::Unavailable,
            ErrorCode::DeadlineExceeded,
            ErrorCode::Aborted,
            ErrorCode::ResourceExhausted,
            ErrorCode::Internal,
            ErrorCode::Unknown,
            ErrorCode::DataLoss,
        ] {
            assert_eq!(
                status(code).recovery_decision(),
                RecoveryDecision::Retry,
                "{code:?} must retry"
            );
        }
        assert_eq!(
            TinkleError::GrpcStatus {
                code: ErrorCode::OutOfRange,
                message: "timeout reading past retention".into(),
            }
            .recovery_decision(),
            RecoveryDecision::Rebuild
        );
        assert_eq!(
            TinkleError::Grpc("connect: refused".into()).recovery_decision(),
            RecoveryDecision::Retry
        );
        for code in [ErrorCode::Unauthenticated, ErrorCode::PermissionDenied] {
            assert_eq!(
                status(code).recovery_decision(),
                RecoveryDecision::Auth,
                "{code:?} must not retry"
            );
        }
        for code in [
            ErrorCode::InvalidArgument,
            ErrorCode::NotFound,
            ErrorCode::AlreadyExists,
            ErrorCode::FailedPrecondition,
            ErrorCode::Unimplemented,
            ErrorCode::Cancelled,
        ] {
            assert_eq!(
                status(code).recovery_decision(),
                RecoveryDecision::Fail,
                "{code:?} must fail fast"
            );
        }
        assert_eq!(
            TinkleError::GrpcConfig("bad endpoint".into()).recovery_decision(),
            RecoveryDecision::Fail
        );
        assert_eq!(
            TinkleError::NotConnected("x".into()).recovery_decision(),
            RecoveryDecision::Fail
        );
    }

    #[test]
    fn grpc_status_preserves_code() {
        let e = status(ErrorCode::OutOfRange);
        assert_eq!(e.code(), ErrorCode::OutOfRange);
    }
}
