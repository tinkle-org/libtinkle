//! Typed client errors. Clients branch on variants/codes, never on text.

use thiserror::Error;

use syntra_core::error::{ErrorCode, SyntraError};
use tinkle_schema::sync::SyncError;

#[derive(Debug, Error, Clone)]
pub enum TinkleError {
    #[error("client is signed out; credentials cannot be restored on this client")]
    SignedOut,
    #[error("protected enrollment storage did not acknowledge durable persistence")]
    SecureStore,
    #[error(transparent)]
    Syntra(#[from] SyntraError),
    #[error("sync: {0}")]
    Sync(String),
    #[error(transparent)]
    SyncError(#[from] SyncError),
    /// Invalid local endpoint, pin, scope or enrollment tuple.
    #[error("configuration: {0}")]
    Configuration(String),
    /// A dial or established carrier became unavailable.
    #[error("native TCP unavailable")]
    NetworkUnavailable,
    /// Channel proof, framing or authentication rejected. No fallback/retry.
    #[error("native TCP trust or protocol rejected")]
    TrustRejected,
    /// Peer readiness was never authenticated. EOF/timeouts here cannot safely
    /// be distinguished from a rejected first-flight proof, so do not retry.
    #[error("native TCP establishment failed before authenticated readiness")]
    EstablishmentFailed,
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
    /// Return the typed recovery code without interpreting diagnostic text.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::SignedOut => ErrorCode::Unauthenticated,
            Self::SecureStore => ErrorCode::FailedPrecondition,
            Self::Syntra(e) => e.code(),
            Self::Sync(_) | Self::SyncError(_) => ErrorCode::Internal,
            Self::NetworkUnavailable => ErrorCode::Unavailable,
            Self::Configuration(_) => ErrorCode::FailedPrecondition,
            Self::TrustRejected => ErrorCode::Unauthenticated,
            Self::EstablishmentFailed => ErrorCode::FailedPrecondition,
            Self::Codec(_) => ErrorCode::InvalidArgument,
            Self::NotConnected(_) => ErrorCode::Unavailable,
            Self::Subscription(_) => ErrorCode::FailedPrecondition,
            Self::CursorStore(_) => ErrorCode::Internal,
            Self::CheckpointFailed { .. } => ErrorCode::Internal,
            Self::Reconcile { .. } => ErrorCode::Internal,
            Self::ReconnectExhausted { .. } => ErrorCode::Unavailable,
        }
    }

    /// Report whether a Syntra failure leaves the remote RPC outcome ambiguous.
    pub fn is_unknown_outcome(&self) -> bool {
        match self {
            Self::Syntra(e) => e.is_unknown_outcome(),
            _ => false,
        }
    }
}

impl TinkleError {
    /// Classify a reconcile-page failure. Syntra RPC failures key on their code: journal reads
    /// are idempotent, so unknown-outcome and transient codes retry.
    /// Everything else keys on the typed status code.
    pub fn recovery_decision(&self) -> RecoveryDecision {
        match self {
            Self::SignedOut => RecoveryDecision::Auth,
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
            Self::NetworkUnavailable => RecoveryDecision::Retry,
            Self::TrustRejected => RecoveryDecision::Auth,
            _ => RecoveryDecision::Fail,
        }
    }

    /// Classify network failures without retaining potentially sensitive transport diagnostics.
    pub(crate) fn from_network(error: syntra_net::Error) -> Self {
        match error {
            syntra_net::Error::Io(_) | syntra_net::Error::Timeout | syntra_net::Error::Closed => {
                Self::NetworkUnavailable
            }
            syntra_net::Error::Configuration => {
                Self::Configuration("Syntra limits rejected".into())
            }
            _ => Self::TrustRejected,
        }
    }

    /// Keep pre-readiness I/O failures distinct from retryable established-network loss.
    pub(crate) fn from_establishment(error: syntra_net::Error) -> Self {
        match Self::from_network(error) {
            Self::NetworkUnavailable => Self::EstablishmentFailed,
            other => other,
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
        TinkleError::Syntra(SyntraError::Rpc {
            code,
            raw_code: None,
            message: "injected".into(),
        })
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
            TinkleError::Syntra(SyntraError::Rpc {
                code: ErrorCode::OutOfRange,
                raw_code: None,
                message: "timeout reading past retention".into(),
            })
            .recovery_decision(),
            RecoveryDecision::Rebuild
        );
        assert_eq!(
            TinkleError::NetworkUnavailable.recovery_decision(),
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
            TinkleError::Configuration("bad endpoint".into()).recovery_decision(),
            RecoveryDecision::Fail
        );
        assert_eq!(
            TinkleError::NotConnected("x".into()).recovery_decision(),
            RecoveryDecision::Fail
        );
    }

    #[test]
    fn rpc_status_preserves_code() {
        let e = status(ErrorCode::OutOfRange);
        assert_eq!(e.code(), ErrorCode::OutOfRange);
    }
}
