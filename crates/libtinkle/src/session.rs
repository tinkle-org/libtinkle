//! Queryable account lifecycle, distinct from connectivity notifications.

use crate::Client;

/// Current transport availability; loss alone says nothing about retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionNetwork {
    /// A live encrypted connection is available.
    Available,
    /// No usable connection is currently observed.
    Unavailable,
}

/// What this coordinator knows about its current authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAuthorization {
    /// No current authenticated Returning proof, or an account RPC was denied.
    Unknown,
    /// The current Returning channel authenticated successfully.
    Active,
    /// This coordinator has irreversibly fenced local authorization.
    SignedOut,
}

/// Retained local cleanup progress; separate from remote revocation confirmation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SessionCleanup {
    /// Local sign-out has not started.
    #[default]
    NotStarted,
    /// An independently owned cleanup attempt is running.
    Pending,
    /// Every required local step acknowledged completion.
    Complete,
    /// Local cleanup needs a retry; authorization remains fenced.
    Failed,
}

/// Account lifecycle query. No error code or network loss implies retirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStatus {
    /// Availability of the current foreground transport.
    pub network: SessionNetwork,
    /// Current authorization knowledge, including irreversible local sign-out.
    pub authorization: SessionAuthorization,
    /// Progress of acknowledged local cleanup.
    pub cleanup: SessionCleanup,
    /// All bound hosts support conditional writes and durable cleanup tombstones.
    /// Restart-safe completion requires this and cleanup == Complete.
    pub restart_safe_store: bool,
    /// Last independent enrollment observation; it never confirms a Logout invocation.
    pub enrollment_observation: crate::EnrollmentObservation,
}

impl Client {
    /// Inspect lifecycle state without issuing a request or scheduling a connection.
    pub async fn session_status(&self) -> SessionStatus {
        let connected = self.is_connected().await;
        let authorization = if self.is_signed_out() {
            SessionAuthorization::SignedOut
        } else if connected
            && self.authorization.authentication_known()
            && !self
                .endpoint_rejected
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            SessionAuthorization::Active
        } else {
            SessionAuthorization::Unknown
        };
        SessionStatus {
            network: if connected {
                SessionNetwork::Available
            } else {
                SessionNetwork::Unavailable
            },
            authorization,
            cleanup: self.logout_cleanup.status(),
            restart_safe_store: self.authorization.restart_safe_store(),
            enrollment_observation: self.retirement.observation(),
        }
    }
}
