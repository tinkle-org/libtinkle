//! Secret-free device-session management for native hosts.

use crate::{FfiError, TinkleClient};

/// Foreground transport availability, independent of account retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum AccountNetwork {
    /// A live encrypted connection is available.
    Available,
    /// No live encrypted connection is available.
    Unavailable,
}

/// Authorization knowledge; Unknown must never be interpreted as retired.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum AccountAuthorization {
    /// Current server authorization cannot be established.
    Unknown,
    /// This Returning channel authenticated successfully.
    Active,
    /// This coordinator is irreversibly signed out.
    SignedOut,
}

/// Retained local cleanup progress, distinct from remote revocation confirmation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum AccountCleanup {
    /// Local cleanup has not started.
    NotStarted,
    /// Cleanup runs independently of host waiters.
    Pending,
    /// Required local storage and cache work acknowledged completion.
    Complete,
    /// Cleanup needs a retry; the coordinator remains signed out.
    Failed,
}

/// Last independent enrollment observation; only Retired supplies negative evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum AccountEnrollmentObservation {
    /// No observation has completed.
    NotObserved,
    /// No usable permanent-retirement evidence was returned.
    Unknown,
    /// Exact current retirement was accepted; inspect cleanup separately.
    Retired,
    /// A newer installation or protected namespace invalidated the observation.
    Superseded,
    /// The bounded request did not obtain a usable response.
    Unavailable,
    /// Endpoint proof is rejected or unverified pending explicit recovery.
    Parked,
    /// Conditional host persistence is unsupported.
    Unsupported,
}

/// Account lifecycle query, separate from contact connectivity observations.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AccountStatus {
    /// Current encrypted transport availability.
    pub network: AccountNetwork,
    /// Current authorization knowledge.
    pub authorization: AccountAuthorization,
    /// Retained local cleanup status.
    pub cleanup: AccountCleanup,
    /// Restart-safe completion additionally requires cleanup == Complete.
    pub restart_safe_store: bool,
    /// Negative-only observation, never confirmation of a Logout invocation.
    pub enrollment_observation: AccountEnrollmentObservation,
}

impl From<libtinkle::SessionStatus> for AccountStatus {
    /// Preserve all lifecycle distinctions without inferring retirement from errors.
    fn from(status: libtinkle::SessionStatus) -> Self {
        Self {
            network: match status.network {
                libtinkle::SessionNetwork::Available => AccountNetwork::Available,
                libtinkle::SessionNetwork::Unavailable => AccountNetwork::Unavailable,
            },
            authorization: match status.authorization {
                libtinkle::SessionAuthorization::Unknown => AccountAuthorization::Unknown,
                libtinkle::SessionAuthorization::Active => AccountAuthorization::Active,
                libtinkle::SessionAuthorization::SignedOut => AccountAuthorization::SignedOut,
            },
            cleanup: match status.cleanup {
                libtinkle::SessionCleanup::NotStarted => AccountCleanup::NotStarted,
                libtinkle::SessionCleanup::Pending => AccountCleanup::Pending,
                libtinkle::SessionCleanup::Complete => AccountCleanup::Complete,
                libtinkle::SessionCleanup::Failed => AccountCleanup::Failed,
            },
            restart_safe_store: status.restart_safe_store,
            enrollment_observation: match status.enrollment_observation {
                libtinkle::EnrollmentObservation::NotObserved => {
                    AccountEnrollmentObservation::NotObserved
                }
                libtinkle::EnrollmentObservation::Unknown => AccountEnrollmentObservation::Unknown,
                libtinkle::EnrollmentObservation::Retired => AccountEnrollmentObservation::Retired,
                libtinkle::EnrollmentObservation::Superseded => {
                    AccountEnrollmentObservation::Superseded
                }
                libtinkle::EnrollmentObservation::Unavailable => {
                    AccountEnrollmentObservation::Unavailable
                }
                libtinkle::EnrollmentObservation::Parked => AccountEnrollmentObservation::Parked,
                libtinkle::EnrollmentObservation::Unsupported => {
                    AccountEnrollmentObservation::Unsupported
                }
            },
        }
    }
}

/// One active device; identifiers and descriptive labels are not credentials.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AccountSession {
    /// Neutral session UUID used for display and revocation.
    pub id: String,
    /// Client-reported device model.
    pub device_model: String,
    /// Client-reported platform.
    pub platform: String,
    /// Client-reported operating system version.
    pub system_version: String,
    /// Client-reported application version.
    pub app_version: String,
    /// Application build identifier, preserving leading zeroes.
    pub app_build: String,
    /// Creation time in Unix seconds.
    pub created_date: i64,
    /// Last activity time in Unix seconds.
    pub active_date: i64,
    /// Server-reported address; hosts must treat this as personal data.
    pub ip: String,
    /// Country code, empty when unknown.
    pub country: String,
    /// Region, empty when unknown.
    pub region: String,
    /// Whether this session issued the listing request.
    pub current: bool,
}

impl From<tinkle_schema::proto::Session> for AccountSession {
    /// Transfer the complete secret-free projection without parsing labels or dates.
    fn from(session: tinkle_schema::proto::Session) -> Self {
        Self {
            id: session.id,
            device_model: session.device_model,
            platform: session.platform,
            system_version: session.system_version,
            app_version: session.app_version,
            app_build: session.app_build,
            created_date: session.created_date,
            active_date: session.active_date,
            ip: session.ip,
            country: session.country,
            region: session.region,
            current: session.current,
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl TinkleClient {
    /// Query transport, authorization, and retained cleanup without network I/O.
    pub async fn session_status(&self) -> AccountStatus {
        self.core_client().session_status().await.into()
    }

    /// List active devices on the current authenticated channel.
    pub async fn get_sessions(&self) -> Result<Vec<AccountSession>, FfiError> {
        self.core_client()
            .account()
            .get_sessions()
            .await
            .map(|sessions| sessions.into_iter().map(AccountSession::from).collect())
            .map_err(FfiError::from)
    }

    /// Revoke one neutral session UUID once; unknown outcomes are never replayed.
    pub async fn revoke_session(&self, session_id: String) -> Result<bool, FfiError> {
        self.core_client()
            .account()
            .revoke_session(&session_id)
            .await
            .map_err(FfiError::from)
    }

    /// Revoke every other device while retaining this device's enrollment.
    pub async fn revoke_other_sessions(&self) -> Result<bool, FfiError> {
        self.core_client()
            .account()
            .revoke_other_sessions()
            .await
            .map_err(FfiError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mapping preserves every public field, including large dates and build strings.
    #[test]
    fn session_projection_preserves_all_fields() {
        let session = tinkle_schema::proto::Session {
            id: "78e5c289-656b-4428-a091-2dddf6828465".into(),
            device_model: "device".into(),
            platform: "ios".into(),
            system_version: "1".into(),
            app_version: "2".into(),
            app_build: "00042".into(),
            created_date: i64::MAX - 1,
            active_date: i64::MAX,
            ip: "127.0.0.1".into(),
            country: "US".into(),
            region: "CA".into(),
            current: true,
        };
        let mapped = AccountSession::from(session.clone());
        assert_eq!(mapped.id, session.id);
        assert_eq!(mapped.device_model, session.device_model);
        assert_eq!(mapped.platform, session.platform);
        assert_eq!(mapped.system_version, session.system_version);
        assert_eq!(mapped.app_version, session.app_version);
        assert_eq!(mapped.app_build, session.app_build);
        assert_eq!(mapped.created_date, session.created_date);
        assert_eq!(mapped.active_date, session.active_date);
        assert_eq!(mapped.ip, session.ip);
        assert_eq!(mapped.country, session.country);
        assert_eq!(mapped.region, session.region);
        assert_eq!(mapped.current, session.current);
    }
}
