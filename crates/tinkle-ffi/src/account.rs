//! Secret-free device-session management for native hosts.

use crate::{FfiError, TinkleClient};
use std::sync::Arc;

/// First cause of irreversible local sign-out; cleanup retries never replace it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum AccountSignOutReason {
    /// The host requested local sign-out.
    SelfLogout,
    /// Exact current authority evidence retired this enrollment.
    SessionRetired,
}

/// Retained, secret-free account state; independent of contact connectivity events.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AccountState {
    /// Monotonic local revision, not a server checkpoint.
    pub revision: u64,
    /// The authoritative lifecycle view also returned by session_status.
    pub status: AccountStatus,
    /// Immutable sign-out cause when known.
    pub reason: Option<AccountSignOutReason>,
    /// None means unavailable/unfetched, Some(empty) is a fetched empty list.
    /// Hosts prompt only for unconfirmed && !current; confirmation eligibility
    /// requires a confirmed current session and is enforced by the server.
    pub sessions: Option<Vec<AccountSession>>,
}

impl From<libtinkle::SessionState> for AccountState {
    /// Project owned records only; no prost or credential-bearing objects cross UniFFI.
    fn from(state: libtinkle::SessionState) -> Self {
        Self {
            revision: state.revision,
            status: state.status.into(),
            reason: state.reason.map(|r| match r {
                libtinkle::SessionSignOutReason::SelfLogout => AccountSignOutReason::SelfLogout,
                libtinkle::SessionSignOutReason::SessionRetired => {
                    AccountSignOutReason::SessionRetired
                }
            }),
            sessions: state
                .sessions
                .map(|sessions| sessions.into_iter().map(Into::into).collect()),
        }
    }
}

/// Opaque one-consumer subscription. It does not retain TinkleClient or a foreign callback.
#[derive(uniffi::Object)]
pub struct AccountStateSubscription {
    inner: libtinkle::SessionStateSubscription,
}

#[uniffi::export(async_runtime = "tokio")]
impl AccountStateSubscription {
    /// Await the initial/latest snapshot. Stop or Client release returns None.
    pub async fn next(&self) -> Option<AccountState> {
        self.inner.next().await.map(Into::into)
    }
    /// Unblock pending next calls and release the consumer slot synchronously.
    pub fn stop(&self) {
        self.inner.stop();
    }
}

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
    /// True until another confirmed session recognizes this login; never prompt for current.
    pub unconfirmed: bool,
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
            unconfirmed: session.unconfirmed,
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl TinkleClient {
    /// Atomically register the Client's sole app consumer with the latest retained state.
    pub async fn subscribe_session_state(&self) -> Result<Arc<AccountStateSubscription>, FfiError> {
        Ok(Arc::new(AccountStateSubscription {
            inner: self
                .core_client()
                .subscribe_session_state()
                .await
                .map_err(FfiError::from)?,
        }))
    }

    /// Confirm one other login, once. Unknown mutation outcomes must not be automatically replayed.
    pub async fn confirm_session(&self, session_id: String) -> Result<AccountSession, FfiError> {
        self.core_client()
            .account()
            .confirm_session(&session_id)
            .await
            .map(Into::into)
            .map_err(confirmation_error)
    }
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
            .map_err(revocation_error)
    }

    /// Revoke every other device while retaining this device's enrollment.
    /// An ambiguous result is Network with an unknown outcome, never Retryable;
    /// do not automatically repeat this mutation after response loss or timeout.
    pub async fn revoke_other_sessions(&self) -> Result<bool, FfiError> {
        self.core_client()
            .account()
            .revoke_other_sessions()
            .await
            .map_err(revocation_error)
    }
}

/// Keep ambiguous revocation outcomes nonretryable without changing other error mappings.
fn revocation_error(error: libtinkle::TinkleError) -> FfiError {
    if error.is_unknown_outcome() {
        FfiError::Network("revocation outcome unknown; do not retry automatically".into())
    } else {
        FfiError::from(error)
    }
}

/// A lost confirmation reply is not a retryable read or proof that the mutation did not commit.
fn confirmation_error(error: libtinkle::TinkleError) -> FfiError {
    if error.is_unknown_outcome() {
        FfiError::Network("confirmation outcome unknown; do not retry automatically".into())
    } else {
        FfiError::from(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Offline native construction exercises the real proc-macro exports without credentials.
    fn offline_client() -> Arc<TinkleClient> {
        TinkleClient::new(
            crate::ClientConfigFfi {
                host: "127.0.0.1".into(),
                primary_port: 9,
                fallback_port: None,
                client_info: crate::ClientInfoFfi {
                    device_model: "fixture".into(),
                    platform: "test".into(),
                    os_version: "1".into(),
                    app_version: "1".into(),
                    app_build: "0001".into(),
                    options: crate::ClientOptionsFfi {
                        agent: String::new(),
                        device_id: String::new(),
                        install_id: String::new(),
                        accept_language: String::new(),
                        time_zone: String::new(),
                        user_agent: String::new(),
                    },
                },
            },
            crate::TrustSourceFfi::bootstrap(crate::ServerTrustFfi {
                server_public_key: vec![1; 32],
                server_key_id: 7,
                scope: "fixture".into(),
            })
            .unwrap(),
            crate::InMemoryFfiStore::new(),
        )
        .unwrap()
    }

    #[tokio::test]
    /// Cancelling a next waiter does not consume the single app consumer or discard terminal state.
    async fn exported_state_subscription_cancellation_stop_and_late_attach() {
        let client = offline_client();
        let stream = client.subscribe_session_state().await.unwrap();
        assert_eq!(
            stream.next().await.unwrap().status,
            client.session_status().await
        );
        assert!(client.subscribe_session_state().await.is_err());
        let waiting = stream.clone();
        let task = tokio::spawn(async move { waiting.next().await });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;
        client.logout().await.unwrap();
        let terminal = stream.next().await.unwrap();
        assert_eq!(terminal.reason, Some(AccountSignOutReason::SelfLogout));
        assert_eq!(
            terminal.status.authorization,
            AccountAuthorization::SignedOut
        );
        assert_eq!(terminal.status.cleanup, AccountCleanup::Complete);
        stream.stop();
        assert!(stream.next().await.is_none());
        let late = client.subscribe_session_state().await.unwrap();
        assert_eq!(late.next().await.unwrap(), terminal);
        client.shutdown().await;
        assert!(late.next().await.is_none());
    }

    #[test]
    /// Missing and explicitly empty snapshots stay distinct and review flags are not inferred.
    fn exported_state_projection_preserves_review_flags_and_absence() {
        let session = AccountSession::from(tinkle_schema::proto::Session {
            unconfirmed: true,
            current: false,
            ..Default::default()
        });
        assert!(session.unconfirmed && !session.current);
        let current = AccountSession::from(tinkle_schema::proto::Session {
            unconfirmed: true,
            current: true,
            ..Default::default()
        });
        assert!(current.unconfirmed && current.current);
    }

    #[tokio::test]
    #[ignore = "run via scripts/with-native-fixture.py --port 0 cargo test -p tinkle-ffi exported_targeted_revocation_returning_lost_response -- --ignored"]
    /// Lose a synthetic Returning mutation reply; reconnect must preserve peers without replay.
    /// This exercises SDK transport/exports, not account authority or retirement policy.
    async fn exported_targeted_revocation_returning_lost_response_is_unknown_and_never_replayed() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };
        use tokio::{
            io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
            net::{TcpListener, TcpStream},
        };

        let regression = async {
            let port: u16 = std::env::var("TINKLE_NATIVE_FIXTURE_CONTROL_PORT")
                .expect("run with the native fixture wrapper")
                .parse()
                .unwrap();
            let mut control =
                BufReader::new(TcpStream::connect(("127.0.0.1", port)).await.unwrap());
            control
                .get_mut()
                .write_all(
                    b"{\"op\":\"create\",\"session_review\":true,\"targeted_revocation\":true}\n",
                )
                .await
                .unwrap();
            let mut reply = String::new();
            control.read_line(&mut reply).await.unwrap();
            // This fixture control reply contains only numeric id/port/pin fields.
            // Application payloads and Syntra framing remain entirely SDK-owned.
            let field = |name: &str| {
                reply
                    .split_once(&format!("\"{name}\":"))
                    .expect("missing fixture field")
                    .1
            };
            let id: u64 = field("id")
                .split([',', '}'])
                .next()
                .unwrap()
                .parse()
                .unwrap();
            let gateway_port: u16 = field("port")
                .split([',', '}'])
                .next()
                .unwrap()
                .parse()
                .unwrap();
            let pin: Vec<u8> = field("pin")
                .strip_prefix('[')
                .unwrap()
                .split(']')
                .next()
                .unwrap()
                .split(',')
                .map(|byte| byte.parse().unwrap())
                .collect();
            assert_eq!(pin.len(), 32);
            let relay = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let relay_port = relay.local_addr().unwrap().port();
            let discard = Arc::new(AtomicBool::new(false));
            let discarded = Arc::new(AtomicBool::new(false));
            let discard_next = discard.clone();
            let observed = discarded.clone();
            let relay_task = tokio::spawn(async move {
                // Forward opaque bytes only: Syntra owns both encrypted handshakes and RPC framing.
                for connection in 0..2 {
                    let (mut client, _) = relay.accept().await.unwrap();
                    let mut gateway = TcpStream::connect(("127.0.0.1", gateway_port))
                        .await
                        .unwrap();
                    if connection == 1 {
                        tokio::io::copy_bidirectional(&mut client, &mut gateway)
                            .await
                            .unwrap();
                        continue;
                    }
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut gateway_read, mut gateway_write) = gateway.into_split();
                    tokio::select! {
                        result = tokio::io::copy(&mut client_read, &mut gateway_write) => { result.unwrap(); }
                        _ = async {
                            let mut bytes = [0; 8192];
                            loop {
                                let count = gateway_read.read(&mut bytes).await.unwrap();
                                if count == 0 { break; }
                                if discard_next.load(Ordering::SeqCst) {
                                    observed.store(true, Ordering::SeqCst);
                                    break;
                                }
                                client_write.write_all(&bytes[..count]).await.unwrap();
                            }
                        } => {}
                    }
                }
            });
            let client = TinkleClient::new(
                crate::ClientConfigFfi {
                    host: "127.0.0.1".into(),
                    primary_port: relay_port,
                    fallback_port: None,
                    client_info: crate::ClientInfoFfi {
                        device_model: "fixture".into(),
                        platform: "test".into(),
                        os_version: "1".into(),
                        app_version: "1".into(),
                        app_build: "1".into(),
                        options: crate::ClientOptionsFfi {
                            agent: String::new(),
                            device_id: String::new(),
                            install_id: String::new(),
                            accept_language: String::new(),
                            time_zone: String::new(),
                            user_agent: String::new(),
                        },
                    },
                },
                crate::TrustSourceFfi::bootstrap(crate::ServerTrustFfi {
                    server_public_key: pin.clone(),
                    server_key_id: 7,
                    scope: "fixture".into(),
                })
                .unwrap(),
                crate::InMemoryFfiStore::new(),
            )
            .unwrap();
            // Test-only host adoption of validated synthetic ACTIVE material. No public bypass API.
            let mut active = tinkle_schema::proto::DeviceEnrollmentStatus {
                operation_id: "11111111-1111-4111-8111-111111111111".into(),
                session_id: "22222222-2222-4222-8222-222222222222".into(),
                handle: vec![8; 8].into(),
                enrollment_epoch: 1,
                account_auth_epoch: 1,
                user_id: 42,
                scope: tinkle_schema::proto::DeviceKeyScope::Account as i32,
                state: tinkle_schema::proto::DeviceEnrollmentState::Active as i32,
                expires_at: Some(Default::default()),
            };
            active.expires_at.as_mut().unwrap().seconds = 253_402_300_799;
            client
                .core_client()
                .replace_trust_source(libtinkle::TrustSource::Returning {
                    trust: libtinkle::ServerTrust {
                        server_public_key: pin.try_into().unwrap(),
                        server_key_id: 7,
                        scope: "fixture".into(),
                    },
                    enrollment: libtinkle::Enrollment::from_active_status([0x22; 32], &active)
                        .unwrap(),
                })
                .await
                .unwrap();
            let states = client.subscribe_session_state().await.unwrap();
            client.connect().await.unwrap();
            let before = loop {
                let state = states.next().await.unwrap();
                if let Some(sessions) = state.sessions {
                    assert_eq!(state.status.authorization, AccountAuthorization::Active);
                    break sessions;
                }
            };
            assert_eq!(before.len(), 3);
            let target = "78e5c289-656b-4428-a091-2dddf6828465";
            assert!(before
                .iter()
                .any(|session| session.id == target && session.unconfirmed && !session.current));
            // Initial snapshot has arrived; discard ciphertext only after initialization/readiness.
            discard.store(true, Ordering::SeqCst);
            let error = client.revoke_session(target.into()).await.unwrap_err();
            assert!(
                discarded.load(Ordering::SeqCst),
                "the peer reply must actually be lost"
            );
            assert!(matches!(error, FfiError::Network(message)
                if message == "revocation outcome unknown; do not retry automatically"));
            assert!(!client.is_signed_out());
            loop {
                let state = states.next().await.unwrap();
                assert_eq!(state.reason, None);
                assert_eq!(state.status.cleanup, AccountCleanup::NotStarted);
                if state.status.network == AccountNetwork::Unavailable {
                    assert_eq!(state.status.authorization, AccountAuthorization::Unknown);
                    assert!(
                        state.sessions.is_none(),
                        "loss is not an authoritative empty snapshot"
                    );
                    break;
                }
            }
            client.reconnect().await.unwrap();
            let remaining: Vec<_> = before
                .into_iter()
                .filter(|session| session.id != target)
                .collect();
            loop {
                let state = states.next().await.unwrap();
                if let Some(sessions) = state.sessions {
                    assert_eq!(state.status.network, AccountNetwork::Available);
                    assert_eq!(state.status.authorization, AccountAuthorization::Active);
                    assert_eq!(state.status.cleanup, AccountCleanup::NotStarted);
                    assert_eq!(state.reason, None);
                    assert_eq!(
                        sessions, remaining,
                        "full snapshot preserves caller and untouched peer"
                    );
                    break;
                }
            }
            assert_eq!(client.get_sessions().await.unwrap(), remaining);
            // Query executions only after reconnect and a successful exported read on that channel.
            let mut control =
                BufReader::new(TcpStream::connect(("127.0.0.1", port)).await.unwrap());
            control
                .get_mut()
                .write_all(format!("{{\"op\":\"requests\",\"id\":{id}}}\n").as_bytes())
                .await
                .unwrap();
            let mut log = String::new();
            control.read_line(&mut log).await.unwrap();
            assert!(
                log.contains(&format!("\"revocations\":[\"{target}\"]")),
                "exactly one server execution required"
            );
            assert!(
                log.contains("\"revocation_mutations\":1"),
                "target mutation must have executed before reply loss"
            );
            assert!(
                log.contains("\"admissions\":[\"returning\",\"returning\"]"),
                "both connections must be encrypted Returning"
            );
            states.stop();
            client.shutdown().await;
            relay_task.await.unwrap();
            let mut control =
                BufReader::new(TcpStream::connect(("127.0.0.1", port)).await.unwrap());
            control
                .get_mut()
                .write_all(format!("{{\"op\":\"close\",\"id\":{id}}}\n").as_bytes())
                .await
                .unwrap();
            let mut reply = String::new();
            control.read_line(&mut reply).await.unwrap();
            assert_eq!(reply.trim(), "{\"closed\":true}");
        };
        tokio::time::timeout(std::time::Duration::from_secs(15), regression)
            .await
            .expect("native FFI response-loss regression timed out");
    }

    #[test]
    /// Ambiguous transport and RPC outcomes must not become retryable revocation errors.
    fn revocation_unknown_outcomes_are_not_retryable() {
        use syntra_core::{ErrorCode, SyntraError};
        let mut errors = vec![
            SyntraError::Unavailable("lost response".into()),
            SyntraError::DeadlineExceeded("expired".into()),
            SyntraError::Closed("closed".into()),
        ];
        for code in [
            ErrorCode::Unavailable,
            ErrorCode::DeadlineExceeded,
            ErrorCode::Internal,
            ErrorCode::Unknown,
        ] {
            errors.push(SyntraError::Rpc {
                code,
                raw_code: None,
                message: "untrusted diagnostic".into(),
            });
        }
        for error in errors {
            let error = libtinkle::TinkleError::Syntra(error);
            assert!(error.is_unknown_outcome());
            assert!(matches!(
                confirmation_error(error.clone()),
                FfiError::Network(_)
            ));
            assert!(matches!(
                revocation_error(error),
                FfiError::Network(message)
                    if message == "revocation outcome unknown; do not retry automatically"
            ));
        }
    }

    #[test]
    /// Preserve normal FFI error categories and messages for nonambiguous failures.
    fn revocation_preserves_other_error_mappings() {
        use syntra_core::{ErrorCode, SyntraError};
        let mut errors = vec![
            libtinkle::TinkleError::NotConnected("offline".into()),
            libtinkle::TinkleError::SignedOut,
            libtinkle::TinkleError::Codec("invalid input".into()),
            libtinkle::TinkleError::NetworkUnavailable,
            libtinkle::TinkleError::Syntra(SyntraError::Cancelled("cancelled".into())),
        ];
        for code in [
            ErrorCode::Unauthenticated,
            ErrorCode::PermissionDenied,
            ErrorCode::ResourceExhausted,
            ErrorCode::InvalidArgument,
            ErrorCode::NotFound,
            ErrorCode::AlreadyExists,
            ErrorCode::FailedPrecondition,
            ErrorCode::OutOfRange,
            ErrorCode::Unimplemented,
            ErrorCode::Cancelled,
            ErrorCode::Aborted,
            ErrorCode::DataLoss,
            ErrorCode::Unspecified,
        ] {
            errors.push(libtinkle::TinkleError::Syntra(SyntraError::Rpc {
                code,
                raw_code: None,
                message: "rejected".into(),
            }));
        }
        for error in errors {
            assert!(!error.is_unknown_outcome());
            let expected = FfiError::from(error.clone());
            let actual = revocation_error(error);
            assert_eq!(
                std::mem::discriminant(&actual),
                std::mem::discriminant(&expected)
            );
            assert_eq!(actual.to_string(), expected.to_string());
        }
    }

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
            unconfirmed: false,
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
        assert_eq!(mapped.unconfirmed, session.unconfirmed);
    }
}
