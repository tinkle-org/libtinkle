//! Focused service review regressions over the published encrypted TCP runtime.
mod support;

use async_trait::async_trait;
use libtinkle::{
    Client, Enrollment, InMemoryCursorStore, SecretBlob, SecureEnrollmentStore, SecureStoreError,
    SecureWrite, SessionAuthorization, SessionCleanup, SessionNetwork, SignInDetails, TinkleError,
    TrustSource,
};
use prost::Message;
use std::sync::{Arc, Mutex};
use syntra_core::{
    proto::{frame::Body, rpc_result::Outcome, Frame, RpcResult},
    secure_channel::{self, Context, KeyStore, Mode, Secret32},
    ErrorCode,
};
use syntra_runtime::{ConnectionConfig, FnHandler, HandlerOutcome};
use tinkle_schema::methods::accountservice as account_methods;
use tinkle_schema::proto::*;
use tokio::net::TcpListener;

fn enrollment(operation_id: String) -> DeviceEnrollment {
    DeviceEnrollment {
        operation_id,
        session_id: "96007910-fd5b-4ef2-bf0c-2e4ae1d44b1b".into(),
        authorization_key: vec![7; 32].into(),
        handle: vec![9; 8].into(),
        enrollment_epoch: 1,
        scope: DeviceKeyScope::Account as i32,
        expires_at: Some(prost_types::Timestamp {
            seconds: 4_102_444_800,
            nanos: 0,
        }),
        state: DeviceEnrollmentState::Staged as i32,
        user_id: 42,
        account_auth_epoch: 1,
    }
}

async fn keep(handle: syntra_net::tcp::HostHandle) {
    while !handle.is_closed() {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    handle.close().await;
}

struct Keys;
impl KeyStore for Keys {
    fn lookup(&self, context: &Context) -> Result<Secret32, secure_channel::Error> {
        assert_eq!(context.handle, [9; 8]);
        assert_eq!(context.epoch, 1);
        Ok(Secret32::new([7; 32]))
    }
}

#[tokio::test]
async fn account_denials_clear_authorization_without_retiring_credentials() {
    for (method, code) in [
        account_methods::GET_SESSIONS,
        account_methods::REVOKE_SESSION,
        account_methods::REVOKE_SESSIONS,
    ]
    .into_iter()
    .flat_map(|method| {
        [
            ErrorCode::PermissionDenied,
            ErrorCode::Unauthenticated,
            ErrorCode::ResourceExhausted,
        ]
        .map(|code| (method, code))
    }) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            // A second Returning admission proves the denied request kept the key.
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut security = support::server_security();
                security.mode = Mode::Returning;
                security.store = Some(Arc::new(Keys));
                let handler = Arc::new(FnHandler(
                    move |wire_method: String, bytes: Vec<u8>, _| async move {
                        let request = InitConnectRequest::decode(bytes.as_slice()).unwrap();
                        assert_eq!(wire_method, "/tinkle.v1.ConnectionService/InitConnect");
                        assert_eq!(request.method, method);
                        HandlerOutcome::error(code, "denied")
                    },
                ));
                keep(
                    syntra_net::tcp::accept_stream(
                        stream,
                        &security,
                        support::factory(),
                        handler,
                        ConnectionConfig::default(),
                        Default::default(),
                    )
                    .await
                    .unwrap(),
                )
                .await;
            }
        });
        let mut active = enrollment("78e5c289-656b-4428-a091-2dddf6828465".into());
        active.state = DeviceEnrollmentState::Active as i32;
        let status = DeviceEnrollmentStatus {
            operation_id: active.operation_id,
            session_id: active.session_id,
            handle: active.handle,
            enrollment_epoch: active.enrollment_epoch,
            scope: active.scope,
            expires_at: active.expires_at,
            state: active.state,
            user_id: active.user_id,
            account_auth_epoch: active.account_auth_epoch,
        };
        let client = Client::new(
            config,
            TrustSource::Returning {
                trust: support::trust(),
                enrollment: Enrollment::from_active_status([7; 32], &status).unwrap(),
            },
            InMemoryCursorStore::new(),
        );
        let store = Arc::new(Store::default());
        client.bind_enrollment_store(store.clone()).await.unwrap();
        client.connect().await.unwrap();
        assert_eq!(
            client.session_status().await.authorization,
            SessionAuthorization::Active
        );
        let connection = client.connection_id().await;
        let account = client.account();
        let error = match method {
            account_methods::GET_SESSIONS => account.get_sessions().await.map(|_| ()).unwrap_err(),
            account_methods::REVOKE_SESSION => account
                .revoke_session("78e5c289-656b-4428-a091-2dddf6828465")
                .await
                .map(|_| ())
                .unwrap_err(),
            account_methods::REVOKE_SESSIONS => account
                .revoke_other_sessions()
                .await
                .map(|_| ())
                .unwrap_err(),
            _ => unreachable!(),
        };
        assert_eq!(error.code(), code);
        let status = client.session_status().await;
        // Pinned Syntra closes on Unauthenticated, but not on PermissionDenied.
        if code != ErrorCode::Unauthenticated {
            assert_eq!(status.network, SessionNetwork::Available, "{code:?}");
            assert_eq!(client.connection_id().await, connection);
        }
        assert_eq!(
            status.authorization,
            if code == ErrorCode::ResourceExhausted {
                SessionAuthorization::Active
            } else {
                SessionAuthorization::Unknown
            }
        );
        assert_eq!(status.cleanup, SessionCleanup::NotStarted);
        assert!(!client.is_signed_out());
        assert_eq!(store.clears.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(store.writes.lock().unwrap().is_empty());
        client.reconnect().await.unwrap();
        assert_eq!(
            client.session_status().await.authorization,
            SessionAuthorization::Active
        );
        client.close().await;
        server.await.unwrap();
    }
}

#[derive(Default)]
struct Store {
    row: Mutex<Option<(String, SecretBlob)>>,
    writes: Mutex<Vec<SecureWrite>>,
    clears: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl SecureEnrollmentStore for Store {
    async fn commit(
        &self,
        kind: SecureWrite,
        id: &str,
        blob: &SecretBlob,
    ) -> Result<(), SecureStoreError> {
        *self.row.lock().unwrap() =
            Some((id.into(), SecretBlob::new(blob.expose_secret().to_vec())));
        self.writes.lock().unwrap().push(kind);
        Ok(())
    }
    async fn load_operation(&self, id: &str) -> Result<Option<SecretBlob>, SecureStoreError> {
        Ok(self
            .row
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(stored, _)| stored == id)
            .map(|(_, blob)| SecretBlob::new(blob.expose_secret().to_vec())))
    }
    async fn load_active(&self) -> Result<Option<SecretBlob>, SecureStoreError> {
        Ok(None)
    }
    async fn clear(&self) -> Result<(), SecureStoreError> {
        self.clears
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.row.lock().unwrap().take();
        Ok(())
    }
}

fn response(message: impl Message) -> Frame {
    Frame {
        version: 1,
        request_id: 1,
        body: Some(Body::Result(RpcResult {
            outcome: Some(Outcome::Payload(message.encode_to_vec())),
        })),
    }
}

#[tokio::test]
async fn revoke_other_sessions_lost_response_is_unknown_and_never_replayed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = calls.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        keep(
            support::accept(stream, move |request, host| {
                assert_eq!(request.method, "/tinkle.v1.AccountService/RevokeSessions");
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    // The mutation ran, but its response cannot reach the caller.
                    host.suspend();
                    response(RevokeSessionsResponse { revoked: true })
                }
            })
            .await
            .unwrap(),
        )
        .await;
    });
    let client = Client::new(config, support::source(), InMemoryCursorStore::new());
    client.connect().await.unwrap();
    let error = client.account().revoke_other_sessions().await.unwrap_err();
    assert!(error.is_unknown_outcome());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(!client.is_signed_out());
    client.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn sign_in_checks_projection_before_candidate_commit_and_keeps_recovery() {
    for (user, accepted) in [
        (
            Some(User {
                user_type: Some(user::UserType::Data(user::UserData {
                    id: 91,
                    ..Default::default()
                })),
            }),
            false,
        ),
        (
            Some(User {
                user_type: Some(user::UserType::Empty(user::Empty { id: 91 })),
            }),
            false,
        ),
        (Some(User::default()), false),
        (
            Some(User {
                user_type: Some(user::UserType::Data(user::UserData {
                    id: 42,
                    ..Default::default()
                })),
            }),
            true,
        ),
        (
            Some(User {
                user_type: Some(user::UserType::Empty(user::Empty { id: 42 })),
            }),
            true,
        ),
        (None, true),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let expected_user = user.clone();
        let server = tokio::spawn(async move {
            let retained = Arc::new(Mutex::new(None::<SignInRequest>));
            for _ in 0..if accepted { 1 } else { 2 } {
                let (stream, _) = listener.accept().await.unwrap();
                let retained = retained.clone();
                let user = user.clone();
                keep(
                    support::accept(stream, move |request, _| {
                        let retained = retained.clone();
                        let user = user.clone();
                        async move {
                            match request.method.as_str() {
                                "/tinkle.v1.AuthService/SignIn" => {
                                    let sign_in =
                                        SignInRequest::decode(request.payload.as_slice()).unwrap();
                                    let delivery = enrollment(sign_in.operation_id.clone());
                                    *retained.lock().unwrap() = Some(sign_in);
                                    response(SignInResponse {
                                        user,
                                        is_new_user: false,
                                        enrollment: Some(delivery),
                                    })
                                }
                                "/tinkle.v1.AuthService/ResumeEnrollment" => {
                                    let resume =
                                        ResumeEnrollmentRequest::decode(request.payload.as_slice())
                                            .unwrap();
                                    let retained = retained.lock().unwrap();
                                    let original = retained.as_ref().unwrap();
                                    assert_eq!(resume.operation_id, original.operation_id);
                                    assert_eq!(resume.resume_secret, original.resume_secret);
                                    response(ResumeEnrollmentResponse {
                                        enrollment: Some(enrollment(resume.operation_id)),
                                    })
                                }
                                _ => panic!("unexpected application method"),
                            }
                        }
                    })
                    .await
                    .unwrap(),
                )
                .await;
            }
        });
        let store = Arc::new(Store::default());
        let client = Client::new(config, support::source(), InMemoryCursorStore::new());
        let operation = client.prepare_enrollment(store.clone()).await.unwrap();
        let before = store
            .load_operation(operation.operation_id())
            .await
            .unwrap()
            .unwrap();
        let result = client
            .auth()
            .sign_in(
                SignInDetails::new("+15550000000".into(), "12345".into(), "verification".into())
                    .unwrap(),
                &operation,
            )
            .await;
        if accepted {
            let result = result.unwrap();
            assert_eq!(result.user, expected_user);
            assert_eq!(result.enrollment.status().user_id, 42);
        } else {
            assert!(
                matches!(result, Err(TinkleError::Codec(message)) if message == "enrollment user mismatch")
            );
            assert_eq!(*store.writes.lock().unwrap(), [SecureWrite::Operation]);
            let after = store
                .load_operation(operation.operation_id())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(before.expose_secret(), after.expose_secret());
            let recovered = client
                .load_enrollment_operation(store.clone(), operation.operation_id())
                .await
                .unwrap();
            let candidate = client.auth().resume_enrollment(&recovered).await.unwrap();
            assert_eq!(candidate.status().user_id, 42);
        }
        assert_eq!(
            *store.writes.lock().unwrap(),
            [SecureWrite::Operation, SecureWrite::Candidate]
        );
        assert_eq!(client.principal().await, "unknown");
        client.close().await;
        server.await.unwrap();
    }
}
