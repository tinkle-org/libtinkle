//! Native recovery of immutable main-only preparations and typed profile/history boundaries.
mod support;
use libtinkle::{Client, Storage, TinkleError};
use prost::Message as _;
use std::sync::{Arc, Mutex};
use tinkle_schema::proto as p;

/// A real PNG artifact isolates durable handoff from platform encoder qualification.
struct PngPreparer(Arc<std::sync::atomic::AtomicUsize>);
#[async_trait::async_trait]
impl libtinkle::media::PlatformMediaPreparer for PngPreparer {
    async fn prepare(
        &self,
        input: Arc<libtinkle::media::NativeMediaInput>,
    ) -> Result<libtinkle::media::NativeMediaOutput, libtinkle::media::MediaError> {
        use image::ImageEncoder as _;
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let file = std::fs::File::create(input.output_path()).unwrap();
        image::codecs::png::PngEncoder::new(file)
            .write_image(&[1, 2, 3, 255], 1, 1, image::ExtendedColorType::Rgba8)
            .unwrap();
        Ok(libtinkle::media::NativeMediaOutput {
            width: 1,
            height: 1,
            frame_count: 1,
            duration_ms: 0,
            audio_channels: 0,
            audio_sample_rate: 0,
            audio_samples: 0,
            waveform: vec![],
        })
    }
}

/// A failed upload followed by restart reuses the frozen artifact even after input replacement.
#[tokio::test]
async fn preparation_manifest_survives_restart_without_reencoding_and_logout_cleans_artifact() {
    for native in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("cache.sqlite");
        let input = dir.path().join("input.bin");
        std::fs::write(&input, b"original artifact").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let remote = requests.clone();
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let remote = remote.clone();
                let handle = support::accept(stream, move |request, _| {
                    let remote = remote.clone();
                    async move {
                        assert!(request.method.ends_with("/BeginUpload"));
                        remote.lock().unwrap().push(request.payload.clone());
                        let request =
                            p::BeginUploadRequest::decode(request.payload.as_slice()).unwrap();
                        let outcome = if attempt == 0 {
                            syntra_core::proto::rpc_result::Outcome::Error(
                                syntra_core::proto::RpcError {
                                    code: 3,
                                    message: "refused".into(),
                                },
                            )
                        } else {
                            syntra_core::proto::rpc_result::Outcome::Payload(
                                p::BeginUploadResponse {
                                    status: p::UploadStatus::Completed as i32,
                                    file: Some(p::FileInfo {
                                        file_id: 7,
                                        size_bytes: request.size_bytes,
                                        sha256: request.sha256.unwrap(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }
                                .encode_to_vec(),
                            )
                        };
                        syntra_core::proto::Frame {
                            version: 1,
                            request_id: 1,
                            body: Some(syntra_core::proto::frame::Body::Result(
                                syntra_core::proto::RpcResult {
                                    outcome: Some(outcome),
                                },
                            )),
                        }
                    }
                })
                .await
                .unwrap();
                while !handle.is_closed() {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                handle.close().await;
            }
        });
        let storage = Arc::new(Storage::open(database.to_str().unwrap()).unwrap());
        let client = Client::new_sqlite(config.clone(), support::source(), storage.clone());
        client.set_principal_user_id(42).await;
        client.connect().await.unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let preparer = Arc::new(PngPreparer(calls.clone()));
        let result = if native {
            client
                .configure_media_preparer(Some(preparer.clone()))
                .unwrap();
            client
                .sending()
                .prepare_attachment(
                    "attachment".into(),
                    &input,
                    libtinkle::media::NativeMediaKind::Image,
                    None,
                )
                .await
        } else {
            client
                .sending()
                .prepare_attachment(
                    "attachment".into(),
                    &input,
                    libtinkle::media::MediaKind::Document,
                    None,
                )
                .await
        };
        assert!(
            matches!(
                &result,
                Err(libtinkle::SendError::Rpc(TinkleError::Syntra(
                    syntra_core::error::SyntraError::Rpc {
                        code: syntra_core::ErrorCode::InvalidArgument,
                        ..
                    }
                )))
            ),
            "{result:?}"
        );
        let manifest = client
            .sending()
            .attachment_manifest("attachment".into())
            .await
            .unwrap()
            .unwrap();
        if !native {
            assert_eq!(
                std::fs::read(&manifest.artifact_path).unwrap(),
                b"original artifact"
            );
        }
        client.close().await;
        drop(client);
        drop(storage);
        std::fs::write(&input, b"replacement must not be prepared").unwrap();
        let storage = Arc::new(Storage::open(database.to_str().unwrap()).unwrap());
        let client = Client::new_sqlite(config, support::source(), storage.clone());
        client.set_principal_user_id(42).await;
        client.connect().await.unwrap();
        let prepared = if native {
            client
                .sending()
                .prepare_attachment(
                    "attachment".into(),
                    &input,
                    libtinkle::media::NativeMediaKind::Image,
                    None,
                )
                .await
        } else {
            client
                .sending()
                .prepare_attachment(
                    "attachment".into(),
                    &input,
                    libtinkle::media::MediaKind::Document,
                    None,
                )
                .await
        }
        .unwrap();
        assert_eq!(prepared.output_sha256, manifest.output_sha256);
        assert!(prepared.thumbnail.is_none());
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(native)
        );
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0], requests[1]);
        }
        client.close().await;
        storage.clear_principal("user:42").unwrap();
        assert!(!std::path::Path::new(&manifest.artifact_path).exists());
        server.await.unwrap();
    }
}

/// Adoption retains facts and a verified copy before upload admission, without retaining or
/// invoking a codec. Restart resumes it through the normal API without any configured encoder.
#[tokio::test]
async fn native_adoption_persists_hash_recipe_and_facts_before_network() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let input = dir.path().join("source.png");
    std::fs::write(&input, b"source fixture").unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let prepared = libtinkle::media::prepare_native_media(
        &input,
        libtinkle::media::NativeMediaKind::Image,
        Arc::new(PngPreparer(calls.clone())),
        None,
    )
    .await
    .unwrap();
    let expected = prepared.main.output_sha256;
    let storage = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
    let config = support::config("127.0.0.1:1".parse().unwrap());
    let client = Client::new_sqlite(config.clone(), support::source(), storage.clone());
    client.set_principal_user_id(42).await;
    let result = client
        .sending()
        .adopt_native_attachment("adopt".into(), &input, prepared, None)
        .await;
    assert!(
        matches!(result, Err(libtinkle::SendError::Rpc(TinkleError::Sync(_)))),
        "{result:?}"
    );
    let manifest = client
        .sending()
        .attachment_manifest("adopt".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.output_sha256, expected);
    assert_eq!(manifest.native_facts.as_ref().unwrap().width, 1);
    assert_eq!(
        manifest.profile,
        libtinkle::media::NATIVE_PREPARATION_PROFILE
    );
    assert!(std::path::Path::new(&manifest.artifact_path).is_file());
    assert!(manifest.artifact_path.ends_with(".png"));
    client.close().await;
    drop((client, storage));
    std::fs::write(&input, b"changed source after adoption").unwrap();
    let storage = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
    let client = Client::new_sqlite(config, support::source(), storage);
    client.set_principal_user_id(42).await;
    assert!(matches!(
        client
            .sending()
            .prepare_attachment(
                "adopt".into(),
                &input,
                libtinkle::media::NativeMediaKind::Image,
                None
            )
            .await,
        Err(libtinkle::SendError::Rpc(TinkleError::Sync(_)))
    ));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        client
            .sending()
            .attachment_manifest("adopt".into())
            .await
            .unwrap()
            .unwrap()
            .artifact_path,
        manifest.artifact_path
    );
    client.close().await;
}

/// Path access cannot authorize changed bytes under a retained native hash or upload key.
#[tokio::test]
async fn modified_native_artifact_is_rejected_before_manifest_or_upload() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("input");
    std::fs::write(&source, b"input").unwrap();
    let prepared = libtinkle::media::prepare_native_media(
        &source,
        libtinkle::media::NativeMediaKind::Image,
        Arc::new(PngPreparer(Arc::new(std::sync::atomic::AtomicUsize::new(
            0,
        )))),
        None,
    )
    .await
    .unwrap();
    std::fs::write(prepared.main.path(), b"changed bytes").unwrap();
    let client = Client::new_sqlite(
        support::config("127.0.0.1:1".parse().unwrap()),
        support::source(),
        Arc::new(Storage::open(dir.path().join("cache.sqlite").to_str().unwrap()).unwrap()),
    );
    client.set_principal_user_id(42).await;
    assert!(matches!(
        client
            .sending()
            .adopt_native_attachment("tampered".into(), &source, prepared, None)
            .await,
        Err(libtinkle::SendError::Storage)
    ));
    assert!(client
        .sending()
        .attachment_manifest("tampered".into())
        .await
        .unwrap()
        .is_none());
    client.close().await;
}

/// Profile facade validation and destructive selection validation run before network admission.
#[tokio::test]
async fn typed_users_and_explicit_delete_operation_validation() {
    let client = Client::new(
        support::config("127.0.0.1:1".parse().unwrap()),
        support::source(),
        libtinkle::InMemoryCursorStore::new(),
    );
    for users in [
        vec![],
        vec![p::UserInput { user_id: 0 }],
        vec![p::UserInput { user_id: 1 }; 101],
    ] {
        assert!(matches!(
            client.users().get_users(p::GetUsersRequest { users }).await,
            Err(TinkleError::Codec(_))
        ));
    }
    assert!(matches!(
        client
            .users()
            .get_user_complete(p::GetUserCompleteRequest { user_id: 0 })
            .await,
        Err(TinkleError::Codec(_))
    ));
    assert!(matches!(
        client
            .users()
            .get_user_complete(p::GetUserCompleteRequest { user_id: 7 })
            .await,
        Err(TinkleError::NotConnected(_))
    ));
    let target = Some(p::Target {
        kind: Some(p::target::Kind::ChatId(7)),
    });
    for operation_id in ["", "00000000-0000-0000-0000-000000000000", "not-a-uuid"] {
        assert!(matches!(
            client
                .messages()
                .delete_history(p::DeleteHistoryRequest {
                    target,
                    operation_id: operation_id.into(),
                    ..Default::default()
                })
                .await,
            Err(TinkleError::Codec(_))
        ));
    }
    assert!(matches!(
        client
            .messages()
            .delete_history(p::DeleteHistoryRequest {
                target,
                operation_id: "11111111-1111-4111-8111-111111111111".into(),
                ..Default::default()
            })
            .await,
        Err(TinkleError::NotConnected(_))
    ));
}

/// Both profile methods dispatch their exact request types on the authenticated channel.
#[tokio::test]
async fn users_facade_crosses_the_encrypted_boundary() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |request, _| async move {
            let payload = match request.method.as_str() {
                "/tinkle.v1.UsersService/GetUsers" => {
                    let request = p::GetUsersRequest::decode(request.payload.as_slice()).unwrap();
                    assert_eq!(
                        request.users.iter().map(|u| u.user_id).collect::<Vec<_>>(),
                        vec![7, 8]
                    );
                    p::GetUsersResponse {
                        users: request
                            .users
                            .iter()
                            .map(|u| p::User {
                                user_type: Some(p::user::UserType::Empty(p::user::Empty {
                                    id: u.user_id,
                                })),
                            })
                            .collect(),
                    }
                    .encode_to_vec()
                }
                "/tinkle.v1.UsersService/GetUserComplete" => {
                    let request =
                        p::GetUserCompleteRequest::decode(request.payload.as_slice()).unwrap();
                    assert_eq!(request.user_id, 7);
                    p::GetUserCompleteResponse {
                        id: 7,
                        name: Some("profile".into()),
                        ..Default::default()
                    }
                    .encode_to_vec()
                }
                other => panic!("unexpected method {other}"),
            };
            syntra_core::proto::Frame {
                version: 1,
                request_id: 1,
                body: Some(syntra_core::proto::frame::Body::Result(
                    syntra_core::proto::RpcResult {
                        outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(payload)),
                    },
                )),
            }
        })
        .await
        .unwrap();
        while !handle.is_closed() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        handle.close().await;
    });
    let client = Client::new(
        config,
        support::source(),
        libtinkle::InMemoryCursorStore::new(),
    );
    client.set_principal_user_id(42).await;
    client.connect().await.unwrap();
    let response = client
        .users()
        .get_users(p::GetUsersRequest {
            users: vec![p::UserInput { user_id: 7 }, p::UserInput { user_id: 8 }],
        })
        .await
        .unwrap();
    assert_eq!(response.users.len(), 2);
    let profile = client
        .users()
        .get_user_complete(p::GetUserCompleteRequest { user_id: 7 })
        .await
        .unwrap();
    assert_eq!(profile.name.as_deref(), Some("profile"));
    client.close().await;
    server.await.unwrap();
}
