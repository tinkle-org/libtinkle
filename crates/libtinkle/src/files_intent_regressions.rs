//! Independent SQLite connections and encrypted SDK clients race at the intent boundary.
use super::*;
use prost::Message as _;

/// Serve exactly two clients with SDK-owned encryption and deterministic Begin replies.
async fn clients(
    path: &std::path::Path,
    hold_first: bool,
) -> (
    crate::Client,
    crate::Client,
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<tokio::sync::Notify>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = crate::enrollment::review_tests::client(false)
        .config()
        .clone();
    config.primary_port = listener.local_addr().unwrap().port();
    let (key, pin) = syntra_core::secure_channel::generate_server_key().unwrap();
    let trust = crate::ServerTrust {
        server_public_key: pin,
        server_key_id: 7,
        scope: "intent-fixture".into(),
    };
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let count = calls.clone();
    let gate = release.clone();
    tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let security = syntra_core::secure_channel::ServerConfig {
                mode: syntra_core::secure_channel::Mode::Bootstrap,
                carrier: syntra_core::secure_channel::Carrier::Tcp,
                server_key_id: 7,
                scope: b"intent-fixture".to_vec(),
                private_key: syntra_core::secure_channel::Secret32::new(*key.expose()),
                store: None,
            };
            let count = count.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                let file = Arc::new(Mutex::new(None::<FileInfo>));
                let handler = Arc::new(syntra_runtime::FnHandler(
                    move |method: String, bytes: Vec<u8>, _| {
                        let count = count.clone();
                        let gate = gate.clone();
                        let file = file.clone();
                        async move {
                            let wrapped = method.ends_with("/InitConnect");
                            let (method, bytes) = if wrapped {
                                let r = tinkle_schema::proto::InitConnectRequest::decode(&*bytes)
                                    .unwrap();
                                (r.method, r.payload.to_vec())
                            } else {
                                (method, bytes)
                            };
                            let payload = if method.ends_with("/BeginUpload") {
                                let r = BeginUploadRequest::decode(&*bytes).unwrap();
                                let info = FileInfo {
                                    file_id: 1,
                                    size_bytes: r.size_bytes,
                                    sha256: r.sha256.unwrap(),
                                    ..Default::default()
                                };
                                *file.lock().unwrap() = Some(info.clone());
                                let index = count.fetch_add(1, Ordering::SeqCst);
                                if hold_first && index == 0 {
                                    gate.notified().await;
                                    BeginUploadResponse {
                                        upload_id: "upload".into(),
                                        status: UploadStatus::Active as i32,
                                        part_size_bytes: Some(65536),
                                        max_parallel_parts: Some(1),
                                        ..Default::default()
                                    }
                                    .encode_to_vec()
                                } else {
                                    BeginUploadResponse {
                                        upload_id: "upload".into(),
                                        status: UploadStatus::Completed as i32,
                                        file: Some(info),
                                        ..Default::default()
                                    }
                                    .encode_to_vec()
                                }
                            } else if method.ends_with("/GetUploadStatus") {
                                let current = file.lock().unwrap().clone().unwrap();
                                tinkle_schema::proto::GetUploadStatusResponse {
                                    status: UploadStatus::Completed as i32,
                                    size_bytes: current.size_bytes,
                                    file: Some(current),
                                    ..Default::default()
                                }
                                .encode_to_vec()
                            } else {
                                panic!("unexpected fixture RPC {method}")
                            };
                            syntra_runtime::HandlerOutcome::Payload(if wrapped {
                                tinkle_schema::proto::InitConnectResponse {
                                    payload: payload.into(),
                                }
                                .encode_to_vec()
                            } else {
                                payload
                            })
                        }
                    },
                ));
                let factory: Arc<dyn syntra_net::AuthenticatorFactory> = Arc::new(
                    |_: &syntra_core::secure_channel::VerifiedIdentity| -> Result<
                        Arc<dyn syntra_runtime::Authenticator>,
                        syntra_runtime::AuthError,
                    > { Ok(Arc::new(syntra_runtime::AllowAnonymous)) },
                );
                let handle = syntra_net::tcp::accept_stream(
                    stream,
                    &security,
                    factory,
                    handler,
                    syntra_runtime::ConnectionConfig::default(),
                    Default::default(),
                )
                .await
                .unwrap();
                while !handle.is_closed() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                handle.close().await;
            });
        }
    });
    let a = crate::Client::new_sqlite(
        config.clone(),
        crate::TrustSource::Bootstrap(trust.clone()),
        Arc::new(Storage::open(path.to_str().unwrap()).unwrap()),
    );
    let b = crate::Client::new_sqlite(
        config,
        crate::TrustSource::Bootstrap(trust),
        Arc::new(Storage::open(path.to_str().unwrap()).unwrap()),
    );
    a.connect().await.unwrap();
    b.connect().await.unwrap();
    a.set_principal_user_id(42).await;
    b.set_principal_user_id(42).await;
    (a, b, calls, release)
}

/// Force both workers past their old absent read before either may claim the real DB key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn different_concurrent_intents_never_replace_or_dispatch_the_loser() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("cache.sqlite");
    let (a, b, calls, _) = clients(&db, false).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    let mut inputs = Vec::new();
    for (index, client) in [a.clone(), b.clone()].into_iter().enumerate() {
        let data = vec![index as u8; 19];
        let path = directory.path().join(format!("input-{index}"));
        std::fs::write(&path, &data).unwrap();
        let mut params = UploadParams::new("race".into(), 0);
        params.sha256 = Some(sha256(&data).to_vec());
        params.mime_type = Some(format!("application/fixture-{index}"));
        inputs.push((params.clone(), path.clone()));
        let mut manager = UploadManager::new(client);
        manager.intent_barrier = Some(barrier.clone());
        tasks.push(tokio::spawn(async move {
            manager.upload_path(params, &path, None).await
        }));
    }
    barrier.wait().await;
    let first = tasks.remove(0).await.unwrap();
    let second = tasks.remove(0).await.unwrap();
    assert_eq!(
        usize::from(first.is_ok()) + usize::from(second.is_ok()),
        1,
        "different intents both admitted"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "loser sent a Begin RPC");
    let winner = usize::from(second.is_ok());
    let retained = a
        .sqlite_storage()
        .unwrap()
        .get_file_transfer("user:42", "upload:race")
        .unwrap()
        .unwrap();
    a.close().await;
    b.close().await;
    let (restart, peer, _, _) = clients(&db, false).await;
    let result = UploadManager::new(restart.clone())
        .upload_path(inputs[winner].0.clone(), &inputs[winner].1, None)
        .await
        .unwrap();
    assert_eq!(
        result.sha256.as_ref(),
        inputs[winner].0.sha256.as_ref().unwrap()
    );
    assert_eq!(
        restart
            .sqlite_storage()
            .unwrap()
            .get_file_transfer("user:42", "upload:race")
            .unwrap()
            .unwrap()
            .session_json,
        retained.session_json
    );
    restart.close().await;
    peer.close().await;
}

/// An identical slower worker may observe ACTIVE after another worker completed;
/// neither its registration nor its late checkpoint can regress durable terminal state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_concurrent_intents_preserve_terminal_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("cache.sqlite");
    let path = directory.path().join("input");
    std::fs::write(&path, b"same").unwrap();
    let (a, b, _, release) = clients(&db, true).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = tokio::task::JoinSet::new();
    for client in [a.clone(), b.clone()] {
        let mut manager = UploadManager::new(client);
        manager.intent_barrier = Some(barrier.clone());
        let path = path.clone();
        tasks.spawn(async move {
            let mut params = UploadParams::new("identical".into(), 0);
            params.sha256 = Some(sha256(b"same").to_vec());
            manager.upload_path(params, &path, None).await
        });
    }
    barrier.wait().await;
    tasks.join_next().await.unwrap().unwrap().unwrap();
    assert_eq!(
        a.sqlite_storage()
            .unwrap()
            .get_file_transfer("user:42", "upload:identical")
            .unwrap()
            .unwrap()
            .state,
        "complete"
    );
    release.notify_one();
    tasks.join_next().await.unwrap().unwrap().unwrap();
    assert_eq!(
        a.sqlite_storage()
            .unwrap()
            .get_file_transfer("user:42", "upload:identical")
            .unwrap()
            .unwrap()
            .state,
        "complete"
    );
    a.close().await;
    b.close().await;
}
