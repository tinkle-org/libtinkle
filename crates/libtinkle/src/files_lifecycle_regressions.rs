//! Transfer-specific lifecycle exclusion regressions without real disk timing assumptions.
use super::*;

/// Retain real SQLite state while using the fixture's loopback-only client configuration.
async fn stored_client() -> (crate::Client, Arc<Storage>) {
    let base = crate::enrollment::review_tests::client(false);
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    let client = crate::Client::new_sqlite(
        base.config().clone(),
        crate::TrustSource::Bootstrap(base.enrollment_trust().await),
        storage.clone(),
    );
    client.set_principal_user_id(42).await;
    (client, storage)
}

/// A stale sending clone must fail at entry, before source I/O, durable jobs or RPC admission.
#[tokio::test]
async fn stale_sending_upload_entry_never_writes_or_dispatches() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = crate::enrollment::review_tests::client(false);
    let mut config = base.config().clone();
    config.primary_port = listener.local_addr().unwrap().port();
    let (private_key, public_key) = syntra_core::secure_channel::generate_server_key().unwrap();
    let trust = crate::ServerTrust {
        server_public_key: public_key,
        server_key_id: 7,
        scope: "transfer-fence".into(),
    };
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = requests.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let security = syntra_core::secure_channel::ServerConfig {
            mode: syntra_core::secure_channel::Mode::Bootstrap,
            carrier: syntra_core::secure_channel::Carrier::Tcp,
            server_key_id: 7,
            scope: b"transfer-fence".to_vec(),
            private_key,
            store: None,
        };
        let handler = Arc::new(syntra_runtime::FnHandler(
            move |_: String, _: Vec<u8>, _| {
                observed.fetch_add(1, Ordering::SeqCst);
                async {
                    syntra_runtime::HandlerOutcome::error(
                        syntra_core::ErrorCode::Internal,
                        "unexpected transfer RPC",
                    )
                }
            },
        ));
        let factory: Arc<dyn syntra_net::AuthenticatorFactory> = Arc::new(
            |_: &syntra_core::secure_channel::VerifiedIdentity| -> Result<Arc<dyn syntra_runtime::Authenticator>, syntra_runtime::AuthError> {
                Ok(Arc::new(syntra_runtime::AllowAnonymous))
            });
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
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    let client = crate::Client::new_sqlite(
        config,
        crate::TrustSource::Bootstrap(trust),
        storage.clone(),
    );
    client.connect().await.unwrap();
    client.set_principal_user_id(42).await;
    let sending =
        client.for_application_scope(client.reconcile_epoch(), client.retirement.revision());
    client.set_principal_user_id(43).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    std::fs::write(&path, b"must not upload").unwrap();
    let result = UploadManager::new(sending)
        .upload_path(UploadParams::new("stale-entry".into(), 0), &path, None)
        .await;
    let no_requests = requests.load(Ordering::SeqCst) == 0;
    client.close().await;
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, Err(TinkleError::Sync(ref message)) if message == "application operation scope changed"),
        "must select stale scope before source/connection admission: {result:?}"
    );
    for principal in ["user:42", "user:43"] {
        assert!(storage
            .list_file_transfers(principal, None)
            .unwrap()
            .is_empty());
    }
    assert!(no_requests, "stale entry dispatched an application RPC");
}

/// Even an incorrectly refreshed scope wrapper cannot authorize an inherited stale caller's commit.
#[tokio::test]
async fn durable_commit_revalidates_inherited_fence() {
    let (client, _) = stored_client().await;
    let sending =
        client.for_application_scope(client.reconcile_epoch(), client.retirement.revision());
    client.set_principal_user_id(43).await;
    let scope = TransferScope {
        principal: "user:43".into(),
        namespace: "fixture".into(),
        epoch: client.reconcile_epoch(),
        revision: client.retirement.revision(),
        authority: client.authorization.generation().unwrap(),
        generation: crate::liveness::LiveGeneration::new(false, 0),
        client: sending,
    };
    let touched = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = touched.clone();
    let result = disk(move || {
        scope.commit(|| {
            observed.store(true, Ordering::SeqCst);
            Ok(())
        })
    })
    .await;
    assert!(result.is_err());
    assert!(!touched.load(Ordering::SeqCst));
}

/// Dropping a waiter leaves its real blocking worker alive. After switch/wipe/ABA
/// recreation, its late durable prefix must not repopulate either principal's jobs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_retained_prefix_worker_cannot_commit_after_principal_recreation() {
    let (client, storage) = stored_client().await;
    let scope = Arc::new(TransferScope {
        principal: "user:42".into(),
        namespace: "fixture".into(),
        epoch: client.reconcile_epoch(),
        revision: client.retirement.revision(),
        authority: client.authorization.generation().unwrap(),
        generation: crate::liveness::LiveGeneration::new(false, 0),
        client: client
            .for_application_scope(client.reconcile_epoch(), client.retirement.revision()),
    });
    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("prefix.part");
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, stalled) = std::sync::mpsc::channel();
    let (done, completed) = tokio::sync::oneshot::channel();
    let old_storage = storage.clone();
    let waiting = tokio::spawn(async move {
        disk(move || {
            std::fs::write(&prefix, b"durable prefix").unwrap();
            std::fs::File::open(&prefix).unwrap().sync_all().unwrap();
            entered.send(()).unwrap();
            stalled.recv().unwrap();
            let result = scope.commit(|| {
                old_storage
                    .upsert_file_transfer(
                        &scope.principal,
                        &tinkle_storage::FileTransferJob {
                            job_id: "download:late".into(),
                            kind: "download".into(),
                            file_scope: String::new(),
                            file_id: 1,
                            upload_id: String::new(),
                            state: "active".into(),
                            session_json: "fixture".into(),
                            temp_path: prefix.to_str().unwrap().into(),
                            resume_offset: 14,
                            updated_at: now_secs(),
                        },
                    )
                    .map_err(|_| transfer_error("fixture store failed"))
            });
            done.send(result).unwrap();
            Ok(())
        })
        .await
    });
    ready.await.unwrap();
    waiting.abort();
    let _ = waiting.await;
    client.set_principal_user_id(43).await;
    storage.clear_principal("user:42").unwrap();
    client.set_principal_user_id(42).await;
    release.send(()).unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), completed)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    for principal in ["user:42", "user:43"] {
        assert!(storage
            .list_file_transfers(principal, None)
            .unwrap()
            .is_empty());
    }
}

/// A blocked admitted disk writer must not hold the synchronous authority mutex.
/// Sign-out fences immediately; the worker remains excluded until its real completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_transfer_commit_does_not_delay_credential_fence() {
    let client = crate::enrollment::review_tests::client(false);
    let scope = Arc::new(TransferScope {
        principal: "user:42".into(),
        namespace: "fixture".into(),
        epoch: client.reconcile_epoch(),
        revision: client.retirement.revision(),
        authority: client.authorization.generation().unwrap(),
        generation: crate::liveness::LiveGeneration::new(false, 0),
        client: client.background_clone(),
    });
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, stalled) = std::sync::mpsc::channel();
    let writer = scope.clone();
    let writing = tokio::spawn(async move {
        disk(move || {
            writer.commit(|| {
                entered.send(()).unwrap();
                stalled.recv().unwrap();
                Ok(())
            })
        })
        .await
    });
    ready.await.unwrap();
    let (fenced, observed) = tokio::sync::oneshot::channel();
    let signing_out = tokio::spawn(async move {
        client.begin_signed_out(crate::SessionSignOutReason::SelfLogout);
        fenced.send(()).unwrap();
    });
    let immediate = tokio::time::timeout(std::time::Duration::from_millis(250), observed).await;
    // Always release the writer, including on failure, so the test cannot strand
    // a blocked worker while its runtime is being torn down.
    release.send(()).unwrap();
    signing_out.await.unwrap();
    assert!(
        writing.await.unwrap().is_err(),
        "late completion must be fenced"
    );
    assert!(
        immediate.is_ok(),
        "credential revocation queued behind disk I/O"
    );
}

/// Capturing newer local transfer metadata must not renew an inherited sending fence.
#[tokio::test]
async fn transfer_caller_preserves_inherited_application_fence() {
    let client = crate::enrollment::review_tests::client(false);
    client.set_principal_user_id(42).await;
    let original =
        client.for_application_scope(client.reconcile_epoch(), client.retirement.revision());
    client.set_principal_user_id(43).await;
    let scope = TransferScope {
        principal: "user:43".into(),
        namespace: "fixture".into(),
        epoch: client.reconcile_epoch(),
        revision: client.retirement.revision(),
        authority: client.authorization.generation().unwrap(),
        generation: crate::liveness::LiveGeneration::new(false, 0),
        client: original,
    };
    assert!(
        matches!(
            scope
                .caller()
                .get_file_info(tinkle_schema::proto::GetFileInfoRequest {
                    file_id: 1,
                    file_access_context: None,
                })
                .await,
            Err(TinkleError::Sync(_))
        ),
        "stale application scope must be rejected before connection admission"
    );
}
