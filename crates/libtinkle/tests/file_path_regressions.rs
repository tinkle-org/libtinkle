//! Disk transfer regressions over the published encrypted TCP fixture.
mod support;

use libtinkle::{CancelToken, Client, DownloadManager, UploadManager, UploadParams};
use prost::Message;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tinkle_schema::proto::*;

// Leave room for each test's deliberate concurrent work under global fail-fast admission.
static TEST_ADMISSION: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

#[derive(Default)]
struct State {
    begin: Option<BeginUploadRequest>,
    parts: BTreeMap<i64, Vec<u8>>,
    bytes: Vec<u8>,
    offsets: Vec<i64>,
    fail_offset: Option<i64>,
    snapshot_completed: bool,
    upload_gate: Option<Arc<Gate>>,
    download_gate: Option<Arc<Gate>>,
    contexts: Vec<(String, Option<FileAccessContext>)>,
    denied_context: Option<FileAccessContext>,
    deny_owner: bool,
}

/// Pause RPC completion outside every state lock for deterministic races.
struct Gate {
    entered: std::sync::atomic::AtomicBool,
    release: tokio::sync::Semaphore,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: false.into(),
            release: tokio::sync::Semaphore::new(0),
        })
    }
    async fn entered(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !self.entered.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

fn info(bytes: &[u8]) -> FileInfo {
    FileInfo {
        file_id: 1,
        size_bytes: bytes.len() as i64,
        sha256: libtinkle::sha256(bytes).to_vec().into(),
        ..Default::default()
    }
}

async fn fixture(storage: Option<Arc<libtinkle::Storage>>) -> (Client, Arc<Mutex<State>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = support::config(listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(State::default()));
    let remote = state.clone();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |req, _| {
            let state = remote.clone();
            async move {
                let gate = {
                    let state = state.lock().unwrap();
                    if req.method.ends_with("/UploadPart") {
                        state.upload_gate.clone()
                    } else if req.method.ends_with("/DownloadPart") {
                        state.download_gate.clone()
                    } else {
                        None
                    }
                };
                if let Some(gate) = gate {
                    gate.entered
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    gate.release.acquire().await.unwrap().forget();
                }
                let mut s = state.lock().unwrap();
                let context = if req.method.ends_with("/GetFileInfo") {
                    Some(
                        GetFileInfoRequest::decode(&*req.payload)
                            .unwrap()
                            .file_access_context,
                    )
                } else if req.method.ends_with("/DownloadPart") {
                    Some(
                        DownloadPartRequest::decode(&*req.payload)
                            .unwrap()
                            .file_access_context,
                    )
                } else {
                    None
                };
                if let Some(context) = context {
                    s.contexts.push((req.method.clone(), context));
                    if (s.deny_owner && context.is_none())
                        || s.denied_context.is_some_and(|c| context == Some(c))
                    {
                        return syntra_core::proto::Frame {
                            version: 1,
                            request_id: 1,
                            body: Some(syntra_core::proto::frame::Body::Result(
                                syntra_core::proto::RpcResult {
                                    outcome: Some(syntra_core::proto::rpc_result::Outcome::Error(
                                        syntra_core::proto::RpcError {
                                            code: 5,
                                            message: "reference not found".into(),
                                        },
                                    )),
                                },
                            )),
                        };
                    }
                }
                let payload = match req.method.rsplit('/').next().unwrap() {
                    "BeginUpload" => {
                        let r = BeginUploadRequest::decode(&*req.payload).unwrap();
                        if let Some(old) = &s.begin {
                            assert_eq!(old, &r);
                        }
                        s.begin = Some(r);
                        BeginUploadResponse {
                            upload_id: "upload".into(),
                            part_size_bytes: Some(65536),
                            max_parallel_parts: Some(3),
                            status: UploadStatus::Active as i32,
                            ..Default::default()
                        }
                        .encode_to_vec()
                    }
                    "GetUploadStatus" => {
                        if s.snapshot_completed {
                            GetUploadStatusResponse {
                                status: UploadStatus::Completed as i32,
                                size_bytes: s.bytes.len() as i64,
                                file: Some(info(&s.bytes)),
                                ..Default::default()
                            }
                            .encode_to_vec()
                        } else {
                            GetUploadStatusResponse {
                                status: UploadStatus::Active as i32,
                                size_bytes: s.begin.as_ref().unwrap().size_bytes,
                                part_size_bytes: Some(65536),
                                received_ranges: s
                                    .parts
                                    .keys()
                                    .map(|i| PartIndexRange {
                                        start_index: *i,
                                        end_index: i + 1,
                                    })
                                    .collect(),
                                ..Default::default()
                            }
                            .encode_to_vec()
                        }
                    }
                    "UploadPart" => {
                        let r = UploadPartRequest::decode(&*req.payload).unwrap();
                        s.parts.insert(r.part_index, r.data.to_vec());
                        UploadPartResponse {
                            part_index: r.part_index,
                            already_present: false,
                        }
                        .encode_to_vec()
                    }
                    "CompleteUpload" => {
                        s.bytes = s.parts.values().flatten().copied().collect();
                        CompleteUploadResponse {
                            file: Some(info(&s.bytes)),
                        }
                        .encode_to_vec()
                    }
                    "GetFileInfo" => GetFileInfoResponse {
                        file: Some(info(&s.bytes)),
                        max_download_part_bytes: Some(65536),
                    }
                    .encode_to_vec(),
                    "DownloadPart" => {
                        let r = DownloadPartRequest::decode(&*req.payload).unwrap();
                        s.offsets.push(r.offset);
                        if s.fail_offset == Some(r.offset) {
                            s.fail_offset = None;
                            return syntra_core::proto::Frame {
                                version: 1,
                                request_id: 1,
                                body: Some(syntra_core::proto::frame::Body::Result(
                                    syntra_core::proto::RpcResult {
                                        outcome: Some(
                                            syntra_core::proto::rpc_result::Outcome::Error(
                                                syntra_core::proto::RpcError {
                                                    code: 9,
                                                    message: "fixture interrupted".into(),
                                                },
                                            ),
                                        ),
                                    },
                                )),
                            };
                        }
                        let end = (r.offset as usize + r.limit as usize).min(s.bytes.len());
                        DownloadPartResponse {
                            offset: r.offset,
                            data: s.bytes[r.offset as usize..end].to_vec().into(),
                            eof: end == s.bytes.len(),
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
                            outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(
                                payload,
                            )),
                        },
                    )),
                }
            }
        })
        .await
        .unwrap();
        while !handle.is_closed() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        handle.close().await;
    });
    let client = match storage {
        Some(s) => Client::new_sqlite(cfg, support::source(), s),
        None => Client::new(
            cfg,
            support::source(),
            libtinkle::InMemoryCursorStore::new(),
        ),
    };
    client.connect().await.unwrap();
    client.set_principal_user_id(42).await;
    (client, state)
}

#[tokio::test]
async fn disk_round_trip_and_durable_prefix_resume() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(libtinkle::Storage::open_in_memory().unwrap());
    let (client, state) = fixture(Some(store.clone())).await;
    let bytes: Vec<u8> = (0..262145).map(|i| (i % 251) as u8).collect();
    let source = dir.path().join("source");
    std::fs::write(&source, &bytes).unwrap();
    let mut params = UploadParams::new("fixed".into(), 0);
    params.file_name = Some("a|b".into());
    params.mime_type = Some(String::new());
    UploadManager::new(client.clone())
        .upload_path(params, &source, None)
        .await
        .unwrap();
    assert_eq!(state.lock().unwrap().bytes, bytes);
    state.lock().unwrap().fail_offset = Some(65536);
    let manager = DownloadManager::new(client.clone()).with_cache_dir(dir.path().join("cache"));
    let dest = dir.path().join("output");
    assert!(manager.download_to_path(1, &dest, None).await.is_err());
    assert!(!dest.exists());
    let jobs = store
        .list_file_transfers("user:42", Some("download"))
        .unwrap();
    assert_eq!(jobs[0].resume_offset, 65536);
    manager.download_to_path(1, &dest, None).await.unwrap();
    assert_eq!(std::fs::read(dest).unwrap(), bytes);
    assert_eq!(
        state
            .lock()
            .unwrap()
            .offsets
            .iter()
            .filter(|x| **x == 0)
            .count(),
        1
    );
    client.close().await;
}

#[tokio::test]
async fn cancelled_path_never_publishes() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let (client, _) = fixture(None).await;
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("no-output");
    let cancel = CancelToken::new();
    cancel.cancel();
    assert!(DownloadManager::new(client.clone())
        .download_to_path(1, &dest, Some(cancel))
        .await
        .is_err());
    assert!(!dest.exists());
    client.close().await;
}

/// A durable source proof prevents changed bytes being sent under a retained key.
#[tokio::test]
async fn changed_source_and_changed_optional_presence_are_rejected() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let store = Arc::new(libtinkle::Storage::open_in_memory().unwrap());
    let (client, _) = fixture(Some(store)).await;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    std::fs::write(&source, b"original").unwrap();
    let manager = UploadManager::new(client.clone());
    let params = UploadParams::new("proof".into(), 0);
    manager
        .upload_path(params.clone(), &source, None)
        .await
        .unwrap();
    std::fs::write(&source, b"modified").unwrap();
    assert!(manager
        .upload_path(params.clone(), &source, None)
        .await
        .is_err());
    std::fs::write(&source, b"original").unwrap();
    let mut changed = params;
    changed.mime_type = Some(String::new());
    assert!(manager.upload_path(changed, &source, None).await.is_err());
    client.close().await;
}

/// Local ack counts never suppress parts that the complete server snapshot lacks.
#[tokio::test]
async fn server_snapshot_overrules_durable_local_acks_and_recovers_completion() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let store = Arc::new(libtinkle::Storage::open_in_memory().unwrap());
    let (client, state) = fixture(Some(store.clone())).await;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let bytes = vec![23; 65537];
    std::fs::write(&source, &bytes).unwrap();
    let manager = UploadManager::new(client.clone());
    let params = UploadParams::new("snapshot".into(), 0);
    manager
        .upload_path(params.clone(), &source, None)
        .await
        .unwrap();
    state.lock().unwrap().parts.clear();
    let mut job = store
        .get_file_transfer("user:42", "upload:snapshot")
        .unwrap()
        .unwrap();
    job.resume_offset = i64::MAX;
    let expected = store
        .file_transfer_intent("user:42", &job.job_id)
        .unwrap()
        .unwrap();
    store
        .compare_and_swap_file_transfer("user:42", &expected, &job)
        .unwrap()
        .unwrap();
    manager
        .upload_path(params.clone(), &source, None)
        .await
        .unwrap();
    assert_eq!(state.lock().unwrap().parts.len(), 2);
    {
        let mut state = state.lock().unwrap();
        state.snapshot_completed = true;
        state.parts.clear();
    }
    manager.upload_path(params, &source, None).await.unwrap();
    assert!(state.lock().unwrap().parts.is_empty());
    client.close().await;
}

/// A failed durable job write is surfaced before BeginUpload can create remote state.
#[tokio::test]
async fn job_store_failure_is_not_ignored() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let store = Arc::new(libtinkle::Storage::open(path.to_str().unwrap()).unwrap());
    let (client, state) = fixture(Some(store)).await;
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("DROP TABLE file_transfers", [])
        .unwrap();
    let source = dir.path().join("source");
    std::fs::write(&source, b"input").unwrap();
    assert!(UploadManager::new(client.clone())
        .upload_path(UploadParams::new("store".into(), 0), &source, None)
        .await
        .is_err());
    assert!(state.lock().unwrap().begin.is_none());
    client.close().await;
}

/// Same principal and file ID on independent deployments cannot share their bytes.
#[tokio::test]
async fn independent_clients_do_not_cross_join_colliding_file_ids() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let (a, sa) = fixture(None).await;
    let (b, sb) = fixture(None).await;
    sa.lock().unwrap().bytes = b"deployment A".to_vec();
    sb.lock().unwrap().bytes = b"deployment B".to_vec();
    let ma = DownloadManager::new(a.clone());
    let mb = DownloadManager::new(b.clone());
    let (ra, rb) = tokio::join!(ma.download_bytes(1, None), mb.download_bytes(1, None));
    assert_eq!(ra.unwrap(), b"deployment A");
    assert_eq!(rb.unwrap(), b"deployment B");
    a.close().await;
    b.close().await;
}

/// A live source pathname can change after admission without changing any frozen part.
#[tokio::test]
async fn mutable_source_never_changes_an_admitted_upload() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let (client, state) = fixture(Some(Arc::new(
        libtinkle::Storage::open_in_memory().unwrap(),
    )))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let original = vec![31; 262145];
    std::fs::write(&source, &original).unwrap();
    let gate = Gate::new();
    state.lock().unwrap().upload_gate = Some(gate.clone());
    let manager = UploadManager::new(client.clone());
    let path = source.clone();
    let task = tokio::spawn(async move {
        manager
            .upload_path(UploadParams::new("frozen".into(), 0), &path, None)
            .await
    });
    gate.entered().await;
    std::fs::write(&source, vec![99; 262145]).unwrap();
    gate.release.add_permits(16);
    task.await.unwrap().unwrap();
    assert_eq!(state.lock().unwrap().bytes, original);
    client.close().await;
}

/// Cancelling the only waiter releases it immediately, but shutdown retains its
/// pending worker join across cancellation until the real RPC response settles.
#[tokio::test]
async fn cancelled_shutdown_retains_download_join_owner() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let (client, state) = fixture(None).await;
    state.lock().unwrap().bytes = vec![42; 65537];
    let gate = Gate::new();
    state.lock().unwrap().download_gate = Some(gate.clone());
    let manager = DownloadManager::new(client.clone());
    let runner = manager.clone();
    let cancel = CancelToken::new();
    let token = cancel.clone();
    let download = tokio::spawn(async move { runner.download_bytes(1, Some(token)).await });
    gate.entered().await;
    cancel.cancel();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), download)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    let closer = manager.clone();
    let closing = tokio::spawn(async move { closer.shutdown().await });
    tokio::task::yield_now().await;
    assert!(!closing.is_finished());
    closing.abort();
    let _ = closing.await;
    gate.release.add_permits(16);
    tokio::time::timeout(std::time::Duration::from_secs(5), manager.shutdown())
        .await
        .unwrap();
    assert!(manager.download_bytes(1, None).await.is_err());
    client.close().await;
}

/// A file beyond the byte API limit round-trips through paths and fixed 64 KiB parts.
/// This fails if either path implementation delegates to the bounded byte API.
#[tokio::test]
async fn large_paths_do_not_use_byte_convenience_buffers() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let (client, state) = fixture(Some(Arc::new(
        libtinkle::Storage::open_in_memory().unwrap(),
    )))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("large");
    std::fs::File::create(&source)
        .unwrap()
        .set_len(12 * 1024 * 1024)
        .unwrap();
    let uploader = UploadManager::new(client.clone());
    uploader
        .upload_path(UploadParams::new("large".into(), 0), &source, None)
        .await
        .unwrap();
    assert!(state
        .lock()
        .unwrap()
        .parts
        .values()
        .all(|part| part.len() <= 65536));
    let manager = DownloadManager::new(client.clone());
    let dest = dir.path().join("published");
    assert!(manager.download_bytes(1, None).await.is_err());
    assert!(state.lock().unwrap().offsets.is_empty());
    manager.download_to_path(1, &dest, None).await.unwrap();
    assert_eq!(std::fs::metadata(dest).unwrap().len(), 12 * 1024 * 1024);
    client.close().await;
}

/// Corrupt durable state cannot truncate another file in the same private directory.
#[tokio::test]
async fn resume_job_cannot_claim_an_unowned_cache_path() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let store = Arc::new(libtinkle::Storage::open_in_memory().unwrap());
    let (client, state) = fixture(Some(store.clone())).await;
    state.lock().unwrap().bytes = vec![19; 65537];
    state.lock().unwrap().fail_offset = Some(65536);
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let manager = DownloadManager::new(client.clone()).with_cache_dir(cache.clone());
    let destination = dir.path().join("output");
    assert!(manager
        .download_to_path(1, &destination, None)
        .await
        .is_err());
    let precious = cache.join("another-principal.bin");
    std::fs::write(&precious, b"keep exact").unwrap();
    let mut job = store
        .list_file_transfers("user:42", Some("download"))
        .unwrap()
        .remove(0);
    job.temp_path = precious.to_str().unwrap().to_string();
    job.resume_offset = 0;
    job.session_json = format!(
        "{}:{}",
        job.session_json.split_once(':').unwrap().0,
        libtinkle::sha256(&[])
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    store.upsert_file_transfer("user:42", &job).unwrap();
    manager
        .download_to_path(1, &destination, None)
        .await
        .unwrap();
    assert_eq!(std::fs::read(precious).unwrap(), b"keep exact");
    client.close().await;
}

/// Reference identity is explicit, never found by searching cached messages.
fn message_context(message_id: i64) -> FileAccessContext {
    FileAccessContext {
        reference: Some(file_access_context::Reference::Message(
            MessageFileReference {
                target: Some(Target {
                    kind: Some(target::Kind::UserId(42)),
                }),
                message_id,
            },
        )),
    }
}

/// Warm cached bytes still need fresh exact-context authorization on every read.
#[tokio::test]
async fn cached_download_rechecks_exact_context_without_owner_fallback() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let (client, state) = fixture(Some(Arc::new(
        libtinkle::Storage::open_in_memory().unwrap(),
    )))
    .await;
    state.lock().unwrap().bytes = vec![9; 17];
    let dir = tempfile::tempdir().unwrap();
    let manager = DownloadManager::new(client.clone()).with_cache_dir(dir.path().to_path_buf());
    let context = message_context(1);
    manager
        .download_bytes_with_context(1, context, None)
        .await
        .unwrap();
    {
        let s = state.lock().unwrap();
        assert!(s.contexts.iter().all(|(_, c)| *c == Some(context)));
    }
    let before = state.lock().unwrap().offsets.len();
    state.lock().unwrap().denied_context = Some(context);
    assert!(manager
        .download_bytes_with_context(1, context, None)
        .await
        .is_err());
    assert_eq!(state.lock().unwrap().offsets.len(), before);
    let other = message_context(2);
    manager
        .download_to_path_with_context(1, &dir.path().join("out"), other, None)
        .await
        .unwrap();
    assert!(
        state.lock().unwrap().offsets.len() > before,
        "contexts cannot substitute cached authority"
    );
    state.lock().unwrap().deny_owner = true;
    assert!(manager.download_bytes(1, None).await.is_err());
    client.close().await;
}

/// A denied waiter cannot join a permitted context's paused in-flight work.
#[tokio::test]
async fn denied_context_cannot_join_another_waiter() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let (client, state) = fixture(None).await;
    state.lock().unwrap().bytes = vec![7; 31];
    let gate = Gate::new();
    state.lock().unwrap().download_gate = Some(gate.clone());
    let manager = DownloadManager::new(client.clone());
    let runner = manager.clone();
    let allowed = message_context(1);
    let denied = message_context(2);
    state.lock().unwrap().denied_context = Some(denied);
    let work =
        tokio::spawn(async move { runner.download_bytes_with_context(1, allowed, None).await });
    gate.entered().await;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        manager.download_bytes_with_context(1, denied, None),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    gate.release.add_permits(8);
    assert_eq!(work.await.unwrap().unwrap(), vec![7; 31]);
    assert!(state
        .lock()
        .unwrap()
        .contexts
        .iter()
        .filter(|(m, _)| m.ends_with("/DownloadPart"))
        .all(|(_, c)| *c == Some(allowed)));
    client.close().await;
}

/// Managed upload never silently drops the pre-RPC durability requirement.
#[tokio::test]
async fn managed_upload_without_intent_storage_sends_no_rpc() {
    let _test = TEST_ADMISSION.acquire().await.unwrap();
    let (client, state) = fixture(None).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input");
    std::fs::write(&path, b"input").unwrap();
    assert!(UploadManager::new(client.clone())
        .upload_path(UploadParams::new("no-store".into(), 0), &path, None)
        .await
        .is_err());
    assert!(state.lock().unwrap().begin.is_none());
    client.close().await;
}
