//! FilesService integration: validation + chunked upload/download happy
//! path over an in-process Syntra mock gateway (mirrors
//! `client_integration.rs`).

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

use syntra_core::proto::{
    frame::Body, rpc_result::Outcome, Frame, Notification, RpcError as ProtoRpcError, RpcRequest,
    RpcResult,
};
use syntra_core::{decode_frame, encode_frame, ConnectionState, ValidationContext};
use tinkle_schema::proto::{
    AbortUploadRequest, AbortUploadResponse, BeginUploadRequest, BeginUploadResponse,
    CompleteUploadRequest, CompleteUploadResponse, DownloadPartRequest, DownloadPartResponse,
    GetFileInfoRequest, GetFileInfoResponse, GetUploadStatusRequest, GetUploadStatusResponse,
    PartIndexRange, UploadPartRequest, UploadPartResponse, UploadStatus,
};

const PART_SIZE: i64 = 5;
const MAX_DOWNLOAD: i32 = 5;

#[derive(Debug, Default)]
struct Session {
    size_bytes: i64,
    parts: BTreeMap<i64, Vec<u8>>,
    aborted: bool,
    completed_file: Option<i64>,
}

#[derive(Debug, Default)]
struct FilesState {
    sessions: HashMap<String, Session>,
    files: HashMap<i64, Vec<u8>>,
    next_upload: i64,
    next_file: i64,
    /// Observed `DownloadPart` requests as `(file_id, offset)`, in order.
    downloads: Vec<(i64, i64)>,
    /// When set, the gateway pauses its first `DownloadPart` response until
    /// the test releases it (deterministic mid-flight cancellation).
    download_gate: Option<Arc<DownloadGate>>,
}

/// One-shot pause for the first `DownloadPart` response.
#[derive(Debug, Default)]
struct DownloadGate {
    /// Set once the mock reaches the paused response.
    entered: AtomicBool,
    /// The test notifies this to let the paused response through.
    release: tokio::sync::Notify,
}

type Shared = Arc<Mutex<FilesState>>;

fn rpc_error(id: u64, code: i32, message: &str) -> Frame {
    Frame {
        version: 1,
        request_id: id,
        body: Some(Body::Result(RpcResult {
            outcome: Some(Outcome::Error(ProtoRpcError {
                code,
                message: message.into(),
            })),
        })),
    }
}

fn payload_frame(id: u64, bytes: Vec<u8>) -> Frame {
    Frame {
        version: 1,
        request_id: id,
        body: Some(Body::Result(RpcResult {
            outcome: Some(Outcome::Payload(bytes)),
        })),
    }
}

fn file_info_for(file_id: i64, bytes: &[u8]) -> tinkle_schema::proto::FileInfo {
    tinkle_schema::proto::FileInfo {
        file_id,
        size_bytes: bytes.len() as i64,
        // Real digest: `DownloadManager` verifies size + SHA-256, so the
        // mock must serve the truthful value (a placeholder would fail
        // verification and hide manager-level regressions).
        sha256: libtinkle::sha256(bytes).to_vec().into(),
        mime_type: None,
        file_name: None,
        encrypted: false,
        created_at: None,
    }
}

fn coalesce(parts: &BTreeMap<i64, Vec<u8>>) -> Vec<PartIndexRange> {
    let mut out = Vec::new();
    let mut start: Option<i64> = None;
    let mut prev: i64 = -2;
    for &idx in parts.keys() {
        match start {
            None => {
                start = Some(idx);
                prev = idx;
            }
            Some(_) if idx == prev + 1 => {
                prev = idx;
            }
            Some(s) => {
                out.push(PartIndexRange {
                    start_index: s,
                    end_index: prev + 1,
                });
                start = Some(idx);
                prev = idx;
            }
        }
    }
    if let Some(s) = start {
        out.push(PartIndexRange {
            start_index: s,
            end_index: prev + 1,
        });
    }
    out
}

fn part_count(size: i64) -> i64 {
    if size <= 0 {
        0
    } else {
        (size + PART_SIZE - 1) / PART_SIZE
    }
}

fn handle_files(id: u64, req: RpcRequest, state: &Shared) -> Frame {
    let mut st = state.lock().unwrap();
    match req.method.as_str() {
        "/tinkle.v1.FilesService/BeginUpload" => {
            let r = match BeginUploadRequest::decode(&*req.payload) {
                Ok(r) => r,
                Err(e) => return rpc_error(id, 3, &e.to_string()),
            };
            if r.size_bytes < 0 {
                return rpc_error(id, 3, "size_bytes must be non-negative");
            }
            st.next_upload += 1;
            let upload_id = format!("upl-{}", st.next_upload);
            if r.size_bytes == 0 {
                st.next_file += 1;
                let fid = st.next_file;
                st.files.insert(fid, Vec::new());
                st.sessions.insert(
                    upload_id.clone(),
                    Session {
                        size_bytes: 0,
                        parts: BTreeMap::new(),
                        aborted: false,
                        completed_file: Some(fid),
                    },
                );
                let resp = BeginUploadResponse {
                    upload_id,
                    part_size_bytes: None,
                    max_parallel_parts: None,
                    expires_at: None,
                    status: UploadStatus::Completed as i32,
                    file: Some(file_info_for(fid, &[])),
                };
                let mut b = Vec::new();
                resp.encode(&mut b).unwrap();
                return payload_frame(id, b);
            }
            st.sessions.insert(
                upload_id.clone(),
                Session {
                    size_bytes: r.size_bytes,
                    parts: BTreeMap::new(),
                    aborted: false,
                    completed_file: None,
                },
            );
            let resp = BeginUploadResponse {
                upload_id,
                part_size_bytes: Some(PART_SIZE),
                max_parallel_parts: Some(4),
                expires_at: None,
                status: UploadStatus::Active as i32,
                file: None,
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            payload_frame(id, b)
        }
        "/tinkle.v1.FilesService/UploadPart" => {
            let r = match UploadPartRequest::decode(&*req.payload) {
                Ok(r) => r,
                Err(e) => return rpc_error(id, 3, &e.to_string()),
            };
            let s = match st.sessions.get_mut(&r.upload_id) {
                Some(s) => s,
                None => return rpc_error(id, 5, "unknown upload"),
            };
            if s.aborted || s.completed_file.is_some() {
                return rpc_error(id, 9, "session terminal");
            }
            if r.part_index < 0 || r.part_index >= part_count(s.size_bytes) {
                return rpc_error(id, 11, "part_index out of range");
            }
            let already_present = s.parts.get(&r.part_index).is_some_and(|v| *v == r.data);
            s.parts.entry(r.part_index).or_insert(r.data.to_vec());
            let resp = UploadPartResponse {
                part_index: r.part_index,
                already_present,
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            payload_frame(id, b)
        }
        "/tinkle.v1.FilesService/GetUploadStatus" => {
            let r = match GetUploadStatusRequest::decode(&*req.payload) {
                Ok(r) => r,
                Err(e) => return rpc_error(id, 3, &e.to_string()),
            };
            let s = match st.sessions.get(&r.upload_id) {
                Some(s) => s,
                None => return rpc_error(id, 5, "unknown upload"),
            };
            let status = if s.completed_file.is_some() {
                UploadStatus::Completed as i32
            } else if s.aborted {
                UploadStatus::Aborted as i32
            } else {
                UploadStatus::Active as i32
            };
            let (part_size, file) = match s.completed_file {
                Some(fid) => {
                    let bytes = st.files.get(&fid).cloned().unwrap_or_default();
                    (None, Some(file_info_for(fid, &bytes)))
                }
                None if s.aborted => (None, None),
                None => (Some(PART_SIZE), None),
            };
            let resp = GetUploadStatusResponse {
                status,
                received_ranges: coalesce(&s.parts),
                next_page_token: None,
                expires_at: None,
                part_size_bytes: part_size,
                size_bytes: s.size_bytes,
                file,
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            payload_frame(id, b)
        }
        "/tinkle.v1.FilesService/CompleteUpload" => {
            let r = match CompleteUploadRequest::decode(&*req.payload) {
                Ok(r) => r,
                Err(e) => return rpc_error(id, 3, &e.to_string()),
            };
            let upload_id = r.upload_id.clone();
            // Snapshot session state under a short borrow, then mutate.
            let (aborted, done, size, parts) = match st.sessions.get(&upload_id) {
                Some(s) => (s.aborted, s.completed_file, s.size_bytes, s.parts.clone()),
                None => return rpc_error(id, 5, "unknown upload"),
            };
            if aborted {
                return rpc_error(id, 9, "session aborted");
            }
            if let Some(fid) = done {
                let bytes = st.files.get(&fid).cloned().unwrap_or_default();
                let resp = CompleteUploadResponse {
                    file: Some(file_info_for(fid, &bytes)),
                };
                let mut b = Vec::new();
                resp.encode(&mut b).unwrap();
                return payload_frame(id, b);
            }
            let n = part_count(size);
            for i in 0..n {
                if !parts.contains_key(&i) {
                    return rpc_error(id, 9, "missing parts");
                }
            }
            let mut bytes = Vec::with_capacity(size as usize);
            for i in 0..n {
                bytes.extend_from_slice(&parts[&i]);
            }
            bytes.truncate(size as usize);
            st.next_file += 1;
            let fid = st.next_file;
            st.files.insert(fid, bytes.clone());
            if let Some(s) = st.sessions.get_mut(&upload_id) {
                s.completed_file = Some(fid);
            }
            let resp = CompleteUploadResponse {
                file: Some(file_info_for(fid, &bytes)),
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            payload_frame(id, b)
        }
        "/tinkle.v1.FilesService/AbortUpload" => {
            let r = match AbortUploadRequest::decode(&*req.payload) {
                Ok(r) => r,
                Err(e) => return rpc_error(id, 3, &e.to_string()),
            };
            let s = match st.sessions.get_mut(&r.upload_id) {
                Some(s) => s,
                None => return rpc_error(id, 5, "unknown upload"),
            };
            if s.completed_file.is_some() {
                let resp = AbortUploadResponse { aborted: false };
                let mut b = Vec::new();
                resp.encode(&mut b).unwrap();
                return payload_frame(id, b);
            }
            s.aborted = true;
            let resp = AbortUploadResponse { aborted: true };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            payload_frame(id, b)
        }
        "/tinkle.v1.FilesService/GetFileInfo" => {
            let r = match GetFileInfoRequest::decode(&*req.payload) {
                Ok(r) => r,
                Err(e) => return rpc_error(id, 3, &e.to_string()),
            };
            let bytes = match st.files.get(&r.file_id) {
                Some(v) => v.clone(),
                None => return rpc_error(id, 5, "unknown file"),
            };
            let resp = GetFileInfoResponse {
                file: Some(file_info_for(r.file_id, &bytes)),
                max_download_part_bytes: Some(MAX_DOWNLOAD),
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            payload_frame(id, b)
        }
        "/tinkle.v1.FilesService/DownloadPart" => {
            let r = match DownloadPartRequest::decode(&*req.payload) {
                Ok(r) => r,
                Err(e) => return rpc_error(id, 3, &e.to_string()),
            };
            let bytes = match st.files.get(&r.file_id) {
                Some(v) => v.clone(),
                None => return rpc_error(id, 5, "unknown file"),
            };
            if r.offset < 0 || r.offset > bytes.len() as i64 {
                return rpc_error(id, 11, "offset out of range");
            }
            if r.limit <= 0 {
                return rpc_error(id, 3, "limit must be positive");
            }
            st.downloads.push((r.file_id, r.offset));
            let start = r.offset as usize;
            let end = (start + r.limit as usize).min(bytes.len());
            let data = bytes[start..end].to_vec();
            let resp = DownloadPartResponse {
                data: data.into(),
                offset: r.offset,
                eof: end == bytes.len(),
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            payload_frame(id, b)
        }
        _ => rpc_error(id, 12, "unknown method"),
    }
}

async fn mock_gateway(listener: TcpListener, state: Shared) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => return,
        };
        let task_state = state.clone();
        tokio::spawn(async move {
            use tokio_tungstenite::tungstenite::handshake::server::ErrorResponse;
            use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
            #[allow(clippy::result_large_err)]
            let callback = |req: &Request, mut resp: Response| -> Result<Response, ErrorResponse> {
                let offered = req
                    .headers()
                    .get("sec-websocket-protocol")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                if offered
                    .split(',')
                    .map(|s| s.trim())
                    .any(|s| s == "syntra.v1")
                {
                    resp.headers_mut()
                        .insert("sec-websocket-protocol", "syntra.v1".parse().unwrap());
                }
                Ok(resp)
            };
            let ws = match tokio_tungstenite::accept_hdr_async(stream, callback).await {
                Ok(w) => w,
                Err(_) => return,
            };
            let (mut write, mut read) = ws.split();
            let hello_msg = match read.next().await {
                Some(Ok(m)) => m,
                _ => return,
            };
            let bytes = hello_msg.into_data();
            let frame = match decode_frame(
                &bytes,
                ValidationContext::pre_handshake(ConnectionState::Connected),
            ) {
                Ok(f) => f,
                Err(_) => return,
            };
            if !matches!(frame.body, Some(Body::Hello(_))) {
                return;
            }
            let ready = Frame {
                version: 1,
                request_id: 0,
                body: Some(Body::Ready(syntra_core::proto::ServerReady {
                    connection_id: format!("mock-files-{}", rand::random::<u32>()),
                })),
            };
            let buf = encode_frame(
                &ready,
                ValidationContext::pre_handshake(ConnectionState::Authenticating),
            )
            .unwrap();
            if write
                .send(tokio_tungstenite::tungstenite::Message::Binary(buf.into()))
                .await
                .is_err()
            {
                return;
            }
            loop {
                let msg = match read.next().await {
                    Some(Ok(m)) => m,
                    _ => break,
                };
                if msg.is_close() {
                    break;
                }
                if msg.is_ping() || msg.is_pong() || msg.is_text() {
                    continue;
                }
                let data = msg.into_data();
                if data.is_empty() {
                    continue;
                }
                let frame = match decode_frame(&data, ValidationContext::active(1)) {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                let (id, req) = match frame.body {
                    Some(Body::Request(r)) => (frame.request_id, r),
                    Some(Body::Cancel(_)) => continue,
                    _ => continue,
                };
                // Keep the notification pump alive shape: after every Files
                // response, also emit an opaque hint.
                let is_download_part =
                    req.method.as_str() == "/tinkle.v1.FilesService/DownloadPart";
                let resp_frame = handle_files(id, req, &task_state);
                // Deterministic mid-flight pause: the first DownloadPart
                // response waits for the test's release. This runs outside
                // `handle_files`, so the state mutex is never held across
                // the wait.
                if is_download_part {
                    let gate = task_state.lock().unwrap().download_gate.clone();
                    if let Some(g) = gate {
                        if !g.entered.swap(true, Ordering::SeqCst) {
                            g.release.notified().await;
                        }
                    }
                }
                let out = encode_frame(&resp_frame, ValidationContext::active(1)).unwrap();
                if write
                    .send(tokio_tungstenite::tungstenite::Message::Binary(out.into()))
                    .await
                    .is_err()
                {
                    break;
                }
                let nf = Frame {
                    version: 1,
                    request_id: 0,
                    body: Some(Body::Notification(Notification {
                        payload: b"hint".to_vec(),
                    })),
                };
                let nbuf = encode_frame(&nf, ValidationContext::active(1)).unwrap();
                let _ = write
                    .send(tokio_tungstenite::tungstenite::Message::Binary(nbuf.into()))
                    .await;
            }
        });
    }
}

async fn start_mock() -> SocketAddr {
    start_mock_logged().await.0
}

async fn start_mock_logged() -> (SocketAddr, Shared) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state: Shared = Arc::new(Mutex::new(FilesState::default()));
    tokio::spawn(mock_gateway(listener, state.clone()));
    (addr, state)
}

fn test_client(addr: SocketAddr) -> libtinkle::Client {
    let cfg = libtinkle::ClientConfig::new(format!("ws://{addr}/syntra/v1/ws")).insecure_dev();
    libtinkle::Client::new(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        libtinkle::InMemoryCursorStore::new(),
    )
}

fn is_codec(e: &libtinkle::TinkleError) -> bool {
    matches!(e, libtinkle::TinkleError::Codec(_))
}

// -- Validation (no transport needed: bad inputs fail before `call_raw`) --

#[tokio::test]
async fn files_validation_rejects_bad_inputs() {
    let c = test_client("127.0.0.1:1".parse().unwrap());
    // BeginUpload
    assert!(is_codec(
        &c.files()
            .begin_upload(BeginUploadRequest {
                idempotency_key: String::new(),
                size_bytes: 10,
                file_name: None,
                mime_type: None,
                sha256: None,
                encrypted: false,
            })
            .await
            .unwrap_err()
    ));
    assert!(is_codec(
        &c.files()
            .begin_upload(BeginUploadRequest {
                idempotency_key: "k".into(),
                size_bytes: -1,
                file_name: None,
                mime_type: None,
                sha256: None,
                encrypted: false,
            })
            .await
            .unwrap_err()
    ));
    assert!(is_codec(
        &c.files()
            .begin_upload(BeginUploadRequest {
                idempotency_key: "k".into(),
                size_bytes: 10,
                file_name: None,
                mime_type: None,
                sha256: Some(vec![0u8; 31].into()),
                encrypted: false,
            })
            .await
            .unwrap_err()
    ));
    // UploadPart
    assert!(is_codec(
        &c.files()
            .upload_part(UploadPartRequest {
                upload_id: String::new(),
                part_index: 0,
                data: vec![1u8].into(),
                sha256: None,
            })
            .await
            .unwrap_err()
    ));
    assert!(is_codec(
        &c.files()
            .upload_part(UploadPartRequest {
                upload_id: "u".into(),
                part_index: -1,
                data: vec![1u8].into(),
                sha256: None,
            })
            .await
            .unwrap_err()
    ));
    assert!(is_codec(
        &c.files()
            .upload_part(UploadPartRequest {
                upload_id: "u".into(),
                part_index: 0,
                data: Vec::new().into(),
                sha256: None,
            })
            .await
            .unwrap_err()
    ));
    assert!(is_codec(
        &c.files()
            .upload_part(UploadPartRequest {
                upload_id: "u".into(),
                part_index: 0,
                data: vec![1u8].into(),
                sha256: Some(vec![0u8; 4].into()),
            })
            .await
            .unwrap_err()
    ));
    // GetUploadStatus
    assert!(is_codec(
        &c.files()
            .get_upload_status(GetUploadStatusRequest {
                upload_id: String::new(),
                page_token: None,
                page_size: None,
            })
            .await
            .unwrap_err()
    ));
    assert!(is_codec(
        &c.files()
            .get_upload_status(GetUploadStatusRequest {
                upload_id: "u".into(),
                page_token: None,
                page_size: Some(0),
            })
            .await
            .unwrap_err()
    ));
    // CompleteUpload
    assert!(is_codec(
        &c.files()
            .complete_upload(CompleteUploadRequest {
                upload_id: String::new(),
                sha256: None,
            })
            .await
            .unwrap_err()
    ));
    assert!(is_codec(
        &c.files()
            .complete_upload(CompleteUploadRequest {
                upload_id: "u".into(),
                sha256: Some(vec![0u8; 7].into()),
            })
            .await
            .unwrap_err()
    ));
    // AbortUpload
    assert!(is_codec(
        &c.files()
            .abort_upload(AbortUploadRequest {
                upload_id: String::new(),
            })
            .await
            .unwrap_err()
    ));
    // GetFileInfo
    assert!(is_codec(
        &c.files()
            .get_file_info(GetFileInfoRequest { file_id: 0 })
            .await
            .unwrap_err()
    ));
    assert!(is_codec(
        &c.files()
            .get_file_info(GetFileInfoRequest { file_id: -3 })
            .await
            .unwrap_err()
    ));
    // DownloadPart
    assert!(is_codec(
        &c.files()
            .download_part(DownloadPartRequest {
                file_id: 0,
                offset: 0,
                limit: 5,
            })
            .await
            .unwrap_err()
    ));
    assert!(is_codec(
        &c.files()
            .download_part(DownloadPartRequest {
                file_id: 1,
                offset: -1,
                limit: 5,
            })
            .await
            .unwrap_err()
    ));
    assert!(is_codec(
        &c.files()
            .download_part(DownloadPartRequest {
                file_id: 1,
                offset: 0,
                limit: 0,
            })
            .await
            .unwrap_err()
    ));
    // Client-level wrappers enforce the same contract.
    assert!(is_codec(
        &c.begin_upload(BeginUploadRequest {
            idempotency_key: "k".into(),
            size_bytes: -5,
            file_name: None,
            mime_type: None,
            sha256: None,
            encrypted: false,
        })
        .await
        .unwrap_err()
    ));
    assert!(is_codec(
        &c.download_part(DownloadPartRequest {
            file_id: 1,
            offset: 0,
            limit: -2,
        })
        .await
        .unwrap_err()
    ));
}

#[tokio::test]
async fn files_chunked_upload_download_happy_path() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();

    let data: Vec<u8> = b"hello world!".to_vec(); // 12 bytes -> 5/5/2
    let begin = c
        .begin_upload(BeginUploadRequest {
            idempotency_key: "chunked-1".into(),
            size_bytes: data.len() as i64,
            file_name: Some("hello.txt".into()),
            mime_type: Some("text/plain".into()),
            sha256: None,
            encrypted: false,
        })
        .await
        .unwrap();
    assert!(!begin.upload_id.is_empty());
    assert_eq!(begin.part_size_bytes, Some(PART_SIZE));
    assert_eq!(begin.status, UploadStatus::Active as i32);
    let upload_id = begin.upload_id.clone();

    // Chunked upload: 3 parts.
    for (i, chunk) in data.chunks(PART_SIZE as usize).enumerate() {
        let r = c
            .upload_part(UploadPartRequest {
                upload_id: upload_id.clone(),
                part_index: i as i64,
                data: chunk.to_vec().into(),
                sha256: None,
            })
            .await
            .unwrap();
        assert_eq!(r.part_index, i as i64);
        assert!(!r.already_present);
    }
    // Retry of part 0 reports already_present.
    let dup = c
        .upload_part(UploadPartRequest {
            upload_id: upload_id.clone(),
            part_index: 0,
            data: data[..5].to_vec().into(),
            sha256: None,
        })
        .await
        .unwrap();
    assert!(dup.already_present);

    let status = c
        .get_upload_status(GetUploadStatusRequest {
            upload_id: upload_id.clone(),
            page_token: None,
            page_size: None,
        })
        .await
        .unwrap();
    assert_eq!(status.status, UploadStatus::Active as i32);
    assert_eq!(status.size_bytes, data.len() as i64);
    assert_eq!(
        status.received_ranges,
        vec![PartIndexRange {
            start_index: 0,
            end_index: 3,
        }]
    );

    let completed = c
        .complete_upload(CompleteUploadRequest {
            upload_id: upload_id.clone(),
            sha256: None,
        })
        .await
        .unwrap();
    let file = completed.file.expect("complete must return FileInfo");
    assert!(file.file_id > 0);
    assert_eq!(file.size_bytes, data.len() as i64);
    assert_eq!(file.sha256.len(), 32);

    let info = c
        .get_file_info(GetFileInfoRequest {
            file_id: file.file_id,
        })
        .await
        .unwrap();
    assert_eq!(info.max_download_part_bytes, Some(MAX_DOWNLOAD));
    assert_eq!(info.file.as_ref().unwrap().file_id, file.file_id);

    // Chunked download loop reassembles the bytes.
    let mut out = Vec::new();
    let mut offset: i64 = 0;
    for _ in 0..100 {
        let part = c
            .download_part(DownloadPartRequest {
                file_id: file.file_id,
                offset,
                limit: MAX_DOWNLOAD,
            })
            .await
            .unwrap();
        assert_eq!(part.offset, offset);
        assert!(
            !part.data.is_empty() || part.eof,
            "empty chunk must imply eof"
        );
        out.extend_from_slice(&part.data);
        offset += part.data.len() as i64;
        if part.eof {
            break;
        }
    }
    assert_eq!(out, data);

    // Abort path on a fresh session.
    let begin2 = c
        .begin_upload(BeginUploadRequest {
            idempotency_key: "abort-me".into(),
            size_bytes: 10,
            file_name: None,
            mime_type: None,
            sha256: None,
            encrypted: false,
        })
        .await
        .unwrap();
    let aborted = c
        .abort_upload(AbortUploadRequest {
            upload_id: begin2.upload_id,
        })
        .await
        .unwrap();
    assert!(aborted.aborted);
    c.close().await;
}

/// Manager-level round trip: `UploadManager::upload_bytes` commits a file
/// and `DownloadManager::download_bytes` returns the identical bytes. The
/// download verifies size + SHA-256 against `GetFileInfo`, so this fails
/// unless the mock serves the truthful digest.
#[tokio::test]
async fn files_managers_upload_download_round_trip() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    // Distinct principal: `download_bytes` join-in-flight is keyed by
    // (principal, file_id), and parallel tests mint colliding file ids on
    // separate mocks — isolation keeps their shared downloads apart.
    c.set_principal_user_id(9001).await;

    let data: Vec<u8> = b"manager round trip".to_vec();
    let uploader = libtinkle::UploadManager::new(c.clone());
    let file = uploader
        .upload_bytes(
            libtinkle::UploadParams::new("mgr-roundtrip-1".into(), data.len()),
            data.clone(),
        )
        .await
        .unwrap();
    assert_eq!(file.size_bytes, data.len() as i64);

    let downloader = libtinkle::DownloadManager::new(c.clone());
    let out = downloader.download_bytes(file.file_id, None).await.unwrap();
    assert_eq!(out, data);
    c.close().await;
}

/// Cancel isolation through the shared join-in-flight: cancelling consumer
/// A mid-download surfaces `Cancelled` for A while consumer B still
/// receives the full bytes. The mock pauses its first `DownloadPart`
/// response, so the cancel lands strictly mid-flight with no sleeps.
#[tokio::test]
async fn download_cancel_isolation_one_consumer_does_not_cancel_other() {
    let (addr, log) = start_mock_logged().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    // Distinct principal: isolates this test's join-in-flight entries from
    // the other manager tests running in parallel (see above).
    c.set_principal_user_id(9002).await;

    // Keep the shared download multi-part (13 requests at MAX_DOWNLOAD = 5)
    // without thousands of sequential RPCs exhausting the CI timeout.
    let data: Vec<u8> = (0..64u32).map(|i| (i % 251) as u8).collect();
    let file = libtinkle::UploadManager::new(c.clone())
        .upload_bytes(
            libtinkle::UploadParams::new("mgr-cancel-1".into(), data.len()),
            data.clone(),
        )
        .await
        .unwrap();

    // Pause the first DownloadPart response before starting the consumers.
    let gate = Arc::new(DownloadGate::default());
    log.lock().unwrap().download_gate = Some(gate.clone());

    let token = libtinkle::CancelToken::new();
    let t = token.clone();
    let mgr_a = libtinkle::DownloadManager::new(c.clone());
    let mgr_b = libtinkle::DownloadManager::new(c.clone());
    let fid = file.file_id;
    let h_a = tokio::spawn(async move { mgr_a.download_bytes(fid, Some(t)).await });
    let h_b = tokio::spawn(async move { mgr_b.download_bytes(fid, None).await });
    // Wait until the mock reaches the paused response (bounded, so a hung
    // mock fails instead of hanging the suite).
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !gate.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mock must reach the paused DownloadPart");
    // Cancel while the shared download is paused: no response has been
    // delivered yet, so A must observe Cancelled rather than a racy select.
    token.cancel();
    let a = tokio::time::timeout(std::time::Duration::from_secs(10), h_a)
        .await
        .expect("cancelled consumer must finish while paused")
        .expect("download task panicked");
    assert!(
        matches!(
            a,
            Err(libtinkle::TinkleError::Syntra(
                syntra_core::error::SyntraError::Cancelled(_)
            ))
        ),
        "cancelled consumer must report Cancelled, got {a:?}"
    );
    // Release the paused response: the survivor completes with full bytes.
    // `notify_one` retains the permit, so the release cannot be lost even
    // if the gateway has not parked its waiter yet (`notify_waiters`
    // would drop a pre-park release and hang the test).
    gate.release.notify_one();
    let b = tokio::time::timeout(std::time::Duration::from_secs(30), h_b)
        .await
        .expect("surviving consumer must finish after release")
        .expect("download task panicked");
    assert_eq!(b.unwrap(), data, "surviving consumer must get full bytes");
    assert_eq!(
        log.lock().unwrap().downloads.len(),
        13,
        "consumers must share one multi-part download"
    );
    c.close().await;
}

/// Downloads always start at offset 0 (no resume prefix): the first
/// `DownloadPart` of every fresh `download_bytes` run requests offset 0.
#[tokio::test]
async fn download_restarts_at_offset_zero() {
    let (addr, log) = start_mock_logged().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    // Distinct principal: isolates this test's join-in-flight entries from
    // the other manager tests running in parallel (see above).
    c.set_principal_user_id(9003).await;

    let data: Vec<u8> = b"offset zero".to_vec();
    let file = libtinkle::UploadManager::new(c.clone())
        .upload_bytes(
            libtinkle::UploadParams::new("mgr-offset-1".into(), data.len()),
            data.clone(),
        )
        .await
        .unwrap();

    let mgr = libtinkle::DownloadManager::new(c.clone());
    let base = log.lock().unwrap().downloads.len();
    let out = mgr.download_bytes(file.file_id, None).await.unwrap();
    assert_eq!(out, data);
    let first_run: Vec<(i64, i64)> = log.lock().unwrap().downloads[base..].to_vec();
    assert!(!first_run.is_empty(), "download must request parts");
    assert_eq!(
        first_run[0].1, 0,
        "fresh download must start at offset 0, got {first_run:?}"
    );

    let base2 = log.lock().unwrap().downloads.len();
    let out2 = mgr.download_bytes(file.file_id, None).await.unwrap();
    assert_eq!(out2, data);
    let second_run: Vec<(i64, i64)> = log.lock().unwrap().downloads[base2..].to_vec();
    assert!(!second_run.is_empty(), "re-download must request parts");
    assert_eq!(
        second_run[0].1, 0,
        "re-download must restart at offset 0, got {second_run:?}"
    );
    c.close().await;
}
