//! Every file purpose uses the same bounded uploader and authoritative negotiated geometry.
#![cfg(tinkle_async_actions)]
mod support;
use prost::Message;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tinkle_schema::proto as p;

#[derive(Default)]
struct Session {
    size: i64,
    parts: BTreeMap<i64, Vec<u8>>,
}
fn reply(bytes: Vec<u8>) -> syntra_core::proto::Frame {
    syntra_core::proto::Frame {
        version: 1,
        request_id: 1,
        body: Some(syntra_core::proto::frame::Body::Result(
            syntra_core::proto::RpcResult {
                outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(bytes)),
            },
        )),
    }
}

/// One/short/multiple parts for images, videos, audio, voice, raw documents and regular thumbnails.
#[tokio::test]
async fn all_file_purposes_share_legal_parts_exact_bytes_and_server_parallelism() {
    for part in [65536, 131072, 262144, 524288] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let sessions = Arc::new(Mutex::new(BTreeMap::<String, Session>::new()));
        let remote = sessions.clone();
        let active = Arc::new(AtomicUsize::new(0));
        let max = Arc::new(AtomicUsize::new(0));
        let server_max = max.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let handle = support::accept(stream, move |r, _| {
                let sessions = remote.clone();
                let active = active.clone();
                let max = server_max.clone();
                async move {
                    if r.method.ends_with("/BeginUpload") {
                        let q = p::BeginUploadRequest::decode(r.payload.as_slice()).unwrap();
                        let intent = if q.idempotency_key.starts_with("image")
                            || q.idempotency_key.starts_with("thumbnail")
                        {
                            Some(1)
                        } else if q.idempotency_key.starts_with("video") {
                            Some(2)
                        } else {
                            None
                        };
                        assert_eq!(q.processing_intent, intent);
                        sessions
                            .lock()
                            .unwrap()
                            .entry(q.idempotency_key.clone())
                            .or_insert(Session {
                                size: q.size_bytes,
                                ..Default::default()
                            });
                        return reply(
                            p::BeginUploadResponse {
                                upload_id: q.idempotency_key,
                                part_size_bytes: Some(part),
                                max_parallel_parts: Some(2),
                                status: 1,
                                expires_at: Some(prost_types::Timestamp {
                                    seconds: 2_000_000_000,
                                    nanos: 0,
                                }),
                                file: None,
                            }
                            .encode_to_vec(),
                        );
                    }
                    if r.method.ends_with("/GetUploadStatus") {
                        let q = p::GetUploadStatusRequest::decode(r.payload.as_slice()).unwrap();
                        let s = sessions.lock().unwrap();
                        let session = s.get(&q.upload_id).unwrap();
                        return reply(
                            p::GetUploadStatusResponse {
                                status: 1,
                                size_bytes: session.size,
                                part_size_bytes: Some(part),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                        );
                    }
                    if r.method.ends_with("/UploadPart") {
                        let q = p::UploadPartRequest::decode(r.payload.as_slice()).unwrap();
                        let n = active.fetch_add(1, Ordering::SeqCst) + 1;
                        assert!(n <= 2);
                        max.fetch_max(n, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        {
                            let mut s = sessions.lock().unwrap();
                            let session = s.get_mut(&q.upload_id).unwrap();
                            let expected = (session.size - q.part_index * part).min(part);
                            assert_eq!(q.data.len() as i64, expected);
                            assert_eq!(q.sha256.unwrap().as_ref(), libtinkle::sha256(&q.data));
                            session.parts.insert(q.part_index, q.data.to_vec());
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                        return reply(
                            p::UploadPartResponse {
                                part_index: q.part_index,
                                already_present: false,
                            }
                            .encode_to_vec(),
                        );
                    }
                    assert!(r.method.ends_with("/CompleteUpload"));
                    let q = p::CompleteUploadRequest::decode(r.payload.as_slice()).unwrap();
                    let s = sessions.lock().unwrap();
                    let session = s.get(&q.upload_id).unwrap();
                    let bytes = session
                        .parts
                        .values()
                        .flatten()
                        .copied()
                        .collect::<Vec<_>>();
                    assert_eq!(bytes.len() as i64, session.size);
                    reply(
                        p::CompleteUploadResponse {
                            file: Some(p::FileInfo {
                                file_id: 7,
                                size_bytes: session.size,
                                sha256: libtinkle::sha256(&bytes).to_vec().into(),
                                ..Default::default()
                            }),
                        }
                        .encode_to_vec(),
                    )
                }
            })
            .await
            .unwrap();
            while !handle.is_closed() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            handle.close().await;
        });
        let root = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            libtinkle::Storage::open(root.path().join("cache.sqlite").to_str().unwrap()).unwrap(),
        );
        let client = libtinkle::Client::new_sqlite(config, support::source(), storage);
        client.set_principal_user_id(42).await;
        client.connect().await.unwrap();
        for purpose in ["image", "video", "audio", "voice", "document", "thumbnail"] {
            for size in [17, part, part * 2 + 17] {
                let bytes = (0..size).map(|n| (n % 251) as u8).collect::<Vec<_>>();
                let path = root.path().join("source");
                std::fs::write(&path, &bytes).unwrap();
                let key = format!("{purpose}-{size}");
                let intent = match purpose {
                    "image" | "thumbnail" => Some(p::MediaProcessingIntent::Image),
                    "video" => Some(p::MediaProcessingIntent::Video),
                    _ => None,
                };
                let result = client
                    .upload_manager()
                    .upload_path(
                        libtinkle::UploadParams {
                            idempotency_key: key.clone(),
                            file_name: None,
                            mime_type: Some("application/octet-stream".into()),
                            sha256: Some(libtinkle::sha256(&bytes).to_vec()),
                            processing_intent: intent,
                        },
                        &path,
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(result.size_bytes, size);
                let s = sessions.lock().unwrap();
                let session = s.get(&key).unwrap();
                assert_eq!(
                    session
                        .parts
                        .values()
                        .flatten()
                        .copied()
                        .collect::<Vec<_>>(),
                    bytes
                );
                assert_eq!(session.parts.len() as i64, (size + part - 1) / part);
            }
        }
        assert_eq!(max.load(Ordering::SeqCst), 2);
        client.close().await;
        server.await.unwrap();
    }
}
