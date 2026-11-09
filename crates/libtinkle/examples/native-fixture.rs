//! Host-only mobile test harness. Application traffic is encrypted Syntra TCP
//! using the published SDK. A separate loopback JSON control socket allocates
//! fixtures and reads request logs; it is never a client transport or SDK API.
use prost::Message;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use syntra_core::secure_channel::{
    generate_server_key, Carrier, Mode, Secret32, ServerConfig, VerifiedIdentity,
};
use syntra_net::AuthenticatorFactory;
use syntra_runtime::{
    AllowAnonymous, AuthError, Authenticator, ConnectionConfig, FnHandler, HandlerOutcome,
};
use tinkle_schema::proto::{
    target, GetHistoryRequest, GetHistoryResponse, GetTargetChangesRequest,
    GetTargetChangesResponse, TargetChange, TargetChangeKind,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;

struct Gateway {
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
    stop: CancellationToken,
    log: Arc<Mutex<Value>>,
}

impl Gateway {
    /// Stop admission and join every accepted socket/transport before replying.
    async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.stop.cancel();
        (&mut self.task).await?
    }
}

impl Drop for Gateway {
    /// Retains transport cleanup ownership when a control waiter disappears.
    fn drop(&mut self) {
        // The task retains its connection joins if a control waiter is dropped.
        self.stop.cancel();
    }
}

/// Maps a decoded application target to the fixture's nonsecret log key.
fn key(target: &Option<tinkle_schema::proto::Target>) -> String {
    match target.as_ref().and_then(|t| t.kind) {
        Some(target::Kind::UserId(id)) => format!("target:user:{id}"),
        Some(target::Kind::ChatId(id)) => format!("target:room:{id}"),
        None => "invalid".into(),
    }
}

#[tokio::main]
/// Starts the loopback control listener and reports its bound port to the runner.
async fn main() -> anyhow::Result<()> {
    let port = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "48080".into())
        .parse::<u16>()?;
    let control = TcpListener::bind(("127.0.0.1", port)).await?;
    println!("{}", control.local_addr()?.port());
    serve_control(control).await
}

/// Serve independent control connections; one disconnected consumer is not fatal.
async fn serve_control(control: TcpListener) -> anyhow::Result<()> {
    let mut gateways: HashMap<u64, Gateway> = HashMap::new();
    let mut next_id = 0u64;
    loop {
        let Ok((stream, _)) = control.accept().await else {
            continue;
        };
        let mut stream = BufReader::new(stream);
        let mut line = String::new();
        if !matches!(
            tokio::time::timeout(Duration::from_secs(2), stream.read_line(&mut line)).await,
            Ok(Ok(count)) if count > 0
        ) {
            continue;
        }
        let Ok(request) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let mut created = None;
        let reply = match request["op"].as_str() {
            Some("create") => {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let port = listener.local_addr()?.port();
                // Explicit opt-in matches the Rust-validated Swift enrollment vector.
                // Synthetic test key only; ordinary fixtures retain fresh random keys.
                let enrollment_review = request["enrollment_review"].as_bool() == Some(true);
                let (private, public) = if enrollment_review {
                    (
                        Secret32::new([0x33; 32]),
                        [
                            0x7b, 0x0d, 0x47, 0xd9, 0x34, 0x27, 0xf8, 0x31, 0x11, 0x60, 0x78, 0x1c,
                            0x7c, 0x73, 0x3f, 0xd8, 0x9f, 0x88, 0x97, 0x0a, 0xef, 0x49, 0x0d, 0x8a,
                            0xa0, 0xee, 0x19, 0xa4, 0xcb, 0x8a, 0x1b, 0x14,
                        ],
                    )
                } else {
                    generate_server_key()?
                };
                let scope = if enrollment_review {
                    "fixture"
                } else {
                    "tinkle-mobile-fixture"
                };
                let log = Arc::new(Mutex::new(
                    json!({"requests": [], "history": [], "admissions": []}),
                ));
                let task_log = log.clone();
                let deletions = request["deletions"].as_array().cloned().unwrap_or_default();
                let stop = CancellationToken::new();
                let task_stop = stop.clone();
                let task = tokio::spawn(async move {
                    let mut connections = tokio::task::JoinSet::new();
                    let mut result = Ok(());
                    loop {
                        let accepted = tokio::select! {
                            biased;
                            _ = task_stop.cancelled() => break,
                            joined = connections.join_next(), if !connections.is_empty() => {
                                if let Some(joined) = joined {
                                    if let Err(error) = joined.map_err(anyhow::Error::from).and_then(|r| r) {
                                        result = Err(error);
                                    }
                                }
                                continue;
                            }
                            accepted = listener.accept() => accepted,
                        };
                        let Ok((socket, _)) = accepted else { break };
                        let log = task_log.clone();
                        let deletions = deletions.clone();
                        let stop = task_stop.clone();
                        let security = ServerConfig {
                            mode: Mode::Bootstrap,
                            carrier: Carrier::Tcp,
                            server_key_id: 7,
                            scope: scope.as_bytes().to_vec(),
                            private_key: Secret32::new(*private.expose()),
                            store: None,
                        };
                        connections.spawn(async move {
                            let admission_log = log.clone();
                            let factory: Arc<dyn AuthenticatorFactory> = Arc::new(move |identity: &VerifiedIdentity| -> Result<Arc<dyn Authenticator>, AuthError> {
                                admission_log.lock().unwrap()["admissions"].as_array_mut().unwrap().push(json!(match identity.context().mode {
                                    Mode::Bootstrap => "bootstrap",
                                    Mode::Returning => "returning",
                                }));
                                Ok(Arc::new(AllowAnonymous))
                            });
                            let initialized = Arc::new(std::sync::atomic::AtomicBool::new(false));
                            let handler = FnHandler(move |method: String, payload: Vec<u8>, _| {
                                let log = log.clone();
                                let deletions = deletions.clone();
                                let initialized = initialized.clone();
                                async move {
                                    let wrapped = method == "/tinkle.v1.ConnectionService/InitConnect";
                                    let (method, payload) = if wrapped {
                                        let Ok(request) = tinkle_schema::proto::InitConnectRequest::decode(payload.as_slice()) else { return HandlerOutcome::error(syntra_core::ErrorCode::InvalidArgument, "invalid initialization") };
                                        let Some(info) = request.client else { return HandlerOutcome::error(syntra_core::ErrorCode::InvalidArgument, "client labels required") };
                                        if [&info.device_model, &info.platform, &info.os_version, &info.app_version, &info.app_build].iter().any(|s| s.trim().is_empty()) { return HandlerOutcome::error(syntra_core::ErrorCode::InvalidArgument, "client labels required"); }
                                        initialized.store(true, std::sync::atomic::Ordering::SeqCst);
                                        (request.method, request.payload.to_vec())
                                    } else {
                                        if !initialized.load(std::sync::atomic::Ordering::SeqCst) { return HandlerOutcome::error(syntra_core::ErrorCode::FailedPrecondition, "initialize first"); }
                                        (method, payload)
                                    };
                                    let result = match method.as_str() {
                                        "/tinkle.v1.MessagesService/GetTargetChanges" => {
                                            let Ok(req) = GetTargetChangesRequest::decode(&*payload) else { return HandlerOutcome::error(syntra_core::ErrorCode::InvalidArgument, "bad fixture request") };
                                            let target_key = key(&req.target);
                                            let (kind, id) = match req.target.and_then(|t| t.kind) {
                                                Some(target::Kind::UserId(id)) => ("user", id),
                                                Some(target::Kind::ChatId(id)) => ("room", id),
                                                None => return HandlerOutcome::error(syntra_core::ErrorCode::InvalidArgument, "missing target"),
                                            };
                                            log.lock().unwrap()["requests"].as_array_mut().unwrap().push(json!({"target_kind":kind,"target_id":id,"after_accept_seq":req.after_accept_seq}));
                                            let deletion = req.after_accept_seq == 0 && deletions.iter().any(|d| d.as_str() == Some(&target_key));
                                            HandlerOutcome::Payload(GetTargetChangesResponse {
                                                changes: if deletion { vec![TargetChange { message_id:101, kind: TargetChangeKind::Deleted as i32, message:None, accept_seq:1 }] } else { vec![] },
                                                more_available: Some(false), last_accept_seq: if deletion {1} else {req.after_accept_seq},
                                            }.encode_to_vec())
                                        }
                                        "/tinkle.v1.MessagesService/GetHistory" => {
                                            let Ok(req) = GetHistoryRequest::decode(&*payload) else { return HandlerOutcome::error(syntra_core::ErrorCode::InvalidArgument, "bad fixture request") };
                                            log.lock().unwrap()["history"].as_array_mut().unwrap().push(json!(key(&req.target)));
                                            HandlerOutcome::Payload(GetHistoryResponse { more_available:Some(false), ..Default::default() }.encode_to_vec())
                                        }
                                        _ => HandlerOutcome::error(syntra_core::ErrorCode::Unimplemented, "fixture method unavailable"),
                                    };
                                    match result {
                                        HandlerOutcome::Payload(payload) if wrapped => HandlerOutcome::Payload(tinkle_schema::proto::InitConnectResponse { payload: payload.into() }.encode_to_vec()),
                                        other => other,
                                    }
                                }
                            });
                            // The pinned accept API owns partial I/O inline until ready;
                            // cancelling it drops the socket before any pump is handed off.
                            let accepted = tokio::select! {
                                biased;
                                _ = stop.cancelled() => return Ok(()),
                                accepted = syntra_net::tcp::accept_stream(socket, &security, factory, Arc::new(handler), ConnectionConfig::default(), Default::default()) => accepted,
                            };
                            if let Ok(mut handle) = accepted {
                                while !handle.is_closed() {
                                    tokio::select! {
                                        _ = stop.cancelled() => break,
                                        _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                                    }
                                }
                                // An expired wait retains the same owner/budget, never
                                // permission to abort its carrier cleanup task.
                                while handle.shutdown_transport(syntra_net::Limits::default().close_timeout).await?
                                    != syntra_net::pump::TransportShutdown::Complete
                                {
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                }
                            }
                            Ok(())
                        });
                    }
                    drop(listener);
                    task_stop.cancel();
                    while let Some(joined) = connections.join_next().await {
                        if let Err(error) = joined.map_err(anyhow::Error::from).and_then(|r| r) {
                            result = Err(error);
                        }
                    }
                    result
                });
                next_id += 1;
                gateways.insert(next_id, Gateway { task, stop, log });
                created = Some(next_id);
                json!({"id": next_id, "port": port, "pin": public})
            }
            Some("requests") => gateways
                .get(&request["id"].as_u64().unwrap_or(0))
                .map(|g| g.log.lock().unwrap().clone())
                .unwrap_or_else(|| json!({"error":"unknown fixture"})),
            Some("close") => {
                if let Some(mut gateway) = gateways.remove(&request["id"].as_u64().unwrap_or(0)) {
                    if gateway.shutdown().await.is_err() {
                        continue;
                    }
                }
                json!({"closed":true})
            }
            _ => json!({"error":"unknown operation"}),
        };
        deliver_reply(stream.get_mut(), &reply, created, &mut gateways).await;
    }
}

/// A failed control delivery owns cleanup of any fixture it just allocated.
async fn deliver_reply(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    reply: &Value,
    created: Option<u64>,
    gateways: &mut HashMap<u64, Gateway>,
) {
    if !matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            stream.write_all(format!("{reply}\n").as_bytes())
        )
        .await,
        Ok(Ok(()))
    ) {
        if let Some(mut gateway) = created.and_then(|id| gateways.remove(&id)) {
            let _ = gateway.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{io::AsyncReadExt, net::TcpStream};

    /// Exchanges one control message with an independently bound fixture server.
    async fn control(address: std::net::SocketAddr, request: Value) -> Value {
        let mut stream = BufReader::new(TcpStream::connect(address).await.unwrap());
        stream
            .get_mut()
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let mut reply = String::new();
        stream.read_line(&mut reply).await.unwrap();
        serde_json::from_str(&reply).unwrap()
    }

    #[tokio::test]
    /// Confirms verified admissions distinguish Bootstrap from a valid Returning credential.
    async fn enrollment_review_admits_bootstrap_but_rejects_returning() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(serve_control(listener));
            let fixture = control(address, json!({"op":"create", "enrollment_review":true})).await;
            let trust = libtinkle::ServerTrust {
                server_public_key: serde_json::from_value(fixture["pin"].clone()).unwrap(),
                server_key_id: 7,
                scope: "fixture".into(),
            };
            let mut config = libtinkle::ClientConfig::new(
                "127.0.0.1",
                libtinkle::ClientInfo::new(
                    "fixture".into(),
                    "test".into(),
                    "1".into(),
                    "1".into(),
                    "1".into(),
                    Default::default(),
                )
                .unwrap(),
            );
            config.primary_port = fixture["port"].as_u64().unwrap().try_into().unwrap();
            config.fallback_port = None;
            let bootstrap = libtinkle::Client::new(
                config.clone(),
                libtinkle::TrustSource::Bootstrap(trust.clone()),
                libtinkle::InMemoryCursorStore::new(),
            );
            bootstrap.connect().await.unwrap();
            // Await a real RPC so the server's verified-identity callback has run.
            assert!(bootstrap
                .auth()
                .send_code(tinkle_schema::proto::SendCodeRequest {
                    phone_number: "+14155550123".into(),
                    lang_code: "en".into(),
                })
                .await
                .is_err());
            let status = tinkle_schema::proto::DeviceEnrollmentStatus {
                operation_id: "11111111-1111-4111-8111-111111111111".into(),
                session_id: "22222222-2222-4222-8222-222222222222".into(),
                handle: vec![8; 8].into(),
                enrollment_epoch: 1,
                account_auth_epoch: 1,
                user_id: 42,
                scope: tinkle_schema::proto::DeviceKeyScope::Account as i32,
                state: tinkle_schema::proto::DeviceEnrollmentState::Active as i32,
                expires_at: Some(prost_types::Timestamp {
                    seconds: 253_402_300_799,
                    nanos: 0,
                }),
            };
            let returning = libtinkle::Client::new(
                config,
                libtinkle::TrustSource::Returning {
                    trust,
                    enrollment: libtinkle::Enrollment::from_active_status([0x22; 32], &status)
                        .unwrap(),
                },
                libtinkle::InMemoryCursorStore::new(),
            );
            assert!(
                returning.connect().await.is_err(),
                "Bootstrap-only fixture must reject a retained Returning source"
            );
            assert_eq!(
                control(address, json!({"op":"requests", "id":fixture["id"]})).await["admissions"],
                json!(["bootstrap"])
            );
            returning.stop().await;
            bootstrap.stop().await;
            control(address, json!({"op":"close", "id":fixture["id"]})).await;
            server.abort();
            let _ = server.await;
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    /// Closing a gateway joins both established transports and incomplete handshakes.
    async fn close_joins_live_transport_and_stalled_socket() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(serve_control(listener));
            let fixture = control(address, json!({"op":"create"})).await;
            let port = fixture["port"].as_u64().unwrap() as u16;
            let mut stalled = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            let info = libtinkle::ClientInfo::new(
                "fixture".into(),
                "test".into(),
                "1".into(),
                "1".into(),
                "1".into(),
                Default::default(),
            )
            .unwrap();
            let mut config = libtinkle::ClientConfig::new("127.0.0.1", info);
            config.primary_port = port;
            config.fallback_port = None;
            let client = libtinkle::Client::new(
                config,
                libtinkle::TrustSource::Bootstrap(libtinkle::ServerTrust {
                    server_public_key: serde_json::from_value(fixture["pin"].clone()).unwrap(),
                    server_key_id: 7,
                    scope: "tinkle-mobile-fixture".into(),
                }),
                libtinkle::InMemoryCursorStore::new(),
            );
            client.connect().await.unwrap();
            assert_eq!(
                control(address, json!({"op":"close", "id":fixture["id"]})).await["closed"],
                true
            );
            assert!(matches!(stalled.read(&mut [0]).await, Ok(0) | Err(_)));
            let rebound = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
            while client.is_connected().await {
                tokio::task::yield_now().await;
            }
            client.close().await;
            assert_eq!(
                control(address, json!({"op":"unknown"})).await["error"],
                "unknown operation"
            );
            drop(rebound);
            server.abort();
            let _ = server.await;
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    /// A failed creation reply drains its gateway without stopping unrelated fixtures.
    async fn failed_reply_joins_only_its_new_gateway() {
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_finished = finished.clone();
        let task = tokio::spawn(async move {
            task_stop.cancelled().await;
            tokio::task::yield_now().await;
            task_finished.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        });
        let mut gateways = HashMap::from([(
            7,
            Gateway {
                task,
                stop,
                log: Default::default(),
            },
        )]);
        let (mut writer, reader) = tokio::io::duplex(1);
        drop(reader);
        deliver_reply(&mut writer, &json!({}), None, &mut gateways).await;
        assert_eq!(
            gateways.len(),
            1,
            "ordinary reply failure preserves fixtures"
        );
        deliver_reply(&mut writer, &json!({}), Some(7), &mut gateways).await;
        assert!(gateways.is_empty());
        assert!(finished.load(std::sync::atomic::Ordering::SeqCst));
    }
}
