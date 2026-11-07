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

struct Gateway {
    task: tokio::task::JoinHandle<()>,
    log: Arc<Mutex<Value>>,
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn key(target: &Option<tinkle_schema::proto::Target>) -> String {
    match target.as_ref().and_then(|t| t.kind) {
        Some(target::Kind::UserId(id)) => format!("target:user:{id}"),
        Some(target::Kind::ChatId(id)) => format!("target:room:{id}"),
        None => "invalid".into(),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let port = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "48080".into())
        .parse::<u16>()?;
    let control = TcpListener::bind(("127.0.0.1", port)).await?;
    println!("{}", control.local_addr()?.port());
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
        let reply = match request["op"].as_str() {
            Some("create") => {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let port = listener.local_addr()?.port();
                let (private, public) = generate_server_key()?;
                let log = Arc::new(Mutex::new(json!({"requests": [], "history": []})));
                let task_log = log.clone();
                let deletions = request["deletions"].as_array().cloned().unwrap_or_default();
                let task = tokio::spawn(async move {
                    let mut connections = tokio::task::JoinSet::new();
                    loop {
                        let Ok((socket, _)) = listener.accept().await else {
                            break;
                        };
                        let log = task_log.clone();
                        let deletions = deletions.clone();
                        let security = ServerConfig {
                            mode: Mode::Bootstrap,
                            carrier: Carrier::Tcp,
                            server_key_id: 7,
                            scope: b"tinkle-mobile-fixture".to_vec(),
                            private_key: Secret32::new(*private.expose()),
                            store: None,
                        };
                        connections.spawn(async move {
                            let factory: Arc<dyn AuthenticatorFactory> = Arc::new(|_: &VerifiedIdentity| -> Result<Arc<dyn Authenticator>, AuthError> { Ok(Arc::new(AllowAnonymous)) });
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
                            if let Ok(handle) = syntra_net::tcp::accept_stream(socket, &security, factory, Arc::new(handler), ConnectionConfig::default(), Default::default()).await {
                                while !handle.is_closed() { tokio::time::sleep(Duration::from_millis(10)).await; }
                                handle.close().await;
                            }
                        });
                        while connections.try_join_next().is_some() {}
                    }
                });
                next_id += 1;
                gateways.insert(next_id, Gateway { task, log });
                json!({"id": next_id, "port": port, "pin": public})
            }
            Some("requests") => gateways
                .get(&request["id"].as_u64().unwrap_or(0))
                .map(|g| g.log.lock().unwrap().clone())
                .unwrap_or_else(|| json!({"error":"unknown fixture"})),
            Some("close") => {
                gateways.remove(&request["id"].as_u64().unwrap_or(0));
                json!({"closed":true})
            }
            _ => json!({"error":"unknown operation"}),
        };
        stream
            .get_mut()
            .write_all(format!("{reply}\n").as_bytes())
            .await?;
    }
}
