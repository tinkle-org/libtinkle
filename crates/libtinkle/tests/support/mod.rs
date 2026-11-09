//! Test-only encrypted native TCP gateway built entirely on the published SDK.
use prost::Message;
use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock, Weak,
    },
};
use syntra_core::{
    proto::{frame::Body, rpc_result::Outcome, Frame, RpcRequest},
    secure_channel::{self, Carrier, Mode, Secret32, ServerConfig, VerifiedIdentity},
};
use syntra_net::{tcp::HostHandle, AuthenticatorFactory};
use syntra_runtime::{
    AllowAnonymous, AuthError, Authenticator, ConnectionConfig, FnHandler, HandlerOutcome, Host,
};

pub fn client_info() -> libtinkle::ClientInfo {
    libtinkle::ClientInfo::new(
        "fixture-device".into(),
        "test".into(),
        "1".into(),
        "1".into(),
        "0001".into(),
        Default::default(),
    )
    .unwrap()
}

fn keys() -> &'static (Secret32, [u8; 32]) {
    static KEYS: OnceLock<(Secret32, [u8; 32])> = OnceLock::new();
    KEYS.get_or_init(|| secure_channel::generate_server_key().unwrap())
}

pub fn trust() -> libtinkle::ServerTrust {
    libtinkle::ServerTrust {
        server_public_key: keys().1,
        server_key_id: 7,
        scope: "tinkle-fixture".into(),
    }
}

pub fn source() -> libtinkle::TrustSource {
    libtinkle::TrustSource::Bootstrap(trust())
}

pub fn config(addr: SocketAddr) -> libtinkle::ClientConfig {
    let mut cfg = libtinkle::ClientConfig::new(addr.ip().to_string(), client_info());
    cfg.primary_port = addr.port();
    cfg.fallback_port = None;
    cfg
}

pub fn server_security() -> ServerConfig {
    ServerConfig {
        mode: Mode::Bootstrap,
        carrier: Carrier::Tcp,
        server_key_id: 7,
        scope: b"tinkle-fixture".to_vec(),
        private_key: Secret32::new(*keys().0.expose()),
        store: None,
    }
}

pub fn factory() -> Arc<dyn AuthenticatorFactory> {
    Arc::new(
        |_: &VerifiedIdentity| -> Result<Arc<dyn Authenticator>, AuthError> {
            Ok(Arc::new(AllowAnonymous))
        },
    )
}

/// Convert application fixture responses; SDK owns all frame IDs/correlation.
pub fn outcome(frame: Frame) -> HandlerOutcome {
    match frame.body {
        Some(Body::Result(result)) => match result.outcome.unwrap() {
            Outcome::Payload(bytes) => HandlerOutcome::Payload(bytes),
            Outcome::Error(error) => HandlerOutcome::Error {
                code: syntra_core::ErrorCode::from_i32_lossy(error.code).0,
                message: error.message,
            },
        },
        _ => panic!("fixture must return an RPC result"),
    }
}

pub async fn accept<F, Fut>(
    stream: tokio::net::TcpStream,
    f: F,
) -> Result<HostHandle, syntra_net::Error>
where
    F: Fn(RpcRequest, Arc<Host>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Frame> + Send + 'static,
{
    let (host_tx, host_rx) = tokio::sync::watch::channel::<Option<Weak<Host>>>(None);
    let f = Arc::new(f);
    let connection_initialized = Arc::new(AtomicBool::new(false));
    let handler = Arc::new(FnHandler(move |method: String, payload: Vec<u8>, _| {
        let mut rx = host_rx.clone();
        let f = f.clone();
        let connection_initialized = connection_initialized.clone();
        async move {
            loop {
                let host = rx.borrow_and_update().as_ref().and_then(Weak::upgrade);
                if let Some(host) = host {
                    let initialized = method == "/tinkle.v1.ConnectionService/InitConnect";
                    if !initialized
                        && method.starts_with("/tinkle.")
                        && !connection_initialized.load(Ordering::SeqCst)
                    {
                        return HandlerOutcome::error(
                            syntra_core::ErrorCode::FailedPrecondition,
                            "InitConnect required",
                        );
                    }
                    let (method, payload) = if initialized {
                        let init =
                            tinkle_schema::proto::InitConnectRequest::decode(payload.as_slice())
                                .unwrap();
                        let info = init.client.unwrap();
                        assert!(
                            !info.device_model.is_empty()
                                && !info.platform.is_empty()
                                && !info.os_version.is_empty()
                                && !info.app_version.is_empty()
                                && !info.app_build.is_empty()
                        );
                        connection_initialized.store(true, Ordering::SeqCst);
                        (init.method, init.payload.to_vec())
                    } else {
                        (method, payload)
                    };
                    let result = outcome(
                        f(
                            RpcRequest {
                                method,
                                payload,
                                timeout_ms: 0,
                            },
                            host,
                        )
                        .await,
                    );
                    return match result {
                        HandlerOutcome::Payload(payload) if initialized => HandlerOutcome::Payload(
                            tinkle_schema::proto::InitConnectResponse {
                                payload: payload.into(),
                            }
                            .encode_to_vec(),
                        ),
                        other => other,
                    };
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
            HandlerOutcome::error(syntra_core::ErrorCode::Unavailable, "fixture closed")
        }
    }));
    let handle = syntra_net::tcp::accept_stream(
        stream,
        &server_security(),
        factory(),
        handler,
        ConnectionConfig::default(),
        Default::default(),
    )
    .await?;
    host_tx.send_replace(Some(Arc::downgrade(&handle.host)));
    Ok(handle)
}
