//! Send a Tinkle message over Syntra.
//!
//! Run against the Tinkle test gateway:
//! ```sh
//! TINKLE_URL=wss://gateway.test.example.com/syntra/v1/ws \
//! TINKLE_TOKEN=<session-token> \
//! cargo run -p libtinkle --example send-message -- 7 "hello"
//! ```
//!
//! For local development against the in-process mock shape, point
//! `TINKLE_URL` at `ws://127.0.0.1:<port>/syntra/v1/ws` with
//! `allow_insecure` (loopback only, never public traffic).

use libtinkle::{Client, ClientConfig, Credential, InMemoryCursorStore};
use tinkle_schema::proto::{target, SendMessageRequest, Target};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url =
        std::env::var("TINKLE_URL").unwrap_or_else(|_| "ws://127.0.0.1:8080/syntra/v1/ws".into());
    let token = std::env::var("TINKLE_TOKEN").unwrap_or_else(|_| "dev-token".into());
    let peer_id: i64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);
    let text = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "hello from libtinkle".into());

    let mut cfg = ClientConfig::new(url);
    if std::env::var("TINKLE_INSECURE").is_ok() {
        cfg = cfg.insecure_dev();
    }
    let client = Client::new(
        cfg,
        Credential::from_session_token(&token),
        InMemoryCursorStore::new(),
    );
    client.connect().await?;
    println!("connected: id={:?}", client.connection_id().await);

    // Persist random_id before sending; retry unchanged on unknown outcome.
    let req = SendMessageRequest {
        target: Some(Target {
            kind: Some(target::Kind::UserId(peer_id)),
        }),
        message: text,
        random_id: rand::random::<i64>().abs().max(1),
        reply_to: None,
        silent: None,
        no_forward: None,
        clear_draft: None,
        entities: vec![],
        schedule_date: None,
    };
    match client.messages().send_message(req).await {
        Ok(resp) => {
            let m = resp.message.unwrap_or_default();
            println!("sent: message_id={} text={:?}", m.message_id, m.message);
        }
        Err(e) if e.is_unknown_outcome() => {
            println!("unknown outcome (transport loss/timeout): retry unchanged random_id with fresh correlation");
            return Err(anyhow::anyhow!("unknown outcome: {e}"));
        }
        Err(e) => return Err(anyhow::anyhow!("send failed: {e}")),
    }
    client.close().await;
    Ok(())
}
