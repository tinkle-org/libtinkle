//! Send over encrypted native TCP with provisioned trust and enrollment.
//! See `support::client` for required inputs.

mod support;
use tinkle_schema::proto::{target, SendMessageRequest, Target};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let peer_id: i64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);
    let text = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "hello from libtinkle".into());

    let client = support::client().await?;
    client.connect().await?;
    println!("connected: id={:?}", client.connection_id().await);

    // Persist idempotency_key before sending; retry unchanged on unknown outcome.
    let req = SendMessageRequest {
        target: Some(Target {
            kind: Some(target::Kind::UserId(peer_id)),
        }),
        message: text,
        idempotency_key: rand::random::<i64>().abs().max(1),
        reply_to: None,
        silent: None,
        forwarding_restricted: None,
        clear_draft: None,
        clear_draft_dialog_id: None,
        clear_draft_revision: None,
        entities: vec![],
        schedule_date: None,
    };
    match client.messages().send_message(req).await {
        Ok(resp) => {
            let m = resp.message.unwrap_or_default();
            println!("sent: message_id={} text={:?}", m.message_id, m.message);
        }
        Err(e) if e.is_unknown_outcome() => {
            println!("unknown outcome (transport loss/timeout): retry unchanged idempotency_key with fresh correlation");
            return Err(anyhow::anyhow!("unknown outcome: {e}"));
        }
        Err(e) => return Err(anyhow::anyhow!("send failed: {e}")),
    }
    client.close().await;
    Ok(())
}
