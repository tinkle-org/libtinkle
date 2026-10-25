//! Subscribe to sync updates and print validated events.
//!
//! ```sh
//! TINKLE_URL=wss://gateway.test.example.com/syntra/v1/ws \
//! TINKLE_TOKEN=<session-token> \
//! cargo run -p libtinkle --example subscribe
//! ```

use libtinkle::{Client, ClientConfig, Credential, InMemoryCursorStore};

/// Apply the validated update plus required refreshes.
/// Returns an error without acknowledging so the checkpoint is retained
/// and the update can be redelivered, per the `SyncEvent::UpdateReady` contract.
fn apply_update(update: &tinkle_schema::proto::Update) -> anyhow::Result<()> {
    // Example placeholder: real apps write to storage / refresh UI here.
    // Fail closed on missing payload so we never acknowledge unapplied data.
    if update.payload.is_none() {
        anyhow::bail!("update payload missing, retaining checkpoint");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url =
        std::env::var("TINKLE_URL").unwrap_or_else(|_| "ws://127.0.0.1:8080/syntra/v1/ws".into());
    let token = std::env::var("TINKLE_TOKEN").unwrap_or_else(|_| "dev-token".into());

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
    let sub = client.subscribe(vec![], vec![]).await?;
    println!(
        "subscribed: stream={} after={}",
        sub.stream, sub.after_cursor
    );

    // Connectivity is ephemeral; updates commit only after application.
    for _ in 0..10 {
        match tokio::time::timeout(std::time::Duration::from_secs(30), client.next_event()).await {
            Ok(Some(ev)) => {
                println!("event: {ev:?}");
                if let libtinkle::SyncEvent::UpdateReady {
                    stream,
                    cursor,
                    update,
                } = ev
                {
                    // Apply first; only persist the checkpoint after apply succeeds.
                    if let Err(e) = apply_update(&update) {
                        eprintln!("apply failed, checkpoint retained: {e}");
                    } else if let Err(e) = client.acknowledge(&stream, cursor).await {
                        eprintln!("acknowledge failed: {e}");
                    }
                }
            }
            Ok(None) => break,
            Err(_) => {
                println!("no events within 30s; still subscribed");
                break;
            }
        }
    }
    client.close().await;
    Ok(())
}
