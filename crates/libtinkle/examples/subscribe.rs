//! Subscribe to realtime batches and print validated events.
//!
//! ```sh
//! TINKLE_URL=wss://gateway.test.example.com/syntra/v1/ws \
//! TINKLE_TOKEN=<session-token> \
//! cargo run -p libtinkle --example subscribe
//! ```

use libtinkle::{Client, ClientConfig, Credential, InMemoryCursorStore};

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

    // Presence is ephemeral; batches commit cursors atomically.
    for _ in 0..10 {
        match tokio::time::timeout(std::time::Duration::from_secs(30), client.next_event()).await {
            Ok(Some(ev)) => println!("event: {ev:?}"),
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
