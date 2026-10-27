//! Subscribe to conversation journals and print reconcile events.
//!
//! ```sh
//! TINKLE_URL=wss://gateway.test.example.com/syntra/v1/ws \
//! TINKLE_TOKEN=<session-token> \
//! cargo run -p libtinkle --example subscribe
//! ```

use libtinkle::{Client, ClientConfig, Credential, InMemoryCursorStore};
use tinkle_schema::proto::{target, Target};

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
    // Example: track room 7. Principal must be set (via sign_in or
    // set_principal_user_id) before subscribing in real apps.
    client.set_principal_user_id(42).await;
    client
        .subscribe(vec![Target {
            kind: Some(target::Kind::ChatId(7)),
        }])
        .await?;
    println!("subscribed: {:?}", client.tracked_targets().await);

    // Connectivity is ephemeral; journal cursors advance atomically with
    // their page commits inside reconcile.
    for _ in 0..10 {
        match tokio::time::timeout(std::time::Duration::from_secs(30), client.next_event()).await {
            Ok(Some(ev)) => {
                println!("event: {ev:?}");
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
