//! Subscribe to conversation journals and print reconcile events.
//!
//! Uses encrypted native TCP; see `support::client` for provisioned inputs.

mod support;
use tinkle_schema::proto::{target, Target};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = support::client().await?;
    client.connect().await?;
    // Example: track room 7 under the restored ACTIVE account scope.
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
