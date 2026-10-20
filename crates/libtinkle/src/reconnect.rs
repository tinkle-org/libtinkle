//! Reconnect + backoff + subscription re-establishment.
//!
//! A new transport connection REQUIRES a new Syntra handshake with fresh
//! credential presentation; `request_id` space, subscriptions, cursors
//! (live), and in-flight RPCs do not survive. Clients re-authenticate
//! per connection and re-subscribe explicitly. In-flight RPCs at loss get
//! `UNAVAILABLE` with unknown outcome; retry only idempotent/with-key
//! with fresh transport correlation and bounded retries.

use std::time::Duration;

use crate::config::ReconnectPolicy;

pub(crate) async fn backoff_sleep(policy: &ReconnectPolicy, attempt: u32) {
    let d: Duration = policy.delay_for(attempt);
    tokio::time::sleep(d).await;
}

pub(crate) fn should_retry(policy: &ReconnectPolicy, attempt: u32) -> bool {
    match policy.max_attempts {
        Some(max) => attempt < max,
        None => true,
    }
}
