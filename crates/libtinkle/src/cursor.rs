//! Durable cursor persistence through an injected storage interface.
//!
//! Checkpoints belong to one authenticated principal, stream, and valid
//! history scope. They survive connections and app restarts while history
//! remains retained. Account switches MUST NOT reuse another account's
//! checkpoint. Persistent user application state lives here, outside the
//! generic Syntra core.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::errors::TinkleError;

/// Durable per-stream cursor store. Implementations persist atomically
/// with batch effects where possible; at minimum they persist the exact
/// `final_cursor` only after all required effects + refreshes commit.
#[async_trait]
pub trait CursorStore: Send + Sync + 'static {
    async fn load(&self, principal: &str, stream: &str) -> Result<Option<u64>, TinkleError>;
    async fn save(&self, principal: &str, stream: &str, cursor: u64) -> Result<(), TinkleError>;
    async fn remove_stream(&self, principal: &str, stream: &str) -> Result<(), TinkleError>;
    async fn clear_principal(&self, principal: &str) -> Result<(), TinkleError>;
}

/// In-memory store (default; tests + ephemeral sessions).
/// Production apps inject SQLite/Keychain/DataStore-backed stores.
#[derive(Debug, Default)]
pub struct InMemoryCursorStore {
    inner: Mutex<HashMap<(String, String), u64>>,
}

impl InMemoryCursorStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl CursorStore for InMemoryCursorStore {
    async fn load(&self, principal: &str, stream: &str) -> Result<Option<u64>, TinkleError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .get(&(principal.to_string(), stream.to_string()))
            .copied())
    }

    async fn save(&self, principal: &str, stream: &str, cursor: u64) -> Result<(), TinkleError> {
        self.inner
            .lock()
            .unwrap()
            .insert((principal.to_string(), stream.to_string()), cursor);
        Ok(())
    }

    async fn remove_stream(&self, principal: &str, stream: &str) -> Result<(), TinkleError> {
        self.inner
            .lock()
            .unwrap()
            .remove(&(principal.to_string(), stream.to_string()));
        Ok(())
    }

    async fn clear_principal(&self, principal: &str) -> Result<(), TinkleError> {
        self.inner
            .lock()
            .unwrap()
            .retain(|(p, _), _| p != principal);
        Ok(())
    }
}
