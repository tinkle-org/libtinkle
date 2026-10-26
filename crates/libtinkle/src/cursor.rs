//! Durable per-conversation journal cursors through an injected store.
//!
//! Journal checkpoints belong to one authenticated principal and one
//! conversation target (`target:user:<id>` / `target:room:<id>`). They hold
//! the last applied `GetTargetChanges.last_accept_seq` (0 is the initial
//! position) and survive connections and app restarts while history remains
//! retained. Account switches MUST NOT reuse another account's cursor.
//! Persistent user application state lives here, outside the generic Syntra
//! core.
//!
//! Reconciliation model: notification frames are opaque hints that kick
//! per-target `GetTargetChanges` paging; the journal cursor advances only
//! inside an atomic `apply_journal_page_fenced` commit (see
//! `tinkle-storage`). Connectivity observations are ephemeral and never
//! advance a cursor.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::errors::TinkleError;

/// Durable per-target journal store. Implementations persist the exact
/// `accept_seq` only after the page's effects commit (SQLite does both
/// atomically via `apply_journal_page_fenced`; checkpoint-only stores persist
/// after the host applies).
///
/// `clear_principal` removes all state owned by the principal.
/// Checkpoint-only stores (e.g. [`InMemoryCursorStore`]) drop cursors;
/// stores that also hold cached bodies (e.g. SQLite via `SqliteCursorStore`)
/// additionally wipe the principal's cached messages/dialogs/users/chats so
/// logout leaves no offline bodies behind.
#[async_trait]
pub trait CursorStore: Send + Sync + 'static {
    async fn load_journal(
        &self,
        principal: &str,
        target_key: &str,
    ) -> Result<Option<i64>, TinkleError>;
    async fn save_journal(
        &self,
        principal: &str,
        target_key: &str,
        accept_seq: i64,
    ) -> Result<(), TinkleError>;
    async fn remove_target(&self, principal: &str, target_key: &str) -> Result<(), TinkleError>;
    /// Remove all principal-scoped state (journal cursors plus, where
    /// applicable, cached bodies). See trait docs for per-implementation
    /// scope.
    async fn clear_principal(&self, principal: &str) -> Result<(), TinkleError>;
}

/// In-memory store (default; tests + ephemeral sessions).
/// Production apps inject SQLite/Keychain/DataStore-backed stores.
#[derive(Debug, Default)]
pub struct InMemoryCursorStore {
    inner: Mutex<HashMap<(String, String), i64>>,
}

impl InMemoryCursorStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl CursorStore for InMemoryCursorStore {
    async fn load_journal(
        &self,
        principal: &str,
        target_key: &str,
    ) -> Result<Option<i64>, TinkleError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .get(&(principal.to_string(), target_key.to_string()))
            .copied())
    }

    async fn save_journal(
        &self,
        principal: &str,
        target_key: &str,
        accept_seq: i64,
    ) -> Result<(), TinkleError> {
        self.inner
            .lock()
            .unwrap()
            .insert((principal.to_string(), target_key.to_string()), accept_seq);
        Ok(())
    }

    async fn remove_target(&self, principal: &str, target_key: &str) -> Result<(), TinkleError> {
        self.inner
            .lock()
            .unwrap()
            .remove(&(principal.to_string(), target_key.to_string()));
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
