//! SQLite-backed journal `CursorStore` for native clients.
//!
//! Wraps `tinkle_storage::Storage` (rusqlite bundled). `Storage` is blocking
//! rusqlite behind a mutex, so every method here hops to the blocking pool
//! (mirroring `tinkle-ffi`); async reconcile loops never block a Tokio worker.
//!
//! Journal cursors (`target:user:<id>` / `target:room:<id>` → `accept_seq`)
//! are the reconciliation checkpoints. Dropping a target's cursor via
//! `remove_target` makes the next reconcile start at 0; when the retained
//! journal skips past the dropped position, callers heal via a `GetHistory`
//! fallback (see `Client::reconcile_target`).

use std::sync::Arc;

use async_trait::async_trait;

use crate::errors::TinkleError;

fn task_failed(e: tokio::task::JoinError) -> TinkleError {
    TinkleError::CursorStore(format!("storage task failed: {e}"))
}

#[derive(Clone)]
pub struct SqliteCursorStore {
    inner: Arc<tinkle_storage::Storage>,
}

impl std::fmt::Debug for SqliteCursorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqliteCursorStore")
    }
}

impl SqliteCursorStore {
    pub fn new(storage: Arc<tinkle_storage::Storage>) -> Self {
        Self { inner: storage }
    }

    pub fn storage(&self) -> &Arc<tinkle_storage::Storage> {
        &self.inner
    }
}

#[async_trait]
impl crate::cursor::CursorStore for SqliteCursorStore {
    async fn load_journal(
        &self,
        principal: &str,
        target_key: &str,
    ) -> Result<Option<i64>, TinkleError> {
        let inner = self.inner.clone();
        let principal = principal.to_string();
        let target_key = target_key.to_string();
        tokio::task::spawn_blocking(move || {
            inner
                .load_journal(&principal, &target_key)
                .map_err(|e| TinkleError::CursorStore(e.to_string()))
        })
        .await
        .map_err(task_failed)?
    }

    async fn save_journal(
        &self,
        principal: &str,
        target_key: &str,
        accept_seq: i64,
    ) -> Result<(), TinkleError> {
        let inner = self.inner.clone();
        let principal = principal.to_string();
        let target_key = target_key.to_string();
        tokio::task::spawn_blocking(move || {
            inner
                .save_journal(&principal, &target_key, accept_seq)
                .map_err(|e| TinkleError::CursorStore(e.to_string()))
        })
        .await
        .map_err(task_failed)?
    }

    async fn remove_target(&self, principal: &str, target_key: &str) -> Result<(), TinkleError> {
        let inner = self.inner.clone();
        let principal = principal.to_string();
        let target_key = target_key.to_string();
        tokio::task::spawn_blocking(move || {
            inner
                .remove_target(&principal, &target_key)
                .map_err(|e| TinkleError::CursorStore(e.to_string()))
        })
        .await
        .map_err(task_failed)?
    }

    async fn clear_principal(&self, principal: &str) -> Result<(), TinkleError> {
        let inner = self.inner.clone();
        let principal = principal.to_string();
        tokio::task::spawn_blocking(move || {
            inner
                .clear_principal(&principal)
                .map_err(|e| TinkleError::CursorStore(e.to_string()))
        })
        .await
        .map_err(task_failed)?
    }
}
