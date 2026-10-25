//! SQLite-backed `CursorStore` for native clients.
//!
//! Wraps `tinkle_storage::Storage` (rusqlite bundled). `Storage` is blocking
//! rusqlite behind a mutex, so every method here hops to the blocking pool
//! (mirroring `tinkle-ffi`); async pump/ack tasks never block a Tokio worker.

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
    async fn load(&self, principal: &str, stream: &str) -> Result<Option<u64>, TinkleError> {
        let inner = self.inner.clone();
        let principal = principal.to_string();
        let stream = stream.to_string();
        tokio::task::spawn_blocking(move || {
            inner
                .load_checkpoint(&principal, &stream)
                .map_err(|e| TinkleError::CursorStore(e.to_string()))
        })
        .await
        .map_err(task_failed)?
    }

    async fn save(&self, principal: &str, stream: &str, cursor: u64) -> Result<(), TinkleError> {
        let inner = self.inner.clone();
        let principal = principal.to_string();
        let stream = stream.to_string();
        tokio::task::spawn_blocking(move || {
            inner
                .save_checkpoint(&principal, &stream, cursor)
                .map_err(|e| TinkleError::CursorStore(e.to_string()))
        })
        .await
        .map_err(task_failed)?
    }

    async fn remove_stream(&self, principal: &str, stream: &str) -> Result<(), TinkleError> {
        let inner = self.inner.clone();
        let principal = principal.to_string();
        let stream = stream.to_string();
        tokio::task::spawn_blocking(move || {
            inner
                .remove_stream(&principal, &stream)
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
