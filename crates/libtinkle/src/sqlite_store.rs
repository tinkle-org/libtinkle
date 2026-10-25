//! SQLite-backed `CursorStore` for native clients.
//!
//! Wraps `tinkle_storage::Storage` (rusqlite bundled). Calls are short,
//! bounded SQLite operations; async callers run them inline. If profiling
//! shows pump stalls, move these to `spawn_blocking` behind the same trait.

use std::sync::Arc;

use async_trait::async_trait;

use crate::errors::TinkleError;

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
        self.inner
            .load_checkpoint(principal, stream)
            .map_err(|e| TinkleError::CursorStore(e.to_string()))
    }

    async fn save(&self, principal: &str, stream: &str, cursor: u64) -> Result<(), TinkleError> {
        self.inner
            .save_checkpoint(principal, stream, cursor)
            .map_err(|e| TinkleError::CursorStore(e.to_string()))
    }

    async fn remove_stream(&self, principal: &str, stream: &str) -> Result<(), TinkleError> {
        self.inner
            .remove_stream(principal, stream)
            .map_err(|e| TinkleError::CursorStore(e.to_string()))
    }

    async fn clear_principal(&self, principal: &str) -> Result<(), TinkleError> {
        self.inner
            .clear_principal(principal)
            .map_err(|e| TinkleError::CursorStore(e.to_string()))
    }
}
