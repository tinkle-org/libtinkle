//! Cursor-store bridge.
//!
//! The durable store is implemented on the foreign side (Swift/Kotlin) via
//! the [`FfiCursorStore`] callback interface, or with the in-memory
//! [`InMemoryFfiStore`] in tests. [`FfiStoreAdapter`] adapts either into
//! the [`libtinkle::CursorStore`] expected by [`libtinkle::Client`].

use std::sync::Arc;

use libtinkle::CursorStore as _;

/// Foreign-implemented durable cursor store.
///
/// All params are owned (`String`/`u64`); references are unsupported in
/// foreign trait methods. Every method returns `Result` so foreign
/// failures convert into [`crate::FfiError`] instead of panicking.
#[uniffi::export(with_foreign)]
#[async_trait::async_trait]
pub trait FfiCursorStore: Send + Sync {
    async fn load(&self, principal: String, stream: String)
        -> Result<Option<u64>, crate::FfiError>;

    async fn save(
        &self,
        principal: String,
        stream: String,
        cursor: u64,
    ) -> Result<(), crate::FfiError>;

    async fn remove_stream(&self, principal: String, stream: String)
        -> Result<(), crate::FfiError>;

    async fn clear_principal(&self, principal: String) -> Result<(), crate::FfiError>;
}

/// Adapter: foreign/Rust [`FfiCursorStore`] -> [`libtinkle::CursorStore`].
pub(crate) struct FfiStoreAdapter(pub Arc<dyn FfiCursorStore>);

#[async_trait::async_trait]
impl libtinkle::CursorStore for FfiStoreAdapter {
    async fn load(
        &self,
        principal: &str,
        stream: &str,
    ) -> Result<Option<u64>, libtinkle::TinkleError> {
        self.0
            .load(principal.to_owned(), stream.to_owned())
            .await
            .map_err(libtinkle::TinkleError::from)
    }

    async fn save(
        &self,
        principal: &str,
        stream: &str,
        cursor: u64,
    ) -> Result<(), libtinkle::TinkleError> {
        self.0
            .save(principal.to_owned(), stream.to_owned(), cursor)
            .await
            .map_err(libtinkle::TinkleError::from)
    }

    async fn remove_stream(
        &self,
        principal: &str,
        stream: &str,
    ) -> Result<(), libtinkle::TinkleError> {
        self.0
            .remove_stream(principal.to_owned(), stream.to_owned())
            .await
            .map_err(libtinkle::TinkleError::from)
    }

    async fn clear_principal(&self, principal: &str) -> Result<(), libtinkle::TinkleError> {
        self.0
            .clear_principal(principal.to_owned())
            .await
            .map_err(libtinkle::TinkleError::from)
    }
}

/// In-memory store for tests and ephemeral sessions. Wraps
/// [`libtinkle::InMemoryCursorStore`]; production apps implement
/// [`FfiCursorStore`] over SQLite/Keychain/DataStore instead.
#[derive(Debug, Default, uniffi::Object)]
pub struct InMemoryFfiStore {
    inner: libtinkle::InMemoryCursorStore,
}

#[uniffi::export]
impl InMemoryFfiStore {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: libtinkle::InMemoryCursorStore::default(),
        })
    }
}

#[async_trait::async_trait]
impl FfiCursorStore for InMemoryFfiStore {
    async fn load(
        &self,
        principal: String,
        stream: String,
    ) -> Result<Option<u64>, crate::FfiError> {
        self.inner
            .load(&principal, &stream)
            .await
            .map_err(crate::FfiError::from)
    }

    async fn save(
        &self,
        principal: String,
        stream: String,
        cursor: u64,
    ) -> Result<(), crate::FfiError> {
        self.inner
            .save(&principal, &stream, cursor)
            .await
            .map_err(crate::FfiError::from)
    }

    async fn remove_stream(
        &self,
        principal: String,
        stream: String,
    ) -> Result<(), crate::FfiError> {
        self.inner
            .remove_stream(&principal, &stream)
            .await
            .map_err(crate::FfiError::from)
    }

    async fn clear_principal(&self, principal: String) -> Result<(), crate::FfiError> {
        self.inner
            .clear_principal(&principal)
            .await
            .map_err(crate::FfiError::from)
    }
}
