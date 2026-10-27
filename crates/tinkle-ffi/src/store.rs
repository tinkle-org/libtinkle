//! Journal cursor-store bridge.
//!
//! The durable store is implemented on the foreign side (Swift/Kotlin) via
//! the [`FfiCursorStore`] callback interface, or with the in-memory
//! [`InMemoryFfiStore`] in tests. [`FfiStoreAdapter`] adapts either into
//! the [`libtinkle::CursorStore`] expected by [`libtinkle::Client`].
//!
//! Vocabulary is per-target journals: `target_key` is
//! `target:user:<id>` / `target:room:<id>` and `accept_seq` is the last
//! applied `GetTargetChanges.last_accept_seq` (0 is the initial position).

use std::sync::Arc;

use libtinkle::CursorStore as _;

/// Foreign-implemented durable journal store.
///
/// All params are owned (`String`/`i64`); references are unsupported in
/// foreign trait methods. Every method returns `Result` so foreign
/// failures convert into [`crate::FfiError`] instead of panicking.
///
/// Ordering: persist `accept_seq` via `save_journal` only after the page's
/// effects commit (SQLite does both atomically via
/// `apply_journal_page_fenced`; checkpoint-only stores persist after the
/// host applies the page). Never advance the cursor before the bodies it
/// covers are durable.
///
/// Scope: `clear_principal` removes all state owned by `principal` — journal
/// cursors plus, where applicable, cached messages/dialogs/users/chats — so
/// logout leaves no offline bodies behind.
// TODO(runtime-acceptance-storage): GRDB/Room acceptance — principal
// isolation between users, no credential persistence in the cursor store,
// crash recovery resumes reconciliation from the last persisted accept_seq.
#[uniffi::export(rust, foreign)]
#[async_trait::async_trait]
pub trait FfiCursorStore: Send + Sync {
    async fn load_journal(
        &self,
        principal: String,
        target_key: String,
    ) -> Result<Option<i64>, crate::FfiError>;

    async fn save_journal(
        &self,
        principal: String,
        target_key: String,
        accept_seq: i64,
    ) -> Result<(), crate::FfiError>;

    async fn remove_target(
        &self,
        principal: String,
        target_key: String,
    ) -> Result<(), crate::FfiError>;

    async fn clear_principal(&self, principal: String) -> Result<(), crate::FfiError>;
}

/// Adapter: foreign/Rust [`FfiCursorStore`] -> [`libtinkle::CursorStore`].
pub(crate) struct FfiStoreAdapter(pub Arc<dyn FfiCursorStore>);

#[async_trait::async_trait]
impl libtinkle::CursorStore for FfiStoreAdapter {
    async fn load_journal(
        &self,
        principal: &str,
        target_key: &str,
    ) -> Result<Option<i64>, libtinkle::TinkleError> {
        self.0
            .load_journal(principal.to_owned(), target_key.to_owned())
            .await
            .map_err(libtinkle::TinkleError::from)
    }

    async fn save_journal(
        &self,
        principal: &str,
        target_key: &str,
        accept_seq: i64,
    ) -> Result<(), libtinkle::TinkleError> {
        self.0
            .save_journal(principal.to_owned(), target_key.to_owned(), accept_seq)
            .await
            .map_err(libtinkle::TinkleError::from)
    }

    async fn remove_target(
        &self,
        principal: &str,
        target_key: &str,
    ) -> Result<(), libtinkle::TinkleError> {
        self.0
            .remove_target(principal.to_owned(), target_key.to_owned())
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
    async fn load_journal(
        &self,
        principal: String,
        target_key: String,
    ) -> Result<Option<i64>, crate::FfiError> {
        self.inner
            .load_journal(&principal, &target_key)
            .await
            .map_err(crate::FfiError::from)
    }

    async fn save_journal(
        &self,
        principal: String,
        target_key: String,
        accept_seq: i64,
    ) -> Result<(), crate::FfiError> {
        self.inner
            .save_journal(&principal, &target_key, accept_seq)
            .await
            .map_err(crate::FfiError::from)
    }

    async fn remove_target(
        &self,
        principal: String,
        target_key: String,
    ) -> Result<(), crate::FfiError> {
        self.inner
            .remove_target(&principal, &target_key)
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
