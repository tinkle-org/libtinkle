//! Shared native SQLite storage for Swift/Kotlin via UniFFI.
//!
//! Rust owns migrations, transactions, and queries. One file is shared by
//! both mobile platforms. All params are owned; errors map to
//! [`crate::FfiError::Storage`] without SQL text.

use std::sync::Arc;

use crate::{FfiCursorStore, FfiError};

/// FFI-safe cached message.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiMessage {
    pub conversation_key: String,
    pub message_id: i64,
    pub peer_kind: String,
    pub peer_id: i64,
    pub from_id: i64,
    pub body: String,
    pub message_proto: Vec<u8>,
    /// The message's own `accept_seq` token (`None` for legacy rows accepted
    /// before the token was introduced; absent tokens never vote on dedup).
    pub accept_seq: Option<i64>,
}

/// FFI-safe cached dialog.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiDialog {
    pub peer_kind: String,
    pub peer_id: i64,
    pub top_message: i64,
    pub unread_count: i32,
    pub read_inbox_max_id: i64,
    pub read_outbox_max_id: i64,
    pub pinned: bool,
    pub dialog_proto: Vec<u8>,
    /// True when an invalidation committed with the checkpoint and no
    /// authoritative refresh has cleared it yet; the host should refresh.
    pub read_state_pending: bool,
}

impl From<tinkle_storage::MessageRow> for FfiMessage {
    fn from(r: tinkle_storage::MessageRow) -> Self {
        Self {
            conversation_key: r.conversation_key,
            message_id: r.message_id,
            peer_kind: r.peer_kind,
            peer_id: r.peer_id,
            from_id: r.from_id,
            body: r.body,
            message_proto: r.proto,
            accept_seq: r.accept_seq,
        }
    }
}

impl From<tinkle_storage::DialogRow> for FfiDialog {
    fn from(r: tinkle_storage::DialogRow) -> Self {
        Self {
            peer_kind: r.peer_kind,
            peer_id: r.peer_id,
            top_message: r.top_message,
            unread_count: r.unread_count,
            read_inbox_max_id: r.read_inbox_max_id,
            read_outbox_max_id: r.read_outbox_max_id,
            pinned: r.pinned,
            dialog_proto: r.proto,
            read_state_pending: r.read_state_pending,
        }
    }
}

fn map_err(e: tinkle_storage::StorageError) -> FfiError {
    FfiError::Storage(e.to_string())
}

/// Rust-owned SQLite database shared by iOS and Android.
#[derive(uniffi::Object)]
pub struct SqliteStorage {
    inner: Arc<tinkle_storage::Storage>,
}

impl SqliteStorage {
    pub(crate) fn native_handle(&self) -> Arc<tinkle_storage::Storage> {
        self.inner.clone()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl SqliteStorage {
    /// Blocking open: does FS I/O + PRAGMAs + schema sync on the calling
    /// thread. Do NOT call on the UI/main thread; call from a background
    /// queue, or prefer [`Self::open_async`] which moves the work to
    /// `spawn_blocking`. A full async refactor (async constructor as the
    /// only open path) is out of scope to avoid breaking existing
    /// Swift/Kotlin call sites; this doc + the async alternative is the
    /// interim guard.
    #[uniffi::constructor]
    pub fn open(path: String) -> Result<Arc<Self>, FfiError> {
        if path.is_empty() {
            return Err(FfiError::Storage("db path must not be empty".into()));
        }
        let inner = Arc::new(tinkle_storage::Storage::open(&path).map_err(map_err)?);
        Ok(Arc::new(Self { inner }))
    }

    /// Non-blocking open for UI callers: runs [`tinkle_storage::Storage::open`]
    /// on Tokio's blocking pool.
    #[uniffi::constructor]
    pub async fn open_async(path: String) -> Result<Arc<Self>, FfiError> {
        if path.is_empty() {
            return Err(FfiError::Storage("db path must not be empty".into()));
        }
        let inner = tokio::task::spawn_blocking(move || {
            tinkle_storage::Storage::open(&path).map_err(map_err)
        })
        .await
        .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))??;
        Ok(Arc::new(Self {
            inner: Arc::new(inner),
        }))
    }

    #[uniffi::constructor]
    pub fn open_in_memory() -> Result<Arc<Self>, FfiError> {
        let inner = Arc::new(tinkle_storage::Storage::open_in_memory().map_err(map_err)?);
        Ok(Arc::new(Self { inner }))
    }

    /// Durable journal cursor for one conversation: the last applied
    /// `GetTargetChanges.last_accept_seq` (`None` when nothing applied yet).
    /// `target_key` is `target:user:<id>` / `target:room:<id>`.
    pub async fn load_journal(
        &self,
        principal: String,
        target_key: String,
    ) -> Result<Option<i64>, FfiError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner.load_journal(&principal, &target_key).map_err(map_err)
        })
        .await
        .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))?
    }

    pub async fn get_messages(
        &self,
        principal: String,
        conversation_key: String,
        limit: u32,
        before_id: Option<i64>,
    ) -> Result<Vec<FfiMessage>, FfiError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner
                .get_messages(&principal, &conversation_key, limit, before_id)
                .map(|rows| rows.into_iter().map(FfiMessage::from).collect())
                .map_err(map_err)
        })
        .await
        .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))?
    }

    pub async fn list_dialogs(
        &self,
        principal: String,
        limit: u32,
    ) -> Result<Vec<FfiDialog>, FfiError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner
                .list_dialogs(&principal, limit)
                .map(|rows| rows.into_iter().map(FfiDialog::from).collect())
                .map_err(map_err)
        })
        .await
        .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))?
    }

    /// Write plumbing for [`Self::get_user_proto`]: the sync projector never
    /// emits user rows, so without an explicit save (e.g. from a
    /// UsersService refresh) the getter stays `None`. Hosts should call this
    /// after fetching user objects via RPC.
    ///
    /// Validates by decoding the protobuf and checking the embedded id
    /// matches `user_id`: a mismatched or missing id is a codec error,
    /// never a silent cache poison.
    pub async fn save_user_proto(
        &self,
        principal: String,
        user_id: i64,
        user_proto: Vec<u8>,
    ) -> Result<(), FfiError> {
        if user_id <= 0 {
            return Err(FfiError::Codec("user_id must be positive".into()));
        }
        if user_proto.is_empty() || user_proto.len() > 1_000_000 {
            return Err(FfiError::Codec("user_proto bad size".into()));
        }
        {
            use prost::Message as _;
            let decoded = tinkle_schema::proto::User::decode(&*user_proto)
                .map_err(|e| FfiError::Codec(format!("user_proto decode failed: {e}")))?;
            let embedded = match decoded.user_type.as_ref() {
                Some(tinkle_schema::proto::user::UserType::Data(d)) => d.id,
                Some(tinkle_schema::proto::user::UserType::Empty(e)) => e.id,
                None => {
                    return Err(FfiError::Codec("user_proto missing user id".into()));
                }
            };
            if embedded <= 0 {
                return Err(FfiError::Codec(
                    "user_proto embedded id must be positive".into(),
                ));
            }
            if embedded != user_id {
                return Err(FfiError::Codec(format!(
                    "user_proto id mismatch: param {user_id} != embedded {embedded}"
                )));
            }
        }
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner
                .upsert_user(
                    &principal,
                    &tinkle_storage::UserRow {
                        user_id,
                        proto: user_proto,
                    },
                )
                .map_err(map_err)
        })
        .await
        .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))?
    }

    /// Write plumbing for [`Self::get_chat_proto`]: see [`Self::save_user_proto`].
    ///
    /// Validates by decoding the protobuf and checking the embedded
    /// `chat_id` matches: mismatches are codec errors, never silent
    /// cache poison.
    pub async fn save_chat_proto(
        &self,
        principal: String,
        chat_id: i64,
        chat_proto: Vec<u8>,
    ) -> Result<(), FfiError> {
        if chat_id <= 0 {
            return Err(FfiError::Codec("chat_id must be positive".into()));
        }
        if chat_proto.is_empty() || chat_proto.len() > 1_000_000 {
            return Err(FfiError::Codec("chat_proto bad size".into()));
        }
        {
            use prost::Message as _;
            let decoded = tinkle_schema::proto::Chat::decode(&*chat_proto)
                .map_err(|e| FfiError::Codec(format!("chat_proto decode failed: {e}")))?;
            if decoded.chat_id <= 0 {
                return Err(FfiError::Codec(
                    "chat_proto embedded id must be positive".into(),
                ));
            }
            if decoded.chat_id != chat_id {
                return Err(FfiError::Codec(format!(
                    "chat_proto id mismatch: param {chat_id} != embedded {}",
                    decoded.chat_id
                )));
            }
        }
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner
                .upsert_chat(
                    &principal,
                    &tinkle_storage::ChatRow {
                        chat_id,
                        proto: chat_proto,
                    },
                )
                .map_err(map_err)
        })
        .await
        .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))?
    }

    pub async fn get_user_proto(
        &self,
        principal: String,
        user_id: i64,
    ) -> Result<Option<Vec<u8>>, FfiError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner
                .get_user(&principal, user_id)
                .map(|opt| opt.map(|u| u.proto))
                .map_err(map_err)
        })
        .await
        .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))?
    }

    pub async fn get_chat_proto(
        &self,
        principal: String,
        chat_id: i64,
    ) -> Result<Option<Vec<u8>>, FfiError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner
                .get_chat(&principal, chat_id)
                .map(|opt| opt.map(|c| c.proto))
                .map_err(map_err)
        })
        .await
        .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))?
    }

    /// Delete all rows for `principal`, including journal cursors.
    ///
    /// Coordination required: [`TinkleClient`] holds in-memory accept
    /// positions. If the client is still running, the next journal commit
    /// fails the SQLite scope fence because memory expects the deleted
    /// cursor, breaking sync until reconciliation. Callers must
    /// `shutdown()`/`suspend()` the client first and only reconnect (fresh
    /// `subscribe`, reconciling from position 0) after clearing. A full fix
    /// would reset the live reconcile positions atomically with this
    /// delete; that cross-object coordination is out of scope, so this doc
    /// is the guard.
    pub async fn clear_principal(&self, principal: String) -> Result<(), FfiError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.clear_principal(&principal).map_err(map_err))
            .await
            .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))?
    }
}

#[async_trait::async_trait]
impl FfiCursorStore for SqliteStorage {
    async fn load_journal(
        &self,
        principal: String,
        target_key: String,
    ) -> Result<Option<i64>, crate::FfiError> {
        self.load_journal(principal, target_key).await
    }

    async fn save_journal(
        &self,
        principal: String,
        target_key: String,
        accept_seq: i64,
    ) -> Result<(), crate::FfiError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner
                .save_journal(&principal, &target_key, accept_seq)
                .map_err(map_err)
        })
        .await
        .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))?
    }

    async fn remove_target(
        &self,
        principal: String,
        target_key: String,
    ) -> Result<(), crate::FfiError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner
                .remove_target(&principal, &target_key)
                .map_err(map_err)
        })
        .await
        .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))?
    }

    async fn clear_principal(&self, principal: String) -> Result<(), crate::FfiError> {
        self.clear_principal(principal).await
    }
}
