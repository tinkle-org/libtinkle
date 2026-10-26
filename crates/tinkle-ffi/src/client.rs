//! UniFFI client object: the single entry point for Swift/Kotlin.
//!
//! [`TinkleClient`] wraps [`libtinkle::Client`]. All method params are
//! owned (`String`/`Vec`/records); no `&str`, no Syntra/prost types, and
//! no cancellation tokens cross the boundary. Dropping the foreign
//! future cancels the in-flight Rust future.
//!
//! Lifecycle (supervised, demand-driven):
//! `new` → `connect` (or `start` for supervised) → `subscribe` →
//! `next_event` → `suspend`/`resume`/`reconnect` → `shutdown`/`stop`.
//! `start` establishes the session and owns automatic reconciliation from
//! there on. Reconciliation is per-target: notification hints and
//! reconnects drive `GetTargetChanges` journal pages per conversation,
//! `accept_seq` dedupes, and `GetMessages` hydrates bodies. There are no
//! replay cursors to acknowledge and no checkpoints to advance by hand:
//! progress is queryable via `target_cursor`/`target_status`/
//! `tracked_targets`, and `reconcile_now` triggers a reconciliation pass
//! on demand.

use std::sync::Arc;

use crate::{ClientConfigFfi, FfiCursorStore, FfiError};

/// One conversation interest for [`TinkleClient::subscribe`] /
/// [`TinkleClient::start`].
///
/// A record (not a tuple) because UniFFI has no tuple type.
/// `target_kind` is `"user"` for direct messages, `"room"`/`"chat"` for
/// group conversations; `target_id` must be positive.
#[derive(Debug, Clone, uniffi::Record)]
pub struct TargetSub {
    pub target_kind: String,
    pub target_id: i64,
}

/// Subscribe confirmation: the canonical keys of the tracked
/// conversations (`target:user:<id>` / `target:room:<id>`).
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiSubscribeResponse {
    pub targets: Vec<String>,
}

/// Per-target reconciliation status (see `libtinkle::ReconcileStatus`).
///
/// `None` from [`TinkleClient::target_status`] means untracked (never
/// subscribed or since removed); any returned variant is the live
/// per-target state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum FfiReconcileStatus {
    Idle,
    Reconciling,
    Failed,
}

impl From<libtinkle::ReconcileStatus> for FfiReconcileStatus {
    fn from(s: libtinkle::ReconcileStatus) -> Self {
        match s {
            libtinkle::ReconcileStatus::Idle => Self::Idle,
            libtinkle::ReconcileStatus::Reconciling => Self::Reconciling,
            libtinkle::ReconcileStatus::Failed => Self::Failed,
        }
    }
}

/// Cache/network policy for [`TinkleClient::load_history`] and
/// [`TinkleClient::load_dialogs`].
///
/// History and dialogs live outside the reconciliation journal: neither
/// policy advances or resets any accept-seq position. `HydrateCache`
/// additionally upserts the authoritative snapshot into the shared
/// SQLite cache (users/chats, plus authoritative dialogs); `NetworkOnly`
/// returns the snapshot without touching the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum FfiLoadPolicy {
    NetworkOnly,
    HydrateCache,
}

/// History page request (mirrors `GetHistoryRequest` without proto types).
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiHistoryRequest {
    /// `"user"` for direct messages, `"room"`/`"chat"` for groups.
    pub target_kind: String,
    pub target_id: i64,
    pub limit: i32,
    pub offset_id: Option<i64>,
    pub add_offset: i32,
    pub max_id: Option<i64>,
    pub min_id: Option<i64>,
    pub offset_date: Option<i32>,
}

/// Dialog list request (mirrors `GetDialogsRequest` without proto types).
///
/// `offset_peer_id` keeps the numeric peer vocabulary: it is a raw target
/// id used together with `offset_date` for stable cursor pagination, not a
/// `Target` kind/id pair.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiDialogsRequest {
    pub limit: Option<i32>,
    pub offset_date: Option<i32>,
    pub offset_peer_id: Option<i64>,
    pub cursor: Option<String>,
    pub space_id: Option<i64>,
}

/// App-visible sync events.
///
/// Journal payloads cross as protobuf bytes where noted; hosts decode with
/// their own generated bindings. Reconciliation progress is queryable via
/// `target_cursor`/`target_status` instead of inferring it from events.
#[derive(Debug, Clone, uniffi::Enum)]
pub enum FfiSyncEvent {
    /// One conversation reconciled forward: `applied_accept_seq` is the new
    /// per-target accept position. Refresh the conversation from the cache
    /// (or hydrate via `get_messages`).
    ConversationChanged {
        target_key: String,
        applied_accept_seq: i64,
    },
    /// Fresh bodies arrived for these message ids in one conversation
    /// (send reconciliation / hydration). Fetch via `get_messages` or read
    /// the cache.
    MessagesChanged {
        target_key: String,
        message_ids: Vec<i64>,
    },
    /// The conversation is gone (deleted/revoked scope). Drop cached rows.
    ConversationRemoved { target_key: String },
    /// Reconciliation for one conversation failed and is parked in `Failed`
    /// status; `error` is diagnostic. Retry via `reconcile_now`.
    ReconcileFailed { target_key: String, error: String },
    Connectivity {
        user_id: i64,
        online: bool,
        known: bool,
    },
    CheckpointFailed {
        target_key: String,
        accept_seq: i64,
        error: String,
    },
}

impl From<libtinkle::SyncEvent> for FfiSyncEvent {
    fn from(e: libtinkle::SyncEvent) -> Self {
        match e {
            libtinkle::SyncEvent::ConversationChanged {
                target_key,
                applied_accept_seq,
            } => Self::ConversationChanged {
                target_key,
                applied_accept_seq,
            },
            libtinkle::SyncEvent::MessagesChanged {
                target_key,
                message_ids,
            } => Self::MessagesChanged {
                target_key,
                message_ids,
            },
            libtinkle::SyncEvent::ConversationRemoved { target_key } => {
                Self::ConversationRemoved { target_key }
            }
            libtinkle::SyncEvent::ReconcileFailed { target_key, error } => {
                Self::ReconcileFailed { target_key, error }
            }
            libtinkle::SyncEvent::Connectivity(c) => Self::Connectivity {
                user_id: c.user_id,
                online: c.online,
                known: c.known,
            },
            libtinkle::SyncEvent::CheckpointFailed {
                target_key,
                accept_seq,
                error,
            } => Self::CheckpointFailed {
                target_key,
                accept_seq,
                error,
            },
        }
    }
}

/// UniFFI client object.
#[derive(uniffi::Object)]
pub struct TinkleClient {
    inner: libtinkle::Client,
}

impl TinkleClient {
    pub(crate) fn core_client(&self) -> &libtinkle::Client {
        &self.inner
    }
}

fn build_config(config: ClientConfigFfi) -> Result<libtinkle::ClientConfig, FfiError> {
    crate::config::validated_core_config(config)
}

fn build_targets(targets: Vec<TargetSub>) -> Result<Vec<tinkle_schema::proto::Target>, FfiError> {
    targets
        .into_iter()
        .map(|t| {
            tinkle_schema::sync::target_from_parts(&t.target_kind, t.target_id)
                .map_err(|e| FfiError::Codec(e.to_string()))
        })
        .collect()
}

fn check_watch(watch: &[i64]) -> Result<(), FfiError> {
    for w in watch {
        if *w <= 0 {
            return Err(FfiError::Codec(
                "watch user_id must be positive".to_string(),
            ));
        }
    }
    Ok(())
}

async fn observe_watch(inner: &libtinkle::Client, watch: Vec<i64>) -> Result<(), FfiError> {
    if watch.is_empty() {
        return Ok(());
    }
    let req = tinkle_schema::proto::ObserveConnectivityRequest { user_ids: watch };
    inner
        .updates()
        .observe_connectivity(req)
        .await
        .map_err(FfiError::from)?;
    Ok(())
}

#[uniffi::export(async_runtime = "tokio")]
impl TinkleClient {
    /// Create a client. `credential` is the opaque session token;
    /// `store` is the foreign (or in-memory) durable cursor store.
    ///
    /// Fails fast with [`FfiError::Internal`] on an invalid non-blank
    /// `grpc_endpoint` (missing scheme, non-`https` without
    /// `allow_insecure`) instead of surfacing a retried network failure
    /// at reconciliation time.
    #[uniffi::constructor]
    pub fn new(
        config: ClientConfigFfi,
        credential: String,
        store: Arc<dyn FfiCursorStore>,
    ) -> Result<Arc<Self>, FfiError> {
        crate::runtime::init();
        let inner = libtinkle::Client::new(
            build_config(config)?,
            libtinkle::Credential::from_session_token(&credential),
            Arc::new(crate::store::FfiStoreAdapter(store)),
        );
        Ok(Arc::new(Self { inner }))
    }

    /// Create a client sharing one native SQLite file. Rust reconciles
    /// journals and commits cache + accept positions atomically, then emits
    /// `ConversationChanged`/`MessagesChanged`; UI reads the shared cache.
    ///
    /// Fails fast on invalid `grpc_endpoint` like [`Self::new`].
    #[uniffi::constructor]
    pub fn new_sqlite(
        config: ClientConfigFfi,
        credential: String,
        storage: Arc<crate::sqlite::SqliteStorage>,
    ) -> Result<Arc<Self>, FfiError> {
        crate::runtime::init();
        let inner = libtinkle::Client::new_sqlite(
            build_config(config)?,
            libtinkle::Credential::from_session_token(&credential),
            storage.native_handle(),
        );
        Ok(Arc::new(Self { inner }))
    }

    /// Foreground connect: WSS upgrade + Syntra handshake to ACTIVE.
    pub async fn connect(&self) -> Result<(), FfiError> {
        self.inner.connect().await.map_err(FfiError::from)
    }

    /// Supervised lifecycle: connect (if needed), subscribe, and start the
    /// reconciliation supervisor, which owns automatic re-reconciliation
    /// and reconnection from here on. The host only consumes `next_event()`;
    /// hints, dead transports, and per-target failures are handled inside
    /// the SDK and reported as events + target status.
    ///
    /// Idempotent: concurrent callers share the single supervisor and get
    /// the initial establishment result. Call `stop()` (or `shutdown()`)
    /// to shut down; `connect`/`subscribe`/`reconnect` remain as
    /// low-level primitives for manual control.
    pub async fn start(
        &self,
        targets: Vec<TargetSub>,
        watch: Vec<i64>,
    ) -> Result<FfiSubscribeResponse, FfiError> {
        check_watch(&watch)?;
        let core_targets = build_targets(targets)?;
        self.inner
            .start(core_targets)
            .await
            .map_err(FfiError::from)?;
        observe_watch(&self.inner, watch).await?;
        Ok(FfiSubscribeResponse {
            targets: self.inner.tracked_targets().await,
        })
    }

    /// Minimal write path end-to-end: send a text message to a conversation.
    /// Returns the server-assigned message id.
    ///
    /// `target_id` must be positive; `target_kind` is `"user"` for direct
    /// messages, `"room"`/`"chat"` for groups. `random_id` must be nonzero
    /// (and fit in `i64` — the application operation key for idempotent
    /// retry). The server response must carry a message with a positive id;
    /// a missing or non-positive id is a codec error, never zero.
    pub async fn send_message(
        &self,
        target_kind: String,
        target_id: i64,
        text: String,
        random_id: u64,
    ) -> Result<u64, FfiError> {
        use tinkle_schema::proto::SendMessageRequest;

        let target = tinkle_schema::sync::target_from_parts(&target_kind, target_id)
            .map_err(|e| FfiError::Codec(e.to_string()))?;
        if random_id == 0 {
            return Err(FfiError::Codec("random_id must be nonzero".to_string()));
        }
        let random_id = i64::try_from(random_id)
            .map_err(|_| FfiError::Codec("random_id out of i64 range".to_string()))?;
        if random_id <= 0 {
            return Err(FfiError::Codec("random_id must be nonzero".to_string()));
        }
        let req = SendMessageRequest {
            target: Some(target),
            message: text,
            random_id,
            reply_to: None,
            silent: None,
            no_forward: None,
            clear_draft: None,
            entities: Vec::new(),
            schedule_date: None,
        };
        let resp = self
            .inner
            .messages()
            .send_message(req)
            .await
            .map_err(FfiError::from)?;
        let id = resp
            .message
            .map(|m| m.message_id)
            .ok_or_else(|| FfiError::Codec("server omitted message in response".to_string()))?;
        if id <= 0 {
            return Err(FfiError::Codec(format!(
                "server returned bad message_id {id}"
            )));
        }
        u64::try_from(id)
            .map_err(|_| FfiError::Codec(format!("server returned bad message_id {id}")))
    }

    /// Subscribe with durable per-target reconciliation. Returns the
    /// confirmation (tracked conversation keys, not reconciliation
    /// completion: journals reconcile in the background and report via
    /// `next_event` + target status).
    pub async fn subscribe(
        &self,
        targets: Vec<TargetSub>,
        watch: Vec<i64>,
    ) -> Result<FfiSubscribeResponse, FfiError> {
        check_watch(&watch)?;
        let core_targets = build_targets(targets)?;
        self.inner
            .subscribe(core_targets)
            .await
            .map_err(FfiError::from)?;
        observe_watch(&self.inner, watch).await?;
        Ok(FfiSubscribeResponse {
            targets: self.inner.tracked_targets().await,
        })
    }

    /// Next sync app event. `None` when the client is closed.
    ///
    /// Reconciliation contract: `ConversationChanged`/`MessagesChanged`
    /// tell the host what to refresh (cache reads or `get_messages`
    /// hydration); `ReconcileFailed`/`CheckpointFailed` are per-target and
    /// recoverable via `reconcile_now` (target status stays `Failed` until
    /// then). There is nothing to acknowledge: positions advance inside the
    /// SDK as journals reconcile.
    pub async fn next_event(&self) -> Option<FfiSyncEvent> {
        self.inner.next_event().await.map(FfiSyncEvent::from)
    }

    /// Fetch message history. History lives outside the reconciliation
    /// journal: the fetch never advances or resets any accept position.
    /// Message bodies ARE cached: `HydrateCache` upserts authoritative
    /// users/chats into the shared SQLite cache and projects message
    /// bodies into message rows committed at the current journal cursor
    /// (the cursor itself is unchanged — history heals the cache without
    /// moving the journal).
    ///
    /// Returns the encoded `GetHistoryResponse` protobuf bytes; hosts
    /// decode with their own generated bindings. `NetworkOnly` returns
    /// bodies without touching the cache.
    pub async fn load_history(
        &self,
        request: FfiHistoryRequest,
        policy: FfiLoadPolicy,
    ) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;
        let req = build_history_request(request)?;
        let resp = match policy {
            FfiLoadPolicy::NetworkOnly => self
                .inner
                .messages()
                .get_history(req)
                .await
                .map_err(FfiError::from)?,
            FfiLoadPolicy::HydrateCache => {
                self.inner.load_history(req).await.map_err(FfiError::from)?
            }
        };
        Ok(resp.encode_to_vec())
    }

    /// Fetch dialogs. Like [`Self::load_history`], the fetch never touches
    /// accept positions.
    ///
    /// Returns the encoded `GetDialogsResponse` protobuf bytes.
    /// `HydrateCache` also upserts authoritative users/chats plus
    /// authoritative dialog rows.
    pub async fn load_dialogs(
        &self,
        request: FfiDialogsRequest,
        policy: FfiLoadPolicy,
    ) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;
        let req = build_dialogs_request(request)?;
        let resp = match policy {
            FfiLoadPolicy::NetworkOnly => self
                .inner
                .messages()
                .get_dialogs(req)
                .await
                .map_err(FfiError::from)?,
            FfiLoadPolicy::HydrateCache => {
                self.inner.load_dialogs(req).await.map_err(FfiError::from)?
            }
        };
        Ok(resp.encode_to_vec())
    }

    /// Fetch specific messages in one conversation (`GetMessages`).
    ///
    /// `target_kind` is `"user"`/`"room"`/`"chat"`; `message_ids` must be
    /// non-empty positive ids in the same conversation. Large batches are
    /// chunked to the protocol bound (`MAX_MESSAGE_IDS_PER_REQUEST`) and
    /// merged; messages the server cannot disclose are omitted, never
    /// errors. Bodies share the message's stable `accept_seq` token: when
    /// two copies disagree, keep the fresher by later `edit_date`.
    ///
    /// Returns the encoded `GetMessagesResponse` protobuf bytes; hosts
    /// decode with their own generated bindings.
    pub async fn get_messages(
        &self,
        target_kind: String,
        target_id: i64,
        message_ids: Vec<i64>,
    ) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;
        use tinkle_schema::proto::GetMessagesResponse;

        let target = tinkle_schema::sync::target_from_parts(&target_kind, target_id)
            .map_err(|e| FfiError::Codec(e.to_string()))?;
        if message_ids.is_empty() {
            return Err(FfiError::Codec("message_ids must not be empty".to_string()));
        }
        for id in &message_ids {
            if *id <= 0 {
                return Err(FfiError::Codec("message_ids must be positive".to_string()));
            }
        }
        let mut messages = Vec::new();
        let mut users = Vec::new();
        let mut chats = Vec::new();
        for chunk in tinkle_schema::sync::chunk_message_ids(&message_ids) {
            let resp = self
                .inner
                .get_messages(target, chunk)
                .await
                .map_err(FfiError::from)?;
            messages.extend(resp.messages);
            users.extend(resp.users);
            chats.extend(resp.chats);
        }
        let merged = GetMessagesResponse {
            messages,
            users,
            chats,
        };
        Ok(merged.encode_to_vec())
    }

    /// List retained conversation mutations strictly after one accept
    /// position (`GetTargetChanges`).
    ///
    /// `after_accept_seq` is the exclusive lower bound (zero requests
    /// everything retained); `limit` bounds one page (`0` selects the
    /// deployment default, `1..=500` otherwise). Changes ascend strictly by
    /// `accept_seq`; edited changes carry the current body while
    /// deleted/revoked changes carry identity and kind only.
    /// `more_available` reports continuation: `Some(true)` means request the
    /// next page from `last_accept_seq`; `Some(false)` confirms caught up;
    /// absent is unknown, never caught up.
    ///
    /// Returns the encoded `GetTargetChangesResponse` protobuf bytes; hosts
    /// decode with their own generated bindings.
    pub async fn get_target_changes(
        &self,
        target_kind: String,
        target_id: i64,
        after_accept_seq: i64,
        limit: i32,
    ) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;

        let target = tinkle_schema::sync::target_from_parts(&target_kind, target_id)
            .map_err(|e| FfiError::Codec(e.to_string()))?;
        if after_accept_seq < 0 {
            return Err(FfiError::Codec(
                "after_accept_seq must be non-negative".to_string(),
            ));
        }
        if !(0..=tinkle_schema::sync::MAX_TARGET_CHANGES_LIMIT).contains(&limit) {
            return Err(FfiError::Codec("limit must be 0..=500".to_string()));
        }
        let resp = self
            .inner
            .get_target_changes(target, after_accept_seq, limit)
            .await
            .map_err(FfiError::from)?;
        Ok(resp.encode_to_vec())
    }

    /// Files upload session: `BeginUpload` (Syntra primary, gRPC fallback).
    ///
    /// `request`/`response` are the encoded `BeginUploadRequest`/
    /// `BeginUploadResponse` protobuf bytes; hosts decode with their own
    /// generated bindings. Local validation failures surface as
    /// [`FfiError::Codec`].
    pub async fn files_begin_upload(&self, request: Vec<u8>) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;
        let req = tinkle_schema::proto::BeginUploadRequest::decode(&*request)
            .map_err(|e| FfiError::Codec(e.to_string()))?;
        let resp = self.inner.begin_upload(req).await.map_err(FfiError::from)?;
        Ok(resp.encode_to_vec())
    }

    /// Files upload: `UploadPart` (Syntra primary, gRPC fallback).
    ///
    /// Encoded `UploadPartRequest`/`UploadPartResponse` protobuf bytes.
    pub async fn files_upload_part(&self, request: Vec<u8>) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;
        let req = tinkle_schema::proto::UploadPartRequest::decode(&*request)
            .map_err(|e| FfiError::Codec(e.to_string()))?;
        let resp = self.inner.upload_part(req).await.map_err(FfiError::from)?;
        Ok(resp.encode_to_vec())
    }

    /// Files upload: `GetUploadStatus` (Syntra primary, gRPC fallback).
    ///
    /// Encoded `GetUploadStatusRequest`/`GetUploadStatusResponse` bytes.
    pub async fn files_get_upload_status(&self, request: Vec<u8>) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;
        let req = tinkle_schema::proto::GetUploadStatusRequest::decode(&*request)
            .map_err(|e| FfiError::Codec(e.to_string()))?;
        let resp = self
            .inner
            .get_upload_status(req)
            .await
            .map_err(FfiError::from)?;
        Ok(resp.encode_to_vec())
    }

    /// Files upload: `CompleteUpload` (Syntra primary, gRPC fallback).
    ///
    /// Encoded `CompleteUploadRequest`/`CompleteUploadResponse` bytes.
    pub async fn files_complete_upload(&self, request: Vec<u8>) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;
        let req = tinkle_schema::proto::CompleteUploadRequest::decode(&*request)
            .map_err(|e| FfiError::Codec(e.to_string()))?;
        let resp = self
            .inner
            .complete_upload(req)
            .await
            .map_err(FfiError::from)?;
        Ok(resp.encode_to_vec())
    }

    /// Files upload: `AbortUpload` (Syntra primary, gRPC fallback).
    ///
    /// Encoded `AbortUploadRequest`/`AbortUploadResponse` bytes.
    pub async fn files_abort_upload(&self, request: Vec<u8>) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;
        let req = tinkle_schema::proto::AbortUploadRequest::decode(&*request)
            .map_err(|e| FfiError::Codec(e.to_string()))?;
        let resp = self.inner.abort_upload(req).await.map_err(FfiError::from)?;
        Ok(resp.encode_to_vec())
    }

    /// Files download: `GetFileInfo` (Syntra primary, gRPC fallback).
    ///
    /// Encoded `GetFileInfoRequest`/`GetFileInfoResponse` bytes.
    pub async fn files_get_file_info(&self, request: Vec<u8>) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;
        let req = tinkle_schema::proto::GetFileInfoRequest::decode(&*request)
            .map_err(|e| FfiError::Codec(e.to_string()))?;
        let resp = self
            .inner
            .get_file_info(req)
            .await
            .map_err(FfiError::from)?;
        Ok(resp.encode_to_vec())
    }

    /// Files download: `DownloadPart` (Syntra primary, gRPC fallback).
    ///
    /// Encoded `DownloadPartRequest`/`DownloadPartResponse` bytes.
    pub async fn files_download_part(&self, request: Vec<u8>) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;
        let req = tinkle_schema::proto::DownloadPartRequest::decode(&*request)
            .map_err(|e| FfiError::Codec(e.to_string()))?;
        let resp = self
            .inner
            .download_part(req)
            .await
            .map_err(FfiError::from)?;
        Ok(resp.encode_to_vec())
    }

    /// Rust-owned upload (bytes in, `FileInfo` bytes out).
    ///
    /// Minimal pass-through over [`libtinkle::UploadManager`]: preserves the
    /// EXACT `BeginUploadRequest` (incl. `Option` presence) across idempotency
    /// retries, decides SOLELY on `BeginUploadResponse.status`, and verifies
    /// the commit. `sha256` is the optional client declaration (32 bytes);
    /// `None` preserves absence for idempotency. Returns the encoded
    /// `FileInfo` protobuf bytes; hosts decode with their own bindings.
    pub async fn files_upload_bytes(
        &self,
        idempotency_key: String,
        data: Vec<u8>,
        file_name: Option<String>,
        mime_type: Option<String>,
        sha256: Option<Vec<u8>>,
        encrypted: bool,
    ) -> Result<Vec<u8>, FfiError> {
        use prost::Message as _;
        if idempotency_key.is_empty() {
            return Err(FfiError::Codec(
                "idempotency_key must be non-empty".to_string(),
            ));
        }
        if let Some(sha) = sha256.as_ref() {
            if sha.len() != 32 {
                return Err(FfiError::Codec("sha256 must be 32 bytes".to_string()));
            }
        }
        let params = libtinkle::UploadParams {
            idempotency_key,
            file_name,
            mime_type,
            sha256,
            encrypted,
        };
        let file = self
            .inner
            .upload_bytes(params, data)
            .await
            .map_err(FfiError::from)?;
        Ok(file.encode_to_vec())
    }

    /// Rust-owned download (file id in, bytes out).
    ///
    /// Minimal pass-through over [`libtinkle::DownloadManager`]: verifies
    /// size + SHA-256 and joins in-flight downloads. Per-consumer cancel is
    /// cooperative via dropping the foreign future (no token crosses FFI);
    /// cancelling one consumer never cancels the shared download.
    pub async fn files_download_bytes(&self, file_id: i64) -> Result<Vec<u8>, FfiError> {
        if file_id <= 0 {
            return Err(FfiError::Codec("file_id must be positive".to_string()));
        }
        self.inner
            .download_bytes(file_id, None)
            .await
            .map_err(FfiError::from)
    }

    /// Trigger a reconciliation pass over the tracked conversations now
    /// (journal pages + hydration for anything behind). Notification hints
    /// and reconnects trigger this automatically; call it after the app
    /// returns to the foreground or after handling `ReconcileFailed`.
    pub async fn reconcile_now(&self) -> Result<(), FfiError> {
        self.inner.reconcile_now().await.map_err(FfiError::from)
    }

    /// Last applied accept position for a tracked conversation
    /// (`target:user:<id>` / `target:room:<id>`). `None` when untracked.
    /// Authoritative for UI progress: informational events may drop under
    /// pressure, positions do not.
    pub async fn target_cursor(&self, target_key: String) -> Option<i64> {
        self.inner.target_cursor(&target_key).await
    }

    /// Per-target reconciliation status. `None` when untracked (never
    /// subscribed or since removed). Poll this instead of inferring demand
    /// from events: `Reconciling` means a journal page run is in flight or
    /// demanded, `Failed` is parked until `reconcile_now`.
    pub async fn target_status(&self, target_key: String) -> Option<FfiReconcileStatus> {
        self.inner
            .target_status(&target_key)
            .await
            .map(FfiReconcileStatus::from)
    }

    /// Canonical keys of the tracked conversations.
    pub async fn tracked_targets(&self) -> Vec<String> {
        self.inner.tracked_targets().await
    }

    /// True while the transport is live (connection exists and its
    /// notification pump has not exited).
    pub async fn is_connected(&self) -> bool {
        self.inner.is_connected().await
    }

    /// Number of outstanding RPCs on the live connection (zero offline).
    /// Send-state query: a nonzero value after reconnect means results
    /// reported `UNAVAILABLE` with unknown outcome — retry only with the
    /// same `random_id` and fresh correlation.
    pub async fn pending_len(&self) -> u64 {
        self.inner.pending_len().await as u64
    }

    /// Background suspension: graceful close, durable positions kept.
    pub async fn suspend(&self) {
        self.inner.suspend().await;
    }

    /// Clear suspension set by [`Self::suspend`]. The supervisor resumes
    /// reconnecting on its next tick; callers needing immediacy can
    /// `reconnect` directly.
    pub async fn resume(&self) {
        self.inner.resume().await;
    }

    /// True while [`Self::suspend`] is in effect.
    pub async fn is_suspended(&self) -> bool {
        self.inner.is_suspended().await
    }

    /// Reconnect + re-handshake + explicit resubscribe from tracked targets
    /// (network change / wake from push).
    pub async fn reconnect(&self) -> Result<(), FfiError> {
        self.inner.reconnect().await.map_err(FfiError::from)
    }

    /// Set the authenticated principal from a known user id.
    pub async fn set_principal_user_id(&self, user_id: i64) -> Result<(), FfiError> {
        if user_id <= 0 {
            return Err(FfiError::Codec("user_id must be positive".to_string()));
        }
        self.inner.set_principal_user_id(user_id).await;
        Ok(())
    }

    /// Stop the sync supervisor (if running) and tear down the connection.
    /// Idempotent. Supervised apps call this instead of `shutdown`;
    /// both tear down the transport, `stop` names the supervisor path.
    pub async fn stop(&self) {
        self.inner.stop().await;
    }

    // Named `shutdown` instead of `close` because UniFFI generates a
    // `Disposable`/`AutoCloseable.close()` for every object, which collides
    // with a user-defined `close()` in Kotlin.
    /// Graceful shutdown: stop RPCs, tear down transport. Equivalent to
    /// [`Self::stop`].
    pub async fn shutdown(&self) {
        self.inner.close().await;
    }
}

fn build_history_request(
    req: FfiHistoryRequest,
) -> Result<tinkle_schema::proto::GetHistoryRequest, FfiError> {
    use tinkle_schema::proto::GetHistoryRequest;

    let target = tinkle_schema::sync::target_from_parts(&req.target_kind, req.target_id)
        .map_err(|e| FfiError::Codec(e.to_string()))?;
    if req.limit <= 0 || req.limit > 100 {
        return Err(FfiError::Codec("limit must be 1..=100".to_string()));
    }
    if let Some(id) = req.offset_id {
        if id <= 0 {
            return Err(FfiError::Codec("offset_id must be positive".to_string()));
        }
    }
    if let Some(id) = req.max_id {
        if id <= 0 {
            return Err(FfiError::Codec("max_id must be positive".to_string()));
        }
    }
    if let Some(id) = req.min_id {
        if id <= 0 {
            return Err(FfiError::Codec("min_id must be positive".to_string()));
        }
    }
    Ok(GetHistoryRequest {
        target: Some(target),
        offset_id: req.offset_id,
        offset_date: req.offset_date,
        add_offset: req.add_offset,
        limit: req.limit,
        max_id: req.max_id,
        min_id: req.min_id,
    })
}

fn build_dialogs_request(
    req: FfiDialogsRequest,
) -> Result<tinkle_schema::proto::GetDialogsRequest, FfiError> {
    use tinkle_schema::proto::GetDialogsRequest;

    if let Some(limit) = req.limit {
        if limit <= 0 || limit > 100 {
            return Err(FfiError::Codec("limit must be 1..=100".to_string()));
        }
    }
    if let Some(id) = req.offset_peer_id {
        if id <= 0 {
            return Err(FfiError::Codec(
                "offset_peer_id must be positive".to_string(),
            ));
        }
    }
    if let Some(cursor) = &req.cursor {
        if !cursor.is_empty() && (req.offset_date.is_some() || req.offset_peer_id.is_some()) {
            return Err(FfiError::Codec(
                "cursor is mutually exclusive with offset_date/offset_peer_id".to_string(),
            ));
        }
    }
    Ok(GetDialogsRequest {
        space_id: req.space_id,
        offset_date: req.offset_date,
        offset_peer_id: req.offset_peer_id,
        limit: req.limit,
        hash: 0,
        cursor: req.cursor,
    })
}
