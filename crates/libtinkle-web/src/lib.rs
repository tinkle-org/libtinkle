//! `libtinkle-web`: wasm-bindgen wrapper over `libtinkle` for browsers.
//!
//! This crate is the browser entry point. It reuses the same async core
//! (`libtinkle` → `syntra-core` + `syntra-ws`) as native; it never
//! reimplements protocol behavior. WASM transport goes through
//! `syntra-ws::wasm` (`ws_stream_wasm`, browser `WebSocket`), not
//! `tokio-tungstenite` (native-only, gated to `not(wasm32)`).
//!
//! Status: compile-verified plus a JS-callable surface. Browser runtime
//! acceptance is NOT established: `libtinkle::Client::connect` drives its
//! notification pump with `tokio::spawn`, which panics without an active
//! Tokio runtime on the browser executor. The `spawn_local` adaptation
//! (drive the pump with `wasm-bindgen-futures::spawn_local` on `wasm32`)
//! is still required before a browser connection can be established.
//! All async entry points below therefore document their runtime state
//! honestly, and `connect`/`start` on `wasm32` return an
//! unsupported-runtime error instead of claiming success.
//!
//! Transport note: the published `syntra-ws =1.0.0-rc.1` `wasm` adapter is
//! fully implemented (`ws_stream_wasm::WsMeta::connect`, binary-only pumps
//! via `wasm-bindgen-futures::spawn_local`, handshake).
//!
//! Sync model: per-target journal reconciliation. Notification hints and
//! reconnects drive `GetTargetChanges` pages per conversation, `accept_seq`
//! dedupes, and `GetMessages` hydrates bodies. There are no replay cursors
//! to acknowledge: progress is queryable via `target_cursor`/
//! `target_status`/`tracked_targets`, and `reconcile_now` triggers a pass
//! on demand.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use wasm_bindgen::prelude::*;

use libtinkle::{Client, ClientConfig, Credential, CursorStore, TinkleError};

fn js_err(e: impl ToString) -> JsValue {
    JsValue::from_str(&e.to_string())
}

fn tinkle_err(e: TinkleError) -> JsValue {
    JsValue::from_str(&e.to_string())
}

/// IndexedDB-backed journal store (stub).
///
/// Currently an in-memory map with the same principal/target isolation as
/// the native store. Production web must replace the interior with
/// `indexed-db-futures` persistence.
///
/// TODO(indexeddb): persist per-principal journal cursors in IndexedDB
/// (one object store keyed by `(principal, target_key)`), isolate
/// by authenticated principal, never persist secrets (positions only).
#[derive(Debug, Default)]
pub struct IndexedDbCursorStore {
    inner: Mutex<HashMap<(String, String), i64>>,
}

impl IndexedDbCursorStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait::async_trait]
impl CursorStore for IndexedDbCursorStore {
    async fn load_journal(
        &self,
        principal: &str,
        target_key: &str,
    ) -> Result<Option<i64>, TinkleError> {
        Ok(self
            .inner
            .lock()
            .map_err(|e| TinkleError::CursorStore(format!("web store lock: {e}")))?
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
            .map_err(|e| TinkleError::CursorStore(format!("web store lock: {e}")))?
            .insert((principal.to_string(), target_key.to_string()), accept_seq);
        Ok(())
    }

    async fn remove_target(&self, principal: &str, target_key: &str) -> Result<(), TinkleError> {
        self.inner
            .lock()
            .map_err(|e| TinkleError::CursorStore(format!("web store lock: {e}")))?
            .remove(&(principal.to_string(), target_key.to_string()));
        Ok(())
    }

    async fn clear_principal(&self, principal: &str) -> Result<(), TinkleError> {
        self.inner
            .lock()
            .map_err(|e| TinkleError::CursorStore(format!("web store lock: {e}")))?
            .retain(|(p, _), _| p != principal);
        Ok(())
    }
}

/// One conversation interest for `start`/`subscribe`
/// (JSON: `{"target_kind":"room","target_id":7}`).
/// `target_kind` is `"user"` for direct messages, `"room"`/`"chat"` for
/// group conversations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TargetInterest {
    target_kind: String,
    target_id: i64,
}

fn build_targets(
    interests: Vec<TargetInterest>,
) -> Result<Vec<tinkle_schema::proto::Target>, JsValue> {
    interests
        .into_iter()
        .map(|t| {
            tinkle_schema::sync::target_from_parts(&t.target_kind, t.target_id)
                .map_err(|e| JsValue::from_str(&e.to_string()))
        })
        .collect()
}

async fn observe_watch(inner: &Client, watch: Vec<i64>) -> Result<(), JsValue> {
    if watch.is_empty() {
        return Ok(());
    }
    let req = tinkle_schema::proto::ObserveConnectivityRequest { user_ids: watch };
    inner
        .updates()
        .observe_connectivity(req)
        .await
        .map_err(tinkle_err)?;
    Ok(())
}

/// Browser client wrapping the shared `libtinkle::Client`.
///
/// Retained across JS calls (unlike the legacy one-shot `tinkle_connect`):
/// construct once, then `connect`/`start`/`subscribe`/`next_event`/
/// `get_messages`/`get_target_changes`/`reconcile_now`/`load_history`/
/// `load_dialogs` on the same object.
/// All async methods run via `wasm-bindgen-futures` (the `#[wasm_bindgen]`
/// async transform); no `tokio` runtime is required on the JS side.
#[wasm_bindgen]
pub struct TinkleWebClient {
    inner: Client,
}

#[wasm_bindgen]
impl TinkleWebClient {
    /// Create a client. `url` must be `wss://host/syntra/v1/ws`
    /// (production); `token` is presented only as
    /// `ClientHello.credential`, never in the URL.
    #[wasm_bindgen(constructor)]
    pub fn new(url: String, token: String) -> Result<TinkleWebClient, JsValue> {
        let config = ClientConfig::new(url);
        let credential = Credential::from_session_token(&token);
        // TODO(indexeddb): share one IndexedDB handle per origin instead
        // of one stub per client once the real backend lands.
        let store = IndexedDbCursorStore::new();
        Ok(Self {
            inner: Client::new(config, credential, store),
        })
    }

    /// Foreground connect: WSS upgrade + Syntra handshake to ACTIVE.
    ///
    /// `wasm32` runtime state: returns an unsupported-runtime error until
    /// the `spawn_local` pump adaptation lands (see module docs). Native
    /// builds connect for testing.
    pub async fn connect(&self) -> Result<(), JsValue> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = &self.inner;
            return Err(JsValue::from_str(
                "unsupported runtime: libtinkle::Client::connect uses tokio::spawn for its \
                 notification pump, which panics without an active Tokio runtime; \
                 browser needs spawn_local adaptation before a connection can be established",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.inner.connect().await.map_err(tinkle_err)
        }
    }

    /// Set the authenticated principal (`user:<id>`) after sign-in and on
    /// every identity change. `new()` leaves the client scoped to the
    /// `unknown` principal, and `start`/`subscribe` reject `unknown`, so
    /// call this with the authenticated user id before subscribing.
    pub async fn set_principal_user_id(&self, user_id: i64) -> Result<(), JsValue> {
        if user_id <= 0 {
            return Err(JsValue::from_str("user_id must be positive"));
        }
        self.inner.set_principal_user_id(user_id).await;
        Ok(())
    }

    /// Supervised lifecycle: connect (if needed), subscribe, and start the
    /// reconciliation supervisor. `targets_json` is a JSON array of
    /// `{"target_kind":"room","target_id":7}`; `watch_json` is a JSON array
    /// of user ids for connectivity observations. Returns the subscribe
    /// confirmation as JSON (`{"targets":["target:room:7"]}`).
    ///
    /// Requires `set_principal_user_id` first: a client still scoped to
    /// the `unknown` principal is rejected before any subscription.
    ///
    /// `wasm32` runtime state: the supervisor monitor cannot spawn on the
    /// browser executor yet; this returns the core's stub error there.
    /// Manual `connect`/`subscribe` share the same limitation until the
    /// `spawn_local` adaptation lands.
    pub async fn start(&self, targets_json: String, watch_json: String) -> Result<String, JsValue> {
        let interests: Vec<TargetInterest> = serde_json::from_str(&targets_json).map_err(js_err)?;
        let watch: Vec<i64> = serde_json::from_str(&watch_json).map_err(js_err)?;
        for w in &watch {
            if *w <= 0 {
                return Err(JsValue::from_str("watch user_id must be positive"));
            }
        }
        let targets = build_targets(interests)?;
        self.inner.start(targets).await.map_err(tinkle_err)?;
        observe_watch(&self.inner, watch).await?;
        Ok(serde_json::json!({
            "targets": self.inner.tracked_targets().await,
        })
        .to_string())
    }

    /// Subscribe with durable per-target reconciliation. Same JSON contract
    /// as [`Self::start`]; returns the confirmation (tracked conversation
    /// keys, not reconciliation completion). Requires
    /// `set_principal_user_id` first (see [`Self::start`]).
    pub async fn subscribe(
        &self,
        targets_json: String,
        watch_json: String,
    ) -> Result<String, JsValue> {
        let interests: Vec<TargetInterest> = serde_json::from_str(&targets_json).map_err(js_err)?;
        let watch: Vec<i64> = serde_json::from_str(&watch_json).map_err(js_err)?;
        for w in &watch {
            if *w <= 0 {
                return Err(JsValue::from_str("watch user_id must be positive"));
            }
        }
        let targets = build_targets(interests)?;
        self.inner.subscribe(targets).await.map_err(tinkle_err)?;
        observe_watch(&self.inner, watch).await?;
        Ok(serde_json::json!({
            "targets": self.inner.tracked_targets().await,
        })
        .to_string())
    }

    /// Next sync app event as JSON, or `undefined` when closed.
    ///
    /// Reconciliation contract: `conversation_changed`/`messages_changed`
    /// tell the host what to refresh; `reconcile_failed`/
    /// `checkpoint_failed` are per-target and recoverable via
    /// `reconcile_now`. There is nothing to acknowledge.
    pub async fn next_event(&self) -> Result<JsValue, JsValue> {
        let evt = self.inner.next_event().await;
        match evt {
            None => Ok(JsValue::UNDEFINED),
            Some(e) => {
                let v = match e {
                    libtinkle::SyncEvent::ConversationChanged {
                        target_key,
                        applied_accept_seq,
                    } => serde_json::json!({
                        "type": "conversation_changed",
                        "target_key": target_key,
                        "applied_accept_seq": applied_accept_seq,
                    }),
                    libtinkle::SyncEvent::MessagesChanged {
                        target_key,
                        message_ids,
                    } => serde_json::json!({
                        "type": "messages_changed",
                        "target_key": target_key,
                        "message_ids": message_ids,
                    }),
                    libtinkle::SyncEvent::ConversationRemoved { target_key } => {
                        serde_json::json!({
                            "type": "conversation_removed",
                            "target_key": target_key,
                        })
                    }
                    libtinkle::SyncEvent::ReconcileFailed { target_key, error } => {
                        serde_json::json!({
                            "type": "reconcile_failed",
                            "target_key": target_key,
                            "error": error,
                        })
                    }
                    libtinkle::SyncEvent::Connectivity(c) => serde_json::json!({
                        "type": "connectivity",
                        "user_id": c.user_id, "online": c.online, "known": c.known,
                    }),
                    libtinkle::SyncEvent::CheckpointFailed {
                        target_key,
                        accept_seq,
                        error,
                    } => {
                        serde_json::json!({
                            "type": "checkpoint_failed",
                            "target_key": target_key,
                            "accept_seq": accept_seq,
                            "error": error,
                        })
                    }
                };
                serde_json::to_string(&v)
                    .map(|s| JsValue::from_str(&s))
                    .map_err(js_err)
            }
        }
    }

    /// Fetch message history. History lives outside the reconciliation
    /// journal: never advances accept positions. `target_kind` is `"user"`
    /// or `"room"`/`"chat"`; `limit` is 1–100. Returns the encoded
    /// `GetHistoryResponse` protobuf bytes (`Uint8Array`).
    ///
    /// `hydrate_cache` selects the cache-hydrating path (native SQLite;
    /// no-op storage on wasm beyond the fetch) versus network-only.
    /// Snapshot messages are returned but not cached as message rows —
    /// the journal owns message durability.
    pub async fn load_history(
        &self,
        target_kind: String,
        target_id: i64,
        limit: i32,
        hydrate_cache: bool,
    ) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        let target = tinkle_schema::sync::target_from_parts(&target_kind, target_id)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        if limit <= 0 || limit > 100 {
            return Err(JsValue::from_str("limit must be 1..=100"));
        }
        let req = tinkle_schema::proto::GetHistoryRequest {
            target: Some(target),
            offset_id: None,
            offset_date: None,
            add_offset: 0,
            limit,
            max_id: None,
            min_id: None,
        };
        let resp = if hydrate_cache {
            self.inner.load_history(req).await.map_err(tinkle_err)?
        } else {
            self.inner
                .messages()
                .get_history(req)
                .await
                .map_err(tinkle_err)?
        };
        Ok(resp.encode_to_vec())
    }

    /// Fetch dialogs. Like [`Self::load_history`], never touches accept
    /// positions. Returns the encoded `GetDialogsResponse` protobuf
    /// bytes (`Uint8Array`). `limit` (when set) is 1–100.
    pub async fn load_dialogs(
        &self,
        limit: Option<i32>,
        hydrate_cache: bool,
    ) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        if let Some(l) = limit {
            if l <= 0 || l > 100 {
                return Err(JsValue::from_str("limit must be 1..=100"));
            }
        }
        let req = tinkle_schema::proto::GetDialogsRequest {
            space_id: None,
            offset_date: None,
            offset_peer_id: None,
            limit,
            hash: 0,
            cursor: None,
        };
        let resp = if hydrate_cache {
            self.inner.load_dialogs(req).await.map_err(tinkle_err)?
        } else {
            self.inner
                .messages()
                .get_dialogs(req)
                .await
                .map_err(tinkle_err)?
        };
        Ok(resp.encode_to_vec())
    }

    /// Fetch specific messages in one conversation (`GetMessages`).
    /// `target_kind` is `"user"`/`"room"`/`"chat"`;
    /// `message_ids_json` is a JSON array of message ids in the same
    /// conversation. Large batches are chunked to the protocol bound and
    /// merged. Returns the encoded `GetMessagesResponse` protobuf bytes
    /// (`Uint8Array`).
    pub async fn get_messages(
        &self,
        target_kind: String,
        target_id: i64,
        message_ids_json: String,
    ) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        let target = tinkle_schema::sync::target_from_parts(&target_kind, target_id)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let message_ids: Vec<i64> = serde_json::from_str(&message_ids_json).map_err(js_err)?;
        if message_ids.is_empty() {
            return Err(JsValue::from_str("message_ids must not be empty"));
        }
        for id in &message_ids {
            if *id <= 0 {
                return Err(JsValue::from_str("message_ids must be positive"));
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
                .map_err(tinkle_err)?;
            messages.extend(resp.messages);
            users.extend(resp.users);
            chats.extend(resp.chats);
        }
        let merged = tinkle_schema::proto::GetMessagesResponse {
            messages,
            users,
            chats,
        };
        Ok(merged.encode_to_vec())
    }

    /// List retained conversation mutations strictly after one accept
    /// position (`GetTargetChanges`). `after_accept_seq` is the exclusive
    /// lower bound (zero requests everything retained); `limit` bounds one
    /// page (`0` selects the deployment default, `1..=500` otherwise).
    /// Returns the encoded `GetTargetChangesResponse` protobuf bytes
    /// (`Uint8Array`).
    pub async fn get_target_changes(
        &self,
        target_kind: String,
        target_id: i64,
        after_accept_seq: i64,
        limit: i32,
    ) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        let target = tinkle_schema::sync::target_from_parts(&target_kind, target_id)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        if after_accept_seq < 0 {
            return Err(JsValue::from_str("after_accept_seq must be non-negative"));
        }
        if !(0..=tinkle_schema::sync::MAX_TARGET_CHANGES_LIMIT).contains(&limit) {
            return Err(JsValue::from_str("limit must be 0..=500"));
        }
        let resp = self
            .inner
            .get_target_changes(target, after_accept_seq, limit)
            .await
            .map_err(tinkle_err)?;
        Ok(resp.encode_to_vec())
    }

    /// Files upload session: `BeginUpload`. `request` is the encoded
    /// `BeginUploadRequest` protobuf bytes; returns the encoded
    /// `BeginUploadResponse` bytes (`Uint8Array`).
    pub async fn files_begin_upload(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        let req = tinkle_schema::proto::BeginUploadRequest::decode(&*request)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let resp = self.inner.begin_upload(req).await.map_err(tinkle_err)?;
        Ok(resp.encode_to_vec())
    }

    /// Files upload: `UploadPart`. Encoded request/response bytes.
    pub async fn files_upload_part(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        let req = tinkle_schema::proto::UploadPartRequest::decode(&*request)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let resp = self.inner.upload_part(req).await.map_err(tinkle_err)?;
        Ok(resp.encode_to_vec())
    }

    /// Files upload: `GetUploadStatus`. Encoded request/response bytes.
    pub async fn files_get_upload_status(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        let req = tinkle_schema::proto::GetUploadStatusRequest::decode(&*request)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let resp = self
            .inner
            .get_upload_status(req)
            .await
            .map_err(tinkle_err)?;
        Ok(resp.encode_to_vec())
    }

    /// Files upload: `CompleteUpload`. Encoded request/response bytes.
    pub async fn files_complete_upload(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        let req = tinkle_schema::proto::CompleteUploadRequest::decode(&*request)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let resp = self.inner.complete_upload(req).await.map_err(tinkle_err)?;
        Ok(resp.encode_to_vec())
    }

    /// Files upload: `AbortUpload`. Encoded request/response bytes.
    pub async fn files_abort_upload(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        let req = tinkle_schema::proto::AbortUploadRequest::decode(&*request)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let resp = self.inner.abort_upload(req).await.map_err(tinkle_err)?;
        Ok(resp.encode_to_vec())
    }

    /// Files download: `GetFileInfo`. Encoded request/response bytes.
    pub async fn files_get_file_info(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        let req = tinkle_schema::proto::GetFileInfoRequest::decode(&*request)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let resp = self.inner.get_file_info(req).await.map_err(tinkle_err)?;
        Ok(resp.encode_to_vec())
    }

    /// Files download: `DownloadPart`. Encoded request/response bytes.
    pub async fn files_download_part(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        let req = tinkle_schema::proto::DownloadPartRequest::decode(&*request)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let resp = self.inner.download_part(req).await.map_err(tinkle_err)?;
        Ok(resp.encode_to_vec())
    }

    /// Rust-owned upload (bytes in, `FileInfo` bytes out).
    ///
    /// Minimal browser pass-through over `UploadManager` (bytes in/out,
    /// no filesystem). `sha256` is an optional 32-byte digest
    /// (`undefined` represents absence, preserving `Option` presence for
    /// idempotency).
    pub async fn files_upload_bytes(
        &self,
        idempotency_key: String,
        data: Vec<u8>,
        file_name: Option<String>,
        mime_type: Option<String>,
        sha256: Option<Vec<u8>>,
        encrypted: bool,
    ) -> Result<Vec<u8>, JsValue> {
        use prost::Message as _;
        if idempotency_key.is_empty() {
            return Err(JsValue::from_str("idempotency_key must be non-empty"));
        }
        if let Some(sha) = sha256.as_ref() {
            if sha.len() != 32 {
                return Err(JsValue::from_str("sha256 must be 32 bytes"));
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
            .map_err(tinkle_err)?;
        Ok(file.encode_to_vec())
    }

    /// Rust-owned download (file id in, bytes out).
    ///
    /// Minimal browser pass-through over `DownloadManager`: verifies size
    /// + SHA-256. No per-consumer token crosses the boundary (passes
    /// `None`); dropping the JS promise does not cancel the shared
    /// download.
    pub async fn files_download_bytes(&self, file_id: i64) -> Result<Vec<u8>, JsValue> {
        if file_id <= 0 {
            return Err(JsValue::from_str("file_id must be positive"));
        }
        self.inner
            .download_bytes(file_id, None)
            .await
            .map_err(tinkle_err)
    }

    /// Trigger a reconciliation pass over the tracked conversations now.
    pub async fn reconcile_now(&self) -> Result<(), JsValue> {
        self.inner.reconcile_now().await.map_err(tinkle_err)
    }

    /// Last applied accept position for a tracked conversation
    /// (`target:user:<id>` / `target:room:<id>`), or `undefined` untracked.
    pub async fn target_cursor(&self, target_key: String) -> Result<JsValue, JsValue> {
        match self.inner.target_cursor(&target_key).await {
            Some(c) => Ok(JsValue::from_f64(c as f64)),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    /// Per-target reconciliation status (`"idle"`/`"reconciling"`/
    /// `"failed"`), or `undefined` untracked.
    pub async fn target_status(&self, target_key: String) -> Result<JsValue, JsValue> {
        match self.inner.target_status(&target_key).await {
            Some(s) => Ok(JsValue::from_str(match s {
                libtinkle::ReconcileStatus::Idle => "idle",
                libtinkle::ReconcileStatus::Reconciling => "reconciling",
                libtinkle::ReconcileStatus::Failed => "failed",
            })),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    /// Canonical keys of the tracked conversations, as a JSON array string.
    pub async fn tracked_targets(&self) -> Result<String, JsValue> {
        Ok(serde_json::json!(self.inner.tracked_targets().await).to_string())
    }

    /// True while the transport is live.
    pub async fn is_connected(&self) -> bool {
        self.inner.is_connected().await
    }

    /// Background suspension: graceful close, durable positions kept.
    pub async fn suspend(&self) {
        self.inner.suspend().await;
    }

    /// Clear suspension set by [`Self::suspend`].
    pub async fn resume(&self) {
        self.inner.resume().await;
    }

    /// Reconnect + re-handshake + explicit resubscribe from tracked targets.
    pub async fn reconnect(&self) -> Result<(), JsValue> {
        self.inner.reconnect().await.map_err(tinkle_err)
    }

    /// Graceful shutdown: stop RPCs, tear down transport.
    pub async fn shutdown(&self) {
        self.inner.close().await;
    }
}

/// Legacy one-shot proof: construct a `Client` with an ephemeral store.
/// Does NOT establish a browser connection.
///
/// On `wasm32` this returns an unsupported-runtime error instead of
/// claiming success (and instead of calling `Client::connect`, which would
/// panic). Native builds retain the one-shot connect path for testing.
/// Prefer [`TinkleWebClient`] for retained multi-call sessions.
///
/// `url` must be `wss://host/syntra/v1/ws` (production) or an explicitly
/// allowed insecure dev URL. `token` is presented only as
/// `ClientHello.credential`, never in the URL.
#[wasm_bindgen]
pub async fn tinkle_connect(url: String, token: String) -> Result<(), JsValue> {
    #[cfg(target_arch = "wasm32")]
    {
        let _ = (url, token);
        return Err(JsValue::from_str(
            "unsupported runtime: libtinkle::Client::connect uses tokio::spawn for its \
             notification pump, which panics without an active Tokio runtime; \
             browser needs spawn_local adaptation before a connection can be established",
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let config = ClientConfig::new(url);
        let credential = Credential::from_session_token(&token);
        let store = IndexedDbCursorStore::new();
        let client = Client::new(config, credential, store);
        client.connect().await.map_err(tinkle_err)?;
        Ok(())
    }
}
