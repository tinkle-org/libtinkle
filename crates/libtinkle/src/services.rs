//! Typed service facades.
//!
//! Method bindings are maintained from BSR `tinkle-org/schema` descriptors
//! (`tinkle-schema::methods::ALL_METHODS`). The generic
//! [`Service::call_typed`] serializes the request protobuf, constructs the
//! Syntra `RpcRequest`, allocates a nonzero `request_id`, sends through
//! the active connection, awaits the matching `RpcResult`, decodes the
//! typed response, and returns typed errors.
//!
//! Reconciliation model: `Updates` is connectivity-only
//! (`observe_connectivity`); journal paging (`get_target_changes`) and body
//! fetches (`get_messages`) live on `Messages` via the new method consts.

use prost::Message;

use crate::client::Client;
use crate::errors::TinkleError;

/// Shared typed-call helper (crate-internal; apps use the facades below).
pub(crate) async fn call_typed<Req, Resp>(
    client: &Client,
    method: &str,
    req: &Req,
    timeout_ms: u32,
) -> Result<Resp, TinkleError>
where
    Req: Message,
    Resp: Message + Default,
{
    let mut buf = Vec::with_capacity(req.encoded_len());
    req.encode(&mut buf)
        .map_err(|e| TinkleError::Codec(format!("encode {method}: {e}")))?;
    let out = client.call_raw(method, buf, timeout_ms).await?;
    Resp::decode(&*out).map_err(|e| TinkleError::Codec(format!("decode {method}: {e}")))
}

/// Messages service (`/tinkle.v1.MessagesService/*`).
pub struct Messages<'a> {
    client: &'a Client,
}

impl<'a> Messages<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn send_message(
        &self,
        req: tinkle_schema::proto::SendMessageRequest,
    ) -> Result<tinkle_schema::proto::SendMessageResponse, TinkleError> {
        if req.random_id == 0 {
            return Err(TinkleError::Codec(
                "SendMessageRequest.random_id must be nonzero".to_string(),
            ));
        }
        call_typed(
            self.client,
            tinkle_schema::methods::messagesservice::SEND_MESSAGE,
            &req,
            30_000,
        )
        .await
    }

    /// Fetch specific messages in one conversation (unary). Callers must
    /// chunk to `MAX_MESSAGE_IDS_PER_REQUEST`; see `Client::get_messages`
    /// for the chunking wrapper.
    pub async fn get_messages(
        &self,
        req: tinkle_schema::proto::GetMessagesRequest,
    ) -> Result<tinkle_schema::proto::GetMessagesResponse, TinkleError> {
        call_typed(
            self.client,
            tinkle_schema::sync::GET_MESSAGES_METHOD,
            &req,
            30_000,
        )
        .await
    }

    /// List retained conversation mutations strictly after one accept
    /// position (unary). Pages validate via `validate_changes_page`.
    pub async fn get_target_changes(
        &self,
        req: tinkle_schema::proto::GetTargetChangesRequest,
    ) -> Result<tinkle_schema::proto::GetTargetChangesResponse, TinkleError> {
        call_typed(
            self.client,
            tinkle_schema::sync::GET_TARGET_CHANGES_METHOD,
            &req,
            30_000,
        )
        .await
    }

    /// Read-only message snapshot. Never advances journal cursors: history
    /// is outside the retained mutation journal. Pair with
    /// `Client::load_history` when the snapshot should also hydrate the
    /// offline cache.
    pub async fn get_history(
        &self,
        req: tinkle_schema::proto::GetHistoryRequest,
    ) -> Result<tinkle_schema::proto::GetHistoryResponse, TinkleError> {
        call_typed(
            self.client,
            tinkle_schema::methods::messagesservice::GET_HISTORY,
            &req,
            30_000,
        )
        .await
    }

    /// Read-only dialog snapshot. Never advances journal cursors.
    pub async fn get_dialogs(
        &self,
        req: tinkle_schema::proto::GetDialogsRequest,
    ) -> Result<tinkle_schema::proto::GetDialogsResponse, TinkleError> {
        call_typed(
            self.client,
            tinkle_schema::methods::messagesservice::GET_DIALOGS,
            &req,
            30_000,
        )
        .await
    }

    /// Generic escape hatch for any other MessagesService method.
    pub async fn call<Req, Resp>(
        &self,
        method: &str,
        req: &Req,
        timeout_ms: u32,
    ) -> Result<Resp, TinkleError>
    where
        Req: Message,
        Resp: Message + Default,
    {
        call_typed(self.client, method, req, timeout_ms).await
    }
}

/// Auth service.
pub struct Auth<'a> {
    client: &'a Client,
}

impl<'a> Auth<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn send_code(
        &self,
        req: tinkle_schema::proto::SendCodeRequest,
    ) -> Result<tinkle_schema::proto::SendCodeResponse, TinkleError> {
        call_typed(
            self.client,
            tinkle_schema::methods::authservice::SEND_CODE,
            &req,
            30_000,
        )
        .await
    }

    pub async fn sign_in(
        &self,
        req: tinkle_schema::proto::SignInRequest,
    ) -> Result<tinkle_schema::proto::SignInResponse, TinkleError> {
        let resp: tinkle_schema::proto::SignInResponse = call_typed(
            self.client,
            tinkle_schema::methods::authservice::SIGN_IN,
            &req,
            30_000,
        )
        .await?;
        if let Some(user) = resp.user.as_ref() {
            let id = match user.user_type.as_ref() {
                Some(tinkle_schema::proto::user::UserType::Data(d)) => Some(d.id),
                Some(tinkle_schema::proto::user::UserType::Empty(e)) => Some(e.id),
                None => None,
            };
            if let Some(id) = id {
                if id > 0 {
                    let next = format!("user:{id}");
                    if self.client.principal().await != next {
                        self.client.set_principal_user_id(id).await;
                    }
                }
            }
        }
        Ok(resp)
    }
}

/// Files service (`/tinkle.v1.FilesService/*`).
///
/// All 7 unary RPCs validate locally per contract before touching the
/// transport and use a 30s timeout. Chunking (multi-part upload/download
/// loops) lives with the caller; see `Client` helpers.
pub struct Files<'a> {
    client: &'a Client,
}

impl<'a> Files<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn begin_upload(
        &self,
        req: tinkle_schema::proto::BeginUploadRequest,
    ) -> Result<tinkle_schema::proto::BeginUploadResponse, TinkleError> {
        if req.idempotency_key.is_empty() {
            return Err(TinkleError::Codec(
                "BeginUploadRequest.idempotency_key must be non-empty".to_string(),
            ));
        }
        if req.size_bytes < 0 {
            return Err(TinkleError::Codec(
                "BeginUploadRequest.size_bytes must be non-negative".to_string(),
            ));
        }
        if let Some(d) = req.sha256.as_ref() {
            if d.len() != 32 {
                return Err(TinkleError::Codec(
                    "BeginUploadRequest.sha256 must be 32 bytes".to_string(),
                ));
            }
        }
        call_typed(
            self.client,
            tinkle_schema::methods::filesservice::BEGIN_UPLOAD,
            &req,
            30_000,
        )
        .await
    }

    pub async fn upload_part(
        &self,
        req: tinkle_schema::proto::UploadPartRequest,
    ) -> Result<tinkle_schema::proto::UploadPartResponse, TinkleError> {
        if req.upload_id.is_empty() {
            return Err(TinkleError::Codec(
                "UploadPartRequest.upload_id must be non-empty".to_string(),
            ));
        }
        if req.part_index < 0 {
            return Err(TinkleError::Codec(
                "UploadPartRequest.part_index must be non-negative".to_string(),
            ));
        }
        if req.data.is_empty() {
            return Err(TinkleError::Codec(
                "UploadPartRequest.data must be non-empty".to_string(),
            ));
        }
        if let Some(d) = req.sha256.as_ref() {
            if d.len() != 32 {
                return Err(TinkleError::Codec(
                    "UploadPartRequest.sha256 must be 32 bytes".to_string(),
                ));
            }
        }
        call_typed(
            self.client,
            tinkle_schema::methods::filesservice::UPLOAD_PART,
            &req,
            30_000,
        )
        .await
    }

    pub async fn get_upload_status(
        &self,
        req: tinkle_schema::proto::GetUploadStatusRequest,
    ) -> Result<tinkle_schema::proto::GetUploadStatusResponse, TinkleError> {
        if req.upload_id.is_empty() {
            return Err(TinkleError::Codec(
                "GetUploadStatusRequest.upload_id must be non-empty".to_string(),
            ));
        }
        if let Some(n) = req.page_size {
            if n <= 0 {
                return Err(TinkleError::Codec(
                    "GetUploadStatusRequest.page_size must be positive".to_string(),
                ));
            }
        }
        call_typed(
            self.client,
            tinkle_schema::methods::filesservice::GET_UPLOAD_STATUS,
            &req,
            30_000,
        )
        .await
    }

    pub async fn complete_upload(
        &self,
        req: tinkle_schema::proto::CompleteUploadRequest,
    ) -> Result<tinkle_schema::proto::CompleteUploadResponse, TinkleError> {
        if req.upload_id.is_empty() {
            return Err(TinkleError::Codec(
                "CompleteUploadRequest.upload_id must be non-empty".to_string(),
            ));
        }
        if let Some(d) = req.sha256.as_ref() {
            if d.len() != 32 {
                return Err(TinkleError::Codec(
                    "CompleteUploadRequest.sha256 must be 32 bytes".to_string(),
                ));
            }
        }
        call_typed(
            self.client,
            tinkle_schema::methods::filesservice::COMPLETE_UPLOAD,
            &req,
            30_000,
        )
        .await
    }

    pub async fn abort_upload(
        &self,
        req: tinkle_schema::proto::AbortUploadRequest,
    ) -> Result<tinkle_schema::proto::AbortUploadResponse, TinkleError> {
        if req.upload_id.is_empty() {
            return Err(TinkleError::Codec(
                "AbortUploadRequest.upload_id must be non-empty".to_string(),
            ));
        }
        call_typed(
            self.client,
            tinkle_schema::methods::filesservice::ABORT_UPLOAD,
            &req,
            30_000,
        )
        .await
    }

    pub async fn get_file_info(
        &self,
        req: tinkle_schema::proto::GetFileInfoRequest,
    ) -> Result<tinkle_schema::proto::GetFileInfoResponse, TinkleError> {
        if req.file_id <= 0 {
            return Err(TinkleError::Codec(
                "GetFileInfoRequest.file_id must be positive".to_string(),
            ));
        }
        call_typed(
            self.client,
            tinkle_schema::methods::filesservice::GET_FILE_INFO,
            &req,
            30_000,
        )
        .await
    }

    pub async fn download_part(
        &self,
        req: tinkle_schema::proto::DownloadPartRequest,
    ) -> Result<tinkle_schema::proto::DownloadPartResponse, TinkleError> {
        if req.file_id <= 0 {
            return Err(TinkleError::Codec(
                "DownloadPartRequest.file_id must be positive".to_string(),
            ));
        }
        if req.offset < 0 {
            return Err(TinkleError::Codec(
                "DownloadPartRequest.offset must be non-negative".to_string(),
            ));
        }
        if req.limit <= 0 {
            return Err(TinkleError::Codec(
                "DownloadPartRequest.limit must be positive".to_string(),
            ));
        }
        call_typed(
            self.client,
            tinkle_schema::methods::filesservice::DOWNLOAD_PART,
            &req,
            30_000,
        )
        .await
    }

    /// Generic escape hatch for any other FilesService method.
    pub async fn call<Req, Resp>(
        &self,
        method: &str,
        req: &Req,
        timeout_ms: u32,
    ) -> Result<Resp, TinkleError>
    where
        Req: Message,
        Resp: Message + Default,
    {
        call_typed(self.client, method, req, timeout_ms).await
    }
}

/// Updates service: connectivity observations only. The durable update feed
/// retired; reconciliation runs over `MessagesService/GetTargetChanges` +
/// `GetMessages`, and notifications are opaque reconcile hints.
pub struct Updates<'a> {
    client: &'a Client,
}

impl<'a> Updates<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Start connectivity observations over Syntra (`ObserveConnectivity`).
    pub async fn observe_connectivity(
        &self,
        req: tinkle_schema::proto::ObserveConnectivityRequest,
    ) -> Result<tinkle_schema::proto::ObserveConnectivityResponse, TinkleError> {
        call_typed(
            self.client,
            tinkle_schema::sync::OBSERVE_METHOD,
            &req,
            30_000,
        )
        .await
    }
}
