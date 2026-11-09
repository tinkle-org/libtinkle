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

/// Account session management on the authenticated channel.
pub struct Account<'a> {
    client: &'a Client,
}

impl<'a> Account<'a> {
    /// Bind the facade to this client's admission and transport lifecycle.
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    fn observe_denial(&self, error: &TinkleError) {
        if matches!(
            error.code(),
            syntra_core::ErrorCode::Unauthenticated | syntra_core::ErrorCode::PermissionDenied
        ) {
            self.client.authorization.set_authentication_known(false);
        }
    }

    /// List active devices with neutral session IDs and descriptive metadata.
    pub async fn get_sessions(&self) -> Result<Vec<tinkle_schema::proto::Session>, TinkleError> {
        let response: tinkle_schema::proto::GetSessionsResponse = call_typed(
            self.client,
            tinkle_schema::methods::accountservice::GET_SESSIONS,
            &tinkle_schema::proto::GetSessionsRequest {},
            3_000,
        )
        .await
        .inspect_err(|error| self.observe_denial(error))?;
        Ok(response.sessions)
    }

    /// Revoke one device UUID once. An unknown transport outcome is never replayed.
    pub async fn revoke_session(&self, session_id: &str) -> Result<bool, TinkleError> {
        if !crate::enrollment::uuid(session_id) {
            return Err(TinkleError::Codec("invalid session identifier".into()));
        }
        let response: tinkle_schema::proto::RevokeSessionResponse = call_typed(
            self.client,
            tinkle_schema::methods::accountservice::REVOKE_SESSION,
            &tinkle_schema::proto::RevokeSessionRequest {
                session_id: session_id.into(),
            },
            3_000,
        )
        .await
        .inspect_err(|error| self.observe_denial(error))?;
        Ok(response.revoked)
    }

    /// Revoke every other device, preserving this device and its local enrollment.
    pub async fn revoke_other_sessions(&self) -> Result<bool, TinkleError> {
        let response: tinkle_schema::proto::RevokeSessionsResponse = call_typed(
            self.client,
            tinkle_schema::methods::accountservice::REVOKE_SESSIONS,
            &tinkle_schema::proto::RevokeSessionsRequest {},
            3_000,
        )
        .await
        .inspect_err(|error| self.observe_denial(error))?;
        Ok(response.revoked)
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
        let response: tinkle_schema::proto::SendCodeResponse = call_typed(
            self.client,
            tinkle_schema::methods::authservice::SEND_CODE,
            &req,
            30_000,
        )
        .await
        .map_err(crate::enrollment::sanitize_auth_error)?;
        if response.verification_id.trim().is_empty() {
            return Err(TinkleError::Codec(
                "server omitted verification identifier".into(),
            ));
        }
        Ok(response)
    }

    pub async fn sign_in(
        &self,
        details: crate::SignInDetails,
        operation: &crate::DurableOperation,
    ) -> Result<crate::SignInOutcome, TinkleError> {
        operation.epoch.check(self.client)?;
        let _flow = self.client.enrollment_lock.lock().await;
        operation.epoch.check(self.client)?;
        let provisioning = self
            .client
            .provisioning_client(
                crate::enrollment::operation_trust(operation),
                &operation.epoch,
            )
            .await?;
        let result: Result<
            crate::enrollment::SecretReply<tinkle_schema::proto::SignInResponse>,
            TinkleError,
        > = crate::enrollment::secret_call(
            &provisioning,
            tinkle_schema::methods::authservice::SIGN_IN,
            details.request(operation)?,
            &operation.epoch,
        )
        .await;
        provisioning.close().await;
        let mut response = result?;
        let delivery = response
            .enrollment
            .as_ref()
            .ok_or_else(|| TinkleError::Codec("missing enrollment".into()))?;
        if let Some(user) = &response.user {
            let id = match &user.user_type {
                Some(tinkle_schema::proto::user::UserType::Data(data)) => data.id,
                Some(tinkle_schema::proto::user::UserType::Empty(empty)) => empty.id,
                None => 0,
            };
            if id != delivery.user_id {
                return Err(TinkleError::Codec("enrollment user mismatch".into()));
            }
        }
        let delivery = response
            .enrollment
            .take()
            .ok_or_else(|| TinkleError::Codec("missing enrollment".into()))?;
        let enrollment = self
            .client
            .persist_delivery(operation, delivery, false)
            .await?;
        Ok(crate::SignInOutcome {
            user: response.user.take(),
            is_new_user: response.is_new_user,
            enrollment,
        })
    }

    /// Recover delivery using only the pre-retained operation/capability.
    pub async fn resume_enrollment(
        &self,
        operation: &crate::DurableOperation,
    ) -> Result<crate::PersistedEnrollment, TinkleError> {
        operation.epoch.check(self.client)?;
        let _flow = self.client.enrollment_lock.lock().await;
        operation.epoch.check(self.client)?;
        let provisioning = self
            .client
            .provisioning_client(
                crate::enrollment::operation_trust(operation),
                &operation.epoch,
            )
            .await?;
        let result: Result<
            crate::enrollment::SecretReply<tinkle_schema::proto::ResumeEnrollmentResponse>,
            TinkleError,
        > = crate::enrollment::secret_call(
            &provisioning,
            "/tinkle.v1.AuthService/ResumeEnrollment",
            crate::enrollment::resume_request(operation)?,
            &operation.epoch,
        )
        .await;
        provisioning.close().await;
        self.client
            .persist_delivery(
                operation,
                result?
                    .enrollment
                    .take()
                    .ok_or_else(|| TinkleError::Codec("missing enrollment".into()))?,
                true,
            )
            .await
    }

    /// Stage rotation on the current ACTIVE Returning connection. The old
    /// active secure-store slot is preserved until the new candidate activates.
    pub async fn rotate_enrollment(
        &self,
        operation: &crate::DurableOperation,
    ) -> Result<crate::PersistedEnrollment, TinkleError> {
        operation.epoch.check(self.client)?;
        let _flow = self.client.enrollment_lock.lock().await;
        operation.epoch.check(self.client)?;
        if !self.client.has_returning_source().await {
            return Err(TinkleError::Configuration(
                "rotation requires Returning".into(),
            ));
        }
        let mut result: crate::enrollment::SecretReply<
            tinkle_schema::proto::RotateEnrollmentResponse,
        > = crate::enrollment::secret_call(
            self.client,
            "/tinkle.v1.AuthService/RotateEnrollment",
            crate::enrollment::rotate_request(operation)?,
            &operation.epoch,
        )
        .await?;
        self.client
            .persist_rotation(
                operation,
                result
                    .enrollment
                    .take()
                    .ok_or_else(|| TinkleError::Codec("missing enrollment".into()))?,
            )
            .await
    }

    /// Acknowledge only an already-persisted candidate via pinned Bootstrap.
    /// Verify ACTIVE, persist that status atomically, then establish fresh Returning.
    pub async fn activate_enrollment(
        &self,
        persisted: &crate::PersistedEnrollment,
    ) -> Result<tinkle_schema::proto::DeviceEnrollmentStatus, TinkleError> {
        persisted.epoch.check(self.client)?;
        let _flow = self.client.enrollment_lock.lock().await;
        persisted.epoch.check(self.client)?;
        self.client.activate_persisted(persisted).await
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
