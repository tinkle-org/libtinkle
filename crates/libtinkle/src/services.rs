//! Typed service facades.
//!
//! Method bindings are maintained from BSR `tinkle-org/schema` descriptors
//! (`tinkle-schema::methods::ALL_METHODS`). The generic
//! [`Service::call_typed`] serializes the request protobuf, constructs the
//! Syntra `RpcRequest`, allocates a nonzero `request_id`, sends through
//! the active connection, awaits the matching `RpcResult`, decodes the
//! typed response, and returns typed errors.

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
        // Learn the authenticated principal (`user:<id>`) for sync scope.
        // Only touch live sync state on an actual principal change:
        // setting the principal clears cursors/pending/delivered, so a
        // repeat sign-in for the same user must be a no-op. Switching to
        // a different user requires resubscribing afterwards.
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

/// Updates service: gRPC catch-up plus Syntra follow binding.
pub struct Updates<'a> {
    client: &'a Client,
}

impl<'a> Updates<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Bounded replay over actual gRPC (`ReadUpdates`).
    pub async fn read_updates(
        &self,
        req: tinkle_schema::proto::ReadUpdatesRequest,
    ) -> Result<tinkle_schema::proto::ReadUpdatesResponse, TinkleError> {
        self.client.read_updates_grpc(req).await
    }

    /// Start a follow watch over Syntra (`FollowUpdates`). The correlated
    /// result confirms the subscription; durable changes then arrive as
    /// `FollowUpdatesResponse` notifications.
    pub async fn follow(
        &self,
        req: tinkle_schema::proto::FollowUpdatesRequest,
    ) -> Result<tinkle_schema::proto::FollowUpdatesResponse, TinkleError> {
        // Empty stream selects the caller's own user stream; the gateway
        // resolves it from authentication and echoes the concrete stream.
        call_typed(
            self.client,
            tinkle_schema::sync::FOLLOW_METHOD,
            &req,
            30_000,
        )
        .await
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
