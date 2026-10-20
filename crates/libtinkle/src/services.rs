//! Typed service facades.
//!
//! Method bindings are maintained from verified public descriptors
//! (`tinkle-schema::methods::ALL_METHODS`, generated from the vendored
//! `tinkle/v1/*.proto` service definitions). The generic
//! [`Service::call_typed`] serializes the request protobuf, constructs the
//! Syntra `RpcRequest`, allocates a nonzero `request_id`, sends through
//! the active connection, awaits the matching `RpcResult`, decodes the
//! typed response, and returns typed errors. Representative hand-written
//! helpers (messages, auth, realtime) demonstrate the pattern; all other
//! methods use the same generic path without manual duplication.

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
        // Persist the exact request + nonzero signed random_id before
        // sending (REALTIME.md §6). A lost result means unknown outcome;
        // retry the unchanged request/key with fresh transport correlation.
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
        call_typed(
            self.client,
            tinkle_schema::methods::authservice::SIGN_IN,
            &req,
            30_000,
        )
        .await
    }
}

/// Realtime subscription RPCs (Syntra dispatcher identifiers, not gRPC).
pub struct Realtime<'a> {
    client: &'a Client,
}

impl<'a> Realtime<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn subscribe_batched(
        &self,
        req: tinkle_schema::proto::SubscribeRequest,
    ) -> Result<tinkle_schema::proto::SubscribeResponse, TinkleError> {
        call_typed(
            self.client,
            tinkle_schema::methods::realtime::SUBSCRIBE_BATCHED,
            &req,
            30_000,
        )
        .await
    }

    pub async fn unsubscribe_batched(
        &self,
        req: tinkle_schema::proto::UnsubscribeRequest,
    ) -> Result<tinkle_schema::proto::UnsubscribeResponse, TinkleError> {
        call_typed(
            self.client,
            tinkle_schema::methods::realtime::UNSUBSCRIBE_BATCHED,
            &req,
            30_000,
        )
        .await
    }
}
