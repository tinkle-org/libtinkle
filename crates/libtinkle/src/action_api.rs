//! Typed actor-private status queries; no cache hydration or mutation replay.
use crate::sending::actions::Status;
use crate::{
    services::{Account, Messages, Stickers},
    Client, TinkleError,
};
use tinkle_schema::proto as p;

/// Channel-owned photo methods use the original channel selector, never a global lookup.
pub struct Channels<'a> {
    client: &'a Client,
}
impl Client {
    /// Typed channel action queries for the currently admitted actor.
    pub fn channels(&self) -> Channels<'_> {
        Channels { client: self }
    }
}

/// Validate before dispatch; generation fencing prevents a delayed result crossing account/logout ABA.
async fn query<Req: prost::Message, Resp: prost::Message + Default>(
    client: &Client,
    method: &str,
    request: &Req,
    cancel: Option<crate::CancelToken>,
) -> Result<Resp, TinkleError> {
    let epoch = client.reconcile_epoch();
    let revision = client.retirement.revision();
    let principal = client.principal().await;
    let caller = client.for_application_scope(epoch, revision);
    let token = cancel.unwrap_or_default();
    let budget = caller.call_budget(30_000)?;
    let manager = caller.live_mgr().await?;
    let hint_revision = manager.action_hint_revision();
    let bytes = caller
        .call_raw_cancellable(
            method,
            request.encode_to_vec(),
            Client::remaining_timeout_ms(&budget)?,
            &token,
        )
        .await?;
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(invalid());
    }
    let mut result = Resp::decode(bytes.as_slice()).map_err(|_| invalid())?;
    let current = caller.live_mgr().await?;
    // A direct cancellation is never submitted twice. Its returned PROCESSING state requires
    // a later lookup; direct reads can refetch once without renewing their original budget.
    if method.contains("/Get")
        && (!std::sync::Arc::ptr_eq(&manager, &current)
            || hint_revision != manager.action_hint_revision())
    {
        let bytes = caller
            .call_raw_cancellable(
                method,
                request.encode_to_vec(),
                Client::remaining_timeout_ms(&budget)?,
                &token,
            )
            .await?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(invalid());
        }
        result = Resp::decode(bytes.as_slice()).map_err(|_| invalid())?;
    }
    if client.reconcile_epoch() != epoch
        || client.retirement.revision() != revision
        || client.principal().await != principal
    {
        return Err(invalid());
    }
    Ok(result)
}
fn invalid() -> TinkleError {
    TinkleError::Codec("invalid domain action status".into())
}
fn uuid(id: &str) -> bool {
    tinkle_schema::sync::session_id(id)
}

/// Optional original bodies must match disclosed receipts; absence never erases acceptance.
trait ResponseBody {
    fn validate_body(&self) -> Result<(), TinkleError>;
}
impl ResponseBody for p::GetSendStatusResponse {
    fn validate_body(&self) -> Result<(), TinkleError> {
        let s = self.send_status.as_ref().ok_or_else(invalid)?;
        if self.messages.is_empty() {
            return if self.users.is_empty() && self.chats.is_empty() {
                Ok(())
            } else {
                Err(invalid())
            };
        }
        if s.state != 3
            || self.messages.len() != s.accepted.len()
            || self.messages.iter().zip(&s.accepted).any(|(m, r)| {
                r.reference_withheld != Some(false)
                    || r.message_id != Some(m.message_id)
                    || m.target != s.target
                    || m.album_id != r.album_id
                    || m.accept_seq.is_none_or(|n| n <= 0)
                    || m.scheduled_state == p::MessageScheduledState::Pending as i32
            })
        {
            return Err(invalid());
        }
        Ok(())
    }
}
macro_rules! photo_body {
    ($response:ty) => {
        impl ResponseBody for $response {
            fn validate_body(&self) -> Result<(), TinkleError> {
                let s = self.photo_update.as_ref().ok_or_else(invalid)?;
                if self.photo.as_ref().is_some_and(|p| {
                    s.state != 3
                        || s.reference_withheld != Some(false)
                        || s.photo_id != Some(p.photo_id)
                        || p.file_id <= 0
                        || p.representation_revision.is_none_or(|r| r == 0)
                }) {
                    return Err(invalid());
                }
                Ok(())
            }
        }
    };
}
photo_body!(p::GetChatPhotoUpdateResponse);
photo_body!(p::GetPhotoUpdateResponse);
photo_body!(p::GetProfilePhotoUpdateResponse);
impl ResponseBody for p::GetAssetChangeResponse {
    fn validate_body(&self) -> Result<(), TinkleError> {
        let s = self.asset_change.as_ref().ok_or_else(invalid)?;
        if self.asset.as_ref().is_some_and(|a| {
            s.state != 3
                || s.reference_withheld != Some(false)
                || s.accepted
                    .as_ref()
                    .is_none_or(|r| r.asset_id != a.id || r.asset_version != a.asset_version)
                || a.file_id <= 0
                || a.representation_revision.is_none_or(|r| r == 0)
        }) {
            return Err(invalid());
        }
        Ok(())
    }
}
macro_rules! no_body {($($response:ty),+)=>{$(impl ResponseBody for $response {fn validate_body(&self)->Result<(),TinkleError>{Ok(())}})+};}
no_body!(
    p::CancelSendResponse,
    p::CancelChatPhotoUpdateResponse,
    p::CancelPhotoUpdateResponse,
    p::CancelProfilePhotoUpdateResponse,
    p::CancelAssetChangeResponse
);

macro_rules! status_query {
    ($owner:ident, $method:ident, $service:literal, $wire:literal, $req:ty, $resp:ty, $field:ident, $cancel:expr, $valid:expr, $bound:expr) => {
        impl $owner<'_> {
            /// Query this actor's exact original domain keys, independently of current bodies.
            pub async fn $method(
                &self,
                request: $req,
                cancel: Option<crate::CancelToken>,
            ) -> Result<$resp, TinkleError> {
                if !($valid)(&request) {
                    return Err(invalid());
                }
                let response: $resp = query(
                    self.client,
                    concat!("/tinkle.v1.", $service, "/", $wire),
                    &request,
                    cancel,
                )
                .await?;
                let status = response.$field.as_ref().ok_or_else(invalid)?;
                status.validate().map_err(|_| invalid())?;
                if !($bound)(&request, status) || ($cancel && status.state == 1) {
                    return Err(invalid());
                }
                response.validate_body()?;
                Ok(response)
            }
        }
    };
}
status_query!(
    Messages,
    get_send_status,
    "MessagesService",
    "GetSendStatus",
    p::GetSendStatusRequest,
    p::GetSendStatusResponse,
    send_status,
    false,
    |r: &p::GetSendStatusRequest| crate::services::validate_target(r.target.as_ref()).is_ok()
        && r.idempotency_key != 0,
    |r: &p::GetSendStatusRequest, s: &p::SendStatus| s.target == r.target
        && (s.state != 3
            || s.accepted
                .iter()
                .any(|a| a.idempotency_key == r.idempotency_key))
);
status_query!(
    Messages,
    cancel_send,
    "MessagesService",
    "CancelSend",
    p::CancelSendRequest,
    p::CancelSendResponse,
    send_status,
    true,
    |r: &p::CancelSendRequest| crate::services::validate_target(r.target.as_ref()).is_ok()
        && r.idempotency_key != 0,
    |r: &p::CancelSendRequest, s: &p::SendStatus| s.target == r.target
        && (s.state != 3
            || s.accepted
                .iter()
                .any(|a| a.idempotency_key == r.idempotency_key))
);
status_query!(
    Messages,
    get_chat_photo_update,
    "MessagesService",
    "GetChatPhotoUpdate",
    p::GetChatPhotoUpdateRequest,
    p::GetChatPhotoUpdateResponse,
    photo_update,
    false,
    |r: &p::GetChatPhotoUpdateRequest| r.chat_id > 0 && r.operation_id > 0,
    |r: &p::GetChatPhotoUpdateRequest, s: &p::ChatPhotoUpdate| r.chat_id == s.chat_id
        && r.operation_id == s.operation_id
);
status_query!(
    Messages,
    cancel_chat_photo_update,
    "MessagesService",
    "CancelChatPhotoUpdate",
    p::CancelChatPhotoUpdateRequest,
    p::CancelChatPhotoUpdateResponse,
    photo_update,
    true,
    |r: &p::CancelChatPhotoUpdateRequest| r.chat_id > 0 && r.operation_id > 0,
    |r: &p::CancelChatPhotoUpdateRequest, s: &p::ChatPhotoUpdate| r.chat_id == s.chat_id
        && r.operation_id == s.operation_id
);
status_query!(
    Channels,
    get_photo_update,
    "ChannelsService",
    "GetPhotoUpdate",
    p::GetPhotoUpdateRequest,
    p::GetPhotoUpdateResponse,
    photo_update,
    false,
    |r: &p::GetPhotoUpdateRequest| r.channel_id > 0 && uuid(&r.request_id),
    |r: &p::GetPhotoUpdateRequest, s: &p::ChannelPhotoUpdate| r.channel_id == s.channel_id
        && r.request_id == s.request_id
);
status_query!(
    Channels,
    cancel_photo_update,
    "ChannelsService",
    "CancelPhotoUpdate",
    p::CancelPhotoUpdateRequest,
    p::CancelPhotoUpdateResponse,
    photo_update,
    true,
    |r: &p::CancelPhotoUpdateRequest| r.channel_id > 0 && uuid(&r.request_id),
    |r: &p::CancelPhotoUpdateRequest, s: &p::ChannelPhotoUpdate| r.channel_id == s.channel_id
        && r.request_id == s.request_id
);
status_query!(
    Account,
    get_profile_photo_update,
    "AccountService",
    "GetProfilePhotoUpdate",
    p::GetProfilePhotoUpdateRequest,
    p::GetProfilePhotoUpdateResponse,
    photo_update,
    false,
    |r: &p::GetProfilePhotoUpdateRequest| uuid(&r.request_id),
    |r: &p::GetProfilePhotoUpdateRequest, s: &p::ProfilePhotoUpdate| r.request_id == s.request_id
);
status_query!(
    Account,
    cancel_profile_photo_update,
    "AccountService",
    "CancelProfilePhotoUpdate",
    p::CancelProfilePhotoUpdateRequest,
    p::CancelProfilePhotoUpdateResponse,
    photo_update,
    true,
    |r: &p::CancelProfilePhotoUpdateRequest| uuid(&r.request_id),
    |r: &p::CancelProfilePhotoUpdateRequest, s: &p::ProfilePhotoUpdate| r.request_id
        == s.request_id
);
status_query!(
    Stickers,
    get_asset_change,
    "StickersService",
    "GetAssetChange",
    p::GetAssetChangeRequest,
    p::GetAssetChangeResponse,
    asset_change,
    false,
    |r: &p::GetAssetChangeRequest| uuid(&r.request_id) && r.asset_id.is_none_or(|id| id > 0),
    |r: &p::GetAssetChangeRequest, s: &p::AssetChange| r.request_id == s.request_id
        && r.asset_id == s.asset_id
);
status_query!(
    Stickers,
    cancel_asset_change,
    "StickersService",
    "CancelAssetChange",
    p::CancelAssetChangeRequest,
    p::CancelAssetChangeResponse,
    asset_change,
    true,
    |r: &p::CancelAssetChangeRequest| uuid(&r.request_id) && r.asset_id.is_none_or(|id| id > 0),
    |r: &p::CancelAssetChangeRequest, s: &p::AssetChange| r.request_id == s.request_id
        && r.asset_id == s.asset_id
);
