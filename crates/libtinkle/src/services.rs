//! Typed service facades.
//!
//! Method bindings are maintained from BSR `tinkle-org/schema` descriptors
//! (`tinkle-schema::methods::ALL_METHODS`). The generic
//! [`call_typed`] serializes the request protobuf, constructs the
//! Syntra `RpcRequest`, allocates a nonzero `request_id`, sends through
//! the active connection, awaits the matching `RpcResult`, decodes the
//! typed response, and returns typed errors.
//!
//! Reconciliation model: `Updates` is connectivity-only
//! (`observe_connectivity`); journal paging (`list_message_changes`) and body
//! fetches (`get_messages`) live on `Messages` via the new method consts.

use prost::Message;

use crate::client::Client;
use crate::errors::TinkleError;
use tinkle_schema::proto as p;

/// Reject malformed application input before allocating a transport request.
fn require(valid: bool, reason: &'static str) -> Result<(), TinkleError> {
    if valid {
        Ok(())
    } else {
        Err(TinkleError::Codec(reason.into()))
    }
}

/// Validate a present conversation; an absent/empty target never selects global scope.
pub(crate) fn validate_target(target: Option<&p::Target>) -> Result<(), TinkleError> {
    require(
        matches!(
            target.and_then(|t| t.kind.as_ref()),
            Some(p::target::Kind::UserId(1..)) | Some(p::target::Kind::ChatId(1..))
        ),
        "positive target required",
    )
}

/// Keep batch work finite and reject nonpositive identifiers without changing caller order.
fn ids(values: &[i64], max: usize, empty: bool) -> Result<(), TinkleError> {
    require(
        (empty || !values.is_empty()) && values.len() <= max && values.iter().all(|id| *id > 0),
        "invalid identifier batch",
    )
}

/// Exact-set APIs reject duplicate identifiers instead of normalizing caller order/content.
fn distinct_ids(values: &[i64], max: usize, empty: bool) -> Result<(), TinkleError> {
    ids(values, max, empty)?;
    require(
        values
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            == values.len(),
        "duplicate identifiers",
    )
}

/// A present file context must name one concrete, positive application reference.
fn file_context(context: Option<&p::FileAccessContext>) -> Result<(), TinkleError> {
    let Some(context) = context else {
        return Ok(());
    };
    match context.reference.as_ref() {
        Some(p::file_access_context::Reference::Message(m)) => {
            validate_target(m.target.as_ref())?;
            require(m.message_id > 0, "invalid message file reference")
        }
        Some(p::file_access_context::Reference::ProfilePhoto(v)) => require(
            v.user_id > 0 && v.photo_id > 0,
            "invalid profile file reference",
        ),
        Some(p::file_access_context::Reference::StickerAsset(v)) => require(
            v.asset_id > 0 && v.asset_version > 0,
            "invalid asset file reference",
        ),
        Some(p::file_access_context::Reference::ChatPhoto(v)) => require(
            v.chat_id > 0 && v.photo_id > 0,
            "invalid chat photo reference",
        ),
        None => Err(TinkleError::Codec("empty file access context".into())),
    }
}

/// Shared page size and optional ID bounds. Zero is accepted only for documented defaults.
fn page(limit: i32, max: i32, default: bool) -> Result<(), TinkleError> {
    require(
        (if default { 0 } else { 1 }..=max).contains(&limit),
        "invalid page size",
    )
}

/// Validate UTF-16 entity ranges without splitting a surrogate pair.
fn entities(text: &str, spans: &[p::Entity]) -> Result<(), TinkleError> {
    require(
        text.len() <= 65536 && spans.len() <= 100,
        "text/entity budget exceeded",
    )?;
    let mut boundaries = std::collections::HashSet::from([0usize]);
    let mut n = 0;
    for c in text.chars() {
        n += c.len_utf16();
        boundaries.insert(n);
    }
    for e in spans {
        let start = usize::try_from(e.offset)
            .map_err(|_| TinkleError::Codec("invalid entity offset".into()))?;
        let len = usize::try_from(e.length)
            .map_err(|_| TinkleError::Codec("invalid entity length".into()))?;
        require(
            len > 0
                && start
                    .checked_add(len)
                    .is_some_and(|end| boundaries.contains(&end))
                && boundaries.contains(&start)
                && (1..=18).contains(&e.r#type),
            "invalid text entity",
        )?;
        if e.r#type == 16 {
            require(
                e.custom_emoji_id > 0
                    && e.asset_version.is_some_and(|v| v > 0)
                    && e.fallback_text
                        .as_ref()
                        .is_some_and(|v| !v.is_empty() && v.len() <= 64),
                "custom emoji requires exact asset version and fallback",
            )?;
        } else {
            require(
                e.custom_emoji_id == 0 && e.asset_version.is_none() && e.fallback_text.is_none(),
                "non-custom entity carries custom emoji metadata",
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod entity_tests {
    use super::*;

    /// Chat photo reads require both exact positive identifiers, never an owner fallback.
    #[test]
    fn chat_photo_context_requires_positive_identity() {
        for (chat_id, photo_id) in [(7, 3), (0, 3), (-1, 3), (7, 0), (7, -1)] {
            let context = p::FileAccessContext {
                reference: Some(p::file_access_context::Reference::ChatPhoto(
                    p::ChatPhotoReference { chat_id, photo_id },
                )),
            };
            assert_eq!(
                file_context(Some(&context)).is_ok(),
                chat_id > 0 && photo_id > 0
            );
        }
    }

    /// Shared Entity ranges use UTF-16 boundaries, and custom emoji retain exact versions.
    #[test]
    fn shared_entity_utf16_ranges_and_version_contract() {
        let valid = p::Entity {
            r#type: p::EntityType::CustomEmoji as i32,
            offset: 1,
            length: 2,
            custom_emoji_id: 9,
            asset_version: Some(u64::MAX),
            fallback_text: Some("🙂".into()),
            ..Default::default()
        };
        assert!(entities("a🙂b", std::slice::from_ref(&valid)).is_ok());
        for invalid in [
            p::Entity {
                offset: 2,
                ..valid.clone()
            },
            p::Entity {
                length: 1,
                ..valid.clone()
            },
            p::Entity {
                asset_version: Some(0),
                ..valid.clone()
            },
            p::Entity {
                asset_version: None,
                ..valid.clone()
            },
            p::Entity {
                custom_emoji_id: 0,
                ..valid.clone()
            },
            p::Entity {
                r#type: 1,
                ..valid.clone()
            },
        ] {
            assert!(entities("a🙂b", &[invalid]).is_err());
        }
        assert!(entities(
            "a🙂b",
            &[p::Entity {
                r#type: 1,
                offset: 1,
                length: 2,
                ..Default::default()
            }]
        )
        .is_ok());
    }
}

/// Reply roots are explicit, positive and conversation-local; server checks visibility.
fn reply(value: Option<&p::ReplyInput>) -> Result<(), TinkleError> {
    if let Some(r) = value {
        require(
            r.reply_to_message_id > 0 && r.thread_root_id.is_none_or(|id| id > 0),
            "invalid reply",
        )?;
        entities(&r.quote_text, &r.quote_entities)?;
    }
    Ok(())
}

/// Validate completed-file references and bounded variant identities, never upload IDs/paths.
fn variants(values: &[p::MediaVariantSource]) -> Result<(), TinkleError> {
    let mut roles = std::collections::HashSet::new();
    let mut files = std::collections::HashSet::new();
    require(values.len() <= 8, "too many media variants")?;
    for v in values {
        require(
            v.file_id > 0 && v.role > 0 && roles.insert(v.role) && files.insert(v.file_id),
            "invalid media variant",
        )?;
    }
    Ok(())
}

/// Validate the typed attachment shape. Ownership, actual media bytes and permissions remain server checks.
pub(crate) fn validate_media(value: Option<&p::MediaSource>) -> Result<(), TinkleError> {
    use p::media_source::Media;
    match value.and_then(|m| m.media.as_ref()) {
        Some(Media::Photo(v)) => {
            require(v.file_id > 0, "invalid photo file")?;
            variants(&v.variants)
        }
        Some(Media::Document(v)) => {
            require(v.file_id > 0 && v.attributes.len() <= 8, "invalid document")?;
            variants(&v.variants)
        }
        Some(Media::Video(v)) => {
            require(v.file_id > 0, "invalid video file")?;
            variants(&v.variants)
        }
        Some(Media::Audio(v)) => {
            require(v.file_id > 0, "invalid audio file")?;
            variants(&v.variants)
        }
        Some(Media::Voice(v)) => require(
            v.file_id > 0
                && v.duration_ms > 0
                && v.waveform.as_ref().is_none_or(|w| w.len() <= 65536),
            "invalid voice",
        ),
        Some(Media::Sticker(v)) => {
            require(
                v.file_id > 0
                    && match (v.asset_id, v.asset_version) {
                        (None, None) => true,
                        (Some(id), Some(version)) => id > 0 && version > 0,
                        _ => false,
                    },
                "invalid sticker exact version",
            )?;
            variants(&v.variants)
        }
        Some(Media::Geo(v)) => require(
            v.lat.is_finite()
                && v.long.is_finite()
                && (-90.0..=90.0).contains(&v.lat)
                && (-180.0..=180.0).contains(&v.long),
            "invalid coordinates",
        ),
        Some(Media::Contact(v)) => require(
            !v.phone_number.is_empty() && !v.first_name.is_empty() && v.user_id >= 0,
            "invalid contact",
        ),
        Some(Media::Poll(v)) => {
            let options: std::collections::HashSet<_> =
                v.answers.iter().map(|a| &a.option).collect();
            require(
                !v.question.trim().is_empty()
                    && (2..=10).contains(&v.answers.len())
                    && options.len() == v.answers.len()
                    && v.answers
                        .iter()
                        .all(|a| !a.option.is_empty() && !a.text.is_empty())
                    && !(v.close_period.is_some() && v.close_date.is_some())
                    && v.close_period.is_none_or(|n| n > 0)
                    && if v.quiz {
                        !v.multiple_choice
                            && v.correct_answers.len() == 1
                            && options.contains(&v.correct_answers[0])
                    } else {
                        v.correct_answers.is_empty()
                    },
                "invalid poll",
            )
        }
        None => Err(TinkleError::Codec("media source required".into())),
    }
}

/// Conditional draft clearing uses only the caller-captured generation/revision, including zero.
pub(crate) fn validate_draft_clear(
    clear: Option<bool>,
    generation: Option<i64>,
    revision: Option<i64>,
) -> Result<(), TinkleError> {
    require(
        if clear == Some(true) {
            generation.is_some_and(|v| v > 0) && revision.is_some_and(|v| v >= 0)
        } else {
            generation.is_none() && revision.is_none()
        },
        "conditional draft clear requires the original generation/revision pair",
    )
}

/// Validate a keyed text send independently of transport admission (also used by durable enqueue).
pub(crate) fn validate_send(req: &p::SendMessageRequest) -> Result<(), TinkleError> {
    validate_draft_clear(
        req.clear_draft,
        req.clear_draft_dialog_id,
        req.clear_draft_revision,
    )?;
    validate_target(req.target.as_ref())?;
    require(
        req.idempotency_key != 0 && !req.message.trim().is_empty(),
        "text and nonzero idempotency_key required",
    )?;
    entities(&req.message, &req.entities)?;
    reply(req.reply_to.as_ref())
}

/// Validate a keyed media send before persistence or dispatch.
pub(crate) fn validate_send_media(req: &p::SendMediaRequest) -> Result<(), TinkleError> {
    validate_draft_clear(
        req.clear_draft,
        req.clear_draft_dialog_id,
        req.clear_draft_revision,
    )?;
    validate_target(req.target.as_ref())?;
    require(req.idempotency_key != 0, "nonzero idempotency_key required")?;
    validate_media(req.media.as_ref())?;
    entities(req.message.as_deref().unwrap_or_default(), &req.entities)?;
    reply(req.reply_to.as_ref())
}

/// Emit concrete typed service methods; each includes its own pre-dispatch validation.
macro_rules! typed_methods {
    ($facade:ident, $service:literal, $( $method:ident($req:ident) -> $resp:ident, $wire:literal, |$r:ident| $validate:block; )*) => {
        impl $facade<'_> { $(
            #[doc = concat!("Validate and invoke `", $service, "/", $wire, "` once; no mutation replay.")]
            pub async fn $method(&self, $r: p::$req) -> Result<p::$resp, TinkleError> {
                $validate
                call_typed(self.client, concat!("/tinkle.v1.", $service, "/", $wire), &$r, 30_000).await
            }
        )* }
    };
}

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
    if method.starts_with("/tinkle.v1.MessagesService/") && req.encoded_len() > 262144 {
        return Err(TinkleError::Codec(
            "Messages request exceeds payload budget".into(),
        ));
    }
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

    /// Read only this caller's exact-version recents. An oversized response is a codec failure,
    /// never a truncated collection or implicit authority to follow current catalog versions.
    pub async fn get_recent_reactions(
        &self,
        req: p::GetRecentReactionsRequest,
    ) -> Result<p::GetRecentReactionsResponse, TinkleError> {
        let response: p::GetRecentReactionsResponse = call_typed(
            self.client,
            tinkle_schema::methods::messagesservice::GET_RECENT_REACTIONS,
            &req,
            30_000,
        )
        .await?;
        require(
            response.reactions.len() <= 32,
            "recent reactions exceed the collection cap",
        )?;
        Ok(response)
    }

    /// Search remotely first; a known local disconnection/network failure may select full cached
    /// messages marked CACHE/CACHED_SUBSET. Continuations never change backend, even
    /// on reconnect. Empty/exhausted cache results never attest complete history.
    pub async fn search(&self, req: p::SearchRequest) -> Result<p::SearchResponse, TinkleError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            crate::cache_search::search(self.client, req).await
        }
        #[cfg(target_arch = "wasm32")]
        {
            call_typed(
                self.client,
                tinkle_schema::methods::messagesservice::SEARCH,
                &req,
                30_000,
            )
            .await
        }
    }

    /// Send a validated keyed text request once. Durable retries use `Client::sending`.
    pub async fn send_message(
        &self,
        req: tinkle_schema::proto::SendMessageRequest,
    ) -> Result<tinkle_schema::proto::SendMessageResponse, TinkleError> {
        validate_send(&req)?;
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
        validate_target(req.target.as_ref())?;
        ids(&req.message_ids, 100, true)?;
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
    pub async fn list_message_changes(
        &self,
        req: tinkle_schema::proto::ListMessageChangesRequest,
    ) -> Result<tinkle_schema::proto::ListMessageChangesResponse, TinkleError> {
        validate_target(req.target.as_ref())?;
        require(req.after_accept_seq >= 0, "negative journal position")?;
        page(req.limit, 500, true)?;
        call_typed(
            self.client,
            tinkle_schema::sync::LIST_MESSAGE_CHANGES_METHOD,
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
        validate_target(req.target.as_ref())?;
        page(req.limit, 100, false)?;
        require(
            !(req.offset_id.is_some() && req.offset_date.is_some()),
            "conflicting history offsets",
        )?;
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
        if let Some(limit) = req.limit {
            page(limit, 100, false)?;
        }
        require(
            !req.cursor.as_ref().is_some_and(|c| {
                !c.is_empty() && (req.offset_date.is_some() || req.offset_peer_id.is_some())
            }),
            "conflicting dialog offsets",
        )?;
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

typed_methods! { Messages, "MessagesService",
    get_scheduled_messages(GetScheduledMessagesRequest) -> GetScheduledMessagesResponse, "GetScheduledMessages", |r| {
        validate_target(r.target.as_ref())?;
        require(r.message_ids.is_empty() != r.idempotency_keys.is_empty(), "select identities or retry keys")?;
        if r.idempotency_keys.is_empty() { distinct_ids(&r.message_ids,100,false)?; }
        else { require(r.idempotency_keys.len()<=100 && r.idempotency_keys.iter().all(|v|*v!=0) && r.idempotency_keys.iter().collect::<std::collections::HashSet<_>>().len()==r.idempotency_keys.len(), "invalid scheduled retry keys")?; }
    };
    cancel_scheduled_messages(CancelScheduledMessagesRequest) -> CancelScheduledMessagesResponse, "CancelScheduledMessages", |r| { validate_target(r.target.as_ref())?; distinct_ids(&r.message_ids,100,false)?; };
    close_poll(ClosePollRequest) -> ClosePollResponse, "ClosePoll", |r| { validate_target(r.target.as_ref())?; require(r.message_id>0,"invalid poll")?; };
    mark_history_read(MarkHistoryReadRequest) -> MarkHistoryReadResponse, "MarkHistoryRead", |r| { validate_target(r.target.as_ref())?; require(r.max_id >= 0, "negative read maximum")?; };
    delete_history(DeleteHistoryRequest) -> DeleteHistoryResponse, "DeleteHistory", |r| {
        validate_target(r.target.as_ref())?;
        require(r.max_id >= 0 && tinkle_schema::sync::session_id(&r.operation_id)
            && r.min_date.is_none_or(|v|v>=0) && r.max_date.is_none_or(|v|v>=0)
            && !matches!((r.min_date,r.max_date),(Some(a),Some(b)) if a>b), "invalid history selection or operation UUID")?;
    };
    delete_messages(DeleteMessagesRequest) -> DeleteMessagesResponse, "DeleteMessages", |r| { validate_target(r.target.as_ref())?; ids(&r.message_ids, 100, false)?; };
    create_chat(CreateChatRequest) -> CreateChatResponse, "CreateChat", |r| { require(!r.title.trim().is_empty() && r.title.len() <= 1024 && r.ttl_period.is_none_or(|v| v >= 0) && r.operation_id>=0 && (!r.ttl_period.is_some_and(|v|v>0) || r.operation_id>0), "invalid chat or effect operation key")?; ids(&r.user_ids, 1000, false)?; };
    edit_chat_title(EditChatTitleRequest) -> EditChatTitleResponse, "EditChatTitle", |r| { require(r.chat_id > 0 && !r.title.trim().is_empty() && r.title.len() <= 1024, "invalid chat title")?; };
    edit_chat_photo(EditChatPhotoRequest) -> EditChatPhotoResponse, "EditChatPhoto", |r| {
        require(r.chat_id > 0, "invalid chat")?;
        match r.photo.as_ref().and_then(|p| p.input.as_ref()) {
            Some(p::chat_photo_source::Input::FileId(id)) => require(*id > 0, "invalid photo file")?,
            Some(p::chat_photo_source::Input::Empty(true)) => {},
            Some(p::chat_photo_source::Input::Photo(p)) => { require(p.file_id > 0, "invalid photo file")?; variants(&p.variants)?; },
            _ => return Err(TinkleError::Codec("photo source required".into())),
        }
    };
    update_chat_description(UpdateChatDescriptionRequest) -> UpdateChatDescriptionResponse, "UpdateChatDescription", |r| { require(r.chat_id > 0 && r.description.len() <= 65536, "invalid chat description")?; };
    get_chats(GetChatsRequest) -> GetChatsResponse, "GetChats", |r| { ids(&r.chat_ids, 100, false)?; };
    get_chat_details(GetChatDetailsRequest) -> GetChatDetailsResponse, "GetChatDetails", |r| { require(r.chat_id > 0, "invalid chat")?; };
    add_chat_participant(AddChatParticipantRequest) -> AddChatParticipantResponse, "AddChatParticipant", |r| { require(r.chat_id > 0 && r.user_id > 0 && r.forward_limit >= 0 && r.operation_id>=0 && (r.forward_limit==0 || r.operation_id>0), "invalid participant or forwarding operation key")?; };
    remove_chat_participant(RemoveChatParticipantRequest) -> RemoveChatParticipantResponse, "RemoveChatParticipant", |r| { require(r.chat_id > 0 && r.user_id > 0, "invalid participant")?; };
    edit_chat_admin(EditChatAdminRequest) -> EditChatAdminResponse, "EditChatAdmin", |r| { require(r.chat_id > 0 && r.user_id > 0, "invalid participant")?; };
    edit_chat_restricted_scopes(EditChatRestrictedScopesRequest) -> EditChatRestrictedScopesResponse, "EditChatRestrictedScopes", |r| { require(r.chat_id > 0 && r.user_id > 0 && r.restricted_scopes.len() <= 18 && r.restricted_scopes.iter().all(|v| p::ChatRestrictedScope::try_from(*v).is_ok_and(|s|s!=p::ChatRestrictedScope::Unspecified)) && r.restricted_scopes.iter().collect::<std::collections::HashSet<_>>().len()==r.restricted_scopes.len(), "invalid or duplicate restricted scopes")?; };
    set_history_ttl(SetHistoryTtlRequest) -> SetHistoryTtlResponse, "SetHistoryTTL", |r| { validate_target(r.target.as_ref())?; require(r.period >= 0, "negative TTL")?; };
    set_message_pin(SetMessagePinRequest) -> SetMessagePinResponse, "SetMessagePin", |r| { validate_target(r.target.as_ref())?; require(r.message_id > 0 && r.pin_scope.is_none_or(|scope| matches!(p::PinScope::try_from(scope), Ok(p::PinScope::Shared | p::PinScope::Personal))), "invalid message or pin scope")?; };
    unpin_all_messages(UnpinAllMessagesRequest) -> UnpinAllMessagesResponse, "UnpinAllMessages", |r| { validate_target(r.target.as_ref())?; require(r.thread_root_id.is_none_or(|id| id > 0), "invalid thread")?; };
    send_media(SendMediaRequest) -> SendMediaResponse, "SendMedia", |r| { validate_send_media(&r)?; };
    send_album(SendAlbumRequest) -> SendAlbumResponse, "SendAlbum", |r| {
        validate_draft_clear(r.clear_draft,r.clear_draft_dialog_id,r.clear_draft_revision)?;
        validate_target(r.target.as_ref())?; reply(r.reply_to.as_ref())?;
        require((2..=10).contains(&r.items.len()), "album requires 2..10 items")?;
        let mut keys = std::collections::HashSet::new();
        for item in &r.items {
            require(item.idempotency_key != 0 && keys.insert(item.idempotency_key), "album keys must be nonzero and distinct")?;
            validate_media(item.media.as_ref())?;
            require(matches!(item.media.as_ref().and_then(|m| m.media.as_ref()), Some(p::media_source::Media::Photo(_) | p::media_source::Media::Video(_) | p::media_source::Media::Document(_))), "invalid album media kind")?;
            entities(&item.message, &item.entities)?;
        }
    };
    edit_message(EditMessageRequest) -> EditMessageResponse, "EditMessage", |r| {
        validate_target(r.target.as_ref())?; require(r.message_id > 0 && (r.message.is_some() || r.media.is_some() || r.schedule_date.is_some() || r.suppress_link_preview.is_some()), "empty/invalid edit")?;
        entities(r.message.as_deref().unwrap_or_default(), &r.entities)?;
        if r.media.is_some() { validate_media(r.media.as_ref())?; }
    };
    set_typing(SetTypingRequest) -> SetTypingResponse, "SetTyping", |r| { validate_target(r.target.as_ref())?; require((1..=10).contains(&r.action) && r.thread_root_id.is_none_or(|id| id > 0), "invalid typing action/thread")?; };
    start_bot(StartBotRequest) -> StartBotResponse, "StartBot", |r| { validate_target(r.target.as_ref())?; validate_target(r.bot.as_ref())?; require(r.idempotency_key != 0 && r.start_param.len() <= 4096, "invalid bot start")?; };
    batch_get_message_reactions(BatchGetMessageReactionsRequest) -> BatchGetMessageReactionsResponse, "BatchGetMessageReactions", |r| { validate_target(r.target.as_ref())?; ids(&r.message_ids, 100, false)?; };
    list_message_reactions(ListMessageReactionsRequest) -> ListMessageReactionsResponse, "ListMessageReactions", |r| { validate_target(r.target.as_ref())?; require(r.message_id > 0, "invalid message")?; page(r.limit, 100, false)?; };
    send_reaction(SendReactionRequest) -> SendReactionResponse, "SendReaction", |r| { validate_target(r.target.as_ref())?;
        require(if r.animate==Some(true) || r.save_to_recents==Some(true) {tinkle_schema::sync::session_id(&r.operation_id)} else {r.operation_id.is_empty() || tinkle_schema::sync::session_id(&r.operation_id)},"reaction effects require a retained operation UUID")?;
        require(r.message_id > 0 && r.reactions.len() <= 100 && r.reactions.iter().all(|v| v.custom_emoji_id >= 0 && !v.emoticon.is_empty() && v.emoticon.len() <= 64 && if v.custom_emoji_id > 0 {v.asset_version.is_some_and(|n|n>0)}else{v.asset_version.is_none()}), "invalid reactions")?; };
    get_unread_mentions(GetUnreadMentionsRequest) -> GetUnreadMentionsResponse, "GetUnreadMentions", |r| { validate_target(r.target.as_ref())?; page(r.limit, 100, false)?; require(r.offset_id >= 0 && r.thread_root_id.is_none_or(|v| v > 0), "invalid mention offset/thread")?; };
    read_mentions(ReadMentionsRequest) -> ReadMentionsResponse, "ReadMentions", |r| { validate_target(r.target.as_ref())?; require(r.thread_root_id.is_none_or(|v| v > 0), "invalid thread")?; };
    get_unread_reactions(GetUnreadReactionsRequest) -> GetUnreadReactionsResponse, "GetUnreadReactions", |r| { validate_target(r.target.as_ref())?; page(r.limit, 100, false)?; require(r.offset_id >= 0 && r.thread_root_id.is_none_or(|v| v > 0), "invalid reaction offset/thread")?; };
    read_reactions(ReadReactionsRequest) -> ReadReactionsResponse, "ReadReactions", |r| { validate_target(r.target.as_ref())?; require(r.thread_root_id.is_none_or(|v| v > 0), "invalid thread")?; };
    send_vote(SendVoteRequest) -> SendVoteResponse, "SendVote", |r| { validate_target(r.target.as_ref())?; require(r.message_id > 0 && r.options.len() <= 10 && r.options.iter().all(|o| !o.is_empty() && o.len() <= 1024) && r.options.iter().collect::<std::collections::HashSet<_>>().len() == r.options.len(), "invalid vote options")?; };
    get_poll_votes(GetPollVotesRequest) -> GetPollVotesResponse, "GetPollVotes", |r| { validate_target(r.target.as_ref())?; require(r.message_id > 0 && r.option.as_ref().is_none_or(|v| !v.is_empty()), "invalid poll/option")?; page(r.limit, 100, false)?; };
    get_poll_results(GetPollResultsRequest) -> GetPollResultsResponse, "GetPollResults", |r| { validate_target(r.target.as_ref())?; require(r.message_id > 0, "invalid poll")?; };
    read_thread(ReadThreadRequest) -> ReadThreadResponse, "ReadThread", |r| { validate_target(r.target.as_ref())?; require(r.thread_root_id > 0 && r.read_max_id >= 0, "invalid thread read")?; };
    list_thread_messages(ListThreadMessagesRequest) -> ListThreadMessagesResponse, "ListThreadMessages", |r| { validate_target(r.target.as_ref())?; require(r.message_id > 0 && !(r.offset_id.is_some() && r.offset_date.is_some()), "invalid thread/offset")?; page(r.limit, 100, false)?; };
}

/// Privacy-filtered user/profile reads on the current authenticated channel.
pub struct Users<'a> {
    client: &'a Client,
}
impl<'a> Users<'a> {
    /// Bind reads without retaining a second transport or caching an unversioned profile.
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }
}
typed_methods! { Users, "UsersService",
    get_users(GetUsersRequest) -> GetUsersResponse, "GetUsers", |r| {
        require(!r.users.is_empty() && r.users.len()<=100 && r.users.iter().all(|u|u.user_id>0), "invalid user batch")?;
    };
    get_user_complete(GetUserCompleteRequest) -> GetUserCompleteResponse, "GetUserComplete", |r| { require(r.user_id>0,"positive user required")?; };
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

    /// List active devices with neutral session IDs and descriptive metadata.
    pub async fn get_sessions(&self) -> Result<Vec<tinkle_schema::proto::Session>, TinkleError> {
        let response: tinkle_schema::proto::GetSessionsResponse = call_typed(
            self.client,
            tinkle_schema::methods::accountservice::GET_SESSIONS,
            &tinkle_schema::proto::GetSessionsRequest {},
            3_000,
        )
        .await?;
        Ok(response.sessions)
    }

    /// Confirm one other login once. The returned commit-point projection never
    /// replaces a newer full snapshot; review refresh remains SDK-owned.
    pub async fn confirm_session(
        &self,
        session_id: &str,
    ) -> Result<tinkle_schema::proto::Session, TinkleError> {
        if !tinkle_schema::sync::session_id(session_id) {
            return Err(TinkleError::Codec("invalid session identifier".into()));
        }
        let generation = self.client.session_generation()?;
        let caller = self.client.for_session_generation(generation.clone());
        // Invalidate an in-flight read before dispatch, including ambiguous outcomes.
        self.client.session_state.dirty(Some(generation.clone()));
        let response: Result<tinkle_schema::proto::ConfirmSessionResponse, TinkleError> =
            call_typed(
                &caller,
                tinkle_schema::methods::accountservice::CONFIRM_SESSION,
                &tinkle_schema::proto::ConfirmSessionRequest {
                    session_id: session_id.into(),
                },
                3_000,
            )
            .await;
        self.client.session_state.dirty(Some(generation.clone()));
        let session = response?
            .session
            .ok_or_else(|| TinkleError::Codec("missing confirmed session".into()))?;
        if session.id != session_id || session.current || session.unconfirmed {
            return Err(TinkleError::Codec("invalid confirmed session".into()));
        }
        // Transport retirement cannot undo a confirmed mutation; local sign-out remains terminal.
        if self.client.is_signed_out() {
            return Err(TinkleError::SignedOut);
        }
        Ok(session)
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
        .await?;
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
        .await?;
        Ok(response.revoked)
    }
}

typed_methods! { Account, "AccountService",
    update_profile(UpdateProfileRequest) -> UpdateProfileResponse, "UpdateProfile", |r| { require(r.name.as_ref().is_none_or(|v| !v.trim().is_empty() && v.chars().count() <= 100) && r.about.as_ref().is_none_or(|v| v.chars().count() <= 500), "invalid profile")?; };
    update_username(UpdateUsernameRequest) -> UpdateUsernameResponse, "UpdateUsername", |r| { require(r.username.len() <= 64, "invalid username")?; };
    validate_username(ValidateUsernameRequest) -> ValidateUsernameResponse, "ValidateUsername", |r| { require(!r.username.is_empty() && r.username.len() <= 64, "invalid username")?; };
    set_profile_photo(SetProfilePhotoRequest) -> SetProfilePhotoResponse, "SetProfilePhoto", |r| {
        require(r.expected_revision > 0, "gallery revision required")?;
        let source = r.source.as_ref().ok_or_else(|| TinkleError::Codec("profile photo source required".into()))?;
        require(source.file_id > 0, "invalid photo file")?;
        let m = source.metadata.as_ref().ok_or_else(|| TinkleError::Codec("photo metadata required".into()))?;
        require(m.width > 0 && m.height > 0 && m.width <= 4096 && m.height <= 4096 && !m.animated && (1..=2).contains(&m.format), "invalid photo metadata")?;
        validation(m.validation.as_ref())?; variants(&source.variants)?;
        require(source.variants.len() <= 1 && source.variants.iter().all(|v| v.role==1 && v.file_id!=source.file_id && v.width.is_some_and(|n|(1..=160).contains(&n)) && v.height.is_some_and(|n|(1..=160).contains(&n))), "invalid profile thumbnail")?;
    };
    get_profile_photos(GetProfilePhotosRequest) -> GetProfilePhotosResponse, "GetProfilePhotos", |r| { require(r.user_id.is_none_or(|v| v > 0), "invalid profile user")?; };
    select_profile_photo(SelectProfilePhotoRequest) -> SelectProfilePhotoResponse, "SelectProfilePhoto", |r| { require(r.photo_id > 0 && r.expected_revision > 0, "gallery photo/revision required")?; };
    delete_profile_photo(DeleteProfilePhotoRequest) -> DeleteProfilePhotoResponse, "DeleteProfilePhoto", |r| { require(r.photo_id > 0 && r.expected_revision > 0, "gallery photo/revision required")?; };
}

/// Sticker and custom-emoji catalog and owner authoring on the authenticated channel.
pub struct Stickers<'a> {
    client: &'a Client,
}
impl<'a> Stickers<'a> {
    /// Bind catalog operations to the same principal and transport as Messages.
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }
}

/// Require the exact authoring validation declaration, never silently downgrade profiles.
fn validation(v: Option<&p::MediaValidation>) -> Result<(), TinkleError> {
    require(
        v.is_some_and(|v| v.profile == 1 && v.version == 1),
        "STRICT_LOSSLESS version 1 required",
    )
}

/// Validate authored metadata without claiming a client assertion proves file validity/ownership.
fn asset(source: Option<&p::StickerAssetSource>, kind: i32) -> Result<(), TinkleError> {
    let s = source.ok_or_else(|| TinkleError::Codec("asset source required".into()))?;
    let m = s
        .metadata
        .as_ref()
        .ok_or_else(|| TinkleError::Codec("sticker metadata required".into()))?;
    let max = if kind == 2 { 128 } else { 512 };
    require(
        s.file_id > 0
            && m.width > 0
            && m.height > 0
            && m.width <= max
            && m.height <= max
            && matches!(m.format, 1 | 4)
            && (!m.animated || m.format == 1)
            && if m.animated {
                m.duration_ms.is_some_and(|v| (1..=3000).contains(&v))
                    && m.frame_count.is_some_and(|v| (2..=180).contains(&v))
            } else {
                m.duration_ms.is_none() && m.frame_count.is_none()
            },
        "invalid sticker metadata",
    )?;
    validation(m.validation.as_ref())?;
    variants(&s.variants)?;
    require(
        s.variants.len() <= 1
            && s.variants.iter().all(|v| {
                v.role == 1
                    && v.file_id != s.file_id
                    && v.width.is_some_and(|n| (1..=128).contains(&n))
                    && v.height.is_some_and(|n| (1..=128).contains(&n))
            }),
        "invalid asset thumbnail",
    )
}

/// Short names are ASCII identifiers, case-insensitive on the server.
fn pack_name(name: &str) -> Result<(), TinkleError> {
    require(
        (5..=32).contains(&name.len())
            && name.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
            && name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'),
        "invalid pack short name",
    )
}

typed_methods! { Stickers, "StickersService",
    get_installed_packs(GetInstalledPacksRequest) -> GetInstalledPacksResponse, "GetInstalledPacks", |r| { require((0..=2).contains(&r.kind), "invalid pack kind")?; };
    install_pack(InstallPackRequest) -> InstallPackResponse, "InstallPack", |r| { require(r.pack_id > 0, "invalid pack")?; };
    uninstall_pack(UninstallPackRequest) -> UninstallPackResponse, "UninstallPack", |r| { require(r.pack_id > 0, "invalid pack")?; };
    reorder_packs(ReorderPacksRequest) -> ReorderPacksResponse, "ReorderPacks", |r| { distinct_ids(&r.pack_ids, 1000, true)?; };
    get_pack(GetPackRequest) -> GetPackResponse, "GetPack", |r| { require(r.pack_id > 0, "invalid pack")?; };
    get_pack_by_short_name(GetPackByShortNameRequest) -> GetPackByShortNameResponse, "GetPackByShortName", |r| { pack_name(&r.short_name)?; };
    search_packs(SearchPacksRequest) -> SearchPacksResponse, "SearchPacks", |r| { require(!r.query.trim().is_empty() && r.query.len() <= 4096 && r.offset >= 0 && (0..=2).contains(&r.kind), "invalid pack search")?; page(r.limit, 50, true)?; };
    get_archived_packs(GetArchivedPacksRequest) -> GetArchivedPacksResponse, "GetArchivedPacks", |r| { require(r.offset_id >= 0 && (0..=2).contains(&r.kind), "invalid archived packs request")?; page(r.limit, 50, true)?; };
    get_recent_stickers(GetRecentStickersRequest) -> GetRecentStickersResponse, "GetRecentStickers", |_r| {};
    save_recent_sticker(SaveRecentStickerRequest) -> SaveRecentStickerResponse, "SaveRecentSticker", |r| { require(r.sticker_id > 0, "invalid sticker")?; };
    clear_recent_stickers(ClearRecentStickersRequest) -> ClearRecentStickersResponse, "ClearRecentStickers", |_r| {};
    get_favorite_stickers(GetFavoriteStickersRequest) -> GetFavoriteStickersResponse, "GetFavoriteStickers", |_r| {};
    toggle_favorite_sticker(ToggleFavoriteStickerRequest) -> ToggleFavoriteStickerResponse, "ToggleFavoriteSticker", |r| { require(r.sticker_id > 0, "invalid sticker")?; };
    create_sticker_asset(CreateStickerAssetRequest) -> CreateStickerAssetResponse, "CreateStickerAsset", |r| { require((1..=2).contains(&r.kind) && if r.kind == 2 { !r.fallback_text.is_empty() && r.fallback_text.len() <= 64 } else { r.fallback_text.is_empty() }, "invalid asset kind/fallback")?; asset(r.source.as_ref(), r.kind)?; };
    update_sticker_asset(UpdateStickerAssetRequest) -> UpdateStickerAssetResponse, "UpdateStickerAsset", |r| { require(r.asset_id > 0 && r.expected_version > 0 && (r.source.is_some() || r.fallback_text.is_some()) && r.fallback_text.as_ref().is_none_or(|v| v.len() <= 64), "invalid asset edit")?; if r.source.is_some() { asset(r.source.as_ref(), 0)?; } };
    delete_sticker_asset(DeleteStickerAssetRequest) -> DeleteStickerAssetResponse, "DeleteStickerAsset", |r| { require(r.asset_id > 0 && r.expected_version > 0, "invalid asset version")?; };
    get_sticker_assets(GetStickerAssetsRequest) -> GetStickerAssetsResponse, "GetStickerAssets", |r| {
        require((0..=2).contains(&r.kind) && r.asset_ids.is_empty()!=r.references.is_empty(), "choose IDs or exact references")?;
        if !r.asset_ids.is_empty() {distinct_ids(&r.asset_ids,100,false)?;require(r.message_reference.is_none(),"message context requires exact references")?;}
        else {
            require(r.references.len()<=100 && r.references.iter().all(|v|v.asset_id>0&&v.asset_version>0) && r.references.iter().map(|v|(v.asset_id,v.asset_version)).collect::<std::collections::HashSet<_>>().len()==r.references.len(),"invalid exact asset references")?;
            if let Some(m)=&r.message_reference {validate_target(m.target.as_ref())?;require(m.message_id>0,"invalid message reference")?;}
        }
    };
    get_owned_packs(GetOwnedPacksRequest) -> GetOwnedPacksResponse, "GetOwnedPacks", |r| { require((0..=2).contains(&r.kind), "invalid pack kind")?; page(r.limit, 50, true)?; };
    create_pack(CreatePackRequest) -> CreatePackResponse, "CreatePack", |r| { require((1..=2).contains(&r.kind) && (1..=100).contains(&r.name.chars().count()), "invalid pack kind/name")?; pack_name(&r.short_name)?; };
    update_pack(UpdatePackRequest) -> UpdatePackResponse, "UpdatePack", |r| { require(r.pack_id > 0 && r.expected_revision > 0 && (r.name.is_some() || r.short_name.is_some()) && r.name.as_ref().is_none_or(|v| (1..=100).contains(&v.chars().count())), "invalid pack edit")?; if let Some(n) = &r.short_name { pack_name(n)?; } };
    delete_pack(DeletePackRequest) -> DeletePackResponse, "DeletePack", |r| { require(r.pack_id > 0 && r.expected_revision > 0, "invalid pack revision")?; };
    add_pack_asset(AddPackAssetRequest) -> AddPackAssetResponse, "AddPackAsset", |r| { require(r.pack_id > 0 && r.asset_id > 0 && r.asset_version > 0 && r.expected_revision > 0, "invalid pack/asset version")?; };
    remove_pack_asset(RemovePackAssetRequest) -> RemovePackAssetResponse, "RemovePackAsset", |r| { require(r.pack_id > 0 && r.asset_id > 0 && r.expected_revision > 0, "invalid pack revision/asset")?; };
    reorder_pack_assets(ReorderPackAssetsRequest) -> ReorderPackAssetsResponse, "ReorderPackAssets", |r| { require(r.pack_id > 0 && r.expected_revision > 0, "invalid pack revision")?; distinct_ids(&r.asset_ids, 120, true)?; };
    publish_pack(PublishPackRequest) -> PublishPackResponse, "PublishPack", |r| { require(r.pack_id > 0 && r.expected_revision > 0, "invalid pack revision")?; };
    unpublish_pack(UnpublishPackRequest) -> UnpublishPackResponse, "UnpublishPack", |r| { require(r.pack_id > 0 && r.expected_revision > 0, "invalid pack revision")?; };
}

/// Auth service.
pub struct Auth<'a> {
    client: &'a Client,
}

impl<'a> Auth<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Request a verification challenge, rejecting a missing identifier and sanitizing auth errors.
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

    /// Deliver enrollment on pinned Bootstrap and await validated candidate persistence.
    /// Reject an account projection that disagrees with the delivered enrollment user.
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
        file_context(req.file_access_context.as_ref())?;
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
        file_context(req.file_access_context.as_ref())?;
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
/// retired; reconciliation runs over `MessagesService/ListMessageChanges` +
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
