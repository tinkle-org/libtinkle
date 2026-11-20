//! Current owner-specific representation reads, independent of accepted action receipts.
use crate::Client;
use prost::Message;
use tinkle_schema::proto as p;

impl Client {
    /// Fetch a complete current representation projection under its exact original owner/version.
    /// Returns a typed protobuf Any (ChatPhoto, ProfilePhoto or StickerView), never a file grant.
    /// Message enrichment uses the existing GetMessages/journal path instead.
    pub async fn refresh_media_representation(
        &self,
        reference: p::FileAccessContext,
        main_file_id: i64,
    ) -> Result<prost_types::Any, crate::SendError> {
        if main_file_id <= 0 {
            return Err(crate::SendError::InvalidRequest);
        }
        let scope = crate::sending::Scope::capture(self).await?;
        let caller = scope.caller();
        use p::file_access_context::Reference as R;
        let (name, value) = match reference.reference.as_ref() {
            Some(R::ChatPhoto(r)) if r.chat_id > 0 && r.photo_id > 0 => {
                let chat = current_photo_room(&caller, r.chat_id).await?;
                let photo = chat
                    .photo
                    .as_ref()
                    .filter(|p| {
                        p.photo_id == r.photo_id
                            && p.file_id == main_file_id
                            && p.representation_revision.is_some_and(|r| r > 0)
                    })
                    .ok_or(crate::SendError::NotFound)?;
                tinkle_schema::representations::validate_thumbnail(
                    photo.file_id,
                    photo.thumbnail.as_ref(),
                )
                .map_err(|_| crate::SendError::InvalidRequest)?;
                let value = photo.encode_to_vec();
                let ops = crate::cache_projector::project_entities_to_ops(&[], &[chat]);
                scope
                    .store(move |s, p, _| s.hydrate_entity_snapshot(p, &ops))
                    .await?;
                ("ChatPhoto", value)
            }
            Some(R::ProfilePhoto(r)) if r.user_id > 0 && r.photo_id > 0 => {
                let response = caller
                    .account()
                    .get_profile_photos(p::GetProfilePhotosRequest {
                        user_id: Some(r.user_id),
                    })
                    .await?;
                if response.photos.len() > 10
                    || response
                        .photos
                        .iter()
                        .map(|v| v.photo_id)
                        .collect::<std::collections::HashSet<_>>()
                        .len()
                        != response.photos.len()
                    || response
                        .photos
                        .iter()
                        .any(|v| v.user_id != r.user_id || v.photo_id <= 0 || v.file_id <= 0)
                {
                    return Err(crate::SendError::InvalidRequest);
                }
                let photo = response
                    .photos
                    .into_iter()
                    .find(|p| {
                        p.user_id == r.user_id
                            && p.photo_id == r.photo_id
                            && p.file_id == main_file_id
                            && p.representation_revision.is_some_and(|r| r > 0)
                    })
                    .ok_or(crate::SendError::NotFound)?;
                tinkle_schema::representations::validate_thumbnail(
                    photo.file_id,
                    photo.thumbnail.as_ref(),
                )
                .map_err(|_| crate::SendError::InvalidRequest)?;
                let value = photo.encode_to_vec();
                if response.selected_photo_id == Some(photo.photo_id) {
                    let photo = photo.clone();
                    scope
                        .store(move |s, principal, _| {
                            if let Some(row) = s.get_user(principal, photo.user_id)? {
                                let mut user =
                                    p::User::decode(row.proto.as_slice()).map_err(|_| {
                                        tinkle_storage::StorageError::Corrupt(
                                            "invalid cached user".into(),
                                        )
                                    })?;
                                if let Some(p::user::UserType::Data(data)) = user.user_type.as_mut()
                                {
                                    if let Some(current) = data.photo.as_mut().filter(|v| {
                                        v.photo_id == photo.photo_id
                                            && v.file_id == photo.file_id
                                            && v.inline_preview == photo.inline_preview
                                    }) {
                                        current.thumbnail = photo.thumbnail;
                                        current.representation_revision =
                                            photo.representation_revision;
                                        let ops = crate::cache_projector::project_entities_to_ops(
                                            &[user],
                                            &[],
                                        );
                                        s.hydrate_entity_snapshot(principal, &ops)?;
                                    }
                                }
                            }
                            Ok(())
                        })
                        .await?;
                }
                ("ProfilePhoto", value)
            }
            Some(R::StickerAsset(r)) if r.asset_id > 0 && r.asset_version > 0 => {
                let response = caller
                    .stickers()
                    .get_sticker_assets(p::GetStickerAssetsRequest {
                        references: vec![p::AssetReference {
                            asset_id: r.asset_id,
                            asset_version: r.asset_version,
                        }],
                        ..Default::default()
                    })
                    .await?;
                if response.assets.len() > 1 {
                    return Err(crate::SendError::InvalidRequest);
                }
                let asset = response
                    .assets
                    .into_iter()
                    .find(|v| {
                        v.id == r.asset_id
                            && v.asset_version == r.asset_version
                            && v.file_id == main_file_id
                            && v.representation_revision.is_some_and(|r| r > 0)
                    })
                    .ok_or(crate::SendError::NotFound)?;
                tinkle_schema::representations::validate_thumbnail(
                    asset.file_id,
                    asset.thumbnail.as_ref(),
                )
                .map_err(|_| crate::SendError::InvalidRequest)?;
                ("StickerView", asset.encode_to_vec())
            }
            _ => return Err(crate::SendError::InvalidRequest),
        };
        scope.check()?;
        Ok(prost_types::Any {
            type_url: format!("type.googleapis.com/tinkle.v1.{name}"),
            value,
        })
    }
}

/// A kindless reference first queries the channel owner; only an authoritative empty result
/// permits the group query. Errors, malformed projections and photo replacements never fall back.
/// Both reads share the original lookup budget (including its configured maximum) and room ID.
async fn current_photo_room(caller: &Client, id: i64) -> Result<p::Chat, crate::SendError> {
    let budget = caller.call_budget(30_000)?;
    let bytes = caller
        .call_raw(
            tinkle_schema::methods::channelsservice::GET_CHANNELS,
            p::GetChannelsRequest {
                channel_ids: vec![id],
            }
            .encode_to_vec(),
            Client::remaining_timeout_ms(&budget)?,
        )
        .await?;
    let response = p::GetChannelsResponse::decode(bytes.as_slice())
        .map_err(|_| crate::SendError::InvalidRequest)?;
    if let Some(room) = select_photo_room(response.chats, id, true)? {
        return Ok(room);
    }
    let bytes = caller
        .call_raw(
            tinkle_schema::methods::messagesservice::GET_CHATS,
            p::GetChatsRequest { chat_ids: vec![id] }.encode_to_vec(),
            Client::remaining_timeout_ms(&budget)?,
        )
        .await?;
    let response = p::GetChatsResponse::decode(bytes.as_slice())
        .map_err(|_| crate::SendError::InvalidRequest)?;
    select_photo_room(response.chats, id, false)?.ok_or(crate::SendError::NotFound)
}

/// Missing results alone are omission evidence; unexpected IDs/kinds or duplicate rows are errors.
fn select_photo_room(
    mut rooms: Vec<p::Chat>,
    id: i64,
    channel: bool,
) -> Result<Option<p::Chat>, crate::SendError> {
    if rooms.is_empty() {
        return Ok(None);
    }
    if rooms.len() != 1 {
        return Err(crate::SendError::InvalidRequest);
    }
    let room = rooms.pop().ok_or(crate::SendError::InvalidRequest)?;
    let kind = if channel {
        matches!(room.chat_type, 2 | 3)
    } else {
        room.chat_type == p::ChatType::Group as i32
    };
    if room.chat_id != id || !kind {
        return Err(crate::SendError::InvalidRequest);
    }
    Ok(Some(room))
}
