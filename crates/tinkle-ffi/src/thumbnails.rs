//! Owned singular-thumbnail records shared by every candidate media purpose.
use crate::FfiSendError;
#[cfg(tinkle_async_actions)]
use prost::Message;

/// One optional completed upload input. Absence is `None`, never zero dimensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Record)]
pub struct FfiThumbnailSource {
    pub file_id: i64,
    pub width: u32,
    pub height: u32,
}

/// One server-measured thumbnail output, separate from inline JPEG display bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Record)]
pub struct FfiThumbnail {
    pub file_id: i64,
    pub width: u32,
    pub height: u32,
}

/// The ordinary uploaded FileInfo and the exact same file's declared thumbnail dimensions.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiUploadedThumbnail {
    pub file_info: Vec<u8>,
    pub source: FfiThumbnailSource,
}

/// Validate purpose dimensions before constructing a source; never infer upload identity.
#[cfg(tinkle_async_actions)]
fn source(
    main: i64,
    value: Option<FfiThumbnailSource>,
    edge: u32,
) -> Result<Option<tinkle_schema::proto::ThumbnailSource>, FfiSendError> {
    if main <= 0
        || value.is_some_and(|v| {
            v.file_id <= 0
                || v.file_id == main
                || !(1..=edge).contains(&v.width)
                || !(1..=edge).contains(&v.height)
        })
    {
        return Err(FfiSendError::InvalidRequest);
    }
    Ok(value.map(|v| tinkle_schema::proto::ThumbnailSource {
        file_id: v.file_id,
        width: v.width,
        height: v.height,
    }))
}

/// Explicit source assembly only; never edits a retained outgoing request or its keys.
#[uniffi::export]
pub fn media_source_with_thumbnail(
    bytes: Vec<u8>,
    thumbnail: Option<FfiThumbnailSource>,
) -> Result<Vec<u8>, FfiSendError> {
    #[cfg(tinkle_async_actions)]
    {
        use tinkle_schema::proto as p;
        if bytes.len() > 262144 {
            return Err(FfiSendError::InvalidRequest);
        }
        tinkle_schema::wire::validate_message(".tinkle.v1.MediaSource", &bytes)
            .map_err(|_| FfiSendError::InvalidRequest)?;
        let mut media =
            p::MediaSource::decode(bytes.as_slice()).map_err(|_| FfiSendError::InvalidRequest)?;
        match media.media.as_mut() {
            Some(p::media_source::Media::Photo(v)) => {
                v.thumbnail = source(v.file_id, thumbnail, 320)?
            }
            Some(p::media_source::Media::Document(v)) => {
                v.thumbnail = source(v.file_id, thumbnail, 320)?
            }
            Some(p::media_source::Media::Video(v)) => {
                v.thumbnail = source(v.file_id, thumbnail, 320)?
            }
            Some(p::media_source::Media::Audio(v)) => {
                v.thumbnail = source(v.file_id, thumbnail, 320)?
            }
            Some(p::media_source::Media::Sticker(v)) => {
                v.thumbnail = source(v.file_id, thumbnail, 128)?
            }
            _ => return Err(FfiSendError::InvalidRequest),
        }
        Ok(media.encode_to_vec())
    }
    #[cfg(not(tinkle_async_actions))]
    {
        let _ = (bytes, thumbnail);
        Err(FfiSendError::Media {
            error: crate::FfiMediaError::Unsupported,
        })
    }
}

macro_rules! assemble {
    ($name:ident, $ty:ident, $edge:literal) => {
        /// Explicitly assemble this purpose's source with one optional completed thumbnail input.
        #[uniffi::export]
        pub fn $name(
            bytes: Vec<u8>,
            thumbnail: Option<FfiThumbnailSource>,
        ) -> Result<Vec<u8>, FfiSendError> {
            #[cfg(tinkle_async_actions)]
            {
                if bytes.len() > 262144 {
                    return Err(FfiSendError::InvalidRequest);
                }
                tinkle_schema::wire::validate_message(
                    concat!(".tinkle.v1.", stringify!($ty)),
                    &bytes,
                )
                .map_err(|_| FfiSendError::InvalidRequest)?;
                let mut value = tinkle_schema::proto::$ty::decode(bytes.as_slice())
                    .map_err(|_| FfiSendError::InvalidRequest)?;
                value.thumbnail = source(value.file_id, thumbnail, $edge)?;
                Ok(value.encode_to_vec())
            }
            #[cfg(not(tinkle_async_actions))]
            {
                let _ = (bytes, thumbnail);
                Err(FfiSendError::Media {
                    error: crate::FfiMediaError::Unsupported,
                })
            }
        }
    };
}
assemble!(profile_photo_source_with_thumbnail, ProfilePhotoSource, 160);
assemble!(sticker_asset_source_with_thumbnail, StickerAssetSource, 128);

/// Explicit group/channel source assembly applies the avatar bound, not the message-photo bound.
#[uniffi::export]
pub fn chat_photo_source_with_thumbnail(
    bytes: Vec<u8>,
    thumbnail: Option<FfiThumbnailSource>,
) -> Result<Vec<u8>, FfiSendError> {
    #[cfg(tinkle_async_actions)]
    {
        use tinkle_schema::proto as p;
        tinkle_schema::wire::validate_message(".tinkle.v1.ChatPhotoSource", &bytes)
            .map_err(|_| FfiSendError::InvalidRequest)?;
        let mut value = p::ChatPhotoSource::decode(bytes.as_slice())
            .map_err(|_| FfiSendError::InvalidRequest)?;
        let Some(p::chat_photo_source::Input::Photo(photo)) = value.input.as_mut() else {
            return Err(FfiSendError::InvalidRequest);
        };
        photo.thumbnail = source(photo.file_id, thumbnail, 160)?;
        Ok(value.encode_to_vec())
    }
    #[cfg(not(tinkle_async_actions))]
    {
        let _ = (bytes, thumbnail);
        Err(FfiSendError::Media {
            error: crate::FfiMediaError::Unsupported,
        })
    }
}

/// Decode the same shared output shape for photo, document, avatar and asset projections.
#[uniffi::export]
pub fn thumbnail_from_protobuf(bytes: Vec<u8>) -> Result<FfiThumbnail, FfiSendError> {
    #[cfg(tinkle_async_actions)]
    {
        if bytes.len() > 128 {
            return Err(FfiSendError::InvalidRequest);
        }
        tinkle_schema::wire::validate_message(".tinkle.v1.Thumbnail", &bytes)
            .map_err(|_| FfiSendError::InvalidRequest)?;
        let v = tinkle_schema::proto::Thumbnail::decode(bytes.as_slice())
            .map_err(|_| FfiSendError::InvalidRequest)?;
        if v.file_id <= 0 || v.width == 0 || v.height == 0 {
            return Err(FfiSendError::InvalidRequest);
        }
        Ok(FfiThumbnail {
            file_id: v.file_id,
            width: v.width,
            height: v.height,
        })
    }
    #[cfg(not(tinkle_async_actions))]
    {
        let _ = bytes;
        Err(FfiSendError::Media {
            error: crate::FfiMediaError::Unsupported,
        })
    }
}

#[cfg(all(test, tinkle_async_actions))]
mod tests {
    use super::*;
    use tinkle_schema::proto as p;

    /// Every source purpose uses the same singular record; voice never gains a thumbnail.
    #[test]
    fn all_source_purposes_keep_absence_and_positive_dimensions() {
        let thumbnail = FfiThumbnailSource {
            file_id: 10,
            width: 1,
            height: 1,
        };
        for media in [
            p::media_source::Media::Photo(p::PhotoSource {
                file_id: 9,
                ..Default::default()
            }),
            p::media_source::Media::Document(p::DocumentSource {
                file_id: 9,
                ..Default::default()
            }),
            p::media_source::Media::Video(p::VideoSource {
                file_id: 9,
                ..Default::default()
            }),
            p::media_source::Media::Audio(p::AudioSource {
                file_id: 9,
                ..Default::default()
            }),
            p::media_source::Media::Sticker(p::StickerSource {
                file_id: 9,
                ..Default::default()
            }),
        ] {
            let bytes = p::MediaSource { media: Some(media) }.encode_to_vec();
            assert_eq!(
                media_source_with_thumbnail(bytes.clone(), None).unwrap(),
                bytes
            );
            assert!(media_source_with_thumbnail(bytes.clone(), Some(thumbnail)).is_ok());
            for (file_id, width, height) in [(9, 1, 1), (10, 0, 1), (10, 1, 0)] {
                let invalid = FfiThumbnailSource {
                    file_id,
                    width,
                    height,
                };
                assert!(media_source_with_thumbnail(bytes.clone(), Some(invalid)).is_err());
            }
        }
        assert!(profile_photo_source_with_thumbnail(
            p::ProfilePhotoSource {
                file_id: 9,
                ..Default::default()
            }
            .encode_to_vec(),
            Some(thumbnail)
        )
        .is_ok());
        assert!(sticker_asset_source_with_thumbnail(
            p::StickerAssetSource {
                file_id: 9,
                ..Default::default()
            }
            .encode_to_vec(),
            Some(thumbnail)
        )
        .is_ok());
        assert!(media_source_with_thumbnail(
            p::MediaSource {
                media: Some(p::media_source::Media::Voice(p::VoiceSource {
                    file_id: 9,
                    ..Default::default()
                }))
            }
            .encode_to_vec(),
            Some(thumbnail)
        )
        .is_err());
        assert!(thumbnail_from_protobuf(p::Thumbnail::default().encode_to_vec()).is_err());
        let chat = p::ChatPhotoSource {
            input: Some(p::chat_photo_source::Input::Photo(p::PhotoSource {
                file_id: 9,
                ..Default::default()
            })),
        }
        .encode_to_vec();
        assert!(chat_photo_source_with_thumbnail(chat.clone(), Some(thumbnail)).is_ok());
        assert!(chat_photo_source_with_thumbnail(
            chat,
            Some(FfiThumbnailSource {
                width: 161,
                ..thumbnail
            })
        )
        .is_err());
    }
}
