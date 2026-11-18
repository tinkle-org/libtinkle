//! Durable owning-domain actions. Exact bytes precede dispatch; recovery only queries original keys.
use super::*;
use std::collections::{HashSet, VecDeque};

/// Private static dispatch keeps each owner's request, status and recovery types together.
trait Action: prost::Message + Default + Clone {
    type Status: Status;
    const METHOD: &'static str;
    const GET: &'static str;
    const CANCEL: &'static str;
    const SEND: bool = false;
    fn target(&self, actor: i64) -> p::Target;
    fn validate(&self) -> Result<(), SendError>;
    fn query(&self, cancel: bool) -> Vec<u8>;
    fn submitted(&self, bytes: &[u8]) -> Result<Self::Status, SendError>;
    fn queried(&self, bytes: &[u8], cancel: bool) -> Result<Self::Status, SendError>;
    fn receipt(status: Self::Status) -> Vec<u8>;
    fn bound(&self, status: &Self::Status) -> bool;
    async fn notify_status(&self, _scope: &Scope, _status: &Self::Status) {}
    async fn reconcile(
        &self,
        _scope: &Arc<Scope>,
        _job: &OutgoingJob,
        _status: &Self::Status,
        _token: crate::CancelToken,
    ) -> Result<(), SendError> {
        Ok(())
    }
}

/// Common lifecycle checks do not introduce a public cross-domain operation or selector.
pub(crate) trait Status: prost::Message + Default + Clone + PartialEq {
    fn state(&self) -> i32;
    fn revision(&self) -> u64;
    fn failure(&self) -> i32;
    fn retain_until(&self) -> Option<&prost_types::Timestamp>;
    fn validate_identity(&self) -> bool;
    fn consistent_identity(&self, old: &Self) -> bool;
    fn remember_disclosed(&mut self, old: &Self);
    fn validate(&self) -> Result<(), SendError> {
        let state = self.state();
        let terminal = (3..=5).contains(&state);
        if !(1..=5).contains(&state)
            || self.revision() == 0
            || (if state == 4 {
                !(1..=5).contains(&self.failure())
            } else {
                self.failure() != 0
            })
            || terminal != self.retain_until().is_some()
            || self.retain_until().is_some_and(|t| {
                !(1..=253_402_300_799).contains(&t.seconds)
                    || !(0..1_000_000_000).contains(&t.nanos)
            })
            || !self.validate_identity()
        {
            return Err(SendError::InvalidRequest);
        }
        Ok(())
    }
    fn follows(&self, old: &Self) -> bool {
        if !self.consistent_identity(old) || self.revision() < old.revision() {
            return false;
        }
        if old.state() >= 3 {
            return self.state() == old.state()
                && self.revision() == old.revision()
                && self.failure() == old.failure()
                && self.retain_until() == old.retain_until();
        }
        if self.revision() == old.revision() {
            return self == old;
        }
        self.state() != old.state() && (old.state() == 1 || self.state() >= 3)
    }
}

/// Explicit disclosure decisions are mandatory only for successful identities.
fn disclosed(state: i32, withheld: Option<bool>, id: Option<i64>) -> bool {
    if state != 3 {
        return withheld.is_none() && id.is_none();
    }
    match withheld {
        Some(true) => id.is_none(),
        Some(false) => id.is_some_and(|id| id > 0),
        None => false,
    }
}

macro_rules! status_fields {
    () => {
        fn state(&self) -> i32 {
            self.state
        }
        fn revision(&self) -> u64 {
            self.revision
        }
        fn failure(&self) -> i32 {
            self.failure
        }
        fn retain_until(&self) -> Option<&prost_types::Timestamp> {
            self.retain_until.as_ref()
        }
    };
}

impl Status for p::SendStatus {
    status_fields!();
    fn validate_identity(&self) -> bool {
        if crate::services::validate_target(self.target.as_ref()).is_err()
            || self.idempotency_key == 0
        {
            return false;
        }
        if self.state != 3 {
            return self.accepted.is_empty();
        }
        if !(1..=10).contains(&self.accepted.len())
            || self.accepted[0].idempotency_key != self.idempotency_key
        {
            return false;
        }
        let album = self.accepted.len() > 1;
        let mut keys = HashSet::new();
        let mut messages = HashSet::new();
        let mut album_id = None;
        self.accepted.iter().all(|r| {
            if r.idempotency_key == 0
                || !keys.insert(r.idempotency_key)
                || !disclosed(3, r.reference_withheld, r.message_id)
            {
                return false;
            }
            if r.reference_withheld == Some(true) {
                return r.album_id.is_none();
            }
            if !messages.insert(r.message_id) {
                return false;
            }
            if !album {
                return r.album_id.is_none();
            }
            let Some(id) = r.album_id.filter(|n| *n > 0) else {
                return false;
            };
            if album_id.is_some_and(|old| old != id) {
                return false;
            }
            album_id = Some(id);
            true
        })
    }
    fn consistent_identity(&self, old: &Self) -> bool {
        self.target == old.target
            && self.idempotency_key == old.idempotency_key
            && (old.state != 3
                || (self.accepted.len() == old.accepted.len()
                    && self.accepted.iter().zip(&old.accepted).all(|(a, b)| {
                        a.idempotency_key == b.idempotency_key
                            && (a.reference_withheld != Some(false)
                                || b.reference_withheld != Some(false)
                                || (a.message_id == b.message_id && a.album_id == b.album_id))
                    })))
    }
    fn remember_disclosed(&mut self, old: &Self) {
        if self.state == 3 && old.state == 3 {
            for (new, known) in self.accepted.iter_mut().zip(&old.accepted) {
                if new.reference_withheld == Some(true) && known.reference_withheld == Some(false) {
                    *new = *known;
                }
            }
        }
    }
}

macro_rules! photo_status {
    ($ty:ty, $($key:ident),+) => {
        impl Status for $ty {
            status_fields!();
            fn validate_identity(&self) -> bool { disclosed(self.state, self.reference_withheld, self.photo_id) }
            fn consistent_identity(&self, old: &Self) -> bool {
                $(self.$key == old.$key &&)+
                (self.reference_withheld != Some(false) || old.reference_withheld != Some(false) || self.photo_id == old.photo_id)
            }
            fn remember_disclosed(&mut self,old:&Self) {
                if self.state==3&&old.state==3&&self.reference_withheld==Some(true)&&old.reference_withheld==Some(false) {self.reference_withheld=Some(false);self.photo_id=old.photo_id;}
            }
        }
    };
}
photo_status!(p::ChatPhotoUpdate, chat_id, operation_id);
photo_status!(p::ChannelPhotoUpdate, channel_id, request_id);
impl Status for p::ProfilePhotoUpdate {
    status_fields!();
    fn validate_identity(&self) -> bool {
        disclosed(self.state, self.reference_withheld, self.photo_id)
            && (if self.state == 3 {
                self.gallery_revision > 0
            } else {
                self.gallery_revision == 0
            })
    }
    fn consistent_identity(&self, old: &Self) -> bool {
        self.request_id == old.request_id
            && (old.state != 3 || self.gallery_revision == old.gallery_revision)
            && (self.reference_withheld != Some(false)
                || old.reference_withheld != Some(false)
                || self.photo_id == old.photo_id)
    }
    fn remember_disclosed(&mut self, old: &Self) {
        if self.state == 3
            && old.state == 3
            && self.reference_withheld == Some(true)
            && old.reference_withheld == Some(false)
        {
            self.reference_withheld = Some(false);
            self.photo_id = old.photo_id;
        }
    }
}
impl Status for p::AssetChange {
    status_fields!();
    fn validate_identity(&self) -> bool {
        if self.asset_id.is_some_and(|id| id <= 0) {
            return false;
        }
        if self.state != 3 {
            return self.reference_withheld.is_none() && self.accepted.is_none();
        }
        match self.reference_withheld {
            Some(true) => self.accepted.is_none(),
            Some(false) => self.accepted.as_ref().is_some_and(|r| {
                r.asset_id > 0
                    && r.asset_version > 0
                    && self.asset_id.is_none_or(|id| id == r.asset_id)
            }),
            None => false,
        }
    }
    fn consistent_identity(&self, old: &Self) -> bool {
        self.request_id == old.request_id
            && self.asset_id == old.asset_id
            && (self.reference_withheld != Some(false)
                || old.reference_withheld != Some(false)
                || self.accepted == old.accepted)
    }
    fn remember_disclosed(&mut self, old: &Self) {
        if self.state == 3
            && old.state == 3
            && self.reference_withheld == Some(true)
            && old.reference_withheld == Some(false)
        {
            self.reference_withheld = Some(false);
            self.accepted = old.accepted;
        }
    }
}

/// Decode only bounded application payloads; unknown input fields are checked before storage.
fn decode<M: prost::Message + Default>(bytes: &[u8]) -> Result<M, SendError> {
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(SendError::InvalidRequest);
    }
    M::decode(bytes).map_err(|_| SendError::InvalidRequest)
}

/// The same original target and ordered keys select single sends and atomic albums.
fn send_bound(status: &p::SendStatus, target: Option<p::Target>, keys: &[i64]) -> bool {
    status.target == target
        && keys.first() == Some(&status.idempotency_key)
        && (status.state != 3
            || status
                .accepted
                .iter()
                .map(|r| r.idempotency_key)
                .eq(keys.iter().copied()))
}

macro_rules! send_action {
    ($request:ty, $response:ty, $method:literal, $keys:expr, $validate:expr) => {
        impl Action for $request {
            type Status = p::SendStatus;
            const METHOD: &'static str = concat!("/tinkle.v1.MessagesService/", $method);
            const GET: &'static str = "/tinkle.v1.MessagesService/GetSendStatus";
            const CANCEL: &'static str = "/tinkle.v1.MessagesService/CancelSend";
            const SEND: bool = true;
            fn target(&self, _: i64) -> p::Target {
                self.target.unwrap_or_default()
            }
            fn validate(&self) -> Result<(), SendError> {
                ($validate)(self)
            }
            fn query(&self, cancel: bool) -> Vec<u8> {
                let keys: Vec<i64> = ($keys)(self);
                if cancel {
                    p::CancelSendRequest {
                        target: self.target,
                        idempotency_key: keys[0],
                    }
                    .encode_to_vec()
                } else {
                    p::GetSendStatusRequest {
                        target: self.target,
                        idempotency_key: keys[0],
                    }
                    .encode_to_vec()
                }
            }
            fn submitted(&self, bytes: &[u8]) -> Result<Self::Status, SendError> {
                decode::<$response>(bytes)?
                    .send_status
                    .ok_or(SendError::InvalidRequest)
            }
            fn queried(&self, bytes: &[u8], cancel: bool) -> Result<Self::Status, SendError> {
                if cancel {
                    decode::<p::CancelSendResponse>(bytes)?.send_status
                } else {
                    decode::<p::GetSendStatusResponse>(bytes)?.send_status
                }
                .ok_or(SendError::InvalidRequest)
            }
            fn receipt(status: Self::Status) -> Vec<u8> {
                let mut response = <$response>::default();
                response.send_status = Some(status);
                response.encode_to_vec()
            }
            fn bound(&self, status: &Self::Status) -> bool {
                send_bound(status, self.target, &($keys)(self))
            }
            async fn notify_status(&self, scope: &Scope, status: &Self::Status) {
                notify_send_status(scope, status).await;
            }
            async fn reconcile(
                &self,
                scope: &Arc<Scope>,
                job: &OutgoingJob,
                status: &Self::Status,
                token: crate::CancelToken,
            ) -> Result<(), SendError> {
                reconcile_send(scope, job, status, token).await
            }
        }
    };
}
send_action!(
    p::SendMediaRequest,
    p::SendMediaResponse,
    "SendMedia",
    |r: &p::SendMediaRequest| vec![r.idempotency_key],
    |r: &p::SendMediaRequest| {
        crate::services::validate_send_media(r)?;
        validate_source(r.media.as_ref())
    }
);
send_action!(
    p::SendAlbumRequest,
    p::SendAlbumResponse,
    "SendAlbum",
    |r: &p::SendAlbumRequest| r
        .items
        .iter()
        .map(|i| i.idempotency_key)
        .collect::<Vec<_>>(),
    |r: &p::SendAlbumRequest| {
        OutgoingRequest::Album(r.clone()).encode()?;
        for i in &r.items {
            validate_source(i.media.as_ref())?;
        }
        Ok(())
    }
);

/// Purpose-specific sources retain complete tiny JPEGs and separately uploaded thumbnail IDs.
fn validate_source(source: Option<&p::MediaSource>) -> Result<(), SendError> {
    let (main, preview, thumbnail) = match source.and_then(|s| s.media.as_ref()) {
        Some(p::media_source::Media::Photo(v)) => {
            (v.file_id, v.inline_preview.as_ref(), v.thumbnail.as_ref())
        }
        Some(p::media_source::Media::Video(v)) => {
            (v.file_id, v.inline_preview.as_ref(), v.thumbnail.as_ref())
        }
        Some(p::media_source::Media::Document(v)) => {
            (v.file_id, v.inline_preview.as_ref(), v.thumbnail.as_ref())
        }
        Some(p::media_source::Media::Audio(v)) => {
            (v.file_id, v.inline_preview.as_ref(), v.thumbnail.as_ref())
        }
        Some(p::media_source::Media::Sticker(v)) => {
            return validate_visual(
                v.file_id,
                v.inline_preview.as_ref(),
                v.thumbnail.as_ref(),
                128,
            );
        }
        Some(p::media_source::Media::Voice(v)) if v.file_id > 0 => return Ok(()),
        _ => return Err(SendError::InvalidRequest),
    };
    validate_visual(main, preview, thumbnail, 320)
}

/// Validate declared completed inputs without inferring a grant or measured main facts.
fn validate_visual(
    main: i64,
    preview: Option<&p::InlinePreview>,
    thumbnail: Option<&p::ThumbnailSource>,
    edge: u32,
) -> Result<(), SendError> {
    if main <= 0
        || thumbnail.is_some_and(|v| {
            v.file_id <= 0
                || v.file_id == main
                || v.width == 0
                || v.width > edge
                || v.height == 0
                || v.height > edge
        })
    {
        return Err(SendError::InvalidRequest);
    }
    if let Some(v) = preview {
        if crate::media::validate_tiny_preview(&v.jpeg)? != (v.width, v.height) {
            return Err(SendError::InvalidRequest);
        }
    }
    Ok(())
}

/// New photos may use a completed main shorthand; removal has no asynchronous action.
fn validate_chat_source(source: Option<&p::ChatPhotoSource>) -> Result<(), SendError> {
    match source.and_then(|s| s.input.as_ref()) {
        Some(p::chat_photo_source::Input::FileId(id)) if *id > 0 => Ok(()),
        Some(p::chat_photo_source::Input::Photo(v)) => validate_visual(
            v.file_id,
            v.inline_preview.as_ref(),
            v.thumbnail.as_ref(),
            160,
        ),
        _ => Err(SendError::InvalidRequest),
    }
}

macro_rules! owner_action {
    ($request:ty, $response:ty, $status:ty, $field:ident, $service:literal, $method:literal,
     $get:ident, $get_response:ty, $cancel:ident, $cancel_response:ty,
     $selector:expr, $target:expr, $validate:expr, $bound:expr) => {
        impl Action for $request {
            type Status = $status;
            const METHOD: &'static str = concat!("/tinkle.v1.", $service, "/", $method);
            const GET: &'static str = concat!("/tinkle.v1.", $service, "/", stringify!($get));
            const CANCEL: &'static str = concat!("/tinkle.v1.", $service, "/", stringify!($cancel));
            fn target(&self, actor: i64) -> p::Target {
                ($target)(self, actor)
            }
            fn validate(&self) -> Result<(), SendError> {
                ($validate)(self)
            }
            fn query(&self, cancel: bool) -> Vec<u8> {
                ($selector)(self, cancel)
            }
            fn submitted(&self, bytes: &[u8]) -> Result<Self::Status, SendError> {
                decode::<$response>(bytes)?
                    .$field
                    .ok_or(SendError::InvalidRequest)
            }
            fn queried(&self, bytes: &[u8], cancel: bool) -> Result<Self::Status, SendError> {
                if cancel {
                    decode::<$cancel_response>(bytes)?.$field
                } else {
                    decode::<$get_response>(bytes)?.$field
                }
                .ok_or(SendError::InvalidRequest)
            }
            fn receipt(status: Self::Status) -> Vec<u8> {
                <$response as OwnerReceipt<$status>>::retain(status).encode_to_vec()
            }
            fn bound(&self, status: &Self::Status) -> bool {
                ($bound)(self, status)
            }
        }
    };
}

/// Owning-method receipt envelopes preserve truthful commit flags without retaining stale bodies.
trait OwnerReceipt<S>: prost::Message {
    fn retain(status: S) -> Self;
}
impl OwnerReceipt<p::ChatPhotoUpdate> for p::EditChatPhotoResponse {
    fn retain(status: p::ChatPhotoUpdate) -> Self {
        Self {
            success: status.state == 3,
            photo_update: Some(status),
        }
    }
}
impl OwnerReceipt<p::ChannelPhotoUpdate> for p::EditPhotoResponse {
    fn retain(status: p::ChannelPhotoUpdate) -> Self {
        Self {
            success: status.state == 3,
            photo_update: Some(status),
        }
    }
}
impl OwnerReceipt<p::ProfilePhotoUpdate> for p::SetProfilePhotoResponse {
    fn retain(status: p::ProfilePhotoUpdate) -> Self {
        Self {
            gallery_revision: status.gallery_revision,
            photo: None,
            photo_update: Some(status),
        }
    }
}
impl OwnerReceipt<p::AssetChange> for p::CreateStickerAssetResponse {
    fn retain(status: p::AssetChange) -> Self {
        Self {
            asset: None,
            asset_change: Some(status),
        }
    }
}
impl OwnerReceipt<p::AssetChange> for p::UpdateStickerAssetResponse {
    fn retain(status: p::AssetChange) -> Self {
        Self {
            asset: None,
            asset_change: Some(status),
        }
    }
}
owner_action!(
    p::EditChatPhotoRequest,
    p::EditChatPhotoResponse,
    p::ChatPhotoUpdate,
    photo_update,
    "MessagesService",
    "EditChatPhoto",
    GetChatPhotoUpdate,
    p::GetChatPhotoUpdateResponse,
    CancelChatPhotoUpdate,
    p::CancelChatPhotoUpdateResponse,
    |r: &p::EditChatPhotoRequest, cancel| if cancel {
        p::CancelChatPhotoUpdateRequest {
            chat_id: r.chat_id,
            operation_id: r.operation_id,
        }
        .encode_to_vec()
    } else {
        p::GetChatPhotoUpdateRequest {
            chat_id: r.chat_id,
            operation_id: r.operation_id,
        }
        .encode_to_vec()
    },
    |r: &p::EditChatPhotoRequest, _| room(r.chat_id),
    |r: &p::EditChatPhotoRequest| {
        if r.chat_id <= 0 || r.operation_id <= 0 {
            return Err(SendError::InvalidRequest);
        }
        validate_chat_source(r.photo.as_ref())
    },
    |r: &p::EditChatPhotoRequest, s: &p::ChatPhotoUpdate| r.chat_id == s.chat_id
        && r.operation_id == s.operation_id
);
owner_action!(
    p::EditPhotoRequest,
    p::EditPhotoResponse,
    p::ChannelPhotoUpdate,
    photo_update,
    "ChannelsService",
    "EditPhoto",
    GetPhotoUpdate,
    p::GetPhotoUpdateResponse,
    CancelPhotoUpdate,
    p::CancelPhotoUpdateResponse,
    |r: &p::EditPhotoRequest, cancel| if cancel {
        p::CancelPhotoUpdateRequest {
            channel_id: r.channel_id,
            request_id: r.request_id.clone(),
        }
        .encode_to_vec()
    } else {
        p::GetPhotoUpdateRequest {
            channel_id: r.channel_id,
            request_id: r.request_id.clone(),
        }
        .encode_to_vec()
    },
    |r: &p::EditPhotoRequest, _| room(r.channel_id),
    |r: &p::EditPhotoRequest| {
        if r.channel_id <= 0 || !uuid(&r.request_id) {
            return Err(SendError::InvalidRequest);
        }
        validate_chat_source(r.photo.as_ref())
    },
    |r: &p::EditPhotoRequest, s: &p::ChannelPhotoUpdate| r.channel_id == s.channel_id
        && r.request_id == s.request_id
);
owner_action!(
    p::SetProfilePhotoRequest,
    p::SetProfilePhotoResponse,
    p::ProfilePhotoUpdate,
    photo_update,
    "AccountService",
    "SetProfilePhoto",
    GetProfilePhotoUpdate,
    p::GetProfilePhotoUpdateResponse,
    CancelProfilePhotoUpdate,
    p::CancelProfilePhotoUpdateResponse,
    |r: &p::SetProfilePhotoRequest, cancel| if cancel {
        p::CancelProfilePhotoUpdateRequest {
            request_id: r.request_id.clone(),
        }
        .encode_to_vec()
    } else {
        p::GetProfilePhotoUpdateRequest {
            request_id: r.request_id.clone(),
        }
        .encode_to_vec()
    },
    |_: &p::SetProfilePhotoRequest, actor| user(actor),
    |r: &p::SetProfilePhotoRequest| {
        if r.expected_revision == 0 || !uuid(&r.request_id) {
            return Err(SendError::InvalidRequest);
        }
        let s = r.source.as_ref().ok_or(SendError::InvalidRequest)?;
        let m = s.metadata.as_ref().ok_or(SendError::InvalidRequest)?;
        if m.width == 0
            || m.height == 0
            || m.width > 4096
            || m.height > 4096
            || m.animated
            || !matches!(m.format, 1 | 2)
            || !m
                .validation
                .as_ref()
                .is_some_and(|v| v.profile == 1 && v.version == 1)
        {
            return Err(SendError::InvalidRequest);
        }
        validate_visual(
            s.file_id,
            s.inline_preview.as_ref(),
            s.thumbnail.as_ref(),
            160,
        )
    },
    |r: &p::SetProfilePhotoRequest, s: &p::ProfilePhotoUpdate| r.request_id == s.request_id
        && (s.state != 3 || s.gallery_revision >= r.expected_revision)
);

macro_rules! asset_action {
    ($request:ty, $response:ty, $method:literal, $asset:expr, $validate:expr, $accepted:expr) => {
        owner_action!(
            $request,
            $response,
            p::AssetChange,
            asset_change,
            "StickersService",
            $method,
            GetAssetChange,
            p::GetAssetChangeResponse,
            CancelAssetChange,
            p::CancelAssetChangeResponse,
            |r: &$request, cancel| if cancel {
                p::CancelAssetChangeRequest {
                    request_id: r.request_id.clone(),
                    asset_id: ($asset)(r),
                }
                .encode_to_vec()
            } else {
                p::GetAssetChangeRequest {
                    request_id: r.request_id.clone(),
                    asset_id: ($asset)(r),
                }
                .encode_to_vec()
            },
            |_: &$request, actor| user(actor),
            $validate,
            |r: &$request, s: &p::AssetChange| r.request_id == s.request_id
                && ($asset)(r) == s.asset_id
                && ($accepted)(r, s)
        );
    };
}
asset_action!(
    p::CreateStickerAssetRequest,
    p::CreateStickerAssetResponse,
    "CreateStickerAsset",
    |_: &p::CreateStickerAssetRequest| None,
    |r: &p::CreateStickerAssetRequest| {
        if !uuid(&r.request_id)
            || !matches!(r.kind, 1 | 2)
            || (if r.kind == 2 {
                r.fallback_text.is_empty() || r.fallback_text.len() > 64
            } else {
                !r.fallback_text.is_empty()
            })
        {
            return Err(SendError::InvalidRequest);
        }
        validate_asset(r.source.as_ref(), r.kind)
    },
    |_: &p::CreateStickerAssetRequest, _: &p::AssetChange| true
);
asset_action!(
    p::UpdateStickerAssetRequest,
    p::UpdateStickerAssetResponse,
    "UpdateStickerAsset",
    |r: &p::UpdateStickerAssetRequest| Some(r.asset_id),
    |r: &p::UpdateStickerAssetRequest| {
        if !uuid(&r.request_id)
            || r.asset_id <= 0
            || r.expected_version == 0
            || r.fallback_text.as_ref().is_some_and(|v| v.len() > 64)
        {
            return Err(SendError::InvalidRequest);
        }
        if r.source.is_some() {
            validate_asset(r.source.as_ref(), 0)?;
        }
        Ok(())
    },
    |r: &p::UpdateStickerAssetRequest, s: &p::AssetChange| s
        .accepted
        .as_ref()
        .is_none_or(|a| a.asset_version > r.expected_version)
);

fn validate_asset(source: Option<&p::StickerAssetSource>, kind: i32) -> Result<(), SendError> {
    crate::services::asset(source, kind)?;
    let s = source.ok_or(SendError::InvalidRequest)?;
    validate_visual(
        s.file_id,
        s.inline_preview.as_ref(),
        s.thumbnail.as_ref(),
        128,
    )
}

/// Existing typed mutations validate the same source/key rules; removal stays synchronous.
pub(crate) fn validate_direct(method: &str, bytes: &[u8]) -> Result<(), SendError> {
    match method {
        p::SetProfilePhotoRequest::METHOD => decode::<p::SetProfilePhotoRequest>(bytes)?.validate(),
        p::CreateStickerAssetRequest::METHOD => {
            decode::<p::CreateStickerAssetRequest>(bytes)?.validate()
        }
        p::UpdateStickerAssetRequest::METHOD => {
            let r = decode::<p::UpdateStickerAssetRequest>(bytes)?;
            if r.source.is_some() || !r.request_id.is_empty() {
                r.validate()
            } else {
                Ok(())
            }
        }
        p::EditChatPhotoRequest::METHOD => {
            let r = decode::<p::EditChatPhotoRequest>(bytes)?;
            if matches!(
                r.photo.as_ref().and_then(|p| p.input.as_ref()),
                Some(p::chat_photo_source::Input::Empty(true))
            ) {
                if r.operation_id == 0 {
                    Ok(())
                } else {
                    Err(SendError::InvalidRequest)
                }
            } else {
                r.validate()
            }
        }
        _ => Ok(()),
    }
}
fn uuid(id: &str) -> bool {
    tinkle_schema::sync::session_id(id)
}
fn room(id: i64) -> p::Target {
    p::Target {
        kind: Some(p::target::Kind::ChatId(id)),
    }
}
fn user(id: i64) -> p::Target {
    p::Target {
        kind: Some(p::target::Kind::UserId(id)),
    }
}
fn actor(scope: &Scope) -> Result<i64, SendError> {
    scope
        .principal
        .strip_prefix("user:")
        .and_then(|s| s.parse().ok())
        .filter(|id| *id > 0)
        .ok_or(SendError::NoPrincipal)
}

/// Only ordinary immediate file-backed sends enter asynchronous recovery; other sends keep their contract.
pub(super) fn is_send(method: &str, payload: &[u8]) -> Result<bool, SendError> {
    match method {
        "/tinkle.v1.MessagesService/SendMedia" => {
            let r: p::SendMediaRequest = decode(payload)?;
            Ok(r.schedule_date.is_none()
                && matches!(
                    r.media.and_then(|m| m.media),
                    Some(
                        p::media_source::Media::Photo(_)
                            | p::media_source::Media::Video(_)
                            | p::media_source::Media::Document(_)
                            | p::media_source::Media::Audio(_)
                            | p::media_source::Media::Voice(_)
                            | p::media_source::Media::Sticker(_)
                    )
                ))
        }
        "/tinkle.v1.MessagesService/SendAlbum" => Ok(decode::<p::SendAlbumRequest>(payload)?
            .schedule_date
            .is_none()),
        _ => Ok(false),
    }
}

/// Validate all new file inputs before durable admission, without allocating or changing keys.
pub(super) fn validate_send_sources(method: &str, payload: &[u8]) -> Result<(), SendError> {
    if !is_send(method, payload)? {
        return Ok(());
    }
    match method {
        p::SendMediaRequest::METHOD => {
            validate_source(decode::<p::SendMediaRequest>(payload)?.media.as_ref())
        }
        p::SendAlbumRequest::METHOD => {
            for item in decode::<p::SendAlbumRequest>(payload)?.items {
                validate_source(item.media.as_ref())?;
            }
            Ok(())
        }
        _ => Err(SendError::InvalidRequest),
    }
}

/// All asynchronous states use non-overlapping persisted discriminants; old rows are never adopted.
fn action_state(state: OutgoingState) -> bool {
    (state as i64) >= 14
}

impl Sending<'_> {
    /// Persist an owner's original request, including its UUID/CAS, without transmitting.
    async fn enqueue_action<A: Action>(
        &self,
        job_id: String,
        request: A,
    ) -> Result<OutgoingJob, SendError> {
        request.validate()?;
        let payload = request.encode_to_vec();
        self.persist_action(job_id, request, payload).await
    }

    /// Preserve exact raw field ordering and explicit scalar presence as the durable request.
    async fn persist_action<A: Action>(
        &self,
        job_id: String,
        request: A,
        payload: Vec<u8>,
    ) -> Result<OutgoingJob, SendError> {
        tinkle_schema::wire::validate_request(A::METHOD, &payload)
            .map_err(|_| SendError::InvalidRequest)?;
        let scope = Scope::capture(self.client).await?;
        let target = request.target(actor(&scope)?);
        crate::services::validate_target(Some(&target))?;
        let target_key = tinkle_schema::sync::target_key_for(
            target.kind.as_ref().ok_or(SendError::InvalidRequest)?,
        );
        let stream = target_key.clone();
        let fence = scope
            .store(move |s, p, _| s.capture_message_scope(p, &stream))
            .await?;
        let job = OutgoingJob {
            job_id: job_id.clone(),
            source: scope.source.clone(),
            target_key,
            method: A::METHOD.into(),
            payload,
            state: OutgoingState::ActionPrepared,
            response: vec![],
            last_outcome: None,
            revision: 0,
            cache_generation: fence.generation(),
            cache_incarnation: fence.incarnation(),
        };
        let copy = job.clone();
        scope
            .store(move |s, p, _| s.insert_outgoing(p, &copy))
            .await?;
        scope
            .store(move |s, p, source| s.outgoing(p, source, &job_id))
            .await?
            .ok_or(SendError::NotFound)
    }

    /// Caller-owned exact bytes and domain keys are persisted before their first dispatch.
    pub async fn enqueue_chat_photo(
        &self,
        job_id: String,
        request: p::EditChatPhotoRequest,
    ) -> Result<OutgoingJob, SendError> {
        self.enqueue_action(job_id, request).await
    }
    /// The channel request UUID must already be chosen and remains immutable through retries.
    pub async fn enqueue_channel_photo(
        &self,
        job_id: String,
        request: p::EditPhotoRequest,
    ) -> Result<OutgoingJob, SendError> {
        self.enqueue_action(job_id, request).await
    }
    /// Retain the original gallery revision, never a refreshed CAS on an uncertain retry.
    pub async fn enqueue_profile_photo(
        &self,
        job_id: String,
        request: p::SetProfilePhotoRequest,
    ) -> Result<OutgoingJob, SendError> {
        self.enqueue_action(job_id, request).await
    }
    /// Retain the exact create source and actor-scoped request UUID.
    pub async fn enqueue_create_asset(
        &self,
        job_id: String,
        request: p::CreateStickerAssetRequest,
    ) -> Result<OutgoingJob, SendError> {
        self.enqueue_action(job_id, request).await
    }
    /// Retain the original asset selector, version and request UUID.
    pub async fn enqueue_update_asset(
        &self,
        job_id: String,
        request: p::UpdateStickerAssetRequest,
    ) -> Result<OutgoingJob, SendError> {
        self.enqueue_action(job_id, request).await
    }

    /// Raw bytes keep legal field ordering/presence; recursively unknown fields are rejected.
    pub async fn enqueue_action_raw(
        &self,
        job_id: String,
        method: String,
        payload: Vec<u8>,
    ) -> Result<OutgoingJob, SendError> {
        tinkle_schema::wire::validate_request(&method, &payload)
            .map_err(|_| SendError::InvalidRequest)?;
        macro_rules! owner {
            ($ty:ty) => {{
                let request: $ty = decode(&payload)?;
                request.validate()?;
                self.persist_action(job_id, request, payload).await
            }};
        }
        match method.as_str() {
            p::EditChatPhotoRequest::METHOD => owner!(p::EditChatPhotoRequest),
            p::EditPhotoRequest::METHOD => owner!(p::EditPhotoRequest),
            p::SetProfilePhotoRequest::METHOD => owner!(p::SetProfilePhotoRequest),
            p::CreateStickerAssetRequest::METHOD => owner!(p::CreateStickerAssetRequest),
            p::UpdateStickerAssetRequest::METHOD => owner!(p::UpdateStickerAssetRequest),
            _ => Err(SendError::InvalidRequest),
        }
    }

    /// Static owning-method dispatch, never a wire-level operation union.
    pub(super) async fn resume_action(
        &self,
        scope: &Arc<Scope>,
        job: OutgoingJob,
        cancel: Option<crate::CancelToken>,
    ) -> Result<OutgoingJob, SendError> {
        macro_rules! run {
            ($ty:ty) => {
                self.run_action::<$ty>(scope, job, false, false, cancel)
                    .await
            };
        }
        match job.method.as_str() {
            p::SendMediaRequest::METHOD => run!(p::SendMediaRequest),
            p::SendAlbumRequest::METHOD => run!(p::SendAlbumRequest),
            p::EditChatPhotoRequest::METHOD => run!(p::EditChatPhotoRequest),
            p::EditPhotoRequest::METHOD => run!(p::EditPhotoRequest),
            p::SetProfilePhotoRequest::METHOD => run!(p::SetProfilePhotoRequest),
            p::CreateStickerAssetRequest::METHOD => run!(p::CreateStickerAssetRequest),
            p::UpdateStickerAssetRequest::METHOD => run!(p::UpdateStickerAssetRequest),
            _ => Err(SendError::InvalidRequest),
        }
    }

    /// A dropped or canceled waiter leaves durable dispatch evidence; it never restores preparation.
    async fn run_action<A: Action>(
        &self,
        scope: &Arc<Scope>,
        mut job: OutgoingJob,
        query: bool,
        cancel_action: bool,
        cancel: Option<crate::CancelToken>,
    ) -> Result<OutgoingJob, SendError> {
        if !action_state(job.state) || job.method != A::METHOD {
            return Err(SendError::Conflict);
        }
        tinkle_schema::wire::validate_request(A::METHOD, &job.payload)
            .map_err(|_| SendError::InvalidRequest)?;
        let request: A = decode(&job.payload)?;
        request.validate()?;
        let target = request.target(actor(scope)?);
        if target
            .kind
            .as_ref()
            .map(tinkle_schema::sync::target_key_for)
            .as_deref()
            != Some(job.target_key.as_str())
        {
            return Err(SendError::Conflict);
        }
        let token = cancel.unwrap_or_default();
        if !query
            && matches!(
                job.state,
                OutgoingState::ActionFailed | OutgoingState::ActionCanceled
            )
        {
            return Ok(job);
        }
        if token.is_cancelled() {
            return Ok(job);
        }
        let query = query || job.state != OutgoingState::ActionPrepared;
        if query && job.state == OutgoingState::ActionPrepared {
            return Err(SendError::Conflict);
        }
        if !query {
            // Check connection before recording dispatch; no implicit dial or mutation replay.
            let generation = self
                .client
                .session_state
                .generation()
                .filter(|g| g.open.load(Ordering::SeqCst))
                .ok_or_else(|| {
                    SendError::Rpc(TinkleError::NotConnected("connect before sending".into()))
                })?;
            let receiving_scope = if A::SEND {
                scope.with_generation(generation.clone())
            } else {
                scope.clone()
            };
            let caller = receiving_scope.caller().for_session_generation(generation);
            scope
                .transition(&mut job, OutgoingState::ActionDispatching, vec![], None)
                .await?;
            let result = caller
                .call_raw_cancellable(A::METHOD, job.payload.clone(), 30_000, &token)
                .await;
            let status = match result {
                Ok(bytes) => request.submitted(&bytes),
                Err(error) => {
                    let (_, mut outcome) = failure_outcome(&error);
                    // Even ResourceExhausted can be a post-admission response-size failure.
                    // No wire error proves that the owner did not persist this action.
                    outcome.unknown_outcome = true;
                    scope
                        .transition(
                            &mut job,
                            OutgoingState::ActionUnconfirmed,
                            vec![],
                            Some(outcome),
                        )
                        .await?;
                    return Err(error.into());
                }
            }?;
            return self
                .accept_action(&receiving_scope, job, &request, status, false, token)
                .await;
        }
        let caller = scope.caller();
        let budget = caller.call_budget(30_000)?;
        let mut deciding_cancel = cancel_action;
        // One extra read consumes the original budget. Continued hint churn stays covered by the
        // existing bounded periodic recovery; a notification can never select an outcome.
        for attempt in 0..2 {
            let receiving_scope = if A::SEND {
                let generation = caller
                    .bound_send_generation()
                    .or_else(|| caller.session_state.generation())
                    .ok_or(SendError::ScopeChanged)?;
                scope.with_generation(generation)
            } else {
                scope.clone()
            };
            let caller = receiving_scope.caller();
            let manager = caller.live_mgr().await?;
            let revision = manager.action_hint_revision();
            let bytes = caller
                .call_raw_cancellable(
                    if deciding_cancel { A::CANCEL } else { A::GET },
                    request.query(deciding_cancel),
                    Client::remaining_timeout_ms(&budget)?,
                    &token,
                )
                .await?;
            let status = request.queried(&bytes, deciding_cancel)?;
            job = self
                .accept_action(
                    &receiving_scope,
                    job,
                    &request,
                    status,
                    deciding_cancel,
                    token.clone(),
                )
                .await?;
            let current = caller.live_mgr().await?;
            if attempt == 1
                || (Arc::ptr_eq(&manager, &current) && revision == manager.action_hint_revision())
            {
                break;
            }
            deciding_cancel = false;
        }
        Ok(job)
    }

    /// Commit identity independently of disclosure; optional submission/query bodies are never cached.
    async fn accept_action<A: Action>(
        &self,
        scope: &Arc<Scope>,
        mut job: OutgoingJob,
        request: &A,
        status: A::Status,
        cancel: bool,
        token: crate::CancelToken,
    ) -> Result<OutgoingJob, SendError> {
        let id = job.job_id.clone();
        let known = scope
            .store(move |s, p, source| s.outgoing_action_evidence(p, source, &id))
            .await?;
        let reduced = reduce_action(&job, request, &status, known.as_deref(), cancel)?;
        let Some((state, response, evidence)) = reduced else {
            // An identical explicit status read also retries interrupted same-receipt hydration.
            // An older reply never authorizes using its disclosure.
            if job.state == OutgoingState::ActionSucceeded
                && request.submitted(&job.response)? == status
            {
                request.reconcile(scope, &job, &status, token).await?;
            }
            return Ok(job);
        };
        scope
            .transition_inner(&mut job, state, response, None, Some(evidence))
            .await?;
        request.notify_status(scope, &status).await;
        if job.state == OutgoingState::ActionSucceeded {
            request.reconcile(scope, &job, &status, token).await?;
        }
        Ok(job)
    }

    /// One bounded fair sweep queries retained work; no submit calls occur in the supervisor.
    pub(crate) async fn recover_next_action(&self, queue: &mut VecDeque<String>) {
        let Ok(generation) = self.client.session_generation() else {
            return;
        };
        let caller = self.client.for_session_generation(generation);
        let sending = caller.sending();
        let Ok(scope) = Scope::capture(&caller).await else {
            return;
        };
        let Ok(due) = scope
            .store(|s, p, source| {
                let hydration: HashSet<_> = s.send_hydration_due(p, source)?.into_iter().collect();
                Ok(s.list_outgoing(p, source)?
                    .into_iter()
                    .filter(|j| match j.state {
                        OutgoingState::ActionDispatching
                        | OutgoingState::ActionUnconfirmed
                        | OutgoingState::ActionPending
                        | OutgoingState::ActionProcessing => true,
                        OutgoingState::ActionSucceeded => match j.method.as_str() {
                            p::SendMediaRequest::METHOD | p::SendAlbumRequest::METHOD => {
                                hydration.contains(&j.job_id)
                            }
                            _ => true,
                        },
                        _ => false,
                    })
                    .map(|j| j.job_id)
                    .collect())
            })
            .await
        else {
            return;
        };
        let id = next_recovery_action(queue, due);
        if let Some(id) = id {
            let Ok(_admission) = SEND_ATTEMPTS.try_acquire() else {
                return;
            };
            let token = crate::CancelToken::new();
            let _guard = CancelOnDrop(Some(token.clone()));
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                // Reuse the scope that selected the job; never rebind it to a new cache
                // incarnation or start a mutation if the ID now names prepared work.
                sending.recover_in_scope(&scope, id, false, Some(token), None),
            )
            .await;
        }
    }
}

/// Reduce command and exact lookup snapshots without I/O; retained identity is never disclosure.
type ReducedAction = (OutgoingState, Vec<u8>, Vec<u8>);

fn reduce_action<A: Action>(
    job: &OutgoingJob,
    request: &A,
    status: &A::Status,
    known: Option<&[u8]>,
    cancel: bool,
) -> Result<Option<ReducedAction>, SendError> {
    status.validate()?;
    if !request.bound(status) || (cancel && status.state() == 1) {
        return Err(SendError::InvalidRequest);
    }
    let old = (!job.response.is_empty())
        .then(|| request.submitted(&job.response))
        .transpose()?;
    let known = known.map(decode::<A::Status>).transpose()?;
    if let Some(old) = &old {
        old.validate()?;
        if A::SEND && status.revision() < old.revision() && old.follows(status) {
            return Ok(None);
        }
        if !status.follows(old) {
            return Err(SendError::Conflict);
        }
    }
    let mut evidence = status.clone();
    if let Some(known) = &known {
        known.validate()?;
        if !status.follows(known) {
            return Err(SendError::Conflict);
        }
        evidence.remember_disclosed(known);
    }
    // Alternating partial disclosures can conflict across ordered slots.
    evidence.validate()?;
    if A::SEND && old.as_ref() == Some(status) && known.as_ref() == Some(&evidence) {
        return Ok(None);
    }
    let state = match status.state() {
        1 => OutgoingState::ActionPending,
        2 => OutgoingState::ActionProcessing,
        3 => OutgoingState::ActionSucceeded,
        4 => OutgoingState::ActionFailed,
        5 => OutgoingState::ActionCanceled,
        _ => return Err(SendError::InvalidRequest),
    };
    Ok(Some((
        state,
        A::receipt(status.clone()),
        evidence.encode_to_vec(),
    )))
}

/// Refresh eligibility in linear expected time while retaining the surviving FIFO rotation.
fn next_recovery_action(queue: &mut VecDeque<String>, due: Vec<String>) -> Option<String> {
    let eligible: HashSet<_> = due.iter().cloned().collect();
    let mut queued = HashSet::new();
    queue.retain(|id| eligible.contains(id) && queued.insert(id.clone()));
    for id in due {
        if queued.insert(id.clone()) {
            queue.push_back(id);
        }
    }
    let id = queue.pop_front()?;
    queue.push_back(id.clone());
    Some(id)
}

/// Domain query/cancel entry points cannot accidentally select another owner's retained job.
macro_rules! recovery_api {
    ($get:ident, $cancel:ident, $inspect:ident, $status:ty, $($request:ty),+) => {
        impl Sending<'_> {
            /// Query the original durable domain keys without submitting a mutation.
            pub async fn $get(&self, job_id: String, cancel: Option<crate::CancelToken>) -> Result<OutgoingJob, SendError> { self.recover_owned(job_id, false, cancel, Some(&[$(<$request>::METHOD),+])).await }
            /// Request a domain cancellation decision; PROCESSING is not rollback.
            pub async fn $cancel(&self, job_id: String, cancel: Option<crate::CancelToken>) -> Result<OutgoingJob, SendError> { self.recover_owned(job_id, true, cancel, Some(&[$(<$request>::METHOD),+])).await }
            /// Offline retained identity snapshot, not a fresh body/file disclosure decision.
            pub async fn $inspect(&self, job_id: String) -> Result<Option<$status>, SendError> {
                let job = self.get(job_id).await?;
                if !action_state(job.state) { return Err(SendError::Conflict); }
                match job.method.as_str() { $(<$request>::METHOD => {
                    if job.response.is_empty() { return Ok(None); }
                    let request: $request = decode(&job.payload)?;
                    let status = request.submitted(&job.response)?;
                    status.validate()?; if !request.bound(&status) { return Err(SendError::InvalidRequest); } Ok(Some(status))
                }),+, _ => Err(SendError::InvalidRequest) }
            }
        }
    };
}
recovery_api!(
    get_send_status,
    cancel_send,
    retained_send_status,
    p::SendStatus,
    p::SendMediaRequest,
    p::SendAlbumRequest
);
recovery_api!(
    get_chat_photo_update,
    cancel_chat_photo_update,
    retained_chat_photo_update,
    p::ChatPhotoUpdate,
    p::EditChatPhotoRequest
);
recovery_api!(
    get_channel_photo_update,
    cancel_channel_photo_update,
    retained_channel_photo_update,
    p::ChannelPhotoUpdate,
    p::EditPhotoRequest
);
recovery_api!(
    get_profile_photo_update,
    cancel_profile_photo_update,
    retained_profile_photo_update,
    p::ProfilePhotoUpdate,
    p::SetProfilePhotoRequest
);
recovery_api!(
    get_asset_change,
    cancel_asset_change,
    retained_asset_change,
    p::AssetChange,
    p::CreateStickerAssetRequest,
    p::UpdateStickerAssetRequest
);

impl Sending<'_> {
    async fn recover_owned(
        &self,
        id: String,
        cancel_action: bool,
        cancel: Option<crate::CancelToken>,
        methods: Option<&[&str]>,
    ) -> Result<OutgoingJob, SendError> {
        let _admission = SEND_ATTEMPTS.try_acquire().map_err(|_| SendError::Busy)?;
        let scope = Scope::capture(self.client).await?;
        self.recover_in_scope(&scope, id, cancel_action, cancel, methods)
            .await
    }

    /// Run a point query or explicit cancellation under the caller's admission and captured scope.
    async fn recover_in_scope(
        &self,
        scope: &Arc<Scope>,
        id: String,
        cancel_action: bool,
        cancel: Option<crate::CancelToken>,
        methods: Option<&[&str]>,
    ) -> Result<OutgoingJob, SendError> {
        let key = format!("{}:{}:{id}", scope.principal, scope.source);
        if !self
            .client
            .outgoing_active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key.clone())
        {
            return Err(SendError::Busy);
        }
        let _flight = Flight {
            active: self.client.outgoing_active.clone(),
            key,
        };
        let job = scope
            .store(move |s, p, source| s.outgoing(p, source, &id))
            .await?
            .ok_or(SendError::NotFound)?;
        if methods.is_some_and(|methods| !methods.contains(&job.method.as_str())) {
            return Err(SendError::InvalidRequest);
        }
        macro_rules! run {
            ($ty:ty) => {
                self.run_action::<$ty>(scope, job, true, cancel_action, cancel)
                    .await
            };
        }
        match job.method.as_str() {
            p::SendMediaRequest::METHOD => run!(p::SendMediaRequest),
            p::SendAlbumRequest::METHOD => run!(p::SendAlbumRequest),
            p::EditChatPhotoRequest::METHOD => run!(p::EditChatPhotoRequest),
            p::EditPhotoRequest::METHOD => run!(p::EditPhotoRequest),
            p::SetProfilePhotoRequest::METHOD => run!(p::SetProfilePhotoRequest),
            p::CreateStickerAssetRequest::METHOD => run!(p::CreateStickerAssetRequest),
            p::UpdateStickerAssetRequest::METHOD => run!(p::UpdateStickerAssetRequest),
            _ => Err(SendError::InvalidRequest),
        }
    }
}

/// Best-effort local notice; consumers reread state and the delivery path checks the live scope.
async fn notify_send_status(scope: &Scope, status: &p::SendStatus) {
    if !scope.generation.as_ref().is_some_and(|g| g.returning) || scope.check().is_err() {
        return;
    }
    if let Ok(manager) = scope.caller().live_mgr().await {
        manager
            .emit_event(crate::SyncEvent::SendStatusChanged(
                crate::sync::MediaHintEvent {
                    notification: p::SendStatusChanged {
                        target: status.target,
                        idempotency_key: status.idempotency_key,
                        revision: status.revision,
                    },
                    principal: scope.principal.clone(),
                    epoch: scope.epoch,
                    source_revision: scope.revision,
                },
            ))
            .await;
    }
}

/// Current bodies come from ordinary authorized reads, never the original request or retained result.
async fn reconcile_send(
    scope: &Arc<Scope>,
    job: &OutgoingJob,
    status: &p::SendStatus,
    token: crate::CancelToken,
) -> Result<(), SendError> {
    let ids = status
        .accepted
        .iter()
        .filter_map(|r| r.message_id)
        .collect::<Vec<_>>();
    if ids.is_empty() {
        let job = job.clone();
        return scope
            .store(move |s, p, _| s.complete_send_hydration(p, &job))
            .await;
    }
    let key = job.target_key.clone();
    let fence = scope
        .store(move |s, p, _| s.capture_message_scope(p, &key))
        .await?;
    // Scope invalidation need not tombstone unknown old IDs. Preserve acceptance but
    // never let its later recovery adopt a revalidated history generation.
    if fence.generation() != job.cache_generation || fence.incarnation() != job.cache_incarnation {
        let retained = job.clone();
        scope
            .store(move |s, p, _| s.complete_send_hydration(p, &retained))
            .await?;
        return Err(SendError::ScopeChanged);
    }
    let bytes = scope
        .caller()
        .call_raw_cancellable(
            tinkle_schema::sync::GET_MESSAGES_METHOD,
            p::GetMessagesRequest {
                target: status.target,
                message_ids: ids.clone(),
            }
            .encode_to_vec(),
            30_000,
            &token,
        )
        .await?;
    let response: p::GetMessagesResponse = decode(&bytes)?;
    let mut seen = HashSet::new();
    if response.messages.len() > ids.len()
        || response.messages.iter().any(|m| {
            !ids.contains(&m.message_id)
                || !seen.insert(m.message_id)
                || m.target != status.target
                || m.accept_seq.is_none_or(|n| n <= 0)
                || m.scheduled_state == p::MessageScheduledState::Pending as i32
        })
    {
        return Err(SendError::InvalidRequest);
    }
    if response.messages.is_empty() {
        let retained = job.clone();
        return scope
            .store(move |s, p, _| s.complete_send_hydration(p, &retained))
            .await;
    }
    let mut ops = crate::cache_projector::project_messages_to_ops(
        &job.target_key,
        &response.messages,
        &Default::default(),
    )
    .map_err(|_| SendError::InvalidRequest)?;
    crate::cache_projector::merge_ops(
        &mut ops,
        crate::cache_projector::project_entities_to_ops(&response.users, &response.chats),
    )
    .map_err(|_| SendError::InvalidRequest)?;
    crate::cache_projector::rebase_file_origins(&mut ops, &job.target_key);
    let retained = job.clone();
    let visible = scope
        .store(move |s, p, _| s.hydrate_send_action(p, &retained, &ops, &fence))
        .await?;
    if !visible.is_empty() {
        notify_send_status(scope, status).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Eligibility changes prune stale work without reordering survivors or duplicating slots.
    #[test]
    fn recovery_queue_preserves_fair_order_across_eligibility_changes() {
        let mut queue = VecDeque::from(["b".into(), "gone".into(), "a".into(), "b".into()]);
        let due = || vec!["a".into(), "b".into(), "c".into(), "c".into()];
        for expected in ["b", "a", "c", "b"] {
            assert_eq!(
                next_recovery_action(&mut queue, due()).as_deref(),
                Some(expected)
            );
            assert_eq!(queue.len(), 3);
        }
        assert_eq!(
            next_recovery_action(&mut queue, vec!["c".into(), "d".into()]).as_deref(),
            Some("c")
        );
        assert_eq!(queue, VecDeque::from(["d".into(), "c".into()]));
        assert_eq!(next_recovery_action(&mut queue, vec![]), None);
        assert!(queue.is_empty());
    }

    fn status() -> p::SendStatus {
        p::SendStatus {
            target: Some(room(7)),
            idempotency_key: i64::MIN,
            state: 3,
            revision: 3,
            failure: 0,
            accepted: [i64::MIN, 19]
                .into_iter()
                .enumerate()
                .map(|(i, key)| p::SendReceipt {
                    idempotency_key: key,
                    message_id: Some(10 + i as i64),
                    album_id: Some(70),
                    reference_withheld: Some(false),
                })
                .collect(),
            retain_until: Some(prost_types::Timestamp {
                seconds: 2_000_000_000,
                nanos: 0,
            }),
        }
    }

    /// Unknown states, partial albums, absent disclosure decisions and terminal regressions fail closed.
    #[test]
    fn status_lifecycle_and_ordered_disclosure_are_strict() {
        let original = status();
        original.validate().unwrap();
        for bad_state in [0, 6, -1] {
            let mut bad = original.clone();
            bad.state = bad_state;
            assert!(bad.validate().is_err());
        }
        let mut bad = original.clone();
        bad.accepted[0].reference_withheld = None;
        assert!(bad.validate().is_err());
        bad = original.clone();
        bad.accepted[1].album_id = Some(71);
        assert!(bad.validate().is_err());
        let mut hidden = original.clone();
        for r in &mut hidden.accepted {
            r.message_id = None;
            r.album_id = None;
            r.reference_withheld = Some(true);
        }
        hidden.validate().unwrap();
        assert!(hidden.follows(&original));
        let mut remembered = hidden.clone();
        remembered.remember_disclosed(&original);
        assert_eq!(remembered, original);
        let mut changed = original.clone();
        changed.accepted[0].message_id = Some(90);
        assert!(!changed.follows(&remembered));
        changed = original.clone();
        changed.accepted.reverse();
        assert!(!changed.follows(&original));
        assert!(!send_bound(&changed, original.target, &[i64::MIN, 19]));
        changed = original.clone();
        changed.state = 2;
        changed.revision = 4;
        assert!(!changed.follows(&original));
        let pending = p::SendStatus {
            target: original.target,
            idempotency_key: i64::MIN,
            state: 1,
            revision: 1,
            ..Default::default()
        };
        pending.validate().unwrap();
        let processing = p::SendStatus {
            state: 2,
            revision: 2,
            ..pending.clone()
        };
        processing.validate().unwrap();
        assert!(processing.follows(&pending));
        assert!(original.follows(&processing));
        assert!(!pending.follows(&processing));
        let incomplete = p::SendStatus {
            accepted: vec![original.accepted[0]],
            ..original.clone()
        };
        assert!(!send_bound(&incomplete, original.target, &[i64::MIN, 19]));
    }

    /// Remembered IDs cannot be carried into a different domain key or asset selector.
    #[test]
    fn photo_and_asset_receipts_keep_immutable_keys_and_original_versions() {
        let original = p::ProfilePhotoUpdate {
            request_id: "11111111-1111-4111-8111-111111111111".into(),
            state: 3,
            revision: 2,
            photo_id: Some(7),
            reference_withheld: Some(false),
            gallery_revision: 8,
            retain_until: Some(prost_types::Timestamp {
                seconds: 2_000_000_000,
                nanos: 0,
            }),
            ..Default::default()
        };
        original.validate().unwrap();
        let mut bad = original.clone();
        bad.gallery_revision = 9;
        assert!(!bad.follows(&original));
        bad = original.clone();
        bad.photo_id = Some(8);
        assert!(!bad.follows(&original));
        let a = p::AssetChange {
            request_id: original.request_id.clone(),
            asset_id: Some(7),
            state: 3,
            revision: 2,
            accepted: Some(p::AssetReference {
                asset_id: 7,
                asset_version: 3,
            }),
            reference_withheld: Some(false),
            retain_until: original.retain_until,
            ..Default::default()
        };
        a.validate().unwrap();
        let mut b = a.clone();
        b.accepted.as_mut().unwrap().asset_id = 8;
        assert!(b.validate().is_err());
        b = a.clone();
        b.accepted.as_mut().unwrap().asset_version = 4;
        assert!(!b.follows(&a));
    }
}
