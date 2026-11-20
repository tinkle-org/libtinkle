//! Durable keyed sends and real native attachment preparation. No background mutation replay.
use crate::{Client, TinkleError};
use prost::Message as _;
use rand::RngCore as _;
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{atomic::Ordering, Arc},
};
use tinkle_schema::proto as p;
pub use tinkle_storage::{OutgoingJob, OutgoingJobSummary, OutgoingState};
#[cfg(tinkle_async_actions)]
#[path = "actions.rs"]
pub(crate) mod actions;

/// Bound admitted send/preparation work, including detached SQLite workers.
static SEND_WORKERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);
/// Network attempts hold admission for their entire original RPC budget.
static SEND_ATTEMPTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);

/// Release only this in-process flight; cancellation leaves durable dispatch state uncertain.
struct Flight {
    active: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    key: String,
}
impl Drop for Flight {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

/// An unfinished preparation waiter cancels its bounded blocking/source-copy work.
/// Successful completion disarms cleanup without cancelling a caller's shared token.
struct CancelOnDrop(Option<crate::CancelToken>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(token) = &self.0 {
            token.cancel();
        }
    }
}

/// An explicit retry never changes the stored bytes or allocates replacement operation keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendRetry {
    /// Dispatch only never-dispatched work; return uncertain work without transmitting.
    Never,
    /// Explicitly retry an uncertain keyed send unchanged (including interrupted dispatch).
    ExactKeyedRequest,
}

/// Normal authoring intent: raw documents or the native normalization recipe, never a decoder-only pass-through.
#[derive(Debug, Clone, Copy)]
pub enum AttachmentKind {
    Document,
    Native(crate::media::NativeMediaKind),
}
impl From<crate::media::MediaKind> for AttachmentKind {
    fn from(kind: crate::media::MediaKind) -> Self {
        use crate::media::{MediaKind as M, NativeMediaKind as N};
        match kind {
            M::Document => Self::Document,
            M::Image => Self::Native(N::Image),
            M::Video => Self::Native(N::Video),
            M::Voice(format) => Self::Native(N::Voice(format)),
        }
    }
}
impl From<crate::media::NativeMediaKind> for AttachmentKind {
    fn from(kind: crate::media::NativeMediaKind) -> Self {
        Self::Native(kind)
    }
}

/// Typed keyed operations supported by the durable coordinator, not arbitrary RPCs.
#[derive(Debug, Clone)]
// Preserve existing value constructors; candidate metadata adds a fixed few hundred stack bytes.
#[cfg_attr(tinkle_async_actions, allow(clippy::large_enum_variant))]
pub enum OutgoingRequest {
    Text(p::SendMessageRequest),
    Media(p::SendMediaRequest),
    Album(p::SendAlbumRequest),
}

/// Sanitized coordinator failures. An RPC failure does not imply rollback or authorize replay.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("native SQLite cache required for durable sending")]
    NoStorage,
    #[error("a current principal is required")]
    NoPrincipal,
    #[error("sending scope changed")]
    ScopeChanged,
    #[error("sending is busy")]
    Busy,
    #[error("outgoing job not found")]
    NotFound,
    #[error("outgoing job changed; reload it")]
    Conflict,
    #[error("outgoing storage failed")]
    Storage,
    #[error("invalid outgoing request")]
    InvalidRequest,
    #[error("secure operation key generation failed")]
    Random,
    #[error(transparent)]
    Media(#[from] crate::media::MediaError),
    #[error(transparent)]
    Rpc(#[from] TinkleError),
}

/// Completed immutable file IDs and media source. No full-size bytes cross the native boundary.
#[derive(Debug, Clone)]
pub struct PreparedAttachment {
    pub file: p::FileInfo,
    pub thumbnail: Option<p::FileInfo>,
    pub source: p::MediaSource,
    pub source_sha256: [u8; 32],
    pub output_sha256: [u8; 32],
    pub preparation_profile: String,
    /// Retained display-only JPEG, not yet represented by the pinned public MediaSource.
    pub tiny_preview: Option<Vec<u8>>,
}

/// Frozen preparation handoff. Artifacts and the exact source declaration survive
/// restart until principal cleanup. Reusing its key never reruns a codec after publication.
#[derive(Debug, Clone)]
pub struct AttachmentManifest {
    pub input_path: String,
    pub settings: String,
    pub artifact_path: String,
    pub source_sha256: [u8; 32],
    pub output_sha256: [u8; 32],
    pub profile: String,
    pub mime_type: String,
    /// MediaSource protobuf with a placeholder file ID, replaced only by completed upload identity.
    pub media_source: Vec<u8>,
    /// Exact native measurements captured with this artifact, absent for raw documents/older manifests.
    pub native_facts: Option<crate::media::NativeMediaOutput>,
    /// Exact JPEG display bytes; absent in older preparations and nonvisual media.
    pub tiny_preview: Option<Vec<u8>>,
    /// Separate immutable regular PNG artifact; its upload never changes the main upload key.
    pub thumbnail: Option<AttachmentThumbnail>,
}

/// Bounded regular-thumbnail upload input retained in the immutable parent preparation.
#[derive(Debug, Clone)]
pub struct AttachmentThumbnail {
    /// SDK-owned private PNG file, separate from the main path.
    pub artifact_path: String,
    /// Digest of the exact retained upload bytes.
    pub sha256: [u8; 32],
    pub width: u32,
    pub height: u32,
}

impl AttachmentManifest {
    /// Bounded canonical manifest bytes are part of the existing immutable transfer intent.
    fn encode(&self) -> Result<String, SendError> {
        let value = serde_json::json!({"version":1,"input":self.input_path,"settings":self.settings,
            "artifact":self.artifact_path,"source_sha256":self.source_sha256.as_slice(),
            "output_sha256":self.output_sha256.as_slice(),"profile":self.profile,
            "mime":self.mime_type,"media":self.media_source,
            "facts":self.native_facts.as_ref().map(|f|serde_json::json!({"width":f.width,"height":f.height,
                "frames":f.frame_count,"duration_ms":f.duration_ms,"channels":f.audio_channels,
                "sample_rate":f.audio_sample_rate,"samples":f.audio_samples,"waveform":f.waveform})),
            "preview":self.tiny_preview.as_ref().map(|bytes| bytes.iter().map(|b|format!("{b:02x}")).collect::<String>()),
            "thumbnail":self.thumbnail.as_ref().map(|t|serde_json::json!({"artifact":t.artifact_path,
                "sha256":t.sha256.as_slice(),"width":t.width,"height":t.height}))});
        #[cfg(tinkle_schema_candidate)]
        let mut value = value;
        #[cfg(tinkle_schema_candidate)]
        {
            value["version"] = serde_json::json!(5);
            value["wire_identity"] = serde_json::json!(env!("TINKLE_SCHEMA_WIRE_ID"));
        }
        #[cfg(tinkle_async_actions)]
        if self
            .settings
            .ends_with(":singular-thumbnail-compact-wire-reset-v1")
        {
            let mut source = p::MediaSource::decode(self.media_source.as_slice())
                .map_err(|_| SendError::Storage)?;
            if source.encode_to_vec() != self.media_source {
                return Err(SendError::Storage);
            }
            let preview = source_preview(&mut source).and_then(Option::take);
            if let Some(preview) = &preview {
                if self.tiny_preview.as_deref() != Some(preview.jpeg.as_ref())
                    || crate::media::validate_tiny_preview(&preview.jpeg)?
                        != (preview.width, preview.height)
                {
                    return Err(SendError::Storage);
                }
            } else if self.tiny_preview.is_some() {
                return Err(SendError::Storage);
            }
            // Keep one copy of JPEG bytes inside the unchanged 16-KiB immutable-intent budget.
            value["media_contract"] = serde_json::json!("singular-thumbnail-compact-wire-reset-v1");
            value["media"] = serde_json::json!(source.encode_to_vec());
            value["preview_in_media"] = serde_json::json!(preview.is_some());
            value["media_hash"] =
                serde_json::json!(format!("{:x}", Sha256::digest(&self.media_source)));
        }
        Ok(value.to_string())
    }

    /// Validate persisted state before any path or protobuf is used.
    fn decode(value: &str) -> Result<Self, SendError> {
        let value: serde_json::Value =
            serde_json::from_str(value).map_err(|_| SendError::Storage)?;
        let text = |key| {
            value[key]
                .as_str()
                .map(str::to_owned)
                .ok_or(SendError::Storage)
        };
        let bytes = |key| {
            serde_json::from_value::<Vec<u8>>(value[key].clone()).map_err(|_| SendError::Storage)
        };
        if value["version"] != 1 && value["version"] != 4 && value["version"] != 5 {
            return Err(SendError::Storage);
        }
        #[cfg(tinkle_schema_candidate)]
        if value["version"] != 5 || value["wire_identity"] != env!("TINKLE_SCHEMA_WIRE_ID") {
            return Err(SendError::Storage);
        }
        #[cfg(tinkle_async_actions)]
        if value["media_contract"] != "singular-thumbnail-compact-wire-reset-v1"
            || !text("settings")?.ends_with(":singular-thumbnail-compact-wire-reset-v1")
        {
            return Err(SendError::Storage);
        }
        #[cfg(not(tinkle_schema_candidate))]
        if value["version"] != 1 {
            return Err(SendError::Media(crate::media::MediaError::Unsupported));
        }
        let tiny_preview = if value["preview"].is_null() {
            None
        } else {
            let hex = value["preview"].as_str().ok_or(SendError::Storage)?;
            if hex.is_empty()
                || hex.len() > crate::media::TINY_PREVIEW_BYTES * 2
                || !hex.len().is_multiple_of(2)
                || !hex.is_ascii()
            {
                return Err(SendError::Storage);
            }
            Some(
                (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| SendError::Storage))
                    .collect::<Result<Vec<_>, _>>()?,
            )
        };
        let thumbnail = if value["thumbnail"].is_null() {
            None
        } else {
            let t = &value["thumbnail"];
            let axis = |key| {
                t[key]
                    .as_u64()
                    .filter(|v| (1..=320).contains(v))
                    .map(|v| v as u32)
                    .ok_or(SendError::Storage)
            };
            Some(AttachmentThumbnail {
                artifact_path: t["artifact"].as_str().ok_or(SendError::Storage)?.into(),
                sha256: serde_json::from_value::<Vec<u8>>(t["sha256"].clone())
                    .map_err(|_| SendError::Storage)?
                    .try_into()
                    .map_err(|_| SendError::Storage)?,
                width: axis("width")?,
                height: axis("height")?,
            })
        };
        let manifest = Self {
            tiny_preview,
            thumbnail,
            input_path: text("input")?,
            settings: text("settings")?,
            artifact_path: text("artifact")?,
            source_sha256: bytes("source_sha256")?
                .try_into()
                .map_err(|_| SendError::Storage)?,
            output_sha256: bytes("output_sha256")?
                .try_into()
                .map_err(|_| SendError::Storage)?,
            profile: text("profile")?,
            mime_type: text("mime")?,
            media_source: bytes("media")?,
            native_facts: if value["facts"].is_object() {
                let f = &value["facts"];
                let number = |key| f[key].as_u64().ok_or(SendError::Storage);
                Some(crate::media::NativeMediaOutput {
                    width: number("width")?
                        .try_into()
                        .map_err(|_| SendError::Storage)?,
                    height: number("height")?
                        .try_into()
                        .map_err(|_| SendError::Storage)?,
                    frame_count: number("frames")?
                        .try_into()
                        .map_err(|_| SendError::Storage)?,
                    duration_ms: number("duration_ms")?,
                    audio_channels: number("channels")?
                        .try_into()
                        .map_err(|_| SendError::Storage)?,
                    audio_sample_rate: number("sample_rate")?
                        .try_into()
                        .map_err(|_| SendError::Storage)?,
                    audio_samples: number("samples")?,
                    waveform: serde_json::from_value(f["waveform"].clone())
                        .map_err(|_| SendError::Storage)?,
                    thumbnail_frame: None,
                })
            } else {
                None
            },
        };
        #[cfg(tinkle_async_actions)]
        let mut manifest = manifest;
        #[cfg(tinkle_async_actions)]
        if value["version"] == 5 {
            let mut source = p::MediaSource::decode(manifest.media_source.as_slice())
                .map_err(|_| SendError::Storage)?;
            let present = value["preview_in_media"]
                .as_bool()
                .ok_or(SendError::Storage)?;
            if present {
                let jpeg = manifest.tiny_preview.as_ref().ok_or(SendError::Storage)?;
                let (width, height) = crate::media::validate_tiny_preview(jpeg)?;
                *source_preview(&mut source).ok_or(SendError::Storage)? = Some(p::InlinePreview {
                    width,
                    height,
                    jpeg: jpeg.clone().into(),
                });
            } else if manifest.tiny_preview.is_some() {
                return Err(SendError::Storage);
            }
            let media = source.encode_to_vec();
            if value["media_hash"].as_str()
                != Some(format!("{:x}", Sha256::digest(&media)).as_str())
            {
                return Err(SendError::Storage);
            }
            manifest.media_source = media;
        }
        manifest.validate_display()?;
        Ok(manifest)
    }

    /// New optional display state is bounded independently; older absent fields remain absent.
    fn validate_display(&self) -> Result<(), SendError> {
        if let Some(preview) = &self.tiny_preview {
            let (width, height) =
                crate::media::validate_tiny_preview(preview).map_err(|_| SendError::Storage)?;
            if self
                .native_facts
                .as_ref()
                .is_none_or(|f| width > f.width || height > f.height)
            {
                return Err(SendError::Storage);
            }
        }
        if let Some(thumbnail) = &self.thumbnail {
            if !(1..=320).contains(&thumbnail.width)
                || !(1..=320).contains(&thumbnail.height)
                || self
                    .native_facts
                    .as_ref()
                    .is_none_or(|f| thumbnail.width > f.width || thumbnail.height > f.height)
            {
                return Err(SendError::Storage);
            }
        }
        Ok(())
    }
}

/// Supported source preview slot; versioned manifests reconstruct protobufs, never image pixels.
#[cfg(tinkle_async_actions)]
fn source_preview(source: &mut p::MediaSource) -> Option<&mut Option<p::InlinePreview>> {
    match source.media.as_mut()? {
        p::media_source::Media::Photo(v) => Some(&mut v.inline_preview),
        p::media_source::Media::Document(v) => Some(&mut v.inline_preview),
        p::media_source::Media::Video(v) => Some(&mut v.inline_preview),
        p::media_source::Media::Audio(v) => Some(&mut v.inline_preview),
        p::media_source::Media::Sticker(v) => Some(&mut v.inline_preview),
        _ => None,
    }
}

/// Parent identity includes provisioned trust and the caller's immutable preparation key.
fn preparation_job(source: &str, key: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(source);
    hash.update(key);
    format!("prepare:{:x}", hash.finalize())
}

/// Borrowed coordinator; the Client owns authorization, cache and transport lifetimes.
pub struct Sending<'a> {
    client: &'a Client,
}

/// Account/source ABA fence, independent of reconnects but not trust/enrollment replacement.
pub(crate) struct Scope {
    client: Client,
    authority: crate::authorization::EnrollmentEpoch,
    principal: String,
    source: String,
    epoch: u64,
    revision: u64,
    storage: Arc<tinkle_storage::Storage>,
    incarnation: u64,
    #[cfg(tinkle_async_actions)]
    generation: Option<Arc<crate::liveness::LiveGeneration>>,
}

impl Scope {
    /// Capture trust from provisioned state, never from an endpoint or response.
    pub(crate) async fn capture(client: &Client) -> Result<Arc<Self>, SendError> {
        let authority = client.authorization.generation()?;
        let epoch = client.reconcile_epoch();
        let revision = client.retirement.revision();
        let principal = client.principal().await;
        if principal == crate::sync::UNKNOWN_PRINCIPAL {
            return Err(SendError::NoPrincipal);
        }
        let source = {
            let source = client.trust_source.read().await;
            let trust = match &*source {
                crate::TrustSource::Bootstrap(t)
                | crate::TrustSource::SignedOut(t)
                | crate::TrustSource::Returning { trust: t, .. } => t,
            };
            let mut hash = Sha256::new();
            hash.update(trust.server_public_key);
            hash.update(trust.server_key_id.to_be_bytes());
            hash.update(trust.scope.as_bytes());
            format!("{:x}", hash.finalize())
        };
        let storage = client.sqlite_storage().ok_or(SendError::NoStorage)?;
        let db = storage.clone();
        let owner = principal.clone();
        let permit = SEND_WORKERS.try_acquire().map_err(|_| SendError::Busy)?;
        let incarnation = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            db.principal_cache_incarnation(&owner)
        })
        .await
        .map_err(|_| SendError::Storage)?
        .map_err(|_| SendError::Storage)?;
        let scope = Arc::new(Self {
            client: client.background_clone(),
            authority,
            principal,
            source,
            epoch,
            revision,
            storage,
            incarnation,
            #[cfg(tinkle_async_actions)]
            generation: client.bound_send_generation(),
        });
        scope.check()?;
        Ok(scope)
    }

    /// Synchronous fence checks bracket every retained result and worker publication.
    pub(crate) fn check(&self) -> Result<(), SendError> {
        self.authority
            .valid()
            .map_err(|_| SendError::ScopeChanged)?;
        if self.client.reconcile_epoch() != self.epoch
            || self.client.retirement.revision() != self.revision
            || self.client.owner_gone().is_cancelled()
        {
            return Err(SendError::ScopeChanged);
        }
        #[cfg(tinkle_async_actions)]
        if self
            .generation
            .as_ref()
            .is_some_and(|g| !g.open.load(Ordering::SeqCst) || g.source_revision != self.revision)
        {
            return Err(SendError::ScopeChanged);
        }
        Ok(())
    }

    /// Pin both request and commit to the socket that supplied the outcome.
    #[cfg(tinkle_async_actions)]
    fn with_generation(&self, generation: Arc<crate::liveness::LiveGeneration>) -> Arc<Self> {
        Arc::new(Self {
            client: self.client.clone(),
            authority: self.authority.duplicate(),
            principal: self.principal.clone(),
            source: self.source.clone(),
            epoch: self.epoch,
            revision: self.revision,
            storage: self.storage.clone(),
            incarnation: self.incarnation,
            generation: Some(generation),
        })
    }

    /// Blocking work owns both lifecycle gates and its admission until actual completion,
    /// even if its waiter is dropped. No SQLite lock is held across an await.
    pub(crate) async fn store<T: Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(&tinkle_storage::Storage, &str, &str) -> Result<T, tinkle_storage::StorageError>
            + Send
            + 'static,
    ) -> Result<T, SendError> {
        self.check()?;
        let permit = SEND_WORKERS.try_acquire().map_err(|_| SendError::Busy)?;
        let scope = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            tokio::runtime::Handle::current().block_on(async {
                let _connection = scope.client.connect_lock.lock().await;
                let _commit = scope.client.checkpoint_save.lock().await;
                scope.check()?;
                // A switch bumps the epoch before its awaited principal write. A capture can
                // otherwise pair that new epoch with the old account; compare the actual owner
                // under the same connect/checkpoint exclusion as the switch, retaining ABA checks.
                if scope.client.principal().await != scope.principal {
                    return Err(SendError::ScopeChanged);
                }
                if scope
                    .storage
                    .principal_cache_incarnation(&scope.principal)
                    .map_err(|_| SendError::Storage)?
                    != scope.incarnation
                {
                    return Err(SendError::ScopeChanged);
                }
                f(&scope.storage, &scope.principal, &scope.source).map_err(|error| match error {
                    tinkle_storage::StorageError::IntentConflict => SendError::Conflict,
                    tinkle_storage::StorageError::CacheScopeChanged
                    | tinkle_storage::StorageError::GenerationMismatch { .. } => {
                        SendError::ScopeChanged
                    }
                    _ => SendError::Storage,
                })
            })
        })
        .await
        .map_err(|_| SendError::Storage)??;
        self.check()?;
        Ok(result)
    }

    /// A request caller fenced to this captured account/source incarnation.
    #[cfg(tinkle_async_actions)]
    pub(crate) fn caller(&self) -> Client {
        let caller = self.client.for_application_scope(self.epoch, self.revision);
        match &self.generation {
            Some(g) => caller.for_session_generation(g.clone()),
            None => caller,
        }
    }

    /// Preserve payload identity and use revision CAS for cross-client/process admission.
    async fn transition(
        self: &Arc<Self>,
        job: &mut OutgoingJob,
        state: OutgoingState,
        response: Vec<u8>,
        outcome: Option<tinkle_storage::OutgoingOutcome>,
    ) -> Result<(), SendError> {
        self.transition_inner(job, state, response, outcome, None)
            .await
    }

    /// Status receipts and private remembered IDs commit atomically under the same lifecycle gates.
    async fn transition_inner(
        self: &Arc<Self>,
        job: &mut OutgoingJob,
        state: OutgoingState,
        response: Vec<u8>,
        outcome: Option<tinkle_storage::OutgoingOutcome>,
        evidence: Option<Vec<u8>>,
    ) -> Result<(), SendError> {
        #[cfg(not(tinkle_async_actions))]
        let _ = &evidence;
        let old = job.clone();
        let retained = response.clone();
        let retained_outcome = outcome.clone();
        if !self
            .store(move |s, p, _| {
                #[cfg(tinkle_async_actions)]
                if old.state as i64 >= 14 {
                    if let Some(evidence) = &evidence {
                        return s.transition_outgoing_action_with_evidence(
                            p, &old, state, &retained, evidence,
                        );
                    }
                    return s.transition_outgoing_action(
                        p,
                        &old,
                        state,
                        &retained,
                        retained_outcome.as_ref(),
                    );
                }
                s.transition_outgoing_with_outcome(
                    p,
                    &old,
                    state,
                    &retained,
                    retained_outcome.as_ref(),
                )
            })
            .await?
        {
            return Err(SendError::Conflict);
        }
        job.state = state;
        job.response = response;
        job.last_outcome = outcome;
        job.revision += 1;
        Ok(())
    }
}

impl OutgoingRequest {
    /// Validate all keyed send shapes before freezing exact application payload bytes.
    fn encode(&self) -> Result<(&'static str, p::Target, Vec<u8>), SendError> {
        use crate::services::{validate_send, validate_send_media, validate_target};
        let size = match self {
            Self::Text(r) => r.encoded_len(),
            Self::Media(r) => r.encoded_len(),
            Self::Album(r) => r.encoded_len(),
        };
        if size > 262144 {
            return Err(SendError::InvalidRequest);
        }
        let (method, target, payload) = match self {
            Self::Text(r) => {
                validate_send(r)?;
                (
                    "/tinkle.v1.MessagesService/SendMessage",
                    r.target,
                    r.encode_to_vec(),
                )
            }
            Self::Media(r) => {
                validate_send_media(r)?;
                (
                    "/tinkle.v1.MessagesService/SendMedia",
                    r.target,
                    r.encode_to_vec(),
                )
            }
            Self::Album(r) => {
                crate::services::validate_draft_clear(
                    r.clear_draft,
                    r.clear_draft_dialog_id,
                    r.clear_draft_revision,
                )?;
                validate_target(r.target.as_ref())?;
                if !(2..=10).contains(&r.items.len()) {
                    return Err(SendError::InvalidRequest);
                }
                let mut keys = std::collections::HashSet::new();
                for item in &r.items {
                    if !keys.insert(item.idempotency_key)
                        || !matches!(
                            item.media.as_ref().and_then(|m| m.media.as_ref()),
                            Some(
                                p::media_source::Media::Photo(_)
                                    | p::media_source::Media::Video(_)
                                    | p::media_source::Media::Document(_)
                            )
                        )
                    {
                        return Err(SendError::InvalidRequest);
                    }
                    validate_send_media(&p::SendMediaRequest {
                        target: r.target,
                        media: item.media.clone(),
                        message: Some(item.message.clone()),
                        entities: item.entities.clone(),
                        idempotency_key: item.idempotency_key,
                        reply_to: r.reply_to.clone(),
                        ..Default::default()
                    })?;
                }
                (
                    "/tinkle.v1.MessagesService/SendAlbum",
                    r.target,
                    r.encode_to_vec(),
                )
            }
        };
        #[cfg(tinkle_async_actions)]
        actions::validate_send_sources(method, &payload)?;
        if payload.len() > 262144 {
            return Err(SendError::InvalidRequest);
        }
        Ok((method, target.ok_or(SendError::InvalidRequest)?, payload))
    }

    /// Fill only omitted random IDs. Re-enqueue uses the original retained keys, never fresh ones.
    fn keys(&mut self, old: Option<&OutgoingJob>) -> Result<(), SendError> {
        let previous = old
            .map(|job| match job.method.as_str() {
                "/tinkle.v1.MessagesService/SendMessage" => {
                    p::SendMessageRequest::decode(job.payload.as_slice()).map(Self::Text)
                }
                "/tinkle.v1.MessagesService/SendMedia" => {
                    p::SendMediaRequest::decode(job.payload.as_slice()).map(Self::Media)
                }
                _ => p::SendAlbumRequest::decode(job.payload.as_slice()).map(Self::Album),
            })
            .transpose()
            .map_err(|_| SendError::Storage)?;
        match self {
            Self::Text(r) if r.idempotency_key == 0 => {
                r.idempotency_key = match previous {
                    Some(Self::Text(p)) => p.idempotency_key,
                    _ => idempotency_key()?,
                }
            }
            Self::Media(r) if r.idempotency_key == 0 => {
                r.idempotency_key = match previous {
                    Some(Self::Media(p)) => p.idempotency_key,
                    _ => idempotency_key()?,
                }
            }
            Self::Album(r) => {
                for (index, item) in r.items.iter_mut().enumerate() {
                    if item.idempotency_key == 0 {
                        item.idempotency_key = match &previous {
                            Some(Self::Album(p)) => {
                                p.items
                                    .get(index)
                                    .ok_or(SendError::Conflict)?
                                    .idempotency_key
                            }
                            _ => idempotency_key()?,
                        };
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// OS randomness failure is an error, never a predictable replacement key.
fn idempotency_key() -> Result<i64, SendError> {
    for _ in 0..8 {
        let mut bytes = [0; 8];
        rand::rngs::OsRng
            .try_fill_bytes(&mut bytes)
            .map_err(|_| SendError::Random)?;
        let id = i64::from_le_bytes(bytes);
        if id != 0 {
            return Ok(id);
        }
    }
    Err(SendError::Random)
}

impl<'a> Sending<'a> {
    /// Bind without spawning or retaining the borrowed Client.
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Freeze the exact typed request and stable operation keys before the first transmission.
    /// A repeated job key is create-only and must describe the identical request.
    pub async fn enqueue(
        &self,
        job_id: String,
        mut request: OutgoingRequest,
    ) -> Result<OutgoingJob, SendError> {
        let scope = Scope::capture(self.client).await?;
        let key = job_id.clone();
        let old = scope
            .store(move |s, p, source| s.outgoing(p, source, &key))
            .await?;
        request.keys(old.as_ref())?;
        let (method, target, payload) = request.encode()?;
        let target_key = tinkle_schema::sync::target_key_for(
            target.kind.as_ref().ok_or(SendError::InvalidRequest)?,
        );
        let stream = target_key.clone();
        let cache_scope = scope
            .store(move |s, p, _| s.capture_message_scope(p, &stream))
            .await?;
        let state = OutgoingState::Prepared;
        #[cfg(tinkle_async_actions)]
        let state = if actions::is_send(method, &payload)? {
            OutgoingState::ActionPrepared
        } else {
            state
        };
        let job = OutgoingJob {
            job_id: job_id.clone(),
            source: scope.source.clone(),
            target_key,
            method: method.into(),
            payload,
            state,
            response: Vec::new(),
            last_outcome: None,
            revision: 0,
            cache_generation: cache_scope.generation(),
            cache_incarnation: cache_scope.incarnation(),
        };
        let insert = job.clone();
        scope
            .store(move |s, p, _| s.insert_outgoing(p, &insert))
            .await?;
        scope
            .store(move |s, p, source| s.outgoing(p, source, &job_id))
            .await?
            .ok_or(SendError::NotFound)
    }

    /// Inspect retained jobs offline. No reconnect or request replay is initiated.
    pub async fn jobs(&self) -> Result<Vec<OutgoingJobSummary>, SendError> {
        Scope::capture(self.client)
            .await?
            .store(|s, p, source| s.list_outgoing(p, source))
            .await
    }

    /// Read one exact retained request/result without transmitting it.
    pub async fn get(&self, job_id: String) -> Result<OutgoingJob, SendError> {
        Scope::capture(self.client)
            .await?
            .store(move |s, p, source| s.outgoing(p, source, &job_id))
            .await?
            .ok_or(SendError::NotFound)
    }

    /// Explicitly discard only prepared/settled work, never uncertain dispatches.
    pub async fn discard(&self, job_id: String) -> Result<bool, SendError> {
        Scope::capture(self.client)
            .await?
            .store(move |s, p, source| s.discard_outgoing(p, source, &job_id))
            .await
    }

    /// One attempt on the captured connection. Never dials, retries or rekeys internally.
    /// Accepted responses are retained before cache hydration; hydration failure cannot resend.
    /// Cancellation/dropped waiters leave Dispatching/Unconfirmed, never Prepared.
    /// Asynchronous jobs query the original domain keys after submission.
    pub async fn resume(
        &self,
        job_id: String,
        retry: SendRetry,
        cancel: Option<crate::CancelToken>,
    ) -> Result<OutgoingJob, SendError> {
        let _admission = SEND_ATTEMPTS.try_acquire().map_err(|_| SendError::Busy)?;
        let scope = Scope::capture(self.client).await?;
        let key = format!("{}:{}:{job_id}", scope.principal, scope.source);
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
        let mut job = scope
            .store(move |s, p, source| s.outgoing(p, source, &job_id))
            .await?
            .ok_or(SendError::NotFound)?;
        #[cfg(tinkle_async_actions)]
        if job.state as i64 >= 14 {
            return self.resume_action(&scope, job, cancel).await;
        }
        #[cfg(tinkle_async_actions)]
        if actions::is_send(&job.method, &job.payload)? {
            return Err(SendError::Conflict);
        }
        if matches!(
            job.state,
            OutgoingState::ActionPending
                | OutgoingState::ActionProcessing
                | OutgoingState::ActionSucceeded
                | OutgoingState::ActionFailed
                | OutgoingState::ActionCanceled
                | OutgoingState::ActionPrepared
                | OutgoingState::ActionDispatching
                | OutgoingState::ActionUnconfirmed
        ) {
            return Err(SendError::Conflict);
        }
        if job.state == OutgoingState::Accepted {
            self.hydrate(&scope, &job).await?;
            return Ok(job);
        }
        if job.state == OutgoingState::AcceptedPending {
            return Ok(job);
        }
        if job.state == OutgoingState::Rejected
            || (job.state != OutgoingState::Prepared && retry == SendRetry::Never)
        {
            return Ok(job);
        }
        let token = cancel.unwrap_or_default();
        if token.is_cancelled() {
            return Ok(job);
        }
        let generation = self
            .client
            .session_state
            .generation()
            .filter(|g| g.open.load(Ordering::SeqCst))
            .ok_or_else(|| {
                SendError::Rpc(TinkleError::NotConnected("connect before sending".into()))
            })?;
        let caller = self
            .client
            .for_application_scope(scope.epoch, scope.revision)
            .for_session_generation(generation);
        scope
            .transition(&mut job, OutgoingState::Dispatching, vec![], None)
            .await?;
        scope.check()?;
        let result = caller
            .call_raw_cancellable(&job.method, job.payload.clone(), 30_000, &token)
            .await;
        match result {
            Ok(response) => {
                // Malformed/mismatched responses leave execution uncertain, not safe to replay.
                let (messages, _, _) = match accepted_messages(&job, &response) {
                    Ok(value) => value,
                    Err(error) => {
                        scope
                            .transition(
                                &mut job,
                                OutgoingState::Unconfirmed,
                                vec![],
                                Some(tinkle_storage::OutgoingOutcome {
                                    code: syntra_core::ErrorCode::DataLoss as i32,
                                    raw_code: None,
                                    unknown_outcome: true,
                                }),
                            )
                            .await?;
                        return Err(error);
                    }
                };
                let pending = messages
                    .iter()
                    .all(|m| m.scheduled_state == p::MessageScheduledState::Pending as i32);
                scope
                    .transition(
                        &mut job,
                        if pending {
                            OutgoingState::AcceptedPending
                        } else {
                            OutgoingState::Accepted
                        },
                        response,
                        None,
                    )
                    .await?;
                if !pending {
                    self.hydrate(&scope, &job).await?;
                }
                Ok(job)
            }
            Err(error) => {
                let (state, outcome) = failure_outcome(&error);
                scope
                    .transition(&mut job, state, vec![], Some(outcome))
                    .await?;
                Err(SendError::Rpc(error))
            }
        }
    }

    /// Same-cursor hydration preserves journal progress and existing body freshness ordering.
    async fn hydrate(&self, scope: &Arc<Scope>, job: &OutgoingJob) -> Result<(), SendError> {
        scope.check()?;
        if !matches!(
            job.method.as_str(),
            "/tinkle.v1.MessagesService/SendMessage"
                | "/tinkle.v1.MessagesService/SendMedia"
                | "/tinkle.v1.MessagesService/SendAlbum"
        ) {
            return Ok(());
        }
        let (messages, users, chats) = accepted_messages(job, &job.response)?;
        // Pending scheduled acknowledgements have no acceptance position and are not history.
        let messages: Vec<_> = messages
            .into_iter()
            .filter(|m| m.scheduled_state != p::MessageScheduledState::Pending as i32)
            .collect();
        let mut ops = crate::cache_projector::project_messages_to_ops(
            &job.target_key,
            &messages,
            &Default::default(),
        )
        .map_err(|_| SendError::InvalidRequest)?;
        crate::cache_projector::merge_ops(
            &mut ops,
            crate::cache_projector::project_entities_to_ops(&users, &chats),
        )
        .map_err(|_| SendError::InvalidRequest)?;
        crate::cache_projector::rebase_file_origins(&mut ops, &job.target_key);
        let job = job.clone();
        scope
            .store(move |s, p, _| s.hydrate_outgoing(p, &job, &ops))
            .await?;
        scope.check()
    }

    /// Normal sending path: raw documents, otherwise the installed native encoder. The durable
    /// manifest always precedes upload. A retained manifest needs no installed encoder on restart.
    pub async fn prepare_attachment(
        &self,
        key: String,
        path: &Path,
        kind: impl Into<AttachmentKind>,
        cancel: Option<crate::CancelToken>,
    ) -> Result<PreparedAttachment, SendError> {
        self.prepare_attachment_mode(key, path, kind.into(), cancel, cfg!(tinkle_async_actions))
            .await
    }

    /// One retained recipe path; source capability is fixed before upload submission.
    async fn prepare_attachment_mode(
        &self,
        key: String,
        path: &Path,
        kind: AttachmentKind,
        cancel: Option<crate::CancelToken>,
        async_actions: bool,
    ) -> Result<PreparedAttachment, SendError> {
        if let AttachmentKind::Native(kind) = kind {
            return self
                .prepare_native_attachment_inner(key, path, kind, None, cancel, async_actions)
                .await;
        }
        let kind = crate::media::MediaKind::Document;
        let _admission = SEND_ATTEMPTS.try_acquire().map_err(|_| SendError::Busy)?;
        let cancel = cancel.unwrap_or_default();
        let mut cancel_on_drop = CancelOnDrop(Some(cancel.clone()));
        let scope = Scope::capture(self.client).await?;
        if key.is_empty() || key.len() > 200 {
            return Err(SendError::InvalidRequest);
        }
        let input = std::path::absolute(path).map_err(|_| SendError::InvalidRequest)?;
        let input = input.to_str().ok_or(SendError::InvalidRequest)?.to_owned();
        let settings = format!(
            "main-only-v1:{kind:?}{}",
            if async_actions {
                ":singular-thumbnail-compact-wire-reset-v1"
            } else if cfg!(tinkle_schema_candidate) {
                ":compact-wire-reset-v1"
            } else {
                ""
            }
        );
        let id = preparation_job(&scope.source, &key);
        let read_id = id.clone();
        let retained = scope
            .store(move |s, p, _| {
                s.file_transfer_intent(p, &read_id).map_err(|_| {
                    tinkle_storage::StorageError::Unavailable("preparation unavailable".into())
                })
            })
            .await?;
        if let Some(intent) = retained {
            let manifest = AttachmentManifest::decode(&intent.job.session_json)?;
            if manifest.input_path != input
                || manifest.settings != settings
                || intent.job.file_scope != scope.source
            {
                return Err(SendError::Conflict);
            }
            let result = self
                .upload_prepared(&scope, &key, intent, manifest, cancel.clone())
                .await?;
            cancel_on_drop.0 = None;
            return Ok(result);
        }
        let scratch_root = scope.store(|s, _, _| s.temporary_directory()).await?;
        let prepared =
            crate::media::prepare_path_in(path, &scratch_root, kind, Some(cancel.clone())).await?;
        scope.check()?;
        let media = p::media_source::Media::Document(p::DocumentSource {
            file_id: 0,
            attributes: vec![],
            #[cfg(not(tinkle_async_actions))]
            variants: vec![],
            #[cfg(tinkle_async_actions)]
            thumbnail: None,
            #[cfg(tinkle_async_actions)]
            inline_preview: None,
        });
        let manifest = AttachmentManifest {
            input_path: input,
            settings,
            artifact_path: String::new(),
            source_sha256: prepared.source_sha256,
            output_sha256: prepared.output_sha256,
            profile: prepared.profile.into(),
            mime_type: prepared.mime_type.into(),
            media_source: p::MediaSource { media: Some(media) }.encode_to_vec(),
            native_facts: None,
            tiny_preview: None,
            thumbnail: None,
        };
        let (intent, manifest) = Self::persist_preparation(&scope, id, manifest, prepared).await?;
        let result = self
            .upload_prepared(&scope, &key, intent, manifest, cancel.clone())
            .await?;
        cancel_on_drop.0 = None;
        Ok(result)
    }

    /// Share owned artifact publication between local and native preparation profiles.
    async fn persist_preparation(
        scope: &Arc<Scope>,
        id: String,
        manifest: AttachmentManifest,
        prepared: crate::media::PreparedMediaPath,
    ) -> Result<
        (
            tinkle_storage::transfer_intent::TransferIntentCheckpoint,
            AttachmentManifest,
        ),
        SendError,
    > {
        manifest.validate_display()?;
        // File publication and parent registration share lifecycle gates. A crash before the
        // SQLite claim leaves only an owned namespace artifact, removed by principal cleanup.
        let incarnation = scope.incarnation;
        scope
            .store(move |s, p, source| {
                let root = s.preparation_directory(p)?;
                std::fs::create_dir_all(&root).map_err(|_| {
                    tinkle_storage::StorageError::Unavailable(
                        "preparation directory unavailable".into(),
                    )
                })?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt as _;
                    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
                        .map_err(|_| {
                            tinkle_storage::StorageError::Unavailable(
                                "preparation directory unavailable".into(),
                            )
                        })?;
                }
                // Native readers may require a container suffix even after a successful encode.
                // This stable SDK-owned name does not change the encoded bytes or upload identity.
                let suffix = match manifest.mime_type.as_str() {
                    "audio/mp4" | "video/mp4" => ".mp4",
                    "image/png" => ".png",
                    "image/webp" => ".webp",
                    _ => ".bin",
                };
                let artifact = tempfile::Builder::new()
                    .prefix("main-")
                    .suffix(suffix)
                    .tempfile_in(&root)
                    .map_err(|_| {
                        tinkle_storage::StorageError::Unavailable("artifact unavailable".into())
                    })?;
                std::fs::copy(prepared.path(), artifact.path()).map_err(|_| {
                    tinkle_storage::StorageError::Unavailable("artifact copy failed".into())
                })?;
                // Verify the adopted copy, not a pathname inspected before the copy. Never
                // publish a manifest if an externally modified artifact no longer matches it.
                use std::io::Read as _;
                let mut copied = std::fs::File::open(artifact.path()).map_err(|_| {
                    tinkle_storage::StorageError::Unavailable("artifact verification failed".into())
                })?;
                let mut hash = Sha256::new();
                let mut buffer = [0u8; 65536];
                loop {
                    let n = copied.read(&mut buffer).map_err(|_| {
                        tinkle_storage::StorageError::Unavailable(
                            "artifact verification failed".into(),
                        )
                    })?;
                    if n == 0 {
                        break;
                    }
                    hash.update(&buffer[..n]);
                }
                if <[u8; 32]>::from(hash.finalize()) != manifest.output_sha256 {
                    return Err(tinkle_storage::StorageError::InvalidInput(
                        "artifact hash changed".into(),
                    ));
                }
                artifact.as_file().sync_all().map_err(|_| {
                    tinkle_storage::StorageError::Unavailable("artifact sync failed".into())
                })?;
                let mut manifest = manifest;
                let thumbnail_artifact = if let Some(thumbnail) = &mut manifest.thumbnail {
                    use std::io::Write as _;
                    let bytes = prepared.thumbnail.as_ref().ok_or_else(|| {
                        tinkle_storage::StorageError::InvalidInput("thumbnail unavailable".into())
                    })?;
                    if bytes.len() > 1024 * 1024 {
                        return Err(tinkle_storage::StorageError::InvalidInput(
                            "thumbnail exceeds budget".into(),
                        ));
                    }
                    if <[u8; 32]>::from(Sha256::digest(bytes)) != thumbnail.sha256 {
                        return Err(tinkle_storage::StorageError::InvalidInput(
                            "thumbnail hash changed".into(),
                        ));
                    }
                    let mut file = tempfile::Builder::new()
                        .prefix("thumbnail-")
                        .suffix(".png")
                        .tempfile_in(&root)
                        .map_err(|_| {
                            tinkle_storage::StorageError::Unavailable(
                                "thumbnail unavailable".into(),
                            )
                        })?;
                    file.write_all(bytes)
                        .and_then(|_| file.as_file().sync_all())
                        .map_err(|_| {
                            tinkle_storage::StorageError::Unavailable(
                                "thumbnail sync failed".into(),
                            )
                        })?;
                    thumbnail.artifact_path = file
                        .path()
                        .to_str()
                        .ok_or_else(|| {
                            tinkle_storage::StorageError::InvalidInput(
                                "thumbnail path unavailable".into(),
                            )
                        })?
                        .into();
                    Some(file)
                } else {
                    None
                };
                manifest.artifact_path = artifact
                    .path()
                    .to_str()
                    .ok_or_else(|| {
                        tinkle_storage::StorageError::InvalidInput(
                            "artifact path unavailable".into(),
                        )
                    })?
                    .into();
                let proof = manifest.encode().map_err(|_| {
                    tinkle_storage::StorageError::InvalidInput(
                        "invalid preparation manifest".into(),
                    )
                })?;
                let initial = tinkle_storage::FileTransferJob {
                    job_id: id,
                    kind: "upload".into(),
                    file_scope: source.into(),
                    file_id: 0,
                    upload_id: String::new(),
                    state: "begin".into(),
                    session_json: proof.clone(),
                    temp_path: manifest.artifact_path.clone(),
                    resume_offset: 0,
                    updated_at: 0,
                };
                // Make the artifact and both newly created directory entries durable before SQL.
                for directory in [
                    Some(root.as_path()),
                    root.parent(),
                    root.parent().and_then(Path::parent),
                ]
                .into_iter()
                .flatten()
                {
                    std::fs::File::open(directory)
                        .and_then(|f| f.sync_all())
                        .map_err(|_| {
                            tinkle_storage::StorageError::Unavailable(
                                "artifact directory sync failed".into(),
                            )
                        })?;
                }
                let intent = s
                    .create_or_compare_preparation_intent(p, incarnation, &initial, &proof)
                    .map_err(tinkle_storage::StorageError::from)?;
                artifact.keep().map_err(|_| {
                    tinkle_storage::StorageError::Unavailable("artifact retention failed".into())
                })?;
                if let Some(file) = thumbnail_artifact {
                    file.keep().map_err(|_| {
                        tinkle_storage::StorageError::Unavailable(
                            "thumbnail retention failed".into(),
                        )
                    })?;
                }
                Ok((intent, manifest))
            })
            .await
    }

    /// Prepare through a real native encoder on the first call only. A retained manifest wins
    /// before callback invocation on retries, including process restart and changed input bytes.
    pub async fn prepare_native_attachment(
        &self,
        key: String,
        path: &Path,
        kind: crate::media::NativeMediaKind,
        preparer: Arc<dyn crate::media::PlatformMediaPreparer>,
        cancel: Option<crate::CancelToken>,
    ) -> Result<PreparedAttachment, SendError> {
        self.prepare_native_attachment_inner(
            key,
            path,
            kind,
            Some(preparer),
            cancel,
            cfg!(tinkle_async_actions),
        )
        .await
    }

    /// Resolve configured encoders only after retained-artifact lookup, so restart never needs one.
    async fn prepare_native_attachment_inner(
        &self,
        key: String,
        path: &Path,
        kind: crate::media::NativeMediaKind,
        preparer: Option<Arc<dyn crate::media::PlatformMediaPreparer>>,
        cancel: Option<crate::CancelToken>,
        async_actions: bool,
    ) -> Result<PreparedAttachment, SendError> {
        let _admission = SEND_ATTEMPTS.try_acquire().map_err(|_| SendError::Busy)?;
        let cancel = cancel.unwrap_or_default();
        let mut cancel_on_drop = CancelOnDrop(Some(cancel.clone()));
        if key.is_empty() || key.len() > 200 {
            return Err(SendError::InvalidRequest);
        }
        let scope = Scope::capture(self.client).await?;
        let input = std::path::absolute(path).map_err(|_| SendError::InvalidRequest)?;
        let input = input.to_str().ok_or(SendError::InvalidRequest)?.to_owned();
        let settings = format!(
            "{}:main-only:{kind:?}{}",
            crate::media::NATIVE_PREPARATION_PROFILE,
            if async_actions {
                ":singular-thumbnail-compact-wire-reset-v1"
            } else if cfg!(tinkle_schema_candidate) {
                ":compact-wire-reset-v1"
            } else {
                ""
            }
        );
        let id = preparation_job(&scope.source, &key);
        let read_id = id.clone();
        let retained = scope
            .store(move |s, p, _| s.file_transfer_intent(p, &read_id).map_err(Into::into))
            .await?;
        if let Some(intent) = retained {
            let manifest = AttachmentManifest::decode(&intent.job.session_json)?;
            if manifest.input_path != input
                || manifest.settings != settings
                || intent.job.file_scope != scope.source
            {
                return Err(SendError::Conflict);
            }
            let result = self
                .upload_prepared(&scope, &key, intent, manifest, cancel.clone())
                .await?;
            cancel_on_drop.0 = None;
            return Ok(result);
        }
        let preparer = preparer
            .or_else(|| {
                self.client
                    .media_preparer
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone()
            })
            .ok_or(crate::media::MediaError::Unsupported)?;
        // A preparation stage owns its own cancellation; finishing it cannot cancel upload.
        let stage_cancel = crate::CancelToken::new();
        let stage_owner = CancelOnDrop(Some(stage_cancel.clone()));
        let scratch_root = scope.store(|s, _, _| s.temporary_directory()).await?;
        let prepared = tokio::select! {biased;
            _=cancel.cancelled()=>return Err(SendError::Media(crate::media::MediaError::Cancelled)),
            _=self.client.authorization.revoked.cancelled()=>return Err(SendError::ScopeChanged),
            result=crate::media::prepare_native_media_in(path,&scratch_root,kind,preparer,Some(stage_cancel))=>result?,
        };
        drop(stage_owner);
        scope.check()?;
        let manifest = Self::native_manifest(input, settings, &prepared)?;
        let (intent, manifest) =
            Self::persist_preparation(&scope, id, manifest, prepared.main).await?;
        let result = self
            .upload_prepared(&scope, &key, intent, manifest, cancel.clone())
            .await?;
        cancel_on_drop.0 = None;
        Ok(result)
    }

    /// Build source metadata only from the immutable artifact's measured output, never input guesses.
    fn native_manifest(
        input: String,
        settings: String,
        prepared: &crate::media::NativePreparedMedia,
    ) -> Result<AttachmentManifest, SendError> {
        let f = &prepared.output;
        let media = match prepared.kind {
            crate::media::NativeMediaKind::Image => p::media_source::Media::Photo(p::PhotoSource {
                file_id: 0,
                metadata: Some(p::ImageMetadata {
                    width: f.width,
                    height: f.height,
                    animated: f.frame_count > 1,
                    ..Default::default()
                }),
                #[cfg(not(tinkle_async_actions))]
                variants: vec![],
                #[cfg(tinkle_async_actions)]
                thumbnail: None,
                #[cfg(tinkle_async_actions)]
                inline_preview: None,
            }),
            crate::media::NativeMediaKind::Video => p::media_source::Media::Video(p::VideoSource {
                file_id: 0,
                metadata: Some(p::VideoMetadata {
                    width: f.width,
                    height: f.height,
                    duration_ms: f.duration_ms,
                    ..Default::default()
                }),
                #[cfg(not(tinkle_async_actions))]
                variants: vec![],
                #[cfg(tinkle_async_actions)]
                thumbnail: None,
                #[cfg(tinkle_async_actions)]
                inline_preview: None,
            }),
            crate::media::NativeMediaKind::Audio => p::media_source::Media::Audio(p::AudioSource {
                file_id: 0,
                metadata: Some(p::AudioMetadata {
                    duration_ms: f.duration_ms,
                    ..Default::default()
                }),
                #[cfg(not(tinkle_async_actions))]
                variants: vec![],
                #[cfg(tinkle_async_actions)]
                thumbnail: None,
                #[cfg(tinkle_async_actions)]
                inline_preview: None,
            }),
            crate::media::NativeMediaKind::Voice(_) => {
                p::media_source::Media::Voice(p::VoiceSource {
                    file_id: 0,
                    duration_ms: f.duration_ms,
                    waveform: Some(f.waveform.clone().into()),
                })
            }
        };
        let source = p::MediaSource { media: Some(media) };
        #[cfg(tinkle_async_actions)]
        let mut source = source;
        #[cfg(tinkle_async_actions)]
        if settings.ends_with(":singular-thumbnail-compact-wire-reset-v1") {
            let preview = prepared.inline_preview()?;
            match source.media.as_mut() {
                Some(p::media_source::Media::Photo(v)) => v.inline_preview = preview,
                Some(p::media_source::Media::Video(v)) => v.inline_preview = preview,
                _ => {}
            }
        }
        Ok(AttachmentManifest {
            input_path: input,
            settings,
            artifact_path: String::new(),
            source_sha256: prepared.main.source_sha256,
            output_sha256: prepared.main.output_sha256,
            profile: prepared.main.profile.into(),
            mime_type: prepared.main.mime_type.into(),
            media_source: source.encode_to_vec(),
            native_facts: Some(prepared.output.clone()),
            tiny_preview: prepared.tiny_preview.clone(),
            thumbnail: prepared
                .main
                .thumbnail
                .as_ref()
                .zip(prepared.main.thumbnail_facts.as_ref())
                .map(|(bytes, facts)| AttachmentThumbnail {
                    artifact_path: String::new(),
                    sha256: Sha256::digest(bytes).into(),
                    width: facts.width.unwrap_or(0),
                    height: facts.height.unwrap_or(0),
                }),
        })
    }

    /// Adopt a retained native result under the current account/trust fence without invoking a
    /// codec. The verified copied bytes, recipe, source hash and facts commit before any upload.
    pub async fn adopt_native_attachment(
        &self,
        key: String,
        source_path: &Path,
        prepared: crate::media::NativePreparedMedia,
        cancel: Option<crate::CancelToken>,
    ) -> Result<PreparedAttachment, SendError> {
        self.adopt_native_attachment_mode(
            key,
            source_path,
            prepared,
            cancel,
            cfg!(tinkle_async_actions),
        )
        .await
    }

    /// Shared immutable adoption; source capability is part of the retained settings.
    async fn adopt_native_attachment_mode(
        &self,
        key: String,
        source_path: &Path,
        prepared: crate::media::NativePreparedMedia,
        cancel: Option<crate::CancelToken>,
        async_actions: bool,
    ) -> Result<PreparedAttachment, SendError> {
        let _admission = SEND_ATTEMPTS.try_acquire().map_err(|_| SendError::Busy)?;
        let cancel = cancel.unwrap_or_default();
        let mut cancel_on_drop = CancelOnDrop(Some(cancel.clone()));
        if key.is_empty() || key.len() > 200 {
            return Err(SendError::InvalidRequest);
        }
        let scope = Scope::capture(self.client).await?;
        let input = std::path::absolute(source_path).map_err(|_| SendError::InvalidRequest)?;
        let input = input.to_str().ok_or(SendError::InvalidRequest)?.to_owned();
        let settings = format!(
            "{}:main-only:{:?}{}",
            prepared.main.profile,
            prepared.kind,
            if async_actions {
                ":singular-thumbnail-compact-wire-reset-v1"
            } else if cfg!(tinkle_schema_candidate) {
                ":compact-wire-reset-v1"
            } else {
                ""
            }
        );
        let manifest = Self::native_manifest(input, settings, &prepared)?;
        let id = preparation_job(&scope.source, &key);
        let read_id = id.clone();
        let retained = scope
            .store(move |s, p, _| s.file_transfer_intent(p, &read_id).map_err(Into::into))
            .await?;
        let (intent, manifest) = if let Some(intent) = retained {
            let retained = AttachmentManifest::decode(&intent.job.session_json)?;
            let mut comparison = retained.clone();
            comparison.artifact_path.clear();
            if let Some(thumbnail) = &mut comparison.thumbnail {
                thumbnail.artifact_path.clear();
            }
            if comparison.encode()? != manifest.encode()? || intent.job.file_scope != scope.source {
                return Err(SendError::Conflict);
            }
            (intent, retained)
        } else {
            if cancel.is_cancelled() {
                return Err(SendError::Media(crate::media::MediaError::Cancelled));
            }
            Self::persist_preparation(&scope, id, manifest, prepared.main).await?
        };
        let result = self
            .upload_prepared(&scope, &key, intent, manifest, cancel)
            .await?;
        cancel_on_drop.0 = None;
        Ok(result)
    }

    /// Resume only the retained artifact and exact upload key; never invoke a codec here.
    async fn upload_prepared(
        &self,
        scope: &Arc<Scope>,
        key: &str,
        intent: tinkle_storage::transfer_intent::TransferIntentCheckpoint,
        manifest: AttachmentManifest,
        cancel: crate::CancelToken,
    ) -> Result<PreparedAttachment, SendError> {
        scope.check()?;
        let path = std::path::PathBuf::from(&manifest.artifact_path);
        let root = scope.store(|s, p, _| s.preparation_directory(p)).await?;
        if path.parent() != Some(root.as_path()) || intent.job.temp_path != manifest.artifact_path {
            return Err(SendError::Storage);
        }
        let old = intent.clone();
        let intent = scope
            .store(move |s, p, _| {
                let mut next = old.job.clone();
                if next.state == "begin" {
                    next.state = "active".into();
                }
                s.advance_upload_intent(p, &old, &next)
                    .map_err(tinkle_storage::StorageError::from)
            })
            .await?;
        let caller = self
            .client
            .for_application_scope(scope.epoch, scope.revision);
        let file = caller
            .upload_manager()
            .upload_path(
                crate::UploadParams {
                    #[cfg(tinkle_async_actions)]
                    processing_intent: if manifest
                        .settings
                        .ends_with(":singular-thumbnail-compact-wire-reset-v1")
                    {
                        match p::MediaSource::decode(manifest.media_source.as_slice())
                            .ok()
                            .and_then(|m| m.media)
                        {
                            Some(p::media_source::Media::Photo(_)) => {
                                Some(p::MediaProcessingIntent::Image)
                            }
                            Some(p::media_source::Media::Video(_)) => {
                                Some(p::MediaProcessingIntent::Video)
                            }
                            _ => None,
                        }
                    } else {
                        None
                    },
                    idempotency_key: format!("{}:main", preparation_job(&scope.source, key)),
                    file_name: None,
                    mime_type: Some(manifest.mime_type.clone()),
                    sha256: Some(manifest.output_sha256.to_vec()),
                },
                &path,
                Some(cancel.clone()),
            )
            .await?;
        scope.check()?;
        // Record confirmed main identity before starting the independent optional transfer.
        let main_id = file.file_id;
        let intent = scope
            .store(move |s, p, _| {
                let mut next = intent.job.clone();
                if next.state != "complete" {
                    next.state = "completing".into();
                }
                if next.file_id != 0 && next.file_id != main_id {
                    return Err(tinkle_storage::StorageError::IntentConflict);
                }
                next.file_id = main_id;
                s.advance_upload_intent(p, &intent, &next)
                    .map_err(Into::into)
            })
            .await?;
        // The optional transfer is a distinct exact-key operation. Propagate uncertain failures;
        // never turn a submitted two-file operation into a different main-only retry.
        let thumbnail = self
            .upload_prepared_thumbnail(scope, key, &manifest, cancel)
            .await?;
        let mut source = p::MediaSource::decode(manifest.media_source.as_slice())
            .map_err(|_| SendError::Storage)?;
        match source.media.as_mut() {
            Some(p::media_source::Media::Photo(v)) => v.file_id = file.file_id,
            Some(p::media_source::Media::Document(v)) => v.file_id = file.file_id,
            Some(p::media_source::Media::Video(v)) => v.file_id = file.file_id,
            Some(p::media_source::Media::Voice(v)) => v.file_id = file.file_id,
            Some(p::media_source::Media::Audio(v)) => v.file_id = file.file_id,
            _ => return Err(SendError::Storage),
        }
        #[cfg(tinkle_async_actions)]
        if manifest
            .settings
            .ends_with(":singular-thumbnail-compact-wire-reset-v1")
        {
            let preview = manifest
                .tiny_preview
                .as_ref()
                .map(|bytes| {
                    crate::media::validate_tiny_preview(bytes).map(|(width, height)| {
                        p::InlinePreview {
                            width,
                            height,
                            jpeg: bytes.clone().into(),
                        }
                    })
                })
                .transpose()?;
            let thumbnail_source =
                thumbnail
                    .as_ref()
                    .zip(manifest.thumbnail.as_ref())
                    .map(|(file, t)| p::ThumbnailSource {
                        file_id: file.file_id,
                        width: t.width,
                        height: t.height,
                    });
            match source.media.as_mut() {
                Some(p::media_source::Media::Photo(v)) => {
                    v.inline_preview = preview;
                    v.thumbnail = thumbnail_source;
                }
                Some(p::media_source::Media::Video(v)) => {
                    v.inline_preview = preview;
                    v.thumbnail = thumbnail_source;
                }
                Some(p::media_source::Media::Document(v)) => {
                    v.inline_preview = preview;
                    v.thumbnail = thumbnail_source;
                }
                Some(p::media_source::Media::Audio(v)) => {
                    v.inline_preview = preview;
                    v.thumbnail = thumbnail_source;
                }
                _ => {}
            }
        }
        let file_id = file.file_id;
        scope
            .store(move |s, p, _| {
                let mut next = intent.job.clone();
                next.state = "complete".into();
                next.file_id = file_id;
                s.advance_upload_intent(p, &intent, &next)
                    .map_err(tinkle_storage::StorageError::from)
            })
            .await?;
        Ok(PreparedAttachment {
            file,
            thumbnail,
            source,
            source_sha256: manifest.source_sha256,
            output_sha256: manifest.output_sha256,
            preparation_profile: manifest.profile,
            tiny_preview: manifest.tiny_preview,
        })
    }

    /// Upload only the retained optional PNG after a main attempt; never prepare or change source bytes.
    async fn upload_prepared_thumbnail(
        &self,
        scope: &Arc<Scope>,
        key: &str,
        manifest: &AttachmentManifest,
        cancel: crate::CancelToken,
    ) -> Result<Option<p::FileInfo>, SendError> {
        let Some(thumbnail) = &manifest.thumbnail else {
            return Ok(None);
        };
        scope.check()?;
        let root = scope.store(|s, p, _| s.preparation_directory(p)).await?;
        let path = Path::new(&thumbnail.artifact_path);
        if path.parent() != Some(root.as_path())
            || thumbnail.artifact_path == manifest.artifact_path
        {
            return Err(SendError::Storage);
        }
        let caller = self
            .client
            .for_application_scope(scope.epoch, scope.revision);
        let file = caller
            .upload_manager()
            .upload_path(
                crate::UploadParams {
                    #[cfg(tinkle_async_actions)]
                    processing_intent: manifest
                        .settings
                        .ends_with(":singular-thumbnail-compact-wire-reset-v1")
                        .then_some(p::MediaProcessingIntent::Image),
                    idempotency_key: format!("{}:thumbnail", preparation_job(&scope.source, key)),
                    file_name: None,
                    mime_type: Some("image/png".into()),
                    sha256: Some(thumbnail.sha256.to_vec()),
                },
                path,
                Some(cancel),
            )
            .await?;
        scope.check()?;
        Ok(Some(file))
    }

    /// Explicitly retry only the immutable optional upload after confirmed main completion.
    /// Errors, including unknown outcomes, propagate without changing the retained media source.
    pub async fn upload_attachment_thumbnail(
        &self,
        key: String,
        cancel: Option<crate::CancelToken>,
    ) -> Result<Option<p::FileInfo>, SendError> {
        let _admission = SEND_ATTEMPTS.try_acquire().map_err(|_| SendError::Busy)?;
        if key.is_empty() || key.len() > 200 {
            return Err(SendError::InvalidRequest);
        }
        let scope = Scope::capture(self.client).await?;
        let id = preparation_job(&scope.source, &key);
        let intent = scope
            .store(move |s, p, _| s.file_transfer_intent(p, &id).map_err(Into::into))
            .await?
            .ok_or(SendError::NotFound)?;
        if intent.job.file_scope != scope.source
            || intent.job.file_id <= 0
            || !matches!(intent.job.state.as_str(), "completing" | "complete")
        {
            return Err(SendError::Conflict);
        }
        let manifest = AttachmentManifest::decode(&intent.job.session_json)?;
        let cancel = cancel.unwrap_or_default();
        let mut owner = CancelOnDrop(Some(cancel.clone()));
        let result = self
            .upload_prepared_thumbnail(&scope, &key, &manifest, cancel)
            .await?;
        scope
            .store(move |s, p, _| {
                let mut next = intent.job.clone();
                next.state = "complete".into();
                s.advance_upload_intent(p, &intent, &next)
                    .map_err(Into::into)
            })
            .await?;
        owner.0 = None;
        Ok(result)
    }

    /// Inspect the durable source/settings/hash/path handoff without preparing or transmitting.
    pub async fn attachment_manifest(
        &self,
        key: String,
    ) -> Result<Option<AttachmentManifest>, SendError> {
        let scope = Scope::capture(self.client).await?;
        let id = preparation_job(&scope.source, &key);
        let job = scope
            .store(move |s, p, _| {
                s.file_transfer_intent(p, &id).map_err(|_| {
                    tinkle_storage::StorageError::Unavailable("preparation unavailable".into())
                })
            })
            .await?;
        job.map(|j| AttachmentManifest::decode(&j.job.session_json))
            .transpose()
    }
}

impl Client {
    /// Install the normal native sending encoder. It must not retain this Client; each in-flight
    /// operation owns its selected callback through native completion. Sign-out clears this slot.
    pub fn configure_media_preparer(
        &self,
        preparer: Option<Arc<dyn crate::media::PlatformMediaPreparer>>,
    ) -> Result<(), TinkleError> {
        let mut slot = self
            .media_preparer
            .write()
            .unwrap_or_else(|e| e.into_inner());
        self.authorization.generation()?;
        let old = std::mem::replace(&mut *slot, preparer);
        drop(slot);
        drop(old);
        Ok(())
    }
    /// Store or clear decoder-only configuration, separate from the normal sending encoder.
    /// This slot does not satisfy prepare_attachment(Video); install configure_media_preparer.
    pub fn configure_video_decoder(
        &self,
        decoder: Option<Arc<dyn crate::media::PlatformVideoDecoder>>,
    ) -> Result<(), TinkleError> {
        let mut slot = self
            .video_decoder
            .write()
            .unwrap_or_else(|e| e.into_inner());
        self.authorization.generation()?;
        let old = std::mem::replace(&mut *slot, decoder);
        drop(slot);
        drop(old);
        Ok(())
    }
    /// Inspect one native transfer checkpoint under the same principal/source fence as sends.
    /// The returned offset is diagnostic, not proof of remote acceptance or permission to skip bytes.
    pub async fn file_transfer_checkpoint(
        &self,
        job_id: String,
    ) -> Result<Option<tinkle_storage::FileTransferJob>, SendError> {
        Scope::capture(self)
            .await?
            .store(move |s, p, _| s.get_file_transfer(p, &job_id))
            .await
    }
}

/// Decode only the expected concrete result and fence every body to the original target.
type AcceptedBodies = (Vec<p::Message>, Vec<p::User>, Vec<p::Chat>);

/// Only explicit transient refusal is retryable; loss/cancellation after dispatch is uncertain.
fn failure_outcome(error: &TinkleError) -> (OutgoingState, tinkle_storage::OutgoingOutcome) {
    use syntra_core::{error::SyntraError, ErrorCode as C};
    let (state, raw_code) = match error {
        TinkleError::Syntra(SyntraError::Rpc { code, raw_code, .. }) => (
            if error.is_unknown_outcome() || matches!(code, C::Unspecified | C::DataLoss) {
                OutgoingState::Unconfirmed
            } else if *code == C::ResourceExhausted {
                OutgoingState::Transient
            } else {
                OutgoingState::Rejected
            },
            *raw_code,
        ),
        _ => (OutgoingState::Unconfirmed, None),
    };
    (
        state,
        tinkle_storage::OutgoingOutcome {
            code: error.code() as i32,
            raw_code,
            unknown_outcome: state == OutgoingState::Unconfirmed,
        },
    )
}

fn accepted_messages(job: &OutgoingJob, response: &[u8]) -> Result<AcceptedBodies, SendError> {
    let decode = |_| SendError::InvalidRequest;
    let (messages, users, chats) = match job.method.as_str() {
        "/tinkle.v1.MessagesService/SendMessage" => {
            let r = p::SendMessageResponse::decode(response).map_err(decode)?;
            (r.message.into_iter().collect::<Vec<_>>(), r.users, r.chats)
        }
        "/tinkle.v1.MessagesService/SendMedia" => {
            let r = p::SendMediaResponse::decode(response).map_err(decode)?;
            (r.message.into_iter().collect(), r.users, r.chats)
        }
        "/tinkle.v1.MessagesService/SendAlbum" => {
            let r = p::SendAlbumResponse::decode(response).map_err(decode)?;
            (r.messages, r.users, r.chats)
        }
        _ => return Err(SendError::InvalidRequest),
    };
    let album = if job.method.ends_with("/SendAlbum") {
        Some(p::SendAlbumRequest::decode(job.payload.as_slice()).map_err(decode)?)
    } else {
        None
    };
    let expected = album.as_ref().map_or(1, |r| r.items.len());
    if let Some(request) = &album {
        let album_id = messages.first().and_then(|m| m.album_id);
        // Missing identity is invalid too; agreement between items alone is insufficient.
        if !(2..=10).contains(&expected)
            || album_id.is_none_or(|id| id <= 0)
            || messages.iter().any(|m| m.album_id != album_id)
            || messages.windows(2).any(|pair| {
                pair[0].scheduled_state != pair[1].scheduled_state
                    || pair[0].schedule_date != pair[1].schedule_date
                    || matches!((pair[0].accept_seq,pair[1].accept_seq),(Some(a),Some(b)) if b<=a)
            })
            || messages.iter().zip(&request.items).any(|(m, item)| {
                m.message != item.message
                    || m.entities != item.entities
                    || match (
                        item.media.as_ref().and_then(|m| m.media.as_ref()),
                        m.media.as_ref().and_then(|m| m.media.as_ref()),
                    ) {
                        (
                            Some(p::media_source::Media::Photo(a)),
                            Some(p::media::Media::Photo(b)),
                        ) => a.file_id != b.file_id,
                        (
                            Some(p::media_source::Media::Document(a)),
                            Some(p::media::Media::Document(b)),
                        ) => a.file_id != b.file_id,
                        (
                            Some(p::media_source::Media::Video(a)),
                            Some(p::media::Media::Document(b)),
                        ) => {
                            a.file_id != b.file_id
                                || !b.attributes.iter().any(|v| {
                                    matches!(v.kind, Some(p::media_attribute::Kind::Video(_)))
                                })
                        }
                        _ => true,
                    }
            })
        {
            return Err(SendError::InvalidRequest);
        }
    }
    let schedule = match job.method.as_str() {
        "/tinkle.v1.MessagesService/SendMessage" => {
            p::SendMessageRequest::decode(job.payload.as_slice())
                .map_err(decode)?
                .schedule_date
        }
        "/tinkle.v1.MessagesService/SendMedia" => {
            p::SendMediaRequest::decode(job.payload.as_slice())
                .map_err(decode)?
                .schedule_date
        }
        _ => album.as_ref().and_then(|r| r.schedule_date),
    };
    if messages.len() != expected
        || messages
            .iter()
            .map(|m| m.message_id)
            .collect::<std::collections::HashSet<_>>()
            .len()
            != expected
        || messages.iter().any(|m| {
            m.message_id <= 0
                || m.accept_seq.is_some_and(|v| v <= 0)
                || (m.scheduled_state != p::MessageScheduledState::Pending as i32
                    && m.accept_seq.is_none())
                || (m.scheduled_state == p::MessageScheduledState::Pending as i32
                    && (m.accept_seq.is_some() || schedule.is_none()))
                || (m.scheduled_state == p::MessageScheduledState::Pending as i32
                    && m.schedule_date != schedule)
                || (m.scheduled_state == p::MessageScheduledState::NotScheduled as i32
                    && (schedule.is_some() || m.schedule_date.is_some()))
                || (m.scheduled_state == p::MessageScheduledState::Sent as i32
                    && (schedule.is_none() || m.schedule_date != schedule))
                || (schedule.is_some()
                    && m.scheduled_state == p::MessageScheduledState::Unspecified as i32)
                || !matches!(
                    p::MessageScheduledState::try_from(m.scheduled_state),
                    Ok(p::MessageScheduledState::Unspecified
                        | p::MessageScheduledState::NotScheduled
                        | p::MessageScheduledState::Pending
                        | p::MessageScheduledState::Sent)
                )
                || m.target
                    .as_ref()
                    .and_then(|t| t.kind.as_ref())
                    .map(tinkle_schema::sync::target_key_for)
                    .as_deref()
                    != Some(job.target_key.as_str())
        })
    {
        return Err(SendError::InvalidRequest);
    }
    Ok((messages, users, chats))
}

#[cfg(test)]
mod completion_tests {
    use super::*;

    /// The full approved JPEG budget fits the original immutable-intent budget without duplicate bytes.
    #[cfg(tinkle_async_actions)]
    #[test]
    fn action_manifest_retains_maximum_preview_once_and_verifies_exact_declaration() {
        let original =
            crate::media::tiny_jpeg_preview(&image::DynamicImage::new_rgb8(1, 1)).unwrap();
        let padding = crate::media::TINY_PREVIEW_BYTES - original.len() - 4;
        let mut jpeg = original[..2].to_vec();
        jpeg.extend([255, 225]);
        jpeg.extend(((padding + 2) as u16).to_be_bytes());
        jpeg.extend(vec![0; padding]);
        jpeg.extend(&original[2..]);
        assert_eq!(jpeg.len(), 4096);
        assert_eq!(crate::media::validate_tiny_preview(&jpeg).unwrap(), (1, 1));
        let media_source = p::MediaSource {
            media: Some(p::media_source::Media::Photo(p::PhotoSource {
                inline_preview: Some(p::InlinePreview {
                    width: 1,
                    height: 1,
                    jpeg: jpeg.clone().into(),
                }),
                ..Default::default()
            })),
        }
        .encode_to_vec();
        let manifest = AttachmentManifest {
            input_path: "/private/import".into(),
            settings: "native:singular-thumbnail-compact-wire-reset-v1".into(),
            artifact_path: "/private/main.png".into(),
            source_sha256: [1; 32],
            output_sha256: [2; 32],
            profile: "native".into(),
            mime_type: "image/png".into(),
            media_source: media_source.clone(),
            native_facts: Some(crate::media::NativeMediaOutput {
                width: 1,
                height: 1,
                frame_count: 1,
                duration_ms: 0,
                audio_channels: 0,
                audio_sample_rate: 0,
                audio_samples: 0,
                waveform: vec![],
                thumbnail_frame: None,
            }),
            tiny_preview: Some(jpeg.clone()),
            thumbnail: None,
        };
        let encoded = manifest.encode().unwrap();
        assert!(encoded.len() <= 16384);
        let value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(value["version"], 5);
        assert_eq!(value["wire_identity"], "compact-wire-reset-v1");
        assert_eq!(
            value["media_contract"],
            "singular-thumbnail-compact-wire-reset-v1"
        );
        let retained = AttachmentManifest::decode(&encoded).unwrap();
        assert_eq!(retained.tiny_preview, Some(jpeg));
        assert_eq!(retained.media_source, media_source);
        let mut corrupt: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        corrupt["media_hash"] = serde_json::json!("wrong");
        assert!(AttachmentManifest::decode(&corrupt.to_string()).is_err());
        for (field, stale) in [
            ("wire_identity", "previous-wire"),
            ("media_contract", "singular-thumbnail-v1"),
            ("settings", "native:singular-thumbnail-v1"),
        ] {
            let mut stale_value = value.clone();
            stale_value[field] = serde_json::json!(stale);
            assert!(
                AttachmentManifest::decode(&stale_value.to_string()).is_err(),
                "{field}"
            );
        }
    }

    /// Absent additive fields preserve legacy pending-source bytes; malformed additions fail closed.
    #[cfg(not(tinkle_schema_candidate))]
    #[test]
    fn legacy_manifest_resumes_without_creating_display_artifacts() {
        let mut legacy = serde_json::json!({"version":1,"input":"/import","settings":"old",
            "artifact":"/private/main","source_sha256":vec![0;32],"output_sha256":vec![1;32],
            "profile":"old","mime":"application/octet-stream","media":vec![1,2,3]});
        let manifest = AttachmentManifest::decode(&legacy.to_string()).unwrap();
        assert_eq!(manifest.media_source, vec![1, 2, 3]);
        assert!(manifest.tiny_preview.is_none() && manifest.thumbnail.is_none());
        legacy["preview"] = serde_json::json!("ff");
        assert!(AttachmentManifest::decode(&legacy.to_string()).is_err());
        legacy["preview"] = serde_json::Value::Null;
        legacy["thumbnail"] = serde_json::json!({"artifact":"/private/thumbnail","sha256":vec![2;32],"width":321,"height":1});
        assert!(AttachmentManifest::decode(&legacy.to_string()).is_err());
    }

    /// Candidate preparation never decodes a prior representation contract as a current source.
    #[cfg(tinkle_schema_candidate)]
    #[test]
    fn old_candidate_manifest_is_not_reinterpreted() {
        for version in [1, 3] {
            let old = serde_json::json!({"version":version,"input":"/import","settings":"native:async-actions",
                "artifact":"/private/main","source_sha256":vec![0;32],"output_sha256":vec![1;32],
                "profile":"old","mime":"image/png","media":vec![8,9]});
            assert!(AttachmentManifest::decode(&old.to_string()).is_err());
        }
        // A byte-simple source can decode identically across resets: content checks are no epoch.
        let old = serde_json::json!({"version":4,"media_contract":"singular-thumbnail-v1",
            "input":"/import","settings":"native:singular-thumbnail-v1",
            "artifact":"/private/main","source_sha256":vec![0;32],"output_sha256":vec![1;32],
            "profile":"old","mime":"application/octet-stream","media":Vec::<u8>::new(),
            "preview_in_media":false,"media_hash":format!("{:x}", Sha256::digest([]))});
        assert!(AttachmentManifest::decode(&old.to_string()).is_err());
    }

    /// An album fixture preserves ordered source identities independently of numeric message IDs.
    fn album() -> (OutgoingJob, p::SendAlbumResponse) {
        let target = Some(p::Target {
            kind: Some(p::target::Kind::ChatId(7)),
        });
        let items = (1..=2)
            .map(|id| p::AlbumItem {
                idempotency_key: id,
                message: format!("item {id}"),
                media: Some(p::MediaSource {
                    media: Some(p::media_source::Media::Photo(p::PhotoSource {
                        file_id: id,
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            })
            .collect();
        let request = p::SendAlbumRequest {
            target,
            items,
            ..Default::default()
        };
        let messages = request
            .items
            .iter()
            .enumerate()
            .map(|(index, item)| p::Message {
                message_id: 100 - index as i64,
                target,
                message: item.message.clone(),
                album_id: Some(9),
                accept_seq: Some(index as i64 + 1),
                media: Some(p::Media {
                    media: Some(p::media::Media::Photo(p::Photo {
                        file_id: index as i64 + 1,
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            })
            .collect();
        (
            OutgoingJob {
                job_id: "album".into(),
                source: "a".repeat(64),
                target_key: "target:room:7".into(),
                method: "/tinkle.v1.MessagesService/SendAlbum".into(),
                payload: request.encode_to_vec(),
                state: OutgoingState::Dispatching,
                response: vec![],
                last_outcome: None,
                revision: 1,
                cache_generation: 0,
                cache_incarnation: 0,
            },
            p::SendAlbumResponse {
                messages,
                ..Default::default()
            },
        )
    }

    /// Atomic acknowledgement must carry one positive album identity and exact ordered items.
    #[test]
    fn album_completion_rejects_empty_mismatched_and_reordered_shapes() {
        let (job, response) = album();
        assert!(accepted_messages(&job, &response.encode_to_vec()).is_ok());
        // All items agree, so missing/nonpositive IDs cannot hide behind the mismatch check.
        for album_id in [
            None,
            Some(i64::MIN),
            Some(-1),
            Some(0),
            Some(1),
            Some(i64::MAX),
        ] {
            let mut candidate = response.clone();
            for message in &mut candidate.messages {
                message.album_id = album_id;
            }
            assert_eq!(
                accepted_messages(&job, &candidate.encode_to_vec()).is_ok(),
                matches!(album_id, Some(id) if id > 0),
                "album ID {album_id:?}"
            );
        }
        for mode in 0..7 {
            let mut bad = response.clone();
            match mode {
                0 => bad.messages.clear(),
                1 => bad.messages[0].album_id = None,
                2 => bad.messages[0].album_id = Some(10),
                3 => bad.messages.swap(0, 1),
                4 => bad.messages[0].message = "different".into(),
                5 => bad.messages[0].accept_seq = None,
                _ => bad.messages[1].scheduled_state = p::MessageScheduledState::Pending as i32,
            }
            assert!(
                accepted_messages(&job, &bad.encode_to_vec()).is_err(),
                "mode {mode}"
            );
        }
        let mut empty = job;
        empty.payload = p::SendAlbumRequest::default().encode_to_vec();
        assert!(
            accepted_messages(&empty, &p::SendAlbumResponse::default().encode_to_vec()).is_err()
        );
    }

    /// Pending acknowledgement is valid only with the same requested schedule and no position.
    #[test]
    fn pending_album_schedule_is_not_delivery() {
        let (mut job, mut response) = album();
        let mut request = p::SendAlbumRequest::decode(job.payload.as_slice()).unwrap();
        request.schedule_date = Some(100);
        job.payload = request.encode_to_vec();
        for message in &mut response.messages {
            message.accept_seq = None;
            message.scheduled_state = p::MessageScheduledState::Pending as i32;
            message.schedule_date = Some(100);
        }
        assert!(accepted_messages(&job, &response.encode_to_vec()).is_ok());
        response.messages[1].schedule_date = Some(101);
        assert!(accepted_messages(&job, &response.encode_to_vec()).is_err());
    }

    /// Known transient refusal, CAS refusal and ambiguous execution retain distinct states.
    #[test]
    fn failure_classification_preserves_status_and_uncertainty() {
        use syntra_core::{error::SyntraError, ErrorCode as C};
        for (code, state) in [
            (C::ResourceExhausted, OutgoingState::Transient),
            (C::Aborted, OutgoingState::Rejected),
            (C::InvalidArgument, OutgoingState::Rejected),
            (C::Unavailable, OutgoingState::Unconfirmed),
            (C::DeadlineExceeded, OutgoingState::Unconfirmed),
            (C::DataLoss, OutgoingState::Unconfirmed),
            (C::Unknown, OutgoingState::Unconfirmed),
        ] {
            let (got, outcome) = failure_outcome(&TinkleError::Syntra(SyntraError::Rpc {
                code,
                raw_code: if code == C::Unknown { Some(1234) } else { None },
                message: "failure".into(),
            }));
            assert_eq!(got, state);
            assert_eq!(outcome.code, code as i32);
            assert_eq!(outcome.unknown_outcome, state == OutgoingState::Unconfirmed);
            if code == C::Unknown {
                assert_eq!(outcome.raw_code, Some(1234));
            }
        }
        assert_eq!(
            failure_outcome(&TinkleError::Syntra(SyntraError::Cancelled(
                "cancelled".into()
            )))
            .0,
            OutgoingState::Unconfirmed
        );
    }
}
