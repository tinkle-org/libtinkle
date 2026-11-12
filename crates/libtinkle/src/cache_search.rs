//! Shared Search coordination and private, bounded cache fallback. No local-only public API.

use crate::{authorization::EnrollmentEpoch, Client, TinkleError};
use prost::Message as _;
use rand::RngCore as _;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tinkle_schema::proto as p;
use tinkle_storage::{MessageSearchSnapshot, SearchPosition, Storage, StorageError};

/// Bound retained continuation state per logical Client (shared by its clones).
const MAX_HANDLES: usize = 64;
/// Absolute sequence lifetime; paging never renews it.
const HANDLE_TTL: Duration = Duration::from_secs(300);
/// Local handles are never interpretable as upstream cursors.
const PREFIX: &str = "tinkle-search:v1:";
/// Preserve the original cache-search candidate and blocking-admission budgets.
const MAX_CANDIDATES: usize = 128;
static SEARCH_WORKERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

#[cfg(test)]
#[path = "search_tests.rs"]
mod tests;

/// No bodies are retained in handles; a page is recomputed under its pinned cache revision.
#[derive(Default)]
pub(crate) struct Continuations(HashMap<String, Arc<Continuation>>);

struct Continuation {
    binding: [u8; 32],
    fence: SearchFence,
    expires: Instant,
    backend: Backend,
}

enum Backend {
    Server(String),
    Cache(LocalScan),
}

/// Local source-order traversal, including a bounded negative-offset boundary search.
#[derive(Clone)]
struct LocalScan {
    snapshot: MessageSearchSnapshot,
    after: Option<SearchPosition>,
    min: Option<i64>,
    max: Option<i64>,
    before: Option<i64>,
    exclude: Option<i64>,
    skip: u32,
    newer: u32,
    highest: Option<i64>,
}

/// Account/source identity is independent of transport availability and reconnect generation.
struct SearchFence {
    authority: EnrollmentEpoch,
    principal: String,
    epoch: u64,
    revision: u64,
}

impl SearchFence {
    /// Capture before the principal read, rejecting account/source ABA after every await.
    async fn capture(client: &Client) -> Result<Self, TinkleError> {
        let authority = client.authorization.generation()?;
        let (epoch, revision) = client.transfer_application_fence()?;
        let principal = client.principal().await;
        let fence = Self {
            authority,
            principal,
            epoch,
            revision,
        };
        fence.check(client)?;
        if fence.principal == crate::sync::UNKNOWN_PRINCIPAL {
            return Err(TinkleError::Configuration(
                "search requires a current principal".into(),
            ));
        }
        Ok(fence)
    }

    /// A final synchronous check leaves no scheduling point between validation and return.
    fn check(&self, client: &Client) -> Result<(), TinkleError> {
        self.authority.check(client)?;
        client.search_admission(&self.principal)?;
        if (self.epoch, self.revision) != (client.reconcile_epoch(), client.retirement.revision()) {
            return Err(TinkleError::Sync(
                "search scope changed; restart search".into(),
            ));
        }
        self.authority.check(client)
    }
}

/// Only known local disconnection/network failures permit a new search to select cache.
/// Syntra status Unavailable can mean current READ authority is unavailable, not offline;
/// runtime/RPC errors all propagate rather than guessing their origin from a code or text.
fn cache_allowed(error: &TinkleError) -> bool {
    matches!(
        error,
        TinkleError::NotConnected(_) | TinkleError::NetworkUnavailable
    )
}

/// Validate the shared request, including full-width offsets, before any remote or cache work.
pub(crate) fn validate(req: &p::SearchRequest) -> Result<(), TinkleError> {
    if req.target.is_some() {
        crate::services::validate_target(req.target.as_ref())?;
    }
    if req
        .from_target
        .as_ref()
        .is_some_and(|t| !matches!(t.kind, Some(p::target::Kind::UserId(1..))))
    {
        return Err(TinkleError::Codec(
            "search sender must be a positive user".into(),
        ));
    }
    let cursor = req.cursor.as_deref().filter(|s| !s.is_empty());
    if !(0..=100).contains(&req.limit)
        || !(0..=11).contains(&req.filter)
        || req.query.len() > 1024
        || req.min_date < 0
        || req.max_date < 0
        || (req.max_date != 0 && req.max_date < req.min_date)
        || req.offset_id < 0
        || !(-100..=100).contains(&req.add_offset)
        || [
            req.thread_root_id,
            req.min_id,
            req.max_id,
            req.offset_message_id,
        ]
        .into_iter()
        .flatten()
        .any(|v| v <= 0)
        || (req.offset_message_id.is_some() && req.offset_id != 0)
        || (req.add_offset < 0 && req.offset_message_id.is_none() && req.offset_id == 0)
        || (req.target.is_none()
            && (req.thread_root_id.is_some()
                || req.min_id.is_some()
                || req.max_id.is_some()
                || req.offset_message_id.is_some()
                || req.offset_id != 0
                || req.add_offset != 0))
        || cursor.is_some_and(|s| {
            s.len() > 2048
                || req.offset_id != 0
                || req.add_offset != 0
                || req.offset_message_id.is_some()
        })
    {
        return Err(TinkleError::Codec(
            "invalid search scope, filters or offsets".into(),
        ));
    }
    Ok(())
}

/// Hash every persistent predicate with exact presence; page size and initial window controls
/// are not predicates. The latter live inside the continuation and must be cleared on paging.
fn binding(req: &p::SearchRequest) -> [u8; 32] {
    let mut req = req.clone();
    req.cursor = None;
    req.limit = 0;
    req.offset_id = 0;
    req.offset_message_id = None;
    req.add_offset = 0;
    Sha256::digest(req.encode_to_vec()).into()
}

/// Invoke remote first for new searches. Continuations select exactly one backend even after
/// reconnect; foreign, expired and raw remote cursors fail closed before transport dispatch.
pub(crate) async fn search(
    client: &Client,
    mut req: p::SearchRequest,
) -> Result<p::SearchResponse, TinkleError> {
    validate(&req)?;
    if req.limit == 0 {
        req.limit = 50;
    }
    let fence = SearchFence::capture(client).await?;
    let bound = binding(&req);
    let continuation = if let Some(cursor) = req.cursor.as_deref().filter(|v| !v.is_empty()) {
        let entry = client
            .search_continuations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .0
            .get(cursor)
            .cloned()
            .ok_or_else(|| TinkleError::Codec("unknown search cursor; restart search".into()))?;
        entry.fence.check(client)?;
        if entry.binding != bound || Instant::now() >= entry.expires {
            return Err(TinkleError::Codec(
                "changed or expired search cursor; restart search".into(),
            ));
        }
        Some(entry)
    } else {
        None
    };
    let expires = continuation
        .as_ref()
        .map_or_else(|| Instant::now() + HANDLE_TTL, |c| c.expires);
    let (mut response, next) = match continuation.as_ref().map(|c| &c.backend) {
        Some(Backend::Cache(scan)) => cache_page(client, &fence, req, Some(scan.clone())).await?,
        backend => {
            req.cursor = match backend {
                Some(Backend::Server(cursor)) => Some(cursor.clone()),
                _ => None,
            };
            let caller = client.for_application_scope(fence.epoch, fence.revision);
            let remote = crate::services::call_typed::<_, p::SearchResponse>(
                &caller,
                tinkle_schema::methods::messagesservice::SEARCH,
                &req,
                30_000,
            )
            .await;
            fence.check(client)?;
            match remote {
                Ok(mut page) => {
                    // Never overwrite an explicit conflicting source/corpus declaration.
                    if !matches!(page.source, 0 | 1)
                        || !matches!(page.coverage, 0 | 1)
                        || page.messages.len() > req.limit as usize
                        || page
                            .next_cursor
                            .as_ref()
                            .is_some_and(|s| s.is_empty() || s.len() > 2048)
                        || (page.more_available == Some(true)) != page.next_cursor.is_some()
                    {
                        return Err(TinkleError::Codec("invalid server search page".into()));
                    }
                    // Unknown source/coverage stay unknown; the socket cannot attest either.
                    let next = page.next_cursor.take().map(Backend::Server);
                    (page, next)
                }
                Err(error)
                    if continuation.is_none()
                        && cache_allowed(&error)
                        && client.sqlite_storage().is_some() =>
                {
                    cache_page(client, &fence, req, None).await?
                }
                Err(error) => return Err(error),
            }
        }
    };
    fence.check(client)?;
    if Instant::now() >= expires {
        return Err(TinkleError::Codec("expired search sequence".into()));
    }
    if let Some(backend) = next {
        let mut random = [0; 16];
        rand::rngs::OsRng
            .try_fill_bytes(&mut random)
            .map_err(|_| TinkleError::Sync("search cursor entropy unavailable".into()))?;
        let handle = format!("{PREFIX}{:032x}", u128::from_be_bytes(random));
        let mut handles = client
            .search_continuations
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        handles.0.retain(|_, c| {
            c.expires > Instant::now()
                && c.fence.epoch == fence.epoch
                && c.fence.revision == fence.revision
        });
        if handles.0.len() == MAX_HANDLES {
            // Bounded LRU-like eviction: oldest expiry loses its restartable read-only sequence.
            if let Some(oldest) = handles
                .0
                .iter()
                .min_by_key(|(_, c)| c.expires)
                .map(|(k, _)| k.clone())
            {
                handles.0.remove(&oldest);
            }
        }
        if handles.0.contains_key(&handle) {
            return Err(TinkleError::Sync("search cursor collision".into()));
        }
        fence.check(client)?;
        handles.0.insert(
            handle.clone(),
            Arc::new(Continuation {
                binding: bound,
                fence,
                expires,
                backend,
            }),
        );
        response.next_cursor = Some(handle);
    }
    Ok(response)
}

/// Map storage failures without diagnostic/payload leakage or pretending corruption is a miss.
fn storage_error(error: StorageError) -> TinkleError {
    match error {
        StorageError::InvalidInput(_) => TinkleError::Codec("invalid cache search input".into()),
        StorageError::SearchChanged
        | StorageError::CacheScopeChanged
        | StorageError::GenerationMismatch { .. } => {
            TinkleError::Sync("search cache scope changed; restart search".into())
        }
        _ => TinkleError::Sync("search cache unavailable or corrupt".into()),
    }
}

/// Blocking scans keep admission through cancellation and the final durable revision check.
async fn cache_page(
    client: &Client,
    fence: &SearchFence,
    req: p::SearchRequest,
    scan: Option<LocalScan>,
) -> Result<(p::SearchResponse, Option<Backend>), TinkleError> {
    let storage = client
        .sqlite_storage()
        .ok_or_else(|| TinkleError::NotConnected("no cache".into()))?;
    let permit = SEARCH_WORKERS
        .try_acquire()
        .map_err(|_| TinkleError::Sync("search workers busy".into()))?;
    let principal = fence.principal.clone();
    let worker = storage.clone();
    let (result, permit) =
        tokio::task::spawn_blocking(move || (scan_cache(&worker, &principal, &req, scan), permit))
            .await
            .map_err(|_| TinkleError::Sync("search worker failed".into()))?;
    let (messages, next, snapshot) = result?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        storage.check_search_snapshot(&snapshot)
    })
    .await
    .map_err(|_| TinkleError::Sync("search worker failed".into()))?
    .map_err(storage_error)?;
    fence.check(client)?;
    Ok((
        p::SearchResponse {
            count: messages.len() as i32,
            messages,
            source: p::SearchSource::Cache as i32,
            coverage: p::SearchCoverage::CachedSubset as i32,
            // This is local continuation only, never a statement about history completeness.
            more_available: Some(next.is_some()),
            ..Default::default()
        },
        next.map(Backend::Cache),
    ))
}

/// Resolve IDs using the cached source position, not ID magnitude or journal progress.
fn reference(
    storage: &Storage,
    snapshot: &MessageSearchSnapshot,
    target: &str,
    id: i64,
) -> Result<i64, TinkleError> {
    let bytes = storage
        .search_reference(snapshot, target, id)
        .map_err(storage_error)?
        .ok_or_else(|| TinkleError::Sync("search boundary is not cached".into()))?;
    let m = p::Message::decode(bytes.as_slice())
        .map_err(|_| TinkleError::Sync("corrupt search boundary".into()))?;
    if m.message_id != id
        || m.target
            .as_ref()
            .and_then(|t| t.kind.as_ref())
            .map(tinkle_schema::sync::target_key_for)
            .as_deref()
            != Some(target)
        || m.accept_seq.is_none_or(|s| s <= 0)
    {
        return Err(TinkleError::Sync("invalid cached search boundary".into()));
    }
    Ok(m.accept_seq.unwrap_or_default())
}

/// Consume at most 128 candidates, retaining even an empty filtered page's continuation.
fn scan_cache(
    storage: &Storage,
    principal: &str,
    req: &p::SearchRequest,
    previous: Option<LocalScan>,
) -> Result<(Vec<p::Message>, Option<LocalScan>, MessageSearchSnapshot), TinkleError> {
    let target = req
        .target
        .as_ref()
        .and_then(|t| t.kind.as_ref())
        .map(tinkle_schema::sync::target_key_for);
    let mut scan = match previous {
        Some(scan) => scan,
        None => {
            let snapshot = storage
                .capture_search_snapshot(principal)
                .map_err(storage_error)?;
            let resolve = |id: Option<i64>| {
                id.map(|id| {
                    reference(
                        storage,
                        &snapshot,
                        target.as_deref().unwrap_or_default(),
                        id,
                    )
                })
                .transpose()
            };
            let min = resolve(req.min_id)?;
            let max = resolve(req.max_id)?;
            if matches!((min,max),(Some(a),Some(b)) if a>b) {
                return Err(TinkleError::Codec("reversed source search bounds".into()));
            }
            let before = resolve(
                req.offset_message_id
                    .or_else(|| (req.offset_id > 0).then_some(i64::from(req.offset_id))),
            )?;
            if let Some(id) = req.thread_root_id {
                reference(
                    storage,
                    &snapshot,
                    target.as_deref().unwrap_or_default(),
                    id,
                )?;
            }
            LocalScan {
                snapshot,
                after: None,
                min,
                max,
                before,
                exclude: before,
                skip: req.add_offset.max(0) as u32,
                newer: (-req.add_offset.min(0)) as u32,
                highest: before,
            }
        }
    };
    // unicode61 FTS cannot find arbitrary literal infixes. Traverse bounded source-order
    // candidates rather than silently imposing token/phrase semantics on shared Search.
    let mut out = Vec::new();
    let mut out_bytes = 0usize;
    let mut examined = 0;
    loop {
        let batch_size = (MAX_CANDIDATES - examined)
            .min(16)
            .min((req.limit as usize - out.len()).max(1)) as u32;
        let candidates = storage
            .search_candidates(
                &scan.snapshot,
                target.as_deref(),
                scan.after.as_ref(),
                scan.newer > 0,
                batch_size,
            )
            .map_err(storage_error)?;
        if candidates.is_empty() {
            if scan.newer > 0 {
                finish_newer(&mut scan);
                continue;
            }
            return Ok((out, None, scan.snapshot));
        }
        for hit in candidates {
            examined += 1;
            scan.after = Some(hit.position);
            let message = p::Message::decode(hit.proto.as_slice())
                .map_err(|_| TinkleError::Sync("corrupt cached search message".into()))?;
            if message.message_id != hit.message_id
                || message.from_id != hit.sender_id
                || message.accept_seq != Some(hit.accept_seq)
                || message
                    .target
                    .as_ref()
                    .and_then(|t| t.kind.as_ref())
                    .map(tinkle_schema::sync::target_key_for)
                    .as_deref()
                    != Some(hit.target_key.as_str())
            {
                return Err(TinkleError::Sync("cached search metadata mismatch".into()));
            }
            let seq = hit.accept_seq;
            if scan.min.is_some_and(|v| seq < v)
                || scan.max.is_some_and(|v| seq > v)
                || scan.exclude == Some(seq)
                || scan
                    .before
                    .is_some_and(|v| if scan.newer > 0 { seq <= v } else { seq >= v })
                || !matches(&message, req, principal)
            {
                continue;
            }
            if scan.newer > 0 {
                scan.highest = Some(seq);
                scan.newer -= 1;
                if scan.newer == 0 {
                    finish_newer(&mut scan);
                    break;
                }
            } else if scan.skip > 0 {
                scan.skip -= 1;
            } else {
                out_bytes += message.encoded_len();
                if out_bytes > 2_000_000 {
                    return Err(TinkleError::Sync(
                        "cached search page exceeds payload budget".into(),
                    ));
                }
                out.push(message);
            }
        }
        if out.len() == req.limit as usize || examined == MAX_CANDIDATES {
            return Ok((out, Some(scan.clone()), scan.snapshot));
        }
    }
}

/// Negative windows first find the nearest newer prefix, then traverse it in source-descending order.
fn finish_newer(scan: &mut LocalScan) {
    scan.newer = 0;
    scan.before = None;
    scan.after = None;
    if let Some(highest) = scan.highest {
        scan.max = Some(scan.max.map_or(highest, |v| v.min(highest)));
    }
}

/// One content classification per cached body, before applying any content filter.
enum ContentKind {
    Photo,
    Sticker,
    Video { round: bool },
    Audio { voice: bool },
    Gif,
    Document,
    Other,
}

/// Mixed document attributes select sticker > video > audio > animated > document,
/// independent of attribute order. Round/voice flags refine only the selected kind;
/// a lower-priority attribute cannot make a video also satisfy audio/GIF filters.
fn content_kind(media: Option<&p::Media>) -> ContentKind {
    match media.and_then(|media| media.media.as_ref()) {
        Some(p::media::Media::Photo(photo)) => {
            if photo.metadata.as_ref().is_some_and(|m| m.animated) {
                ContentKind::Gif
            } else {
                ContentKind::Photo
            }
        }
        Some(p::media::Media::Document(document)) => {
            use p::media_attribute::Kind;
            let mut video = None;
            let mut audio = None;
            let mut animated = false;
            for attribute in &document.attributes {
                match &attribute.kind {
                    Some(Kind::Sticker(_)) => return ContentKind::Sticker,
                    Some(Kind::Video(v)) => {
                        video.get_or_insert(v.round_video);
                    }
                    Some(Kind::Audio(v)) => {
                        audio.get_or_insert(v.voice_message);
                    }
                    Some(Kind::Image(v)) => animated |= v.animated,
                    _ => {}
                }
            }
            match (video, audio, animated) {
                (Some(round), _, _) => ContentKind::Video { round },
                (None, Some(voice), _) => ContentKind::Audio { voice },
                (None, None, true) => ContentKind::Gif,
                _ => ContentKind::Document,
            }
        }
        _ => ContentKind::Other,
    }
}

/// Literal substring matches over the full cached body. No query operators, stemming,
/// semantic ranking or remote-equivalent recall is claimed for this cached subset.
fn matches(m: &p::Message, req: &p::SearchRequest, principal: &str) -> bool {
    if m.scheduled_state == p::MessageScheduledState::Pending as i32 {
        return false;
    }
    if m.ttl_period > 0 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_secs()).ok());
        if now.is_none_or(|now| {
            m.date
                .as_ref()
                .is_none_or(|d| d.seconds.saturating_add(i64::from(m.ttl_period)) <= now)
        }) {
            return false;
        }
    }
    if let Some(from) = req.from_target.as_ref() {
        if !matches!(from.kind,Some(p::target::Kind::UserId(id)) if id==m.from_id) {
            return false;
        }
    }
    if req
        .thread_root_id
        .is_some_and(|id| m.reply_to.as_ref().is_none_or(|r| r.thread_root_id != id))
    {
        return false;
    }
    if (req.min_date > 0 || req.max_date > 0)
        && m.date.as_ref().is_none_or(|d| {
            d.seconds < i64::from(req.min_date)
                || (req.max_date > 0
                    && (d.seconds > i64::from(req.max_date)
                        || (d.seconds == i64::from(req.max_date) && d.nanos > 0)))
        })
    {
        return false;
    }
    let text = m.message.to_lowercase();
    if !text.contains(&req.query.to_lowercase()) {
        return false;
    }
    match req.filter {
        0 => true,
        10 => m.pinned,
        11 => m.entities.iter().any(|e| {
            e.r#type == p::EntityType::MentionName as i32
                && principal == format!("user:{}", e.user_id)
        }),
        5 => m.entities.iter().any(|e| matches!(e.r#type, 7 | 8)),
        filter => matches!(
            (filter, content_kind(m.media.as_ref())),
            (1 | 3, ContentKind::Photo)
                | (2 | 3, ContentKind::Video { .. })
                | (4, ContentKind::Document)
                | (6, ContentKind::Gif)
                | (7, ContentKind::Audio { voice: true })
                | (8, ContentKind::Audio { voice: false })
                | (9, ContentKind::Video { round: true })
        ),
    }
}
