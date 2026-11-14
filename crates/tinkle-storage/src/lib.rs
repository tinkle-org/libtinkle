//! Shared native SQLite cache + per-conversation journal cursors for libtinkle.
//!
//! Rust owns migrations, schema, transactions, and queries. Foreign hosts
//! access the same file through UniFFI (`tinkle-ffi`). This crate exposes
//! only owned, FFI-friendly values and never leaks SQL in errors.
//!
//! Threading: `Storage` holds one `Mutex<Connection>`. `rusqlite::Connection`
//! is `Send` but not `Sync`; the mutex makes `Storage` `Send + Sync`.
//! Calls are short, bounded, and synchronous. Async callers must run them
//! via `spawn_blocking` (see `tinkle-ffi`) so Tokio workers are not blocked.
//!
//! Reconciliation model (v3):
//! - `checkpoints(principal, stream, cursor)` rows are per-conversation
//!   journal cursors. `stream` holds a journal key `target:user:<id>` or
//!   `target:room:<id>` (`ChatId` maps to `room`); `cursor` holds the journal
//!   `accept_seq` (0 is the initial position, positions start at 1).
//! - `stream_state` is per-conversation validity + generation fencing.
//!   `replay_floor` is retained but always zero; `generation` is bumped on
//!   every [`Storage::mark_rebuild`] and preserved across checkpoint advances.
//! - [`Storage::apply_journal_page_fenced`] atomically commits one validated
//!   `ListMessageChanges` page (message upserts with `accept_seq` dedup,
//!   removals, dialog touches, entity hydration, read-state marks) plus the
//!   checkpoint advance, fenced by `expected_generation`.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension};
use thiserror::Error;

mod search;
pub use search::{SearchCandidate, SearchPosition};

pub mod transfer_intent;

/// Schema v9 separates nonterminal body revision floors from permanent removals.
const SCHEMA_VERSION: i64 = 9;

/// Errors surfaced from storage. Messages are safe for FFI; no SQL text.
#[derive(Debug, Error, Clone)]
pub enum StorageError {
    /// An immutable intent or CAS revision changed; refetch, never blindly retry the write.
    #[error("retained intent changed; refetch before retrying")]
    IntentConflict,
    /// A pre-request cache proof belongs to an invalidated or cleared scope.
    #[error("message cache scope changed")]
    CacheScopeChanged,
    /// A search continuation or result belongs to a changed cache snapshot.
    #[error("search cache changed; restart the search")]
    SearchChanged,
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("cursor scope mismatch: expected checkpoint {expected}, found {found} on {stream}")]
    ScopeMismatch {
        stream: String,
        expected: i64,
        found: i64,
    },
    #[error("generation fence mismatch: expected {expected}, found {found} on {stream}")]
    GenerationMismatch {
        stream: String,
        expected: u64,
        found: u64,
    },
    #[error("storage corrupt: {0}")]
    Corrupt(String),
}

impl From<rusqlite::Error> for StorageError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Unavailable(e.to_string())
    }
}

fn check_accept_seq(cursor: i64) -> Result<(), StorageError> {
    if cursor < 0 {
        return Err(StorageError::InvalidInput(
            "accept_seq must be 0..9223372036854775807".into(),
        ));
    }
    Ok(())
}

fn check_cursor_u64(cursor: u64) -> Result<(), StorageError> {
    if cursor > i64::MAX as u64 {
        return Err(StorageError::InvalidInput(
            "cursor must be 0..9223372036854775807".into(),
        ));
    }
    Ok(())
}

fn check_principal(principal: &str) -> Result<(), StorageError> {
    let (kind, id) = principal.split_once(':').ok_or_else(|| {
        StorageError::InvalidInput("principal must be user:<id> or room:<id>".into())
    })?;
    if kind != "user" && kind != "room" {
        return Err(StorageError::InvalidInput(
            "principal kind must be user/room".into(),
        ));
    }
    match id.parse::<i64>() {
        Ok(v) if v > 0 => Ok(()),
        _ => Err(StorageError::InvalidInput(
            "principal id must be positive".into(),
        )),
    }
}

/// Journal key grammar: `target:user:<id>` or `target:room:<id>` with a
/// positive id. `ChatId` maps to `room`.
fn check_target_key(target_key: &str) -> Result<(), StorageError> {
    let rest = target_key.strip_prefix("target:").ok_or_else(|| {
        StorageError::InvalidInput("target_key must be target:user:<id> or target:room:<id>".into())
    })?;
    let (kind, id) = rest.split_once(':').ok_or_else(|| {
        StorageError::InvalidInput("target_key must be target:user:<id> or target:room:<id>".into())
    })?;
    if kind != "user" && kind != "room" {
        return Err(StorageError::InvalidInput(
            "target_key kind must be user/room".into(),
        ));
    }
    match id.parse::<i64>() {
        Ok(v) if v > 0 => Ok(()),
        _ => Err(StorageError::InvalidInput(
            "target_key id must be positive".into(),
        )),
    }
}

/// Provenance origin: a valid journal target key, or `""` for
/// legacy/global rows. `""` pins the row: scoped rebuilds never delete it.
fn check_origin(origin: &str) -> Result<(), StorageError> {
    if origin.is_empty() {
        return Ok(());
    }
    check_target_key(origin).map_err(|_| {
        StorageError::InvalidInput(
            "origin_stream must be '' or target:user:<id>/target:room:<id>".into(),
        )
    })
}

fn check_conversation_key(key: &str) -> Result<(), StorageError> {
    check_target_key(key).map_err(|_| StorageError::InvalidInput("bad conversation_key".into()))
}

fn check_file_id(file_id: i64) -> Result<(), StorageError> {
    if file_id <= 0 {
        return Err(StorageError::InvalidInput(
            "file_id must be positive".into(),
        ));
    }
    Ok(())
}

/// File scope namespace: `""` is the global pin (never deleted by scoped
/// rebuilds); otherwise an opaque short token (journal targets use
/// `target:user:<id>` / `target:room:<id>`). Mirrors the `check_origin`
/// pin convention without over-constraining future scopes.
fn check_file_scope(scope: &str) -> Result<(), StorageError> {
    if scope.is_empty() {
        return Ok(());
    }
    if scope.len() > 256 || scope.contains('\0') {
        return Err(StorageError::InvalidInput("bad file scope".into()));
    }
    Ok(())
}

fn check_job_id(job_id: &str) -> Result<(), StorageError> {
    if job_id.is_empty() || job_id.len() > 256 {
        return Err(StorageError::InvalidInput(
            "job_id must be 1..256 chars".into(),
        ));
    }
    Ok(())
}

fn check_transfer_kind(kind: &str) -> Result<(), StorageError> {
    if kind != "upload" && kind != "download" {
        return Err(StorageError::InvalidInput(
            "transfer kind must be upload/download".into(),
        ));
    }
    Ok(())
}

fn check_file_role(role: &str) -> Result<(), StorageError> {
    if role.len() > 64 || role.contains('\0') {
        return Err(StorageError::InvalidInput("bad file role".into()));
    }
    Ok(())
}

/// Entity kinds that can own file refs (avatars, chat photos, stickers).
fn check_entity_file_kind(kind: &str) -> Result<(), StorageError> {
    if kind != "user" && kind != "chat" && kind != "sticker" {
        return Err(StorageError::InvalidInput(
            "entity file kind must be user/chat/sticker".into(),
        ));
    }
    Ok(())
}

fn check_availability(state: &str) -> Result<(), StorageError> {
    match state {
        "missing" | "downloading" | "complete" | "corrupt" => Ok(()),
        _ => Err(StorageError::InvalidInput(
            "availability must be missing/downloading/complete/corrupt".into(),
        )),
    }
}

/// Cached message row. `proto` is the complete `tinkle.v1.Message` bytes.
/// `origin_stream` records provenance; canonical `conversation_key` ownership
/// still applies when an older producer left that origin empty.
/// `accept_seq` is the message's own acceptance token (`None` for legacy
/// rows accepted before the token was introduced; absent tokens never vote
/// on dedup). `edit_date_secs/nanos` carry the message `edit_date` for
/// body freshness ordering when either copy lacks a body revision. Acceptance tokens
/// order distinct messages, never copies of one body.
#[derive(Debug, Clone)]
pub struct MessageRow {
    pub conversation_key: String,
    pub message_id: i64,
    pub peer_kind: String,
    pub peer_id: i64,
    pub from_id: i64,
    pub body: String,
    pub proto: Vec<u8>,
    pub origin_stream: String,
    pub accept_seq: Option<i64>,
    pub edit_date_secs: Option<i64>,
    pub edit_date_nanos: Option<i32>,
}

/// Literal query interpretation using SQLite unicode61 (Latin diacritics folded).
/// Contiguous Han characters form one token: phrase matches the whole token,
/// prefix matches its beginning; there is no Chinese word segmentation or infix search.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MessageSearchMode {
    /// All whitespace-separated literal terms must match (punctuation is tokenized).
    #[default]
    Terms,
    /// The entire literal input is one ordered token phrase.
    Phrase,
    /// Every whitespace-separated literal term is a token prefix (at least two characters).
    Prefix,
}

/// Bounded local search. Limits outside 1..=50 and queries outside 1..=512 bytes fail.
#[derive(Debug, Clone)]
pub struct MessageSearchRequest {
    pub query: String,
    pub mode: MessageSearchMode,
    /// Canonical target key, or every cached target of this principal.
    pub target_key: Option<String>,
    pub sender_id: Option<i64>,
    pub limit: u32,
    pub cursor: Option<MessageSearchCursor>,
}

impl Default for MessageSearchRequest {
    fn default() -> Self {
        Self {
            query: String::new(),
            mode: MessageSearchMode::Terms,
            target_key: None,
            sender_id: None,
            limit: 20,
            cursor: None,
        }
    }
}

/// Opaque keyset continuation, bound to the exact query, principal and cache revision.
/// Any cached message mutation invalidates it instead of silently skipping edited hits.
/// Ordering is descending local document ID, not relevance or server acceptance order.
#[derive(Debug, Clone)]
pub struct MessageSearchCursor {
    snapshot: MessageSearchSnapshot,
    query: String,
    mode: MessageSearchMode,
    target_key: Option<String>,
    sender_id: Option<i64>,
    before_document_id: i64,
}

/// A committed local snapshot identity, independent of journal/acceptance positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSearchSnapshot {
    database_id: String,
    principal: String,
    revision: i64,
}

/// Cached candidate with full protobuf bytes, not a separate snippet/renderer model.
#[derive(Debug, Clone)]
pub struct MessageSearchHit {
    pub target_key: String,
    pub message_id: i64,
    pub sender_id: i64,
    pub proto: Vec<u8>,
}

/// Search results from a single SQLite read snapshot. Cache contents are only a subset
/// of server history; neither an empty page nor exhaustion establishes completeness.
#[derive(Debug, Clone)]
pub struct MessageSearchPage {
    pub hits: Vec<MessageSearchHit>,
    pub next: Option<MessageSearchCursor>,
    pub snapshot: MessageSearchSnapshot,
}

/// Escape every user-supplied string as an FTS literal; operators never pass through.
fn literal_search_query(request: &MessageSearchRequest) -> Result<String, StorageError> {
    if request.query.len() > 512 || !(1..=50).contains(&request.limit) {
        return Err(StorageError::InvalidInput(
            "bad search query or limit".into(),
        ));
    }
    let query = request.query.trim();
    let terms: Vec<_> = query.split_whitespace().collect();
    if terms.is_empty()
        || terms.len() > 32
        || !query.chars().any(char::is_alphanumeric)
        || query.chars().any(char::is_control)
        || request.sender_id.is_some_and(|id| id <= 0)
    {
        return Err(StorageError::InvalidInput(
            "bad search query, sender or limit".into(),
        ));
    }
    if let Some(target) = &request.target_key {
        check_target_key(target)?;
    }
    let quote = |text: &str| format!("\"{}\"", text.replace('"', "\"\""));
    if request.mode == MessageSearchMode::Phrase {
        return Ok(quote(query));
    }
    let mut literals = Vec::with_capacity(terms.len());
    for term in terms {
        let mut literal = quote(term);
        if request.mode == MessageSearchMode::Prefix {
            if term.chars().filter(|c| c.is_alphanumeric()).count() < 2 {
                return Err(StorageError::InvalidInput(
                    "search prefixes need two characters".into(),
                ));
            }
            literal.push('*');
        }
        literals.push(literal);
    }
    Ok(literals.join(" AND "))
}

/// Cached dialog row. `proto` is the complete `tinkle.v1.Dialog` bytes.
/// `origin_stream` scopes the row to its producing journal target (`""` =
/// legacy/global pin, never deleted by scoped rebuilds).
/// `read_state_pending` means an invalidation committed with the checkpoint
/// and no authoritative refresh has cleared it yet; hosts should refresh.
#[derive(Debug, Clone)]
pub struct DialogRow {
    pub peer_kind: String,
    pub peer_id: i64,
    pub top_message: i64,
    pub unread_count: i32,
    pub read_inbox_max_id: i64,
    pub read_outbox_max_id: i64,
    pub pinned: bool,
    pub proto: Vec<u8>,
    pub origin_stream: String,
    pub read_state_pending: bool,
}

/// Cached user row. `proto` is the complete `tinkle.v1.User` bytes.
#[derive(Debug, Clone)]
pub struct UserRow {
    pub user_id: i64,
    pub proto: Vec<u8>,
}

/// Cached chat row. `proto` is the complete `tinkle.v1.Chat` bytes.
#[derive(Debug, Clone)]
pub struct ChatRow {
    pub chat_id: i64,
    pub proto: Vec<u8>,
}

/// Canonical immutable file metadata, keyed `(principal, scope, file_id)`.
///
/// `scope` namespaces the file (`""` = global pin, otherwise the owning
/// scope such as a journal target). `origin_stream` records the producing
/// journal target (`""` = legacy/global pin that scoped rebuilds never
/// delete). Rows are immutable: journal commits use `INSERT OR IGNORE`
/// so the first writer wins and replays are no-ops.
#[derive(Debug, Clone)]
pub struct FileRow {
    pub scope: String,
    pub file_id: i64,
    pub size_bytes: i64,
    pub sha256: Vec<u8>,
    pub mime_type: String,
    pub file_name: String,
    pub encrypted: bool,
    pub created_at: i64,
    pub origin_stream: String,
}

/// Message → file reference. `file_scope` + `file_id` address the `files`
/// row; `role` distinguishes attachment kinds (`photo`, `document`,
/// `thumbnail`, ...). Replays are idempotent (`INSERT OR IGNORE`).
#[derive(Debug, Clone)]
pub struct MessageFileRef {
    pub conversation_key: String,
    pub message_id: i64,
    pub file_scope: String,
    pub file_id: i64,
    pub role: String,
}

/// Entity → file reference (user avatar, chat photo, sticker pack file).
#[derive(Debug, Clone)]
pub struct EntityFileRef {
    pub kind: String,
    pub entity_id: i64,
    pub file_scope: String,
    pub file_id: i64,
    pub role: String,
}

/// Local cache state for one file. This table records bytes on disk and is
/// intentionally decoupled from `files`: scoped rebuilds never delete
/// these rows (only `message_files` refs and unreferenced `files` rows are
/// collected), so re-downloads are avoided.
#[derive(Debug, Clone)]
pub struct FileCacheEntry {
    pub scope: String,
    pub file_id: i64,
    pub relative_path: String,
    pub actual_size: i64,
    pub availability: String,
    pub last_access: i64,
    pub pinned: bool,
}

/// Durable upload/download job. `file_scope` + `file_id` address the
/// `files` row (`upload_id` carries the pending remote id for uploads
/// before the file is confirmed). `session_json` is an opaque resume
/// blob owned by the transfer layer.
#[derive(Debug, Clone)]
pub struct FileTransferJob {
    pub job_id: String,
    pub kind: String,
    pub file_scope: String,
    pub file_id: i64,
    pub upload_id: String,
    pub state: String,
    pub session_json: String,
    pub temp_path: String,
    pub resume_offset: i64,
    pub updated_at: i64,
}

/// Persistent push/catch-up work item. Claimed process-safely via
/// [`Storage::claim_background_job`] (compare-and-set on `claimed_by`
/// plus `attempts` bump).
#[derive(Debug, Clone)]
pub struct BackgroundSyncJob {
    pub job_id: String,
    pub kind: String,
    pub target_key: String,
    pub state: String,
    pub payload: Vec<u8>,
    pub attempts: i64,
    pub next_due: i64,
    pub claimed_by: Option<String>,
    pub updated_at: i64,
}

/// Immutable keyed send plus retained outcome. Payload/response are application protobufs,
/// never credentials. A dispatched job is uncertain until an actual result is retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutgoingJob {
    pub job_id: String,
    /// Hash of provisioned server key, key ID and scope; not endpoint-learned trust.
    pub source: String,
    pub target_key: String,
    pub method: String,
    pub payload: Vec<u8>,
    pub state: OutgoingState,
    pub response: Vec<u8>,
    /// Last observed RPC failure, retained atomically with state; absence is not success.
    pub last_outcome: Option<OutgoingOutcome>,
    /// CAS revision, advanced before each dispatch and retained result.
    pub revision: i64,
    /// Target cache generation at intent creation; rebuilding fences old acknowledgements.
    pub cache_generation: u64,
    /// Durable principal incarnation captured with the intent; clearing never reuses it.
    pub cache_incarnation: u64,
}

/// Structured failure evidence, never permission to replay an uncertain mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutgoingOutcome {
    pub code: i32,
    pub raw_code: Option<i32>,
    pub unknown_outcome: bool,
}

/// Sealed pre-request cache scope. Capture before I/O and carry unchanged into hydration.
/// It binds the database, principal, canonical target, validity, and durable wipe/rebuild fences.
#[derive(Debug, Clone)]
pub struct MessageCacheFence {
    database_id: String,
    principal: String,
    target_key: String,
    generation: u64,
    incarnation: u64,
    valid: bool,
}

/// Sealed global-dialog request proof. A principal-local revision covers unknown response targets
/// without enumerating an unbounded target set; incarnation and database identity fence wipes/ABA.
#[derive(Debug, Clone)]
pub struct DialogCacheFence {
    snapshot: MessageSearchSnapshot,
    incarnation: u64,
}
impl MessageCacheFence {
    /// Target generation for a persisted outgoing intent, never a journal cursor.
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Durable principal incarnation for persisted outgoing intent fencing.
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }
}

/// Select actual journal progress or same-cursor hydration with its original request proof.
#[derive(Clone, Copy)]
enum CacheWrite<'a> {
    Journal(i64),
    JournalFenced(i64, &'a MessageCacheFence),
    Snapshot(&'a MessageCacheFence),
    Outgoing(&'a OutgoingJob),
}

/// Bounded inventory projection; listing jobs never loads every retained payload/result into memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutgoingJobSummary {
    pub job_id: String,
    pub target_key: String,
    pub method: String,
    pub state: OutgoingState,
    pub revision: i64,
}

/// Sending states deliberately distinguish local preparation from uncertain remote execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum OutgoingState {
    Prepared = 0,
    Dispatching = 1,
    Unconfirmed = 2,
    Accepted = 3,
    Rejected = 4,
    /// A known transient refusal; an explicit exact-request retry may be selected.
    Transient = 5,
    /// Server retained a scheduled message, not yet accepted into delivered history.
    AcceptedPending = 6,
}

impl OutgoingState {
    /// Decode only known persisted states; corruption never authorizes a retry.
    fn decode(value: i64) -> Result<Self, StorageError> {
        match value {
            0 => Ok(Self::Prepared),
            1 => Ok(Self::Dispatching),
            2 => Ok(Self::Unconfirmed),
            3 => Ok(Self::Accepted),
            4 => Ok(Self::Rejected),
            5 => Ok(Self::Transient),
            6 => Ok(Self::AcceptedPending),
            _ => Err(StorageError::Corrupt("invalid outgoing state".into())),
        }
    }
}

/// Raw `files` row as read from SQLite before mapping into [`FileRow`].
type FileRowTuple = (i64, Vec<u8>, String, String, i32, i64, String);
/// Stored acceptance/date/revision ordering tuple, independent of journal progress.
type BodyOrdering = (Option<i64>, Option<i64>, Option<i32>, Option<i64>);

/// Raw `file_transfers` row as read from SQLite before mapping into
/// [`FileTransferJob`].
type FileTransferJobRow = (
    String,
    String,
    i64,
    String,
    String,
    String,
    String,
    i64,
    i64,
);

/// Raw `background_sync_jobs` row as read from SQLite before mapping into
/// [`BackgroundSyncJob`].
type BackgroundSyncJobRow = (
    String,
    String,
    String,
    Vec<u8>,
    i64,
    i64,
    Option<String>,
    i64,
);

/// Cache writes applied atomically with one journal checkpoint advance.
/// `read_state_scopes` lists journal targets whose dialogs must be marked
/// `read_state_pending` in the same transaction.
/// `remove_messages` lists `(target_key, message_id)` rows to delete
/// (Deleted/Revoked journal changes disclose identity and kind only).
#[derive(Debug, Default, Clone)]
pub struct CacheOps {
    pub messages: Vec<MessageRow>,
    /// Positive authoritative body versions keyed by message ID in this single-target batch.
    /// Absent entries mean unknown; they are never synthesized from event/cursor positions.
    pub message_body_revisions: Vec<(i64, i64)>,
    /// Nonterminal `(target, message_id, minimum_body_revision)` invalidations.
    /// They evict old bodies/refs without recording a permanent viewer deletion.
    pub body_invalidations: Vec<(String, i64, i64)>,
    pub dialogs: Vec<DialogRow>,
    pub users: Vec<UserRow>,
    pub chats: Vec<ChatRow>,
    pub read_state_scopes: Vec<String>,
    pub remove_messages: Vec<(String, i64)>,
    /// Canonical file metadata upserts (immutable, first-wins).
    pub files: Vec<FileRow>,
    /// Message → file refs (idempotent).
    pub message_files: Vec<MessageFileRef>,
    /// Entity → file refs (idempotent).
    pub entity_files: Vec<EntityFileRef>,
    /// `(kind, entity_id)` whose current ref set is replaced by
    /// `entity_files` in the same transaction. Populated for every hydrated
    /// entity (even when its new ref set is empty) so avatar removals clear
    /// stale refs instead of leaking them.
    pub replace_entity_files: Vec<(String, i64)>,
}

/// Durable per-conversation validity + fencing generation.
///
/// `generation` is bumped on every [`Storage::mark_rebuild`] and preserved
/// across checkpoint advances. Upper layers snapshot it via
/// [`Storage::stream_state`] (or the return value of
/// [`Storage::install_snapshot`]) and pass it as `expected_generation` to
/// [`Storage::apply_journal_page_fenced`] / [`Storage::upsert_dialog_if_newer`];
/// a mismatch means a rebuild raced the write and the batch must be dropped.
/// An untouched principal starts at generation 0. After a principal clear,
/// missing targets inherit a durable watermark above every retired generation.
/// `replay_floor` is retained but
/// always zero under the journal model.
#[derive(Debug, Clone)]
pub struct StreamState {
    pub valid: bool,
    pub replay_floor: u64,
    pub head: Option<u64>,
    pub generation: u64,
}

/// Shared SQLite storage.
pub struct Storage {
    inner: Mutex<Connection>,
}

impl Storage {
    pub fn open(path: &str) -> Result<Self, StorageError> {
        if path.is_empty() {
            return Err(StorageError::InvalidInput(
                "db path must not be empty".into(),
            ));
        }
        let conn = Connection::open(Path::new(path))?;
        let s = Self {
            inner: Mutex::new(conn),
        };
        s.init()?;
        Ok(s)
    }

    pub fn open_in_memory() -> Result<Self, StorageError> {
        let conn = Connection::open_in_memory()?;
        let s = Self {
            inner: Mutex::new(conn),
        };
        s.init()?;
        Ok(s)
    }

    fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool, StorageError> {
        let sql = format!("SELECT name FROM pragma_table_info('{table}')");
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            if name == column {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// DDL for the v4 file-cache subsystem. Additive only: existing
    /// tables are never altered here. All statements use `IF NOT EXISTS`
    /// so both fresh `init()` and `migrate_3_to_4` replays are idempotent.
    /// `file_cache_entries` intentionally has NO hard `FOREIGN KEY` to
    /// `files` (logical link only): scoped rebuilds must preserve bytes on
    /// disk even when the `files` metadata row is garbage-collected, and a
    /// CASCADE would delete bytes while NO ACTION would block the collect.
    const V4_DDL: &'static str = "
              CREATE TABLE IF NOT EXISTS files(
                principal TEXT NOT NULL, scope TEXT NOT NULL DEFAULT '',
                file_id INTEGER NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                sha256 BLOB,
                mime_type TEXT NOT NULL DEFAULT '',
                file_name TEXT NOT NULL DEFAULT '',
                encrypted INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL DEFAULT 0,
                origin_stream TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(principal, scope, file_id));
              CREATE INDEX IF NOT EXISTS idx_files_origin
                ON files(principal, origin_stream);
              CREATE TABLE IF NOT EXISTS file_cache_entries(
                principal TEXT NOT NULL, scope TEXT NOT NULL DEFAULT '',
                file_id INTEGER NOT NULL,
                relative_path TEXT NOT NULL DEFAULT '',
                actual_size INTEGER NOT NULL DEFAULT 0,
                availability TEXT NOT NULL DEFAULT 'missing',
                last_access INTEGER NOT NULL DEFAULT 0,
                pinned INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(principal, scope, file_id));
              CREATE INDEX IF NOT EXISTS idx_file_cache_pinned
                ON file_cache_entries(principal, pinned);
              CREATE TABLE IF NOT EXISTS message_files(
                principal TEXT NOT NULL, conversation_key TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                file_scope TEXT NOT NULL DEFAULT '',
                file_id INTEGER NOT NULL,
                role TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(principal, conversation_key, message_id, file_scope, file_id, role));
              CREATE INDEX IF NOT EXISTS idx_message_files_refcount
                ON message_files(principal, file_scope, file_id);
              CREATE INDEX IF NOT EXISTS idx_message_files_conv
                ON message_files(principal, conversation_key, message_id);
              CREATE TABLE IF NOT EXISTS entity_files(
                principal TEXT NOT NULL, kind TEXT NOT NULL,
                entity_id INTEGER NOT NULL,
                file_scope TEXT NOT NULL DEFAULT '',
                file_id INTEGER NOT NULL,
                role TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(principal, kind, entity_id, file_scope, file_id, role));
              CREATE INDEX IF NOT EXISTS idx_entity_files_refcount
                ON entity_files(principal, file_scope, file_id);
              CREATE TABLE IF NOT EXISTS file_transfers(
                principal TEXT NOT NULL, job_id TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT '',
                file_scope TEXT NOT NULL DEFAULT '',
                file_id INTEGER NOT NULL DEFAULT 0,
                upload_id TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT '',
                session_json TEXT NOT NULL DEFAULT '',
                temp_path TEXT NOT NULL DEFAULT '',
                resume_offset INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(principal, job_id));
              CREATE INDEX IF NOT EXISTS idx_file_transfers_state
                ON file_transfers(principal, state);
              CREATE TABLE IF NOT EXISTS background_sync_jobs(
                principal TEXT NOT NULL, job_id TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT '',
                target_key TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT '',
                payload BLOB NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_due INTEGER NOT NULL DEFAULT 0,
                claimed_by TEXT,
                updated_at INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(principal, job_id));
              CREATE INDEX IF NOT EXISTS idx_sync_jobs_due
                ON background_sync_jobs(principal, state, next_due);";

    fn init(&self) -> Result<(), StorageError> {
        let mut conn = self
            .inner
            .lock()
            .map_err(|_| StorageError::Corrupt("storage lock poisoned during init".into()))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             PRAGMA busy_timeout=5000;",
        )?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS checkpoints(
               principal TEXT NOT NULL, stream TEXT NOT NULL, cursor INTEGER NOT NULL,
               PRIMARY KEY(principal, stream));
             CREATE TABLE IF NOT EXISTS stream_state(
               principal TEXT NOT NULL, stream TEXT NOT NULL,
               valid INTEGER NOT NULL DEFAULT 1,
               replay_floor INTEGER NOT NULL DEFAULT 0,
               head INTEGER,
               generation INTEGER NOT NULL DEFAULT 0,
               PRIMARY KEY(principal, stream));
             CREATE TABLE IF NOT EXISTS messages(
               principal TEXT NOT NULL, conversation_key TEXT NOT NULL,
               message_id INTEGER NOT NULL, peer_kind TEXT NOT NULL, peer_id INTEGER NOT NULL,
               from_id INTEGER NOT NULL, body TEXT NOT NULL, proto BLOB NOT NULL,
               origin_stream TEXT NOT NULL DEFAULT '',
               accept_seq INTEGER,
               edit_date_secs INTEGER,
               edit_date_nanos INTEGER,
               PRIMARY KEY(principal, conversation_key, message_id));
             CREATE INDEX IF NOT EXISTS idx_messages_conv
               ON messages(principal, conversation_key, message_id);
             CREATE TABLE IF NOT EXISTS dialogs(
               principal TEXT NOT NULL, peer_kind TEXT NOT NULL, peer_id INTEGER NOT NULL,
               top_message INTEGER NOT NULL DEFAULT 0, unread_count INTEGER NOT NULL DEFAULT 0,
               read_inbox_max_id INTEGER NOT NULL DEFAULT 0,
               read_outbox_max_id INTEGER NOT NULL DEFAULT 0,
               pinned INTEGER NOT NULL DEFAULT 0, proto BLOB NOT NULL,
               origin_stream TEXT NOT NULL DEFAULT '',
               read_state_pending INTEGER NOT NULL DEFAULT 0,
               PRIMARY KEY(principal, peer_kind, peer_id));
             CREATE TABLE IF NOT EXISTS users(
               principal TEXT NOT NULL, user_id INTEGER NOT NULL, proto BLOB NOT NULL,
               PRIMARY KEY(principal, user_id));
              CREATE TABLE IF NOT EXISTS chats(
               principal TEXT NOT NULL, chat_id INTEGER NOT NULL, proto BLOB NOT NULL,
               PRIMARY KEY(principal, chat_id));",
        )?;
        conn.execute_batch(Self::V4_DDL)?;
        let tx = conn.transaction()?;
        let version: Option<String> = tx
            .query_row(
                "SELECT value FROM meta WHERE key='schema_version'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        match version {
            None => {
                Self::migrate_1_to_2(&tx)?;
                Self::migrate_2_to_3(&tx)?;
                Self::migrate_3_to_4(&tx)?;
            }
            Some(v) => {
                let parsed: i64 = v
                    .parse()
                    .map_err(|_| StorageError::Corrupt("bad schema_version in meta".into()))?;
                if parsed == 1 {
                    Self::migrate_1_to_2(&tx)?;
                    Self::migrate_2_to_3(&tx)?;
                    Self::migrate_3_to_4(&tx)?;
                } else if parsed == 2 {
                    Self::migrate_2_to_3(&tx)?;
                    Self::migrate_3_to_4(&tx)?;
                } else if parsed == 3 {
                    Self::migrate_3_to_4(&tx)?;
                } else if parsed == 4
                    || parsed == 5
                    || parsed == 6
                    || parsed == 7
                    || parsed == 8
                    || parsed == SCHEMA_VERSION
                {
                    if !Self::has_column(&tx, "stream_state", "generation")? {
                        tx.execute_batch(
                            "ALTER TABLE stream_state ADD COLUMN generation INTEGER NOT NULL DEFAULT 0;",
                        )?;
                    }
                    for (table, col, ddl) in [
                        (
                            "messages",
                            "accept_seq",
                            "ALTER TABLE messages ADD COLUMN accept_seq INTEGER;",
                        ),
                        (
                            "messages",
                            "edit_date_secs",
                            "ALTER TABLE messages ADD COLUMN edit_date_secs INTEGER;",
                        ),
                        (
                            "messages",
                            "edit_date_nanos",
                            "ALTER TABLE messages ADD COLUMN edit_date_nanos INTEGER;",
                        ),
                    ] {
                        if !Self::has_column(&tx, table, col)? {
                            tx.execute_batch(ddl)?;
                        }
                    }
                    // Repair path: a v4 row whose tables were partially
                    // created still converges (DDL is IF NOT EXISTS).
                    tx.execute_batch(Self::V4_DDL)?;
                } else {
                    return Err(StorageError::Corrupt(format!(
                        "unsupported schema_version {parsed}, expected {SCHEMA_VERSION}"
                    )));
                }
            }
        }
        if !Self::has_column(&tx, "messages", "body_revision")? {
            tx.execute_batch("ALTER TABLE messages ADD COLUMN body_revision INTEGER;")?;
        }
        Self::init_search_schema(&tx)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS outgoing_jobs(
            principal TEXT NOT NULL, source TEXT NOT NULL, job_id TEXT NOT NULL,
            target_key TEXT NOT NULL, method TEXT NOT NULL, payload BLOB NOT NULL,
            state INTEGER NOT NULL DEFAULT 0, response BLOB NOT NULL DEFAULT X'',
            revision INTEGER NOT NULL DEFAULT 0, cache_generation INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(principal,source,job_id));
            CREATE TABLE IF NOT EXISTS cache_incarnations(
                principal TEXT PRIMARY KEY, incarnation INTEGER NOT NULL
                CHECK(typeof(incarnation)='integer' AND incarnation>=0));
             CREATE TABLE IF NOT EXISTS message_tombstones(
                principal TEXT NOT NULL,conversation_key TEXT NOT NULL,message_id INTEGER NOT NULL,
                 last_body_revision INTEGER,PRIMARY KEY(principal,conversation_key,message_id));
             CREATE TABLE IF NOT EXISTS message_body_floors(
                 principal TEXT NOT NULL,conversation_key TEXT NOT NULL,message_id INTEGER NOT NULL,
                 minimum_revision INTEGER NOT NULL CHECK(minimum_revision>0),
                 PRIMARY KEY(principal,conversation_key,message_id));
             INSERT INTO meta(key,value) VALUES('schema_version','9')
                 ON CONFLICT(key) DO UPDATE SET value='9';",
        )?;
        if !Self::has_column(&tx, "outgoing_jobs", "cache_incarnation")? {
            tx.execute_batch("ALTER TABLE outgoing_jobs ADD COLUMN cache_incarnation INTEGER NOT NULL DEFAULT 0;")?;
        }
        if !Self::has_column(&tx, "outgoing_jobs", "outcome_code")? {
            tx.execute_batch(
                "ALTER TABLE outgoing_jobs ADD COLUMN outcome_code INTEGER;
                ALTER TABLE outgoing_jobs ADD COLUMN outcome_raw INTEGER;
                ALTER TABLE outgoing_jobs ADD COLUMN outcome_unknown INTEGER;",
            )?;
        }
        // Repair previously invalidated caches, including rows whose old global origin hid
        // their canonical conversation ownership from the former rebuild implementation.
        tx.execute_batch("DELETE FROM message_files WHERE EXISTS(
            SELECT 1 FROM stream_state s WHERE s.principal=message_files.principal AND s.valid=0 AND s.stream=message_files.conversation_key);
            DELETE FROM messages WHERE EXISTS(SELECT 1 FROM stream_state s WHERE s.principal=messages.principal AND s.valid=0 AND (s.stream=messages.conversation_key OR s.stream=messages.origin_stream));
            DELETE FROM dialogs WHERE EXISTS(SELECT 1 FROM stream_state s WHERE s.principal=dialogs.principal AND s.valid=0 AND (s.stream=dialogs.origin_stream OR s.stream='target:'||dialogs.peer_kind||':'||dialogs.peer_id));")?;
        tx.commit()?;
        Ok(())
    }

    /// Install and backfill the FTS index transactionally. The mapping stores keys only;
    /// its explicit AUTOINCREMENT primary key survives VACUUM and is never reused.
    /// A view supplies bodies directly from messages, with no second full-text copy.
    fn init_search_schema(conn: &Connection) -> Result<(), StorageError> {
        let installed: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='message_search')",
            [],
            |r| r.get(0),
        )?;
        conn.execute_batch("
            CREATE INDEX IF NOT EXISTS idx_messages_search_source
                ON messages(principal,conversation_key DESC,accept_seq DESC,message_id DESC)
                WHERE accept_seq>0 AND message_id>0;
            INSERT OR IGNORE INTO meta(key,value) VALUES('search_database_id', lower(hex(randomblob(16))));
            CREATE TABLE IF NOT EXISTS message_search_revisions(
                principal TEXT PRIMARY KEY, revision INTEGER NOT NULL
                CHECK(typeof(revision)='integer' AND revision>=0));
            CREATE TABLE IF NOT EXISTS message_documents(
                document_id INTEGER PRIMARY KEY AUTOINCREMENT,
                principal TEXT NOT NULL, conversation_key TEXT NOT NULL, message_id INTEGER NOT NULL,
                UNIQUE(principal, conversation_key, message_id));
            CREATE VIEW IF NOT EXISTS message_search_content AS
                SELECT d.document_id, m.body FROM message_documents d JOIN messages m
                USING(principal, conversation_key, message_id);
            CREATE VIRTUAL TABLE IF NOT EXISTS message_search USING fts5(
                body, content='message_search_content', content_rowid='document_id',
                tokenize='unicode61 remove_diacritics 2');
            CREATE TRIGGER IF NOT EXISTS messages_search_insert AFTER INSERT ON messages BEGIN
                INSERT INTO message_documents(principal,conversation_key,message_id)
                    VALUES(new.principal,new.conversation_key,new.message_id);
                INSERT INTO message_search(rowid,body)
                    SELECT document_id,new.body FROM message_documents
                    WHERE principal=new.principal AND conversation_key=new.conversation_key AND message_id=new.message_id;
                INSERT INTO message_search_revisions VALUES(new.principal,1)
                    ON CONFLICT(principal) DO UPDATE SET revision=revision+1;
            END;
            CREATE TRIGGER IF NOT EXISTS messages_search_delete AFTER DELETE ON messages BEGIN
                INSERT INTO message_search(message_search,rowid,body)
                    SELECT 'delete',document_id,old.body FROM message_documents
                    WHERE principal=old.principal AND conversation_key=old.conversation_key AND message_id=old.message_id;
                DELETE FROM message_documents WHERE principal=old.principal
                    AND conversation_key=old.conversation_key AND message_id=old.message_id;
                INSERT INTO message_search_revisions VALUES(old.principal,1)
                    ON CONFLICT(principal) DO UPDATE SET revision=revision+1;
            END;
            CREATE TRIGGER IF NOT EXISTS messages_search_update AFTER UPDATE ON messages BEGIN
                INSERT INTO message_search(message_search,rowid,body)
                    SELECT 'delete',document_id,old.body FROM message_documents
                    WHERE principal=old.principal AND conversation_key=old.conversation_key AND message_id=old.message_id;
                UPDATE message_documents SET principal=new.principal,conversation_key=new.conversation_key,message_id=new.message_id
                    WHERE principal=old.principal AND conversation_key=old.conversation_key AND message_id=old.message_id;
                INSERT INTO message_search(rowid,body)
                    SELECT document_id,new.body FROM message_documents
                    WHERE principal=new.principal AND conversation_key=new.conversation_key AND message_id=new.message_id;
                INSERT INTO message_search_revisions VALUES(old.principal,1)
                    ON CONFLICT(principal) DO UPDATE SET revision=revision+1;
                INSERT INTO message_search_revisions SELECT new.principal,1 WHERE new.principal!=old.principal
                    ON CONFLICT(principal) DO UPDATE SET revision=revision+1;
            END;
        ")?;
        if !installed {
            conn.execute_batch(
                "
                INSERT INTO message_documents(principal,conversation_key,message_id)
                    SELECT principal,conversation_key,message_id FROM messages;
                INSERT INTO message_search(message_search) VALUES('rebuild');
            ",
            )?;
        }
        conn.execute_batch(
            "INSERT INTO meta(key,value) VALUES('schema_version','5')
            ON CONFLICT(key) DO UPDATE SET value='5';",
        )?;
        Ok(())
    }

    /// v1 → v2: provenance columns, dialog pending flag, generation fencing
    /// column, and the shared provenance table. Idempotent.
    fn migrate_1_to_2(conn: &Connection) -> Result<(), StorageError> {
        if !Self::has_column(conn, "messages", "origin_stream")? {
            conn.execute_batch(
                "ALTER TABLE messages ADD COLUMN origin_stream TEXT NOT NULL DEFAULT '';",
            )?;
        }
        if !Self::has_column(conn, "dialogs", "origin_stream")? {
            conn.execute_batch(
                "ALTER TABLE dialogs ADD COLUMN origin_stream TEXT NOT NULL DEFAULT '';",
            )?;
        }
        if !Self::has_column(conn, "dialogs", "read_state_pending")? {
            conn.execute_batch(
                "ALTER TABLE dialogs ADD COLUMN read_state_pending INTEGER NOT NULL DEFAULT 0;",
            )?;
        }
        if !Self::has_column(conn, "stream_state", "generation")? {
            conn.execute_batch(
                "ALTER TABLE stream_state ADD COLUMN generation INTEGER NOT NULL DEFAULT 0;",
            )?;
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS entity_provenance(
               principal TEXT NOT NULL, kind TEXT NOT NULL, entity_id INTEGER NOT NULL,
               stream TEXT NOT NULL,
               PRIMARY KEY(principal, kind, entity_id, stream));
             CREATE INDEX IF NOT EXISTS idx_messages_origin
               ON messages(principal, origin_stream);
             CREATE INDEX IF NOT EXISTS idx_dialogs_origin
               ON dialogs(principal, origin_stream);
             INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
               SELECT principal, 'user', user_id, '' FROM users;
             INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
               SELECT principal, 'chat', chat_id, '' FROM chats;
             INSERT INTO meta(key, value) VALUES('schema_version', '2')
               ON CONFLICT(key) DO UPDATE SET value='2';",
        )?;
        Ok(())
    }

    /// v2 → v3: per-conversation journal keys, message dedup columns,
    /// `replay_floor` zeroing. Transactional and idempotent (same pattern
    /// as v2).
    ///
    /// - `stream_state` / `entity_provenance` / message and dialog
    ///   `origin_stream` / message `conversation_key` rows migrate from
    ///   `user:<id>` / `room:<id>` to `target:user:<id>` / `target:room:<id>`
    ///   (`ChatId` maps to `room`). Rows already carrying the `target:`
    ///   prefix are left alone, so re-running is a no-op.
    /// - Legacy `checkpoints` rows are DELETED, not rekeyed: v2 cursors are
    ///   stream-replay positions, not journal `accept_seq` positions, and
    ///   reusing them as journal cursors would skip reconciliation from a
    ///   bogus position. Journaling restarts at 0 per conversation.
    ///   `target:`-prefixed checkpoint rows (already v3) are preserved.
    /// - `messages` gains nullable `accept_seq`, `edit_date_secs`,
    ///   `edit_date_nanos` for journal dedup.
    /// - `stream_state.replay_floor` is reset to 0 on every row (retained
    ///   column, always zero under the journal model).
    fn migrate_2_to_3(conn: &Connection) -> Result<(), StorageError> {
        for (col, ddl) in [
            (
                "accept_seq",
                "ALTER TABLE messages ADD COLUMN accept_seq INTEGER;",
            ),
            (
                "edit_date_secs",
                "ALTER TABLE messages ADD COLUMN edit_date_secs INTEGER;",
            ),
            (
                "edit_date_nanos",
                "ALTER TABLE messages ADD COLUMN edit_date_nanos INTEGER;",
            ),
        ] {
            if !Self::has_column(conn, "messages", col)? {
                conn.execute_batch(ddl)?;
            }
        }
        conn.execute_batch(
            "DELETE FROM checkpoints WHERE stream NOT LIKE 'target:%';
             UPDATE stream_state SET stream='target:' || stream WHERE stream NOT LIKE 'target:%';
             UPDATE stream_state SET replay_floor=0;
             UPDATE messages SET conversation_key='target:' || conversation_key
               WHERE conversation_key NOT LIKE 'target:%'
               AND (conversation_key LIKE 'room:%' OR conversation_key LIKE 'user:%');
             UPDATE messages SET origin_stream='target:' || origin_stream
               WHERE origin_stream != '' AND origin_stream NOT LIKE 'target:%'
               AND (origin_stream LIKE 'room:%' OR origin_stream LIKE 'user:%');
             UPDATE dialogs SET origin_stream='target:' || origin_stream
               WHERE origin_stream != '' AND origin_stream NOT LIKE 'target:%'
               AND (origin_stream LIKE 'room:%' OR origin_stream LIKE 'user:%');
             UPDATE entity_provenance SET stream='target:' || stream
               WHERE stream != '' AND stream NOT LIKE 'target:%'
               AND (stream LIKE 'room:%' OR stream LIKE 'user:%');
              INSERT INTO meta(key, value) VALUES('schema_version', '3')
                ON CONFLICT(key) DO UPDATE SET value='3';",
        )?;
        Ok(())
    }

    /// v3 → v4: file-cache subsystem. Transactional and idempotent (same
    /// pattern as v2 → v3).
    ///
    /// - Creates `files`, `file_cache_entries`, `message_files`,
    ///   `entity_files`, `file_transfers`, `background_sync_jobs` with
    ///   `IF NOT EXISTS`, so re-running is a no-op.
    /// - Existing tables are never altered or backfilled: message, dialog,
    ///   user, chat, checkpoint, and provenance rows are untouched.
    /// - Bumps `meta.schema_version` to `4` in the same transaction as the
    ///   DDL, so a crash before commit replays the migration and a crash
    ///   after commit skips it.
    fn migrate_3_to_4(conn: &Connection) -> Result<(), StorageError> {
        conn.execute_batch(Self::V4_DDL)?;
        conn.execute_batch(
            "INSERT INTO meta(key, value) VALUES('schema_version', '4')
               ON CONFLICT(key) DO UPDATE SET value='4';",
        )?;
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Corrupt("storage lock poisoned".into()))
    }

    /// A durable, non-content wipe fence, also the minimum valid target generation.
    /// Clear allocates it above all retired target versions; rebuilds allocate above
    /// that floor. These are one local invalidation namespace, never server positions
    /// or runtime/authorization epochs. It survives deletion of every target/state row.
    fn cache_incarnation(conn: &Connection, principal: &str) -> Result<u64, StorageError> {
        let value: i64 = conn.query_row(
            "SELECT COALESCE((SELECT incarnation FROM cache_incarnations WHERE principal=?1),0)",
            [principal],
            |r| r.get(0),
        )?;
        u64::try_from(value).map_err(|_| StorageError::Corrupt("invalid cache incarnation".into()))
    }

    /// Read the durable principal wipe fence for other cache workers (including transfer proofs).
    /// It is opaque and may jump past retired target generations; compare for equality, not as
    /// a count/time. It is not authorization or a journal position. Async callers use a worker.
    pub fn cache_principal_incarnation(&self, principal: &str) -> Result<u64, StorageError> {
        check_principal(principal)?;
        let conn = self.lock()?;
        Self::cache_incarnation(&conn, principal)
    }

    /// Capture target validity/generation and principal incarnation in one read snapshot before I/O.
    pub fn capture_message_scope(
        &self,
        principal: &str,
        target_key: &str,
    ) -> Result<MessageCacheFence, StorageError> {
        check_principal(principal)?;
        check_target_key(target_key)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let state:Option<(i64,i64)>=tx.query_row("SELECT valid,COALESCE(generation,0) FROM stream_state WHERE principal=?1 AND stream=?2",params![principal,target_key],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let (valid, generation) = state.unwrap_or((1, 0));
        let incarnation = Self::cache_incarnation(&tx, principal)?;
        Ok(MessageCacheFence {
            database_id: tx.query_row(
                "SELECT value FROM meta WHERE key='search_database_id'",
                [],
                |r| r.get(0),
            )?,
            principal: principal.into(),
            target_key: target_key.into(),
            generation: u64::try_from(generation)
                .map_err(|_| StorageError::Corrupt("invalid target generation".into()))?
                .max(incarnation),
            incarnation,
            valid: valid == 1,
        })
    }

    /// Capture a bounded principal-local proof before a global dialog request starts.
    pub fn capture_dialog_scope(&self, principal: &str) -> Result<DialogCacheFence, StorageError> {
        check_principal(principal)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        Ok(DialogCacheFence {
            snapshot: Self::search_snapshot(&tx, principal)?,
            incarnation: Self::cache_incarnation(&tx, principal)?,
        })
    }

    /// Validate visibility and commit dialog/entity hydration in one transaction. Reject a known
    /// removed head/body rather than inventing replacement heads or unread counts. Never advances journals.
    pub fn hydrate_dialog_snapshot_fenced(
        &self,
        principal: &str,
        dialogs: &[DialogRow],
        bodies: &[(String, i64)],
        entities: &CacheOps,
        fence: &DialogCacheFence,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        if dialogs.len() > 256 || bodies.len() > 512 {
            return Err(StorageError::InvalidInput(
                "dialog snapshot too large".into(),
            ));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        if fence.snapshot.principal != principal
            || Self::search_snapshot(&tx, principal)? != fence.snapshot
            || Self::cache_incarnation(&tx, principal)? != fence.incarnation
        {
            return Err(StorageError::CacheScopeChanged);
        }
        let check_identity = |target: &str, id: i64| -> Result<(), StorageError> {
            check_target_key(target)?;
            if id < 0 {
                return Err(StorageError::InvalidInput("invalid dialog message".into()));
            }
            let invalid:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM stream_state WHERE principal=?1 AND stream=?2 AND valid=0) OR EXISTS(SELECT 1 FROM message_tombstones WHERE principal=?1 AND conversation_key=?2 AND message_id=?3)",params![principal,target,id],|r|r.get(0))?;
            if invalid {
                return Err(StorageError::CacheScopeChanged);
            }
            Ok(())
        };
        for d in dialogs {
            check_identity(&Self::dialog_target(d)?, d.top_message)?;
        }
        for (target, id) in bodies {
            if *id <= 0 {
                return Err(StorageError::InvalidInput("invalid dialog body".into()));
            }
            check_identity(target, *id)?;
        }
        Self::hydrate_entity_snapshot_tx(&tx, principal, entities)?;
        for d in dialogs {
            Self::write_dialog_authoritative(&tx, principal, d, true)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Canonical conversation identity for a typed dialog row; origins never replace ownership.
    fn dialog_target(row: &DialogRow) -> Result<String, StorageError> {
        let kind = match row.peer_kind.as_str() {
            "user" => "user",
            "room" | "chat" => "room",
            _ => return Err(StorageError::InvalidInput("invalid dialog peer".into())),
        };
        if row.peer_id <= 0 {
            return Err(StorageError::InvalidInput("invalid dialog peer".into()));
        }
        Ok(format!("target:{kind}:{}", row.peer_id))
    }

    /// Canonical conversation ownership dominates an empty or historical provenance label.
    fn purge_target_content(
        conn: &Connection,
        principal: &str,
        target_key: &str,
    ) -> Result<(), StorageError> {
        conn.execute("DELETE FROM message_files WHERE principal=?1 AND (conversation_key=?2 OR EXISTS(SELECT 1 FROM messages m WHERE m.principal=message_files.principal AND m.conversation_key=message_files.conversation_key AND m.message_id=message_files.message_id AND m.origin_stream=?2))",params![principal,target_key])?;
        conn.execute(
            "DELETE FROM messages WHERE principal=?1 AND (conversation_key=?2 OR origin_stream=?2)",
            params![principal, target_key],
        )?;
        conn.execute("DELETE FROM dialogs WHERE principal=?1 AND (origin_stream=?2 OR 'target:'||CASE peer_kind WHEN 'chat' THEN 'room' ELSE peer_kind END||':'||peer_id=?2)",params![principal,target_key])?;
        Ok(())
    }

    /// Validating an unknown/invalid scope never makes its old cached bodies visible again.
    fn prepare_scope_validation(
        conn: &Connection,
        principal: &str,
        target_key: &str,
    ) -> Result<(), StorageError> {
        let valid: Option<i64> = conn
            .query_row(
                "SELECT valid FROM stream_state WHERE principal=?1 AND stream=?2",
                params![principal, target_key],
                |r| r.get(0),
            )
            .optional()?;
        if valid != Some(1) {
            Self::purge_target_content(conn, principal, target_key)?;
            Self::bump_search_revision(conn, principal)?;
        }
        Ok(())
    }

    // -- journal checkpoints -----------------------------------------------

    /// Durable journal cursor for one conversation: the last applied
    /// `ListMessageChanges.last_accept_seq` (0 when nothing applied yet).
    pub fn load_journal(
        &self,
        principal: &str,
        target_key: &str,
    ) -> Result<Option<i64>, StorageError> {
        check_principal(principal)?;
        check_target_key(target_key)?;
        let conn = self.lock()?;
        let v: Option<i64> = conn
            .query_row(
                "SELECT cursor FROM checkpoints WHERE principal=?1 AND stream=?2",
                params![principal, target_key],
                |r| r.get(0),
            )
            .optional()?;
        Ok(v)
    }

    /// Persist one conversation's journal cursor plus `valid=1`
    /// (preserving `generation`). `accept_seq` 0 is the initial position.
    pub fn save_journal(
        &self,
        principal: &str,
        target_key: &str,
        accept_seq: i64,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_target_key(target_key)?;
        check_accept_seq(accept_seq)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        Self::prepare_scope_validation(&tx, principal, target_key)?;
        let generation = Self::cache_incarnation(&tx, principal)?;
        tx.execute(
            "INSERT INTO checkpoints(principal, stream, cursor) VALUES(?1,?2,?3)
             ON CONFLICT(principal, stream) DO UPDATE SET cursor=excluded.cursor",
            params![principal, target_key, accept_seq],
        )?;
        tx.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor,generation)
             VALUES(?1,?2,1,0,?3)
             ON CONFLICT(principal, stream) DO UPDATE SET valid=1, replay_floor=0,generation=max(COALESCE(generation,0),excluded.generation)",
            params![principal, target_key,generation as i64],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Drop one conversation's journal cursor (checkpoint row only).
    ///
    /// Room history-scope invalidation uses this: dropping the cursor makes
    /// the next reconcile start at 0. When the retained journal skips past
    /// the dropped position, callers heal via a `GetHistory` fallback rather
    /// than treating the skip as fatal (see `Client::reconcile_target`).
    pub fn remove_target(&self, principal: &str, target_key: &str) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_target_key(target_key)?;
        let conn = self.lock()?;
        conn.execute(
            "DELETE FROM checkpoints WHERE principal=?1 AND stream=?2",
            params![principal, target_key],
        )?;
        Ok(())
    }

    /// Compat: durable cursor read as `u64` (journal values 0..=MAX fit).
    pub fn load_checkpoint(
        &self,
        principal: &str,
        stream: &str,
    ) -> Result<Option<u64>, StorageError> {
        Ok(self.load_journal(principal, stream)?.map(|v| v as u64))
    }

    /// Compat: durable cursor write from `u64`.
    pub fn save_checkpoint(
        &self,
        principal: &str,
        stream: &str,
        cursor: u64,
    ) -> Result<(), StorageError> {
        check_cursor_u64(cursor)?;
        self.save_journal(principal, stream, cursor as i64)
    }

    /// Compat: drop one conversation's checkpoint row.
    pub fn remove_stream(&self, principal: &str, stream: &str) -> Result<(), StorageError> {
        self.remove_target(principal, stream)
    }

    /// Remove cached content and work owned by `principal`: checkpoints, stream_state,
    /// cached messages/dialogs/users/chats, and the v4 file-cache subsystem
    /// (`files`, `file_cache_entries`, `message_files`, `entity_files`,
    /// `file_transfers`, `background_sync_jobs`). Logout-scoped wipe so a
    /// signed-out principal leaves no searchable offline bodies behind. A non-content
    /// search revision tombstone remains to reject results and cursors from before the wipe.
    pub fn clear_principal(&self, principal: &str) -> Result<(), StorageError> {
        check_principal(principal)?;
        // Remove owned artifacts before forgetting their cleanup namespace. Failure retains rows.
        let artifacts = self.preparation_directory(principal)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        match std::fs::remove_dir_all(artifacts) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {
                return Err(StorageError::Unavailable(
                    "preparation cleanup failed".into(),
                ))
            }
        }
        Self::bump_search_revision(&tx, principal)?;
        tx.execute(
            // A principal wipe allocates above every target watermark being retired.
            // Both sealed read proofs and the generation-only API reject old writers.
            "INSERT INTO cache_incarnations(principal,incarnation)
             VALUES(?1,max(COALESCE((SELECT incarnation FROM cache_incarnations WHERE principal=?1),0),COALESCE((SELECT max(generation) FROM stream_state WHERE principal=?1),0))+1)
             ON CONFLICT(principal) DO UPDATE SET incarnation=excluded.incarnation",
            [principal],
        )?;
        for table in [
            "checkpoints",
            "stream_state",
            "messages",
            "dialogs",
            "users",
            "chats",
            "entity_provenance",
            "files",
            "file_cache_entries",
            "message_files",
            "entity_files",
            "file_transfers",
            "background_sync_jobs",
            "outgoing_jobs",
            "message_tombstones",
            "message_body_floors",
        ] {
            let sql = format!("DELETE FROM {table} WHERE principal=?1");
            tx.execute(&sql, params![principal])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Durable principal wipe fence for retained preparation/transfer owners.
    pub fn principal_cache_incarnation(&self, principal: &str) -> Result<u64, StorageError> {
        check_principal(principal)?;
        let conn = self.lock()?;
        Self::cache_incarnation(&conn, principal)
    }

    // -- per-conversation validity / generation ------------------------------

    /// Create-only immutable outgoing intent. Reusing a job key for different bytes fails.
    /// The per-principal bound includes retained outcomes: explicitly discard settled jobs.
    pub fn insert_outgoing(&self, principal: &str, job: &OutgoingJob) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_job_id(&job.job_id)?;
        check_target_key(&job.target_key)?;
        if job.source.len() != 64
            || !job.source.bytes().all(|c| c.is_ascii_hexdigit())
            || !matches!(
                job.method.as_str(),
                "/tinkle.v1.MessagesService/SendMessage"
                    | "/tinkle.v1.MessagesService/SendMedia"
                    | "/tinkle.v1.MessagesService/SendAlbum"
            )
            || job.payload.is_empty()
            || job.payload.len() > 262144
            || job.state != OutgoingState::Prepared
            || job.revision != 0
            || job.cache_generation > i64::MAX as u64
            || job.cache_incarnation > i64::MAX as u64
            || !job.response.is_empty()
            || job.last_outcome.is_some()
        {
            return Err(StorageError::InvalidInput("invalid outgoing intent".into()));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        if Self::cache_incarnation(&tx, principal)? != job.cache_incarnation {
            return Err(StorageError::InvalidInput(
                "outgoing cache incarnation changed".into(),
            ));
        }
        let old: Option<(String,String,Vec<u8>)> = tx.query_row(
            "SELECT target_key,method,payload FROM outgoing_jobs WHERE principal=?1 AND source=?2 AND job_id=?3",
            params![principal, job.source, job.job_id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).optional()?;
        if let Some(old) = old {
            if old
                != (
                    job.target_key.clone(),
                    job.method.clone(),
                    job.payload.clone(),
                )
            {
                return Err(StorageError::IntentConflict);
            }
        } else {
            let count: i64 = tx.query_row(
                "SELECT count(*) FROM outgoing_jobs WHERE principal=?1",
                [principal],
                |r| r.get(0),
            )?;
            if count >= 256 {
                return Err(StorageError::InvalidInput("outgoing queue full".into()));
            }
            tx.execute("INSERT INTO outgoing_jobs(principal,source,job_id,target_key,method,payload,cache_generation,cache_incarnation) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![principal, job.source, job.job_id, job.target_key, job.method, job.payload,job.cache_generation as i64,job.cache_incarnation as i64])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Read one exact principal/source job; another source never inherits replay permission.
    pub fn outgoing(
        &self,
        principal: &str,
        source: &str,
        job_id: &str,
    ) -> Result<Option<OutgoingJob>, StorageError> {
        check_principal(principal)?;
        check_job_id(job_id)?;
        let conn = self.lock()?;
        let row = conn.query_row("SELECT target_key,method,payload,state,response,revision,cache_generation,cache_incarnation FROM outgoing_jobs WHERE principal=?1 AND source=?2 AND job_id=?3",
            params![principal,source,job_id], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,Vec<u8>>(2)?,r.get::<_,i64>(3)?,r.get::<_,Vec<u8>>(4)?,r.get::<_,i64>(5)?,r.get::<_,i64>(6)?,r.get::<_,i64>(7)?))).optional()?;
        row.map(
            |(
                target_key,
                method,
                payload,
                state,
                response,
                revision,
                cache_generation,
                cache_incarnation,
            )| {
                let last_outcome = conn.query_row("SELECT outcome_code,outcome_raw,outcome_unknown FROM outgoing_jobs WHERE principal=?1 AND source=?2 AND job_id=?3",
                    params![principal,source,job_id], |r|Ok((r.get::<_,Option<i32>>(0)?,r.get::<_,Option<i32>>(1)?,r.get::<_,Option<bool>>(2)?)))?;
                Ok(OutgoingJob {
                    job_id: job_id.into(),
                    source: source.into(),
                    target_key,
                    method,
                    payload,
                    state: OutgoingState::decode(state)?,
                    response,
                    last_outcome: last_outcome.0.map(|code|OutgoingOutcome {code,raw_code:last_outcome.1,unknown_outcome:last_outcome.2.unwrap_or(true)}),
                    revision,
                    cache_generation: u64::try_from(cache_generation).map_err(|_| {
                        StorageError::Corrupt("invalid outgoing cache generation".into())
                    })?,
                    cache_incarnation: u64::try_from(cache_incarnation)
                        .map_err(|_| StorageError::Corrupt("invalid cache incarnation".into()))?,
                })
            },
        )
        .transpose()
    }

    /// Bounded retained inventory in stable job-key order, including terminal results.
    pub fn list_outgoing(
        &self,
        principal: &str,
        source: &str,
    ) -> Result<Vec<OutgoingJobSummary>, StorageError> {
        check_principal(principal)?;
        let conn = self.lock()?;
        let mut stmt = conn.prepare("SELECT job_id,target_key,method,state,revision FROM outgoing_jobs WHERE principal=?1 AND source=?2 ORDER BY job_id LIMIT 256")?;
        let rows = stmt.query_map(params![principal, source], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get::<_, i64>(3)?,
                r.get(4)?,
            ))
        })?;
        rows.map(|row| {
            let (job_id, target_key, method, state, revision) = row?;
            Ok(OutgoingJobSummary {
                job_id,
                target_key,
                method,
                state: OutgoingState::decode(state)?,
                revision,
            })
        })
        .collect()
    }

    /// CAS only the lifecycle/result columns; never replace an intent's payload or scope.
    pub fn transition_outgoing(
        &self,
        principal: &str,
        job: &OutgoingJob,
        next: OutgoingState,
        response: &[u8],
    ) -> Result<bool, StorageError> {
        self.transition_outgoing_with_outcome(principal, job, next, response, None)
    }

    /// CAS state, exact response and structured failure as one SQLite write.
    pub fn transition_outgoing_with_outcome(
        &self,
        principal: &str,
        job: &OutgoingJob,
        next: OutgoingState,
        response: &[u8],
        outcome: Option<&OutgoingOutcome>,
    ) -> Result<bool, StorageError> {
        check_principal(principal)?;
        let allowed = matches!(
            (job.state, next),
            (
                OutgoingState::Prepared
                    | OutgoingState::Unconfirmed
                    | OutgoingState::Dispatching
                    | OutgoingState::Transient,
                OutgoingState::Dispatching
            ) | (
                OutgoingState::Dispatching,
                OutgoingState::Unconfirmed
                    | OutgoingState::Accepted
                    | OutgoingState::Rejected
                    | OutgoingState::Transient
                    | OutgoingState::AcceptedPending
            )
        );
        if !allowed || response.len() > 4 * 1024 * 1024 || job.revision == i64::MAX {
            return Err(StorageError::InvalidInput(
                "invalid outgoing transition".into(),
            ));
        }
        let conn = self.lock()?;
        Ok(conn.execute("UPDATE outgoing_jobs SET state=?1,response=?2,revision=revision+1,outcome_code=?9,outcome_raw=?10,outcome_unknown=?11 WHERE principal=?3 AND source=?4 AND job_id=?5 AND revision=?6 AND state=?7 AND payload=?8 AND target_key=?12 AND method=?13 AND cache_incarnation=?14",
            params![next as i64,response,principal,job.source,job.job_id,job.revision,job.state as i64,job.payload,outcome.map(|o|o.code),outcome.and_then(|o|o.raw_code),outcome.map(|o|o.unknown_outcome),job.target_key,job.method,job.cache_incarnation as i64])? == 1)
    }

    /// Explicitly remove a settled or never-dispatched intent; uncertain work cannot be forgotten here.
    pub fn discard_outgoing(
        &self,
        principal: &str,
        source: &str,
        job_id: &str,
    ) -> Result<bool, StorageError> {
        check_principal(principal)?;
        check_job_id(job_id)?;
        Ok(self.lock()?.execute("DELETE FROM outgoing_jobs WHERE principal=?1 AND source=?2 AND job_id=?3 AND state IN (0,3,4,5,6)", params![principal,source,job_id])? == 1)
    }

    /// Invalidate one conversation's cache scope. Canonically owned messages/dialogs
    /// and rows carrying this target's origin are deleted, including empty origins.
    /// Users/chats are deleted only when no provenance
    /// reference remains, so rebuilding one conversation never evicts an
    /// entity still used elsewhere. File refs follow the same pattern:
    /// `message_files` rows for this conversation are deleted (their
    /// messages are gone), then `files` rows produced by this target
    /// (`origin_stream`) are garbage-collected ONLY when no refs remain in
    /// `message_files` or `entity_files` — mirroring the users/chats
    /// provenance guard. `entity_files` (global avatar/sticker pins),
    /// `file_cache_entries` (bytes on disk), `file_transfers`, and
    /// `background_sync_jobs` are never touched by a scoped rebuild.
    /// Bumps `generation` so fenced writers fail instead of resurrecting
    /// stale rows.
    pub fn mark_rebuild(&self, principal: &str, stream: &str) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_target_key(stream)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let floor = Self::cache_incarnation(&tx, principal)?;
        Self::bump_search_revision(&tx, principal)?;
        tx.execute(
            "DELETE FROM checkpoints WHERE principal=?1 AND stream=?2",
            params![principal, stream],
        )?;
        tx.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor, generation)
              VALUES(?1,?2,0,0,?3+1)
             ON CONFLICT(principal, stream)
              DO UPDATE SET valid=0, generation=max(COALESCE(generation,0),?3)+1",
            params![principal, stream, floor as i64],
        )?;
        Self::purge_target_content(&tx, principal, stream)?;
        tx.execute(
            "DELETE FROM entity_provenance WHERE principal=?1 AND stream=?2",
            params![principal, stream],
        )?;
        for (table, kind, id_col) in [("users", "user", "user_id"), ("chats", "chat", "chat_id")] {
            let sql = format!(
                "DELETE FROM {table} WHERE principal=?1 AND NOT EXISTS (
                   SELECT 1 FROM entity_provenance
                   WHERE principal=?1 AND kind=?2 AND entity_id={table}.{id_col})"
            );
            tx.execute(&sql, params![principal, kind])?;
        }
        // Scoped file refs: messages for this conversation are gone, so
        // their file refs go too. Entity refs are global pins and survive.
        tx.execute(
            "DELETE FROM message_files WHERE principal=?1 AND conversation_key=?2",
            params![principal, stream],
        )?;
        // Refcount-guarded file metadata collect: only rows produced by this
        // target (`origin_stream`), pinned `''` rows always survive, and a
        // row survives while any message/entity ref still addresses it.
        tx.execute(
            "DELETE FROM files WHERE principal=?1 AND origin_stream=?2
               AND NOT EXISTS (
                 SELECT 1 FROM message_files
                 WHERE message_files.principal=files.principal
                   AND message_files.file_scope=files.scope
                   AND message_files.file_id=files.file_id)
               AND NOT EXISTS (
                 SELECT 1 FROM entity_files
                 WHERE entity_files.principal=files.principal
                   AND entity_files.file_scope=files.scope
                   AND entity_files.file_id=files.file_id)",
            params![principal, stream],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn stream_state(&self, principal: &str, stream: &str) -> Result<StreamState, StorageError> {
        check_principal(principal)?;
        check_target_key(stream)?;
        let conn = self.lock()?;
        let minimum_generation = Self::cache_incarnation(&conn, principal)?;
        let row: Option<(i64, i64, Option<i64>, Option<i64>)> = conn
            .query_row(
                "SELECT valid, replay_floor, head, generation FROM stream_state WHERE principal=?1 AND stream=?2",
                params![principal, stream],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        Ok(match row {
            None => StreamState {
                valid: true,
                replay_floor: 0,
                head: None,
                generation: minimum_generation,
            },
            Some((valid, floor, head, gen)) => StreamState {
                valid: valid != 0,
                replay_floor: floor as u64,
                head: head.map(|h| h as u64),
                generation: (gen.unwrap_or(0) as u64).max(minimum_generation),
            },
        })
    }

    /// Atomically install a fresh journal snapshot for one conversation:
    /// checkpoint + `valid=1` + bounds commit in one transaction while
    /// preserving the current `generation`. Returns the preserved
    /// generation so callers can fence subsequent
    /// [`Self::apply_journal_page_fenced`] writes.
    pub fn install_snapshot(
        &self,
        principal: &str,
        stream: &str,
        cursor: u64,
        floor: u64,
        head: Option<u64>,
    ) -> Result<u64, StorageError> {
        check_principal(principal)?;
        check_target_key(stream)?;
        check_cursor_u64(cursor)?;
        if floor > i64::MAX as u64 {
            return Err(StorageError::InvalidInput("floor out of range".into()));
        }
        if let Some(h) = head {
            if h > i64::MAX as u64 {
                return Err(StorageError::InvalidInput("head out of range".into()));
            }
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        Self::prepare_scope_validation(&tx, principal, stream)?;
        let gen: Option<i64> = tx
            .query_row(
                "SELECT generation FROM stream_state WHERE principal=?1 AND stream=?2",
                params![principal, stream],
                |r| r.get(0),
            )
            .optional()?;
        let generation = (gen.unwrap_or(0) as u64).max(Self::cache_incarnation(&tx, principal)?);
        tx.execute(
            "INSERT INTO checkpoints(principal, stream, cursor) VALUES(?1,?2,?3)
             ON CONFLICT(principal, stream) DO UPDATE SET cursor=excluded.cursor",
            params![principal, stream, cursor as i64],
        )?;
        tx.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor, head, generation)
             VALUES(?1,?2,1,?3,?4,?5)
             ON CONFLICT(principal, stream)
             DO UPDATE SET valid=1, replay_floor=excluded.replay_floor,
               head=excluded.head, generation=excluded.generation",
            params![
                principal,
                stream,
                floor as i64,
                head.map(|h| h as i64),
                generation as i64
            ],
        )?;
        tx.commit()?;
        Ok(generation)
    }

    pub fn set_replay_bounds(
        &self,
        principal: &str,
        stream: &str,
        floor: u64,
        head: Option<u64>,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_target_key(stream)?;
        if floor > i64::MAX as u64 {
            return Err(StorageError::InvalidInput("floor out of range".into()));
        }
        if let Some(h) = head {
            if h > i64::MAX as u64 {
                return Err(StorageError::InvalidInput("head out of range".into()));
            }
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        Self::prepare_scope_validation(&tx, principal, stream)?;
        let generation = Self::cache_incarnation(&tx, principal)?;
        tx.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor, head,generation)
             VALUES(?1,?2,1,?3,?4,?5)
             ON CONFLICT(principal, stream)
             DO UPDATE SET replay_floor=excluded.replay_floor, head=excluded.head,generation=max(COALESCE(generation,0),excluded.generation)",
            params![principal, stream, floor as i64, head.map(|h| h as i64),generation as i64],
        )?;
        tx.commit()?;
        Ok(())
    }

    // -- atomic journal application ------------------------------------------
    //
    // Crash invariant: cache mutations and the journal checkpoint advance
    // commit in one SQLite transaction. A crash before commit replays the
    // cursor; a crash after commit skips it. UI state is rebuilt from this
    // database.

    /// Fenced journal-page commit: applies `ops` and advances the
    /// conversation's journal cursor to `new_accept_seq` atomically, only if
    /// the stored `stream_state.generation` still equals
    /// `expected_generation` and the scope is valid.
    ///
    /// Fencing mirrors the old span batch: a [`StorageError::GenerationMismatch`]
    /// means a [`Self::mark_rebuild`] raced the write (drop the page and
    /// re-read via [`Self::stream_state`]); a [`StorageError::ScopeMismatch`]
    /// means `new_accept_seq` moved backward under us. Message copies compare positive
    /// body revisions when both report them, otherwise edit dates only. File refs are
    /// replaced with the winning body in the same transaction; a stale body cannot add refs.
    /// File metadata upserts remain immutable first-wins.
    pub fn apply_journal_page_fenced(
        &self,
        principal: &str,
        target_key: &str,
        ops: &CacheOps,
        new_accept_seq: i64,
        expected_generation: u64,
    ) -> Result<(), StorageError> {
        self.apply_page(
            principal,
            target_key,
            ops,
            expected_generation,
            CacheWrite::Journal(new_accept_seq),
        )
        .map(|_| ())
    }

    /// Hydrate a retained accepted send at the current checkpoint, never its message accept_seq.
    /// The same transaction checks retained outcome identity, original scope, and per-message
    /// tombstones. Message acceptance tokens are never compared with mutation checkpoints.
    pub fn hydrate_outgoing(
        &self,
        principal: &str,
        job: &OutgoingJob,
        ops: &CacheOps,
    ) -> Result<(), StorageError> {
        if job.state != OutgoingState::Accepted {
            return Err(StorageError::InvalidInput(
                "outgoing outcome is not accepted".into(),
            ));
        }
        self.apply_page(
            principal,
            &job.target_key,
            ops,
            job.cache_generation,
            CacheWrite::Outgoing(job),
        )
        .map(|_| ())
    }

    /// Hydrate fetched bodies using the scope captured before the request. The checkpoint is
    /// read and preserved inside the transaction; per-message tombstones mask delayed bodies.
    pub fn hydrate_messages_fenced(
        &self,
        principal: &str,
        target_key: &str,
        ops: &CacheOps,
        fence: &MessageCacheFence,
    ) -> Result<Vec<i64>, StorageError> {
        self.apply_page(
            principal,
            target_key,
            ops,
            fence.generation,
            CacheWrite::Snapshot(fence),
        )
    }

    /// Commit a journal response using its pre-request target/principal proof as well as the
    /// server's mutation position. A wipe/rebuild cannot be adopted after the response arrives.
    pub fn apply_journal_page_with_scope(
        &self,
        principal: &str,
        target_key: &str,
        ops: &CacheOps,
        cursor: i64,
        fence: &MessageCacheFence,
    ) -> Result<(), StorageError> {
        self.apply_page(
            principal,
            target_key,
            ops,
            fence.generation,
            CacheWrite::JournalFenced(cursor, fence),
        )
        .map(|_| ())
    }

    /// Share the existing validated atomic projection; only outgoing hydration selects same-cursor mode.
    fn apply_page(
        &self,
        principal: &str,
        target_key: &str,
        ops: &CacheOps,
        expected_generation: u64,
        write: CacheWrite<'_>,
    ) -> Result<Vec<i64>, StorageError> {
        check_principal(principal)?;
        check_target_key(target_key)?;
        if let CacheWrite::Journal(cursor) | CacheWrite::JournalFenced(cursor, _) = write {
            check_accept_seq(cursor)?;
        }
        if ops.messages.len() > 512
            || ops.message_body_revisions.len() > 512
            || ops.body_invalidations.len() > 512
            || ops.dialogs.len() > 256
            || ops.users.len() > 512
            || ops.chats.len() > 256
            || ops.read_state_scopes.len() > 64
            || ops.remove_messages.len() > 512
            || ops.files.len() > 512
            || ops.message_files.len() > 1024
            || ops.entity_files.len() > 512
            || ops.replace_entity_files.len() > 512
        {
            return Err(StorageError::InvalidInput("sync batch too large".into()));
        }
        let mut body_revisions = std::collections::HashMap::new();
        for (target, id, revision) in &ops.body_invalidations {
            if target != target_key || *id <= 0 || *revision <= 0 {
                return Err(StorageError::InvalidInput(
                    "invalid nonterminal body invalidation".into(),
                ));
            }
        }
        for &(id, revision) in &ops.message_body_revisions {
            if revision <= 0
                || !ops.messages.iter().any(|m| m.message_id == id)
                || body_revisions.insert(id, revision).is_some()
            {
                return Err(StorageError::InvalidInput(
                    "invalid message body revision".into(),
                ));
            }
        }
        for m in &ops.messages {
            check_conversation_key(&m.conversation_key)?;
            check_origin(&m.origin_stream)?;
            if m.conversation_key != target_key {
                return Err(StorageError::InvalidInput(
                    "message conversation_key must equal page target_key".into(),
                ));
            }
            if m.origin_stream != target_key {
                return Err(StorageError::InvalidInput(
                    "message origin_stream must equal page target_key".into(),
                ));
            }
            if let Some(a) = m.accept_seq {
                if a <= 0 {
                    return Err(StorageError::InvalidInput(
                        "accept_seq must be positive".into(),
                    ));
                }
            }
        }
        for d in &ops.dialogs {
            check_origin(&d.origin_stream)?;
            if d.origin_stream != target_key {
                return Err(StorageError::InvalidInput(
                    "dialog origin_stream must equal page target_key".into(),
                ));
            }
        }
        for scope in &ops.read_state_scopes {
            check_target_key(scope)?;
        }
        for (tk, mid) in &ops.remove_messages {
            check_target_key(tk)?;
            if tk != target_key {
                return Err(StorageError::InvalidInput(
                    "remove target_key must equal page target_key".into(),
                ));
            }
            if *mid <= 0 {
                return Err(StorageError::InvalidInput("bad remove message_id".into()));
            }
        }
        for f in &ops.files {
            check_file_scope(&f.scope)?;
            check_file_id(f.file_id)?;
            check_origin(&f.origin_stream)?;
            if f.origin_stream != target_key {
                return Err(StorageError::InvalidInput(
                    "file origin_stream must equal page target_key".into(),
                ));
            }
            if f.size_bytes < 0 || f.size_bytes > 1_000_000_000 {
                return Err(StorageError::InvalidInput("bad file size_bytes".into()));
            }
            if f.sha256.len() > 64 {
                return Err(StorageError::InvalidInput("bad file sha256".into()));
            }
            if f.mime_type.len() > 128 || f.file_name.len() > 256 {
                return Err(StorageError::InvalidInput("bad file mime/name".into()));
            }
            if f.created_at < 0 {
                return Err(StorageError::InvalidInput("bad file created_at".into()));
            }
        }
        for r in &ops.message_files {
            check_conversation_key(&r.conversation_key)?;
            check_file_scope(&r.file_scope)?;
            check_file_id(r.file_id)?;
            check_file_role(&r.role)?;
            if r.conversation_key != target_key {
                return Err(StorageError::InvalidInput(
                    "message_file conversation_key must equal page target_key".into(),
                ));
            }
            if r.message_id <= 0 {
                return Err(StorageError::InvalidInput(
                    "bad message_file message_id".into(),
                ));
            }
        }
        for r in &ops.entity_files {
            check_entity_file_kind(&r.kind)?;
            check_file_scope(&r.file_scope)?;
            check_file_id(r.file_id)?;
            check_file_role(&r.role)?;
            if r.entity_id <= 0 {
                return Err(StorageError::InvalidInput(
                    "bad entity_file entity_id".into(),
                ));
            }
        }
        for (kind, entity_id) in &ops.replace_entity_files {
            check_entity_file_kind(kind)?;
            if *entity_id <= 0 {
                return Err(StorageError::InvalidInput(
                    "bad replace_entity_file entity_id".into(),
                ));
            }
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        match write {
            CacheWrite::Snapshot(fence) | CacheWrite::JournalFenced(_, fence) => {
                let database: String = tx.query_row(
                    "SELECT value FROM meta WHERE key='search_database_id'",
                    [],
                    |r| r.get(0),
                )?;
                if !fence.valid
                    || fence.principal != principal
                    || fence.target_key != target_key
                    || fence.database_id != database
                    || fence.incarnation != Self::cache_incarnation(&tx, principal)?
                {
                    return Err(StorageError::CacheScopeChanged);
                }
            }
            CacheWrite::Outgoing(job) => {
                if job.cache_incarnation != Self::cache_incarnation(&tx, principal)? {
                    return Err(StorageError::CacheScopeChanged);
                }
            }
            CacheWrite::Journal(_) => {}
        }
        let row: Option<(i64, Option<i64>)> = tx
            .query_row(
                "SELECT valid, generation FROM stream_state WHERE principal=?1 AND stream=?2",
                params![principal, target_key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let found_gen = (row.and_then(|(_, g)| g).unwrap_or(0) as u64)
            .max(Self::cache_incarnation(&tx, principal)?);
        if found_gen != expected_generation {
            return Err(StorageError::GenerationMismatch {
                stream: target_key.to_string(),
                expected: expected_generation,
                found: found_gen,
            });
        }
        if let Some((valid, _)) = row {
            if valid == 0 {
                return Err(StorageError::Corrupt(format!(
                    "stream {target_key} requires rebuild; refusing to commit"
                )));
            }
        } else {
            Self::prepare_scope_validation(&tx, principal, target_key)?;
        }
        let current: Option<i64> = tx
            .query_row(
                "SELECT cursor FROM checkpoints WHERE principal=?1 AND stream=?2",
                params![principal, target_key],
                |r| r.get(0),
            )
            .optional()?;
        let current = current.unwrap_or(0);
        let new_accept_seq = if let CacheWrite::Outgoing(job) = write {
            let retained:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM outgoing_jobs WHERE principal=?1 AND source=?2 AND job_id=?3 AND state=3 AND revision=?4 AND payload=?5 AND response=?6 AND cache_generation=?7 AND target_key=?8 AND method=?9 AND cache_incarnation=?10)",params![principal,job.source,job.job_id,job.revision,job.payload,job.response,job.cache_generation as i64,target_key,job.method,job.cache_incarnation as i64],|r|r.get(0))?;
            if !retained {
                return Err(StorageError::InvalidInput(
                    "accepted outgoing outcome changed".into(),
                ));
            }
            current
        } else {
            match write {
                CacheWrite::Journal(cursor) | CacheWrite::JournalFenced(cursor, _) => cursor,
                _ => current,
            }
        };
        if new_accept_seq < current {
            return Err(StorageError::ScopeMismatch {
                stream: target_key.to_string(),
                expected: new_accept_seq,
                found: current,
            });
        }
        // Removal is an identity-scoped terminal visibility fact. Never compare message
        // acceptance tokens with mutation positions to infer whether a removal occurred.
        for (target, id) in &ops.remove_messages {
            tx.execute("INSERT INTO message_tombstones(principal,conversation_key,message_id,last_body_revision)
                VALUES(?1,?2,?3,(SELECT body_revision FROM messages WHERE principal=?1 AND conversation_key=?2 AND message_id=?3))
                ON CONFLICT(principal,conversation_key,message_id) DO NOTHING",params![principal,target,id])?;
        }
        let mut visible_ids = Vec::with_capacity(ops.messages.len());
        // Floors survive body eviction/rebuild. They are not terminal visibility
        // facts: a later authorized response at the same or newer revision may win.
        for (target, id, revision) in &ops.body_invalidations {
            tx.execute("INSERT INTO message_body_floors(principal,conversation_key,message_id,minimum_revision)
                VALUES(?1,?2,?3,?4) ON CONFLICT(principal,conversation_key,message_id)
                DO UPDATE SET minimum_revision=max(minimum_revision,excluded.minimum_revision)",params![principal,target,id,revision])?;
            let newer:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM messages WHERE principal=?1 AND conversation_key=?2 AND message_id=?3 AND body_revision>?4)",params![principal,target,id,revision],|r|r.get(0))?;
            if !newer {
                tx.execute("DELETE FROM message_files WHERE principal=?1 AND conversation_key=?2 AND message_id=?3",params![principal,target,id])?;
                tx.execute("DELETE FROM messages WHERE principal=?1 AND conversation_key=?2 AND message_id=?3",params![principal,target,id])?;
                tx.execute("UPDATE dialogs SET read_state_pending=1 WHERE principal=?1 AND top_message=?3 AND 'target:'||CASE peer_kind WHEN 'chat' THEN 'room' ELSE peer_kind END||':'||peer_id=?2",params![principal,target,id])?;
            }
        }
        let mut winning_ids = std::collections::HashSet::new();
        for m in &ops.messages {
            let removed:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM message_tombstones WHERE principal=?1 AND conversation_key=?2 AND message_id=?3)",params![principal,target_key,m.message_id],|r|r.get(0))?;
            if removed {
                continue;
            }
            let floor:Option<i64>=tx.query_row("SELECT minimum_revision FROM message_body_floors WHERE principal=?1 AND conversation_key=?2 AND message_id=?3",params![principal,target_key,m.message_id],|r|r.get(0)).optional()?;
            if floor.is_some_and(|floor| {
                body_revisions
                    .get(&m.message_id)
                    .is_none_or(|revision| *revision < floor)
            }) {
                continue;
            }
            visible_ids.push(m.message_id);
            let peer_ok = m.peer_id != 0 || m.peer_kind == "unknown";
            if m.message_id <= 0 || !peer_ok || m.proto.len() > 2_000_000 {
                return Err(StorageError::InvalidInput("bad message row".into()));
            }
            let existing: Option<BodyOrdering> = tx
                .query_row(
                    "SELECT accept_seq, edit_date_secs, edit_date_nanos, body_revision FROM messages
                     WHERE principal=?1 AND conversation_key=?2 AND message_id=?3",
                    params![principal, m.conversation_key, m.message_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .optional()?;
            let should_write = match existing {
                None => true,
                Some((_old_accept, old_secs, old_nanos, old_revision)) => {
                    // Acceptance order ranks different messages, never copies of one body.
                    let fresher_by_date = {
                        let old_has = old_secs.is_some();
                        let new_has = m.edit_date_secs.is_some();
                        match (old_has, new_has) {
                            (false, true) => true,
                            (true, true) => {
                                let old_pair = (old_secs.unwrap_or(0), old_nanos.unwrap_or(0));
                                let new_pair = (
                                    m.edit_date_secs.unwrap_or(0),
                                    m.edit_date_nanos.unwrap_or(0),
                                );
                                new_pair > old_pair
                            }
                            _ => false,
                        }
                    };
                    match (old_revision, body_revisions.get(&m.message_id)) {
                        (Some(old), Some(new)) => *new > old,
                        _ => fresher_by_date,
                    }
                }
            };
            if !should_write {
                continue;
            }
            winning_ids.insert(m.message_id);
            tx.execute("DELETE FROM message_files WHERE principal=?1 AND conversation_key=?2 AND message_id=?3",
                params![principal, m.conversation_key, m.message_id])?;
            tx.execute(
                "INSERT INTO messages(principal, conversation_key, message_id, peer_kind, peer_id, from_id, body, proto, origin_stream, accept_seq, edit_date_secs, edit_date_nanos,body_revision)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)
                 ON CONFLICT(principal, conversation_key, message_id)
                 DO UPDATE SET peer_kind=excluded.peer_kind, peer_id=excluded.peer_id,
                   from_id=excluded.from_id, body=excluded.body, proto=excluded.proto,
                   origin_stream=excluded.origin_stream, accept_seq=excluded.accept_seq,
                   edit_date_secs=excluded.edit_date_secs, edit_date_nanos=excluded.edit_date_nanos,body_revision=excluded.body_revision",
                params![
                    principal,
                    m.conversation_key,
                    m.message_id,
                    m.peer_kind,
                    m.peer_id,
                    m.from_id,
                    m.body,
                    m.proto,
                    m.origin_stream,
                    m.accept_seq,
                    m.edit_date_secs,
                    m.edit_date_nanos,
                    body_revisions.get(&m.message_id)
                ],
            )?;
        }
        for (tk, mid) in &ops.remove_messages {
            // Invalidate the visible head while retaining counters as explicitly pending. The
            // tombstone hides its proto until a valid accepted successor or snapshot replaces it.
            tx.execute("UPDATE dialogs SET read_state_pending=1 WHERE principal=?1 AND top_message=?3 AND 'target:'||CASE peer_kind WHEN 'chat' THEN 'room' ELSE peer_kind END||':'||peer_id=?2",params![principal,tk,mid])?;
            tx.execute(
                "DELETE FROM messages WHERE principal=?1 AND conversation_key=?2 AND message_id=?3",
                params![principal, tk, mid],
            )?;
            // Removing the message drops its file refs so refcounts stay
            // exact; `files` rows themselves are collected by rebuild GC.
            tx.execute(
                "DELETE FROM message_files WHERE principal=?1 AND conversation_key=?2 AND message_id=?3",
                params![principal, tk, mid],
            )?;
        }
        for d in &ops.dialogs {
            let removed:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM message_tombstones WHERE principal=?1 AND conversation_key=?2 AND message_id=?3)",params![principal,target_key,d.top_message],|r|r.get(0))?;
            if removed {
                continue;
            }
            if d.peer_id == 0 || d.proto.len() > 1_000_000 {
                return Err(StorageError::InvalidInput("bad dialog row".into()));
            }
            let existing: Option<i64> = tx.query_row(
                "SELECT top_message FROM dialogs WHERE principal=?1 AND peer_kind=?2 AND peer_id=?3",
                params![principal,d.peer_kind,d.peer_id], |r|r.get(0)).optional()?;
            if let Some(old) = existing {
                let position = |id| -> Result<Option<i64>, StorageError> {
                    Ok(tx.query_row("SELECT accept_seq FROM messages WHERE principal=?1 AND conversation_key=?2 AND message_id=?3",
                        params![principal,target_key,id], |r|r.get(0)).optional()?.flatten())
                };
                let removed_old:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM message_tombstones WHERE principal=?1 AND conversation_key=?2 AND message_id=?3)",params![principal,target_key,old],|r|r.get(0))?;
                let candidate = position(d.top_message)?;
                // Keep the removed-head fast path first; it must not query the old position.
                if !(removed_old && candidate.is_some_and(|p| p > 0)
                    || matches!((position(old)?,candidate),(Some(a),Some(b)) if b>a))
                {
                    continue;
                }
            }
            tx.execute(
                "INSERT INTO dialogs(principal, peer_kind, peer_id, top_message, unread_count,
                   read_inbox_max_id, read_outbox_max_id, pinned, proto, origin_stream)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
                 ON CONFLICT(principal, peer_kind, peer_id)
                  DO UPDATE SET top_message=excluded.top_message,
                    proto=excluded.proto,
                   origin_stream=excluded.origin_stream",
                params![
                    principal,
                    d.peer_kind,
                    d.peer_id,
                    d.top_message,
                    d.unread_count,
                    d.read_inbox_max_id,
                    d.read_outbox_max_id,
                    d.pinned as i32,
                    d.proto,
                    d.origin_stream
                ],
            )?;
        }
        let written_entities = Self::insert_hydrated_entities(&tx, principal, ops)?;
        for u in &ops.users {
            tx.execute(
                "INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
                 VALUES(?1,'user',?2,?3)",
                params![principal, u.user_id, target_key],
            )?;
        }
        for c in &ops.chats {
            tx.execute(
                "INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
                 VALUES(?1,'chat',?2,?3)",
                params![principal, c.chat_id, target_key],
            )?;
        }
        // File-cache subsystem: immutable metadata first, then idempotent
        // refs, all in the same fenced transaction.
        for f in &ops.files {
            if !ops.message_files.iter().any(|r| {
                winning_ids.contains(&r.message_id)
                    && r.file_scope == f.scope
                    && r.file_id == f.file_id
            }) && !ops.entity_files.iter().any(|r| {
                written_entities.contains(&(r.kind.as_str(), r.entity_id))
                    && r.file_scope == f.scope
                    && r.file_id == f.file_id
            }) && (!ops.messages.is_empty() || !ops.users.is_empty() || !ops.chats.is_empty())
            {
                continue;
            }
            tx.execute(
                "INSERT OR IGNORE INTO files(principal, scope, file_id, size_bytes, sha256,
                   mime_type, file_name, encrypted, created_at, origin_stream)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    principal,
                    f.scope,
                    f.file_id,
                    f.size_bytes,
                    f.sha256,
                    f.mime_type,
                    f.file_name,
                    f.encrypted as i32,
                    f.created_at,
                    f.origin_stream
                ],
            )?;
        }
        for r in &ops.message_files {
            if !winning_ids.contains(&r.message_id) {
                continue;
            }
            let removed:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM message_tombstones WHERE principal=?1 AND conversation_key=?2 AND message_id=?3)",params![principal,r.conversation_key,r.message_id],|row|row.get(0))?;
            if removed {
                continue;
            }
            tx.execute(
                "INSERT OR IGNORE INTO message_files(principal, conversation_key, message_id,
                   file_scope, file_id, role)
                 VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    principal,
                    r.conversation_key,
                    r.message_id,
                    r.file_scope,
                    r.file_id,
                    r.role
                ],
            )?;
        }
        // Replace semantics: clear each hydrated entity's current ref set
        // first so stale avatar refs are collected; the fresh set below is
        // inserted in the same transaction.
        {
            use std::collections::HashSet;
            let mut seen: HashSet<(&str, i64)> = HashSet::new();
            for (kind, entity_id) in &ops.replace_entity_files {
                if !written_entities.contains(&(kind.as_str(), *entity_id)) {
                    continue;
                }
                if seen.insert((kind.as_str(), *entity_id)) {
                    tx.execute(
                        "DELETE FROM entity_files
                          WHERE principal=?1 AND kind=?2 AND entity_id=?3",
                        params![principal, kind, entity_id],
                    )?;
                }
            }
        }
        for r in &ops.entity_files {
            let hydrated = match r.kind.as_str() {
                "user" => ops.users.iter().any(|u| u.user_id == r.entity_id),
                "chat" => ops.chats.iter().any(|c| c.chat_id == r.entity_id),
                _ => false,
            };
            if (hydrated || matches!(write, CacheWrite::Outgoing(_)))
                && !written_entities.contains(&(r.kind.as_str(), r.entity_id))
            {
                continue;
            }
            tx.execute(
                "INSERT OR IGNORE INTO entity_files(principal, kind, entity_id,
                   file_scope, file_id, role)
                  VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    principal,
                    r.kind,
                    r.entity_id,
                    r.file_scope,
                    r.file_id,
                    r.role
                ],
            )?;
        }
        for scope in &ops.read_state_scopes {
            tx.execute(
                "UPDATE dialogs SET read_state_pending=1
                 WHERE principal=?1 AND (origin_stream=?2 OR origin_stream='')",
                params![principal, scope],
            )?;
        }
        tx.execute(
            "INSERT INTO checkpoints(principal, stream, cursor) VALUES(?1,?2,?3)
             ON CONFLICT(principal, stream) DO UPDATE SET cursor=excluded.cursor",
            params![principal, target_key, new_accept_seq],
        )?;
        tx.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor,generation)
             VALUES(?1,?2,1,0,?3)
             ON CONFLICT(principal, stream) DO UPDATE SET valid=1, replay_floor=0,generation=max(COALESCE(generation,0),excluded.generation)",
            params![principal, target_key,found_gen as i64],
        )?;
        tx.commit()?;
        Ok(visible_ids)
    }

    // -- offline queries ----------------------------------------------------

    /// Fence even empty clears/invalidations so stale empty results cannot survive replacement.
    fn bump_search_revision(conn: &Connection, principal: &str) -> Result<(), StorageError> {
        conn.execute(
            "INSERT INTO message_search_revisions VALUES(?1,1)
            ON CONFLICT(principal) DO UPDATE SET revision=revision+1",
            [principal],
        )?;
        Ok(())
    }

    /// Read a cache-local revision. Never used as a journal cursor or server acceptance token.
    fn search_snapshot(
        conn: &Connection,
        principal: &str,
    ) -> Result<MessageSearchSnapshot, StorageError> {
        let (database_id, revision) = conn.query_row(
            "SELECT value, COALESCE((SELECT revision FROM message_search_revisions WHERE principal=?1),0)
             FROM meta WHERE key='search_database_id'", [principal], |r| Ok((r.get(0)?,r.get(1)?)))?;
        Ok(MessageSearchSnapshot {
            database_id,
            principal: principal.into(),
            revision,
        })
    }

    /// Reject a result computed before a clear, invalidation or cached-message mutation.
    pub fn check_search_snapshot(
        &self,
        snapshot: &MessageSearchSnapshot,
    ) -> Result<(), StorageError> {
        let conn = self.lock()?;
        if Self::search_snapshot(&conn, &snapshot.principal)? != *snapshot {
            return Err(StorageError::SearchChanged);
        }
        Ok(())
    }

    /// Search cached bodies only; never performs server I/O or advances journal progress.
    /// One read transaction covers candidates and the revision. Next pages use
    /// a strict document-ID keyset and reject any intervening message-cache mutation.
    pub fn search_messages(
        &self,
        principal: &str,
        request: &MessageSearchRequest,
    ) -> Result<MessageSearchPage, StorageError> {
        check_principal(principal)?;
        let query = literal_search_query(request)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let snapshot = Self::search_snapshot(&tx, principal)?;
        if let Some(cursor) = &request.cursor {
            if cursor.snapshot != snapshot {
                return Err(StorageError::SearchChanged);
            }
            if cursor.query != request.query
                || cursor.mode != request.mode
                || cursor.target_key != request.target_key
                || cursor.sender_id != request.sender_id
            {
                return Err(StorageError::InvalidInput(
                    "search cursor query mismatch".into(),
                ));
            }
        }
        let mut stmt = tx.prepare(
            "SELECT d.document_id,m.conversation_key,m.message_id,m.from_id,m.proto,m.body,
                length(m.proto),length(CAST(m.body AS BLOB))
            FROM message_search JOIN message_documents d ON d.document_id=message_search.rowid
            JOIN messages m USING(principal,conversation_key,message_id)
            JOIN stream_state s ON s.principal=m.principal AND s.stream=m.conversation_key AND s.valid=1
            WHERE message_search MATCH ?1 AND d.principal=?2
              AND NOT EXISTS(SELECT 1 FROM message_tombstones t WHERE t.principal=m.principal AND t.conversation_key=m.conversation_key AND t.message_id=m.message_id)
              AND (?3 IS NULL OR d.conversation_key=?3) AND (?4 IS NULL OR m.from_id=?4)
              AND (?5 IS NULL OR message_search.rowid<?5)
            ORDER BY message_search.rowid DESC LIMIT ?6",
        )?;
        let mut rows = stmt.query(params![
            query,
            principal,
            request.target_key,
            request.sender_id,
            request.cursor.as_ref().map(|c| c.before_document_id),
            i64::from(request.limit) + 1
        ])?;
        let mut hits = Vec::new();
        let mut last_id = 0;
        let mut more = false;
        while let Some(row) = rows.next()? {
            if hits.len() == request.limit as usize {
                more = true;
                break;
            }
            // Bound Rust copies even for caches populated by other native consumers.
            let proto_bytes: i64 = row.get(6)?;
            let body_bytes: i64 = row.get(7)?;
            if proto_bytes > 2_000_000 || body_bytes > 2_000_000 {
                return Err(StorageError::Corrupt(
                    "search message exceeds cache budget".into(),
                ));
            }
            let id: i64 = row.get(0)?;
            hits.push(MessageSearchHit {
                target_key: row.get(1)?,
                message_id: row.get(2)?,
                sender_id: row.get(3)?,
                proto: row.get(4)?,
            });
            last_id = id;
        }
        Ok(MessageSearchPage {
            hits,
            next: more.then(|| MessageSearchCursor {
                snapshot: snapshot.clone(),
                query: request.query.clone(),
                mode: request.mode,
                target_key: request.target_key.clone(),
                sender_id: request.sender_id,
                before_document_id: last_id,
            }),
            snapshot,
        })
    }

    /// Read only messages in a valid canonical scope, excluding durable removals even if an
    /// older external cache writer inserted a stale physical row. No journal progress is changed.
    pub fn get_messages(
        &self,
        principal: &str,
        conversation_key: &str,
        limit: u32,
        before_id: Option<i64>,
    ) -> Result<Vec<MessageRow>, StorageError> {
        check_principal(principal)?;
        check_conversation_key(conversation_key)?;
        let limit = (limit.clamp(1, 500)) as i64;
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT conversation_key, message_id, peer_kind, peer_id, from_id, body, proto,
                    origin_stream, accept_seq, edit_date_secs, edit_date_nanos
             FROM messages WHERE principal=?1 AND conversation_key=?2
              AND EXISTS(SELECT 1 FROM stream_state s WHERE s.principal=messages.principal AND s.stream=messages.conversation_key AND s.valid=1)
              AND NOT EXISTS(SELECT 1 FROM message_tombstones t WHERE t.principal=messages.principal AND t.conversation_key=messages.conversation_key AND t.message_id=messages.message_id)
             AND (?3 IS NULL OR message_id < ?3)
             ORDER BY message_id DESC LIMIT ?4",
        )?;
        let rows = stmt.query_map(
            params![principal, conversation_key, before_id, limit],
            |r| {
                Ok(MessageRow {
                    conversation_key: r.get(0)?,
                    message_id: r.get(1)?,
                    peer_kind: r.get(2)?,
                    peer_id: r.get(3)?,
                    from_id: r.get(4)?,
                    body: r.get(5)?,
                    proto: r.get(6)?,
                    origin_stream: r.get(7)?,
                    accept_seq: r.get(8)?,
                    edit_date_secs: r.get(9)?,
                    edit_date_nanos: r.get(10)?,
                })
            },
        )?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        out.reverse();
        Ok(out)
    }

    /// Cached message ids for one conversation (ascending). Used by the
    /// new-message catch-up to diff a history window against the offline
    /// cache without pulling full bodies.
    pub fn cached_message_ids(
        &self,
        principal: &str,
        conversation_key: &str,
    ) -> Result<Vec<i64>, StorageError> {
        check_principal(principal)?;
        check_conversation_key(conversation_key)?;
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT message_id FROM messages WHERE principal=?1 AND conversation_key=?2
             AND EXISTS(SELECT 1 FROM stream_state s WHERE s.principal=messages.principal AND s.stream=messages.conversation_key AND s.valid=1)
             AND NOT EXISTS(SELECT 1 FROM message_tombstones t WHERE t.principal=messages.principal AND t.conversation_key=messages.conversation_key AND t.message_id=messages.message_id)
             ORDER BY message_id ASC",
        )?;
        let rows = stmt.query_map(params![principal, conversation_key], |r| r.get(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Whether a table exists in this database. Callers use this to
    /// feature-detect optional tables (e.g. `background_sync_jobs` on
    /// pre-v4 databases): when the table is missing they must fall back
    /// to in-memory retry rather than failing.
    pub fn has_table(&self, table: &str) -> Result<bool, StorageError> {
        if table.is_empty()
            || table.len() > 64
            || !table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(StorageError::InvalidInput("bad table name".into()));
        }
        let conn = self.lock()?;
        let sql = format!("SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'");
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        Ok(rows.next()?.is_some())
    }

    /// Enqueue (or refresh) a background catch-up job. Upserts by
    /// `(principal, job_id)` and resets the claim. Returns `Ok(false)`
    /// when the `background_sync_jobs` table is missing (caller falls back
    /// to in-memory demand retry); any other error propagates.
    pub fn enqueue_background_sync_job(
        &self,
        principal: &str,
        job: &BackgroundSyncJob,
    ) -> Result<bool, StorageError> {
        check_principal(principal)?;
        check_job_id(&job.job_id)?;
        let conn = self.lock()?;
        let outcome: Result<usize, rusqlite::Error> = conn.execute(
            "INSERT INTO background_sync_jobs(principal, job_id, kind, target_key, state, payload, attempts, next_due, claimed_by, updated_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
             ON CONFLICT(principal, job_id) DO UPDATE SET kind=excluded.kind, target_key=excluded.target_key,
               state=excluded.state, payload=excluded.payload, attempts=excluded.attempts,
               next_due=excluded.next_due, claimed_by=excluded.claimed_by, updated_at=excluded.updated_at",
            params![
                principal,
                job.job_id,
                job.kind,
                job.target_key,
                job.state,
                job.payload,
                job.attempts,
                job.next_due,
                job.claimed_by,
                job.updated_at
            ],
        );
        match outcome {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.to_string().contains("no such table") {
                    return Ok(false);
                }
                Err(StorageError::from(e))
            }
        }
    }

    /// List background jobs for one principal, optionally filtered by state.
    /// Returns an empty vec (not an error) when the table is missing so
    /// callers transparently fall back to in-memory retry.
    pub fn list_background_sync_jobs(
        &self,
        principal: &str,
        state: Option<&str>,
    ) -> Result<Vec<BackgroundSyncJob>, StorageError> {
        check_principal(principal)?;
        let conn = self.lock()?;
        let query = match state {
            Some(_) => "SELECT job_id, kind, target_key, state, payload, attempts, next_due, claimed_by, updated_at FROM background_sync_jobs WHERE principal=?1 AND state=?2 ORDER BY next_due ASC",
            None => "SELECT job_id, kind, target_key, state, payload, attempts, next_due, claimed_by, updated_at FROM background_sync_jobs WHERE principal=?1 ORDER BY next_due ASC",
        };
        let mut stmt = match conn.prepare(query) {
            Ok(s) => s,
            Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
            Err(e) => return Err(StorageError::from(e)),
        };
        let rows: Result<Vec<BackgroundSyncJob>, rusqlite::Error> = match state {
            Some(s) => stmt
                .query_map(params![principal, s], |r| {
                    Ok(BackgroundSyncJob {
                        job_id: r.get(0)?,
                        kind: r.get(1)?,
                        target_key: r.get(2)?,
                        state: r.get(3)?,
                        payload: r.get(4)?,
                        attempts: r.get(5)?,
                        next_due: r.get(6)?,
                        claimed_by: r.get(7)?,
                        updated_at: r.get(8)?,
                    })
                })?
                .collect(),
            None => stmt
                .query_map(params![principal], |r| {
                    Ok(BackgroundSyncJob {
                        job_id: r.get(0)?,
                        kind: r.get(1)?,
                        target_key: r.get(2)?,
                        state: r.get(3)?,
                        payload: r.get(4)?,
                        attempts: r.get(5)?,
                        next_due: r.get(6)?,
                        claimed_by: r.get(7)?,
                        updated_at: r.get(8)?,
                    })
                })?
                .collect(),
        };
        match rows {
            Ok(v) => Ok(v),
            Err(e) if e.to_string().contains("no such table") => Ok(Vec::new()),
            Err(e) => Err(StorageError::from(e)),
        }
    }

    /// Delete one background job. Returns `Ok(false)` when the table is
    /// missing (nothing durable to clear); otherwise `Ok(true)`.
    pub fn delete_background_sync_job(
        &self,
        principal: &str,
        job_id: &str,
    ) -> Result<bool, StorageError> {
        check_principal(principal)?;
        check_job_id(job_id)?;
        let conn = self.lock()?;
        match conn.execute(
            "DELETE FROM background_sync_jobs WHERE principal=?1 AND job_id=?2",
            params![principal, job_id],
        ) {
            Ok(_) => Ok(true),
            Err(e) if e.to_string().contains("no such table") => Ok(false),
            Err(e) => Err(StorageError::from(e)),
        }
    }

    pub fn list_dialogs(
        &self,
        principal: &str,
        limit: u32,
    ) -> Result<Vec<DialogRow>, StorageError> {
        check_principal(principal)?;
        let limit = (limit.clamp(1, 500)) as i64;
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT peer_kind, peer_id, top_message, unread_count, read_inbox_max_id,
                    read_outbox_max_id, pinned, proto, origin_stream, read_state_pending
             FROM dialogs WHERE principal=?1
               AND NOT EXISTS(SELECT 1 FROM message_tombstones t WHERE t.principal=dialogs.principal AND t.conversation_key='target:'||CASE dialogs.peer_kind WHEN 'chat' THEN 'room' ELSE dialogs.peer_kind END||':'||dialogs.peer_id AND t.message_id=dialogs.top_message)
             ORDER BY pinned DESC, top_message DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![principal, limit], |r| {
            Ok(DialogRow {
                peer_kind: r.get(0)?,
                peer_id: r.get(1)?,
                top_message: r.get(2)?,
                unread_count: r.get(3)?,
                read_inbox_max_id: r.get(4)?,
                read_outbox_max_id: r.get(5)?,
                pinned: r.get::<_, i32>(6)? != 0,
                proto: r.get(7)?,
                origin_stream: r.get(8)?,
                read_state_pending: r.get::<_, i32>(9)? != 0,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn get_user(&self, principal: &str, user_id: i64) -> Result<Option<UserRow>, StorageError> {
        check_principal(principal)?;
        if user_id <= 0 {
            return Err(StorageError::InvalidInput(
                "user_id must be positive".into(),
            ));
        }
        let conn = self.lock()?;
        let v: Option<Vec<u8>> = conn
            .query_row(
                "SELECT proto FROM users WHERE principal=?1 AND user_id=?2",
                params![principal, user_id],
                |r| r.get(0),
            )
            .optional()?;
        Ok(v.map(|proto| UserRow { user_id, proto }))
    }

    pub fn get_chat(&self, principal: &str, chat_id: i64) -> Result<Option<ChatRow>, StorageError> {
        check_principal(principal)?;
        if chat_id <= 0 {
            return Err(StorageError::InvalidInput(
                "chat_id must be positive".into(),
            ));
        }
        let conn = self.lock()?;
        let v: Option<Vec<u8>> = conn
            .query_row(
                "SELECT proto FROM chats WHERE principal=?1 AND chat_id=?2",
                params![principal, chat_id],
                |r| r.get(0),
            )
            .optional()?;
        Ok(v.map(|proto| ChatRow { chat_id, proto }))
    }

    /// Insert unversioned entities without treating arrival order or message journal positions
    /// as entity revisions. The caller commits file refs only for the returned inserted set.
    fn insert_hydrated_entities(
        tx: &rusqlite::Transaction<'_>,
        principal: &str,
        ops: &CacheOps,
    ) -> Result<std::collections::HashSet<(&'static str, i64)>, StorageError> {
        let mut inserted = std::collections::HashSet::new();
        for u in &ops.users {
            if u.user_id <= 0 || u.proto.is_empty() || u.proto.len() > 1_000_000 {
                return Err(StorageError::InvalidInput("bad user row".into()));
            }
            if tx.execute(
                "INSERT OR IGNORE INTO users(principal,user_id,proto) VALUES(?1,?2,?3)",
                params![principal, u.user_id, u.proto],
            )? == 1
            {
                inserted.insert(("user", u.user_id));
            }
        }
        for c in &ops.chats {
            if c.chat_id <= 0 || c.proto.is_empty() || c.proto.len() > 1_000_000 {
                return Err(StorageError::InvalidInput("bad chat row".into()));
            }
            if tx.execute(
                "INSERT OR IGNORE INTO chats(principal,chat_id,proto) VALUES(?1,?2,?3)",
                params![principal, c.chat_id, c.proto],
            )? == 1
            {
                inserted.insert(("chat", c.chat_id));
            }
        }
        Ok(inserted)
    }

    /// Hydrate global dialog-list entities and their file refs atomically, insert-only.
    /// Unversioned snapshots cannot replace existing entities or alter their reference sets.
    /// Only users, chats, files and entity-file fields of `ops` are consumed.
    pub fn hydrate_entity_snapshot(
        &self,
        principal: &str,
        ops: &CacheOps,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        Self::hydrate_entity_snapshot_tx(&tx, principal, ops)?;
        tx.commit()?;
        Ok(())
    }

    /// Shared insert-only entity/ref projection inside an existing snapshot transaction.
    fn hydrate_entity_snapshot_tx(
        tx: &rusqlite::Transaction<'_>,
        principal: &str,
        ops: &CacheOps,
    ) -> Result<(), StorageError> {
        if ops.users.len() > 512
            || ops.chats.len() > 256
            || ops.files.len() > 512
            || ops.entity_files.len() > 512
        {
            return Err(StorageError::InvalidInput(
                "entity snapshot too large".into(),
            ));
        }
        let inserted = Self::insert_hydrated_entities(tx, principal, ops)?;
        for u in &ops.users {
            tx.execute("INSERT OR IGNORE INTO entity_provenance(principal,kind,entity_id,stream) VALUES(?1,'user',?2,'')", params![principal,u.user_id])?;
        }
        for c in &ops.chats {
            tx.execute("INSERT OR IGNORE INTO entity_provenance(principal,kind,entity_id,stream) VALUES(?1,'chat',?2,'')", params![principal,c.chat_id])?;
        }
        for f in &ops.files {
            check_file_scope(&f.scope)?;
            check_file_id(f.file_id)?;
            if !f.origin_stream.is_empty()
                || f.size_bytes < 0
                || f.size_bytes > 1_000_000_000
                || f.sha256.len() > 64
                || f.mime_type.len() > 128
                || f.file_name.len() > 256
                || f.created_at < 0
            {
                return Err(StorageError::InvalidInput("bad snapshot file".into()));
            }
            if !ops.entity_files.iter().any(|r| {
                inserted.contains(&(r.kind.as_str(), r.entity_id))
                    && r.file_scope == f.scope
                    && r.file_id == f.file_id
            }) {
                continue;
            }
            tx.execute("INSERT OR IGNORE INTO files(principal,scope,file_id,size_bytes,sha256,mime_type,file_name,encrypted,created_at,origin_stream) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'')",
                params![principal,f.scope,f.file_id,f.size_bytes,f.sha256,f.mime_type,f.file_name,f.encrypted as i32,f.created_at])?;
        }
        for r in &ops.entity_files {
            check_entity_file_kind(&r.kind)?;
            check_file_scope(&r.file_scope)?;
            check_file_id(r.file_id)?;
            check_file_role(&r.role)?;
            if r.entity_id <= 0 {
                return Err(StorageError::InvalidInput("bad entity reference".into()));
            }
            if inserted.contains(&(r.kind.as_str(), r.entity_id)) {
                tx.execute("INSERT OR IGNORE INTO entity_files(principal,kind,entity_id,file_scope,file_id,role) VALUES(?1,?2,?3,?4,?5,?6)",
                    params![principal,r.kind,r.entity_id,r.file_scope,r.file_id,r.role])?;
            }
        }
        Ok(())
    }

    /// Explicit authoritative user replacement. Automatic unversioned hydration is insert-only.
    pub fn upsert_user(&self, principal: &str, row: &UserRow) -> Result<(), StorageError> {
        check_principal(principal)?;
        if row.user_id <= 0 || row.proto.is_empty() || row.proto.len() > 1_000_000 {
            return Err(StorageError::InvalidInput("bad user row".into()));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO users(principal, user_id, proto) VALUES(?1,?2,?3)
             ON CONFLICT(principal, user_id) DO UPDATE SET proto=excluded.proto",
            params![principal, row.user_id, row.proto],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
             VALUES(?1,'user',?2,'')",
            params![principal, row.user_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Authoritative chat write (e.g. GetDialogs refresh). Pins globally.
    pub fn upsert_chat(&self, principal: &str, row: &ChatRow) -> Result<(), StorageError> {
        check_principal(principal)?;
        if row.chat_id <= 0 || row.proto.is_empty() || row.proto.len() > 1_000_000 {
            return Err(StorageError::InvalidInput("bad chat row".into()));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO chats(principal, chat_id, proto) VALUES(?1,?2,?3)
             ON CONFLICT(principal, chat_id) DO UPDATE SET proto=excluded.proto",
            params![principal, row.chat_id, row.proto],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
             VALUES(?1,'chat',?2,'')",
            params![principal, row.chat_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Authoritative dialog write (e.g. GetDialogs refresh).
    pub fn upsert_dialog(&self, principal: &str, row: &DialogRow) -> Result<(), StorageError> {
        self.upsert_dialog_authoritative(principal, row, true)
    }

    /// Authoritative dialog write with explicit pending handling.
    pub fn upsert_dialog_authoritative(
        &self,
        principal: &str,
        row: &DialogRow,
        clear_pending: bool,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        Self::write_dialog_authoritative(&tx, principal, row, clear_pending)?;
        tx.commit()?;
        Ok(())
    }

    /// Authoritative row write only after its identity is checked, including retained removals.
    fn write_dialog_authoritative(
        tx: &rusqlite::Transaction<'_>,
        principal: &str,
        row: &DialogRow,
        clear_pending: bool,
    ) -> Result<(), StorageError> {
        if row.peer_id == 0 || row.proto.len() > 1_000_000 {
            return Err(StorageError::InvalidInput("bad dialog row".into()));
        }
        check_origin(&row.origin_stream)?;
        let target = Self::dialog_target(row)?;
        let removed:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM message_tombstones WHERE principal=?1 AND conversation_key=?2 AND message_id=?3)",params![principal,target,row.top_message],|r|r.get(0))?;
        if removed {
            return Err(StorageError::CacheScopeChanged);
        }
        let pending_new: i64 = if row.read_state_pending {
            1
        } else if clear_pending {
            0
        } else {
            let existing: Option<i64> = tx
                .query_row(
                    "SELECT read_state_pending FROM dialogs WHERE principal=?1 AND peer_kind=?2 AND peer_id=?3",
                    params![principal, row.peer_kind, row.peer_id],
                    |r| r.get(0),
                )
                .optional()?;
            existing.unwrap_or(0)
        };
        tx.execute(
            "INSERT INTO dialogs(principal, peer_kind, peer_id, top_message, unread_count,
               read_inbox_max_id, read_outbox_max_id, pinned, proto, origin_stream,
               read_state_pending)
              VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
              ON CONFLICT(principal, peer_kind, peer_id)
              DO UPDATE SET top_message=excluded.top_message, unread_count=excluded.unread_count,
                read_inbox_max_id=excluded.read_inbox_max_id,
                read_outbox_max_id=excluded.read_outbox_max_id,
                pinned=excluded.pinned, proto=excluded.proto,
                origin_stream=excluded.origin_stream, read_state_pending=excluded.read_state_pending",
            params![
                principal,
                row.peer_kind,
                row.peer_id,
                row.top_message,
                row.unread_count,
                row.read_inbox_max_id,
                row.read_outbox_max_id,
                row.pinned as i32,
                row.proto,
                row.origin_stream,
                pending_new
            ],
        )?;
        Ok(())
    }

    /// Compare-and-swap dialog touch: advances `top_message` only when newer.
    /// Missing stream rows inherit the principal's cache-incarnation generation floor.
    pub fn upsert_dialog_if_newer(
        &self,
        principal: &str,
        row: &DialogRow,
        expected_generation: Option<u64>,
    ) -> Result<bool, StorageError> {
        check_principal(principal)?;
        if row.peer_id == 0 || row.proto.len() > 1_000_000 {
            return Err(StorageError::InvalidInput("bad dialog row".into()));
        }
        check_origin(&row.origin_stream)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        if let (Some(expected_gen), origin) = (expected_generation, &row.origin_stream) {
            if !origin.is_empty() {
                let gen: Option<i64> = tx
                    .query_row(
                        "SELECT generation FROM stream_state WHERE principal=?1 AND stream=?2",
                        params![principal, origin],
                        |r| r.get(0),
                    )
                    .optional()?;
                let found = (gen.unwrap_or(0) as u64).max(Self::cache_incarnation(&tx, principal)?);
                if found != expected_gen {
                    return Err(StorageError::GenerationMismatch {
                        stream: origin.clone(),
                        expected: expected_gen,
                        found,
                    });
                }
            }
        }
        let existing: Option<i64> = tx
            .query_row(
                "SELECT top_message FROM dialogs WHERE principal=?1 AND peer_kind=?2 AND peer_id=?3",
                params![principal, row.peer_kind, row.peer_id],
                |r| r.get(0),
            )
            .optional()?;
        match existing {
            None => {
                tx.execute(
                    "INSERT INTO dialogs(principal, peer_kind, peer_id, top_message, unread_count,
                       read_inbox_max_id, read_outbox_max_id, pinned, proto, origin_stream,
                       read_state_pending)
                      VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,0)",
                    params![
                        principal,
                        row.peer_kind,
                        row.peer_id,
                        row.top_message,
                        row.unread_count,
                        row.read_inbox_max_id,
                        row.read_outbox_max_id,
                        row.pinned as i32,
                        row.proto,
                        row.origin_stream
                    ],
                )?;
                tx.commit()?;
                Ok(true)
            }
            Some(current_top) => {
                let target = format!("target:{}:{}", row.peer_kind, row.peer_id);
                let position = |id| -> Result<Option<i64>, StorageError> {
                    Ok(tx.query_row("SELECT accept_seq FROM messages WHERE principal=?1 AND conversation_key=?2 AND message_id=?3",
                        params![principal,target,id],|r|r.get(0)).optional()?.flatten())
                };
                let target = Self::dialog_target(row)?;
                let removed_old:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM message_tombstones WHERE principal=?1 AND conversation_key=?2 AND message_id=?3)",params![principal,target,current_top],|r|r.get(0))?;
                let candidate = position(row.top_message)?;
                // Keep the removed-head fast path first; it must not query the old position.
                if !(removed_old && candidate.is_some_and(|p| p > 0)
                    || matches!((position(current_top)?,candidate),(Some(a),Some(b)) if b>a))
                {
                    return Ok(false);
                }
                tx.execute(
                    "UPDATE dialogs SET top_message=?4, proto=?5, origin_stream=?6
                      WHERE principal=?1 AND peer_kind=?2 AND peer_id=?3",
                    params![
                        principal,
                        row.peer_kind,
                        row.peer_id,
                        row.top_message,
                        row.proto,
                        row.origin_stream
                    ],
                )?;
                tx.commit()?;
                Ok(true)
            }
        }
    }

    // -- file-cache subsystem (v4) -------------------------------------------

    fn check_file_row(row: &FileRow) -> Result<(), StorageError> {
        check_file_scope(&row.scope)?;
        check_file_id(row.file_id)?;
        check_origin(&row.origin_stream)?;
        if row.size_bytes < 0 || row.size_bytes > 1_000_000_000 {
            return Err(StorageError::InvalidInput("bad file size_bytes".into()));
        }
        if row.sha256.len() > 64 {
            return Err(StorageError::InvalidInput("bad file sha256".into()));
        }
        if row.mime_type.len() > 128 || row.file_name.len() > 256 {
            return Err(StorageError::InvalidInput("bad file mime/name".into()));
        }
        if row.created_at < 0 {
            return Err(StorageError::InvalidInput("bad file created_at".into()));
        }
        Ok(())
    }

    /// Authoritative file write (e.g. avatar/prefetch refresh). Immutable:
    /// the first writer wins (`INSERT OR IGNORE`); replays are no-ops.
    /// `origin_stream == ""` pins globally so scoped rebuilds never collect
    /// the row while refs remain.
    pub fn upsert_file(&self, principal: &str, row: &FileRow) -> Result<(), StorageError> {
        check_principal(principal)?;
        Self::check_file_row(row)?;
        let conn = self.lock()?;
        conn.execute(
            "INSERT OR IGNORE INTO files(principal, scope, file_id, size_bytes, sha256,
               mime_type, file_name, encrypted, created_at, origin_stream)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                principal,
                row.scope,
                row.file_id,
                row.size_bytes,
                row.sha256,
                row.mime_type,
                row.file_name,
                row.encrypted as i32,
                row.created_at,
                row.origin_stream
            ],
        )?;
        Ok(())
    }

    pub fn get_file(
        &self,
        principal: &str,
        scope: &str,
        file_id: i64,
    ) -> Result<Option<FileRow>, StorageError> {
        check_principal(principal)?;
        check_file_scope(scope)?;
        check_file_id(file_id)?;
        let conn = self.lock()?;
        let row: Option<FileRowTuple> = conn
            .query_row(
                "SELECT size_bytes, sha256, mime_type, file_name, encrypted, created_at, origin_stream
                 FROM files WHERE principal=?1 AND scope=?2 AND file_id=?3",
                params![principal, scope, file_id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get::<_, Option<Vec<u8>>>(1)?.unwrap_or_default(),
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                    ))
                },
            )
            .optional()?;
        Ok(row.map(
            |(size_bytes, sha256, mime_type, file_name, encrypted, created_at, origin_stream)| {
                FileRow {
                    scope: scope.to_string(),
                    file_id,
                    size_bytes,
                    sha256,
                    mime_type,
                    file_name,
                    encrypted: encrypted != 0,
                    created_at,
                    origin_stream,
                }
            },
        ))
    }

    /// Total live refs addressing one file: `message_files` + `entity_files`
    /// rows for `(principal, scope, file_id)`. Used to gate `files`
    /// garbage collection (row deleted only when this returns 0 and the
    /// owning scope is rebuilt).
    pub fn file_refcount(
        &self,
        principal: &str,
        scope: &str,
        file_id: i64,
    ) -> Result<i64, StorageError> {
        check_principal(principal)?;
        check_file_scope(scope)?;
        check_file_id(file_id)?;
        let conn = self.lock()?;
        let messages: i64 = conn.query_row(
            "SELECT COUNT(*) FROM message_files
             WHERE principal=?1 AND file_scope=?2 AND file_id=?3",
            params![principal, scope, file_id],
            |r| r.get(0),
        )?;
        let entities: i64 = conn.query_row(
            "SELECT COUNT(*) FROM entity_files
             WHERE principal=?1 AND file_scope=?2 AND file_id=?3",
            params![principal, scope, file_id],
            |r| r.get(0),
        )?;
        Ok(messages + entities)
    }

    pub fn list_message_files(
        &self,
        principal: &str,
        conversation_key: &str,
        message_id: i64,
    ) -> Result<Vec<MessageFileRef>, StorageError> {
        check_principal(principal)?;
        check_conversation_key(conversation_key)?;
        if message_id <= 0 {
            return Err(StorageError::InvalidInput(
                "message_id must be positive".into(),
            ));
        }
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT conversation_key, message_id, file_scope, file_id, role
             FROM message_files WHERE principal=?1 AND conversation_key=?2 AND message_id=?3",
        )?;
        let rows = stmt.query_map(params![principal, conversation_key, message_id], |r| {
            Ok(MessageFileRef {
                conversation_key: r.get(0)?,
                message_id: r.get(1)?,
                file_scope: r.get(2)?,
                file_id: r.get(3)?,
                role: r.get(4)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Local cache-state write (bytes on disk). Upsert: latest state wins.
    pub fn upsert_file_cache_entry(
        &self,
        principal: &str,
        entry: &FileCacheEntry,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_file_scope(&entry.scope)?;
        check_file_id(entry.file_id)?;
        check_availability(&entry.availability)?;
        if entry.relative_path.len() > 1024 || entry.relative_path.contains('\0') {
            return Err(StorageError::InvalidInput("bad cache relative_path".into()));
        }
        if entry.actual_size < 0 || entry.last_access < 0 {
            return Err(StorageError::InvalidInput("bad cache size/access".into()));
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO file_cache_entries(principal, scope, file_id, relative_path,
               actual_size, availability, last_access, pinned)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
             ON CONFLICT(principal, scope, file_id)
             DO UPDATE SET relative_path=excluded.relative_path,
               actual_size=excluded.actual_size, availability=excluded.availability,
               last_access=excluded.last_access, pinned=excluded.pinned",
            params![
                principal,
                entry.scope,
                entry.file_id,
                entry.relative_path,
                entry.actual_size,
                entry.availability,
                entry.last_access,
                entry.pinned as i32
            ],
        )?;
        Ok(())
    }

    pub fn get_file_cache_entry(
        &self,
        principal: &str,
        scope: &str,
        file_id: i64,
    ) -> Result<Option<FileCacheEntry>, StorageError> {
        check_principal(principal)?;
        check_file_scope(scope)?;
        check_file_id(file_id)?;
        let conn = self.lock()?;
        let row: Option<(String, i64, String, i64, i32)> = conn
            .query_row(
                "SELECT relative_path, actual_size, availability, last_access, pinned
                 FROM file_cache_entries WHERE principal=?1 AND scope=?2 AND file_id=?3",
                params![principal, scope, file_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
        Ok(row.map(
            |(relative_path, actual_size, availability, last_access, pinned)| FileCacheEntry {
                scope: scope.to_string(),
                file_id,
                relative_path,
                actual_size,
                availability,
                last_access,
                pinned: pinned != 0,
            },
        ))
    }

    /// Durable transfer-job write (latest state wins).
    pub fn upsert_file_transfer(
        &self,
        principal: &str,
        job: &FileTransferJob,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_job_id(&job.job_id)?;
        check_transfer_kind(&job.kind)?;
        check_file_scope(&job.file_scope)?;
        if job.file_id < 0 {
            return Err(StorageError::InvalidInput("bad transfer file_id".into()));
        }
        if job.resume_offset < 0 || job.updated_at < 0 {
            return Err(StorageError::InvalidInput("bad transfer offsets".into()));
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO file_transfers(principal, job_id, kind, file_scope, file_id,
               upload_id, state, session_json, temp_path, resume_offset, updated_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
             ON CONFLICT(principal, job_id)
             DO UPDATE SET kind=excluded.kind, file_scope=excluded.file_scope,
               file_id=excluded.file_id, upload_id=excluded.upload_id, state=excluded.state,
               session_json=excluded.session_json, temp_path=excluded.temp_path,
               resume_offset=excluded.resume_offset, updated_at=excluded.updated_at",
            params![
                principal,
                job.job_id,
                job.kind,
                job.file_scope,
                job.file_id,
                job.upload_id,
                job.state,
                job.session_json,
                job.temp_path,
                job.resume_offset,
                job.updated_at
            ],
        )?;
        Ok(())
    }

    pub fn get_file_transfer(
        &self,
        principal: &str,
        job_id: &str,
    ) -> Result<Option<FileTransferJob>, StorageError> {
        check_principal(principal)?;
        check_job_id(job_id)?;
        let conn = self.lock()?;
        let row: Option<FileTransferJobRow> = conn
            .query_row(
                "SELECT kind, file_scope, file_id, upload_id, state, session_json,
                    temp_path, resume_offset, updated_at
                 FROM file_transfers WHERE principal=?1 AND job_id=?2",
                params![principal, job_id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                    ))
                },
            )
            .optional()?;
        Ok(row.map(
            |(
                kind,
                file_scope,
                file_id,
                upload_id,
                state,
                session_json,
                temp_path,
                resume_offset,
                updated_at,
            )| {
                FileTransferJob {
                    job_id: job_id.to_string(),
                    kind,
                    file_scope,
                    file_id,
                    upload_id,
                    state,
                    session_json,
                    temp_path,
                    resume_offset,
                    updated_at,
                }
            },
        ))
    }

    pub fn delete_file_transfer(&self, principal: &str, job_id: &str) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_job_id(job_id)?;
        let conn = self.lock()?;
        conn.execute(
            "DELETE FROM file_transfers WHERE principal=?1 AND job_id=?2",
            params![principal, job_id],
        )?;
        Ok(())
    }

    /// Delete one local cache-state row (eviction of bytes on disk).
    /// Metadata in `files` and refs are untouched; use `file_refcount`
    /// to gate collection of unreferenced rows.
    pub fn delete_file_cache_entry(
        &self,
        principal: &str,
        scope: &str,
        file_id: i64,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_file_scope(scope)?;
        check_file_id(file_id)?;
        let conn = self.lock()?;
        conn.execute(
            "DELETE FROM file_cache_entries WHERE principal=?1 AND scope=?2 AND file_id=?3",
            params![principal, scope, file_id],
        )?;
        Ok(())
    }

    /// List local cache-state rows for one principal (eviction scan).
    /// Ordered by `last_access` ascending (least-recently-used first).
    pub fn list_file_cache_entries(
        &self,
        principal: &str,
    ) -> Result<Vec<FileCacheEntry>, StorageError> {
        check_principal(principal)?;
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT scope, file_id, relative_path, actual_size, availability, last_access, pinned
             FROM file_cache_entries WHERE principal=?1 ORDER BY last_access ASC",
        )?;
        let rows = stmt.query_map(params![principal], |r| {
            Ok(FileCacheEntry {
                scope: r.get(0)?,
                file_id: r.get(1)?,
                relative_path: r.get(2)?,
                actual_size: r.get(3)?,
                availability: r.get(4)?,
                last_access: r.get(5)?,
                pinned: r.get::<_, i32>(6)? != 0,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Idempotent entity → file ref insert (global avatar/sticker pins).
    /// Used by snapshot ingest paths that have no journal page to commit
    /// through; journal paths keep using `apply_journal_page_fenced`.
    pub fn upsert_entity_file_ref(
        &self,
        principal: &str,
        r: &EntityFileRef,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_entity_file_kind(&r.kind)?;
        check_file_scope(&r.file_scope)?;
        check_file_id(r.file_id)?;
        check_file_role(&r.role)?;
        if r.entity_id <= 0 {
            return Err(StorageError::InvalidInput(
                "bad entity_file entity_id".into(),
            ));
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT OR IGNORE INTO entity_files(principal, kind, entity_id, file_scope, file_id, role)
             VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                principal,
                r.kind,
                r.entity_id,
                r.file_scope,
                r.file_id,
                r.role
            ],
        )?;
        Ok(())
    }

    /// Transactional replace of one entity's ref set (snapshot ingest):
    /// delete current refs for `(kind, entity_id)`, then insert `new_refs`.
    /// Stale avatar refs are collected; an empty `new_refs` clears removals.
    pub fn replace_entity_file_refs(
        &self,
        principal: &str,
        kind: &str,
        entity_id: i64,
        new_refs: &[EntityFileRef],
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_entity_file_kind(kind)?;
        if entity_id <= 0 {
            return Err(StorageError::InvalidInput(
                "bad entity_file entity_id".into(),
            ));
        }
        for r in new_refs {
            check_file_scope(&r.file_scope)?;
            check_file_id(r.file_id)?;
            check_file_role(&r.role)?;
            if r.kind != kind || r.entity_id != entity_id {
                return Err(StorageError::InvalidInput(
                    "replace refs must match (kind, entity_id)".into(),
                ));
            }
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM entity_files WHERE principal=?1 AND kind=?2 AND entity_id=?3",
            params![principal, kind, entity_id],
        )?;
        for r in new_refs {
            tx.execute(
                "INSERT OR IGNORE INTO entity_files(principal, kind, entity_id, file_scope, file_id, role)
                 VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    principal,
                    r.kind,
                    r.entity_id,
                    r.file_scope,
                    r.file_id,
                    r.role
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Idempotent message → file ref insert. Journal paths keep using
    /// `apply_journal_page_fenced`; this covers out-of-band registration.
    pub fn upsert_message_file_ref(
        &self,
        principal: &str,
        r: &MessageFileRef,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_conversation_key(&r.conversation_key)?;
        check_file_scope(&r.file_scope)?;
        check_file_id(r.file_id)?;
        check_file_role(&r.role)?;
        if r.message_id <= 0 {
            return Err(StorageError::InvalidInput(
                "bad message_file message_id".into(),
            ));
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT OR IGNORE INTO message_files(principal, conversation_key, message_id, file_scope, file_id, role)
             VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                principal,
                r.conversation_key,
                r.message_id,
                r.file_scope,
                r.file_id,
                r.role
            ],
        )?;
        Ok(())
    }

    /// List durable transfer jobs for one principal, optionally filtered
    /// by `kind` (`upload`/`download`). Used for crash recovery
    /// (resume or garbage-collect orphaned `temp_path` files).
    pub fn list_file_transfers(
        &self,
        principal: &str,
        kind: Option<&str>,
    ) -> Result<Vec<FileTransferJob>, StorageError> {
        check_principal(principal)?;
        if let Some(k) = kind {
            check_transfer_kind(k)?;
        }
        let conn = self.lock()?;
        let query = match kind {
            Some(_) => "SELECT job_id, kind, file_scope, file_id, upload_id, state, session_json, temp_path, resume_offset, updated_at FROM file_transfers WHERE principal=?1 AND kind=?2 ORDER BY updated_at ASC",
            None => "SELECT job_id, kind, file_scope, file_id, upload_id, state, session_json, temp_path, resume_offset, updated_at FROM file_transfers WHERE principal=?1 ORDER BY updated_at ASC",
        };
        let mut stmt = conn.prepare(query)?;
        let map_row = |r: &rusqlite::Row<'_>| {
            Ok(FileTransferJob {
                job_id: r.get(0)?,
                kind: r.get(1)?,
                file_scope: r.get(2)?,
                file_id: r.get(3)?,
                upload_id: r.get(4)?,
                state: r.get(5)?,
                session_json: r.get(6)?,
                temp_path: r.get(7)?,
                resume_offset: r.get(8)?,
                updated_at: r.get(9)?,
            })
        };
        let rows: Result<Vec<FileTransferJob>, rusqlite::Error> = match kind {
            Some(k) => stmt.query_map(params![principal, k], map_row)?.collect(),
            None => stmt.query_map(params![principal], map_row)?.collect(),
        };
        Ok(rows?)
    }

    /// Enqueue persistent push/catch-up work. Idempotent replay
    /// (`INSERT OR IGNORE`): the first enqueue wins, retries are no-ops.
    pub fn enqueue_background_job(
        &self,
        principal: &str,
        job: &BackgroundSyncJob,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_job_id(&job.job_id)?;
        if !job.target_key.is_empty() {
            check_target_key(&job.target_key)?;
        }
        if job.attempts < 0 || job.next_due < 0 || job.updated_at < 0 {
            return Err(StorageError::InvalidInput("bad sync job counters".into()));
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT OR IGNORE INTO background_sync_jobs(principal, job_id, kind, target_key,
               state, payload, attempts, next_due, claimed_by, updated_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                principal,
                job.job_id,
                job.kind,
                job.target_key,
                job.state,
                job.payload,
                job.attempts,
                job.next_due,
                job.claimed_by,
                job.updated_at
            ],
        )?;
        Ok(())
    }

    pub fn get_background_job(
        &self,
        principal: &str,
        job_id: &str,
    ) -> Result<Option<BackgroundSyncJob>, StorageError> {
        check_principal(principal)?;
        check_job_id(job_id)?;
        let conn = self.lock()?;
        let row: Option<BackgroundSyncJobRow> = conn
            .query_row(
                "SELECT kind, target_key, state, payload, attempts, next_due, claimed_by, updated_at
                 FROM background_sync_jobs WHERE principal=?1 AND job_id=?2",
                params![principal, job_id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get::<_, Option<Vec<u8>>>(3)?.unwrap_or_default(),
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                    ))
                },
            )
            .optional()?;
        Ok(row.map(
            |(kind, target_key, state, payload, attempts, next_due, claimed_by, updated_at)| {
                BackgroundSyncJob {
                    job_id: job_id.to_string(),
                    kind,
                    target_key,
                    state,
                    payload,
                    attempts,
                    next_due,
                    claimed_by,
                    updated_at,
                }
            },
        ))
    }

    /// Process-safe claim: compare-and-set on `claimed_by` plus an
    /// `attempts` bump in one atomic `UPDATE`.
    ///
    /// - `expected == None` claims only when currently unclaimed
    ///   (`claimed_by IS NULL`); `expected == Some(owner)` claims only when
    ///   currently held by `owner` (re-claim / handoff).
    /// - Only `pending` jobs can be claimed; claimed/done jobs return `false`.
    /// - On success sets `state='claimed'`, `claimed_by`, `attempts+1`.
    ///   Returns `true` when this caller won the race.
    pub fn claim_background_job(
        &self,
        principal: &str,
        job_id: &str,
        claimed_by: &str,
        expected: Option<&str>,
        now: i64,
    ) -> Result<bool, StorageError> {
        check_principal(principal)?;
        check_job_id(job_id)?;
        check_job_id(claimed_by)?;
        if now < 0 {
            return Err(StorageError::InvalidInput("bad claim timestamp".into()));
        }
        let conn = self.lock()?;
        let changed = match expected {
            None => conn.execute(
                "UPDATE background_sync_jobs
                 SET state='claimed', claimed_by=?3, attempts=attempts+1, updated_at=?4
                 WHERE principal=?1 AND job_id=?2
                   AND claimed_by IS NULL AND state='pending'",
                params![principal, job_id, claimed_by, now],
            )?,
            Some(owner) => conn.execute(
                "UPDATE background_sync_jobs
                 SET state='claimed', claimed_by=?3, attempts=attempts+1, updated_at=?4
                 WHERE principal=?1 AND job_id=?2
                   AND claimed_by=?5 AND state='claimed'",
                params![principal, job_id, claimed_by, now, owner],
            )?,
        };
        Ok(changed == 1)
    }

    /// Terminal transition for a claimed job (`done` / `failed`).
    /// Clears `claimed_by` so the row reads as finished, not leased.
    pub fn complete_background_job(
        &self,
        principal: &str,
        job_id: &str,
        state: &str,
        now: i64,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_job_id(job_id)?;
        if state != "done" && state != "failed" {
            return Err(StorageError::InvalidInput(
                "complete state must be done/failed".into(),
            ));
        }
        if now < 0 {
            return Err(StorageError::InvalidInput("bad complete timestamp".into()));
        }
        let conn = self.lock()?;
        conn.execute(
            "UPDATE background_sync_jobs
             SET state=?3, claimed_by=NULL, updated_at=?4
             WHERE principal=?1 AND job_id=?2",
            params![principal, job_id, state, now],
        )?;
        Ok(())
    }

    /// Release a claimed/failed job back to `pending` for retry.
    /// Used by the push path when a wake is deferred (deadline, logout
    /// fence lost, transient failure): the durable row survives, the
    /// lease is cleared, and the next wake re-claims it. Returns `true`
    /// when a row was released.
    pub fn release_background_job(
        &self,
        principal: &str,
        job_id: &str,
        next_due: i64,
        now: i64,
    ) -> Result<bool, StorageError> {
        check_principal(principal)?;
        check_job_id(job_id)?;
        if next_due < 0 || now < 0 {
            return Err(StorageError::InvalidInput("bad release timestamps".into()));
        }
        let conn = self.lock()?;
        let changed = conn.execute(
            "UPDATE background_sync_jobs
             SET state='pending', claimed_by=NULL, next_due=?3, updated_at=?4
             WHERE principal=?1 AND job_id=?2 AND state IN ('claimed','failed','pending')",
            params![principal, job_id, next_due, now],
        )?;
        Ok(changed == 1)
    }

    /// Requeue stale `claimed` jobs whose lease expired (`updated_at` older
    /// than `stale_before`). Crash recovery: a push extension killed
    /// mid-wake leaves a `claimed` row that no live owner will complete;
    /// the next cold start calls this to make those rows `pending` again.
    /// Returns the number of rows requeued.
    pub fn requeue_stale_background_jobs(
        &self,
        principal: &str,
        stale_before: i64,
        next_due: i64,
        now: i64,
    ) -> Result<usize, StorageError> {
        check_principal(principal)?;
        if stale_before < 0 || next_due < 0 || now < 0 {
            return Err(StorageError::InvalidInput("bad requeue timestamps".into()));
        }
        let conn = self.lock()?;
        let changed = conn.execute(
            "UPDATE background_sync_jobs
             SET state='pending', claimed_by=NULL, next_due=?3, updated_at=?4
             WHERE principal=?1 AND state='claimed' AND updated_at<?2",
            params![principal, stale_before, next_due, now],
        )?;
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Literal FTS input cannot activate operators; raw cached payloads are never rewritten.
    #[test]
    fn search_literals_unicode_filters_and_raw_payloads() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        for (id, body) in [
            (1, "Café HELLO 世界你好"),
            (2, "hello OR world"),
            (3, "hello world"),
            (4, "hello <script>world</script> \u{1f}o"),
        ] {
            let mut row = msg(id);
            row.body = body.into();
            row.proto = body.as_bytes().to_vec();
            ops.messages.push(row);
        }
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 0, 0)
            .unwrap();
        let search = |query: &str, mode| {
            s.search_messages(
                "user:1",
                &MessageSearchRequest {
                    query: query.into(),
                    mode,
                    ..Default::default()
                },
            )
            .unwrap()
        };
        assert_eq!(search("cafe", MessageSearchMode::Terms).hits.len(), 1);
        assert_eq!(
            search("hello OR world", MessageSearchMode::Terms)
                .hits
                .len(),
            1
        );
        assert_eq!(
            search("hello\" OR \"world", MessageSearchMode::Phrase)
                .hits
                .len(),
            1
        );
        assert_eq!(
            search("hello world", MessageSearchMode::Phrase).hits.len(),
            1
        );
        assert_eq!(search("hel", MessageSearchMode::Prefix).hits.len(), 4);
        assert_eq!(search("世界你好", MessageSearchMode::Phrase).hits.len(), 1);
        assert_eq!(search("世界", MessageSearchMode::Prefix).hits.len(), 1);
        assert!(search("你好", MessageSearchMode::Terms).hits.is_empty());
        let hit = &search("world", MessageSearchMode::Terms).hits[0];
        assert_eq!(hit.proto, b"hello <script>world</script> \x1fo");
        let filtered = MessageSearchRequest {
            query: "hello".into(),
            sender_id: Some(9),
            ..Default::default()
        };
        assert!(s
            .search_messages("user:1", &filtered)
            .unwrap()
            .hits
            .is_empty());
        for query in ["", "***", "hello\0world"] {
            assert!(s
                .search_messages(
                    "user:1",
                    &MessageSearchRequest {
                        query: query.into(),
                        ..Default::default()
                    }
                )
                .is_err());
        }
    }

    /// Large cached payloads are preserved whole; input budgets still fail rather than truncate.
    #[test]
    fn search_full_payload_and_input_budgets() {
        let s = Storage::open_in_memory().unwrap();
        let mut row = msg(1);
        row.body = format!("{} café <b>literal</b>", "x".repeat(40_000));
        row.proto = row.body.as_bytes().to_vec();
        let original = row.proto.clone();
        let ops = CacheOps {
            messages: vec![row],
            ..Default::default()
        };
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 0, 0)
            .unwrap();
        let page = s
            .search_messages(
                "user:1",
                &MessageSearchRequest {
                    query: "cafe".into(),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(page.hits[0].proto, original);
        for request in [
            MessageSearchRequest {
                query: "hello".into(),
                limit: 0,
                ..Default::default()
            },
            MessageSearchRequest {
                query: "hello".into(),
                limit: 51,
                ..Default::default()
            },
            MessageSearchRequest {
                query: "hello".repeat(103),
                ..Default::default()
            },
            MessageSearchRequest {
                query: "h".into(),
                mode: MessageSearchMode::Prefix,
                ..Default::default()
            },
        ] {
            assert!(matches!(
                s.search_messages("user:1", &request),
                Err(StorageError::InvalidInput(_))
            ));
        }
    }

    /// Explicit document IDs survive VACUUM/reopen; paging rejects edits and query/account reuse.
    #[test]
    fn search_document_identity_and_cursor_fences() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("search.sqlite");
        let path = path.to_str().unwrap();
        let s = Storage::open(path).unwrap();
        let mut ops = CacheOps::default();
        for id in [3, 20, 9] {
            let mut row = msg(id);
            row.body = "needle".into();
            ops.messages.push(row);
        }
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 0, 0)
            .unwrap();
        let req = MessageSearchRequest {
            query: "needle".into(),
            limit: 1,
            ..Default::default()
        };
        let page = s.search_messages("user:1", &req).unwrap();
        assert_eq!(page.hits[0].message_id, 9);
        let ids = || {
            s.lock()
                .unwrap()
                .prepare("SELECT document_id FROM message_documents ORDER BY document_id")
                .unwrap()
                .query_map([], |r| r.get::<_, i64>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        let before = ids();
        s.lock().unwrap().execute_batch("VACUUM").unwrap();
        assert_eq!(ids(), before);
        drop(s);
        let s = Storage::open(path).unwrap();
        assert!(!s.has_table("message_search_content").unwrap()); // The content object is a view.
        let mut req = MessageSearchRequest {
            cursor: page.next.clone(),
            ..req
        };
        assert_eq!(
            s.search_messages("user:1", &req).unwrap().hits[0].message_id,
            20
        );
        assert!(matches!(
            s.search_messages("user:2", &req),
            Err(StorageError::SearchChanged)
        ));
        req.query = "different".into();
        assert!(matches!(
            s.search_messages("user:1", &req),
            Err(StorageError::InvalidInput(_))
        ));
        req.query = "needle".into();
        s.clear_principal("user:1").unwrap();
        assert!(matches!(
            s.search_messages("user:1", &req),
            Err(StorageError::SearchChanged)
        ));
        assert!(s.check_search_snapshot(&page.snapshot).is_err());
        let fresh_generation = s
            .stream_state("user:1", "target:room:7")
            .unwrap()
            .generation;
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 0, fresh_generation)
            .unwrap();
        let next_id: i64 = s
            .lock()
            .unwrap()
            .query_row("SELECT min(document_id) FROM message_documents", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(next_id > *before.last().unwrap());
    }

    /// Upgrading an existing v4 cache backfills bodies once without duplicating full text.
    #[test]
    fn search_migrates_v4_and_reopens_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v4.sqlite");
        let path = path.to_str().unwrap();
        let s = Storage::open(path).unwrap();
        s.lock().unwrap().execute_batch("DROP TRIGGER messages_search_insert;
            DROP TRIGGER messages_search_update; DROP TRIGGER messages_search_delete;
            DROP TABLE message_search; DROP VIEW message_search_content; DROP TABLE message_documents;
            DROP TABLE message_search_revisions; DELETE FROM meta WHERE key='search_database_id';
            UPDATE meta SET value='4' WHERE key='schema_version';
             INSERT INTO stream_state(principal,stream,valid,replay_floor,generation) VALUES('user:1','target:room:7',1,0,0);
             INSERT INTO messages(principal,conversation_key,message_id,peer_kind,peer_id,from_id,body,proto)
            VALUES('user:1','target:room:7',10,'room',7,42,'backfilled',X'');").unwrap();
        drop(s);
        let req = MessageSearchRequest {
            query: "backfilled".into(),
            ..Default::default()
        };
        for _ in 0..2 {
            let s = Storage::open(path).unwrap();
            assert_eq!(s.search_messages("user:1", &req).unwrap().hits.len(), 1);
            assert!(!s.has_table("message_search_content").unwrap());
            s.lock()
                .unwrap()
                .execute(
                    "INSERT INTO message_search(message_search,rank) VALUES('integrity-check',1)",
                    [],
                )
                .unwrap();
        }
    }

    /// Search observes same-cursor hydration, edits, rollback, removals and principal wipes.
    #[test]
    fn search_tracks_atomic_cache_mutations() {
        let s = Storage::open_in_memory().unwrap();
        let request = MessageSearchRequest {
            query: "hello".into(),
            ..Default::default()
        };
        let mut row = msg(10);
        row.body = "hello world".into();
        let mut ops = CacheOps {
            messages: vec![row.clone()],
            ..Default::default()
        };
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 0, 0)
            .unwrap();
        let page = s.search_messages("user:1", &request).unwrap();
        assert_eq!(page.hits.len(), 1);
        assert!(s
            .search_messages("user:2", &request)
            .unwrap()
            .hits
            .is_empty());
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(0));
        row.body = "goodbye world".into();
        row.edit_date_secs = Some(1);
        ops.messages = vec![row.clone()];
        ops.dialogs = vec![dlg(0, 10)]; // Fails after the message write.
        assert!(s
            .apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .is_err());
        assert_eq!(s.search_messages("user:1", &request).unwrap().hits.len(), 1);
        ops.dialogs.clear();
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        assert!(s
            .search_messages("user:1", &request)
            .unwrap()
            .hits
            .is_empty());
        let goodbye = MessageSearchRequest {
            query: "goodbye".into(),
            ..Default::default()
        };
        assert_eq!(s.search_messages("user:1", &goodbye).unwrap().hits.len(), 1);
        // An older body with the same acceptance token never re-enters the index.
        let mut stale = row.clone();
        stale.body = "hello stale".into();
        stale.edit_date_secs = None;
        let stale_ops = CacheOps {
            messages: vec![stale],
            ..Default::default()
        };
        s.apply_journal_page_fenced("user:1", "target:room:7", &stale_ops, 1, 0)
            .unwrap();
        assert!(s
            .search_messages("user:1", &request)
            .unwrap()
            .hits
            .is_empty());
        s.mark_rebuild("user:1", "target:room:7").unwrap();
        assert!(s
            .search_messages("user:1", &goodbye)
            .unwrap()
            .hits
            .is_empty());
        s.install_snapshot("user:1", "target:room:7", 0, 0, None)
            .unwrap();
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 0, 1)
            .unwrap();
        let removals = CacheOps {
            remove_messages: vec![("target:room:7".into(), 10)],
            ..Default::default()
        };
        s.apply_journal_page_fenced("user:1", "target:room:7", &removals, 1, 1)
            .unwrap();
        assert!(s
            .search_messages("user:1", &goodbye)
            .unwrap()
            .hits
            .is_empty());
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 1)
            .unwrap();
        s.clear_principal("user:1").unwrap();
        assert!(s
            .search_messages("user:1", &goodbye)
            .unwrap()
            .hits
            .is_empty());
        s.lock()
            .unwrap()
            .execute(
                "INSERT INTO message_search(message_search, rank) VALUES('integrity-check', 1)",
                [],
            )
            .unwrap();
    }

    fn msg(id: i64) -> MessageRow {
        MessageRow {
            conversation_key: "target:room:7".into(),
            message_id: id,
            peer_kind: "room".into(),
            peer_id: 7,
            from_id: 42,
            body: format!("m{id}"),
            proto: vec![1, 2, 3],
            origin_stream: "target:room:7".into(),
            accept_seq: Some(id),
            edit_date_secs: None,
            edit_date_nanos: None,
        }
    }

    fn dlg(peer_id: i64, top: i64) -> DialogRow {
        DialogRow {
            peer_kind: "room".into(),
            peer_id,
            top_message: top,
            unread_count: 0,
            read_inbox_max_id: 0,
            read_outbox_max_id: 0,
            pinned: false,
            proto: vec![4, 5, 6],
            origin_stream: "target:room:7".into(),
            read_state_pending: false,
        }
    }

    #[test]
    fn journal_round_trip_and_principal_isolation() {
        let s = Storage::open_in_memory().unwrap();
        assert_eq!(s.load_journal("user:1", "target:user:1").unwrap(), None);
        s.save_journal("user:1", "target:user:1", 3).unwrap();
        assert_eq!(s.load_journal("user:1", "target:user:1").unwrap(), Some(3));
        assert_eq!(s.load_journal("user:2", "target:user:2").unwrap(), None);
        s.clear_principal("user:1").unwrap();
        assert_eq!(s.load_journal("user:1", "target:user:1").unwrap(), None);
    }

    #[test]
    fn journal_page_commits_cache_and_checkpoint_together() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 3, 0)
            .unwrap();
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(3));
        let got = s.get_messages("user:1", "target:room:7", 10, None).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].message_id, 10);
        // Backward move must not commit partial writes.
        let mut ops2 = CacheOps::default();
        ops2.messages.push(msg(11));
        let err = s
            .apply_journal_page_fenced("user:1", "target:room:7", &ops2, 2, 0)
            .unwrap_err();
        assert!(matches!(err, StorageError::ScopeMismatch { .. }));
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(3));
        assert_eq!(
            s.get_messages("user:1", "target:room:7", 10, None)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn empty_page_advance_is_idempotent() {
        let s = Storage::open_in_memory().unwrap();
        s.apply_journal_page_fenced("user:1", "target:room:7", &CacheOps::default(), 0, 0)
            .unwrap();
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(0));
        s.apply_journal_page_fenced("user:1", "target:room:7", &CacheOps::default(), 5, 0)
            .unwrap();
        // Same cursor again is a no-op success.
        s.apply_journal_page_fenced("user:1", "target:room:7", &CacheOps::default(), 5, 0)
            .unwrap();
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(5));
    }

    #[test]
    fn body_date_not_acceptance_orders_copies() {
        let s = Storage::open_in_memory().unwrap();
        let mut first = msg(10);
        first.accept_seq = Some(5);
        first.body = "v1".into();
        let mut ops = CacheOps::default();
        ops.messages.push(first);
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 5, 0)
            .unwrap();
        // Stale token does not overwrite.
        let mut stale = msg(10);
        stale.accept_seq = Some(3);
        stale.body = "stale".into();
        let mut ops2 = CacheOps::default();
        ops2.messages.push(stale);
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops2, 6, 0)
            .unwrap();
        let got = s.get_messages("user:1", "target:room:7", 10, None).unwrap();
        assert_eq!(got[0].body, "v1");
        assert_eq!(got[0].accept_seq, Some(5));
        // Only a later edit date orders an unversioned body.
        let mut newer = msg(10);
        newer.accept_seq = Some(9);
        newer.edit_date_secs = Some(20);
        newer.body = "v2".into();
        let mut ops3 = CacheOps::default();
        ops3.messages.push(newer);
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops3, 9, 0)
            .unwrap();
        let got = s.get_messages("user:1", "target:room:7", 10, None).unwrap();
        assert_eq!(got[0].body, "v2");
    }

    #[test]
    fn null_accept_seq_orders_by_edit_date_only() {
        let s = Storage::open_in_memory().unwrap();
        let mut legacy = msg(10);
        legacy.accept_seq = None;
        legacy.edit_date_secs = None;
        legacy.edit_date_nanos = None;
        legacy.body = "legacy".into();
        let mut ops = CacheOps::default();
        ops.messages.push(legacy);
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        // Absent edit_date never beats absent: no replace.
        let mut undated = msg(10);
        undated.accept_seq = None;
        undated.edit_date_secs = None;
        undated.body = "undated2".into();
        let mut ops2 = CacheOps::default();
        ops2.messages.push(undated);
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops2, 2, 0)
            .unwrap();
        assert_eq!(
            s.get_messages("user:1", "target:room:7", 10, None).unwrap()[0].body,
            "legacy"
        );
        // Dated beats absent.
        let mut dated = msg(10);
        dated.accept_seq = None;
        dated.edit_date_secs = Some(20);
        dated.edit_date_nanos = Some(0);
        dated.body = "dated".into();
        let mut ops3 = CacheOps::default();
        ops3.messages.push(dated);
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops3, 3, 0)
            .unwrap();
        assert_eq!(
            s.get_messages("user:1", "target:room:7", 10, None).unwrap()[0].body,
            "dated"
        );
        // Older dated does not beat newer dated.
        let mut older = msg(10);
        older.accept_seq = None;
        older.edit_date_secs = Some(10);
        older.body = "older".into();
        let mut ops4 = CacheOps::default();
        ops4.messages.push(older);
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops4, 4, 0)
            .unwrap();
        assert_eq!(
            s.get_messages("user:1", "target:room:7", 10, None).unwrap()[0].body,
            "dated"
        );
    }

    #[test]
    fn remove_messages_deletes_only_targeted() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        let mut m11 = msg(11);
        m11.body = "m11".into();
        ops.messages.push(m11);
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 2, 0)
            .unwrap();
        assert_eq!(
            s.get_messages("user:1", "target:room:7", 10, None)
                .unwrap()
                .len(),
            2
        );
        let mut del = CacheOps::default();
        del.remove_messages.push(("target:room:7".into(), 10));
        s.apply_journal_page_fenced("user:1", "target:room:7", &del, 3, 0)
            .unwrap();
        let got = s.get_messages("user:1", "target:room:7", 10, None).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].message_id, 11);
    }

    #[test]
    fn rebuild_fences_commits_until_checkpoint_resumes() {
        let s = Storage::open_in_memory().unwrap();
        s.save_journal("user:1", "target:user:1", 5).unwrap();
        s.mark_rebuild("user:1", "target:user:1").unwrap();
        assert!(!s.stream_state("user:1", "target:user:1").unwrap().valid);
        assert_eq!(s.load_journal("user:1", "target:user:1").unwrap(), None);
        let ops = CacheOps::default();
        assert!(s
            .apply_journal_page_fenced("user:1", "target:user:1", &ops, 1, 0)
            .is_err());
        s.save_journal("user:1", "target:user:1", 1).unwrap();
        assert!(s.stream_state("user:1", "target:user:1").unwrap().valid);
    }

    #[test]
    fn rebuild_resets_cache_until_snapshot_recommits() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        ops.dialogs.push(dlg(7, 10));
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        assert_eq!(
            s.get_messages("user:1", "target:room:7", 10, None)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(s.list_dialogs("user:1", 10).unwrap().len(), 1);
        s.mark_rebuild("user:1", "target:room:7").unwrap();
        assert!(s
            .get_messages("user:1", "target:room:7", 10, None)
            .unwrap()
            .is_empty());
        assert!(s.list_dialogs("user:1", 10).unwrap().is_empty());
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), None);
        assert!(!s.stream_state("user:1", "target:room:7").unwrap().valid);
        s.save_journal("user:1", "target:room:7", 1).unwrap();
        let mut ops2 = CacheOps::default();
        let mut m11 = msg(11);
        m11.body = "m11".into();
        ops2.messages.push(m11);
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops2, 2, 1)
            .unwrap();
        assert_eq!(
            s.get_messages("user:1", "target:room:7", 10, None)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn rebuild_is_target_scoped() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops7 = CacheOps::default();
        ops7.messages.push(msg(10));
        ops7.dialogs.push(dlg(7, 10));
        ops7.users.push(UserRow {
            user_id: 42,
            proto: vec![1],
        });
        ops7.chats.push(ChatRow {
            chat_id: 7,
            proto: vec![2],
        });
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops7, 1, 0)
            .unwrap();

        let mut ops8 = CacheOps::default();
        let mut m8 = msg(20);
        m8.conversation_key = "target:room:8".into();
        m8.peer_id = 8;
        m8.origin_stream = "target:room:8".into();
        ops8.messages.push(m8);
        let mut d8 = dlg(8, 20);
        d8.origin_stream = "target:room:8".into();
        ops8.dialogs.push(d8);
        ops8.users.push(UserRow {
            user_id: 42,
            proto: vec![1],
        });
        ops8.chats.push(ChatRow {
            chat_id: 7,
            proto: vec![2],
        });
        s.apply_journal_page_fenced("user:1", "target:room:8", &ops8, 1, 0)
            .unwrap();

        s.mark_rebuild("user:1", "target:room:7").unwrap();

        assert!(s
            .get_messages("user:1", "target:room:7", 10, None)
            .unwrap()
            .is_empty());
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), None);
        assert!(!s.stream_state("user:1", "target:room:7").unwrap().valid);
        assert_eq!(
            s.get_messages("user:1", "target:room:8", 10, None)
                .unwrap()
                .len(),
            1
        );
        let dialogs = s.list_dialogs("user:1", 10).unwrap();
        assert_eq!(dialogs.len(), 1);
        assert_eq!(dialogs[0].peer_id, 8);
        assert!(s.get_user("user:1", 42).unwrap().is_some());
        assert!(s.get_chat("user:1", 7).unwrap().is_some());
        assert_eq!(s.load_journal("user:1", "target:room:8").unwrap(), Some(1));
    }

    #[test]
    fn shared_entity_deleted_after_last_reference() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.users.push(UserRow {
            user_id: 42,
            proto: vec![1],
        });
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        assert!(s.get_user("user:1", 42).unwrap().is_some());
        s.mark_rebuild("user:1", "target:room:7").unwrap();
        assert!(s.get_user("user:1", 42).unwrap().is_none());
    }

    #[test]
    fn global_pin_survives_target_rebuild() {
        let s = Storage::open_in_memory().unwrap();
        s.upsert_user(
            "user:1",
            &UserRow {
                user_id: 42,
                proto: vec![1],
            },
        )
        .unwrap();
        s.upsert_chat(
            "user:1",
            &ChatRow {
                chat_id: 7,
                proto: vec![2],
            },
        )
        .unwrap();
        s.mark_rebuild("user:1", "target:room:7").unwrap();
        assert!(s.get_user("user:1", 42).unwrap().is_some());
        assert!(s.get_chat("user:1", 7).unwrap().is_some());
    }

    #[test]
    fn migration_2_to_3_rekeys_to_journal_targets() {
        let dir = std::env::temp_dir().join(format!(
            "tinkle-mig3-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tinkle.db");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta(key, value) VALUES('schema_version', '2');
                 CREATE TABLE checkpoints(principal TEXT NOT NULL, stream TEXT NOT NULL, cursor INTEGER NOT NULL, PRIMARY KEY(principal, stream));
                 INSERT INTO checkpoints(principal, stream, cursor) VALUES('user:1', 'room:7', 4);
                 CREATE TABLE stream_state(principal TEXT NOT NULL, stream TEXT NOT NULL, valid INTEGER NOT NULL DEFAULT 1, replay_floor INTEGER NOT NULL DEFAULT 0, head INTEGER, generation INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(principal, stream));
                 INSERT INTO stream_state(principal, stream, valid, replay_floor, head, generation) VALUES('user:1', 'room:7', 1, 9, 12, 0);
                 CREATE TABLE messages(principal TEXT NOT NULL, conversation_key TEXT NOT NULL, message_id INTEGER NOT NULL, peer_kind TEXT NOT NULL, peer_id INTEGER NOT NULL, from_id INTEGER NOT NULL, body TEXT NOT NULL, proto BLOB NOT NULL, origin_stream TEXT NOT NULL DEFAULT '', PRIMARY KEY(principal, conversation_key, message_id));
                 INSERT INTO messages(principal, conversation_key, message_id, peer_kind, peer_id, from_id, body, proto, origin_stream) VALUES('user:1', 'room:7', 10, 'room', 7, 42, 'm10', X'010203', 'room:7');
                 CREATE TABLE dialogs(principal TEXT NOT NULL, peer_kind TEXT NOT NULL, peer_id INTEGER NOT NULL, top_message INTEGER NOT NULL DEFAULT 0, unread_count INTEGER NOT NULL DEFAULT 0, read_inbox_max_id INTEGER NOT NULL DEFAULT 0, read_outbox_max_id INTEGER NOT NULL DEFAULT 0, pinned INTEGER NOT NULL DEFAULT 0, proto BLOB NOT NULL, origin_stream TEXT NOT NULL DEFAULT '', read_state_pending INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(principal, peer_kind, peer_id));
                 CREATE TABLE users(principal TEXT NOT NULL, user_id INTEGER NOT NULL, proto BLOB NOT NULL, PRIMARY KEY(principal, user_id));
                 CREATE TABLE chats(principal TEXT NOT NULL, chat_id INTEGER NOT NULL, proto BLOB NOT NULL, PRIMARY KEY(principal, chat_id));
                 CREATE TABLE entity_provenance(principal TEXT NOT NULL, kind TEXT NOT NULL, entity_id INTEGER NOT NULL, stream TEXT NOT NULL, PRIMARY KEY(principal, kind, entity_id, stream));
                 INSERT INTO entity_provenance(principal, kind, entity_id, stream) VALUES('user:1', 'user', 42, 'room:7');",
            )
            .unwrap();
        }
        let s = Storage::open(path.to_str().unwrap()).unwrap();
        // Legacy v2 checkpoint cursors are stream-replay positions, not
        // journal positions: migration drops them so journaling restarts at
        // 0 instead of skipping from a bogus cursor.
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), None);
        let st = s.stream_state("user:1", "target:room:7").unwrap();
        assert_eq!(st.replay_floor, 0);
        let msgs = s.get_messages("user:1", "target:room:7", 10, None).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].origin_stream, "target:room:7");
        let conn = rusqlite::Connection::open(&path).unwrap();
        let v: String = conn
            .query_row(
                "SELECT value FROM meta WHERE key='schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        // The 1→2→3→4 chain runs in one open: a v2 database lands on v4
        // with the file-cache tables present and legacy rows intact.
        assert_eq!(v, SCHEMA_VERSION.to_string());
        for table in [
            "files",
            "file_cache_entries",
            "message_files",
            "entity_files",
            "file_transfers",
            "background_sync_jobs",
        ] {
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    rusqlite::params![table],
                    |r| r.get::<_, i64>(0),
                )
                .map(|n| n == 1)
                .unwrap();
            assert!(exists, "missing v4 table {table}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_state_pending_commits_atomically_and_clears_on_refresh() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        ops.dialogs.push(dlg(7, 10));
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        assert!(!s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);

        let mut inv = CacheOps::default();
        inv.read_state_scopes.push("target:room:7".into());
        s.apply_journal_page_fenced("user:1", "target:room:7", &inv, 2, 0)
            .unwrap();
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(2));
        assert!(s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);

        let mut touch = CacheOps::default();
        let mut m11 = msg(11);
        m11.body = "m11".into();
        touch.messages.push(m11);
        touch.dialogs.push(dlg(7, 11));
        s.apply_journal_page_fenced("user:1", "target:room:7", &touch, 3, 0)
            .unwrap();
        assert!(s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);

        s.upsert_dialog("user:1", &dlg(7, 11)).unwrap();
        assert!(!s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);
    }

    #[test]
    fn rejects_bad_inputs_without_sql_leak() {
        let s = Storage::open_in_memory().unwrap();
        assert!(s.load_journal("bad", "target:user:1").is_err());
        assert!(s.load_journal("user:1", "room:7").is_err());
        assert!(s.save_journal("user:1", "target:user:1", -1).is_err());
        assert!(s.get_messages("user:1", "", 10, None).is_err());
        assert!(s.get_messages("user:1", "room:7", 10, None).is_err());
    }

    #[test]
    fn peerless_message_advances_journal_without_dialog() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(MessageRow {
            conversation_key: "target:room:7".into(),
            message_id: 10,
            peer_kind: "unknown".into(),
            peer_id: 0,
            from_id: 42,
            body: "m10".into(),
            proto: vec![1, 2, 3],
            origin_stream: "target:room:7".into(),
            accept_seq: Some(1),
            edit_date_secs: None,
            edit_date_nanos: None,
        });
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(1));
        assert_eq!(
            s.get_messages("user:1", "target:room:7", 10, None)
                .unwrap()
                .len(),
            1
        );
        assert!(s.list_dialogs("user:1", 10).unwrap().is_empty());
        let mut bad = CacheOps::default();
        bad.messages.push(MessageRow {
            conversation_key: "target:room:7".into(),
            message_id: 11,
            peer_kind: "room".into(),
            peer_id: 0,
            from_id: 42,
            body: "m11".into(),
            proto: vec![1, 2, 3],
            origin_stream: "target:room:7".into(),
            accept_seq: Some(2),
            edit_date_secs: None,
            edit_date_nanos: None,
        });
        assert!(s
            .apply_journal_page_fenced("user:1", "target:room:7", &bad, 2, 0)
            .is_err());
    }

    #[test]
    fn fresh_open_write_close_reopen_v3() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tinkle.db");
        let path_str = path.to_str().unwrap().to_string();
        {
            let s = Storage::open(&path_str).unwrap();
            let mut ops = CacheOps::default();
            ops.messages.push(msg(10));
            ops.dialogs.push(dlg(7, 10));
            s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
                .unwrap();
            assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(1));
            let conn = rusqlite::Connection::open(&path).unwrap();
            let v: String = conn
                .query_row(
                    "SELECT value FROM meta WHERE key='schema_version'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(v, SCHEMA_VERSION.to_string());
        }
        for _ in 0..2 {
            let s = Storage::open(&path_str).unwrap();
            assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(1));
            assert_eq!(
                s.get_messages("user:1", "target:room:7", 10, None)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(s.list_dialogs("user:1", 10).unwrap().len(), 1);
            let st = s.stream_state("user:1", "target:room:7").unwrap();
            assert!(st.valid);
            assert_eq!(st.generation, 0);
            assert_eq!(st.replay_floor, 0);
        }
    }

    #[test]
    fn clear_principal_empties_all_tables() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        ops.dialogs.push(dlg(7, 10));
        ops.users.push(UserRow {
            user_id: 42,
            proto: vec![1],
        });
        ops.chats.push(ChatRow {
            chat_id: 7,
            proto: vec![2],
        });
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        s.upsert_user(
            "user:1",
            &UserRow {
                user_id: 99,
                proto: vec![9],
            },
        )
        .unwrap();
        let mut other = CacheOps::default();
        other.messages.push(MessageRow {
            conversation_key: "target:room:8".into(),
            message_id: 5,
            peer_kind: "room".into(),
            peer_id: 8,
            from_id: 1,
            body: "hi".into(),
            proto: vec![7],
            origin_stream: "target:room:8".into(),
            accept_seq: Some(1),
            edit_date_secs: None,
            edit_date_nanos: None,
        });
        s.apply_journal_page_fenced("user:2", "target:room:8", &other, 1, 0)
            .unwrap();
        s.clear_principal("user:1").unwrap();
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), None);
        assert!(s
            .get_messages("user:1", "target:room:7", 10, None)
            .unwrap()
            .is_empty());
        assert!(s.list_dialogs("user:1", 10).unwrap().is_empty());
        assert!(s.get_user("user:1", 42).unwrap().is_none());
        assert!(s.get_user("user:1", 99).unwrap().is_none());
        assert!(s.get_chat("user:1", 7).unwrap().is_none());
        let st = s.stream_state("user:1", "target:room:7").unwrap();
        assert!(st.valid);
        assert!(st.generation > 0);
        assert_eq!(
            st.generation,
            s.cache_principal_incarnation("user:1").unwrap()
        );
        assert_eq!(st.replay_floor, 0);
        assert_eq!(st.head, None);
        assert_eq!(s.load_journal("user:2", "target:room:8").unwrap(), Some(1));
        assert_eq!(
            s.get_messages("user:2", "target:room:8", 10, None)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn generation_fencing_rejects_stale_page() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        let gen0 = s
            .stream_state("user:1", "target:room:7")
            .unwrap()
            .generation;
        assert_eq!(gen0, 0);
        let mut m11 = msg(11);
        m11.body = "m11".into();
        let mut ops2 = CacheOps::default();
        ops2.messages.push(m11);
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops2, 2, 0)
            .unwrap();
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(2));
        s.mark_rebuild("user:1", "target:room:7").unwrap();
        let gen1 = s
            .stream_state("user:1", "target:room:7")
            .unwrap()
            .generation;
        assert_eq!(gen1, 1);
        let mut stale = msg(12);
        stale.body = "stale".into();
        let mut stale_ops = CacheOps::default();
        stale_ops.messages.push(stale);
        let err = s
            .apply_journal_page_fenced("user:1", "target:room:7", &stale_ops, 1, 0)
            .unwrap_err();
        assert!(matches!(err, StorageError::GenerationMismatch { .. }));
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), None);
        assert!(s
            .apply_journal_page_fenced("user:1", "target:room:7", &CacheOps::default(), 1, 0)
            .is_err());
        let returned = s
            .install_snapshot("user:1", "target:room:7", 5, 0, Some(10))
            .unwrap();
        assert_eq!(returned, 1);
        let st = s.stream_state("user:1", "target:room:7").unwrap();
        assert!(st.valid);
        assert_eq!(st.generation, 1);
        assert_eq!(st.replay_floor, 0);
        assert_eq!(st.head, Some(10));
        assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(5));
        let mut ops3 = CacheOps::default();
        let mut m13 = msg(13);
        m13.body = "m13".into();
        ops3.messages.push(m13);
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops3, 6, 1)
            .unwrap();
        let err2 = s
            .apply_journal_page_fenced("user:1", "target:room:7", &CacheOps::default(), 7, 0)
            .unwrap_err();
        assert!(matches!(err2, StorageError::GenerationMismatch { .. }));
    }

    #[test]
    fn read_state_pending_preservation() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        ops.dialogs.push(dlg(7, 10));
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        let mut inv = CacheOps::default();
        inv.read_state_scopes.push("target:room:7".into());
        s.apply_journal_page_fenced("user:1", "target:room:7", &inv, 2, 0)
            .unwrap();
        assert!(s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);
        let mut d = dlg(7, 12);
        d.unread_count = 3;
        s.upsert_dialog_authoritative("user:1", &d, false).unwrap();
        let got = s.list_dialogs("user:1", 10).unwrap();
        assert_eq!(got[0].top_message, 12);
        assert_eq!(got[0].unread_count, 3);
        assert!(got[0].read_state_pending);
        s.upsert_dialog_authoritative("user:1", &d, true).unwrap();
        assert!(!s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);
        let mut inv2 = CacheOps::default();
        inv2.read_state_scopes.push("target:room:7".into());
        s.apply_journal_page_fenced("user:1", "target:room:7", &inv2, 3, 0)
            .unwrap();
        assert!(s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);
        let gen = s
            .stream_state("user:1", "target:room:7")
            .unwrap()
            .generation;
        let mut heads = CacheOps::default();
        for (id, position) in [(12, 1), (20, 2)] {
            let mut row = msg(id);
            row.accept_seq = Some(position);
            heads.messages.push(row);
        }
        s.apply_journal_page_fenced("user:1", "target:room:7", &heads, 3, gen)
            .unwrap();
        assert!(!s
            .upsert_dialog_if_newer("user:1", &dlg(7, 5), Some(gen))
            .unwrap());
        assert!(s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);
        assert!(s
            .upsert_dialog_if_newer("user:1", &dlg(7, 20), Some(gen))
            .unwrap());
        let got2 = s.list_dialogs("user:1", 10).unwrap();
        assert_eq!(got2[0].top_message, 20);
        assert!(got2[0].read_state_pending);
        let err = s
            .upsert_dialog_if_newer("user:1", &dlg(7, 21), Some(gen + 99))
            .unwrap_err();
        assert!(matches!(err, StorageError::GenerationMismatch { .. }));
    }

    /// Wiping a principal retires old dialog writers even before a new stream row exists.
    #[test]
    fn dialog_touch_uses_principal_generation_floor_after_wipe() {
        let s = Storage::open_in_memory().unwrap();
        let row = dlg(7, 10);
        s.clear_principal("user:1").unwrap();
        let generation = s
            .stream_state("user:1", &row.origin_stream)
            .unwrap()
            .generation;
        assert!(generation > 0);
        assert!(matches!(
            s.upsert_dialog_if_newer("user:1", &row, Some(0)),
            Err(StorageError::GenerationMismatch { expected: 0, found, .. }) if found == generation
        ));
        assert!(s.list_dialogs("user:1", 10).unwrap().is_empty());
        assert!(s
            .upsert_dialog_if_newer("user:1", &row, Some(generation))
            .unwrap());
        // Another principal still starts at zero; the incarnation floor is not global.
        assert!(s.upsert_dialog_if_newer("user:2", &row, Some(0)).unwrap());
        s.clear_principal("user:1").unwrap();
        assert!(matches!(
            s.upsert_dialog_if_newer("user:1", &row, Some(generation)),
            Err(StorageError::GenerationMismatch { .. })
        ));
        assert!(s.list_dialogs("user:1", 10).unwrap().is_empty());
    }

    #[test]
    fn journal_key_grammar_rejected() {
        let s = Storage::open_in_memory().unwrap();
        assert!(s.load_journal("user:1", "room:7").is_err());
        assert!(s.load_journal("user:1", "target:dm:7").is_err());
        assert!(s.load_journal("user:1", "target:user:0").is_err());
        assert!(s
            .apply_journal_page_fenced("user:1", "target:room:7", &CacheOps::default(), -1, 0)
            .is_err());
    }

    // -- v4 file-cache subsystem -----------------------------------------

    fn file_row(scope: &str, file_id: i64, origin: &str) -> FileRow {
        FileRow {
            scope: scope.into(),
            file_id,
            size_bytes: 1024,
            sha256: vec![0xabu8; 32],
            mime_type: "image/jpeg".into(),
            file_name: format!("photo{file_id}.jpg"),
            encrypted: false,
            created_at: 1_700_000_000,
            origin_stream: origin.into(),
        }
    }

    fn msg_file(conv: &str, message_id: i64, file_scope: &str, file_id: i64) -> MessageFileRef {
        MessageFileRef {
            conversation_key: conv.into(),
            message_id,
            file_scope: file_scope.into(),
            file_id,
            role: "attachment".into(),
        }
    }

    fn sync_job(job_id: &str, target: &str) -> BackgroundSyncJob {
        BackgroundSyncJob {
            job_id: job_id.into(),
            kind: "catch_up".into(),
            target_key: target.into(),
            state: "pending".into(),
            payload: vec![1, 2, 3],
            attempts: 0,
            next_due: 100,
            claimed_by: None,
            updated_at: 100,
        }
    }

    #[test]
    fn migration_3_to_4_is_idempotent_and_preserves_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tinkle.db");
        let path_str = path.to_str().unwrap().to_string();
        {
            // Hand-built v3 database: no file-cache tables, version '3'.
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta(key, value) VALUES('schema_version', '3');
                 CREATE TABLE checkpoints(principal TEXT NOT NULL, stream TEXT NOT NULL, cursor INTEGER NOT NULL, PRIMARY KEY(principal, stream));
                 INSERT INTO checkpoints(principal, stream, cursor) VALUES('user:1', 'target:room:7', 4);
                 CREATE TABLE stream_state(principal TEXT NOT NULL, stream TEXT NOT NULL, valid INTEGER NOT NULL DEFAULT 1, replay_floor INTEGER NOT NULL DEFAULT 0, head INTEGER, generation INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(principal, stream));
                 INSERT INTO stream_state(principal,stream,valid) VALUES('user:1','target:room:7',1);
                 CREATE TABLE messages(principal TEXT NOT NULL, conversation_key TEXT NOT NULL, message_id INTEGER NOT NULL, peer_kind TEXT NOT NULL, peer_id INTEGER NOT NULL, from_id INTEGER NOT NULL, body TEXT NOT NULL, proto BLOB NOT NULL, origin_stream TEXT NOT NULL DEFAULT '', accept_seq INTEGER, edit_date_secs INTEGER, edit_date_nanos INTEGER, PRIMARY KEY(principal, conversation_key, message_id));
                 INSERT INTO messages(principal, conversation_key, message_id, peer_kind, peer_id, from_id, body, proto, origin_stream, accept_seq) VALUES('user:1', 'target:room:7', 10, 'room', 7, 42, 'm10', X'010203', 'target:room:7', 4);
                 CREATE TABLE dialogs(principal TEXT NOT NULL, peer_kind TEXT NOT NULL, peer_id INTEGER NOT NULL, top_message INTEGER NOT NULL DEFAULT 0, unread_count INTEGER NOT NULL DEFAULT 0, read_inbox_max_id INTEGER NOT NULL DEFAULT 0, read_outbox_max_id INTEGER NOT NULL DEFAULT 0, pinned INTEGER NOT NULL DEFAULT 0, proto BLOB NOT NULL, origin_stream TEXT NOT NULL DEFAULT '', read_state_pending INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(principal, peer_kind, peer_id));
                 CREATE TABLE users(principal TEXT NOT NULL, user_id INTEGER NOT NULL, proto BLOB NOT NULL, PRIMARY KEY(principal, user_id));
                 CREATE TABLE chats(principal TEXT NOT NULL, chat_id INTEGER NOT NULL, proto BLOB NOT NULL, PRIMARY KEY(principal, chat_id));
                 CREATE TABLE entity_provenance(principal TEXT NOT NULL, kind TEXT NOT NULL, entity_id INTEGER NOT NULL, stream TEXT NOT NULL, PRIMARY KEY(principal, kind, entity_id, stream));",
            )
            .unwrap();
        }
        // First open migrates 3→4; second + third opens replay idempotently.
        for _ in 0..3 {
            let s = Storage::open(&path_str).unwrap();
            assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(4));
            assert_eq!(
                s.get_messages("user:1", "target:room:7", 10, None)
                    .unwrap()
                    .len(),
                1
            );
            let conn = rusqlite::Connection::open(&path).unwrap();
            let v: String = conn
                .query_row(
                    "SELECT value FROM meta WHERE key='schema_version'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(v, SCHEMA_VERSION.to_string());
        }
        // v4 writes survive a reopen (migration did not wipe them).
        {
            let s = Storage::open(&path_str).unwrap();
            s.upsert_file("user:1", &file_row("", 9, "")).unwrap();
            assert!(s.get_file("user:1", "", 9).unwrap().is_some());
        }
        {
            let s = Storage::open(&path_str).unwrap();
            assert!(s.get_file("user:1", "", 9).unwrap().is_some());
            let v: String = rusqlite::Connection::open(&path)
                .unwrap()
                .query_row(
                    "SELECT value FROM meta WHERE key='schema_version'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(v, SCHEMA_VERSION.to_string());
        }
    }

    #[test]
    fn journal_page_upserts_files_and_refs_atomically() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        ops.files
            .push(file_row("target:room:7", 50, "target:room:7"));
        ops.message_files
            .push(msg_file("target:room:7", 10, "target:room:7", 50));
        ops.entity_files.push(EntityFileRef {
            kind: "user".into(),
            entity_id: 42,
            file_scope: "target:room:7".into(),
            file_id: 50,
            role: "avatar".into(),
        });
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        assert!(s.get_file("user:1", "target:room:7", 50).unwrap().is_some());
        // Immutable metadata: a replay with different bytes keeps the first.
        let mut replay = CacheOps::default();
        let mut changed = file_row("target:room:7", 50, "target:room:7");
        changed.size_bytes = 999_999;
        changed.file_name = "evil.jpg".into();
        replay.files.push(changed);
        replay
            .message_files
            .push(msg_file("target:room:7", 10, "target:room:7", 50));
        s.apply_journal_page_fenced("user:1", "target:room:7", &replay, 2, 0)
            .unwrap();
        let kept = s.get_file("user:1", "target:room:7", 50).unwrap().unwrap();
        assert_eq!(kept.size_bytes, 1024);
        assert_eq!(kept.file_name, "photo50.jpg");
        assert_eq!(s.file_refcount("user:1", "target:room:7", 50).unwrap(), 2);
        // Ref replay is idempotent: no duplicate rows, refcount unchanged.
        assert_eq!(
            s.list_message_files("user:1", "target:room:7", 10)
                .unwrap()
                .len(),
            1
        );
        // Validators follow the check_* pattern: bad file_id / scope /
        // origin fail without SQL text.
        let mut bad = CacheOps::default();
        let mut bad_file = file_row("target:room:7", 0, "target:room:7");
        bad_file.file_id = 0;
        bad.files.push(bad_file);
        let err = s
            .apply_journal_page_fenced("user:1", "target:room:7", &bad, 3, 0)
            .unwrap_err();
        assert!(matches!(err, StorageError::InvalidInput(_)));
        assert!(!format!("{err}").contains("SELECT"));
        let mut bad_origin = CacheOps::default();
        bad_origin
            .files
            .push(file_row("target:room:7", 51, "target:room:8"));
        assert!(s
            .apply_journal_page_fenced("user:1", "target:room:7", &bad_origin, 3, 0)
            .is_err());
        // Backward cursor still refuses the whole batch (no partial file write).
        let mut stale = CacheOps::default();
        stale
            .files
            .push(file_row("target:room:7", 77, "target:room:7"));
        let err = s
            .apply_journal_page_fenced("user:1", "target:room:7", &stale, 1, 0)
            .unwrap_err();
        assert!(matches!(err, StorageError::ScopeMismatch { .. }));
        assert!(s.get_file("user:1", "target:room:7", 77).unwrap().is_none());
    }

    #[test]
    fn file_refcount_preserved_across_scoped_rebuilds() {
        let s = Storage::open_in_memory().unwrap();
        // One file shared by two conversations (message ref each).
        for (conv, mid, seq) in [("target:room:7", 10, 1), ("target:room:8", 20, 1)] {
            let mut ops = CacheOps::default();
            let mut m = msg(mid);
            m.conversation_key = conv.into();
            m.peer_id = if conv == "target:room:7" { 7 } else { 8 };
            m.origin_stream = conv.into();
            ops.messages.push(m);
            ops.files.push(file_row("shared", 60, conv));
            // Note: shared scope row inserted twice (second is a no-op);
            // each conversation holds its own message ref.
            ops.message_files.push(msg_file(conv, mid, "shared", 60));
            s.apply_journal_page_fenced("user:1", conv, &ops, seq, 0)
                .unwrap();
        }
        // Pin local bytes: rebuilds must never drop cache state.
        s.upsert_file_cache_entry(
            "user:1",
            &FileCacheEntry {
                scope: "shared".into(),
                file_id: 60,
                relative_path: "files/shared/60.bin".into(),
                actual_size: 1024,
                availability: "complete".into(),
                last_access: 200,
                pinned: false,
            },
        )
        .unwrap();
        assert_eq!(s.file_refcount("user:1", "shared", 60).unwrap(), 2);
        // Rebuilding room:7 drops its message ref; the shared file survives
        // via room:8's ref, and the bytes record survives regardless.
        // Files with a non-matching origin_stream are out of GC scope, so
        // give the shared row a room:7 origin to exercise the guard.
        s.upsert_file("user:1", &file_row("shared7", 61, "target:room:7"))
            .unwrap();
        let mut ops7 = CacheOps::default();
        ops7.message_files
            .push(msg_file("target:room:7", 10, "shared7", 61));
        // Direct ref insert via journal page (file row pinned above).
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops7, 2, 0)
            .unwrap();
        s.mark_rebuild("user:1", "target:room:7").unwrap();
        // Room:7 message refs are gone; room:8 refs remain.
        assert!(s
            .list_message_files("user:1", "target:room:7", 10)
            .unwrap()
            .is_empty());
        assert_eq!(s.file_refcount("user:1", "shared", 60).unwrap(), 1);
        // Bytes record untouched by the rebuild.
        let cache = s
            .get_file_cache_entry("user:1", "shared", 60)
            .unwrap()
            .unwrap();
        assert_eq!(cache.availability, "complete");
        // The room:7-origin file lost its only ref → collected.
        assert!(s.get_file("user:1", "shared7", 61).unwrap().is_none());
        // Rebuilding the last holder collects the now-unreferenced file row
        // only if its origin matches; "shared"/60 has origin room:7 which no
        // longer matches room:8's rebuild... so re-pin coverage: a file with
        // origin room:8 and a single ref is collected after rebuild.
        s.upsert_file("user:1", &file_row("shared8", 62, "target:room:8"))
            .unwrap();
        let mut ops8 = CacheOps::default();
        ops8.message_files
            .push(msg_file("target:room:8", 20, "shared8", 62));
        s.apply_journal_page_fenced("user:1", "target:room:8", &ops8, 2, 0)
            .unwrap();
        s.mark_rebuild("user:1", "target:room:8").unwrap();
        assert!(s.get_file("user:1", "shared8", 62).unwrap().is_none());
        // Global pin ('') never collected by scoped rebuilds.
        s.upsert_file("user:1", &file_row("", 63, "")).unwrap();
        s.mark_rebuild("user:1", "target:room:7").unwrap();
        assert!(s.get_file("user:1", "", 63).unwrap().is_some());
    }

    #[test]
    fn rebuild_scoping_preserves_entity_refs_cache_transfers_and_jobs() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        ops.files
            .push(file_row("target:room:7", 70, "target:room:7"));
        ops.message_files
            .push(msg_file("target:room:7", 10, "target:room:7", 70));
        ops.entity_files.push(EntityFileRef {
            kind: "sticker".into(),
            entity_id: 5,
            file_scope: "".into(),
            file_id: 71,
            role: "sticker".into(),
        });
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        // Global pin arrives via the authoritative path (journal pages
        // require origin == target_key; "" pins use upsert_file).
        s.upsert_file("user:1", &file_row("", 71, "")).unwrap();
        s.upsert_file_cache_entry(
            "user:1",
            &FileCacheEntry {
                scope: "target:room:7".into(),
                file_id: 70,
                relative_path: "files/70.bin".into(),
                actual_size: 1024,
                availability: "downloading".into(),
                last_access: 1,
                pinned: true,
            },
        )
        .unwrap();
        s.upsert_file_transfer(
            "user:1",
            &FileTransferJob {
                job_id: "dl-1".into(),
                kind: "download".into(),
                file_scope: "target:room:7".into(),
                file_id: 70,
                upload_id: "".into(),
                state: "active".into(),
                session_json: "{}".into(),
                temp_path: "tmp/dl-1.part".into(),
                resume_offset: 512,
                updated_at: 10,
            },
        )
        .unwrap();
        s.enqueue_background_job("user:1", &sync_job("job-1", "target:room:7"))
            .unwrap();
        s.mark_rebuild("user:1", "target:room:7").unwrap();
        // Scoped message ref gone; its now-unreferenced file collected.
        assert!(s.get_file("user:1", "target:room:7", 70).unwrap().is_none());
        // Entity refs are global pins: untouched, and their file survives.
        assert_eq!(s.file_refcount("user:1", "", 71).unwrap(), 1);
        assert!(s.get_file("user:1", "", 71).unwrap().is_some());
        // Bytes, transfers, and sync jobs are never rebuild-scoped.
        assert!(s
            .get_file_cache_entry("user:1", "target:room:7", 70)
            .unwrap()
            .is_some());
        assert!(s.get_file_transfer("user:1", "dl-1").unwrap().is_some());
        assert!(s.get_background_job("user:1", "job-1").unwrap().is_some());
    }

    #[test]
    fn remove_message_drops_refs_but_keeps_shared_file() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        let mut m10 = msg(10);
        m10.body = "m10".into();
        let mut m11 = msg(11);
        m11.body = "m11".into();
        ops.messages.push(m10);
        ops.messages.push(m11);
        ops.files
            .push(file_row("target:room:7", 80, "target:room:7"));
        ops.message_files
            .push(msg_file("target:room:7", 10, "target:room:7", 80));
        ops.message_files
            .push(msg_file("target:room:7", 11, "target:room:7", 80));
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        assert_eq!(s.file_refcount("user:1", "target:room:7", 80).unwrap(), 2);
        let mut del = CacheOps::default();
        del.remove_messages.push(("target:room:7".into(), 10));
        s.apply_journal_page_fenced("user:1", "target:room:7", &del, 2, 0)
            .unwrap();
        assert_eq!(s.file_refcount("user:1", "target:room:7", 80).unwrap(), 1);
        assert!(s.get_file("user:1", "target:room:7", 80).unwrap().is_some());
    }

    #[test]
    fn background_claim_is_compare_and_set() {
        let s = Storage::open_in_memory().unwrap();
        s.enqueue_background_job("user:1", &sync_job("job-1", "target:room:7"))
            .unwrap();
        // Enqueue replay is idempotent (first wins, no duplicate).
        s.enqueue_background_job("user:1", &sync_job("job-1", "target:room:7"))
            .unwrap();
        // First claim wins; loser sees false (no overwrite, attempts +1 once).
        assert!(s
            .claim_background_job("user:1", "job-1", "proc-a", None, 200)
            .unwrap());
        assert!(!s
            .claim_background_job("user:1", "job-1", "proc-b", None, 201)
            .unwrap());
        let held = s.get_background_job("user:1", "job-1").unwrap().unwrap();
        assert_eq!(held.claimed_by.as_deref(), Some("proc-a"));
        assert_eq!(held.attempts, 1);
        assert_eq!(held.state, "claimed");
        // Wrong expected owner fails; correct handoff succeeds.
        assert!(!s
            .claim_background_job("user:1", "job-1", "proc-c", Some("proc-b"), 202)
            .unwrap());
        assert!(s
            .claim_background_job("user:1", "job-1", "proc-c", Some("proc-a"), 203)
            .unwrap());
        let moved = s.get_background_job("user:1", "job-1").unwrap().unwrap();
        assert_eq!(moved.claimed_by.as_deref(), Some("proc-c"));
        assert_eq!(moved.attempts, 2);
        // Terminal transition clears the lease.
        s.complete_background_job("user:1", "job-1", "done", 204)
            .unwrap();
        let done = s.get_background_job("user:1", "job-1").unwrap().unwrap();
        assert_eq!(done.state, "done");
        assert_eq!(done.claimed_by, None);
        // Done jobs cannot be re-claimed.
        assert!(!s
            .claim_background_job("user:1", "job-1", "proc-d", None, 205)
            .unwrap());
        // Validators: empty job/owner rejected without SQL text.
        let err = s
            .claim_background_job("user:1", "", "proc-d", None, 205)
            .unwrap_err();
        assert!(matches!(err, StorageError::InvalidInput(_)));
        assert!(!format!("{err}").contains("SELECT"));
    }

    #[test]
    fn clear_principal_wipes_file_subsystem_per_principal() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        ops.files
            .push(file_row("target:room:7", 90, "target:room:7"));
        ops.message_files
            .push(msg_file("target:room:7", 10, "target:room:7", 90));
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
            .unwrap();
        s.upsert_file_cache_entry(
            "user:1",
            &FileCacheEntry {
                scope: "target:room:7".into(),
                file_id: 90,
                relative_path: "files/90.bin".into(),
                actual_size: 1,
                availability: "missing".into(),
                last_access: 0,
                pinned: false,
            },
        )
        .unwrap();
        s.upsert_file_transfer(
            "user:1",
            &FileTransferJob {
                job_id: "up-1".into(),
                kind: "upload".into(),
                file_scope: "target:room:7".into(),
                file_id: 90,
                upload_id: "u1".into(),
                state: "queued".into(),
                session_json: "{}".into(),
                temp_path: "".into(),
                resume_offset: 0,
                updated_at: 1,
            },
        )
        .unwrap();
        s.enqueue_background_job("user:1", &sync_job("job-9", "target:room:7"))
            .unwrap();
        // Other principal untouched.
        s.upsert_file("user:2", &file_row("", 91, "")).unwrap();
        s.clear_principal("user:1").unwrap();
        assert!(s.get_file("user:1", "target:room:7", 90).unwrap().is_none());
        assert!(s
            .get_file_cache_entry("user:1", "target:room:7", 90)
            .unwrap()
            .is_none());
        assert!(s.get_file_transfer("user:1", "up-1").unwrap().is_none());
        assert!(s.get_background_job("user:1", "job-9").unwrap().is_none());
        assert_eq!(s.file_refcount("user:1", "target:room:7", 90).unwrap(), 0);
        assert!(s
            .get_messages("user:1", "target:room:7", 10, None)
            .unwrap()
            .is_empty());
        assert!(s.get_file("user:2", "", 91).unwrap().is_some());
    }

    #[test]
    fn file_validators_reject_bad_inputs() {
        let s = Storage::open_in_memory().unwrap();
        assert!(s.get_file("user:1", "", 0).is_err());
        assert!(s.get_file("bad", "", 1).is_err());
        assert!(s
            .upsert_file_cache_entry(
                "user:1",
                &FileCacheEntry {
                    scope: "".into(),
                    file_id: 1,
                    relative_path: "a".into(),
                    actual_size: 0,
                    availability: "bogus".into(),
                    last_access: 0,
                    pinned: false,
                }
            )
            .is_err());
        assert!(s
            .upsert_file_transfer(
                "user:1",
                &FileTransferJob {
                    job_id: "t".into(),
                    kind: "sideways".into(),
                    file_scope: "".into(),
                    file_id: 1,
                    upload_id: "".into(),
                    state: "".into(),
                    session_json: "".into(),
                    temp_path: "".into(),
                    resume_offset: 0,
                    updated_at: 0,
                }
            )
            .is_err());
    }
}
