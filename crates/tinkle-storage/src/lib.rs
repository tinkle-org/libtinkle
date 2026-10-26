//! Shared native SQLite cache + per-conversation journal cursors for libtinkle.
//!
//! Rust owns migrations, schema, transactions, and queries. Swift/Kotlin
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
//!   `GetTargetChanges` page (message upserts with `accept_seq` dedup,
//!   removals, dialog touches, entity hydration, read-state marks) plus the
//!   checkpoint advance, fenced by `expected_generation`.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension};
use thiserror::Error;

/// Schema v4: per-conversation journal keys (`target:user:<id>` /
/// `target:room:<id>`), `accept_seq` + `edit_date` dedup columns on
/// `messages`, `replay_floor` retained-but-zero, plus the file-cache
/// subsystem (`files`, `file_cache_entries`, `message_files`,
/// `entity_files`, `file_transfers`, `background_sync_jobs`).
const SCHEMA_VERSION: i64 = 4;

/// Errors surfaced from storage. Messages are safe for FFI; no SQL text.
#[derive(Debug, Error, Clone)]
pub enum StorageError {
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
/// `origin_stream` is the journal target that produced this row; `""` is a
/// legacy/global pin that scoped rebuilds never delete.
/// `accept_seq` is the message's own acceptance token (`None` for legacy
/// rows accepted before the token was introduced; absent tokens never vote
/// on dedup). `edit_date_secs/nanos` carry the message `edit_date` for
/// `fresher_body` ordering when either side lacks a token.
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

/// Raw `files` row as read from SQLite before mapping into [`FileRow`].
type FileRowTuple = (i64, Vec<u8>, String, String, i32, i64, String);

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
}

/// Durable per-conversation validity + fencing generation.
///
/// `generation` is bumped on every [`Storage::mark_rebuild`] and preserved
/// across checkpoint advances. Upper layers snapshot it via
/// [`Storage::stream_state`] (or the return value of
/// [`Storage::install_snapshot`]) and pass it as `expected_generation` to
/// [`Storage::apply_journal_page_fenced`] / [`Storage::upsert_dialog_if_newer`];
/// a mismatch means a rebuild raced the write and the batch must be dropped.
/// A missing row (fresh target or after [`Storage::clear_principal`], which
/// deletes all rows) reads as generation 0. `replay_floor` is retained but
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
                } else if parsed == SCHEMA_VERSION {
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
        tx.commit()?;
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

    // -- journal checkpoints -----------------------------------------------

    /// Durable journal cursor for one conversation: the last applied
    /// `GetTargetChanges.last_accept_seq` (0 when nothing applied yet).
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
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO checkpoints(principal, stream, cursor) VALUES(?1,?2,?3)
             ON CONFLICT(principal, stream) DO UPDATE SET cursor=excluded.cursor",
            params![principal, target_key, accept_seq],
        )?;
        conn.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor)
             VALUES(?1,?2,1,0)
             ON CONFLICT(principal, stream) DO UPDATE SET valid=1, replay_floor=0",
            params![principal, target_key],
        )?;
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

    /// Remove every row owned by `principal`: checkpoints, stream_state,
    /// cached messages/dialogs/users/chats, and the v4 file-cache subsystem
    /// (`files`, `file_cache_entries`, `message_files`, `entity_files`,
    /// `file_transfers`, `background_sync_jobs`). Logout-scoped wipe so a
    /// signed-out principal leaves no offline bodies behind.
    pub fn clear_principal(&self, principal: &str) -> Result<(), StorageError> {
        check_principal(principal)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
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
        ] {
            let sql = format!("DELETE FROM {table} WHERE principal=?1");
            tx.execute(&sql, params![principal])?;
        }
        tx.commit()?;
        Ok(())
    }

    // -- per-conversation validity / generation ------------------------------

    /// Invalidate one conversation's cache scope. Messages/dialogs carrying
    /// this target's origin are deleted; legacy `''`-origin rows are pinned
    /// and survive. Users/chats are deleted only when no provenance
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
        tx.execute(
            "DELETE FROM checkpoints WHERE principal=?1 AND stream=?2",
            params![principal, stream],
        )?;
        tx.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor, generation)
             VALUES(?1,?2,0,0,1)
             ON CONFLICT(principal, stream)
             DO UPDATE SET valid=0, generation=COALESCE(generation,0)+1",
            params![principal, stream],
        )?;
        tx.execute(
            "DELETE FROM messages WHERE principal=?1 AND origin_stream=?2",
            params![principal, stream],
        )?;
        tx.execute(
            "DELETE FROM dialogs WHERE principal=?1 AND origin_stream=?2",
            params![principal, stream],
        )?;
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
                generation: 0,
            },
            Some((valid, floor, head, gen)) => StreamState {
                valid: valid != 0,
                replay_floor: floor as u64,
                head: head.map(|h| h as u64),
                generation: gen.unwrap_or(0) as u64,
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
        let gen: Option<i64> = tx
            .query_row(
                "SELECT generation FROM stream_state WHERE principal=?1 AND stream=?2",
                params![principal, stream],
                |r| r.get(0),
            )
            .optional()?;
        let generation = gen.unwrap_or(0) as u64;
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
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor, head)
             VALUES(?1,?2,1,?3,?4)
             ON CONFLICT(principal, stream)
             DO UPDATE SET replay_floor=excluded.replay_floor, head=excluded.head",
            params![principal, stream, floor as i64, head.map(|h| h as i64)],
        )?;
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
    /// means `new_accept_seq` moved backward under us. Message upserts
    /// enforce `accept_seq` dedup (higher wins; equal tokens fall back to
    /// `edit_date`; absent tokens order only by `edit_date` via the same
    /// rule as `fresher_body`: dated beats absent, later beats earlier,
    /// absent never beats dated). File metadata upserts are immutable
    /// first-wins (`INSERT OR IGNORE`) and file refs are idempotent
    /// (`INSERT OR IGNORE`), committed in the same transaction.
    pub fn apply_journal_page_fenced(
        &self,
        principal: &str,
        target_key: &str,
        ops: &CacheOps,
        new_accept_seq: i64,
        expected_generation: u64,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_target_key(target_key)?;
        check_accept_seq(new_accept_seq)?;
        if ops.messages.len() > 512
            || ops.dialogs.len() > 256
            || ops.users.len() > 512
            || ops.chats.len() > 256
            || ops.read_state_scopes.len() > 64
            || ops.remove_messages.len() > 512
            || ops.files.len() > 512
            || ops.message_files.len() > 1024
            || ops.entity_files.len() > 512
        {
            return Err(StorageError::InvalidInput("sync batch too large".into()));
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
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let row: Option<(i64, Option<i64>)> = tx
            .query_row(
                "SELECT valid, generation FROM stream_state WHERE principal=?1 AND stream=?2",
                params![principal, target_key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let found_gen = row.and_then(|(_, g)| g).unwrap_or(0) as u64;
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
        }
        let current: Option<i64> = tx
            .query_row(
                "SELECT cursor FROM checkpoints WHERE principal=?1 AND stream=?2",
                params![principal, target_key],
                |r| r.get(0),
            )
            .optional()?;
        let current = current.unwrap_or(0);
        if new_accept_seq < current {
            return Err(StorageError::ScopeMismatch {
                stream: target_key.to_string(),
                expected: new_accept_seq,
                found: current,
            });
        }
        for m in &ops.messages {
            let peer_ok = m.peer_id != 0 || m.peer_kind == "unknown";
            if m.message_id <= 0 || !peer_ok || m.proto.len() > 2_000_000 {
                return Err(StorageError::InvalidInput("bad message row".into()));
            }
            let existing: Option<(Option<i64>, Option<i64>, Option<i32>)> = tx
                .query_row(
                    "SELECT accept_seq, edit_date_secs, edit_date_nanos FROM messages
                     WHERE principal=?1 AND conversation_key=?2 AND message_id=?3",
                    params![principal, m.conversation_key, m.message_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let should_write = match existing {
                None => true,
                Some((old_accept, old_secs, old_nanos)) => {
                    // Token-stable edits keep the same accept_seq across
                    // mutations: when tokens tie, fall back to edit_date
                    // (same rule as fresher_body).
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
                    match (old_accept, m.accept_seq) {
                        (Some(o), Some(n)) => {
                            if n > o {
                                true
                            } else if n == o {
                                fresher_by_date
                            } else {
                                false
                            }
                        }
                        _ => fresher_by_date,
                    }
                }
            };
            if !should_write {
                continue;
            }
            tx.execute(
                "INSERT INTO messages(principal, conversation_key, message_id, peer_kind, peer_id, from_id, body, proto, origin_stream, accept_seq, edit_date_secs, edit_date_nanos)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)
                 ON CONFLICT(principal, conversation_key, message_id)
                 DO UPDATE SET peer_kind=excluded.peer_kind, peer_id=excluded.peer_id,
                   from_id=excluded.from_id, body=excluded.body, proto=excluded.proto,
                   origin_stream=excluded.origin_stream, accept_seq=excluded.accept_seq,
                   edit_date_secs=excluded.edit_date_secs, edit_date_nanos=excluded.edit_date_nanos",
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
                    m.edit_date_nanos
                ],
            )?;
        }
        for (tk, mid) in &ops.remove_messages {
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
            if d.peer_id == 0 || d.proto.len() > 1_000_000 {
                return Err(StorageError::InvalidInput("bad dialog row".into()));
            }
            tx.execute(
                "INSERT INTO dialogs(principal, peer_kind, peer_id, top_message, unread_count,
                   read_inbox_max_id, read_outbox_max_id, pinned, proto, origin_stream)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
                 ON CONFLICT(principal, peer_kind, peer_id)
                 DO UPDATE SET top_message=max(top_message, excluded.top_message),
                   proto=CASE WHEN excluded.top_message > top_message
                     THEN excluded.proto ELSE proto END,
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
        for u in &ops.users {
            if u.user_id <= 0 || u.proto.len() > 1_000_000 {
                return Err(StorageError::InvalidInput("bad user row".into()));
            }
            tx.execute(
                "INSERT INTO users(principal, user_id, proto) VALUES(?1,?2,?3)
                 ON CONFLICT(principal, user_id) DO UPDATE SET proto=excluded.proto",
                params![principal, u.user_id, u.proto],
            )?;
        }
        for c in &ops.chats {
            if c.chat_id <= 0 || c.proto.len() > 1_000_000 {
                return Err(StorageError::InvalidInput("bad chat row".into()));
            }
            tx.execute(
                "INSERT INTO chats(principal, chat_id, proto) VALUES(?1,?2,?3)
                 ON CONFLICT(principal, chat_id) DO UPDATE SET proto=excluded.proto",
                params![principal, c.chat_id, c.proto],
            )?;
        }
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
        for r in &ops.entity_files {
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
            "INSERT INTO stream_state(principal, stream, valid, replay_floor)
             VALUES(?1,?2,1,0)
             ON CONFLICT(principal, stream) DO UPDATE SET valid=1, replay_floor=0",
            params![principal, target_key],
        )?;
        tx.commit()?;
        Ok(())
    }

    // -- offline queries ----------------------------------------------------

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
            "SELECT message_id FROM messages WHERE principal=?1 AND conversation_key=?2 ORDER BY message_id ASC",
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
             FROM dialogs WHERE principal=?1 ORDER BY pinned DESC, top_message DESC LIMIT ?2",
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

    /// Authoritative user write (e.g. GetUsers refresh). Pins globally.
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
        if row.peer_id == 0 || row.proto.len() > 1_000_000 {
            return Err(StorageError::InvalidInput("bad dialog row".into()));
        }
        check_origin(&row.origin_stream)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let pending_new: i64 = if clear_pending {
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
        tx.commit()?;
        Ok(())
    }

    /// Compare-and-swap dialog touch: advances `top_message` only when newer.
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
                let found = gen.unwrap_or(0) as u64;
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
                if row.top_message <= current_top {
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
    fn accept_seq_dedup_higher_wins() {
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
        // Newer token wins.
        let mut newer = msg(10);
        newer.accept_seq = Some(9);
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
        assert_eq!(v, "4");
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
            assert_eq!(v, "4");
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
        assert_eq!(st.generation, 0);
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
            assert_eq!(v, "4");
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
            assert_eq!(v, "4");
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
