//! Cache encoding cutover rejects incompatible durable state without deleting or rewriting it.
use tinkle_storage::Storage;

#[cfg(tinkle_schema_candidate)]
fn assert_unmarked_state_is_rejected(table: &str, fixture: &str) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let path = path.to_str().unwrap();
    drop(Storage::open(path).unwrap());
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute("DELETE FROM meta WHERE key='media_encoding'", [])
        .unwrap();
    conn.execute_batch(fixture).unwrap();
    // Check for commits by the attempted opener, even if the row count stays the same.
    let data_version = || {
        conn.pragma_query_value(None, "data_version", |r| r.get::<_, i64>(0))
            .unwrap()
    };
    let before = data_version();
    assert!(
        matches!(
            Storage::open(path),
            Err(tinkle_storage::StorageError::Corrupt(_))
        ),
        "unmarked {table} must fail the compatibility gate"
    );
    assert_eq!(data_version(), before, "mutated {table} on rejection");
    let count: i64 = conn
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1, "failed open must retain the {table} row");
    let marked: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key='media_encoding')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!marked, "failed open must not relabel {table}");
}

/// Orphaned membership and queued jobs are durable media state even without cached bodies.
#[cfg(tinkle_schema_candidate)]
#[test]
fn unmarked_media_references_and_work_require_explicit_clean_reset() {
    for (table, insert) in [
        ("message_files", "INSERT INTO message_files(principal,conversation_key,message_id,file_id) VALUES('user:1','target:room:7',1,9)"),
        ("entity_files", "INSERT INTO entity_files(principal,kind,entity_id,file_id) VALUES('user:1','user',1,9)"),
        ("file_cache_entries", "INSERT INTO file_cache_entries(principal,file_id) VALUES('user:1',9)"),
        ("background_sync_jobs", "INSERT INTO background_sync_jobs(principal,job_id,payload) VALUES('user:1','pending',X'0809')"),
    ] {
        assert_unmarked_state_is_rejected(table, insert);
    }
}

/// Each table is the sole source of retained state, so none can mask a missing check.
#[cfg(tinkle_schema_candidate)]
#[test]
fn unmarked_visibility_journal_and_scope_state_require_explicit_clean_reset() {
    for (table, fixture) in [
        ("message_tombstones", "INSERT INTO message_tombstones VALUES('user:1','target:room:7',1,9)"),
        ("message_body_floors", "INSERT INTO message_body_floors VALUES('user:1','target:room:7',1,9)"),
        ("checkpoints", "UPDATE meta SET value='2' WHERE key='schema_version';
                         INSERT INTO checkpoints VALUES('user:1','room:7',9)"),
        ("stream_state", "INSERT INTO stream_state(principal,stream,valid,generation) VALUES('user:1','target:room:7',0,9)"),
        ("cache_incarnations", "INSERT INTO cache_incarnations VALUES('user:1',9)"),
        ("entity_provenance", "INSERT INTO entity_provenance VALUES('user:1','user',1,'target:room:7')"),
        ("message_search_revisions", "INSERT INTO message_search_revisions VALUES('user:1',9)"),
        ("message_documents", "INSERT INTO message_documents(principal,conversation_key,message_id) VALUES('user:1','target:room:7',1)"),
        ("send_update_state", "CREATE TABLE send_update_state(
             principal TEXT NOT NULL, source TEXT NOT NULL, cursor BLOB NOT NULL DEFAULT X'',
             refresh_revision INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(principal,source));
             INSERT INTO send_update_state VALUES('user:1','source',X'0809',1)"),
    ] {
        assert_unmarked_state_is_rejected(table, fixture);
    }
}

/// Schema metadata and an initialized but empty index do not prevent restamping.
#[test]
fn empty_unmarked_cache_is_restamped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let path = path.to_str().unwrap();
    drop(Storage::open(path).unwrap());
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute("DELETE FROM meta WHERE key='media_encoding'", [])
        .unwrap();

    drop(Storage::open(path).unwrap());
    let marker: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key='media_encoding'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(marker, env!("TINKLE_SCHEMA_WIRE_ID"));
}

/// Opening a cache from another media contract cannot reinterpret its uncertain outgoing bytes.
#[test]
fn old_candidate_media_encoding_is_rejected_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    drop(Storage::open(path.to_str().unwrap()).unwrap());
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO meta(key,value) VALUES('media_encoding','singular-thumbnail-v1')",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO outgoing_jobs(principal,source,job_id,target_key,method,payload,state) VALUES('user:1','source','original','target:room:7','SendMedia',X'0809',2)", []).unwrap();
    conn.execute("INSERT INTO messages(principal,conversation_key,message_id,peer_kind,peer_id,from_id,body,proto) VALUES('user:1','target:room:7',1,'room',7,1,'retained body',X'0809')", []).unwrap();
    assert!(Storage::open(path.to_str().unwrap()).is_err());
    let retained: (Vec<u8>, i64) = conn
        .query_row(
            "SELECT payload,state FROM outgoing_jobs WHERE job_id='original'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(retained, (vec![8, 9], 2));
    let encoding: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key='media_encoding'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(encoding, "singular-thumbnail-v1");
    let body: (String, Vec<u8>) = conn
        .query_row(
            "SELECT body,proto FROM messages WHERE message_id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(body, ("retained body".into(), vec![8, 9]));
}

/// Even an empty published-format cache cannot be relabelled by either candidate mode.
#[cfg(tinkle_schema_candidate)]
#[test]
fn published_marker_is_rejected_without_relabelling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    drop(Storage::open(path.to_str().unwrap()).unwrap());
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE meta SET value='published-media-v1' WHERE key='media_encoding'",
        [],
    )
    .unwrap();
    assert!(Storage::open(path.to_str().unwrap()).is_err());
    let marker: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key='media_encoding'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(marker, "published-media-v1");
}

/// A fresh cache retains its build's wire identity across reopening.
#[test]
fn fresh_cache_reopens_with_exact_wire_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    drop(Storage::open(path.to_str().unwrap()).unwrap());
    drop(Storage::open(path.to_str().unwrap()).unwrap());
    let conn = rusqlite::Connection::open(&path).unwrap();
    let marker: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key='media_encoding'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        marker,
        if cfg!(tinkle_schema_candidate) {
            "compact-wire-reset-v1"
        } else {
            "published-media-v1"
        }
    );
}
