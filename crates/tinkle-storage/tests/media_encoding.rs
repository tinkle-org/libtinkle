//! Cache encoding cutover rejects incompatible durable state without deleting or rewriting it.
use tinkle_storage::Storage;

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
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite");
        drop(Storage::open(path.to_str().unwrap()).unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute("DELETE FROM meta WHERE key='media_encoding'", []).unwrap();
        conn.execute(insert, []).unwrap();
        assert!(Storage::open(path.to_str().unwrap()).is_err(), "{table}");
        let count: i64 = conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1);
        let marked: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM meta WHERE key='media_encoding')", [], |r| r.get(0)).unwrap();
        assert!(!marked, "failed open must not relabel {table}");
    }
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
