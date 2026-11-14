//! Migration-era global-origin rows belong to their canonical conversation, not a global grant.
use tinkle_storage::{CacheOps, MessageRow, MessageSearchRequest, Storage};

/// An old cache already marked invalid must stay non-searchable when a fresh snapshot validates it.
#[test]
fn migrated_empty_origin_bodies_do_not_survive_rebuild_or_scope_revalidation() {
    for already_invalid in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite");
        let path = path.to_str().unwrap();
        {
            let s = Storage::open(path).unwrap();
            s.apply_journal_page_fenced(
                "user:1",
                "target:room:7",
                &CacheOps {
                    messages: vec![MessageRow {
                        conversation_key: "target:room:7".into(),
                        message_id: 1,
                        peer_kind: "room".into(),
                        peer_id: 7,
                        from_id: 1,
                        body: "retiredneedle".into(),
                        proto: vec![1],
                        origin_stream: "target:room:7".into(),
                        accept_seq: Some(100),
                        edit_date_secs: None,
                        edit_date_nanos: None,
                    }],
                    ..Default::default()
                },
                0,
                0,
            )
            .unwrap();
        }
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch("UPDATE messages SET origin_stream='';UPDATE meta SET value='4' WHERE key='schema_version';").unwrap();
        if already_invalid {
            conn.execute_batch("UPDATE stream_state SET valid=0,generation=1;")
                .unwrap();
        }
        drop(conn);
        let s = Storage::open(path).unwrap();
        let request = MessageSearchRequest {
            query: "retiredneedle".into(),
            ..Default::default()
        };
        if !already_invalid {
            s.mark_rebuild("user:1", "target:room:7").unwrap();
        }
        assert!(
            s.search_messages("user:1", &request)
                .unwrap()
                .hits
                .is_empty(),
            "invalid migrated scope leaked"
        );
        s.install_snapshot("user:1", "target:room:7", 0, 0, None)
            .unwrap();
        assert!(
            s.search_messages("user:1", &request)
                .unwrap()
                .hits
                .is_empty(),
            "revalidation resurrected old global-origin body"
        );
        assert!(s
            .get_messages("user:1", "target:room:7", 10, None)
            .unwrap()
            .is_empty());
    }
}

/// An older external writer may leave physical rows while invalid. Fresh FTS must check
/// scope validity independently of provenance/deletion, and validation must purge those rows.
#[test]
fn invalid_scope_filters_external_legacy_rows_and_validation_cannot_reveal_them() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let path = path.to_str().unwrap();
    let s = Storage::open(path).unwrap();
    s.mark_rebuild("user:1", "target:room:7").unwrap();
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("INSERT INTO messages(principal,conversation_key,message_id,peer_kind,peer_id,from_id,body,proto,origin_stream) VALUES('user:1','target:room:7',1,'room',7,1,'retiredneedle',X'01','');").unwrap();
    drop(conn);
    let request = MessageSearchRequest {
        query: "retiredneedle".into(),
        ..Default::default()
    };
    assert!(s
        .search_messages("user:1", &request)
        .unwrap()
        .hits
        .is_empty());
    s.save_journal("user:1", "target:room:7", 0).unwrap();
    assert!(s
        .search_messages("user:1", &request)
        .unwrap()
        .hits
        .is_empty());
    assert!(s
        .get_messages("user:1", "target:room:7", 10, None)
        .unwrap()
        .is_empty());
}
