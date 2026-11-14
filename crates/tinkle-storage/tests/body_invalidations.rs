//! Nonterminal identity invalidations fence held bodies without preventing rehydration.
use tinkle_storage::{CacheOps, MessageFileRef, MessageRow, Storage};

fn body(revision: Option<i64>, file: i64) -> CacheOps {
    CacheOps {
        messages: vec![MessageRow {
            conversation_key: "target:room:7".into(),
            message_id: 1,
            peer_kind: "room".into(),
            peer_id: 7,
            from_id: 10,
            body: "old body".into(),
            proto: vec![1],
            origin_stream: "target:room:7".into(),
            accept_seq: Some(1),
            edit_date_secs: None,
            edit_date_nanos: None,
        }],
        message_body_revisions: revision.map(|v| (1, v)).into_iter().collect(),
        message_files: vec![MessageFileRef {
            conversation_key: "target:room:7".into(),
            message_id: 1,
            file_scope: String::new(),
            file_id: file,
            role: "main".into(),
        }],
        ..Default::default()
    }
}

/// A newer withheld body evicts the prior grant, rejects held/unknown replies,
/// and allows an explicitly authorized same-revision body after reopen.
#[test]
fn invalidation_floor_is_durable_nonterminal_and_evicts_refs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let s = Storage::open(path.to_str().unwrap()).unwrap();
    s.apply_journal_page_fenced("user:1", "target:room:7", &body(Some(1), 10), 0, 0)
        .unwrap();
    let invalidation = CacheOps {
        body_invalidations: vec![("target:room:7".into(), 1, 2)],
        ..Default::default()
    };
    s.apply_journal_page_fenced("user:1", "target:room:7", &invalidation, 50, 0)
        .unwrap();
    assert!(
        s.get_messages("user:1", "target:room:7", 10, None)
            .unwrap()
            .is_empty(),
        "withheld current body must evict the cached grant"
    );
    assert!(s
        .list_message_files("user:1", "target:room:7", 1)
        .unwrap()
        .is_empty());
    drop(s);
    let s = Storage::open(path.to_str().unwrap()).unwrap();
    for version in [None, Some(1)] {
        s.apply_journal_page_fenced("user:1", "target:room:7", &body(version, 11), 50, 0)
            .unwrap();
    }
    assert!(s
        .get_messages("user:1", "target:room:7", 10, None)
        .unwrap()
        .is_empty());
    s.apply_journal_page_fenced("user:1", "target:room:7", &body(Some(2), 20), 50, 0)
        .unwrap();
    assert_eq!(
        s.get_messages("user:1", "target:room:7", 10, None)
            .unwrap()
            .len(),
        1
    );
    let removal = CacheOps {
        remove_messages: vec![("target:room:7".into(), 1)],
        ..Default::default()
    };
    s.apply_journal_page_fenced("user:1", "target:room:7", &removal, 51, 0)
        .unwrap();
    s.apply_journal_page_fenced("user:1", "target:room:7", &body(Some(3), 30), 51, 0)
        .unwrap();
    assert!(
        s.get_messages("user:1", "target:room:7", 10, None)
            .unwrap()
            .is_empty(),
        "viewer deletion remains terminal"
    );
}

/// Late lower-revision invalidation cannot evict a newer body or another user.
#[test]
fn invalidation_is_revision_ordered_and_principal_scoped() {
    let s = Storage::open_in_memory().unwrap();
    s.apply_journal_page_fenced("user:1", "target:room:7", &body(Some(3), 30), 0, 0)
        .unwrap();
    s.apply_journal_page_fenced("user:2", "target:room:7", &body(Some(1), 10), 0, 0)
        .unwrap();
    for revision in [2, 1] {
        let op = CacheOps {
            body_invalidations: vec![("target:room:7".into(), 1, revision)],
            ..Default::default()
        };
        s.apply_journal_page_fenced("user:1", "target:room:7", &op, 5, 0)
            .unwrap();
    }
    assert_eq!(
        s.get_messages("user:1", "target:room:7", 10, None)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        s.list_message_files("user:1", "target:room:7", 1).unwrap()[0].file_id,
        30
    );
    assert_eq!(
        s.get_messages("user:2", "target:room:7", 10, None)
            .unwrap()
            .len(),
        1
    );
    let invalid = CacheOps {
        body_invalidations: vec![("target:room:7".into(), 1, 0)],
        ..Default::default()
    };
    assert!(s
        .apply_journal_page_fenced("user:1", "target:room:7", &invalid, 6, 0)
        .is_err());
    assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(5));
}
