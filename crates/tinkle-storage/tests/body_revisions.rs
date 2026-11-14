//! Authoritative body revision ordering and FTS snapshot invalidation, without journal forgery.
use tinkle_storage::{CacheOps, MessageRow, MessageSearchRequest, Storage, StorageError};

/// Construct one row with an independently supplied body version, not a mutation position.
fn ops(id: i64, revision: Option<i64>, body: &str, proto: u8) -> CacheOps {
    CacheOps {
        messages: vec![MessageRow {
            conversation_key: "target:room:7".into(),
            message_id: id,
            peer_kind: "room".into(),
            peer_id: 7,
            from_id: 1,
            body: body.into(),
            proto: vec![proto],
            origin_stream: "target:room:7".into(),
            accept_seq: Some(id),
            edit_date_secs: None,
            edit_date_nanos: None,
        }],
        message_body_revisions: revision.map(|r| (id, r)).into_iter().collect(),
        ..Default::default()
    }
}

/// Metadata revisions replace tied acceptance/edit dates, invalidate search snapshots, and never
/// let an older/equal revision overwrite a newer cached body even with a later edit date.
#[test]
fn metadata_body_versions_win_without_synthetic_edit_dates() {
    let s = Storage::open_in_memory().unwrap();
    s.apply_journal_page_fenced(
        "user:1",
        "target:room:7",
        &ops(1, Some(1), "needle", 1),
        0,
        0,
    )
    .unwrap();
    s.apply_journal_page_fenced(
        "user:1",
        "target:room:7",
        &ops(2, Some(1), "needle", 1),
        0,
        0,
    )
    .unwrap();
    let snapshot = s
        .search_messages(
            "user:1",
            &MessageSearchRequest {
                query: "needle".into(),
                limit: 1,
                ..Default::default()
            },
        )
        .unwrap();
    s.apply_journal_page_fenced(
        "user:1",
        "target:room:7",
        &ops(1, Some(3), "needle", 3),
        0,
        0,
    )
    .unwrap();
    assert!(matches!(
        s.check_search_snapshot(&snapshot.snapshot),
        Err(StorageError::SearchChanged)
    ));
    let mut stale = ops(1, Some(2), "stale text", 2);
    stale.messages[0].edit_date_secs = Some(999);
    s.apply_journal_page_fenced("user:1", "target:room:7", &stale, 0, 0)
        .unwrap();
    s.apply_journal_page_fenced(
        "user:1",
        "target:room:7",
        &ops(1, Some(3), "equal revision", 4),
        0,
        0,
    )
    .unwrap();
    let rows = s.get_messages("user:1", "target:room:7", 10, None).unwrap();
    assert_eq!(rows[0].proto, vec![3]);
    assert_eq!(rows[0].edit_date_secs, None);
    assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(0));
    assert!(s
        .apply_journal_page_fenced("user:1", "target:room:7", &ops(1, Some(0), "bad", 0), 0, 0)
        .is_err());
}

/// Reopening a schema-v6 cache adds nullable revisions without inventing versions or losing bodies.
#[test]
fn body_revision_migration_preserves_unknown_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let path = path.to_str().unwrap();
    {
        let s = Storage::open(path).unwrap();
        s.apply_journal_page_fenced("user:1", "target:room:7", &ops(1, None, "old", 1), 0, 0)
            .unwrap();
    }
    {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch("ALTER TABLE messages DROP COLUMN body_revision;UPDATE meta SET value='6' WHERE key='schema_version';").unwrap();
    }
    let s = Storage::open(path).unwrap();
    assert_eq!(
        s.get_messages("user:1", "target:room:7", 10, None).unwrap()[0].body,
        "old"
    );
    let mut dated = ops(1, Some(2), "new", 2);
    dated.messages[0].edit_date_secs = Some(1);
    s.apply_journal_page_fenced("user:1", "target:room:7", &dated, 0, 0)
        .unwrap();
    assert_eq!(
        s.get_messages("user:1", "target:room:7", 10, None).unwrap()[0].body,
        "new"
    );
}

/// Acceptance positions cannot make an older body win, or prevent a later edit from winning.
#[test]
fn absent_revision_uses_only_edit_date() {
    let s = Storage::open_in_memory().unwrap();
    let mut first = ops(1, None, "new", 1);
    first.messages[0].edit_date_secs = Some(20);
    first.messages[0].accept_seq = Some(2);
    s.apply_journal_page_fenced("user:1", "target:room:7", &first, 0, 0)
        .unwrap();
    let mut stale = ops(1, None, "stale", 2);
    stale.messages[0].edit_date_secs = Some(10);
    stale.messages[0].accept_seq = Some(100);
    s.apply_journal_page_fenced("user:1", "target:room:7", &stale, 0, 0)
        .unwrap();
    assert_eq!(
        s.get_messages("user:1", "target:room:7", 10, None).unwrap()[0].body,
        "new"
    );
    let mut fresh = ops(1, None, "fresher", 3);
    fresh.messages[0].edit_date_secs = Some(30);
    fresh.messages[0].accept_seq = Some(1);
    s.apply_journal_page_fenced("user:1", "target:room:7", &fresh, 0, 0)
        .unwrap();
    assert_eq!(
        s.get_messages("user:1", "target:room:7", 10, None).unwrap()[0].body,
        "fresher"
    );
}

/// File references are replaced only by the transaction's winning body, including removal.
#[test]
fn winning_body_replaces_refs_and_stale_body_cannot_add_them() {
    use tinkle_storage::MessageFileRef;
    let s = Storage::open_in_memory().unwrap();
    let with_file = |revision, file_id| {
        let mut value = ops(1, Some(revision), "body", revision as u8);
        value.message_files.push(MessageFileRef {
            conversation_key: "target:room:7".into(),
            message_id: 1,
            file_scope: String::new(),
            file_id,
            role: "main".into(),
        });
        value
    };
    for (revision, file) in [(1, 10), (3, 30), (2, 20)] {
        s.apply_journal_page_fenced("user:1", "target:room:7", &with_file(revision, file), 0, 0)
            .unwrap();
    }
    let refs = s.list_message_files("user:1", "target:room:7", 1).unwrap();
    assert_eq!(refs.iter().map(|r| r.file_id).collect::<Vec<_>>(), vec![30]);
    s.apply_journal_page_fenced(
        "user:1",
        "target:room:7",
        &ops(1, Some(4), "no files", 4),
        0,
        0,
    )
    .unwrap();
    assert!(s
        .list_message_files("user:1", "target:room:7", 1)
        .unwrap()
        .is_empty());
}

/// A later accepted message can have a smaller numeric ID; the dialog head follows acceptance.
#[test]
fn dialog_head_uses_acceptance_order() {
    use tinkle_storage::DialogRow;
    let s = Storage::open_in_memory().unwrap();
    for (id, position) in [(900, 1), (20, 2), (999, 1)] {
        let mut value = ops(id, Some(1), "body", 1);
        value.messages[0].accept_seq = Some(position);
        value.dialogs.push(DialogRow {
            peer_kind: "room".into(),
            peer_id: 7,
            top_message: id,
            unread_count: 0,
            read_inbox_max_id: 0,
            read_outbox_max_id: 0,
            pinned: false,
            proto: vec![1],
            origin_stream: "target:room:7".into(),
            read_state_pending: false,
        });
        s.apply_journal_page_fenced("user:1", "target:room:7", &value, 0, 0)
            .unwrap();
    }
    assert_eq!(s.list_dialogs("user:1", 10).unwrap()[0].top_message, 20);
}

/// Removing an uncached-next head invalidates its visible pointer in the same transaction.
/// A smaller-ID accepted successor can advance after reopen without affecting another account.
#[test]
fn removed_dialog_head_does_not_stall_the_next_accepted_message_after_reopen() {
    use tinkle_storage::DialogRow;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let value = |id, accept| {
        let mut value = ops(id, Some(1), "body", 1);
        value.messages[0].accept_seq = Some(accept);
        value.dialogs.push(DialogRow {
            peer_kind: "room".into(),
            peer_id: 7,
            top_message: id,
            unread_count: 5,
            read_inbox_max_id: 0,
            read_outbox_max_id: 0,
            pinned: false,
            proto: vec![1],
            origin_stream: "target:room:7".into(),
            read_state_pending: false,
        });
        value
    };
    {
        let s = Storage::open(path.to_str().unwrap()).unwrap();
        for principal in ["user:1", "user:2"] {
            s.apply_journal_page_fenced(principal, "target:room:7", &value(900, 1), 0, 0)
                .unwrap();
        }
        s.apply_journal_page_fenced(
            "user:1",
            "target:room:7",
            &CacheOps {
                remove_messages: vec![("target:room:7".into(), 900)],
                ..Default::default()
            },
            1,
            0,
        )
        .unwrap();
        assert!(
            s.list_dialogs("user:1", 10).unwrap().is_empty(),
            "removed head remains visible"
        );
    }
    let s = Storage::open(path.to_str().unwrap()).unwrap();
    s.apply_journal_page_fenced("user:1", "target:room:7", &value(20, 2), 2, 0)
        .unwrap();
    let a = s.list_dialogs("user:1", 10).unwrap();
    assert_eq!(a[0].top_message, 20);
    assert_eq!(a[0].unread_count, 5);
    assert!(a[0].read_state_pending);
    assert_eq!(s.list_dialogs("user:2", 10).unwrap()[0].top_message, 900);
    drop(s);
    assert_eq!(
        Storage::open(path.to_str().unwrap())
            .unwrap()
            .list_dialogs("user:1", 10)
            .unwrap()[0]
            .top_message,
        20
    );
}

/// Neither a later journal position nor a delayed history read versions user/chat snapshots.
/// Keep their current bodies and avatar refs together until an explicit authoritative refresh.
#[test]
fn unversioned_hydration_cannot_replace_entities_or_pin_stale_avatars() {
    use tinkle_storage::{ChatRow, EntityFileRef, FileRow, UserRow};
    let s = Storage::open_in_memory().unwrap();
    let entities = |version: u8| CacheOps {
        users: vec![UserRow {
            user_id: 9,
            proto: vec![version],
        }],
        chats: vec![ChatRow {
            chat_id: 7,
            proto: vec![version],
        }],
        files: vec![FileRow {
            scope: String::new(),
            file_id: i64::from(version),
            size_bytes: 1,
            sha256: vec![],
            mime_type: "image/png".into(),
            file_name: String::new(),
            encrypted: false,
            created_at: 0,
            origin_stream: "target:room:7".into(),
        }],
        entity_files: vec![EntityFileRef {
            kind: "user".into(),
            entity_id: 9,
            file_scope: String::new(),
            file_id: i64::from(version),
            role: "avatar".into(),
        }],
        replace_entity_files: vec![("user".into(), 9)],
        ..Default::default()
    };
    s.apply_journal_page_fenced("user:1", "target:room:7", &entities(1), 1, 0)
        .unwrap();
    s.apply_journal_page_fenced("user:1", "target:room:7", &entities(2), 2, 0)
        .unwrap();
    let fence = s.capture_message_scope("user:1", "target:room:7").unwrap();
    s.hydrate_messages_fenced("user:1", "target:room:7", &entities(3), &fence)
        .unwrap();
    let mut snapshot = entities(4);
    snapshot.files[0].origin_stream.clear();
    s.hydrate_entity_snapshot("user:1", &snapshot).unwrap();
    assert_eq!(s.get_user("user:1", 9).unwrap().unwrap().proto, vec![1]);
    assert_eq!(s.get_chat("user:1", 7).unwrap().unwrap().proto, vec![1]);
    assert_eq!(s.file_refcount("user:1", "", 1).unwrap(), 1);
    for file in [2, 3, 4] {
        assert_eq!(s.file_refcount("user:1", "", file).unwrap(), 0);
        assert!(s.get_file("user:1", "", file).unwrap().is_none());
    }
    assert_eq!(s.load_journal("user:1", "target:room:7").unwrap(), Some(2));
    // A separate account can seed its own version and refs, with no checkpoint fabrication.
    s.hydrate_entity_snapshot("user:2", &snapshot).unwrap();
    assert_eq!(s.get_user("user:2", 9).unwrap().unwrap().proto, vec![4]);
    assert_eq!(s.file_refcount("user:2", "", 4).unwrap(), 1);
    assert_eq!(s.load_journal("user:2", "target:room:7").unwrap(), None);
    let mut invalid = snapshot;
    invalid.users[0].user_id = 10;
    invalid.files[0].size_bytes = -1;
    assert!(s.hydrate_entity_snapshot("user:2", &invalid).is_err());
    assert!(s.get_user("user:2", 10).unwrap().is_none());
}
