//! Storage-owned regressions use opaque payloads, independently of candidate protobuf generation.
use tinkle_storage::{
    transfer_intent::TransferIntentError, CacheOps, ChatRow, EntityFileRef, FileRow,
    FileTransferJob, MessageFileRef, MessageRow, Storage, StorageError, UserRow,
};

/// Construct an opaque body whose first byte represents its selected semantic revision.
fn body(revision: i64) -> CacheOps {
    CacheOps {
        messages: vec![MessageRow {
            conversation_key: "target:room:7".into(),
            message_id: 1,
            peer_kind: "room".into(),
            peer_id: 7,
            from_id: 1,
            body: "selected semantics".into(),
            proto: vec![revision as u8, 1],
            origin_stream: "target:room:7".into(),
            accept_seq: Some(1),
            edit_date_secs: None,
            edit_date_nanos: None,
        }],
        message_body_revisions: vec![(1, revision)],
        ..Default::default()
    }
}

/// Model a pure adapter retaining stored semantics but adopting a new thumbnail representation.
fn merge_representation(
    old: &[u8],
    incoming: &MessageRow,
) -> Result<Option<CacheOps>, StorageError> {
    let mut projected = body(i64::from(old[0]));
    projected.messages[0].proto[1] = incoming.proto[1];
    projected.message_files.push(MessageFileRef {
        conversation_key: incoming.conversation_key.clone(),
        message_id: incoming.message_id,
        file_scope: String::new(),
        file_id: i64::from(incoming.proto[1]),
        role: "thumbnail".into(),
    });
    Ok(Some(projected))
}

/// A selected hybrid keeps its own clock; older incoming semantics must not hide fresh thumbnails.
#[test]
fn older_incoming_body_can_merge_representation_above_retained_floor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let storage = Storage::open(path.to_str().unwrap()).unwrap();
    storage
        .apply_journal_page_fenced("user:1", "target:room:7", &body(5), 0, 0)
        .unwrap();
    storage
        .apply_journal_page_fenced(
            "user:1",
            "target:room:7",
            &CacheOps {
                body_invalidations: vec![("target:room:7".into(), 1, 4)],
                ..Default::default()
            },
            10,
            0,
        )
        .unwrap();
    let mut incoming = body(3);
    incoming.messages[0].proto[1] = 9;
    incoming.message_merger = Some(merge_representation);
    storage
        .apply_journal_page_fenced("user:1", "target:room:7", &incoming, 10, 0)
        .unwrap();
    drop(storage);
    let storage = Storage::open(path.to_str().unwrap()).unwrap();
    assert_eq!(
        storage
            .get_messages("user:1", "target:room:7", 10, None)
            .unwrap()[0]
            .proto,
        vec![5, 9]
    );
    assert_eq!(
        storage
            .list_message_files("user:1", "target:room:7", 1)
            .unwrap()[0]
            .file_id,
        9
    );
    // A version between incoming and selected must not overwrite the hybrid after restart.
    storage
        .apply_journal_page_fenced("user:1", "target:room:7", &body(4), 10, 0)
        .unwrap();
    assert_eq!(
        storage
            .get_messages("user:1", "target:room:7", 10, None)
            .unwrap()[0]
            .proto,
        vec![5, 9]
    );
    assert_eq!(
        storage.load_journal("user:1", "target:room:7").unwrap(),
        Some(10)
    );
}

/// An application adapter cannot bypass the selected clock's floor, even with fresh input metadata.
#[test]
fn selected_body_revision_floor_is_still_enforced() {
    for selected in [None, Some(3)] {
        let storage = Storage::open_in_memory().unwrap();
        storage
            .apply_journal_page_fenced("user:1", "target:room:7", &body(5), 0, 0)
            .unwrap();
        let mut incoming = body(6);
        incoming
            .body_invalidations
            .push(("target:room:7".into(), 1, 4));
        incoming.message_merger = Some(if selected.is_some() {
            select_old_revision
        } else {
            select_unknown_revision
        });
        storage
            .apply_journal_page_fenced("user:1", "target:room:7", &incoming, 1, 0)
            .unwrap();
        assert_eq!(
            storage
                .get_messages("user:1", "target:room:7", 10, None)
                .unwrap()[0]
                .proto,
            vec![5, 1]
        );
        assert!(storage
            .list_message_files("user:1", "target:room:7", 1)
            .unwrap()
            .is_empty());
    }
}

/// Supply a below-floor selection independently of the incoming row's version.
fn select_old_revision(_: &[u8], _: &MessageRow) -> Result<Option<CacheOps>, StorageError> {
    Ok(Some(body(3)))
}

/// Supply an unversioned selection independently of the incoming row's version.
fn select_unknown_revision(_: &[u8], _: &MessageRow) -> Result<Option<CacheOps>, StorageError> {
    let mut projected = body(5);
    projected.message_body_revisions.clear();
    Ok(Some(projected))
}

/// Without a valid stored body, merging must never authorize an old or unknown incoming body.
#[test]
fn missing_stored_body_keeps_incoming_floor_and_tombstone_rejection() {
    let storage = Storage::open_in_memory().unwrap();
    storage
        .apply_journal_page_fenced(
            "user:1",
            "target:room:7",
            &CacheOps {
                body_invalidations: vec![("target:room:7".into(), 1, 4)],
                ..Default::default()
            },
            1,
            0,
        )
        .unwrap();
    let mut incoming = body(3);
    incoming.message_merger = Some(merge_representation);
    for unknown in [false, true] {
        if unknown {
            incoming.message_body_revisions.clear();
        }
        storage
            .apply_journal_page_fenced("user:1", "target:room:7", &incoming, 1, 0)
            .unwrap();
        assert!(storage
            .get_messages("user:1", "target:room:7", 10, None)
            .unwrap()
            .is_empty());
    }
    incoming = body(5);
    incoming.message_merger = Some(merge_representation);
    storage
        .apply_journal_page_fenced("user:1", "target:room:7", &incoming, 1, 0)
        .unwrap();
    storage
        .apply_journal_page_fenced(
            "user:1",
            "target:room:7",
            &CacheOps {
                remove_messages: vec![("target:room:7".into(), 1)],
                ..Default::default()
            },
            2,
            0,
        )
        .unwrap();
    storage
        .apply_journal_page_fenced("user:1", "target:room:7", &incoming, 2, 0)
        .unwrap();
    assert!(storage
        .get_messages("user:1", "target:room:7", 10, None)
        .unwrap()
        .is_empty());
}

/// Accept only a larger opaque representation clock for this storage-level fixture.
fn advance_entity(_: &str, old: &[u8], new: &[u8]) -> Result<bool, StorageError> {
    Ok(new[0] > old[0])
}

/// Both entity kinds carry main and thumbnail refs, or an explicit empty replacement set.
fn entities(version: u8, with_files: bool) -> CacheOps {
    let mut ops = CacheOps {
        users: vec![UserRow {
            user_id: 9,
            proto: vec![version],
        }],
        chats: vec![ChatRow {
            chat_id: 7,
            proto: vec![version],
        }],
        replace_entity_files: vec![("user".into(), 9), ("chat".into(), 7)],
        entity_representation_advance: Some(advance_entity),
        ..Default::default()
    };
    if with_files {
        for (index, (kind, id)) in [("user", 9), ("chat", 7)].into_iter().enumerate() {
            for (offset, role) in [(0, "avatar"), (1, "thumbnail")] {
                let file_id = i64::from(version) * 100 + index as i64 * 10 + offset;
                ops.files.push(FileRow {
                    scope: String::new(),
                    file_id,
                    size_bytes: 1,
                    sha256: vec![],
                    mime_type: "image/png".into(),
                    file_name: String::new(),
                    encrypted: false,
                    created_at: 0,
                    origin_stream: String::new(),
                });
                ops.entity_files.push(EntityFileRef {
                    kind: kind.into(),
                    entity_id: id,
                    file_scope: String::new(),
                    file_id,
                    role: role.into(),
                });
            }
        }
    }
    ops
}

/// Global snapshots replace and remove thumbnail membership with the selected user/chat payloads.
#[test]
fn entity_snapshot_representation_replaces_and_removes_thumbnail_refs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let storage = Storage::open(path.to_str().unwrap()).unwrap();
    for principal in ["user:1", "user:2"] {
        storage
            .hydrate_entity_snapshot(principal, &entities(1, true))
            .unwrap();
    }
    storage
        .hydrate_entity_snapshot("user:1", &entities(2, true))
        .unwrap();
    for file in &entities(1, true).files {
        assert_eq!(
            storage.file_refcount("user:1", "", file.file_id).unwrap(),
            0
        );
        assert_eq!(
            storage.file_refcount("user:2", "", file.file_id).unwrap(),
            1
        );
    }
    for file in &entities(2, true).files {
        assert_eq!(
            storage.file_refcount("user:1", "", file.file_id).unwrap(),
            1
        );
    }
    storage
        .hydrate_entity_snapshot("user:1", &entities(3, false))
        .unwrap();
    drop(storage);
    let storage = Storage::open(path.to_str().unwrap()).unwrap();
    assert_eq!(
        storage.get_user("user:1", 9).unwrap().unwrap().proto,
        vec![3]
    );
    assert_eq!(
        storage.get_chat("user:1", 7).unwrap().unwrap().proto,
        vec![3]
    );
    for file in &entities(2, true).files {
        assert_eq!(
            storage.file_refcount("user:1", "", file.file_id).unwrap(),
            0
        );
    }
    // Rejected stale projections cannot clear the current representation's refs.
    storage
        .hydrate_entity_snapshot("user:2", &entities(1, false))
        .unwrap();
    for file in &entities(1, true).files {
        assert_eq!(
            storage.file_refcount("user:2", "", file.file_id).unwrap(),
            1
        );
    }
    assert_eq!(
        storage.load_journal("user:1", "target:room:7").unwrap(),
        None
    );
}

/// A late reference validation failure rolls back entity bodies, deletions and newly added files.
#[test]
fn entity_snapshot_reference_replacement_rolls_back_with_invalid_thumbnail() {
    let storage = Storage::open_in_memory().unwrap();
    storage
        .hydrate_entity_snapshot("user:1", &entities(1, true))
        .unwrap();
    let mut invalid = entities(2, true);
    invalid.entity_files.last_mut().unwrap().file_id = 0;
    assert!(storage.hydrate_entity_snapshot("user:1", &invalid).is_err());
    assert_eq!(
        storage.get_user("user:1", 9).unwrap().unwrap().proto,
        vec![1]
    );
    assert_eq!(
        storage.get_chat("user:1", 7).unwrap().unwrap().proto,
        vec![1]
    );
    for file in &entities(1, true).files {
        assert_eq!(
            storage.file_refcount("user:1", "", file.file_id).unwrap(),
            1
        );
    }
    for file in &invalid.files {
        assert!(storage
            .get_file("user:1", "", file.file_id)
            .unwrap()
            .is_none());
    }
}

/// Snapshot replacement keys share journal validation and admission limits; errors are atomic.
#[test]
fn entity_snapshot_replacement_keys_are_validated_and_bounded() {
    let storage = Storage::open_in_memory().unwrap();
    storage
        .hydrate_entity_snapshot("user:1", &entities(1, true))
        .unwrap();
    for replacements in [
        vec![("user".into(), 0)],
        vec![("invalid".into(), 9)],
        vec![("user".into(), 9); 513],
    ] {
        let mut invalid = entities(2, false);
        invalid.replace_entity_files = replacements;
        assert!(storage.hydrate_entity_snapshot("user:1", &invalid).is_err());
        assert_eq!(
            storage.get_user("user:1", 9).unwrap().unwrap().proto,
            vec![1]
        );
        assert_eq!(
            storage.get_chat("user:1", 7).unwrap().unwrap().proto,
            vec![1]
        );
        for file in &entities(1, true).files {
            assert_eq!(
                storage.file_refcount("user:1", "", file.file_id).unwrap(),
                1
            );
        }
    }
}

/// The journal path keeps the same exact replacement/removal semantics as global snapshots.
#[test]
fn journal_entity_representation_replaces_and_removes_thumbnail_refs() {
    let storage = Storage::open_in_memory().unwrap();
    for (revision, with_files) in [(1, true), (2, true), (3, false)] {
        let mut ops = entities(revision, with_files);
        for file in &mut ops.files {
            file.origin_stream = "target:room:7".into();
        }
        storage
            .apply_journal_page_fenced("user:1", "target:room:7", &ops, i64::from(revision), 0)
            .unwrap();
        for version in 1..=2 {
            for file in &entities(version, true).files {
                assert_eq!(
                    storage.file_refcount("user:1", "", file.file_id).unwrap(),
                    i64::from(version == revision)
                );
            }
        }
    }
    assert_eq!(
        storage.load_journal("user:1", "target:room:7").unwrap(),
        Some(3)
    );
}

/// Construct a registered upload before any server session has been disclosed.
fn upload() -> FileTransferJob {
    FileTransferJob {
        job_id: "upload:review".into(),
        kind: "upload".into(),
        file_scope: String::new(),
        file_id: 0,
        upload_id: String::new(),
        state: "begin".into(),
        session_json: "proof".into(),
        temp_path: String::new(),
        resume_offset: 0,
        updated_at: 1,
    }
}

/// Neither checkpoint entry point may expose an unbound session or conflict with a prior binding.
#[test]
fn upload_checkpoints_require_the_durable_bound_session() {
    for direct_cas in [false, true] {
        let storage = Storage::open_in_memory().unwrap();
        let initial = upload();
        let original = storage
            .create_or_compare_file_transfer_intent("user:1", &initial, "proof")
            .unwrap();
        let mut active = initial.clone();
        active.state = "active".into();
        active.upload_id = "remote".into();
        let attempt = |job: &FileTransferJob| {
            if direct_cas {
                storage
                    .compare_and_swap_file_transfer("user:1", &original, job)
                    .map(|_| ())
            } else {
                storage
                    .advance_upload_intent("user:1", &original, job)
                    .map(|_| ())
            }
        };
        assert_eq!(attempt(&active), Err(TransferIntentError::Conflict));
        storage
            .bind_upload_part_size("user:1", &original, "remote", 65536)
            .unwrap();
        let mut conflict = active.clone();
        conflict.upload_id = "different".into();
        assert_eq!(attempt(&conflict), Err(TransferIntentError::Conflict));
        let retained = storage
            .file_transfer_intent("user:1", &initial.job_id)
            .unwrap()
            .unwrap();
        assert_eq!(retained.revision, original.revision);
        assert!(retained.job.upload_id.is_empty());
        assert_eq!(
            storage.bind_upload_part_size("user:1", &original, "remote", 131072),
            Err(TransferIntentError::Conflict)
        );
        attempt(&active).unwrap();
    }
}

/// A crash after binding but before checkpoint publication cannot let another connection rebind.
#[test]
fn upload_binding_precedes_checkpoint_publication_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let storage = Storage::open(path.to_str().unwrap()).unwrap();
    let initial = upload();
    let original = storage
        .create_or_compare_file_transfer_intent("user:1", &initial, "proof")
        .unwrap();
    storage
        .bind_upload_part_size("user:1", &original, "remote", 65536)
        .unwrap();
    drop(storage);
    let storage = Storage::open(path.to_str().unwrap()).unwrap();
    let other = Storage::open(path.to_str().unwrap()).unwrap();
    let retained = other
        .file_transfer_intent("user:1", &initial.job_id)
        .unwrap()
        .unwrap();
    assert_eq!(retained.revision, original.revision);
    assert!(retained.job.upload_id.is_empty());
    for (session, size) in [("remote", 131072), ("different", 65536)] {
        assert_eq!(
            other.bind_upload_part_size("user:1", &retained, session, size),
            Err(TransferIntentError::Conflict)
        );
    }
    let mut active = initial;
    active.upload_id = "different".into();
    active.state = "active".into();
    assert!(matches!(
        other.advance_upload_intent("user:1", &retained, &active),
        Err(TransferIntentError::Conflict)
    ));
    active.upload_id = "remote".into();
    storage
        .bind_upload_part_size("user:1", &original, "remote", 65536)
        .unwrap();
    let advanced = storage
        .advance_upload_intent("user:1", &original, &active)
        .unwrap();
    assert_eq!(advanced.job.upload_id, "remote");
    assert_eq!(advanced.revision, original.revision + 1);
    // A stale recovery writer may join the same negotiation, but cannot switch boundaries.
    other
        .bind_upload_part_size("user:1", &retained, "remote", 65536)
        .unwrap();
    assert_eq!(
        other.bind_upload_part_size("user:1", &retained, "remote", 131072),
        Err(TransferIntentError::Conflict)
    );
}
