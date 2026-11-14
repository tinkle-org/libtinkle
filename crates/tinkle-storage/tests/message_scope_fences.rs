//! Durable message scope proofs bind reads before I/O and cannot be refreshed by late results.
use tinkle_storage::{CacheOps, MessageRow, Storage};

/// Empty global snapshots still belong to their original database/principal/incarnation.
#[test]
fn dialog_proof_cannot_cross_databases_principals_or_an_empty_scope_wipe() {
    let s = Storage::open_in_memory().unwrap();
    let other = Storage::open_in_memory().unwrap();
    let proof = s.capture_dialog_scope("user:1").unwrap();
    let entities = CacheOps::default();
    assert!(other
        .hydrate_dialog_snapshot_fenced("user:1", &[], &[], &entities, &proof)
        .is_err());
    assert!(s
        .hydrate_dialog_snapshot_fenced("user:2", &[], &[], &entities, &proof)
        .is_err());
    s.clear_principal("user:2").unwrap();
    s.hydrate_dialog_snapshot_fenced("user:1", &[], &[], &entities, &proof)
        .unwrap();
    s.clear_principal("user:1").unwrap();
    assert!(s
        .hydrate_dialog_snapshot_fenced("user:1", &[], &[], &entities, &proof)
        .is_err());
}

fn body(id: i64) -> CacheOps {
    CacheOps {
        messages: vec![MessageRow {
            conversation_key: "target:room:7".into(),
            message_id: id,
            peer_kind: "room".into(),
            peer_id: 7,
            from_id: 1,
            body: "needle".into(),
            proto: vec![1],
            origin_stream: "target:room:7".into(),
            accept_seq: Some(100),
            edit_date_secs: None,
            edit_date_nanos: None,
        }],
        ..Default::default()
    }
}

/// Empty targets have generation zero on both sides of a wipe; durable incarnation, not that
/// reused generation, rejects the old read even after the replacement scope is installed.
#[test]
fn pre_request_scope_proofs_bind_database_principal_and_durable_wipes() {
    let s = Storage::open_in_memory().unwrap();
    let other = Storage::open_in_memory().unwrap();
    let fence = s.capture_message_scope("user:1", "target:room:7").unwrap();
    assert_eq!(s.cache_principal_incarnation("user:1").unwrap(), 0);
    assert!(other
        .hydrate_messages_fenced("user:1", "target:room:7", &body(1), &fence)
        .is_err());
    assert!(s
        .hydrate_messages_fenced("user:2", "target:room:7", &body(1), &fence)
        .is_err());
    s.clear_principal("user:1").unwrap();
    s.install_snapshot("user:1", "target:room:7", 0, 0, None)
        .unwrap();
    assert_eq!(s.cache_principal_incarnation("user:1").unwrap(), 1);
    assert!(s
        .hydrate_messages_fenced("user:1", "target:room:7", &body(1), &fence)
        .is_err());
    let fresh = s.capture_message_scope("user:1", "target:room:7").unwrap();
    s.hydrate_messages_fenced("user:1", "target:room:7", &body(2), &fresh)
        .unwrap();
    assert_eq!(
        s.get_messages("user:1", "target:room:7", 10, None).unwrap()[0].message_id,
        2
    );
}

/// Removal IDs survive restart and rebuild. Revalidation never erases evidence or accepts a
/// proof issued while invalid; a fresh proof may hydrate an unrelated, genuinely new message.
#[test]
fn removal_evidence_survives_restart_and_scope_revalidation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let path = path.to_str().unwrap();
    {
        let s = Storage::open(path).unwrap();
        s.apply_journal_page_fenced(
            "user:1",
            "target:room:7",
            &CacheOps {
                remove_messages: vec![("target:room:7".into(), 1)],
                ..Default::default()
            },
            1,
            0,
        )
        .unwrap();
    }
    let s = Storage::open(path).unwrap();
    s.mark_rebuild("user:1", "target:room:7").unwrap();
    let invalid = s.capture_message_scope("user:1", "target:room:7").unwrap();
    s.install_snapshot("user:1", "target:room:7", 0, 0, None)
        .unwrap();
    assert!(s
        .hydrate_messages_fenced("user:1", "target:room:7", &body(2), &invalid)
        .is_err());
    let fresh = s.capture_message_scope("user:1", "target:room:7").unwrap();
    assert!(s
        .hydrate_messages_fenced("user:1", "target:room:7", &body(1), &fresh)
        .unwrap()
        .is_empty());
    assert_eq!(
        s.hydrate_messages_fenced("user:1", "target:room:7", &body(2), &fresh)
            .unwrap(),
        vec![2]
    );
}

/// The original generation-only journal API must not recycle generation zero after a wipe.
/// Its opaque generation watermark, like sealed snapshot proofs, must fence delayed writers.
#[test]
fn generation_only_writers_cannot_reuse_a_retired_principal_generation() {
    let s = Storage::open_in_memory().unwrap();
    let old = s
        .stream_state("user:1", "target:room:7")
        .unwrap()
        .generation;
    s.apply_journal_page_fenced("user:1", "target:room:7", &body(1), 0, old)
        .unwrap();
    s.mark_rebuild("user:1", "target:room:7").unwrap();
    let rebuilt = s
        .install_snapshot("user:1", "target:room:7", 0, 0, None)
        .unwrap();
    s.clear_principal("user:1").unwrap();
    assert!(s
        .apply_journal_page_fenced("user:1", "target:room:7", &body(1), 0, old)
        .is_err());
    assert!(s
        .apply_journal_page_fenced("user:1", "target:room:7", &body(1), 0, rebuilt)
        .is_err());
    let fresh = s
        .stream_state("user:1", "target:room:7")
        .unwrap()
        .generation;
    assert!(fresh > rebuilt);
    s.apply_journal_page_fenced("user:1", "target:room:7", &body(2), 0, fresh)
        .unwrap();
    assert_eq!(
        s.get_messages("user:1", "target:room:7", 10, None).unwrap()[0].message_id,
        2
    );
}
