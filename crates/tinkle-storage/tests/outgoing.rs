//! Outgoing intent persistence, CAS and isolation regressions independent of schema candidates.
use tinkle_storage::{OutgoingJob, OutgoingState as S, Storage};

/// A compact immutable intent whose bytes include an application operation key.
fn job() -> OutgoingJob {
    OutgoingJob {
        job_id: "send-1".into(),
        source: "a".repeat(64),
        target_key: "target:room:7".into(),
        method: "/tinkle.v1.MessagesService/SendMessage".into(),
        payload: vec![1, 2, 3],
        state: S::Prepared,
        response: vec![],
        last_outcome: None,
        revision: 0,
        cache_generation: 0,
        cache_incarnation: 0,
    }
}

/// Repeated creation is exact-byte idempotent, never an upsert that can mutate a send.
#[test]
fn immutable_request_and_revision_cas() {
    let s = Storage::open_in_memory().unwrap();
    let mut j = job();
    s.insert_outgoing("user:1", &j).unwrap();
    s.insert_outgoing("user:1", &j).unwrap();
    let mut conflict = j.clone();
    conflict.payload.push(4);
    assert!(s.insert_outgoing("user:1", &conflict).is_err());
    assert!(s
        .transition_outgoing("user:1", &j, S::Dispatching, &[])
        .unwrap());
    assert!(!s
        .transition_outgoing("user:1", &j, S::Dispatching, &[])
        .unwrap());
    assert!(!s.discard_outgoing("user:1", &j.source, &j.job_id).unwrap());
    j = s.outgoing("user:1", &j.source, &j.job_id).unwrap().unwrap();
    assert!(s
        .transition_outgoing("user:1", &j, S::Accepted, &[9])
        .unwrap());
    let accepted = s.outgoing("user:1", &j.source, &j.job_id).unwrap().unwrap();
    assert_eq!(accepted.payload, vec![1, 2, 3]);
    assert_eq!(accepted.response, vec![9]);
    assert!(s
        .transition_outgoing("user:1", &accepted, S::Dispatching, &[])
        .is_err());
}

/// Restart retains uncertain work/results; another principal/source cannot see or wipe it.
#[test]
fn restart_isolation_and_logout_wipe() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let path = path.to_str().unwrap();
    let j = job();
    {
        let s = Storage::open(path).unwrap();
        s.insert_outgoing("user:1", &j).unwrap();
        assert!(s
            .transition_outgoing("user:1", &j, S::Dispatching, &[])
            .unwrap());
    }
    let s = Storage::open(path).unwrap();
    assert_eq!(
        s.outgoing("user:1", &j.source, &j.job_id)
            .unwrap()
            .unwrap()
            .state,
        S::Dispatching
    );
    assert!(s
        .outgoing("user:2", &j.source, &j.job_id)
        .unwrap()
        .is_none());
    assert!(s
        .outgoing("user:1", &"b".repeat(64), &j.job_id)
        .unwrap()
        .is_none());
    s.clear_principal("user:2").unwrap();
    assert_eq!(s.list_outgoing("user:1", &j.source).unwrap().len(), 1);
    s.clear_principal("user:1").unwrap();
    assert!(s.list_outgoing("user:1", &j.source).unwrap().is_empty());
    // A late CAS cannot resurrect a row removed by logout.
    assert!(!s
        .transition_outgoing("user:1", &j, S::Dispatching, &[])
        .unwrap());
}

/// The finite queue counts settled outcomes until explicit discard, not just pending work.
#[test]
fn finite_outgoing_admission() {
    let s = Storage::open_in_memory().unwrap();
    let mut j = job();
    for i in 0..256 {
        j.job_id = format!("job-{i}");
        s.insert_outgoing("user:1", &j).unwrap();
    }
    j.job_id = "overflow".into();
    assert!(s.insert_outgoing("user:1", &j).is_err());
    assert!(s.discard_outgoing("user:1", &j.source, "job-0").unwrap());
    s.insert_outgoing("user:1", &j).unwrap();
    j.payload = vec![0; 262145];
    j.job_id = "oversized".into();
    assert!(s.insert_outgoing("user:2", &j).is_err());
}

/// Accepted-body cache hydration is atomic with an unchanged checkpoint and respects both
/// already-covered removals and rebuild generations, including replay of a retained outcome.
#[test]
fn accepted_hydration_cannot_resurrect_removed_or_rebuilt_messages() {
    use tinkle_storage::{CacheOps, MessageRow};
    let s = Storage::open_in_memory().unwrap();
    let j = job();
    s.insert_outgoing("user:1", &j).unwrap();
    s.transition_outgoing("user:1", &j, S::Dispatching, &[])
        .unwrap();
    let j = s.outgoing("user:1", &j.source, &j.job_id).unwrap().unwrap();
    s.transition_outgoing("user:1", &j, S::Accepted, &[9])
        .unwrap();
    let j = s.outgoing("user:1", &j.source, &j.job_id).unwrap().unwrap();
    let ops = CacheOps {
        messages: vec![MessageRow {
            conversation_key: j.target_key.clone(),
            message_id: 1,
            peer_kind: "room".into(),
            peer_id: 7,
            from_id: 1,
            body: "accepted".into(),
            proto: vec![1],
            origin_stream: j.target_key.clone(),
            accept_seq: Some(1),
            edit_date_secs: None,
            edit_date_nanos: None,
        }],
        ..Default::default()
    };
    s.hydrate_outgoing("user:1", &j, &ops).unwrap();
    assert_eq!(s.load_journal("user:1", &j.target_key).unwrap(), Some(0));
    assert_eq!(
        s.get_messages("user:1", &j.target_key, 10, None)
            .unwrap()
            .len(),
        1
    );
    let removal = CacheOps {
        remove_messages: vec![(j.target_key.clone(), 1)],
        ..Default::default()
    };
    s.apply_journal_page_fenced("user:1", &j.target_key, &removal, 2, 0)
        .unwrap();
    s.hydrate_outgoing("user:1", &j, &ops).unwrap();
    assert!(s
        .get_messages("user:1", &j.target_key, 10, None)
        .unwrap()
        .is_empty());
    assert_eq!(s.load_journal("user:1", &j.target_key).unwrap(), Some(2));
    s.mark_rebuild("user:1", &j.target_key).unwrap();
    assert!(s.hydrate_outgoing("user:1", &j, &ops).is_err());
    s.clear_principal("user:1").unwrap();
    assert!(s.hydrate_outgoing("user:1", &j, &ops).is_err());
    assert!(s
        .get_messages("user:1", &j.target_key, 10, None)
        .unwrap()
        .is_empty());
}

/// Message acceptance tokens and mutation positions are independent domains. Retained albums
/// must suppress only removed IDs, not an unaffected item whose token happens to be smaller.
#[test]
fn replayed_accepted_jobs_never_order_message_acceptance_against_mutation_positions() {
    use tinkle_storage::{CacheOps, MessageRow, MessageSearchRequest};
    for (acceptance, removal_position) in [(100, 1), (1, 100)] {
        for album in [false, true] {
            let s = Storage::open_in_memory().unwrap();
            let mut j = job();
            if album {
                j.method = "/tinkle.v1.MessagesService/SendAlbum".into();
            }
            s.insert_outgoing("user:1", &j).unwrap();
            s.transition_outgoing("user:1", &j, S::Dispatching, &[])
                .unwrap();
            let j = s.outgoing("user:1", &j.source, &j.job_id).unwrap().unwrap();
            s.transition_outgoing("user:1", &j, S::Accepted, &[9])
                .unwrap();
            let j = s.outgoing("user:1", &j.source, &j.job_id).unwrap().unwrap();
            let row = |id, body: &str, token| MessageRow {
                conversation_key: j.target_key.clone(),
                message_id: id,
                peer_kind: "room".into(),
                peer_id: 7,
                from_id: 1,
                body: body.into(),
                proto: vec![1],
                origin_stream: j.target_key.clone(),
                accept_seq: Some(token),
                edit_date_secs: None,
                edit_date_nanos: None,
            };
            let removed = CacheOps {
                messages: vec![row(1, "retiredneedle", acceptance)],
                ..Default::default()
            };
            s.hydrate_outgoing("user:1", &j, &removed).unwrap();
            s.apply_journal_page_fenced(
                "user:1",
                &j.target_key,
                &CacheOps {
                    remove_messages: vec![(j.target_key.clone(), 1)],
                    ..Default::default()
                },
                removal_position,
                0,
            )
            .unwrap();
            let mut replay = removed;
            if album {
                replay
                    .messages
                    .push(row(2, "survivorneedle", acceptance + 1));
            }
            s.hydrate_outgoing("user:1", &j, &replay).unwrap();
            assert!(
                s.search_messages(
                    "user:1",
                    &MessageSearchRequest {
                        query: "retiredneedle".into(),
                        ..Default::default()
                    }
                )
                .unwrap()
                .hits
                .is_empty(),
                "acceptance={acceptance}, removal={removal_position}, album={album}"
            );
            let rows = s.get_messages("user:1", &j.target_key, 10, None).unwrap();
            assert!(!rows.iter().any(|m| m.message_id == 1));
            assert_eq!(
                rows.iter().any(|m| m.message_id == 2),
                album,
                "unrelated album item must survive"
            );
            assert_eq!(
                s.load_journal("user:1", &j.target_key).unwrap(),
                Some(removal_position)
            );
        }
    }
}

/// An accepted response has no entity version and cannot replace a separately fetched snapshot.
#[test]
fn accepted_response_preserves_existing_user_and_chat_snapshots() {
    use tinkle_storage::{CacheOps, ChatRow, UserRow};
    let s = Storage::open_in_memory().unwrap();
    let initial = CacheOps {
        users: vec![UserRow {
            user_id: 7,
            proto: vec![7],
        }],
        chats: vec![ChatRow {
            chat_id: 7,
            proto: vec![7],
        }],
        ..Default::default()
    };
    s.apply_journal_page_fenced("user:1", "target:room:7", &initial, 0, 0)
        .unwrap();
    let j = job();
    s.insert_outgoing("user:1", &j).unwrap();
    s.transition_outgoing("user:1", &j, S::Dispatching, &[])
        .unwrap();
    let j = s.outgoing("user:1", &j.source, &j.job_id).unwrap().unwrap();
    s.transition_outgoing("user:1", &j, S::Accepted, &[1])
        .unwrap();
    let j = s.outgoing("user:1", &j.source, &j.job_id).unwrap().unwrap();
    let stale = CacheOps {
        users: vec![UserRow {
            user_id: 7,
            proto: vec![1],
        }],
        chats: vec![ChatRow {
            chat_id: 7,
            proto: vec![1],
        }],
        ..Default::default()
    };
    s.hydrate_outgoing("user:1", &j, &stale).unwrap();
    assert_eq!(s.get_user("user:1", 7).unwrap().unwrap().proto, vec![7]);
    assert_eq!(s.get_chat("user:1", 7).unwrap().unwrap().proto, vec![7]);
}

/// Failure status and state are retained together across process restart, fenced by exact CAS.
#[test]
fn structured_outcome_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let path = path.to_str().unwrap();
    let j = job();
    {
        let s = Storage::open(path).unwrap();
        s.insert_outgoing("user:1", &j).unwrap();
        s.transition_outgoing("user:1", &j, S::Dispatching, &[])
            .unwrap();
        let current = s.outgoing("user:1", &j.source, &j.job_id).unwrap().unwrap();
        let outcome = tinkle_storage::OutgoingOutcome {
            code: 2,
            raw_code: Some(1234),
            unknown_outcome: true,
        };
        assert!(s
            .transition_outgoing_with_outcome(
                "user:1",
                &current,
                S::Unconfirmed,
                &[],
                Some(&outcome)
            )
            .unwrap());
        assert!(!s
            .transition_outgoing_with_outcome("user:1", &current, S::Rejected, &[], Some(&outcome))
            .unwrap());
    }
    let s = Storage::open(path).unwrap();
    let current = s.outgoing("user:1", &j.source, &j.job_id).unwrap().unwrap();
    assert_eq!(current.state, S::Unconfirmed);
    assert_eq!(current.last_outcome.unwrap().raw_code, Some(1234));
}
