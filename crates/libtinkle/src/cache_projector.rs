//! Project validated `TargetChange` pages + `Message` bodies into SQLite `CacheOps`.
//!
//! Crash invariant: message upserts/removals, dialog touches, entity writes,
//! and the journal checkpoint advance commit in one
//! `Storage::apply_journal_page_fenced` transaction. Required network
//! fetches (`GetMessages` for edited bodies) happen before this projection;
//! this module never does I/O.
//!
//! Body copies compare body revisions when both report them, otherwise only edit dates.
//! Acceptance positions order distinct messages and never rank body freshness.

use std::collections::HashSet;

use prost::Message as _;

use tinkle_schema::proto::{
    target, Chat as TinkleChat, Message as TinkleMessage, TargetChange, TargetChangeKind,
    User as TinkleUser,
};

fn peer_of(msg: &TinkleMessage) -> (String, i64) {
    match msg.target.as_ref().and_then(|t| t.kind.as_ref()) {
        Some(target::Kind::ChatId(id)) => ("room".into(), *id),
        Some(target::Kind::UserId(id)) => ("user".into(), *id),
        None => ("unknown".into(), 0),
    }
}

fn edit_parts(msg: &TinkleMessage) -> (Option<i64>, Option<i32>) {
    match &msg.edit_date {
        Some(ts) => (Some(ts.seconds), Some(ts.nanos)),
        None => (None, None),
    }
}

/// Build cache writes for one validated `TargetChange`.
/// - Edited with a body → message upsert (hydrated body).
/// - Explicit nonterminal body invalidation → revision floor, no body or file refs.
/// - Deleted/Revoked → `remove_messages` (identity + kind only, never a body).
pub fn project_change_to_ops(
    target_key: &str,
    change: &TargetChange,
) -> Result<tinkle_storage::CacheOps, String> {
    let mut ops = tinkle_storage::CacheOps::default();
    let kind = TargetChangeKind::try_from(change.kind)
        .map_err(|_| format!("unknown target change kind {}", change.kind))?;
    if change.body_invalidated {
        let revision = change
            .message_revision
            .filter(|r| *r > 0)
            .ok_or("body invalidation requires a positive revision")?;
        if change.message_id <= 0
            || change.message.is_some()
            || matches!(
                kind,
                TargetChangeKind::Deleted
                    | TargetChangeKind::Revoked
                    | TargetChangeKind::Unspecified
            )
        {
            return Err("invalid nonterminal body invalidation".into());
        }
        ops.body_invalidations
            .push((target_key.to_owned(), change.message_id, revision));
        return Ok(ops);
    }
    match kind {
        TargetChangeKind::Edited
        | TargetChangeKind::Pin
        | TargetChangeKind::Reaction
        | TargetChangeKind::Poll
        | TargetChangeKind::Ack => {
            let msg = change.message.as_ref().ok_or_else(|| {
                format!(
                    "edited change for message {} must carry a body",
                    change.message_id
                )
            })?;
            if msg.message_id != change.message_id {
                return Err(format!(
                    "edited body id {} must match change {}",
                    msg.message_id, change.message_id
                ));
            }
            push_message(&mut ops, target_key, msg)?;
        }
        TargetChangeKind::Deleted | TargetChangeKind::Revoked => {
            if change.message.is_some() {
                return Err(format!(
                    "deleted/revoked change for message {} must not carry a body",
                    change.message_id
                ));
            }
            if change.message_id <= 0 {
                return Err("message_id must be positive".into());
            }
            ops.remove_messages
                .push((target_key.to_string(), change.message_id));
        }
        TargetChangeKind::Unspecified => {
            return Err(format!(
                "change for message {} has unspecified kind",
                change.message_id
            ));
        }
    }
    Ok(ops)
}

/// Build cache writes for one fetched `Message` body (e.g. from
/// `GetMessages` hydration of edited ids). Upserts carry body revisions and edit dates
/// for storage freshness checks, independently of message acceptance positions.
pub fn project_message_to_ops(
    target_key: &str,
    msg: &TinkleMessage,
) -> Result<tinkle_storage::CacheOps, String> {
    let mut ops = tinkle_storage::CacheOps::default();
    push_message(&mut ops, target_key, msg)?;
    Ok(ops)
}

fn push_message(
    ops: &mut tinkle_storage::CacheOps,
    target_key: &str,
    msg: &TinkleMessage,
) -> Result<(), String> {
    if msg.message_id <= 0 {
        return Err("message_id must be positive".into());
    }
    if let Some(revision) = msg.body_revision {
        if revision <= 0 {
            return Err("message body_revision must be positive".into());
        }
        ops.message_body_revisions.push((msg.message_id, revision));
    }
    if target_key.is_empty() {
        return Err("target_key must not be empty".into());
    }
    let (peer_kind, peer_id) = peer_of(msg);
    let mut proto = Vec::new();
    msg.encode(&mut proto)
        .map_err(|e| format!("encode message: {e}"))?;
    if proto.len() > 2_000_000 {
        return Err("message proto too large".into());
    }
    let (edit_secs, edit_nanos) = edit_parts(msg);
    ops.messages.push(tinkle_storage::MessageRow {
        conversation_key: target_key.to_string(),
        message_id: msg.message_id,
        peer_kind: peer_kind.clone(),
        peer_id,
        from_id: msg.from_id,
        body: msg.message.clone(),
        proto,
        origin_stream: target_key.to_string(),
        accept_seq: msg.accept_seq,
        edit_date_secs: edit_secs,
        edit_date_nanos: edit_nanos,
    });
    if peer_id != 0 {
        let dialog_proto = build_dialog_proto(&peer_kind, peer_id, msg.message_id);
        ops.dialogs.push(tinkle_storage::DialogRow {
            peer_kind,
            peer_id,
            top_message: msg.message_id,
            unread_count: 0,
            read_inbox_max_id: 0,
            read_outbox_max_id: 0,
            pinned: false,
            proto: dialog_proto,
            origin_stream: target_key.to_string(),
            read_state_pending: false,
        });
    }
    // File-ref registration (shared records): same file_id across messages
    // shares one `files` row (scope ""), with per-message refs. Projected
    // in the same fenced transaction as the body.
    let file_ops = crate::files::project_message_files_to_ops(target_key, msg.message_id, msg);
    ops.files.extend(file_ops.files);
    ops.message_files.extend(file_ops.message_files);
    Ok(())
}

/// Merge `extra` into `base` (message/dialog/user/chat/removal vectors
/// concatenated; read-state scopes concatenated). Dialog touches are
/// coalesced so one journal page (up to 500 changes, all for the same
/// conversation peer) commits a single dialog row instead of tripping the
/// SQLite 256-dialog batch limit.
pub fn merge_ops(base: &mut tinkle_storage::CacheOps, extra: tinkle_storage::CacheOps) {
    // Coalesce repeated message IDs before pairing rows with revision metadata.
    // A newer journal marker can carry the same or an older fetched body.
    let mut winners = HashSet::new();
    for message in extra.messages {
        let revision = extra
            .message_body_revisions
            .iter()
            .find(|(id, _)| *id == message.message_id)
            .map(|(_, r)| *r);
        let index = base
            .messages
            .iter()
            .position(|m| m.message_id == message.message_id);
        let replace = index.is_none_or(|index| {
            let old = &base.messages[index];
            let old_revision = base
                .message_body_revisions
                .iter()
                .find(|(id, _)| *id == message.message_id)
                .map(|(_, r)| *r);
            match (old_revision, revision) {
                (Some(a), Some(b)) => b > a,
                _ => match (old.edit_date_secs, message.edit_date_secs) {
                    (None, Some(_)) => true,
                    (Some(a), Some(b)) => {
                        (b, message.edit_date_nanos.unwrap_or(0))
                            > (a, old.edit_date_nanos.unwrap_or(0))
                    }
                    _ => false,
                },
            }
        });
        if replace {
            winners.insert(message.message_id);
            base.message_files
                .retain(|r| r.message_id != message.message_id);
            base.message_body_revisions
                .retain(|(id, _)| *id != message.message_id);
            if let Some(r) = revision {
                base.message_body_revisions.push((message.message_id, r));
            }
            if let Some(index) = index {
                base.messages[index] = message;
            } else {
                base.messages.push(message);
            }
        }
    }
    base.dialogs.extend(extra.dialogs);
    base.users.extend(extra.users);
    base.chats.extend(extra.chats);
    base.read_state_scopes.extend(extra.read_state_scopes);
    base.remove_messages.extend(extra.remove_messages);
    base.body_invalidations.extend(extra.body_invalidations);
    base.files.extend(extra.files);
    base.message_files.extend(
        extra
            .message_files
            .into_iter()
            .filter(|r| winners.contains(&r.message_id)),
    );
    base.entity_files.extend(extra.entity_files);
    base.replace_entity_files.extend(extra.replace_entity_files);
    coalesce_dialog_touches(base);
}

/// Coalesce dialog touches to one row per `(peer_kind, peer_id)`, keeping
/// the greatest known message acceptance position (and its proto/origin). Journal pages batch
/// one dialog touch per change; without this a 500-change page for one
/// conversation would exceed the SQLite 256-dialog
/// `apply_journal_page_fenced` batch limit even though every touch targets
/// the same peer.
pub fn coalesce_dialog_touches(ops: &mut tinkle_storage::CacheOps) {
    if ops.dialogs.len() <= 1 {
        return;
    }
    use std::collections::HashMap;
    let mut best: HashMap<(String, i64), usize> = HashMap::new();
    let mut out: Vec<tinkle_storage::DialogRow> = Vec::with_capacity(ops.dialogs.len());
    for d in ops.dialogs.drain(..) {
        let key = (d.peer_kind.clone(), d.peer_id);
        if let Some(&idx) = best.get(&key) {
            let position = |id| {
                ops.messages
                    .iter()
                    .find(|m| m.message_id == id)
                    .and_then(|m| m.accept_seq)
            };
            if matches!((position(out[idx].top_message),position(d.top_message)),(Some(a),Some(b)) if b>a)
            {
                out[idx] = d;
            }
            continue;
        }
        best.insert(key, out.len());
        out.push(d);
    }
    ops.dialogs = out;
}

/// Encode one user row, skipping non-positive ids and encode failures
/// (`None` when the row must be dropped).
pub fn encode_user_row(user: &TinkleUser) -> Option<tinkle_storage::UserRow> {
    let id = match user.user_type.as_ref() {
        Some(tinkle_schema::proto::user::UserType::Data(d)) => d.id,
        Some(tinkle_schema::proto::user::UserType::Empty(e)) => e.id,
        None => 0,
    };
    if id <= 0 {
        return None;
    }
    let mut proto = Vec::new();
    if user.encode(&mut proto).is_err() {
        return None;
    }
    Some(tinkle_storage::UserRow { user_id: id, proto })
}

/// Encode one chat row, skipping non-positive ids and encode failures.
pub fn encode_chat_row(chat: &TinkleChat) -> Option<tinkle_storage::ChatRow> {
    if chat.chat_id <= 0 {
        return None;
    }
    let mut proto = Vec::new();
    if chat.encode(&mut proto).is_err() {
        return None;
    }
    Some(tinkle_storage::ChatRow {
        chat_id: chat.chat_id,
        proto,
    })
}

/// Project entity hydration (users/chats) into `CacheOps` with scoped
/// provenance (provenance stream is bound at commit time to the page
/// `target_key` by `apply_journal_page_fenced`).
///
/// File refs (user avatars) are projected alongside entities: same `file_id`
/// across users/messages shares one `files` row (scope `""`), with
/// per-entity refs. Chat photos carry no `file_id` today, so chats
/// contribute no refs (hook retained for future shapes). Repeated unversioned entities
/// keep the first complete snapshot and its refs, never a union of conflicting copies.
pub fn project_entities_to_ops(
    users: &[TinkleUser],
    chats: &[TinkleChat],
) -> tinkle_storage::CacheOps {
    let mut ops = tinkle_storage::CacheOps::default();
    let mut seen_users = HashSet::new();
    let mut selected_users = Vec::new();
    for u in users {
        if let Some(row) = encode_user_row(u) {
            if seen_users.insert(row.user_id) {
                ops.users.push(row);
                selected_users.push(u.clone());
            }
        }
    }
    let mut seen_chats = HashSet::new();
    let mut selected_chats = Vec::new();
    for c in chats {
        if let Some(row) = encode_chat_row(c) {
            if seen_chats.insert(row.chat_id) {
                ops.chats.push(row);
                selected_chats.push(c.clone());
            }
        }
    }
    // Shared file refs alongside entities (origin "" = global pin; journal
    // callers rewrite to the page target via `rebase_file_origins`).
    let file_ops = crate::files::project_entity_files_to_ops(&selected_users, &selected_chats);
    ops.files.extend(file_ops.files);
    ops.entity_files.extend(file_ops.entity_files);
    ops.replace_entity_files
        .extend(file_ops.replace_entity_files);
    ops
}

/// Rewrite placeholder file origins (`""`) to a journal `target_key` so the
/// batch passes `apply_journal_page_fenced` validation
/// (`file origin_stream must equal page target_key`). Global-pin snapshots
/// keep `""`.
pub fn rebase_file_origins(ops: &mut tinkle_storage::CacheOps, target_key: &str) {
    for f in &mut ops.files {
        if f.origin_stream.is_empty() {
            f.origin_stream = target_key.to_string();
        }
    }
}

/// Project sticker-pack file refs (`StickerView.file_id` + `thumb_file_id`)
/// into `CacheOps` (`kind = "sticker"`). Shares records with message/user
/// refs when the same `file_id` appears elsewhere.
#[allow(dead_code)]
pub fn project_stickers_to_ops(
    stickers: &[tinkle_schema::proto::StickerView],
) -> tinkle_storage::CacheOps {
    crate::files::project_sticker_files_to_ops(stickers)
}

/// Batch-project fetched message bodies into `CacheOps`.
///
/// Shared by the reconcile journal path (`build_page_ops` edited hydration),
/// the history/push ingest path (`ingest_messages`), and the new-message
/// catch-up (`reconcile_new_messages`). Messages whose ids appear in
/// `exclude` (deletions/revocations from the same run) are skipped so a
/// delayed body fetch can never resurrect a deleted row. Freshness
/// (body revision / edit date) travels with the row and is enforced atomically by storage.
pub fn project_messages_to_ops(
    target_key: &str,
    messages: &[TinkleMessage],
    exclude: &HashSet<i64>,
) -> Result<tinkle_storage::CacheOps, String> {
    let mut ops = tinkle_storage::CacheOps::default();
    for m in messages {
        if exclude.contains(&m.message_id) {
            continue;
        }
        let extra = project_message_to_ops(target_key, m)?;
        merge_ops(&mut ops, extra);
    }
    Ok(ops)
}

fn build_dialog_proto(peer_kind: &str, peer_id: i64, top_message: i64) -> Vec<u8> {
    use tinkle_schema::proto::{target::Kind, Dialog, Target};
    let kind = match peer_kind {
        "room" => Some(Kind::ChatId(peer_id)),
        "user" => Some(Kind::UserId(peer_id)),
        _ => None,
    };
    let dialog = Dialog {
        target: kind.map(|k| Target { kind: Some(k) }),
        top_message,
        unread_count: 0,
        read_inbox_max_id: 0,
        read_outbox_max_id: None,
        pinned: false,
        space_id: 0,
        draft: None,
        draft_dialog_id: None,
        draft_revision: None,
        ..Default::default()
    };
    let mut buf = Vec::new();
    let _ = dialog.encode(&mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinkle_schema::proto::{target::Kind, Target, TargetChange};

    fn target_room7() -> Target {
        Target {
            kind: Some(Kind::ChatId(7)),
        }
    }

    fn message(id: i64, accept: Option<i64>, edit_secs: Option<i64>) -> TinkleMessage {
        TinkleMessage {
            message_id: id,
            target: Some(target_room7()),
            from_id: 42,
            message: format!("m{id}"),
            accept_seq: accept,
            edit_date: edit_secs.map(|s| prost_types::Timestamp {
                seconds: s,
                nanos: 0,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn key_for_targets() {
        use tinkle_schema::sync::target_key_for;
        assert_eq!(target_key_for(&Kind::UserId(7)), "target:user:7");
        assert_eq!(target_key_for(&Kind::ChatId(7)), "target:room:7");
    }

    /// Invalidation projects a durable revision floor without a terminal tombstone.
    #[test]
    fn body_invalidation_projects_nonterminal_floor() {
        let c = TargetChange {
            message_id: 10,
            kind: TargetChangeKind::Edited as i32,
            accept_seq: 100,
            message_revision: Some(2),
            body_invalidated: true,
            ..Default::default()
        };
        let ops = project_change_to_ops("target:room:7", &c).unwrap();
        assert_eq!(
            ops.body_invalidations,
            vec![("target:room:7".into(), 10, 2)]
        );
        assert!(ops.remove_messages.is_empty());
        assert!(ops.messages.is_empty());
        assert!(ops.message_files.is_empty());
        let mut merged = tinkle_storage::CacheOps::default();
        merge_ops(&mut merged, ops);
        assert_eq!(merged.body_invalidations.len(), 1);
    }

    #[test]
    fn edited_projects_upsert_with_token() {
        let change = TargetChange {
            message_id: 10,
            kind: TargetChangeKind::Edited as i32,
            message: Some(message(10, Some(4), None)),
            accept_seq: 4,
            ..Default::default()
        };
        let ops = project_change_to_ops("target:room:7", &change).unwrap();
        assert_eq!(ops.messages.len(), 1);
        assert_eq!(ops.messages[0].accept_seq, Some(4));
        assert!(ops.remove_messages.is_empty());
        assert_eq!(ops.dialogs.len(), 1);
    }

    #[test]
    fn deleted_and_revoked_project_removals() {
        for kind in [TargetChangeKind::Deleted, TargetChangeKind::Revoked] {
            let change = TargetChange {
                message_id: 11,
                kind: kind as i32,
                message: None,
                accept_seq: 5,
                ..Default::default()
            };
            let ops = project_change_to_ops("target:room:7", &change).unwrap();
            assert!(ops.messages.is_empty());
            assert_eq!(ops.remove_messages, vec![("target:room:7".to_string(), 11)]);
        }
    }

    #[test]
    fn edited_without_body_rejected() {
        let change = TargetChange {
            message_id: 10,
            kind: TargetChangeKind::Edited as i32,
            message: None,
            accept_seq: 4,
            ..Default::default()
        };
        assert!(project_change_to_ops("target:room:7", &change).is_err());
    }

    #[test]
    fn body_with_body_rejected() {
        let change = TargetChange {
            message_id: 10,
            kind: TargetChangeKind::Deleted as i32,
            message: Some(message(10, Some(4), None)),
            accept_seq: 4,
            ..Default::default()
        };
        assert!(project_change_to_ops("target:room:7", &change).is_err());
    }

    #[test]
    fn fetched_body_projects_with_fresher_fields() {
        let ops = project_message_to_ops("target:room:7", &message(10, None, Some(20))).unwrap();
        assert_eq!(ops.messages.len(), 1);
        assert_eq!(ops.messages[0].accept_seq, None);
        assert_eq!(ops.messages[0].edit_date_secs, Some(20));
    }

    /// Coalescing several metadata markers preserves the freshest body's own revision and bytes.
    #[test]
    fn metadata_projection_coalesces_without_mislabeling_old_bodies() {
        let mut base = tinkle_storage::CacheOps::default();
        for revision in [2, 5, 3, 5] {
            let mut body = message(10, Some(1), None);
            body.body_revision = Some(revision);
            body.pinned = revision == 5;
            let change = TargetChange {
                message_id: 10,
                kind: TargetChangeKind::Pin as i32,
                message: Some(body),
                message_revision: Some(2),
                accept_seq: revision,
                body_invalidated: false,
            };
            merge_ops(
                &mut base,
                project_change_to_ops("target:room:7", &change).unwrap(),
            );
        }
        assert_eq!(base.messages.len(), 1);
        assert_eq!(base.message_body_revisions, vec![(10, 5)]);
        let body = TinkleMessage::decode(base.messages[0].proto.as_slice()).unwrap();
        assert!(body.pinned);
        assert_eq!(body.body_revision, Some(5));
        assert_eq!(body.edit_date, None);
    }

    #[test]
    fn peerless_body_projects_no_dialog() {
        let mut m = message(10, Some(4), None);
        m.target = None;
        let ops = project_message_to_ops("target:room:7", &m).unwrap();
        assert_eq!(ops.messages.len(), 1);
        assert_eq!(ops.messages[0].peer_id, 0);
        assert!(ops.dialogs.is_empty());
    }

    #[test]
    fn dialog_touches_coalesce_per_peer() {
        let mut ops = tinkle_storage::CacheOps::default();
        for id in [10, 11, 12] {
            let extra =
                project_message_to_ops("target:room:7", &message(id, Some(id), None)).unwrap();
            merge_ops(&mut ops, extra);
        }
        assert_eq!(ops.messages.len(), 3);
        // Same conversation peer: 3 touches coalesce to 1 row, keeping the
        // greatest top_message so a 500-change page never trips the 256
        // dialog batch limit.
        assert_eq!(ops.dialogs.len(), 1);
        assert_eq!(ops.dialogs[0].top_message, 12);
    }

    /// Chunked hydration may repeat unversioned entities. Select one entire snapshot,
    /// including its refs, rather than attaching stale avatar IDs to the selected body.
    #[test]
    fn repeated_entities_keep_one_body_and_matching_file_refs() {
        let users: Vec<_> = [10, 20]
            .into_iter()
            .map(|file_id| TinkleUser {
                user_type: Some(tinkle_schema::proto::user::UserType::Data(
                    tinkle_schema::proto::user::UserData {
                        id: 9,
                        photo: Some(tinkle_schema::proto::UserPhoto {
                            file_id,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )),
            })
            .collect();
        let chats = [
            TinkleChat {
                chat_id: 7,
                title: "first".into(),
                ..Default::default()
            },
            TinkleChat {
                chat_id: 7,
                title: "unranked".into(),
                ..Default::default()
            },
        ];
        let ops = project_entities_to_ops(&users, &chats);
        assert_eq!(ops.users.len(), 1);
        assert_eq!(ops.chats.len(), 1);
        assert_eq!(
            TinkleUser::decode(ops.users[0].proto.as_slice()).unwrap(),
            users[0]
        );
        assert_eq!(
            TinkleChat::decode(ops.chats[0].proto.as_slice()).unwrap(),
            chats[0]
        );
        assert_eq!(
            ops.entity_files
                .iter()
                .map(|r| r.file_id)
                .collect::<Vec<_>>(),
            vec![10]
        );
        assert_eq!(
            ops.files.iter().map(|r| r.file_id).collect::<Vec<_>>(),
            vec![10]
        );
    }
}
