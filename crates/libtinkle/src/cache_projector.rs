//! Project validated `TargetChange` pages + `Message` bodies into SQLite `CacheOps`.
//!
//! Crash invariant: message upserts/removals, dialog touches, entity writes,
//! and the journal checkpoint advance commit in one
//! `Storage::apply_journal_page_fenced` transaction. Required network
//! fetches (`GetMessages` for edited bodies) happen before this projection;
//! this module never does I/O.
//!
//! Dedup: message rows carry `accept_seq` + `edit_date`. Higher `accept_seq`
//! wins; rows with an absent token order only by `edit_date` via
//! `fresher_body` (dated beats absent, later beats earlier, absent never
//! beats dated).

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
/// - Deleted/Revoked → `remove_messages` (identity + kind only, never a body).
pub fn project_change_to_ops(
    target_key: &str,
    change: &TargetChange,
) -> Result<tinkle_storage::CacheOps, String> {
    let mut ops = tinkle_storage::CacheOps::default();
    let kind = TargetChangeKind::try_from(change.kind)
        .map_err(|_| format!("unknown target change kind {}", change.kind))?;
    match kind {
        TargetChangeKind::Edited => {
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
/// `GetMessages` hydration of edited ids). Upserts carry `accept_seq` /
/// `edit_date` so storage dedup (`higher accept_seq wins`, else
/// `fresher_body`) applies; absent tokens never vote on dedup.
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
/// concatenated; read-state scopes concatenated).
pub fn merge_ops(base: &mut tinkle_storage::CacheOps, extra: tinkle_storage::CacheOps) {
    base.messages.extend(extra.messages);
    base.dialogs.extend(extra.dialogs);
    base.users.extend(extra.users);
    base.chats.extend(extra.chats);
    base.read_state_scopes.extend(extra.read_state_scopes);
    base.remove_messages.extend(extra.remove_messages);
    base.files.extend(extra.files);
    base.message_files.extend(extra.message_files);
    base.entity_files.extend(extra.entity_files);
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
/// contribute no refs (hook retained for future shapes).
pub fn project_entities_to_ops(
    users: &[TinkleUser],
    chats: &[TinkleChat],
) -> tinkle_storage::CacheOps {
    let mut ops = tinkle_storage::CacheOps::default();
    for u in users {
        if let Some(row) = encode_user_row(u) {
            ops.users.push(row);
        }
    }
    for c in chats {
        if let Some(row) = encode_chat_row(c) {
            ops.chats.push(row);
        }
    }
    // Shared file refs alongside entities (origin "" = global pin; journal
    // callers rewrite to the page target via `rebase_file_origins`).
    let file_ops = crate::files::project_entity_files_to_ops(users, chats);
    ops.files.extend(file_ops.files);
    ops.entity_files.extend(file_ops.entity_files);
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
/// (`accept_seq` / `edit_date`) travels on the row and is enforced by
/// storage dedup (`higher accept_seq wins`, ties fall back to `edit_date`,
/// absent tokens order only by `edit_date`).
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
        read_outbox_max_id: 0,
        pinned: false,
        space_id: 0,
        draft: None,
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

    #[test]
    fn edited_projects_upsert_with_token() {
        let change = TargetChange {
            message_id: 10,
            kind: TargetChangeKind::Edited as i32,
            message: Some(message(10, Some(4), None)),
            accept_seq: 4,
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

    #[test]
    fn peerless_body_projects_no_dialog() {
        let mut m = message(10, Some(4), None);
        m.target = None;
        let ops = project_message_to_ops("target:room:7", &m).unwrap();
        assert_eq!(ops.messages.len(), 1);
        assert_eq!(ops.messages[0].peer_id, 0);
        assert!(ops.dialogs.is_empty());
    }
}
