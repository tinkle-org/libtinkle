//! Project validated `Update`s into SQLite `CacheOps`.
//!
//! Crash invariant: message/dialog/entity writes and the checkpoint advance
//! commit in one `Storage::apply_sync_batch` transaction. Required network
//! refreshes happen before this projection; this module never does I/O.

use prost::Message as _;

use tinkle_schema::proto::{update::Payload as UpdatePayload, Update};

/// Derive conversation key for offline queries.
/// Prefers `MessageContext.conversation_key` when present; otherwise derives
/// from the message peer (`room:<id>` or direct `user:<a>/user:<b>` pair).
pub fn conversation_key_for(
    principal: &str,
    context_key: Option<&str>,
    peer_kind: &str,
    peer_id: i64,
) -> String {
    if let Some(k) = context_key {
        if !k.is_empty() && k.len() <= 128 {
            return k.to_string();
        }
    }
    if peer_kind == "room" && peer_id > 0 {
        return format!("room:{peer_id}");
    }
    if peer_kind == "user" && peer_id > 0 {
        // Direct conversation: sorted pair so both sides share the key.
        // Principal is `user:<own>`; peer is the other user.
        let own: i64 = principal
            .strip_prefix("user:")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if own > 0 {
            let (a, b) = if own <= peer_id {
                (own, peer_id)
            } else {
                (peer_id, own)
            };
            return format!("{a}:{b}");
        }
        return format!("user:{peer_id}");
    }
    // Fallback: scope to principal so we never mix streams.
    principal.to_string()
}

fn peer_of(msg: &tinkle_schema::proto::Message) -> (String, i64) {
    match msg.peer.as_ref().and_then(|p| p.target.as_ref()) {
        Some(tinkle_schema::proto::peer_input::Target::ChatId(id)) => ("room".into(), *id),
        Some(tinkle_schema::proto::peer_input::Target::UserId(id)) => ("user".into(), *id),
        None => ("unknown".into(), 0),
    }
}

/// Build cache writes for one validated update. Returns empty ops for
/// payloads that carry no cacheable body (ContentOmitted, ReadStateInvalidated).
pub fn project_update_to_ops(
    principal: &str,
    update: &Update,
) -> Result<tinkle_storage::CacheOps, String> {
    let mut ops = tinkle_storage::CacheOps::default();
    match update.payload.as_ref() {
        None => return Err("update payload missing".into()),
        Some(UpdatePayload::ContentOmitted(_)) => {
            // Advances checkpoint only; must not invent deletions.
        }
        Some(UpdatePayload::ReadStateInvalidated(_)) => {
            // No cacheable body. Storage marks the update's stream scope
            // read-state-pending in the same transaction as the checkpoint
            // advance (see CacheOps); SQLite-mode auto-commit re-emits
            // UpdateReady (see SyncManager) so hosts still refresh, and the
            // authoritative refresh clears the flag.
            ops.read_state_scopes.push(update.stream.clone());
        }
        Some(UpdatePayload::MessageRefresh(m)) => {
            let msg = m
                .message
                .as_ref()
                .ok_or("message_refresh.message missing")?;
            if msg.message_id <= 0 {
                return Err("message_id must be positive".into());
            }
            let (peer_kind, peer_id) = peer_of(msg);
            let ctx_key = m.context.as_ref().map(|c| c.conversation_key.as_str());
            let conversation_key = conversation_key_for(principal, ctx_key, &peer_kind, peer_id);
            let mut proto = Vec::new();
            msg.encode(&mut proto)
                .map_err(|e| format!("encode message: {e}"))?;
            if proto.len() > 2_000_000 {
                return Err("message proto too large".into());
            }
            ops.messages.push(tinkle_storage::MessageRow {
                conversation_key,
                message_id: msg.message_id,
                peer_kind: peer_kind.clone(),
                peer_id,
                from_id: msg.from_id,
                body: msg.message.clone(),
                proto,
                origin_stream: update.stream.clone(),
            });
            // Touch dialog so offline list stays ordered without extra RPC.
            // Storage applies this as a top_message-only touch with
            // MAX(top_message): unread/read/pinned columns are preserved and
            // an older message (e.g. edit) never moves the top backwards.
            // Full dialog details arrive via GetDialogs refreshes.
            // Peer-less messages (optional Message.peer absent) have no valid
            // dialog identity: storage rejects peer_id 0 here, so skip the
            // touch. The message row above still commits and advances the
            // checkpoint via its derived conversation_key.
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
                    origin_stream: update.stream.clone(),
                    // Storage-owned: batch touches never set or clear this;
                    // invalidations mark it, authoritative refreshes clear it.
                    read_state_pending: false,
                });
            }
        }
        Some(UpdatePayload::SendAttemptFailed(m)) => {
            // Private to sender; reconcile without disclosing to others.
            // Cache the failed attempt's public projection if present so the
            // sender's outbox can show it offline.
            if let Some(msg) = m.message.as_ref() {
                if msg.message_id > 0 {
                    // Send attempts use transient ids (negative); only cache
                    // when the server assigned a durable positive message_id.
                    // Storage rejects message_id <= 0, so caching a negative
                    // id would fail the batch and leave the cursor unadvanced.
                    let (peer_kind, peer_id) = peer_of(msg);
                    let ctx_key = m.context.as_ref().map(|c| c.conversation_key.as_str());
                    let conversation_key =
                        conversation_key_for(principal, ctx_key, &peer_kind, peer_id);
                    let mut proto = Vec::new();
                    msg.encode(&mut proto)
                        .map_err(|e| format!("encode attempt message: {e}"))?;
                    ops.messages.push(tinkle_storage::MessageRow {
                        conversation_key,
                        message_id: msg.message_id,
                        peer_kind,
                        peer_id,
                        from_id: msg.from_id,
                        body: msg.message.clone(),
                        proto,
                        origin_stream: update.stream.clone(),
                    });
                }
            }
        }
    }
    Ok(ops)
}

fn build_dialog_proto(peer_kind: &str, peer_id: i64, top_message: i64) -> Vec<u8> {
    use tinkle_schema::proto::{peer_input::Target, Dialog, PeerInput};
    let target = match peer_kind {
        "room" => Some(Target::ChatId(peer_id)),
        "user" => Some(Target::UserId(peer_id)),
        _ => None,
    };
    let dialog = Dialog {
        peer: target.map(|t| PeerInput { target: Some(t) }),
        top_message,
        unread_count: 0,
        read_inbox_max_id: 0,
        read_outbox_max_id: 0,
        pinned: false,
        space_id: 0,
        draft: None,
    };
    let mut buf = Vec::new();
    // Encoding a locally built dialog cannot fail; fall back to empty on error.
    let _ = dialog.encode(&mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinkle_schema::proto::{
        peer_input::Target, update::Payload as P, ContentOmitted, Message, MessageContext,
        MessageRefresh, PeerInput,
    };

    fn message_update() -> Update {
        Update {
            event_id: "11111111-1111-4111-8111-111111111111".into(),
            stream: "room:7".into(),
            cursor: 1,
            occurred_at: None,
            source_seq: None,
            source_revision: None,
            payload: Some(P::MessageRefresh(MessageRefresh {
                message: Some(Message {
                    message_id: 10,
                    peer: Some(PeerInput {
                        target: Some(Target::ChatId(7)),
                    }),
                    from_id: 42,
                    date: None,
                    message: "hello".into(),
                    media: None,
                    entities: vec![],
                    reply_to: None,
                    edit_date: None,
                    reactions: None,
                    replies: None,
                    pinned: false,
                    ttl_period: 0,
                }),
                random_id: None,
                context: Some(MessageContext {
                    conversation_key: "room:7".into(),
                    order_key: String::new(),
                    resolve_user_id: None,
                    resolve_conversation: false,
                }),
            })),
        }
    }

    #[test]
    fn projects_message_and_dialog() {
        let ops = project_update_to_ops("user:1", &message_update()).unwrap();
        assert_eq!(ops.messages.len(), 1);
        assert_eq!(ops.messages[0].message_id, 10);
        assert_eq!(ops.messages[0].conversation_key, "room:7");
        assert_eq!(ops.dialogs.len(), 1);
        assert_eq!(ops.dialogs[0].top_message, 10);
    }

    #[test]
    fn peerless_message_projects_no_dialog() {
        // Optional Message.peer absent: message row keeps the derived
        // conversation_key, but no dialog touch (storage has no valid
        // dialog identity for peer_id 0).
        let mut u = message_update();
        if let Some(P::MessageRefresh(m)) = u.payload.as_mut() {
            if let Some(msg) = m.message.as_mut() {
                msg.peer = None;
            }
        }
        let ops = project_update_to_ops("user:1", &u).unwrap();
        assert_eq!(ops.messages.len(), 1);
        assert_eq!(ops.messages[0].peer_id, 0);
        assert_eq!(ops.messages[0].conversation_key, "room:7");
        assert!(ops.dialogs.is_empty());
    }

    #[test]
    fn omitted_projects_no_writes() {
        let update = Update {
            event_id: String::new(),
            stream: "user:1".into(),
            cursor: 2,
            occurred_at: None,
            source_seq: None,
            source_revision: None,
            payload: Some(P::ContentOmitted(ContentOmitted {})),
        };
        let ops = project_update_to_ops("user:1", &update).unwrap();
        assert!(ops.messages.is_empty());
        assert!(ops.dialogs.is_empty());
        assert!(ops.read_state_scopes.is_empty());
    }

    #[test]
    fn invalidated_projects_stream_scope_only() {
        use tinkle_schema::proto::ReadStateInvalidated;
        let update = Update {
            event_id: "22222222-2222-4222-8222-222222222222".into(),
            stream: "room:7".into(),
            cursor: 5,
            occurred_at: None,
            source_seq: None,
            source_revision: None,
            payload: Some(P::ReadStateInvalidated(ReadStateInvalidated {})),
        };
        let ops = project_update_to_ops("user:1", &update).unwrap();
        assert!(ops.messages.is_empty());
        assert!(ops.dialogs.is_empty());
        // The payload names no dialog: the mark is stream-scoped.
        assert_eq!(ops.read_state_scopes, vec!["room:7".to_string()]);
    }
}
