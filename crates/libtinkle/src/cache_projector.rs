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
            // Caller refreshes dialogs/read state via RPC; nothing to project.
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
                conversation_key: conversation_key.clone(),
                message_id: msg.message_id,
                peer_kind: peer_kind.clone(),
                peer_id,
                from_id: msg.from_id,
                body: msg.message.clone(),
                proto,
            });
            // Touch dialog so offline list stays ordered without extra RPC.
            // Full dialog details arrive via GetDialogs refreshes.
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
            });
            // Best-effort sender hint: context may carry resolve_user_id, but
            // full user objects arrive via UsersService refreshes elsewhere.
            let _ = (&conversation_key, principal);
        }
        Some(UpdatePayload::SendAttemptFailed(m)) => {
            // Private to sender; reconcile without disclosing to others.
            // Cache the failed attempt's public projection if present so the
            // sender's outbox can show it offline.
            if let Some(msg) = m.message.as_ref() {
                if msg.message_id != 0 {
                    // Send attempts use transient ids; only cache when the
                    // server assigned a durable message_id.
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
    }
}
