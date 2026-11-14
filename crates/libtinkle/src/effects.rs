//! Bounded ephemeral reaction presentation. No database, task, body hydration or mutation replay.
use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tinkle_schema::proto::MessageChanged;

/// App-visible body-free notification; hosts must still check the original deadline at display time.
/// Private scope/monotonic fences prevent queued events escaping through a retired app generation.
#[derive(Debug, Clone)]
pub struct ReactionEffectEvent {
    /// Exact body-free application hint, including the original deadline and captured asset versions.
    pub notification: MessageChanged,
    pub(crate) principal: String,
    pub(crate) epoch: u64,
    pub(crate) source_revision: u64,
    deadline: Instant,
}
impl ReactionEffectEvent {
    /// Presentation expires at the original absolute deadline, never delivery time plus five seconds.
    pub(crate) fn unexpired(&self) -> bool {
        Instant::now() < self.deadline
            && self
                .notification
                .reaction_effect
                .as_ref()
                .and_then(|e| e.expires_at.as_ref())
                .and_then(expiry)
                .is_some_and(|t| t > SystemTime::now())
    }
}

/// Reuse the existing app event admission bound. A full cache drops new effects; it never evicts
/// live dedup keys, which could permit the same operation to animate twice inside its window.
#[derive(Default)]
pub(crate) struct ReactionEffects {
    /// Local clock high-water extrapolation: a corrected/backward wall clock cannot revive expiry.
    clock: Option<(Instant, SystemTime)>,
    // Retain only negative dedup evidence across same-client ABA/rotation. Viewer keys are isolated;
    // queued presentation has separate epoch/source fences and is never carried across a switch.
    seen: HashMap<(String, i64, String), Instant>,
}

/// Checked conversion avoids signed/unsigned timestamp coercion and malformed nanos.
fn expiry(t: &prost_types::Timestamp) -> Option<SystemTime> {
    if t.seconds < 0 || !(0..1_000_000_000).contains(&t.nanos) {
        return None;
    }
    UNIX_EPOCH.checked_add(Duration::new(t.seconds as u64, t.nanos as u32))
}
impl ReactionEffects {
    /// Called with the caller's current epoch/source checked under this cache's lock. Same-viewer
    /// reconnect/ABA keeps dedup until original expiry. Clock rollback cannot extend a lease.
    pub(crate) fn admit(
        &mut self,
        notification: MessageChanged,
        principal: &str,
        epoch: u64,
        source_revision: u64,
    ) -> Option<ReactionEffectEvent> {
        self.admit_at(
            notification,
            principal,
            epoch,
            source_revision,
            (Instant::now(), SystemTime::now()),
        )
    }

    /// Paired clock samples keep admission deterministic in clock-discontinuity regressions.
    fn admit_at(
        &mut self,
        notification: MessageChanged,
        principal: &str,
        epoch: u64,
        source_revision: u64,
        clocks: (Instant, SystemTime),
    ) -> Option<ReactionEffectEvent> {
        let effect = notification.reaction_effect.as_ref()?;
        if !tinkle_schema::sync::valid_reaction_effect(effect) {
            return None;
        }
        // Sample monotonic time first: descheduling between clock reads must shorten, never
        // renew, the original remaining window even if the wall clock subsequently moves back.
        let (now, wall) = clocks;
        let wall = if let Some((previous, absolute)) = self.clock {
            wall.max(absolute.checked_add(now.checked_duration_since(previous)?)?)
        } else {
            wall
        };
        self.clock = Some((now, wall));
        let remaining = expiry(effect.expires_at.as_ref()?)?
            .duration_since(wall)
            .ok()?;
        if remaining.is_zero() || remaining > Duration::from_secs(5) {
            return None;
        }
        self.seen.retain(|_, deadline| *deadline > now);
        let key = (
            principal.to_owned(),
            effect.actor_id,
            effect.operation_id.clone(),
        );
        if self.seen.contains_key(&key) || self.seen.len() >= crate::sync::EVENT_CHANNEL_CAPACITY {
            return None;
        }
        let deadline = now.checked_add(remaining)?;
        self.seen.insert(key, deadline);
        Some(ReactionEffectEvent {
            notification,
            principal: principal.into(),
            epoch,
            source_revision,
            deadline,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinkle_schema::proto as p;
    /// A wall-clock rollback must not resurrect an operation whose original monotonic window ended.
    #[test]
    fn expired_operation_cannot_resurrect_after_wall_clock_rollback() {
        let now = Instant::now();
        let wall = SystemTime::now();
        let expiry = (wall + Duration::from_secs(4))
            .duration_since(UNIX_EPOCH)
            .unwrap();
        let hint = MessageChanged {
            reaction_effect: Some(p::ReactionEffect {
                actor_id: 42,
                operation_id: "11111111-1111-4111-8111-111111111111".into(),
                expires_at: Some(prost_types::Timestamp {
                    seconds: expiry.as_secs() as i64,
                    nanos: expiry.subsec_nanos() as i32,
                }),
                reactions: vec![p::Reaction {
                    emoticon: "x".into(),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };
        let mut cache = ReactionEffects::default();
        assert!(cache
            .admit_at(hint.clone(), "user:42", 1, 1, (now, wall))
            .is_some());
        assert!(
            cache
                .admit_at(
                    hint,
                    "user:42",
                    1,
                    1,
                    (now + Duration::from_secs(6), wall + Duration::from_secs(1))
                )
                .is_none(),
            "expired presentation resurrected after clock rollback"
        );
    }
    /// Admission pressure drops new effects, never evicts a still-live operation to reanimate it.
    #[test]
    fn dedup_is_bounded_and_keeps_original_operations_until_expiry() {
        let time = (SystemTime::now() + Duration::from_secs(4))
            .duration_since(UNIX_EPOCH)
            .unwrap();
        let hint = |i| MessageChanged {
            reaction_effect: Some(p::ReactionEffect {
                actor_id: 42,
                operation_id: format!("00000000-0000-4000-8000-{i:012x}"),
                expires_at: Some(prost_types::Timestamp {
                    seconds: time.as_secs() as i64,
                    nanos: time.subsec_nanos() as i32,
                }),
                reactions: vec![p::Reaction {
                    emoticon: "x".into(),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };
        let mut cache = ReactionEffects::default();
        for i in 0..crate::sync::EVENT_CHANNEL_CAPACITY {
            assert!(cache.admit(hint(i), "user:42", 1, 1).is_some());
        }
        assert!(cache.admit(hint(5000), "user:42", 1, 1).is_none());
        assert!(cache.admit(hint(0), "user:42", 1, 1).is_none());
        assert_eq!(cache.seen.len(), crate::sync::EVENT_CHANNEL_CAPACITY);
        // Pressure cannot evict negative evidence even on a switch. Without pressure viewers are
        // independent, while returning to the original viewer cannot replay the old operation.
        assert!(cache.admit(hint(0), "user:99", 2, 1).is_none());
        let mut isolated = ReactionEffects::default();
        assert!(isolated.admit(hint(0), "user:42", 1, 1).is_some());
        assert!(isolated.admit(hint(0), "user:99", 2, 1).is_some());
        assert!(isolated.admit(hint(0), "user:42", 3, 2).is_none());
    }
}
