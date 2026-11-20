//! Independent representation ordering for exact message captures; hints never grant membership.
use crate::{proto as p, sync::SyncError};
use prost::Message;

/// Permit only independent output-set advancement in an otherwise identical complete entity.
/// Unversioned user/chat fields and replacement photo identities remain insert-only.
pub fn entity_representation_advance(
    kind: &str,
    old: &[u8],
    new: &[u8],
) -> Result<bool, SyncError> {
    let bad = || SyncError::InvalidUpdate("invalid entity representation".into());
    match kind {
        "chat" => {
            let mut a = p::Chat::decode(old).map_err(|_| bad())?;
            let mut b = p::Chat::decode(new).map_err(|_| bad())?;
            let Some((x, y)) = a.photo.as_mut().zip(b.photo.as_mut()) else {
                return Ok(false);
            };
            let revision = (x.representation_revision, y.representation_revision);
            let thumbnails = (x.thumbnail, y.thumbnail);
            validate_thumbnail(x.file_id, x.thumbnail.as_ref())?;
            validate_thumbnail(y.file_id, y.thumbnail.as_ref())?;
            x.thumbnail = None;
            y.thumbnail = None;
            x.representation_revision = None;
            y.representation_revision = None;
            if a != b {
                return Ok(false);
            }
            advance(revision, thumbnails)
        }
        "user" => {
            let mut a = p::User::decode(old).map_err(|_| bad())?;
            let mut b = p::User::decode(new).map_err(|_| bad())?;
            let (Some(p::user::UserType::Data(x)), Some(p::user::UserType::Data(y))) =
                (a.user_type.as_mut(), b.user_type.as_mut())
            else {
                return Ok(false);
            };
            let Some((x, y)) = x.photo.as_mut().zip(y.photo.as_mut()) else {
                return Ok(false);
            };
            let revision = (x.representation_revision, y.representation_revision);
            let thumbnails = (x.thumbnail, y.thumbnail);
            validate_thumbnail(x.file_id, x.thumbnail.as_ref())?;
            validate_thumbnail(y.file_id, y.thumbnail.as_ref())?;
            x.thumbnail = None;
            y.thumbnail = None;
            x.representation_revision = None;
            y.representation_revision = None;
            if a != b {
                return Ok(false);
            }
            advance(revision, thumbnails)
        }
        _ => Err(bad()),
    }
}

/// Equal complete sets never diverge; absent legacy revisions cannot be upgraded by hydration.
fn advance(
    revision: (Option<u64>, Option<u64>),
    thumbnails: (Option<p::Thumbnail>, Option<p::Thumbnail>),
) -> Result<bool, SyncError> {
    match revision {
        (Some(0), _) | (_, Some(0)) => Err(SyncError::InvalidUpdate(
            "zero representation revision".into(),
        )),
        (Some(a), Some(b)) if a == b && thumbnails.0 != thumbnails.1 => Err(
            SyncError::InvalidUpdate("equal representations disagree".into()),
        ),
        (Some(a), Some(b)) => Ok(b > a),
        _ => Ok(false),
    }
}

/// Return the authorized complete body with monotonic semantic and representation state.
/// Revisions compare only for the same message/target and exact immutable media identity.
/// Missing semantic revisions use edit dates (ties keep `old`) without bypassing output merging.
pub fn merge_message(old: &p::Message, incoming: &p::Message) -> Result<p::Message, SyncError> {
    for message in [old, incoming] {
        validate_message_representations(message)?;
    }
    if old.message_id != incoming.message_id || old.target != incoming.target {
        return Err(SyncError::InvalidUpdate(
            "message merge scope mismatch".into(),
        ));
    }
    let newer = match (old.body_revision, incoming.body_revision) {
        (Some(a), Some(b)) if a > 0 && b > 0 => b.cmp(&a),
        _ => {
            let date = |m: &p::Message| m.edit_date.as_ref().map(|t| (t.seconds, t.nanos));
            // Date ties retain the cached semantics, but never bypass the independent output clock.
            if date(incoming).is_some() && date(incoming) > date(old) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
    };
    let mut result = if newer.is_lt() {
        old.clone()
    } else {
        incoming.clone()
    };
    let other = if newer.is_lt() { incoming } else { old };
    let mut a = old.clone();
    let mut b = incoming.clone();
    clear_representations(&mut a);
    clear_representations(&mut b);
    if newer.is_eq() && a != b {
        return Err(SyncError::InvalidUpdate(
            "equal semantic revisions disagree".into(),
        ));
    }
    match (
        other.media.as_ref().and_then(|m| m.media.as_ref()),
        result.media.as_mut().and_then(|m| m.media.as_mut()),
    ) {
        (Some(p::media::Media::Photo(a)), Some(p::media::Media::Photo(b))) => {
            let mut x = a.clone();
            let mut y = b.clone();
            x.thumbnail = None;
            y.thumbnail = None;
            x.representation_revision = None;
            y.representation_revision = None;
            if x == y {
                merge_set(
                    a.representation_revision,
                    &a.thumbnail,
                    &mut b.representation_revision,
                    &mut b.thumbnail,
                )?;
            }
        }
        (Some(p::media::Media::Document(a)), Some(p::media::Media::Document(b))) => {
            let mut x = a.clone();
            let mut y = b.clone();
            x.thumbnail = None;
            y.thumbnail = None;
            x.representation_revision = None;
            y.representation_revision = None;
            if x == y {
                merge_set(
                    a.representation_revision,
                    &a.thumbnail,
                    &mut b.representation_revision,
                    &mut b.thumbnail,
                )?;
            }
        }
        _ => {}
    }
    Ok(result)
}

/// Strip only independent representation fields when testing equality of authored semantics.
fn clear_representations(message: &mut p::Message) {
    match message.media.as_mut().and_then(|m| m.media.as_mut()) {
        Some(p::media::Media::Photo(v)) => {
            v.thumbnail = None;
            v.representation_revision = None;
        }
        Some(p::media::Media::Document(v)) => {
            v.thumbnail = None;
            v.representation_revision = None;
        }
        _ => {}
    }
}

/// Legacy captures stay frozen; equal independent revisions must have identical complete sets.
fn merge_set(
    old: Option<u64>,
    thumbnail: &Option<p::Thumbnail>,
    new: &mut Option<u64>,
    output: &mut Option<p::Thumbnail>,
) -> Result<(), SyncError> {
    if old == Some(0) || *new == Some(0) {
        return Err(SyncError::InvalidUpdate(
            "zero representation revision".into(),
        ));
    }
    match (old, *new) {
        (None, _) => {
            *new = None;
            *output = *thumbnail;
        }
        // Absence on either capture freezes its membership, regardless of semantic ordering.
        (_, None) => {}
        (Some(a), Some(b)) if a == b && thumbnail != output => {
            return Err(SyncError::InvalidUpdate(
                "equal representations disagree".into(),
            ))
        }
        (Some(a), Some(b)) if b < a => {
            *new = Some(a);
            *output = *thumbnail;
        }
        _ => {}
    }
    Ok(())
}

/// An absent thumbnail is distinct from a present but empty or main-file reference.
pub fn validate_thumbnail(main: i64, thumbnail: Option<&p::Thumbnail>) -> Result<(), SyncError> {
    if thumbnail
        .is_some_and(|t| t.file_id <= 0 || t.file_id == main || t.width == 0 || t.height == 0)
    {
        return Err(SyncError::InvalidUpdate("invalid thumbnail".into()));
    }
    Ok(())
}

/// Validate a first hydration as strictly as a later independent representation merge.
pub fn validate_message_representations(message: &p::Message) -> Result<(), SyncError> {
    let (main, thumbnail, revision) = match message.media.as_ref().and_then(|m| m.media.as_ref()) {
        Some(p::media::Media::Photo(v)) => {
            (v.file_id, v.thumbnail.as_ref(), v.representation_revision)
        }
        Some(p::media::Media::Document(v)) => {
            (v.file_id, v.thumbnail.as_ref(), v.representation_revision)
        }
        _ => return Ok(()),
    };
    if revision == Some(0) {
        return Err(SyncError::InvalidUpdate(
            "zero representation revision".into(),
        ));
    }
    validate_thumbnail(main, thumbnail)
}
