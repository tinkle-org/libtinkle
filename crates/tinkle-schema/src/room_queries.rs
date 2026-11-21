//! Shape checks for room reads. The server decides membership and disclosure.

use crate::proto::{self as p, room_query_target::Room};
use thiserror::Error;

/// A room read request or response violated the protobuf contract.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RoomQueryError {
    /// A chat or channel must be selected by positive ID.
    #[error("positive room target required")]
    Target,
    /// A negative cursor cannot select a page.
    #[error("negative room member cursor")]
    Cursor,
    /// Only zero (default) and page sizes 1 through 200 are allowed.
    #[error("invalid room member page limit")]
    Limit,
    /// The response exceeds the effective page size or has invalid ordering or continuation.
    #[error("invalid room member page")]
    Page,
    /// A disclosed sample needs a nonnegative count and a valid observation time.
    #[error("invalid room online count sample")]
    Sample,
}

/// A room selector is not a conversation target or proof of membership.
fn validate_target(target: Option<&p::RoomQueryTarget>) -> Result<(), RoomQueryError> {
    match target.and_then(|target| target.room.as_ref()) {
        Some(Room::ChatId(1..) | Room::ChannelId(1..)) => Ok(()),
        _ => Err(RoomQueryError::Target),
    }
}

/// Check a page request without rewriting the server-default limit or cursor.
pub fn validate_members_request(req: &p::GetRoomMembersRequest) -> Result<(), RoomQueryError> {
    validate_target(req.target.as_ref())?;
    if req.after_user_id < 0 {
        return Err(RoomQueryError::Cursor);
    }
    if !(0..=200).contains(&req.limit) {
        return Err(RoomQueryError::Limit);
    }
    Ok(())
}

/// Check a returned page against its request. A short page may still have a continuation.
pub fn validate_members_page(
    req: &p::GetRoomMembersRequest,
    page: &p::GetRoomMembersResponse,
) -> Result<(), RoomQueryError> {
    validate_members_request(req)?;
    let limit = if req.limit == 0 { 50 } else { req.limit };
    if page.members.len() > limit as usize {
        return Err(RoomQueryError::Page);
    }
    let mut previous = req.after_user_id;
    for member in &page.members {
        if member.user_id <= previous {
            return Err(RoomQueryError::Page);
        }
        previous = member.user_id;
    }
    if page.next_after_user_id != 0
        && (page.members.is_empty() || page.next_after_user_id != previous)
    {
        return Err(RoomQueryError::Page);
    }
    Ok(())
}

/// Check the selector for one sampled aggregate read.
pub fn validate_online_count_request(
    req: &p::GetRoomOnlineCountRequest,
) -> Result<(), RoomQueryError> {
    validate_target(req.target.as_ref())
}

/// Check a disclosed sample without turning an absent sample into a zero count.
pub fn validate_online_count_response(
    response: &p::GetRoomOnlineCountResponse,
) -> Result<(), RoomQueryError> {
    if let Some(sample) = &response.sample {
        if sample.count < 0
            || !sample.sampled_at.as_ref().is_some_and(|time| {
                (-62_135_596_800..=253_402_300_799).contains(&time.seconds)
                    && (0..1_000_000_000).contains(&time.nanos)
            })
        {
            return Err(RoomQueryError::Sample);
        }
    }
    Ok(())
}
