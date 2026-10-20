//! Codec helpers: length bounds + prost encode/decode entry points.

use prost::Message;

use crate::error::{ErrorCode, SyntraError};
use crate::frame::ValidationContext;
use crate::proto::Frame;

pub use crate::frame::MAX_FRAME_BYTES;

/// Encode a validated `Frame`. Validates before encoding so malformed
/// envelopes are never emitted (SNTR-SER-010).
pub fn encode_frame(frame: &Frame, ctx: ValidationContext) -> Result<Vec<u8>, SyntraError> {
    crate::frame::validate_frame(frame, ctx)?;
    let mut buf = Vec::with_capacity(frame.encoded_len());
    frame.encode(&mut buf).map_err(|_| SyntraError::Protocol {
        code: ErrorCode::Internal,
        message: "frame encode failed".to_string(),
    })?;
    if buf.len() > MAX_FRAME_BYTES {
        return Err(SyntraError::Protocol {
            code: ErrorCode::ResourceExhausted,
            message: "encoded frame exceeds maximum envelope size".to_string(),
        });
    }
    Ok(buf)
}

/// Decode + validate received bytes (SNTR-SER-005/006/011).
pub fn decode_frame(bytes: &[u8], ctx: ValidationContext) -> Result<Frame, SyntraError> {
    crate::frame::decode_frame(bytes, ctx)
}
