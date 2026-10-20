//! Frame codec + envelope validation (`SNTR-WIRE-*`, `SNTR-SER-*`).
//!
//! Every Syntra unit on every transport is exactly one serialized
//! `Frame`. Producers set exactly one `body` variant; validators enforce
//! `request_id` discipline, version checks, size bounds, and
//! variant-state legality before any dispatch. First failure determines
//! the error.

use prost::Message;

use crate::error::{ErrorCode, SyntraError};
use crate::method::validate_method;
use crate::proto::frame::Body;
use crate::proto::Frame;
use crate::state::ConnectionState;

/// Default maximum serialized `Frame` bytes (SNTR-WIRE-006).
pub const MAX_FRAME_BYTES: usize = 1_048_576;

/// What a validated frame contains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    Hello,
    Ready,
    Request,
    Result,
    Cancel,
    Notification,
}

/// Context for validation: current state + negotiated version.
#[derive(Debug, Clone, Copy)]
pub struct ValidationContext {
    /// Current connection state.
    pub state: ConnectionState,
    /// Selected version once handshake completed; None before ACTIVE.
    pub negotiated_version: Option<u32>,
}

impl ValidationContext {
    pub fn pre_handshake(state: ConnectionState) -> Self {
        Self {
            state,
            negotiated_version: None,
        }
    }
    pub fn active(version: u32) -> Self {
        Self {
            state: ConnectionState::Active,
            negotiated_version: Some(version),
        }
    }
}

/// Encode a `Frame` to protobuf bytes.
pub fn encode_frame(frame: &Frame) -> Vec<u8> {
    frame.encode_to_vec()
}

/// Decode + validate a received `Frame`.
///
/// Steps in order (SNTR-WIRE-005): (1) well-formedness + size/depth,
/// (2) version, (3) `request_id` discipline, (4) variant-state legality,
/// (5) per-variant semantics. First failure determines the error;
/// validation completes before dispatch.
pub fn decode_frame(bytes: &[u8], ctx: ValidationContext) -> Result<Frame, SyntraError> {
    if bytes.is_empty() {
        return Err(SyntraError::Protocol {
            code: ErrorCode::InvalidArgument,
            message: "empty frame bytes".to_string(),
        });
    }
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(SyntraError::Protocol {
            code: ErrorCode::ResourceExhausted,
            message: "frame exceeds maximum envelope size".to_string(),
        });
    }
    // Reserved numbers 16/17 and names event/resume/ack must be rejected
    // (SNTR-SER-008). Prost drops unknown fields, so scan raw wire bytes
    // for top-level field numbers 16/17 before decoding.
    if contains_reserved_top_level(bytes) {
        return Err(SyntraError::Protocol {
            code: ErrorCode::InvalidArgument,
            message: "frame uses reserved field number".to_string(),
        });
    }
    let frame = Frame::decode(bytes).map_err(|_| SyntraError::Protocol {
        code: ErrorCode::InvalidArgument,
        message: "malformed protobuf frame".to_string(),
    })?;
    validate_frame(&frame, ctx)?;
    Ok(frame)
}

/// Validate an already-decoded `Frame` (also used before sending).
pub fn validate_frame(frame: &Frame, ctx: ValidationContext) -> Result<FrameKind, SyntraError> {
    let body = frame.body.as_ref().ok_or(SyntraError::Protocol {
        code: ErrorCode::InvalidArgument,
        message: "frame body must be set".to_string(),
    })?;

    let kind = match body {
        Body::Hello(_) => FrameKind::Hello,
        Body::Ready(_) => FrameKind::Ready,
        Body::Request(_) => FrameKind::Request,
        Body::Result(_) => FrameKind::Result,
        Body::Cancel(_) => FrameKind::Cancel,
        Body::Notification(_) => FrameKind::Notification,
    };

    // Version check (SNTR-WIRE-005 step 2).
    match ctx.state {
        ConnectionState::Active => {
            let v = ctx.negotiated_version.ok_or(SyntraError::Protocol {
                code: ErrorCode::FailedPrecondition,
                message: "no negotiated version in ACTIVE".to_string(),
            })?;
            if frame.version != v {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::InvalidArgument,
                    message: "frame version mismatch".to_string(),
                });
            }
        }
        _ => {
            // Pre-handshake: hello carries proposed max; ready carries
            // selected. Zero is never a negotiated version.
            if frame.version == 0 {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::InvalidArgument,
                    message: "frame version must be nonzero".to_string(),
                });
            }
        }
    }

    // request_id discipline (SNTR-WIRE-003).
    match kind {
        FrameKind::Hello | FrameKind::Ready | FrameKind::Notification => {
            if frame.request_id != 0 {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::InvalidArgument,
                    message: "hello/ready/notification must use request_id 0".to_string(),
                });
            }
        }
        FrameKind::Request | FrameKind::Result | FrameKind::Cancel => {
            if frame.request_id == 0 {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::InvalidArgument,
                    message: "request/result/cancel must use nonzero request_id".to_string(),
                });
            }
        }
    }

    // Variant-state legality (SNTR-WIRE-003, SNTR-HS-012).
    match ctx.state {
        ConnectionState::Connected => {
            if kind != FrameKind::Hello {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::FailedPrecondition,
                    message: "only hello is legal in CONNECTED".to_string(),
                });
            }
        }
        ConnectionState::Negotiating | ConnectionState::Authenticating => {
            // Initiator must not send application frames before ACTIVE;
            // second hello / ready-to-host etc. are handshake failures.
            match kind {
                FrameKind::Request | FrameKind::Cancel | FrameKind::Notification => {
                    return Err(SyntraError::Protocol {
                        code: ErrorCode::FailedPrecondition,
                        message: "application frames forbidden before ACTIVE".to_string(),
                    })
                }
                _ => {}
            }
        }
        ConnectionState::Active => match kind {
            FrameKind::Hello | FrameKind::Ready => {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::FailedPrecondition,
                    message: "hello/ready forbidden in ACTIVE".to_string(),
                })
            }
            _ => {}
        },
        ConnectionState::Closing | ConnectionState::Closed => {
            if matches!(kind, FrameKind::Request) {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::FailedPrecondition,
                    message: "no new RPCs in CLOSING/CLOSED".to_string(),
                });
            }
        }
    }

    // Per-variant semantics (SNTR-WIRE-005 step 5).
    match body {
        Body::Hello(h) => {
            crate::handshake::validate_hello_range(h.min_version, h.max_version)?;
            if h.credential.len() > crate::handshake::MAX_CREDENTIAL_BYTES {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::ResourceExhausted,
                    message: "hello credential exceeds bound".to_string(),
                });
            }
        }
        Body::Ready(r) => {
            crate::handshake::validate_connection_id(&r.connection_id)?;
        }
        Body::Request(req) => {
            validate_method(&req.method)?;
            // Payload bytes + envelope overhead must fit MAX_FRAME_BYTES.
            // A 1 MiB payload alone does not fit (rpc.md §1D, open B8);
            // enforce the envelope bound on serialized bytes at send time.
            let encoded = frame.encoded_len();
            if encoded > MAX_FRAME_BYTES {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::ResourceExhausted,
                    message: "request exceeds maximum envelope size".to_string(),
                });
            }
        }
        Body::Result(res) => {
            // outcome must be set, including for empty success (SNTR-RPC-006).
            if res.outcome.is_none() {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::InvalidArgument,
                    message: "result outcome must be set".to_string(),
                });
            }
            if let Some(crate::proto::rpc_result::Outcome::Error(e)) = &res.outcome {
                if e.code == 0 {
                    return Err(SyntraError::Protocol {
                        code: ErrorCode::InvalidArgument,
                        message: "error code must be nonzero".to_string(),
                    });
                }
            }
        }
        Body::Cancel(_) => {}
        Body::Notification(_) => {
            // Empty payload is valid (SNTR-WIRE-007); no further checks.
        }
    }

    Ok(kind)
}

/// Scan top-level protobuf wire bytes for field numbers 16/17.
///
/// Prost discards unknown fields, so reserved-number detection must
/// happen on raw bytes. This scans only the top-level Frame fields
/// (varint tags) and reports true if 16 or 17 appear with any wire type.
fn contains_reserved_top_level(bytes: &[u8]) -> bool {
    let mut i = 0;
    while i < bytes.len() {
        // Decode varint tag.
        let mut tag: u64 = 0;
        let mut shift = 0;
        let mut consumed = false;
        while i < bytes.len() {
            let b = bytes[i];
            i += 1;
            tag |= ((b & 0x7F) as u64) << shift;
            shift += 7;
            if b & 0x80 == 0 {
                consumed = true;
                break;
            }
            if shift > 64 {
                return false;
            }
        }
        if !consumed {
            return false;
        }
        let field = (tag >> 3) as u32;
        let wire = (tag & 0x07) as u8;
        if field == 16 || field == 17 {
            return true;
        }
        // Skip the field value per wire type.
        match wire {
            0 => {
                // varint
                while i < bytes.len() {
                    let b = bytes[i];
                    i += 1;
                    if b & 0x80 == 0 {
                        break;
                    }
                }
            }
            1 => {
                // 64-bit
                if i + 8 > bytes.len() {
                    return false;
                }
                i += 8;
            }
            2 => {
                // length-delimited
                let mut len: u64 = 0;
                let mut sh = 0;
                let mut ok = false;
                while i < bytes.len() {
                    let b = bytes[i];
                    i += 1;
                    len |= ((b & 0x7F) as u64) << sh;
                    sh += 7;
                    if b & 0x80 == 0 {
                        ok = true;
                        break;
                    }
                    if sh > 64 {
                        return false;
                    }
                }
                if !ok {
                    return false;
                }
                if i + (len as usize) > bytes.len() {
                    return false;
                }
                i += len as usize;
            }
            5 => {
                // 32-bit
                if i + 4 > bytes.len() {
                    return false;
                }
                i += 4;
            }
            _ => return false,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::*;

    fn ctx_active() -> ValidationContext {
        ValidationContext::active(1)
    }

    #[test]
    fn rejects_empty_body() {
        let f = Frame {
            version: 1,
            request_id: 4,
            body: None,
        };
        assert!(validate_frame(&f, ctx_active()).is_err());
    }

    #[test]
    fn request_id_discipline() {
        // hello with nonzero id
        let f = Frame {
            version: 1,
            request_id: 1,
            body: Some(Body::Hello(ClientHello {
                min_version: 1,
                max_version: 1,
                credential: vec![].into(),
            })),
        };
        assert!(validate_frame(
            &f,
            ValidationContext::pre_handshake(ConnectionState::Connected)
        )
        .is_err());
        // request with zero id
        let f = Frame {
            version: 1,
            request_id: 0,
            body: Some(Body::Request(RpcRequest {
                method: "/example.Echo/Echo".into(),
                payload: vec![].into(),
                timeout_ms: 500,
            })),
        };
        assert!(validate_frame(&f, ctx_active()).is_err());
    }

    #[test]
    fn result_requires_outcome() {
        let f = Frame {
            version: 1,
            request_id: 7,
            body: Some(Body::Result(RpcResult { outcome: None })),
        };
        assert!(validate_frame(&f, ctx_active()).is_err());
    }

    #[test]
    fn empty_payload_round_trip_is_success() {
        let f = Frame {
            version: 1,
            request_id: 8,
            body: Some(Body::Result(RpcResult {
                outcome: Some(crate::proto::rpc_result::Outcome::Payload(vec![].into())),
            })),
        };
        assert!(validate_frame(&f, ctx_active()).is_ok());
    }
}
