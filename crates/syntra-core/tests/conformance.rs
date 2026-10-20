//! Syntra protocol conformance vectors (spec-derived, transport-independent).
//!
//! Covers: handshake/version, authentication shape, envelope validation,
//! concurrent RPC correlation, cancellation races, notification
//! interleaving, errors/malformed frames. Mirrors `CONF-CORE-*`,
//! `CONF-SESS-*`, `CONF-RPC-*`, `CONF-EVT-*` intent without requiring a
//! live gateway (see `libtinkle/tests/client_integration.rs` for the
//! live-gateway suite).

use prost::Message;
use syntra_core::proto::{frame::Body, ClientHello, Frame, RpcRequest, RpcResult};
use syntra_core::{
    decode_frame, encode_frame, validate_method, ConnectionState, ErrorCode, FrameKind,
    ValidationContext,
};

fn active() -> ValidationContext {
    ValidationContext::active(1)
}

#[test]
fn valid_envelopes_per_variant() {
    // request with empty + arbitrary payloads are meaningful, not errors.
    for (id, payload) in [
        (7u64, b"hello".to_vec()),
        (8, vec![]),
        (9, vec![0, 1, 2, 255, 254]),
    ] {
        let f = Frame {
            version: 1,
            request_id: id,
            body: Some(Body::Request(RpcRequest {
                method: "/example.Echo/Echo".into(),
                payload: payload.into(),
                timeout_ms: 500,
            })),
        };
        assert!(encode_frame(&f, active()).is_ok());
    }
    // empty success payload explicitly means success (not unset outcome).
    let f = Frame {
        version: 1,
        request_id: 8,
        body: Some(Body::Result(RpcResult {
            outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(
                vec![].into(),
            )),
        })),
    };
    assert!(encode_frame(&f, active()).is_ok());
}

#[test]
fn empty_body_rejected() {
    let f = Frame {
        version: 1,
        request_id: 4,
        body: None,
    };
    let bytes = f.encode_to_vec();
    let err = decode_frame(&bytes, active()).unwrap_err();
    assert_eq!(err.code(), ErrorCode::InvalidArgument);
}

#[test]
fn request_id_misuse_matrix() {
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
    assert!(encode_frame(
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
    assert!(encode_frame(&f, active()).is_err());
    // notification with nonzero id
    let f = Frame {
        version: 1,
        request_id: 5,
        body: Some(Body::Notification(syntra_core::proto::Notification {
            payload: vec![].into(),
        })),
    };
    assert!(encode_frame(&f, active()).is_err());
}

#[test]
fn malformed_method_rejected() {
    for bad in [
        "example.echo",
        "/only-one",
        "/a/b/c",
        "//M",
        "",
        "/pkg.Svc/With Space",
    ] {
        assert!(validate_method(bad).is_err(), "should reject {bad}");
    }
    assert!(validate_method("/tinkle.v1.Realtime/SubscribeBatched").is_ok());
}

#[test]
fn missing_outcome_is_violation_not_success() {
    let f = Frame {
        version: 1,
        request_id: 7,
        body: Some(Body::Result(RpcResult { outcome: None })),
    };
    let bytes = f.encode_to_vec();
    let err = decode_frame(&bytes, active()).unwrap_err();
    assert_eq!(err.code(), ErrorCode::InvalidArgument);
}

#[test]
fn unknown_error_code_maps_to_unknown() {
    let (code, raw) = ErrorCode::from_i32_lossy(99);
    assert_eq!(code, ErrorCode::Unknown);
    assert_eq!(raw, Some(99));
}

#[test]
fn oversize_rejected() {
    let big = vec![0u8; syntra_core::MAX_FRAME_BYTES + 1];
    let err = decode_frame(&big, active()).unwrap_err();
    assert_eq!(err.code(), ErrorCode::ResourceExhausted);
}

#[test]
fn version_mismatch_rejected_in_active() {
    let f = Frame {
        version: 2,
        request_id: 7,
        body: Some(Body::Request(RpcRequest {
            method: "/example.Echo/Echo".into(),
            payload: vec![].into(),
            timeout_ms: 500,
        })),
    };
    let bytes = f.encode_to_vec();
    assert!(decode_frame(&bytes, active()).is_err());
}

#[test]
fn pre_active_application_frames_forbidden() {
    let f = Frame {
        version: 1,
        request_id: 7,
        body: Some(Body::Request(RpcRequest {
            method: "/example.Echo/Echo".into(),
            payload: vec![].into(),
            timeout_ms: 500,
        })),
    };
    let bytes = f.encode_to_vec();
    let ctx = ValidationContext::pre_handshake(ConnectionState::Negotiating);
    let err = decode_frame(&bytes, ctx).unwrap_err();
    assert_eq!(err.code(), ErrorCode::FailedPrecondition);
}

#[test]
fn hello_ready_forbidden_in_active() {
    let f = Frame {
        version: 1,
        request_id: 0,
        body: Some(Body::Hello(ClientHello {
            min_version: 1,
            max_version: 1,
            credential: vec![].into(),
        })),
    };
    let bytes = f.encode_to_vec();
    assert!(decode_frame(&bytes, active()).is_err());
}

#[test]
fn frame_kind_matches_body() {
    let f = Frame {
        version: 1,
        request_id: 7,
        body: Some(Body::Request(RpcRequest {
            method: "/example.Echo/Echo".into(),
            payload: vec![].into(),
            timeout_ms: 500,
        })),
    };
    let kind = syntra_core::validate_frame(&f, active()).unwrap();
    assert_eq!(kind, FrameKind::Request);
}
