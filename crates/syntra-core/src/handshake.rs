//! Handshake helpers (`SNTR-HS-003/004/010/020`).
//!
//! The initiator sends exactly one `ClientHello` as the first Syntra
//! envelope; the host emits exactly one terminal outcome (`ServerReady`
//! on success). There is no `ServerHello` in v1. Credentials travel only
//! in `ClientHello.credential` — never in URLs, logs, or debug output.

use crate::error::{ErrorCode, SyntraError};

/// Minimum supported Syntra version for this SDK.
pub const MIN_VERSION: u32 = 1;
/// Maximum supported Syntra version for this SDK.
pub const MAX_VERSION: u32 = 1;
/// Default handshake deadline (SNTR-HS-002/011). Transport profiles may
/// state otherwise; this value is documented per deployment.
pub const HANDSHAKE_TIMEOUT_DEFAULT: std::time::Duration = std::time::Duration::from_secs(5);
/// Maximum credential bytes accepted before policy evaluation.
/// Bounds unauthenticated input (DoS boundary); deployments document theirs.
pub const MAX_CREDENTIAL_BYTES: usize = 16 * 1024;

/// Build a `ClientHello` range check. Both bounds nonzero, min <= max.
pub fn validate_hello_range(min_version: u32, max_version: u32) -> Result<(), SyntraError> {
    if min_version == 0 || max_version == 0 {
        return Err(SyntraError::Protocol {
            code: ErrorCode::InvalidArgument,
            message: "hello versions must be nonzero".to_string(),
        });
    }
    if min_version > max_version {
        return Err(SyntraError::Protocol {
            code: ErrorCode::InvalidArgument,
            message: "hello min_version must be <= max_version".to_string(),
        });
    }
    Ok(())
}

/// Select the highest overlap between initiator range and host support,
/// or fail with `UNIMPLEMENTED` (SNTR-HS-003/010).
pub fn negotiate_version(
    min_version: u32,
    max_version: u32,
    host_supported: &[u32],
) -> Result<u32, SyntraError> {
    validate_hello_range(min_version, max_version)?;
    let best = host_supported
        .iter()
        .copied()
        .filter(|v| *v >= min_version && *v <= max_version)
        .max();
    best.ok_or(SyntraError::Handshake {
        code: ErrorCode::Unimplemented,
        message: "no overlapping protocol version".to_string(),
    })
}

/// Validate a received `connection_id`: opaque UTF-8 diagnostic, never an
/// authenticator or resume token. Must be non-empty and bounded.
pub fn validate_connection_id(id: &str) -> Result<(), SyntraError> {
    if id.is_empty() || id.len() > 256 {
        return Err(SyntraError::Protocol {
            code: ErrorCode::InvalidArgument,
            message: "invalid connection_id length".to_string(),
        });
    }
    Ok(())
}

/// Build the hello credential holder. The caller owns the secret bytes;
/// this function never logs them. Empty is a valid anonymous attempt
/// subject to host policy (least privilege on accept).
pub fn build_hello(
    min_version: u32,
    max_version: u32,
    credential: Vec<u8>,
) -> Result<crate::proto::ClientHello, SyntraError> {
    validate_hello_range(min_version, max_version)?;
    if credential.len() > MAX_CREDENTIAL_BYTES {
        return Err(SyntraError::Protocol {
            code: ErrorCode::ResourceExhausted,
            message: "credential exceeds bound".to_string(),
        });
    }
    Ok(crate::proto::ClientHello {
        min_version,
        max_version,
        credential: credential.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiates_highest_overlap() {
        assert_eq!(negotiate_version(1, 2, &[1]).unwrap(), 1);
        assert!(negotiate_version(2, 3, &[1]).is_err());
    }

    #[test]
    fn rejects_zero_versions() {
        assert!(validate_hello_range(0, 1).is_err());
        assert!(validate_hello_range(2, 1).is_err());
    }
}
