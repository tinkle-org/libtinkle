//! Method identifier validation (`SNTR-RPC-001`, clarified in rpc.md §1C).
//!
//! Wire syntax is canonical `/package.Service/Method`: leading slash,
//! exactly two slashes, non-empty segments, ASCII printable excluding
//! spaces/control, byte-exact case-sensitive match, no normalization.

use crate::error::{ErrorCode, SyntraError};

/// Validate an `RpcRequest.method` before sending or dispatching.
/// Returns Ok(()) for valid paths, else `INVALID_ARGUMENT`.
pub fn validate_method(method: &str) -> Result<(), SyntraError> {
    if method.is_empty() || method.len() > 512 {
        return Err(SyntraError::Protocol {
            code: ErrorCode::InvalidArgument,
            message: "method must be 1..512 bytes".to_string(),
        });
    }
    if !method.starts_with('/') {
        return Err(SyntraError::Protocol {
            code: ErrorCode::InvalidArgument,
            message: "method must start with '/'".to_string(),
        });
    }
    // Exactly two slashes.
    if method.matches('/').count() != 2 {
        return Err(SyntraError::Protocol {
            code: ErrorCode::InvalidArgument,
            message: "method must be /package.Service/Method".to_string(),
        });
    }
    let mut parts = method.split('/');
    // split('/') on "/a/b" yields ["", "a", "b"]
    let _empty = parts.next();
    let pkg_svc = parts.next().unwrap_or("");
    let meth = parts.next().unwrap_or("");
    if pkg_svc.is_empty() || meth.is_empty() {
        return Err(SyntraError::Protocol {
            code: ErrorCode::InvalidArgument,
            message: "method segments must be non-empty".to_string(),
        });
    }
    for seg in [pkg_svc, meth] {
        // Dotted forms like `example.echo` are shorthand only; the wire
        // form still uses slashes. Dots inside segments are allowed
        // (e.g. `tinkle.v1.Realtime`), but segments must not be empty
        // around dots and must be ASCII printable without spaces/control.
        if seg.contains(' ') {
            return Err(SyntraError::Protocol {
                code: ErrorCode::InvalidArgument,
                message: "method must not contain spaces".to_string(),
            });
        }
        for b in seg.bytes() {
            if !(0x21..=0x7E).contains(&b) {
                return Err(SyntraError::Protocol {
                    code: ErrorCode::InvalidArgument,
                    message: "method must be ASCII printable excluding spaces/control".to_string(),
                });
            }
        }
    }
    // No trailing content after the two segments (exactly two slashes
    // already guarantees this, but double-check for embedded slashes).
    if pkg_svc.contains('/') || meth.contains('/') {
        return Err(SyntraError::Protocol {
            code: ErrorCode::InvalidArgument,
            message: "method must contain exactly two '/'".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_canonical() {
        assert!(validate_method("/example.Echo/Echo").is_ok());
        assert!(validate_method("/tinkle.v1.MessagesService/SendMessage").is_ok());
        assert!(validate_method("/tinkle.v1.Realtime/SubscribeBatched").is_ok());
    }

    #[test]
    fn rejects_shorthand_and_malformed() {
        assert!(validate_method("example.echo").is_err());
        assert!(validate_method("/only-one").is_err());
        assert!(validate_method("/a/b/c").is_err());
        assert!(validate_method("//Method").is_err());
        assert!(validate_method("/pkg./").is_err());
        assert!(validate_method("/pkg.Svc/With Space").is_err());
        assert!(validate_method("").is_err());
    }
}
