//! FFI-safe client configuration.

use crate::FfiError;

/// Record mirrored into [`libtinkle::ClientConfig`]. All fields owned.
///
/// `grpc_endpoint` enables the direct-unary gRPC fallback for reconciliation
/// RPCs (`GetMessages`/`GetTargetChanges`). Syntra stays primary on the live
/// connection; when set, retryable Syntra failures fall back to this endpoint
/// automatically. It is validated fail-fast by [`TinkleClient::new`] and
/// [`TinkleClient::new_sqlite`]: a non-blank-but-invalid endpoint (missing
/// scheme, non-`https` without `allow_insecure`) returns
/// [`FfiError::Internal`] at construction instead of surfacing as a retried
/// network failure at reconciliation time. `None` (or blank, treated as
/// unset for backward compatibility) keeps Syntra-only reconciliation.
#[derive(Debug, Clone, uniffi::Record)]
pub struct ClientConfigFfi {
    /// Gateway URL, e.g. `wss://gateway.example.com/syntra/v1/ws`.
    pub url: String,
    /// Allow `ws://` (development loopback/isolated nets only).
    pub allow_insecure: bool,
    /// Optional user-agent string.
    pub user_agent: Option<String>,
    /// Direct-unary gRPC base endpoint for reconciliation RPCs.
    pub grpc_endpoint: Option<String>,
}

impl ClientConfigFfi {
    /// Validate `grpc_endpoint` fail-fast, mirroring
    /// [`libtinkle::ClientConfig::validate_grpc_endpoint`]. Returns the
    /// trimmed endpoint when set and valid, `None` when unset/blank.
    pub(crate) fn validated_grpc_endpoint(
        &self,
        allow_insecure: bool,
    ) -> Result<Option<String>, FfiError> {
        match &self.grpc_endpoint {
            None => Ok(None),
            Some(ep) => {
                let trimmed = ep.trim().to_string();
                if trimmed.is_empty() {
                    // Backward compatible: blank means unset (client falls
                    // back to Syntra unary reconciliation).
                    return Ok(None);
                }
                libtinkle::ClientConfig::validate_grpc_endpoint(&trimmed, allow_insecure)
                    .map_err(FfiError::from)?;
                Ok(Some(trimmed))
            }
        }
    }
}

/// Fail-fast conversion: validates `grpc_endpoint` (missing scheme,
/// non-`https` without `allow_insecure`, blank handling) and returns
/// [`FfiError::Internal`] on misconfig instead of silently dropping the
/// endpoint. Used by [`crate::TinkleClient::new`] and
/// [`crate::TinkleClient::new_sqlite`].
pub(crate) fn validated_core_config(
    c: ClientConfigFfi,
) -> Result<libtinkle::ClientConfig, FfiError> {
    let allow_insecure = c.allow_insecure;
    // Validate before building so misconfig fails fast here.
    let grpc_endpoint = ClientConfigFfi {
        url: c.url.clone(),
        allow_insecure,
        user_agent: c.user_agent.clone(),
        grpc_endpoint: c.grpc_endpoint.clone(),
    }
    .validated_grpc_endpoint(allow_insecure)?;
    let mut cfg = libtinkle::ClientConfig::new(c.url);
    if allow_insecure {
        cfg = cfg.insecure_dev();
    }
    cfg.user_agent = c.user_agent;
    cfg.grpc_endpoint = grpc_endpoint;
    Ok(cfg)
}
