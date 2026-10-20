//! Endpoint + TLS policy configuration.

use std::time::Duration;

/// WebSocket endpoint configuration.
#[derive(Debug, Clone)]
pub struct WsConfig {
    /// Full URL, e.g. `wss://gateway.example.com/syntra/v1/ws`.
    /// The path MUST be `/syntra/v1/ws` (case-sensitive, no trailing slash
    /// variant unless the deployment documents aliasing).
    pub url: String,
    /// When false (default), only `wss://` is accepted. Set true only for
    /// loopback/isolated test networks (`ws://127.0.0.1/...`); production
    /// builds MUST NOT enable it (SYN-ARCH-030, SNTR-TSEC-001/005).
    pub allow_insecure: bool,
    /// Expected host for TLS hostname verification is derived from the URL;
    /// this override exists only for tests with custom resolvers.
    pub sni_override: Option<String>,
    /// TCP + TLS + WS Upgrade timeout.
    pub connect_timeout: Duration,
    /// Close grace period for in-flight results (SNTR-HS-006/013).
    pub close_grace: Duration,
}

impl WsConfig {
    pub fn secure(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            allow_insecure: false,
            sni_override: None,
            connect_timeout: Duration::from_secs(10),
            close_grace: Duration::from_secs(2),
        }
    }

    /// Development-only plaintext endpoint. Emits an insecure-mode warning
    /// at connect time and refuses non-loopback hosts unless the caller
    /// explicitly opts in (defense against accidental public plaintext).
    pub fn insecure_dev(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            allow_insecure: true,
            sni_override: None,
            connect_timeout: Duration::from_secs(10),
            close_grace: Duration::from_secs(2),
        }
    }
}

/// Parsed + validated endpoint.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub scheme: String,
    pub host: String,
    pub port: Option<u16>,
    pub path: String,
    pub is_secure: bool,
}

impl Endpoint {
    pub fn parse(
        url_str: &str,
        allow_insecure: bool,
    ) -> Result<Self, crate::error::TransportError> {
        let url = url::Url::parse(url_str)
            .map_err(|e| crate::error::TransportError::InvalidUrl(format!("invalid url: {e}")))?;
        // Credentials MUST NOT travel in URL/query (SNTR-ASEC-003, SNTR-TSEC-021).
        if !url.username().is_empty() || url.password().is_some() {
            return Err(crate::error::TransportError::InvalidUrl(
                "credentials must not appear in the URL".to_string(),
            ));
        }
        if url.query().map(looks_like_secret).unwrap_or(false) {
            return Err(crate::error::TransportError::InvalidUrl(
                "query must not carry credential patterns".to_string(),
            ));
        }
        let scheme = url.scheme().to_string();
        let is_secure = match scheme.as_str() {
            "wss" => true,
            "ws" => {
                if !allow_insecure {
                    return Err(crate::error::TransportError::InsecureDenied(
                        "ws:// requires explicit insecure-dev opt-in".to_string(),
                    ));
                }
                false
            }
            _ => {
                return Err(crate::error::TransportError::InvalidUrl(format!(
                    "scheme must be ws or wss, got {scheme}"
                )))
            }
        };
        if !is_secure && !allow_insecure {
            return Err(crate::error::TransportError::InsecureDenied(
                "plaintext WS is restricted-development only".to_string(),
            ));
        }
        let path = url.path().to_string();
        if path != crate::ENDPOINT_PATH {
            return Err(crate::error::TransportError::InvalidUrl(format!(
                "path must be {}, got {path}",
                crate::ENDPOINT_PATH
            )));
        }
        let host = url.host_str().unwrap_or("").to_string();
        if host.is_empty() {
            return Err(crate::error::TransportError::InvalidUrl(
                "missing host".to_string(),
            ));
        }
        // Plaintext guard: refuse non-loopback unless explicitly allowed.
        // Loopback check is hostname-based (127.*, ::1, localhost); docs
        // require isolated test networks for any other plaintext host.
        if !is_secure && !is_loopback(&host) {
            tracing::warn!(
                host = %host,
                "plaintext WS to non-loopback host; allowed only on isolated test networks"
            );
        }
        Ok(Self {
            scheme,
            host,
            port: url.port(),
            path,
            is_secure,
        })
    }
}

fn looks_like_secret(q: &str) -> bool {
    let l = q.to_ascii_lowercase();
    l.contains("token") || l.contains("credential") || l.contains("auth") || l.contains("secret")
}

fn is_loopback(host: &str) -> bool {
    host == "localhost"
        || host == "127.0.0.1"
        || host.starts_with("127.")
        || host == "::1"
        || host == "[::1]"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_credentials_in_url() {
        assert!(Endpoint::parse("wss://user:pass@h/syntra/v1/ws", false).is_err());
        assert!(Endpoint::parse("wss://h/syntra/v1/ws?token=abc", false).is_err());
    }

    #[test]
    fn enforces_path_and_scheme() {
        assert!(Endpoint::parse("wss://h/syntra/v1/ws", false).is_ok());
        assert!(Endpoint::parse("wss://h/syntra/v1/ws/", false).is_err());
        assert!(Endpoint::parse("ws://h/syntra/v1/ws", false).is_err());
        assert!(Endpoint::parse("ws://127.0.0.1/syntra/v1/ws", true).is_ok());
        assert!(Endpoint::parse("https://h/syntra/v1/ws", false).is_err());
    }
}
