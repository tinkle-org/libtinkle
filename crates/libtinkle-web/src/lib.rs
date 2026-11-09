//! Portable schema validation. Encrypted native TCP is unavailable in browsers.
//! This crate deliberately has no runtime, network client or secret storage.

use wasm_bindgen::prelude::*;

/// Browser network support is explicitly unavailable for this native package.
#[wasm_bindgen]
pub fn network_supported() -> bool {
    false
}

/// Explicit unsupported entry point; never accepts credentials or opens a socket.
#[wasm_bindgen]
pub fn tinkle_connect() -> Result<(), JsValue> {
    Err(JsValue::from_str(
        "unsupported: libtinkle networking requires native encrypted TCP",
    ))
}

/// Validate a serialized connectivity observation without a network dependency.
#[wasm_bindgen]
pub fn validate_connectivity(payload: &[u8]) -> Result<(), JsValue> {
    tinkle_schema::sync::validate_connectivity_notification(payload)
        .map(|_| ())
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

#[cfg(test)]
mod tests {
    #[test]
    /// Keep browser networking explicitly disabled in the portable schema package.
    fn browser_network_is_unsupported() {
        assert!(!super::network_supported());
    }

    #[test]
    /// Malformed connectivity bytes must fail portable validation without opening a socket.
    fn portable_schema_checks_reject_malformed_payload() {
        assert!(tinkle_schema::sync::validate_connectivity_notification(&[0xff]).is_err());
    }
}
