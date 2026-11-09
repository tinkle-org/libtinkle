//! External application acceptance entry point. Parent starts the real gateway;
//! this test never starts/builds Go or substitutes the SDK/transport.
use libtinkle::{Client, ClientConfig, ClientInfo, InMemoryCursorStore, ServerTrust, TrustSource};

/// Require a runner-provisioned input without echoing its value when it is missing.
fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("required external fixture input: {name}"))
}

#[tokio::test]
#[ignore = "requires externally started client-api; see docs/device-enrollment.md"]
/// Verify pinned Bootstrap initialization accepts SendCode without establishing an account principal.
async fn pinned_client_api_bootstrap_initializes_and_accepts_send_code() {
    let pin = env("TINKLE_E2E_SERVER_PIN");
    assert!(
        pin.is_ascii() && pin.len() == 64,
        "pin must be 64 hex characters"
    );
    let mut server_public_key = [0; 32];
    for (i, byte) in server_public_key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&pin[i * 2..i * 2 + 2], 16).expect("hex server pin");
    }
    let info = ClientInfo::new(
        env("TINKLE_E2E_DEVICE_MODEL"),
        env("TINKLE_E2E_PLATFORM"),
        env("TINKLE_E2E_OS_VERSION"),
        env("TINKLE_E2E_APP_VERSION"),
        env("TINKLE_E2E_APP_BUILD"),
        Default::default(),
    )
    .unwrap();
    let mut config = ClientConfig::new(env("TINKLE_E2E_HOST"), info);
    config.primary_port = env("TINKLE_E2E_PORT").parse().expect("TCP port");
    config.fallback_port = None;
    let client = Client::new(
        config,
        TrustSource::Bootstrap(ServerTrust {
            server_public_key,
            server_key_id: env("TINKLE_E2E_SERVER_KEY_ID")
                .parse()
                .expect("server key ID"),
            scope: env("TINKLE_E2E_SCOPE"),
        }),
        InMemoryCursorStore::new(),
    );
    client.connect().await.unwrap();
    // The first wire RPC is InitConnect wrapping this exact SendCode request.
    let response = client
        .auth()
        .send_code(tinkle_schema::proto::SendCodeRequest {
            phone_number: env("TINKLE_E2E_PHONE"),
            lang_code: env("TINKLE_E2E_LANG_CODE"),
        })
        .await
        .unwrap();
    assert!(
        !response.verification_id.is_empty(),
        "verification identifier required"
    );
    assert_eq!(
        client.principal().await,
        "unknown",
        "Bootstrap must not become an account session"
    );
    client.close().await;
}
