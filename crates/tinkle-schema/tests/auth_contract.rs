//! Descriptor-grounded checks of the public enrollment and initialization API.
use prost::Message;
use std::collections::BTreeSet;
use tinkle_schema::proto::FILE_DESCRIPTOR_SET;

#[test]
/// Compare every public method and request/response type with the pinned descriptor artifact.
fn canonical_method_inventory_matches_the_actual_message_artifact() {
    let descriptors = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap();
    let mut actual = BTreeSet::new();
    for file in &descriptors.file {
        if file.package.as_deref() != Some("tinkle.v1") {
            continue;
        }
        for service in &file.service {
            for method in &service.method {
                actual.insert((
                    format!(
                        "/tinkle.v1.{}/{}",
                        service.name.as_ref().unwrap(),
                        method.name.as_ref().unwrap()
                    ),
                    method
                        .input_type
                        .as_ref()
                        .unwrap()
                        .rsplit('.')
                        .next()
                        .unwrap()
                        .replace("TTL", "Ttl"),
                    method
                        .output_type
                        .as_ref()
                        .unwrap()
                        .rsplit('.')
                        .next()
                        .unwrap()
                        .replace("TTL", "Ttl"),
                ));
            }
        }
    }
    let expected: BTreeSet<_> = tinkle_schema::methods::ALL_METHODS
        .iter()
        .map(|(m, req, resp)| (m.to_string(), req.to_string(), resp.to_string()))
        .collect();
    let missing: Vec<_> = actual.difference(&expected).collect();
    let extra: Vec<_> = expected.difference(&actual).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "missing={missing:?}; extra={extra:?}"
    );
}

#[test]
/// Lock enrollment field numbers and require ordinary uint64 KeepAlive sequence fields.
fn enrollment_fields_and_capability_activation_are_exact() {
    let descriptors = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap();
    for (message, fields) in [
        ("ConfirmSessionRequest", vec![("session_id", 1)]),
        ("ConfirmSessionResponse", vec![("session", 1)]),
        ("SessionChanged", vec![("session_id", 1)]),
        ("KeepAliveRequest", vec![("sequence", 1)]),
        ("KeepAliveResponse", vec![("sequence", 1)]),
        (
            "DeviceEnrollmentReference",
            vec![
                ("session_id", 1),
                ("handle", 2),
                ("enrollment_epoch", 3),
                ("scope", 4),
                ("account_auth_epoch", 5),
            ],
        ),
        ("GetEnrollmentDispositionRequest", vec![("reference", 1)]),
        (
            "GetEnrollmentDispositionResponse",
            vec![("reference", 1), ("disposition", 2)],
        ),
        (
            "SignInRequest",
            vec![
                ("phone_number", 1),
                ("code", 2),
                ("verification_id", 3),
                ("operation_id", 4),
                ("resume_secret", 5),
            ],
        ),
        (
            "SignInResponse",
            vec![("user", 1), ("is_new_user", 3), ("enrollment", 4)],
        ),
        (
            "ActivateEnrollmentRequest",
            vec![
                ("operation_id", 1),
                ("session_id", 2),
                ("handle", 3),
                ("enrollment_epoch", 4),
                ("scope", 5),
                ("resume_secret", 6),
            ],
        ),
        (
            "DeviceEnrollment",
            vec![
                ("operation_id", 1),
                ("session_id", 2),
                ("authorization_key", 3),
                ("handle", 4),
                ("enrollment_epoch", 5),
                ("scope", 6),
                ("expires_at", 7),
                ("state", 8),
                ("user_id", 9),
                ("account_auth_epoch", 10),
            ],
        ),
        (
            "InitConnectRequest",
            vec![("client", 1), ("method", 2), ("payload", 3)],
        ),
    ] {
        let found = descriptors
            .file
            .iter()
            .flat_map(|file| &file.message_type)
            .find(|value| value.name.as_deref() == Some(message))
            .unwrap();
        let actual: Vec<_> = found
            .field
            .iter()
            .map(|f| (f.name.as_deref().unwrap(), f.number.unwrap()))
            .collect();
        assert_eq!(actual, fields, "{message}");
        if matches!(message, "KeepAliveRequest" | "KeepAliveResponse") {
            use prost_types::field_descriptor_proto::{Label, Type};
            assert_eq!(
                found.field[0].r#type,
                Some(Type::Uint64 as i32),
                "{message}"
            );
            assert_eq!(
                found.field[0].label,
                Some(Label::Optional as i32),
                "{message}"
            );
            assert!(
                !found.field[0].proto3_optional.unwrap_or(false),
                "{message}"
            );
        }
    }
}

#[test]
/// Review metadata and mutation responses come from the published artifact, not local protos.
fn session_review_types_and_field_presence_are_exact() {
    use prost_types::field_descriptor_proto::{Label, Type};
    let descriptors = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap();
    let message = |name: &str| {
        descriptors
            .file
            .iter()
            .flat_map(|f| &f.message_type)
            .find(|m| m.name.as_deref() == Some(name))
            .unwrap()
    };
    let session = message("Session");
    assert_eq!(session.field.len(), 13);
    let unconfirmed = session
        .field
        .iter()
        .find(|f| f.name.as_deref() == Some("unconfirmed"))
        .unwrap();
    assert_eq!(unconfirmed.number, Some(13));
    assert_eq!(unconfirmed.r#type, Some(Type::Bool as i32));
    assert_eq!(unconfirmed.label, Some(Label::Optional as i32));
    assert!(!unconfirmed.proto3_optional.unwrap_or(false));
    assert_eq!(
        message("ConfirmSessionResponse").field[0]
            .type_name
            .as_deref(),
        Some(".tinkle.v1.Session")
    );
    assert_eq!(
        tinkle_schema::methods::accountservice::CONFIRM_SESSION,
        "/tinkle.v1.AccountService/ConfirmSession"
    );
    let hint = tinkle_schema::proto::SessionChanged {
        session_id: "78e5c289-656b-4428-a091-2dddf6828465".into(),
    };
    assert_eq!(
        tinkle_schema::proto::SessionChanged::decode(hint.encode_to_vec().as_slice()).unwrap(),
        hint
    );
}

/// Retirement is negative-only and has no application-level query identifier.
#[test]
fn disposition_enum_and_rpc_types_are_exact() {
    let descriptors = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap();
    let reference = descriptors
        .file
        .iter()
        .flat_map(|file| &file.message_type)
        .find(|message| message.name.as_deref() == Some("DeviceEnrollmentReference"))
        .unwrap();
    use prost_types::field_descriptor_proto::Type;
    let fields: Vec<_> = reference
        .field
        .iter()
        .map(|field| (field.name.as_deref().unwrap(), field.r#type.unwrap()))
        .collect();
    assert_eq!(
        fields,
        [
            ("session_id", Type::String as i32),
            ("handle", Type::Bytes as i32),
            ("enrollment_epoch", Type::Uint64 as i32),
            ("scope", Type::Enum as i32),
            ("account_auth_epoch", Type::Uint64 as i32)
        ]
    );
    assert_eq!(
        reference.field[3].type_name.as_deref(),
        Some(".tinkle.v1.DeviceKeyScope")
    );
    let enumeration = descriptors
        .file
        .iter()
        .flat_map(|file| &file.enum_type)
        .find(|value| value.name.as_deref() == Some("DeviceEnrollmentDisposition"))
        .unwrap();
    let values: Vec<_> = enumeration
        .value
        .iter()
        .map(|value| (value.name.as_deref().unwrap(), value.number.unwrap()))
        .collect();
    assert_eq!(
        values,
        [
            ("DEVICE_ENROLLMENT_DISPOSITION_UNSPECIFIED", 0),
            ("DEVICE_ENROLLMENT_DISPOSITION_UNKNOWN", 1),
            ("DEVICE_ENROLLMENT_DISPOSITION_RETIRED", 2),
        ]
    );
    let method = descriptors
        .file
        .iter()
        .flat_map(|file| &file.service)
        .find(|service| service.name.as_deref() == Some("AuthService"))
        .unwrap()
        .method
        .iter()
        .find(|method| method.name.as_deref() == Some("GetEnrollmentDisposition"))
        .unwrap();
    assert_eq!(
        method.input_type.as_deref(),
        Some(".tinkle.v1.GetEnrollmentDispositionRequest")
    );
    assert_eq!(
        method.output_type.as_deref(),
        Some(".tinkle.v1.GetEnrollmentDispositionResponse")
    );
    assert!(!method.client_streaming.unwrap_or(false));
    assert!(!method.server_streaming.unwrap_or(false));
}
