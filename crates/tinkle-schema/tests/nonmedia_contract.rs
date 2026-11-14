//! Descriptor checks for explicit destructive operation identity and profile reads.
use prost::Message as _;
use prost_types::{DescriptorProto, FileDescriptorSet};

/// Resolve an application message without accepting a missing or nested substitute.
fn message<'a>(set: &'a FileDescriptorSet, name: &str) -> &'a DescriptorProto {
    set.file
        .iter()
        .filter(|f| f.package.as_deref() == Some("tinkle.v1"))
        .flat_map(|f| &f.message_type)
        .find(|m| m.name.as_deref() == Some(name))
        .unwrap()
}

/// The caller must retain the deletion UUID and distinguish progress from completion.
#[test]
fn delete_history_operation_identity_and_completion_are_explicit() {
    let set = FileDescriptorSet::decode(tinkle_schema::proto::FILE_DESCRIPTOR_SET).unwrap();
    let request = message(&set, "DeleteHistoryRequest");
    let operation = request
        .field
        .iter()
        .find(|f| f.name.as_deref() == Some("operation_id"))
        .unwrap();
    assert_eq!(operation.number, Some(6));
    assert_eq!(operation.r#type, Some(9));
    let response = message(&set, "DeleteHistoryResponse");
    for (name, number) in [
        ("operation_id", 3),
        ("completed", 4),
        ("processed_count", 5),
    ] {
        assert_eq!(
            response
                .field
                .iter()
                .find(|f| f.name.as_deref() == Some(name))
                .unwrap()
                .number,
            Some(number)
        );
    }
    assert_eq!(
        response
            .field
            .iter()
            .find(|f| f.name.as_deref() == Some("completed"))
            .unwrap()
            .proto3_optional,
        Some(true)
    );
}

/// Profile facades bind concrete request shapes and public file declarations expose no encryption mode.
#[test]
fn profile_reads_and_file_declarations_match_current_shapes() {
    let set = FileDescriptorSet::decode(tinkle_schema::proto::FILE_DESCRIPTOR_SET).unwrap();
    assert_eq!(
        message(&set, "GetUsersRequest").field[0].name.as_deref(),
        Some("users")
    );
    assert_eq!(
        message(&set, "GetUserCompleteRequest").field[0]
            .name
            .as_deref(),
        Some("user_id")
    );
    for name in ["BeginUploadRequest", "FileInfo"] {
        assert!(!message(&set, name)
            .field
            .iter()
            .any(|f| f.name.as_deref() == Some("encrypted")));
    }
}
