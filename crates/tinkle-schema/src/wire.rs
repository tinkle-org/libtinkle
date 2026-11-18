//! Bounded descriptor-driven unknown-field rejection without reserializing original request bytes.
use crate::{proto, sync::SyncError};
use prost::Message;
use prost_types::{
    field_descriptor_proto::Type, DescriptorProto, FieldDescriptorProto, FileDescriptorSet,
};

/// Validate recursive known field numbers/types while preserving byte-order and explicit presence.
pub fn validate_request(method: &str, bytes: &[u8]) -> Result<(), SyncError> {
    if bytes.len() > 262144 {
        return Err(invalid());
    }
    let descriptors =
        FileDescriptorSet::decode(proto::FILE_DESCRIPTOR_SET).map_err(|_| invalid())?;
    let method = method.strip_prefix('/').ok_or_else(invalid)?;
    let (service, name) = method.split_once('/').ok_or_else(invalid)?;
    let input = descriptors
        .file
        .iter()
        .find_map(|f| {
            f.service.iter().find_map(|s| {
                if format!(
                    "{}.{}",
                    f.package.as_deref().unwrap_or_default(),
                    s.name.as_deref().unwrap_or_default()
                ) != service
                {
                    return None;
                }
                s.method
                    .iter()
                    .find(|m| m.name.as_deref() == Some(name))
                    .and_then(|m| m.input_type.clone())
            })
        })
        .ok_or_else(invalid)?;
    validate_message(&input, bytes)
}

/// Reject unknown/reserved fields in an exact schema type before any source assembly.
pub fn validate_message(name: &str, bytes: &[u8]) -> Result<(), SyncError> {
    if bytes.len() > 262144 {
        return Err(invalid());
    }
    let descriptors =
        FileDescriptorSet::decode(proto::FILE_DESCRIPTOR_SET).map_err(|_| invalid())?;
    let mut types = std::collections::HashMap::new();
    for file in &descriptors.file {
        for message in &file.message_type {
            register(
                &mut types,
                file.package.as_deref().unwrap_or_default(),
                message,
            );
        }
    }
    // The registry descriptor can omit imported well-known types. Keep Timestamp
    // validation strict even when its descriptor is not embedded (e.g. PollSource.close_date).
    let timestamp = DescriptorProto {
        field: [(1, Type::Int64), (2, Type::Int32)]
            .into_iter()
            .map(|(number, ty)| FieldDescriptorProto {
                number: Some(number),
                r#type: Some(ty as i32),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    types
        .entry(".google.protobuf.Timestamp".into())
        .or_insert(&timestamp);
    let mut fields = 0usize;
    validate(&types, name, bytes, 0, &mut fields)
}

/// Include nested messages so unknown fields cannot hide inside a source, preview or CAS record.
fn register<'a>(
    types: &mut std::collections::HashMap<String, &'a DescriptorProto>,
    prefix: &str,
    message: &'a DescriptorProto,
) {
    let name = format!("{}.{}", prefix, message.name.as_deref().unwrap_or_default());
    types.insert(format!(".{name}"), message);
    for nested in &message.nested_type {
        register(types, &name, nested);
    }
}

/// Traverse bounded wire fields, allowing protobuf's legal packed/unpacked numeric encodings.
fn validate(
    types: &std::collections::HashMap<String, &DescriptorProto>,
    name: &str,
    mut bytes: &[u8],
    depth: usize,
    fields: &mut usize,
) -> Result<(), SyncError> {
    if depth > 32 {
        return Err(invalid());
    }
    let message = types.get(name).ok_or_else(invalid)?;
    while !bytes.is_empty() {
        *fields += 1;
        if *fields > 65536 {
            return Err(invalid());
        }
        let key = varint(&mut bytes)?;
        let number = i32::try_from(key >> 3).map_err(|_| invalid())?;
        let wire = key & 7;
        let field = message
            .field
            .iter()
            .find(|f| f.number == Some(number))
            .ok_or_else(invalid)?;
        let ty = field.r#type.ok_or_else(invalid)?;
        let expected = match ty {
            1 | 6 | 16 => 1,
            2 | 7 | 15 => 5,
            9 | 11 | 12 => 2,
            _ => 0,
        };
        let packed = wire == 2 && field.label == Some(3) && !matches!(ty, 9 | 11 | 12);
        if wire != expected && !packed {
            return Err(invalid());
        }
        match wire {
            0 => {
                varint(&mut bytes)?;
            }
            1 | 5 => {
                let n = if wire == 1 { 8 } else { 4 };
                bytes = bytes.get(n..).ok_or_else(invalid)?;
            }
            2 => {
                let n = usize::try_from(varint(&mut bytes)?).map_err(|_| invalid())?;
                let body = bytes.get(..n).ok_or_else(invalid)?;
                bytes = bytes.get(n..).ok_or_else(invalid)?;
                if ty == 11 {
                    validate(
                        types,
                        field.type_name.as_deref().ok_or_else(invalid)?,
                        body,
                        depth + 1,
                        fields,
                    )?;
                }
                if packed {
                    let mut packed = body;
                    while !packed.is_empty() {
                        match expected {
                            0 => {
                                varint(&mut packed)?;
                            }
                            1 => packed = packed.get(8..).ok_or_else(invalid)?,
                            5 => packed = packed.get(4..).ok_or_else(invalid)?,
                            _ => return Err(invalid()),
                        }
                    }
                }
            }
            _ => return Err(invalid()),
        }
    }
    Ok(())
}

/// Checked ten-byte unsigned wire integer; groups and overflowing tags cannot bypass validation.
fn varint(bytes: &mut &[u8]) -> Result<u64, SyncError> {
    let mut n = 0u64;
    for i in 0..10 {
        let b = *bytes.first().ok_or_else(invalid)?;
        *bytes = &bytes[1..];
        if i == 9 && b > 1 {
            return Err(invalid());
        }
        n |= u64::from(b & 127) << (i * 7);
        if b & 128 == 0 {
            return Ok(n);
        }
    }
    Err(invalid())
}
fn invalid() -> SyncError {
    SyncError::InvalidUpdate("invalid or unknown request field".into())
}
