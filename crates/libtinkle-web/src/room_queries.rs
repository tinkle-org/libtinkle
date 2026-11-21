//! Offline validation of room query protobuf bytes; never opens a connection.

use prost::Message;
use tinkle_schema::room_queries;
use wasm_bindgen::prelude::*;

/// Bound the byte input before allocating a decoded message.
fn decode<T: Message + Default>(payload: &[u8]) -> Result<T, &'static str> {
    if payload.len() > 262144 {
        return Err("room query payload exceeds validation budget");
    }
    T::decode(payload).map_err(|_| "invalid room query protobuf")
}

/// Check serialized `GetRoomMembersRequest` bytes, without sending them.
#[wasm_bindgen]
pub fn validate_room_members_request(payload: &[u8]) -> Result<(), JsValue> {
    room_queries::validate_members_request(&decode(payload).map_err(JsValue::from_str)?)
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Check serialized `GetRoomMembersResponse` bytes against their original request.
#[wasm_bindgen]
pub fn validate_room_members_page(request: &[u8], response: &[u8]) -> Result<(), JsValue> {
    room_queries::validate_members_page(
        &decode(request).map_err(JsValue::from_str)?,
        &decode(response).map_err(JsValue::from_str)?,
    )
    .map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Check serialized `GetRoomOnlineCountRequest` bytes, without sending them.
#[wasm_bindgen]
pub fn validate_room_online_count_request(payload: &[u8]) -> Result<(), JsValue> {
    room_queries::validate_online_count_request(&decode(payload).map_err(JsValue::from_str)?)
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Check serialized `GetRoomOnlineCountResponse` bytes, accepting an absent sample.
#[wasm_bindgen]
pub fn validate_room_online_count_response(payload: &[u8]) -> Result<(), JsValue> {
    room_queries::validate_online_count_response(&decode(payload).map_err(JsValue::from_str)?)
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinkle_schema::proto as p;

    #[test]
    fn offline_exports_validate_room_payloads_without_network() {
        let target = Some(p::RoomQueryTarget {
            room: Some(p::room_query_target::Room::ChannelId(i64::MAX)),
        });
        let page = p::GetRoomMembersRequest {
            target,
            ..Default::default()
        }
        .encode_to_vec();
        assert!(validate_room_members_request(&page).is_ok());
        assert!(validate_room_members_page(&page, &[]).is_ok());
        let count = p::GetRoomOnlineCountRequest { target }.encode_to_vec();
        assert!(validate_room_online_count_request(&count).is_ok());
        assert!(validate_room_online_count_response(&[]).is_ok());
        assert!(decode::<p::GetRoomMembersRequest>(&[0xff]).is_err());
        assert!(decode::<p::GetRoomMembersRequest>(&vec![0; 262145]).is_err());
        assert!(!crate::network_supported());
    }
}
