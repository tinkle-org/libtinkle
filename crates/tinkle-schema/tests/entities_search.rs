//! Wire compatibility for shared entities and source-qualified full Search pages.
use prost::Message as _;
use tinkle_schema::proto as p;

/// Neutral entity/reaction names preserve messaging field numbers and exact captured versions.
#[test]
fn entities_reactions_and_full_search_roundtrip() {
    let entity = p::Entity {
        r#type: p::EntityType::CustomEmoji as i32,
        offset: 1,
        length: 2,
        custom_emoji_id: 99,
        fallback_text: Some("🙂".into()),
        asset_version: Some(u64::MAX),
        ..Default::default()
    };
    let bytes = entity.encode_to_vec();
    // Field 7 remains a varint asset ID, 8 UTF-8 fallback, and 9 an exact uint64 version.
    assert!(bytes.windows(2).any(|w| w == [0x38, 99]));
    assert!(bytes.contains(&0x42));
    assert!(bytes.contains(&0x48));
    assert_eq!(p::Entity::decode(bytes.as_slice()).unwrap(), entity);
    let reaction = p::Reaction {
        emoticon: "🙂".into(),
        custom_emoji_id: 99,
        asset_version: Some(u64::MAX),
    };
    let m = p::Message {
        message_id: 1,
        message: "a🙂b".into(),
        entities: vec![entity.clone()],
        reply_to: Some(p::MessageReplyHeader {
            quote_text: "a🙂b".into(),
            quote_entities: vec![entity],
            ..Default::default()
        }),
        reactions: Some(p::MessageReactions {
            results: vec![p::ReactionCount {
                reaction: Some(reaction),
                ..Default::default()
            }],
            ..Default::default()
        }),
        body_revision: Some(11),
        ..Default::default()
    };
    let response = p::SearchResponse {
        messages: vec![m],
        source: p::SearchSource::Cache as i32,
        coverage: p::SearchCoverage::CachedSubset as i32,
        more_available: Some(false),
        ..Default::default()
    };
    assert_eq!(
        p::SearchResponse::decode(response.encode_to_vec().as_slice()).unwrap(),
        response
    );
    for (value, name) in [
        (0, "SEARCH_SOURCE_UNSPECIFIED"),
        (1, "SEARCH_SOURCE_SERVER"),
        (2, "SEARCH_SOURCE_CACHE"),
    ] {
        assert_eq!(
            p::SearchSource::try_from(value).unwrap().as_str_name(),
            name
        );
    }
    for (value, name) in [
        (0, "SEARCH_COVERAGE_UNSPECIFIED"),
        (1, "SEARCH_COVERAGE_INDEXED"),
        (2, "SEARCH_COVERAGE_CACHED_SUBSET"),
    ] {
        assert_eq!(
            p::SearchCoverage::try_from(value).unwrap().as_str_name(),
            name
        );
    }
}
