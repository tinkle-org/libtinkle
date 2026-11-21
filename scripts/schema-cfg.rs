//! The published pin and isolated replacement use the same compact contract.
fn main() {
    const WIRE: &str = "compact-wire-reset-v1";
    const MEDIA: &str = "singular-thumbnail-compact-wire-reset-v1";
    let keys = [
        "TINKLE_SCHEMA_CANDIDATE_WIRE_ID",
        "TINKLE_SCHEMA_CANDIDATE_ASYNC_ACTIONS",
        "TINKLE_SCHEMA_CANDIDATE_MEDIA_CONTRACT",
    ];
    const ROOM_QUERIES: &str = "TINKLE_SCHEMA_CANDIDATE_ROOM_QUERIES";
    for key in keys {
        println!("cargo:rerun-if-env-changed={key}");
    }
    let [wire, actions, media] = keys.map(std::env::var);
    let candidate = matches!(
        (wire.as_deref(), actions.as_deref(), media.as_deref()),
        (Ok(WIRE), Ok("1"), Ok(MEDIA))
    );
    let published = [&wire, &actions, &media]
        .iter()
        .all(|value| matches!(value, Err(std::env::VarError::NotPresent)));
    assert!(
        candidate || published,
        "invalid candidate contract; use schema-candidate.py --async-actions"
    );
    println!("cargo:rerun-if-env-changed={ROOM_QUERIES}");
    println!("cargo:rustc-check-cfg=cfg(tinkle_room_queries)");
    let room_queries = match std::env::var(ROOM_QUERIES).as_deref() {
        Err(std::env::VarError::NotPresent) => published,
        Ok("1") if candidate => true,
        _ => panic!(
            "invalid room query capability; use schema-candidate.py --async-actions --room-queries"
        ),
    };
    if room_queries {
        println!("cargo:rustc-cfg=tinkle_room_queries");
    }
    // Historical cfg names: these describe the schema shape, not its registry source.
    for capability in ["tinkle_schema_candidate", "tinkle_async_actions"] {
        println!("cargo:rustc-check-cfg=cfg({capability})");
        println!("cargo:rustc-cfg={capability}");
    }
    println!("cargo:rustc-env=TINKLE_SCHEMA_WIRE_ID={WIRE}");
}
