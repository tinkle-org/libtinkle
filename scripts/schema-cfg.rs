//! The published pin and isolated replacement use the same compact contract.
fn main() {
    const WIRE: &str = "compact-wire-reset-v1";
    const MEDIA: &str = "singular-thumbnail-compact-wire-reset-v1";
    let keys = [
        "TINKLE_SCHEMA_CANDIDATE_WIRE_ID",
        "TINKLE_SCHEMA_CANDIDATE_ASYNC_ACTIONS",
        "TINKLE_SCHEMA_CANDIDATE_MEDIA_CONTRACT",
    ];
    for key in keys {
        println!("cargo:rerun-if-env-changed={key}");
    }
    let [wire, actions, media] = keys.map(std::env::var);
    assert!(
        matches!(
            (wire.as_deref(), actions.as_deref(), media.as_deref()),
            (Ok(WIRE), Ok("1"), Ok(MEDIA))
                | (
                    Err(std::env::VarError::NotPresent),
                    Err(std::env::VarError::NotPresent),
                    Err(std::env::VarError::NotPresent)
                )
        ),
        "invalid candidate contract; use schema-candidate.py --async-actions"
    );
    // Historical cfg names: these describe the schema shape, not its registry source.
    for capability in ["tinkle_schema_candidate", "tinkle_async_actions"] {
        println!("cargo:rustc-check-cfg=cfg({capability})");
        println!("cargo:rustc-cfg={capability}");
    }
    println!("cargo:rustc-env=TINKLE_SCHEMA_WIRE_ID={WIRE}");
}
