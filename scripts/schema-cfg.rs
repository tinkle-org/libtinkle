//! Isolated wire identity is independent of optional asynchronous application actions.
fn main() {
    println!("cargo:rustc-check-cfg=cfg(tinkle_schema_candidate)");
    println!("cargo:rustc-check-cfg=cfg(tinkle_async_actions)");
    println!("cargo:rerun-if-env-changed=TINKLE_SCHEMA_CANDIDATE_WIRE_ID");
    println!("cargo:rerun-if-env-changed=TINKLE_SCHEMA_CANDIDATE_ASYNC_ACTIONS");
    println!("cargo:rerun-if-env-changed=TINKLE_SCHEMA_CANDIDATE_MEDIA_CONTRACT");
    let wire = std::env::var("TINKLE_SCHEMA_CANDIDATE_WIRE_ID");
    let candidate = match wire.as_deref() {
        Err(std::env::VarError::NotPresent) => false,
        Ok("compact-wire-reset-v1") => true,
        _ => panic!("invalid candidate wire identity; use schema-candidate.py"),
    };
    let actions = match std::env::var("TINKLE_SCHEMA_CANDIDATE_ASYNC_ACTIONS").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("0") => false,
        Ok("1") => true,
        _ => panic!("candidate async-actions must be 0 or 1"),
    };
    if actions {
        assert!(
            candidate,
            "candidate actions require an explicit reset wire identity"
        );
        assert_eq!(
            std::env::var("TINKLE_SCHEMA_CANDIDATE_MEDIA_CONTRACT").as_deref(),
            Ok("singular-thumbnail-compact-wire-reset-v1"),
            "candidate actions require the reset media contract"
        );
        println!("cargo:rustc-cfg=tinkle_async_actions");
    } else {
        assert!(
            matches!(
                std::env::var("TINKLE_SCHEMA_CANDIDATE_MEDIA_CONTRACT").as_deref(),
                Err(std::env::VarError::NotPresent) | Ok("")
            ),
            "candidate media contract requires async-actions"
        );
    }
    // The reset removed variant fields still used by the nonasync runtime adapter.
    if candidate && !actions {
        assert!(
            !matches!(
                std::env::var("CARGO_PKG_NAME").as_deref(),
                Ok("libtinkle" | "tinkle-ffi")
            ),
            "nonasync reset runtime is unsupported; use schema-candidate.py --async-actions"
        );
    }
    if candidate {
        println!("cargo:rustc-cfg=tinkle_schema_candidate");
    }
    println!(
        "cargo:rustc-env=TINKLE_SCHEMA_WIRE_ID={}",
        if candidate {
            "compact-wire-reset-v1"
        } else {
            "published-media-v1"
        }
    );
}
