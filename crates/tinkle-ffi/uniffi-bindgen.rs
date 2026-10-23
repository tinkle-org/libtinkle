//! Binding-generator entry point. Does not ship in the library.
//!
//! Generate Swift/Kotlin bindings with e.g.:
//! ```sh
//! bash scripts/generate-bindings.sh --write
//! ```

fn main() {
    uniffi::uniffi_bindgen_main();
}
