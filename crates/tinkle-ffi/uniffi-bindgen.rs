//! Binding-generator entry point. Does not ship in the library.
//!
//! Generate Swift/Kotlin bindings with e.g.:
//! ```sh
//! cargo run -p tinkle-ffi --bin uniffi-bindgen -- \
//!   generate --library target/debug/libtinkle_ffi.dylib \
//!   --language swift --out-dir out/swift
//! ```

fn main() {
    uniffi::uniffi_bindgen_main();
}
