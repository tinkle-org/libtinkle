## Summary

<!-- What does this PR do, and why? Keep it to 2-5 sentences for reviewers. -->

## Related issues

<!-- Link tracking issues. Use `Closes #123` / `Fixes #123` so merge auto-closes. -->
<!-- e.g. Closes #123 -->

## Type of change

<!-- Check all that apply. -->

- [ ] Bug fix (non-breaking)
- [ ] New feature (non-breaking)
- [ ] Breaking change (major version / public API / wire behaviour)
- [ ] Protocol change (Syntra framing, handshake, ids, reconnect, realtime grammar)
- [ ] Platform binding change (UniFFI Swift/Kotlin, wasm-bindgen JS)
- [ ] Docs / examples only
- [ ] CI / tooling / deps only

## Crates / areas touched

<!-- Check all that apply. Apps must only depend on `libtinkle` / `tinkle-ffi` / `libtinkle-web`. -->

- [ ] `syntra-core` (codec, state, handshake, ids, pending, timeouts, reconnect)
- [ ] `syntra-ws` (`native` tokio-tungstenite / `wasm` ws_stream_wasm)
- [ ] `tinkle-schema` (methods, realtime validation)
- [ ] `libtinkle` (public `Client`, facades, `CursorStore`)
- [ ] `tinkle-ffi` (UniFFI — iOS Swift / Android Kotlin)
- [ ] `libtinkle-web` (wasm-bindgen — browser/WASM)
- [ ] Docs (`docs/`, `README.md`, examples)
- [ ] CI / release (`.github/workflows`, `release-plz`, BSR pinning)

## Platform impact

<!-- This SDK ships one logical runtime to iOS, Android, and Web. Call out per-target behaviour. -->

- [ ] No platform-specific behaviour (pure Rust core)
- [ ] iOS (Swift via UniFFI: runtime threading, `Task.cancel()`, Core Data/GRDB/Keychain store, APNs `sync_after_wake`)
- [ ] Android (Kotlin via UniFFI/JNI: coroutines, Room/EncryptedSharedPreferences, FCM/WorkManager/Doze)
- [ ] Web / WASM (wasm-bindgen: `spawn_local`, `ws_stream_wasm`, IndexedDB cursors, no `Authorization` header, no secrets in `localStorage`)
- [ ] `#[cfg(target_arch = "wasm32")]` boundary preserved (no browser APIs in native, no `std::net`/raw sockets in `wasm.rs`)

### Platform notes

<!-- e.g. N/A, or: iOS suspend/resume verified on simulator; Android Doze path; Chrome/Safari WASM smoke test. -->

## Protocol / compatibility

- [ ] No Syntra wire change (frame, handshake, `request_id`, `RpcResult`, notifications, reconnect)
- [ ] Syntra change is spec-aligned and covered by `CONF-CORE-*` vectors in `syntra-core/tests/conformance.rs`
- [ ] Tinkle app change keeps `tinkle-schema` single-decoder (`RealtimeNotification`, complete-batch grammar, cursor rules)
- [ ] No change to public API, or public API change is documented below
- [ ] BSR / `tinkle-proto` pin unchanged, or `PROTO_PINNING.md` updated with `buf registry sdk version` output

## Security considerations

<!-- See `docs/security.md`. Credential / TLS regressions must block merge. -->

- [ ] No secrets in logs, errors, panics, `Debug`, URLs, query, headers, or event callbacks
- [ ] `Credential` still redacted + `zeroize` on drop, cloned only for handshake write
- [ ] No `ClientHello` / raw `Frame` bytes logged (prost `Debug` can leak)
- [ ] Production path still requires `wss://` (no downgrade to `ws://`, no new `allow_insecure` default)
- [ ] No new persistence of session tokens (Keychain/Keystore/EncryptedSharedPreferences only — never cleartext, `localStorage`, or URL params)

If any box cannot be checked, explain:

## How tested

<!-- Reviewers rely on this. Paste commands + results. CI runs fmt, clippy (ubuntu+macos), test (ubuntu+macos), wasm check, msrv. -->

```sh
# paste actual runs, e.g.:
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features --no-deps -- -D warnings
cargo test --locked --workspace --all-features --verbose
python3 -m unittest discover -s tests -p 'test_sdk_release.py' -v
bash scripts/generate-bindings.sh --check
cargo check --locked -p syntra-ws -p libtinkle-web --target wasm32-unknown-unknown
```

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --locked --workspace --all-targets --all-features --no-deps -- -D warnings`
- [ ] `cargo test --locked --workspace --all-features --verbose` (incl. `client_integration` live mock gateway if touched)
- [ ] `python3 -m unittest discover -s tests -p 'test_sdk_release.py' -v` (release-test)
- [ ] `bash scripts/generate-bindings.sh --check` (bindings parity, if `tinkle-ffi` touched)
- [ ] `cargo check --locked -p syntra-ws -p libtinkle-web --target wasm32-unknown-unknown` (if `syntra-ws` / web / `cfg(wasm32)` touched)
- [ ] UniFFI bindings regen / Swift+Kotlin smoke (if `tinkle-ffi` touched)
- [ ] WASM browser smoke (if `libtinkle-web` touched)

### Test evidence

<!-- Paste relevant output, gateway logs, simulator/emulator/browser versions. Redact tokens. -->

## Breaking changes / migration

<!-- If none, write `None`. If yes: what breaks (Rust / Swift / Kotlin / JS), and migration steps. -->

## Checklist

- [ ] I reviewed my own diff first (typos, debug leftovers, redacted secrets)
- [ ] Small, focused PR — one purpose, no unrelated drive-bys
- [ ] Tests added/updated for behaviour changes (`CONF-CORE-*`, realtime vectors, or `client_integration`)
- [ ] Docs updated (`docs/`, `README.md`, rustdoc) where behaviour or platform guidance changed
- [ ] Commit messages are meaningful (squash-safe)
- [ ] Linked issues with `Closes`/`Fixes` where applicable
- [ ] Requested review from code owners / platform owners (iOS / Android / Web as needed)
