#!/usr/bin/env bash
# Regenerate with the in-workspace generator and the exact library's metadata.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:---check}"
if [[ "$MODE" != --check && "$MODE" != --write ]] || [[ $# -gt 2 ]]; then
  printf 'Usage: %s [--check|--write] [unstripped-library]\n' "$0" >&2
  exit 2
fi
LIBRARY="${2:-}"
if [[ -n "$LIBRARY" ]]; then
  LIBRARY="$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).resolve(strict=True))' "$LIBRARY")"
fi
cd "$ROOT"
TARGET_DIR="$(cargo metadata --locked --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
if [[ -z "$LIBRARY" ]]; then
  cargo rustc --locked --profile release -p tinkle-ffi --lib --crate-type cdylib
  case "$(uname -s)" in
    Darwin) LIBRARY="$TARGET_DIR/release/libtinkle_ffi.dylib" ;;
    Linux) LIBRARY="$TARGET_DIR/release/libtinkle_ffi.so" ;;
    *) printf 'error: generation currently supports macOS/Linux hosts.\n' >&2; exit 1 ;;
  esac
fi
STAGE="$(mktemp -d "${TMPDIR:-/tmp}/tinkle-bindings.XXXXXX")"
trap 'rm -rf "$STAGE"' EXIT
cargo run --locked -p tinkle-ffi --features cli --bin uniffi-bindgen -- \
  generate "$LIBRARY" --config "$ROOT/uniffi-bindgen.toml" \
  --language swift --language kotlin --no-format --out-dir "$STAGE"
python3 - "$ROOT" "$STAGE" "$MODE" <<'PY'
import difflib
from pathlib import Path
import sys

root, stage, mode = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
files = {
    "TinkleSDK.swift": "bindings/swift/TinkleSDK.swift",
    "TinkleSDKFFI.h": "bindings/swift/TinkleSDKFFI.h",
    "TinkleSDKFFI.modulemap": "bindings/swift/module.modulemap",
    "com/tinkle/sdk/tinkle_ffi.kt": "bindings/kotlin/com/tinkle/sdk/tinkle_ffi.kt",
}
different = False
for generated, relative in files.items():
    source, destination = stage / generated, root / relative
    if not source.is_file():
        raise SystemExit(f"error: generator did not produce {generated}")
    # Normalize only whitespace, never generated API behavior.
    text = "\n".join(line.rstrip() for line in source.read_text().splitlines()) + "\n"
    previous = destination.read_text() if destination.is_file() else ""
    if previous == text:
        continue
    if mode == "--write":
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text)
        print(f"Updated {relative}")
    else:
        different = True
        sys.stderr.writelines(difflib.unified_diff(previous.splitlines(True), text.splitlines(True),
                                                 fromfile=relative, tofile=f"generated/{generated}"))
if different:
    raise SystemExit("error: stale bindings; run bash scripts/generate-bindings.sh --write")
print("Bindings match the compiled FFI contract." if mode == "--check" else "Bindings regenerated.")
PY
