#!/usr/bin/env bash
# Build all supported iOS slices from the locked workspace and package UniFFI.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export IPHONEOS_DEPLOYMENT_TARGET=15.0
export COPYFILE_DISABLE=1

if [[ "$(uname -s)" != Darwin ]]; then
  printf 'error: iOS packaging requires macOS and Xcode.\n' >&2
  exit 1
fi
for tool in cargo python3 xcrun xcodebuild ditto; do
  command -v "$tool" >/dev/null || { printf 'error: missing %s\n' "$tool" >&2; exit 1; }
done
if [[ $# -ne 0 ]]; then
  printf 'Usage: %s (configure SDK_OUTPUT_DIR and optionally CARGO_TARGET_DIR)\n' "$0" >&2
  exit 2
fi

# Resolve relative caller paths before changing working directory.
OUTPUT="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "${SDK_OUTPUT_DIR:-$ROOT/target/sdk}")"
cd "$ROOT"
TARGET_DIR="$(cargo metadata --locked --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
STAGE="$(mktemp -d "${TMPDIR:-/tmp}/tinkle-ios.XXXXXX")"
trap 'rm -rf "$STAGE"' EXIT

for name in TinkleSDK.swift TinkleSDKFFI.h module.modulemap; do
  [[ -s "$ROOT/bindings/swift/$name" ]] || {
    printf 'error: missing bindings/swift/%s; run scripts/generate-bindings.sh --write first.\n' "$name" >&2
    exit 1
  }
done
xcodebuild -version
xcode-select -p
DEVICE_SDK="$(xcrun --sdk iphoneos --show-sdk-path)"
SIMULATOR_SDK="$(xcrun --sdk iphonesimulator --show-sdk-path)"
for target in aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios; do
  sdk="$SIMULATOR_SDK"
  [[ "$target" != aarch64-apple-ios ]] || sdk="$DEVICE_SDK"
  printf '\nBuilding %s (minimum iOS %s)\n' "$target" "$IPHONEOS_DEPLOYMENT_TARGET"
  SDKROOT="$sdk" cargo rustc --locked --profile ios-release --target "$target" \
    --crate-type staticlib -p tinkle-ffi --lib -- --print=native-static-libs
  [[ -s "$TARGET_DIR/$target/ios-release/libtinkle_ffi.a" ]] || {
    printf 'error: Cargo did not produce the expected static library for %s\n' "$target" >&2
    exit 1
  }
done

# Compare the checked-in bindings against this build, without modifying them.
bash "$ROOT/scripts/generate-bindings.sh" --check "$TARGET_DIR/aarch64-apple-ios/ios-release/libtinkle_ffi.a"
mkdir -p "$STAGE/device/headers" "$STAGE/simulator/headers" "$STAGE/bindings"
for platform in device simulator; do
  cp "$ROOT/bindings/swift/TinkleSDKFFI.h" "$ROOT/bindings/swift/module.modulemap" "$STAGE/$platform/headers/"
done
cp "$TARGET_DIR/aarch64-apple-ios/ios-release/libtinkle_ffi.a" "$STAGE/device/"
# Never combine device arm64 with simulator arm64.
xcrun lipo -create "$TARGET_DIR/aarch64-apple-ios-sim/ios-release/libtinkle_ffi.a" \
  "$TARGET_DIR/x86_64-apple-ios/ios-release/libtinkle_ffi.a" -output "$STAGE/simulator/libtinkle_ffi.a"
xcodebuild -create-xcframework \
  -library "$STAGE/device/libtinkle_ffi.a" -headers "$STAGE/device/headers" \
  -library "$STAGE/simulator/libtinkle_ffi.a" -headers "$STAGE/simulator/headers" \
  -output "$STAGE/TinkleSDKFFI.xcframework"
bash "$ROOT/scripts/verify-ios.sh" "$STAGE/TinkleSDKFFI.xcframework"

# Catch import/type-generation issues at the declared minimum on all slices.
for entry in 'arm64 iphoneos' 'arm64 iphonesimulator' 'x86_64 iphonesimulator'; do
  read -r arch sdk_name <<< "$entry"
  triple="$arch-apple-ios15.0"
  [[ "$sdk_name" != iphonesimulator ]] || triple="$triple-simulator"
  xcrun --sdk "$sdk_name" swiftc -typecheck -swift-version 5 \
    -target "$triple" -sdk "$(xcrun --sdk "$sdk_name" --show-sdk-path)" \
    -I "$STAGE/device/headers" "$ROOT/bindings/swift/TinkleSDK.swift"
done

# Normalize mtimes for byte-reproducible ZIPs (match Android's fixed 1980 timestamps).
# ditto stores build-time mtimes from the cp'd staging files, so every run would
# otherwise produce a different SHA-256 and fail the immutable-asset digest check.
find "$STAGE/TinkleSDKFFI.xcframework" -exec touch -h -t 198001010000 {} +
ditto -c -k --norsrc --keepParent "$STAGE/TinkleSDKFFI.xcframework" "$STAGE/TinkleSDKFFI.xcframework.zip"
cp "$ROOT/bindings/swift/TinkleSDK.swift" "$ROOT/bindings/swift/TinkleSDKFFI.h" \
  "$ROOT/bindings/swift/module.modulemap" "$STAGE/bindings/"
find "$STAGE/bindings" -exec touch -h -t 198001010000 {} +
# No --keepParent here: the three generated files must be at the ZIP root.
ditto -c -k --norsrc "$STAGE/bindings" "$STAGE/swift-bindings.zip"
python3 - "$STAGE" <<'PY'
import pathlib, sys, zipfile
root = pathlib.Path(sys.argv[1])
for name in ("TinkleSDKFFI.xcframework.zip", "swift-bindings.zip"):
    with zipfile.ZipFile(root / name) as archive:
        names = archive.namelist()
        if any(p.startswith("/") or ".." in pathlib.PurePosixPath(p).parts or
               "__MACOSX" in pathlib.PurePosixPath(p).parts or
               any(part.startswith("._") for part in pathlib.PurePosixPath(p).parts) for p in names):
            raise SystemExit(f"error: unsafe path or AppleDouble metadata in {name}")
        if name == "swift-bindings.zip" and set(names) != {"TinkleSDK.swift", "TinkleSDKFFI.h", "module.modulemap"}:
            raise SystemExit("error: Swift bindings archive must contain exactly three root files")
PY
mkdir -p "$OUTPUT"
for name in TinkleSDKFFI.xcframework.zip swift-bindings.zip; do
  # Replace only these output files. Never delete an existing output directory.
  [[ ! -d "$OUTPUT/$name" ]] || { printf 'error: output is a directory: %s\n' "$OUTPUT/$name" >&2; exit 1; }
  mv -f "$STAGE/$name" "$OUTPUT/$name"
  printf 'Created %s\n' "$OUTPUT/$name"
done
printf 'Artifact and three-target Swift typechecks passed. Simulator runtime tests are a separate check.\n'
