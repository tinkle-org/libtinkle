#!/usr/bin/env bash
# Verify the actual XCFramework, not assumptions about Xcode slice names.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -gt 1 ]]; then
  printf 'Usage: %s [TinkleSDKFFI.xcframework | TinkleSDKFFI.xcframework.zip]\n' "$0" >&2
  exit 2
fi
python3 - "${1:-${SDK_OUTPUT_DIR:-$ROOT/target/sdk}/TinkleSDKFFI.xcframework.zip}" "$ROOT/bindings/swift" <<'PY'
import pathlib
import plistlib
import re
import subprocess
import sys
import tempfile
import zipfile

def require(condition, message):
    if not condition:
        raise SystemExit("error: " + message)

def run(*args):
    return subprocess.check_output(args, text=True)

source = pathlib.Path(sys.argv[1]).resolve()
bindings = pathlib.Path(sys.argv[2])
with tempfile.TemporaryDirectory(prefix="tinkle-verify-ios-") as staging:
    if source.is_file():
        with zipfile.ZipFile(source) as archive:
            for item in archive.infolist():
                path = pathlib.PurePosixPath(item.filename)
                require(not path.is_absolute() and ".." not in path.parts, "unsafe ZIP path")
                require("__MACOSX" not in path.parts and not any(p.startswith("._") for p in path.parts), "AppleDouble ZIP metadata")
                require((item.external_attr >> 16) & 0o170000 != 0o120000, "symlinks are not allowed in the ZIP")
            archive.extractall(staging)
        framework = pathlib.Path(staging) / "TinkleSDKFFI.xcframework"
        require({p.name for p in pathlib.Path(staging).iterdir()} == {framework.name}, "unexpected XCFramework ZIP root")
    else:
        framework = source
    require(framework.is_dir(), f"XCFramework not found: {source}")
    with (framework / "Info.plist").open("rb") as handle:
        info = plistlib.load(handle)
    slices = info.get("AvailableLibraries", [])
    require(len(slices) == 2, "expected exactly device and universal simulator slices")
    seen = set()
    reference_exports = None
    for item in slices:
        require(item.get("SupportedPlatform") == "ios", "non-iOS slice")
        variant = item.get("SupportedPlatformVariant", "device")
        require(variant in {"device", "simulator"} and variant not in seen, "unexpected/duplicate slice variant")
        seen.add(variant)
        expected_arches = {"arm64"} if variant == "device" else {"arm64", "x86_64"}
        require(set(item["SupportedArchitectures"]) == expected_arches, f"wrong declared architectures for {variant}")
        directory = framework / item["LibraryIdentifier"]
        library = directory / item["LibraryPath"]
        require(library.name == "libtinkle_ffi.a", "unexpected static library name")
        require(set(run("xcrun", "lipo", "-archs", str(library)).split()) == expected_arches, f"wrong actual architectures for {variant}")
        headers = directory / item["HeadersPath"]
        for name in ("TinkleSDKFFI.h", "module.modulemap"):
            require((headers / name).read_bytes() == (bindings / name).read_bytes(), f"{variant} header differs: {name}")
        modulemap = (headers / "module.modulemap").read_text()
        require(re.search(r"\bmodule\s+TinkleSDKFFI\s*\{", modulemap), "C module must be TinkleSDKFFI")
        header = (headers / "TinkleSDKFFI.h").read_text()
        declared = set(re.findall(r"\b((?:uniffi_|ffi_)\w+)\s*\(", header))
        require("uniffi_tinkle_ffi_fn_func_tinkle_ffi_init" in declared, "missing exported runtime initializer")
        for arch in sorted(expected_arches):
            # Rust's compiler_builtins contains LLVM bitcode newer than Apple's
            # reader. Our public ABI is native Mach-O; don't ask nm to decode
            # unrelated compiler bitcode while verifying its exported symbols.
            symbols = run("xcrun", "nm", "--no-llvm-bc", "--quiet", "-arch", arch, "-gU", str(library))
            exports = set(re.findall(r"\b_((?:uniffi_|ffi_)\w+)\s*$", symbols, re.M))
            require(declared <= exports, f"{variant}/{arch}: missing header symbols: {sorted(declared - exports)}")
            if reference_exports is None:
                reference_exports = exports
            require(exports == reference_exports, f"{variant}/{arch}: target-dependent native API")
            commands = run("xcrun", "otool", "-arch", arch, "-l", str(library))
            blocks = re.split(r"(?=\s*cmd LC_)", commands)
            minimums = []
            platforms = []
            for block in blocks:
                if re.search(r"\bcmd LC_BUILD_VERSION\b", block):
                    platform = re.search(r"\bplatform\s+(\S+)", block)
                    minimum = re.search(r"\bminos\s+([\d.]+)", block)
                    require(platform is not None and minimum is not None, "incomplete LC_BUILD_VERSION")
                    platforms.append(platform.group(1).upper())
                    minimums.append(minimum.group(1))
                elif re.search(r"\bcmd LC_VERSION_MIN_", block):
                    require("LC_VERSION_MIN_IPHONEOS" in block, f"{variant}/{arch}: wrong legacy platform")
                    require(variant == "device" or arch == "x86_64", "ambiguous arm64 simulator legacy platform")
                    minimum = re.search(r"\bversion\s+([\d.]+)", block)
                    require(minimum is not None, "missing legacy minimum OS")
                    minimums.append(minimum.group(1))
            accepted = {"2", "IOS"} if variant == "device" else {"7", "IOSSIMULATOR"}
            require(platforms and all(p in accepted for p in platforms), f"{variant}/{arch}: incorrect Mach-O platform(s): {set(platforms)}")
            require(minimums, "no Mach-O deployment information")
            for minimum in minimums:
                version = tuple((list(map(int, minimum.split("."))) + [0, 0])[:3])
                require(version <= (15, 0, 0), f"{variant}/{arch}: object requires iOS {minimum}, exceeds 15.0")
            print(f"PASS {variant}/{arch}: platform verified, {len(minimums)} object minimums <= 15.0, {len(exports)} matching FFI exports")
    require(seen == {"device", "simulator"}, "missing platform slice")
print("XCFramework structure, headers, architectures, platforms, minimum OS, and FFI exports verified.")
PY
