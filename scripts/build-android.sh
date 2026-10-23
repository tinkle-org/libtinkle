#!/usr/bin/env bash
# Release builds always include the complete ABI matrix. --abis is local-only.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 - "$ROOT" "$@" <<'PY'
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import zipfile

root = Path(sys.argv[1])
parser = argparse.ArgumentParser(description="Build locked Android cdylibs with NDK r30")
parser.add_argument("--abis", help="explicit comma-separated development subset; forbidden in CI/releases")
args = parser.parse_args(sys.argv[2:])
matrix = {
    "arm64-v8a": ("aarch64-linux-android", "aarch64-linux-android"),
    "armeabi-v7a": ("armv7-linux-androideabi", "armv7a-linux-androideabi"),
    "x86_64": ("x86_64-linux-android", "x86_64-linux-android"),
    "x86": ("i686-linux-android", "i686-linux-android"),
}
abis = args.abis.split(",") if args.abis else list(matrix)
if not abis or len(set(abis)) != len(abis) or any(a not in matrix for a in abis):
    parser.error("--abis must select distinct ABIs from " + ",".join(matrix))
if args.abis and any(os.environ.get(v, "").lower() not in ("", "0", "false")
                     for v in ("CI", "SDK_RELEASE", "GITHUB_ACTIONS")):
    parser.error("development ABI subsets are forbidden in CI/releases")
ndk_version = "30.0.16248370"
sdk = Path(os.environ.get("ANDROID_HOME", os.environ.get("ANDROID_SDK_ROOT", str(Path.home() / "Library/Android/sdk"))))
ndk = Path(os.environ.get("ANDROID_NDK_HOME", str(sdk / "ndk" / ndk_version)))
if not (ndk / "source.properties").is_file():
    sys.exit(f"error: install ndk;{ndk_version} with sdkmanager; set ANDROID_HOME or ANDROID_NDK_HOME")
properties = {k.strip(): v.strip() for k, v in
              (line.split("=", 1) for line in (ndk / "source.properties").read_text().splitlines() if "=" in line)}
if properties.get("Pkg.Revision") != ndk_version:
    sys.exit(f"error: required NDK {ndk_version}, found {properties.get('Pkg.Revision')}")
host = {"Darwin": "darwin-x86_64", "Linux": "linux-x86_64"}.get(platform.system())
if not host:
    sys.exit("error: run Android packaging on macOS or Linux")
tools = ndk / "toolchains/llvm/prebuilt" / host / "bin"
platforms = json.loads((ndk / "meta/platforms.json").read_text())
abi_meta = json.loads((ndk / "meta/abis.json").read_text())
min_sdk = 24
if int(platforms["min"]) > min_sdk:
    sys.exit("error: NDK API floor exceeds minSdk 24")
env = os.environ.copy()
env.pop("RUSTFLAGS", None)
env.pop("CARGO_ENCODED_RUSTFLAGS", None)
# Target-specific environment flags also override the checked-in Android flags.
for target, _ in matrix.values():
    env.pop("CARGO_TARGET_" + target.upper().replace("-", "_") + "_RUSTFLAGS", None)
output = Path(env.get("SDK_OUTPUT_DIR", str(root / "target/sdk"))).resolve()

def run(*command, capture=False, environment=env):
    print("+ " + " ".join(map(str, command)), flush=True)
    return subprocess.run(list(map(str, command)), cwd=root, env=environment, check=True,
                          text=True, stdout=subprocess.PIPE if capture else None).stdout

metadata = json.loads(run("cargo", "metadata", "--locked", "--no-deps", "--format-version", "1", capture=True))
target_dir = Path(metadata["target_directory"])
bindings = root / "bindings/kotlin/com/tinkle/sdk/tinkle_ffi.kt"
if not bindings.is_file() or not (root / "scripts/generate-bindings.sh").is_file():
    sys.exit("error: generate checked-in Kotlin bindings with scripts/generate-bindings.sh --write first")
with tempfile.TemporaryDirectory(prefix="tinkle-android-") as temporary:
    stage = Path(temporary)
    records = {}
    for abi in abis:
        target, driver = matrix[abi]
        floor = int(abi_meta[abi].get("min_os_version", platforms["min"]))
        if floor > min_sdk:
            sys.exit(f"error: {abi} NDK API floor {floor} exceeds minSdk 24")
        api = min(max(min_sdk, floor), int(platforms["max"]))
        cc = tools / f"{driver}{api}-clang"
        cxx = tools / f"{driver}{api}-clang++"
        if not cc.is_file():
            sys.exit(f"error: missing NDK driver {cc}")
        abi_env = env.copy()
        key = target.upper().replace("-", "_")
        abi_env[f"CARGO_TARGET_{key}_LINKER"] = str(cc)
        # cc-rs recognizes both forms; avoid any host compiler leaking into a cross build.
        for suffix in (target, target.replace("-", "_")):
            for variable, value in (("CC", cc), ("CXX", cxx), ("AR", tools / "llvm-ar"), ("RANLIB", tools / "llvm-ranlib")):
                abi_env[f"{variable}_{suffix}"] = str(value)
        run("cargo", "rustc", "--locked", "--profile", "android-release", "--target", target,
            "--crate-type", "cdylib", "-p", "tinkle-ffi", "--lib", environment=abi_env)
        library = target_dir / target / "android-release/libtinkle_ffi.so"
        # Bindgen must inspect the unstripped input on EVERY shipping target.
        run("bash", root / "scripts/generate-bindings.sh", "--check", library)
        shipped = stage / "jniLibs" / abi / "libtinkle_ffi.so"
        symbols = stage / "symbols" / abi / "libtinkle_ffi.so.dbg"
        shipped.parent.mkdir(parents=True)
        symbols.parent.mkdir(parents=True)
        run(tools / "llvm-objcopy", "--only-keep-debug", library, symbols)
        if ".debug_line" not in run(tools / "llvm-readelf", "--sections", "--wide", symbols, capture=True):
            sys.exit(f"error: no line tables retained for {abi}; android-release must use debug=line-tables-only and strip=none")
        shutil.copyfile(library, shipped)
        run(tools / "llvm-strip", "--strip-unneeded", shipped)
        run(tools / "llvm-objcopy", f"--add-gnu-debuglink={symbols}", shipped)
        def build_id(path):
            import re
            notes = run(tools / "llvm-readelf", "-n", path, capture=True)
            ids = re.findall(r"Build ID: ([0-9a-f]+)", notes)
            if len(ids) != 1 or len(ids[0]) != 40:
                sys.exit(f"error: expected a SHA1 GNU build ID in {path}")
            return ids[0]
        identity = build_id(library)
        if build_id(shipped) != identity or build_id(symbols) != identity:
            sys.exit(f"error: mismatched shipped/debug build IDs for {abi}")
        records[abi] = {"target": target, "api": api, "build_id": identity,
                        "bytes": shipped.stat().st_size,
                        "sha256": hashlib.sha256(shipped.read_bytes()).hexdigest()}
    verify_args = ["bash", root / "scripts/verify-android.sh", stage / "jniLibs"]
    if args.abis:
        verify_args += ["--abis", args.abis]
    verify_env = env.copy()
    verify_env["ANDROID_NDK_HOME"] = str(ndk)
    run(*verify_args, environment=verify_env)
    version = next(p["version"] for p in metadata["packages"] if p["name"] == "tinkle-ffi")
    manifest = {"version": version, "source_revision": run("git", "rev-parse", "HEAD", capture=True).strip(),
                "ndk_version": ndk_version, "min_sdk": min_sdk, "profile": "android-release",
                "release_matrix": not bool(args.abis), "libraries": records}
    (stage / "android-build-metadata.json").write_text(json.dumps(manifest, indent=2) + "\n")
    output.mkdir(parents=True, exist_ok=True)
    def archive(name, entries):
        destination = stage / name
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
            for source, entry in sorted(entries, key=lambda item: item[1]):
                info = zipfile.ZipInfo(entry, (1980, 1, 1, 0, 0, 0))
                info.external_attr = 0o100644 << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                zip_file.writestr(info, source.read_bytes())
        shutil.copyfile(destination, output / name)
        print(f"Created {output / name}")
    # Subsets cannot be mistaken for the canonical release archive by downstream jobs.
    archive("android-native-libraries-dev.zip" if args.abis else "android-native-libraries.zip",
            [(p, p.relative_to(stage).as_posix()) for p in (stage / "jniLibs").rglob("*.so")])
    archive("android-native-debug-symbols-dev.zip" if args.abis else "android-native-debug-symbols.zip",
            [(p, p.relative_to(stage / "symbols").as_posix()) for p in (stage / "symbols").rglob("*.dbg")])
    archive("kotlin-bindings.zip", [(bindings, "com/tinkle/sdk/tinkle_ffi.kt")])
    shutil.copyfile(stage / "android-build-metadata.json", output / "android-build-metadata.json")
print("Native ELF and binding parity checks passed; installed APK checks are separate.")
PY
