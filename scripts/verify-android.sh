#!/usr/bin/env bash
# Inspect the final jniLibs tree, or every native library extracted from an APK/AAR.
set -euo pipefail
exec python3 - "$@" <<'PY'
import argparse
import os
from pathlib import Path, PurePosixPath
import platform
import re
import subprocess
import tempfile
import zipfile

parser = argparse.ArgumentParser(description="Verify Android ELF machine, all LOADs, dependencies, exports and SHA1 build IDs")
parser.add_argument("artifact", type=Path, help="jniLibs directory, APK, AAR or native ZIP")
parser.add_argument("--abis", default="arm64-v8a,armeabi-v7a,x86_64,x86", help="explicit development/device ABI set")
args = parser.parse_args()
machines = {"arm64-v8a": ("AArch64", "ELF64"), "armeabi-v7a": ("ARM", "ELF32"),
            "x86_64": ("Advanced Micro Devices X86-64", "ELF64"), "x86": ("Intel 80386", "ELF32")}
abis = args.abis.split(",")
if len(set(abis)) != len(abis) or any(a not in machines for a in abis):
    parser.error("invalid ABI set")
default_sdk = Path.home() / ("Library/Android/sdk" if platform.system() == "Darwin" else "Android/Sdk")
sdk = Path(os.environ.get("ANDROID_HOME", os.environ.get("ANDROID_SDK_ROOT", str(default_sdk))))
ndk = Path(os.environ.get("ANDROID_NDK_HOME", str(sdk / "ndk/30.0.16248370")))
host = {"Darwin": "darwin-x86_64", "Linux": "linux-x86_64"}.get(platform.system())
if not host:
    parser.error("verification requires macOS or Linux")
readelf = ndk / "toolchains/llvm/prebuilt" / host / "bin/llvm-readelf"
if not readelf.is_file():
    parser.error("NDK llvm-readelf missing; install NDK 30.0.16248370 or set ANDROID_NDK_HOME")
def inspect(*options):
    return subprocess.check_output([str(readelf), *map(str, options)], text=True)
def require(condition, message):
    if not condition:
        raise SystemExit("error: " + message)
with tempfile.TemporaryDirectory(prefix="tinkle-verify-") as temporary:
    root = args.artifact
    if root.is_file():
        with zipfile.ZipFile(root) as archive:
            names = set()
            extracted = set()
            expanded = 0
            for entry in archive.infolist():
                path = PurePosixPath(entry.filename)
                parts = entry.filename.rstrip("/").split("/")
                require(not path.is_absolute() and all(p not in ("", ".", "..") for p in parts)
                        and "\\" not in entry.filename and ":" not in entry.filename,
                        f"unsafe archive entry {entry.filename}")
                require(len(names) < 20000 and entry.filename not in names, "duplicate/excessive archive entries")
                names.add(entry.filename)
                if path.suffix == ".so":
                    require(len(path.parts) == 3 and path.parts[0] in ("lib", "jni", "jniLibs"),
                            f"unexpected native entry {entry.filename}")
                    require(path.parts[1] in machines and entry.file_size <= 512 * 1024**2, "invalid native entry")
                    destination = Path(temporary) / path.parts[1] / path.name
                    require(destination not in extracted, "colliding native archive paths")
                    extracted.add(destination)
                    expanded += entry.file_size
                    require(expanded <= 2 * 1024**3, "native archive expands beyond size limit")
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(archive.read(entry))
        root = Path(temporary)
    require(root.is_dir(), f"missing native artifact {args.artifact}")
    for abi in abis:
        folder = root / abi
        require((folder / "libtinkle_ffi.so").is_file(), f"missing {abi}/libtinkle_ffi.so")
        libraries = list(folder.glob("*.so"))
        for library in libraries:
            header = inspect("-h", library)
            machine, elf_class = machines[abi]
            require(re.search(r"Type:\s+DYN\b", header) is not None, f"{library}: not a shared object")
            require(re.search(r"Machine:\s+" + re.escape(machine) + r"\s*$", header, re.M) is not None,
                    f"{library}: wrong ELF machine")
            require(re.search(r"Class:\s+" + elf_class, header) is not None, f"{library}: wrong ELF class")
            loads = [line.split() for line in inspect("-lW", library).splitlines() if line.strip().startswith("LOAD ")]
            require(bool(loads), f"{library}: no LOAD segments")
            for load in loads:
                alignment = int(load[-1], 16)
                floor = 16384 if elf_class == "ELF64" else 4096
                require(alignment >= floor and alignment & (alignment - 1) == 0,
                        f"{library}: LOAD alignment {alignment} < {floor} or not power of two")
                require((int(load[1], 16) - int(load[2], 16)) % alignment == 0,
                        f"{library}: incongruent LOAD file/virtual offsets")
            ids = re.findall(r"Build ID: ([0-9a-f]+)", inspect("-n", library))
            require(len(ids) == 1 and len(ids[0]) == 40, f"{library}: missing SHA1 GNU build ID")
            needed = re.findall(r"\(NEEDED\).*\[([^]]+)\]", inspect("-d", library))
            system = {"libc.so", "libm.so", "libdl.so", "liblog.so", "libandroid.so", "libz.so"}
            require(all(n in system or (folder / n).is_file() for n in needed),
                    f"{library}: unbundled non-system dependencies {needed}")
            symbols = []
            for line in inspect("--dyn-syms", "--wide", library).splitlines():
                fields = line.split()
                if len(fields) >= 8 and fields[4] in ("GLOBAL", "WEAK") and fields[6] != "UND" and fields[3] in ("FUNC", "OBJECT"):
                    symbols.append(fields[7])
            if library.name == "libtinkle_ffi.so":
                require(bool(symbols), f"{library}: no FFI exports")
                require(all(s.startswith(("uniffi_", "ffi_tinkle_ffi_", "UNIFFI_META_")) or s == "JNI_OnLoad" for s in symbols),
                        f"{library}: unexpected dynamic exports: " + str([s for s in symbols if not s.startswith(("uniffi_", "ffi_tinkle_ffi_", "UNIFFI_META_")) and s != "JNI_OnLoad"]))
            print(f"OK {abi}/{library.name}: {len(loads)} aligned LOADs, build ID {ids[0]}, {library.stat().st_size} bytes")
print("ELF checks passed (including dependency libraries). APK ZIP alignment and installed runtime are separate checks.")
PY
