#!/usr/bin/env bash
# Run after publishing/building a candidate AAR and booting an Android emulator.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 - "$ROOT" "$@" <<'PY'
import argparse
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys

parser = argparse.ArgumentParser(description="Installed R8 release consumer smoke, ELF and APK ZIP alignment")
parser.add_argument("--core", required=True, type=Path, help="libtinkle checkout containing scripts/verify-android.sh")
parser.add_argument("--aar", type=Path, help="exact final AAR (default consumes the locally published Maven component)")
parser.add_argument("--page-size", required=True, choices=("4096", "16384"))
parser.add_argument("--serial", help="adb/emulator serial")
args = parser.parse_args(sys.argv[2:])
root = Path(sys.argv[1])
env = os.environ.copy()
if args.serial:
    env["ANDROID_SERIAL"] = args.serial
def run(*command, capture=False):
    print("+ " + " ".join(map(str, command)), flush=True)
    return subprocess.run(list(map(str, command)), cwd=root, env=env, check=True,
                          text=True, stdout=subprocess.PIPE if capture else None).stdout
page_size = run("adb", "shell", "getconf", "PAGE_SIZE", capture=True).strip()
if page_size != args.page_size:
    sys.exit(f"error: expected {args.page_size}-byte device pages, got {page_size}")
api = int(run("adb", "shell", "getprop", "ro.build.version.sdk", capture=True).strip())
if api < 24:
    sys.exit("error: device is below minSdk 24")
if args.page_size == "16384":
    for key, value in (("bionic.linker.16kb.app_compat.enabled", "fatal" if api >= 37 else "false"),
                       ("pm.16kb.app_compat.disabled", "true")):
        run("adb", "shell", "setprop", key, value)
        if run("adb", "shell", "getprop", key, capture=True).strip() != value:
            sys.exit(f"error: could not disable 16 KB compatibility mode: {key}")
command = ["bash", root / "gradlew", "--no-daemon", ":sample:connectedReleaseAndroidTest", "-PsmokeBuildType=release"]
if args.aar:
    args.aar = args.aar.resolve()
    before = hashlib.sha256(args.aar.read_bytes()).hexdigest()
    command.append(f"-PtinkleSdkTestAar={args.aar}")
run(*command)
if args.aar and hashlib.sha256(args.aar.read_bytes()).hexdigest() != before:
    sys.exit("error: candidate AAR changed during installed verification")
sdk = Path(os.environ["ANDROID_HOME"])
zipalign = sdk / "build-tools/36.0.0/zipalign"
apks = list((root / "sample/build/outputs/apk/release").glob("*.apk"))
if not apks:
    sys.exit("error: no final release APK")
for apk in apks:
    run("bash", args.core.resolve() / "scripts/verify-android.sh", apk)
    run(zipalign, "-c", "-P", "16", "-v", "4", apk)
    manifests = list((root / "sample/build/intermediates/merged_manifests/release").rglob("AndroidManifest.xml"))
    if not manifests or any(re.search(r'pageSizeCompat\s*=\s*"(?:enabled|true)"', p.read_text()) for p in manifests):
        sys.exit("error: missing merged manifest or enabled page-size compatibility")
    print(f"Final APK sha256: {hashlib.sha256(apk.read_bytes()).hexdigest()}")
print(f"Installed R8 release smoke passed on API {api}, PAGE_SIZE={page_size}; all bundled native ELFs and ZIP alignment checked.")
PY
