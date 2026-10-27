#!/usr/bin/env python3
"""Automation-only native consumer checks. Never rebuilds release-candidate Rust."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
import urllib.request
import zipfile

spec = importlib.util.spec_from_file_location("sdk_release", Path(__file__).with_name("sdk-release.py"))
assert spec and spec.loader
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)

pkg_spec = importlib.util.spec_from_file_location("sdk_packages", Path(__file__).with_name("sdk-packages.py"))
assert pkg_spec and pkg_spec.loader
packages = importlib.util.module_from_spec(pkg_spec)
pkg_spec.loader.exec_module(packages)

CORE_ROOT = Path(__file__).resolve().parents[1]


def require_consumer_files(sdk, present, revision):
    expected = set(local_package_files(sdk))
    missing = sorted(expected - set(present))
    release.require(not missing,
                    f"BLOCKED: {sdk} consumer at {revision} is incomplete; missing files: "
                    + ", ".join(missing) + ". Commit the SDK package sources under packages/<sdk>/.")


def local_package_files(sdk):
    """Canonical package sources live in this repo at packages/<sdk>/."""
    return packages.package_files(CORE_ROOT, sdk)


def copy_local_package(sdk, destination):
    """Copy canonical package sources into a disposable test directory.

    Never mutates the working tree. Preserves git executable bits so the
    staged copy matches what promotion snapshots.
    """
    destination = Path(destination)
    release.require(not any(p.is_symlink() for p in [destination, *destination.parents]),
                    "symlink package destination")
    if destination.exists():
        release.require(destination.is_dir(), "package destination must be a directory")
    destination.mkdir(parents=True, exist_ok=True)
    snapshot = packages.snapshot(CORE_ROOT, release.run("git", "rev-parse", "HEAD", cwd=CORE_ROOT), sdk)
    for path, content in snapshot.items():
        target = destination / path
        release.require(not any(p.is_symlink() for p in [target, *target.parents]),
                        f"symlink in package destination: {path}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        mode = 0o755 if snapshot.modes.get(path, 0o100644) == 0o100755 else 0o644
        target.chmod(mode)
    return snapshot


def consumer_checkout(sdk, destination):
    destination = Path(destination)
    expected = local_package_files(sdk)
    require_consumer_files(sdk, (name for name in expected
                                if (destination / name).is_file()), destination)


def call(*args, cwd=None, env=None):
    subprocess.run([str(a) for a in args], cwd=cwd, env=env, check=True)


def toolchain():
    version = release.run("rustc", "--version")
    release.require(version.startswith("rustc 1.99.1 "), "native builds require Rust 1.99.1")
    # Full metadata checks the resolved runtime and bindgen dependency versions.
    data = json.loads(release.run("cargo", "metadata", "--locked", "--all-features", "--format-version", "1"))
    for name in ("uniffi", "uniffi_bindgen", "uniffi_core", "uniffi_macros"):
        versions = {p["version"] for p in data["packages"] if p["name"] == name}
        release.require(versions == {"0.32.2"}, f"{name} must resolve only to 0.32.2: {versions}")


def sync_package(sdk, destination, source=None):
    """Copy canonical packages/<sdk>/ sources into a disposable directory.

    Used by CI before staging: single checkout at the tested SHA already
    contains the package sources, so no external SDK repository is involved.
    Never mutates the working tree.
    """
    if source is None:
        # Default path: committed package bytes with git executable bits, so a
        # dirty worktree cannot stage uncommitted edits as test evidence.
        snapshot = copy_local_package(sdk, destination)
        consumer_checkout(sdk, destination)
        print(f"Synced packages/{sdk} into disposable {Path(destination).resolve()}", flush=True)
        return snapshot
    destination = Path(destination)
    src = Path(source).resolve()
    release.require(src.is_dir(), f"package source missing: {src}")
    release.require(not any(p.is_symlink() for p in [src, *src.parents]),
                    "symlink package source")
    expected = local_package_files(sdk)
    for name in expected:
        src_file = src / name
        release.require(not any(p.is_symlink() for p in [src_file, *src_file.parents]),
                        f"symlink package source: {name}")
        release.require(src_file.is_file(), f"package source missing: {name}")
    release.require(not any(p.is_symlink() for p in [destination, *destination.parents]),
                    "symlink package destination")
    if destination.exists():
        release.require(destination.is_dir(), "package destination must be a directory")
    destination.mkdir(parents=True, exist_ok=True)
    for name in expected:
        target = destination / name
        release.require(not any(p.is_symlink() for p in [target, *target.parents]),
                        f"symlink in package destination: {name}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((src / name).read_bytes())
    consumer_checkout(sdk, destination)
    print(f"Synced {src} into disposable {destination.resolve()}", flush=True)


def stage(args):
    destination = args.destination.resolve()
    artifacts = args.artifacts.resolve()
    sdk = args.sdk
    if getattr(args, "source", None):
        sync_package(sdk, destination, args.source)
    else:
        # Same-revision default: stage() does not silently heal an incomplete
        # checkout. CI explicitly syncs first; see sync-package.
        consumer_checkout(sdk, destination)
    native = "ios" if sdk == "swift" else "android"
    bindings, source, target = release.BINDINGS[sdk]
    if args.bootstrap:
        release.require(not (destination / "sdk-artifact.json").exists(),
                        "bootstrap cannot replace a real SDK release")
        for key in (native, bindings):
            path = artifacts / release.ARTIFACTS[key]
            release.inspect_zip(path)
            print(f"BOOTSTRAP ONLY {path.name} sha256={release.digest(path)}")
        with zipfile.ZipFile(artifacts / release.ARTIFACTS[bindings]) as archive:
            release.write_scoped(destination, target, archive.read(source))
    else:
        metadata = artifacts / "sdk-release.json" if args.candidate else destination / "sdk-artifact.json"
        manifest = release.load(metadata)
        release.verify(manifest, artifacts, [native, bindings], args.download)
        if args.candidate:
            # PR candidates can share the last release's Cargo version. This is
            # a disposable consumer checkout, not a production promotion.
            with zipfile.ZipFile(artifacts / release.ARTIFACTS[bindings]) as archive:
                release.write_scoped(destination, target, archive.read(source))
            release.write_scoped(destination, "sdk-artifact.json", release.encoded(manifest))
        else:
            release.promote(manifest, artifacts, sdk, destination, check=True)
        print(f"Source: {manifest['source_revision']}; {native}: {manifest['artifacts'][native]['sha256']}")
    if sdk == "swift":
        artifact_dir = destination / "Artifacts"
        release.require(not (artifact_dir / "TinkleSDKFFI.xcframework").exists(),
                        "consumer must start without a stale local XCFramework")
        release.extract_zip(artifacts / release.ARTIFACTS["ios"], artifact_dir)
        release.require((artifact_dir / "TinkleSDKFFI.xcframework/Info.plist").is_file(),
                        "missing packaged XCFramework")
        release.require({p.name for p in artifact_dir.iterdir()} == {"TinkleSDKFFI.xcframework"},
                        "unexpected XCFramework ZIP roots")


def swift_test(destination):
    consumer_checkout("swift", destination)
    call("xcodebuild", "-version")
    call("swift", "--version")
    devices = json.loads(release.run("xcrun", "simctl", "list", "devices", "available", "--json"))
    candidates = [(runtime, device) for runtime, rows in devices["devices"].items()
                  if ".iOS-" in runtime for device in rows
                  if device.get("isAvailable") and device["name"].startswith("iPhone")]
    release.require(bool(candidates), "BLOCKED: no available iOS simulator runtime")
    runtime, device = sorted(candidates, key=lambda item: item[0])[-1]
    print(f"Runtime proof: {runtime}/{device['name']}; deployment target: iOS 15.0.", flush=True)
    print("iOS 15 runtime proof remains unverified; the installed simulator is not a minimum-OS test.", flush=True)
    env = dict(os.environ, TINKLE_SDK_LOCAL="1", IPHONEOS_DEPLOYMENT_TARGET="15.0")
    call("xcodebuild", "-scheme", "TinkleSDK", "-destination", f"platform=iOS Simulator,id={device['udid']}",
         "-derivedDataPath", str(destination / ".build/ci-derived"),
         "-resultBundlePath", str(destination / ".build/ci-results.xcresult"),
         "IPHONEOS_DEPLOYMENT_TARGET=15.0", "CODE_SIGNING_ALLOWED=NO", "test", cwd=destination, env=env)


def android_env(artifacts):
    directory = artifacts.resolve()
    return dict(os.environ, TINKLE_SDK_ARTIFACT_DIR=str(directory),
                TINKLE_SDK_ANDROID_SHA256=release.digest(directory / release.ARTIFACTS["android"]),
                TINKLE_SDK_KOTLIN_BINDINGS_SHA256=release.digest(directory / release.ARTIFACTS["kotlin_bindings"]))


def android_build(destination, artifacts):
    env = android_env(artifacts)
    gradlew = destination / "gradlew"
    release.require(gradlew.is_file(),
                    "BLOCKED: Kotlin consumer checkout has no gradlew.")
    call("bash", gradlew, "--no-daemon", "-p", "fixture-tests", "test", cwd=destination, env=env)
    call("bash", gradlew, "-p", "buildSrc", "test", cwd=destination, env=env)
    call("bash", gradlew, "--no-daemon", "assembleRelease", "testReleaseUnitTest",
         cwd=destination, env=env)
    aars = list((destination / "build/outputs/aar").glob("*-release.aar"))
    release.require(len(aars) == 1, "expected one final release AAR")
    aar = aars[0]
    release.inspect_zip(aar)
    call("bash", Path(__file__).with_name("verify-android.sh"), aar, env=env)
    print(f"Final AAR: {aar.name} sha256={release.digest(aar)}", flush=True)
    proof = {"path": str(aar.resolve()), "sha256": release.digest(aar)}
    release.write_scoped(destination, "build/sdk-ci-aar.json", (json.dumps(proof) + "\n").encode())


def android_auxv_page_size(raw):
    """Read AT_PAGESZ from native x86_64 system utilities in our emulator matrix."""
    release.require(bool(raw) and len(raw) % 16 == 0, "invalid x86_64 auxv length")
    entries = list(struct.iter_unpack("<QQ", raw))
    end = next((i for i, (tag, _) in enumerate(entries) if tag == 0), None)
    release.require(end is not None and all(entry == (0, 0) for entry in entries[end:]),
                    "invalid auxv terminator")
    pages = [value for tag, value in entries[:end] if tag == 6]  # AT_PAGESZ
    release.require(len(pages) == 1 and pages[0] > 0 and pages[0] & (pages[0] - 1) == 0,
                    "missing, duplicate, or invalid AT_PAGESZ")
    return pages[0]


def android_runtime_page_size():
    # Android's x86_64 ps16k image simulates 16 KB application pages on 4 KB
    # kernel mappings. smaps/KernelPageSize measures the latter, not sysconf().
    # A successful getconf result is authoritative even when it is the WRONG
    # size for this lane; never fall back in search of the expected value.
    for key in ("PAGE_SIZE", "PAGESIZE"):
        try:
            value = release.run("adb", "shell", "getconf", key).strip()
        except subprocess.CalledProcessError:
            continue
        release.require(re.fullmatch(r"[1-9][0-9]*", value), f"invalid getconf {key} output: {value!r}")
        return int(value), f"getconf {key}"
    # API 24's toybox lacks getconf. Read the same AT_PAGESZ supplied by the
    # device kernel to Bionic. exec-out is binary-safe; release.run is not.
    result = subprocess.run(["adb", "exec-out", "cat", "/proc/self/auxv"],
                            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return android_auxv_page_size(result.stdout), "AT_PAGESZ"


def android_diagnostics(destination):
    """Best-effort fixture evidence while the emulator is still alive.

    Never let reporting replace a failing instrumentation command. Only test
    exception tags and named test-result directories are collected, not the
    runner environment, Gradle caches, or credential-bearing configuration.
    """
    try:
        result = subprocess.run(
            ["adb", "logcat", "-d", "-v", "threadtime", "-t", "2000",
             "System.err:W", "AndroidRuntime:E", "*:S"],
            capture_output=True, timeout=20, check=False)
        release.write_scoped(destination, "build/diagnostics/logcat.txt", result.stdout)
        if result.returncode:
            print("Warning: device exception logcat collection failed", flush=True)
    except (OSError, ValueError, subprocess.SubprocessError):
        print("Warning: device exception logcat unavailable", flush=True)
    results = destination / "sample/build/outputs/androidTest-results/connected"
    try:
        for path in sorted(results.rglob("*.xml")):
            if path.is_symlink() or path.stat().st_size > 4 * 1024 * 1024:
                continue
            for case in ET.parse(path).iter("testcase"):
                for failure in (*case.findall("failure"), *case.findall("error")):
                    print(f"Instrumentation failure: {case.get('classname')}.{case.get('name')}", flush=True)
                    print(failure.get("message", ""), flush=True)
                    print(failure.text or "", flush=True)
    except (OSError, ET.ParseError):
        print("Warning: instrumentation XML report unavailable", flush=True)


def android_test(destination, artifacts, page_size, api_level):
    # A wrong API or ABI must fail before probing or executing the consumer.
    actual_api = release.run("adb", "shell", "getprop", "ro.build.version.sdk").strip()
    release.require(actual_api == str(api_level), f"wrong emulator API: expected {api_level}, got {actual_api}")
    abi = release.run("adb", "shell", "getprop", "ro.product.cpu.abi").strip()
    release.require(abi == "x86_64", f"wrong emulator ABI: expected x86_64, got {abi}")
    actual_pages, probe = android_runtime_page_size()
    release.require(actual_pages == page_size,
                    f"wrong emulator page size: expected {page_size}, got {actual_pages} via {probe}")
    print(f"Runtime proof: Android API {actual_api}, ABI={abi}, "
          f"application page size={actual_pages} bytes via {probe}", flush=True)
    call("adb", "shell", "getprop", "ro.product.cpu.abilist")
    proof = json.loads((destination / "build/sdk-ci-aar.json").read_text())
    release.require(release.digest(proof["path"]) == proof["sha256"], "AAR changed before consumer test")
    env = dict(android_env(artifacts), TINKLE_SDK_TEST_AAR=proof["path"])
    gradlew = destination / "gradlew"
    release.require(gradlew.is_file(),
                    "BLOCKED: Kotlin consumer checkout has no gradlew.")
    # The sample's CI mode consumes this file AAR, never project(':') or jniLibs.
    try:
        call("bash", gradlew, "--no-daemon", ":sample:connectedDebugAndroidTest",
             "-PtinkleSdkTestAar=" + proof["path"], cwd=destination, env=env)
    finally:
        android_diagnostics(destination)
    release.require(release.digest(proof["path"]) == proof["sha256"], "AAR changed during consumer test")
    apks = list((destination / "sample/build/outputs/apk/debug").glob("*.apk"))
    release.require(bool(apks), "missing final consumer APK for ELF/ZIP verification")
    for apk in apks:
        release.inspect_zip(apk)
        call("bash", Path(__file__).with_name("verify-android.sh"), apk, env=env)
        sdk = Path(os.environ.get("ANDROID_HOME", os.environ.get("ANDROID_SDK_ROOT", "")))
        release.require(sdk.is_dir() and str(sdk) != ".", "ANDROID_HOME or ANDROID_SDK_ROOT must name the installed SDK")
        call(sdk / "build-tools/36.0.0/zipalign", "-c", "-P", "16", "-v", "4", apk, env=env)


def published_manifest(manifest):
    url = (f"https://github.com/{release.REPOSITORY}/releases/download/"
           f"{manifest['source_tag']}/sdk-release.json")
    opener = urllib.request.build_opener(release.ReleaseRedirect())
    def fetch():
        with opener.open(url, timeout=120) as response:
            content = response.read(65537)
        release.require(len(content) <= 65536, "published manifest too large")
        actual = release.validate(json.loads(content, object_pairs_hook=release.unique_object))
        release.require(actual == manifest, "SDK provenance differs from published core release")
    release.retry(fetch)


def sdk_tag(destination, sdk, create):
    """Authorize v tags solely through a merged, same-repository scoped release PR."""
    repo = f"tinkle-org/tinkle-sdk-{sdk}"
    release.require(os.environ.get("GITHUB_REPOSITORY") == repo, "wrong SDK repository")
    release.require(os.environ.get("GITHUB_EVENT_NAME") == "push", "SDK tags require push validation")
    ref = os.environ.get("GITHUB_REF", "")
    release.require(ref == "refs/heads/main" if create else ref.startswith("refs/tags/v"),
                    "SDK tag creation requires main; verification requires a version tag")
    sha = release.run("git", "rev-parse", "HEAD", cwd=destination)
    release.require(sha == os.environ.get("GITHUB_SHA"), "SDK checkout differs from the validated event SHA")
    manifest = release.load(destination / "sdk-artifact.json")
    published_manifest(manifest)
    release.require(release.remote_tag_revision(manifest["source_tag"]) == manifest["source_revision"],
                    "published core tag differs from SDK source provenance")
    prs = json.loads(release.run("gh", "api", f"repos/{repo}/commits/{sha}/pulls"))
    def is_release_pr(pr):
        return (pr["merged_at"] and pr["merge_commit_sha"] == sha
                and pr["base"]["ref"] == "main" and pr["head"]["repo"]
                and pr["head"]["repo"]["full_name"] == repo
                and pr["head"]["ref"] == "sdk-release/" + manifest["version"]
                and pr["title"] == "chore: release SDK v" + manifest["version"])
    eligible = [pr for pr in prs if is_release_pr(pr)]
    release.require(len(eligible) == 1, "tag requires a merged same-repository SDK release PR")
    number = eligible[0]["number"]
    # The association endpoint omits changed_files; get authoritative PR detail
    # and recheck eligibility before trusting its count or fetching its file list.
    pr = json.loads(release.run("gh", "api", f"repos/{repo}/pulls/{number}"))
    release.require(pr["number"] == number and is_release_pr(pr), "release PR changed during authorization")
    files = json.loads(release.run("gh", "api", f"repos/{repo}/pulls/{pr['number']}/files?per_page=100"))
    # Full snapshot scope: package sources + generated binding + inventory +
    # provenance. Reuse the canonical expected mapping without needing artifacts.
    # Authorized deletions come from the immutable promotion contract, never from
    # an ad-hoc allowlist: only paths removed by promotion from the merge's first
    # parent may disappear, and only with a removed file status proven absent.
    snapshot = release.package_snapshot(manifest, sdk)
    allowed = {*snapshot, release.BINDINGS[sdk][2], release.INVENTORY, "sdk-artifact.json"}
    base = release.run("git", "rev-parse", sha + "^1", cwd=destination)
    removed = set(release.promotion_deletions(destination, base, manifest, sdk))
    release.require(type(pr.get("changed_files")) is int and pr["changed_files"] == len(files)
                    and "sdk-artifact.json" in {f["filename"] for f in files}
                    and {f["filename"] for f in files} <= allowed | removed
                    and all(f["status"] == "removed" if f["filename"] in removed
                            else f["status"] in ("added", "modified") for f in files),
                    "release PR must change only the managed SDK snapshot")
    release.run("git", "merge-base", "--is-ancestor", sha, "origin/main", cwd=destination)
    with tempfile.TemporaryDirectory() as tmp:
        release.verify(manifest, tmp, [release.BINDINGS[sdk][0]], fetch=True)
        release.promote(manifest, tmp, sdk, destination, check=True)
        expected = release.expected_files(manifest, tmp, sdk)
        release.verify_snapshot_at(destination, sha, expected, removed)
    tag = "v" + manifest["version"]
    if not create:
        release.require(os.environ["GITHUB_REF"] == "refs/tags/" + tag, "SDK tag/version mismatch")
    # List refs to distinguish an absent ref from authorization/network failure.
    refs = json.loads(release.run("gh", "api", f"repos/{repo}/git/matching-refs/tags/{tag}"))
    refs = [ref for ref in refs if ref["ref"] == "refs/tags/" + tag]
    if refs:
        release.require(len(refs) == 1 and refs[0]["object"]["type"] == "commit"
                        and refs[0]["object"]["sha"] == sha, "refusing to overwrite an SDK tag")
    elif create:
        release.run("gh", "api", "--method", "POST", f"repos/{repo}/git/refs",
                    "-f", "ref=refs/tags/" + tag, "-f", "sha=" + sha)
    else:
        raise ValueError("missing SDK tag")
    print(tag)
    return tag


def jitpack(tag, destination):
    release.require(tag.startswith("v") and release.version_valid(tag[1:]), "invalid JitPack version")
    destination = destination.resolve()
    gradlew = destination / "gradlew"
    release.require(gradlew.is_file(),
                    "BLOCKED: Kotlin consumer checkout has no gradlew.")
    # Fetching this coordinate triggers the JitPack build. Retry the same immutable
    # version; no Maven Central/signing credentials and no mutable branch versions.
    base = f"https://jitpack.io/com/github/tinkle-org/tinkle-sdk-kotlin/{tag}/tinkle-sdk-kotlin-{tag}"
    manifest = release.load(destination / "sdk-artifact.json")
    release.require(tag == "v" + manifest["version"], "JitPack version/provenance mismatch")
    aar = destination / "build/jitpack/tinkle-sdk-kotlin.aar"
    aar.parent.mkdir(parents=True, exist_ok=True)
    for suffix in (".pom", ".aar"):
        def fetch():
            with urllib.request.urlopen(base + suffix, timeout=300) as response:
                with tempfile.TemporaryFile() as output:
                    size = 0
                    while chunk := response.read(1024 * 1024):
                        size += len(chunk)
                        release.require(size <= release.MAX_DOWNLOAD, "JitPack artifact too large")
                        output.write(chunk)
                    release.require(size > 0, "empty JitPack artifact")
                    if suffix == ".aar":
                        output.seek(0)
                        with zipfile.ZipFile(output) as archive:
                            release.require("classes.jar" in archive.namelist(), "invalid JitPack AAR")
                        output.seek(0)
                        with aar.open("wb") as target:
                            shutil.copyfileobj(output, target)
        release.retry(fetch, attempts=20, delay=15)
    release.inspect_zip(aar)
    # Trust model: JitPack builds the AAR from the GitHub tag `v<version>` (authorized
    # via the sdk-tag flow). No independently published digest exists for JitPack
    # artifacts, so JitPack is trusted as-is to build from that tag. The digest below
    # only proves Gradle resolved the same bytes as this direct download (consistency),
    # not independent provenance.
    proof = {"path": str(aar), "sha256": release.digest(aar)}
    release.write_scoped(destination, "build/sdk-ci-aar.json", (json.dumps(proof) + "\n").encode())
    env = dict(os.environ, TINKLE_EXPECTED_SDK_VERSION=tag,
               TINKLE_EXPECTED_SDK_AAR_SHA256=proof["sha256"])
    # Use the published coordinate without the file-AAR override so its POM must
    # supply JNA/coroutines. Compare Gradle's resolved AAR with the runtime input.
    env.pop("TINKLE_SDK_TEST_AAR", None)
    call("bash", gradlew, "--no-daemon", "--init-script",
         Path(__file__).with_name("sdk-consumer.init.gradle"),
         ":sample:verifyPublishedSdkCoordinate",
         "-PtinkleSdkCoordinate=com.github.tinkle-org:tinkle-sdk-kotlin:" + tag,
         cwd=destination, env=env)
    print(f"Resolved com.github.tinkle-org:tinkle-sdk-kotlin:{tag}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("toolchain")
    sync_cmd = sub.add_parser("sync-package")
    sync_cmd.add_argument("--sdk", choices=release.BINDINGS, required=True)
    sync_cmd.add_argument("--destination", type=Path, required=True)
    sync_cmd.add_argument("--source", type=Path, default=None)
    for name in ("stage", "swift-test", "android-build", "android-test", "sdk-tag"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--destination", type=Path, required=True)
        if name in ("stage", "android-build", "android-test"):
            cmd.add_argument("--artifacts", type=Path, required=True)
        if name in ("stage", "sdk-tag"):
            cmd.add_argument("--sdk", choices=release.BINDINGS, required=True)
        if name == "stage":
            group = cmd.add_mutually_exclusive_group()
            group.add_argument("--candidate", action="store_true")
            group.add_argument("--bootstrap", action="store_true")
            cmd.add_argument("--download", action="store_true")
            cmd.add_argument("--source", type=Path, default=None,
                             help="Package source dir; defaults to this checkout's packages/<sdk>")
        if name == "android-test":
            cmd.add_argument("--page-size", type=int, choices=(4096, 16384), required=True)
            cmd.add_argument("--api-level", type=int, choices=(24, 36), required=True)
        if name == "sdk-tag":
            cmd.add_argument("--create", action="store_true")
    warm = sub.add_parser("jitpack")
    warm.add_argument("--tag", required=True)
    warm.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "toolchain":
        toolchain()
    elif args.command == "sync-package":
        sync_package(args.sdk, args.destination.resolve(), args.source)
    elif args.command == "stage":
        stage(args)
    elif args.command == "swift-test":
        swift_test(args.destination.resolve())
    elif args.command == "android-build":
        android_build(args.destination.resolve(), args.artifacts)
    elif args.command == "android-test":
        android_test(args.destination.resolve(), args.artifacts, args.page_size, args.api_level)
    elif args.command == "sdk-tag":
        sdk_tag(args.destination.resolve(), args.sdk, args.create)
    else:
        jitpack(args.tag, args.destination)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError, zipfile.BadZipFile) as error:
        sys.exit(f"sdk-ci: {error}")
