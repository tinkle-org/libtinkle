#!/usr/bin/env python3
"""Automation-only native consumer checks. Never rebuilds release-candidate Rust."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile

spec = importlib.util.spec_from_file_location("sdk_release", Path(__file__).with_name("sdk-release.py"))
assert spec and spec.loader
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


CONSUMER_FILES = {
    "swift": (
        "Package.swift", "Sources/TinkleSDK/Bootstrap.swift",
        "Tests/TinkleSDKTests/TinkleSDKTests.swift",
        "Tests/TinkleSDKTests/LoopbackGateway.swift",
    ),
    "kotlin": (
        "settings.gradle.kts", "build.gradle.kts", "gradle.properties", "gradlew",
        "gradle/wrapper/gradle-wrapper.jar", "gradle/wrapper/gradle-wrapper.properties",
        "consumer-rules.pro", "src/main/AndroidManifest.xml",
        "src/main/kotlin/com/tinkle/sdk/TinkleSdk.kt",
        "buildSrc/build.gradle", "buildSrc/settings.gradle",
        "buildSrc/src/main/groovy/com/tinkle/build/PrepareSdkArtifacts.groovy",
        "buildSrc/src/main/groovy/com/tinkle/build/SdkArtifacts.groovy",
        "buildSrc/src/test/groovy/com/tinkle/build/PrepareSdkArtifactsTest.groovy",
        "buildSrc/src/test/groovy/com/tinkle/build/SdkArtifactsTest.groovy",
        "sample/build.gradle.kts", "sample/smoke-rules.pro",
        "sample/src/main/AndroidManifest.xml",
        "sample/src/main/kotlin/com/tinkle/sdk/smoke/SmokeChecks.kt",
        "sample/src/main/kotlin/com/tinkle/sdk/smoke/LoopbackGateway.kt",
        "sample/src/androidTest/kotlin/com/tinkle/sdk/smoke/NativeSmokeTest.kt",
    ),
}


def consumer_refs(pins, publish=False, swift_ref="", kotlin_ref=""):
    overrides = {"swift": swift_ref, "kotlin": kotlin_ref}
    if publish:
        release.require(all(ref in ("", "main") for ref in overrides.values()),
                        "Release consumers must be main revisions")
        return dict.fromkeys(CONSUMER_FILES, "main")
    data = json.loads(Path(pins).read_text(), object_pairs_hook=release.unique_object)
    release.require(isinstance(data, dict) and set(data) == {"schema_version", *CONSUMER_FILES},
                    "consumer pins must contain schema_version, swift, and kotlin only")
    release.require(type(data["schema_version"]) is int and data["schema_version"] == 1,
                    "unsupported consumer pin schema_version")
    for sdk in CONSUMER_FILES:
        for ref in (data[sdk], overrides[sdk] or data[sdk]):
            release.require(isinstance(ref, str) and re.fullmatch("[0-9a-f]{40}", ref),
                            f"{sdk} validation consumer must be a full lowercase commit SHA")
    return {sdk: overrides[sdk] or data[sdk] for sdk in CONSUMER_FILES}


def require_consumer_files(sdk, present, revision):
    missing = sorted(set(CONSUMER_FILES[sdk]) - set(present))
    release.require(not missing,
                    f"BLOCKED: {sdk} consumer at {revision} is incomplete; missing files: "
                    + ", ".join(missing)
                    + ". Commit the SDK scaffold/build logic, then update scripts/sdk-consumers.json "
                    "for validation. Publishing requires the complete scaffold on SDK main.")


def consumer_checkout(sdk, destination):
    destination = Path(destination)
    require_consumer_files(sdk, (name for name in CONSUMER_FILES[sdk]
                                if (destination / name).is_file()), destination)


def pin_consumers(pins, publish=False, swift_ref="", kotlin_ref=""):
    refs = consumer_refs(pins, publish, swift_ref, kotlin_ref)
    resolved = {}
    for sdk, ref in refs.items():
        repo = f"tinklehq/tinkle-sdk-{sdk}"
        commit = json.loads(release.run("gh", "api", f"repos/{repo}/commits/{ref}"))["sha"]
        release.require(isinstance(commit, str) and re.fullmatch("[0-9a-f]{40}", commit),
                        f"invalid {sdk} consumer commit")
        release.require(ref == "main" or commit == ref, f"{sdk} consumer revision mismatch")
        # Inspect the immutable tree without running consumer build code. A partial
        # tree must not be accepted as evidence of a missing (or complete) scaffold.
        tree = json.loads(release.run("gh", "api", f"repos/{repo}/git/trees/{commit}?recursive=1"))
        release.require(tree.get("truncated") is False, f"truncated {sdk} consumer tree")
        present = [entry["path"] for entry in tree["tree"]
                   if entry["type"] == "blob" and entry["mode"] in ("100644", "100755")]
        require_consumer_files(sdk, present, f"{repo}@{commit}")
        resolved[sdk] = commit
        print(f"Validated {repo}@{commit} (requested {ref})", flush=True)
    # Publish outputs atomically with respect to validation: never export a
    # partially validated pair or let downstream builds start on missing inputs.
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            for sdk, commit in resolved.items():
                output.write(f"{sdk}_sha={commit}\n")
    return resolved


def call(*args, cwd=None, env=None):
    subprocess.run([str(a) for a in args], cwd=cwd, env=env, check=True)


def select_xcode(version, applications=Path("/Applications")):
    candidates = [applications / f"Xcode_{version}.app/Contents/Developer",
                  applications / "Xcode.app/Contents/Developer"]
    developer = next((path for path in candidates if path.is_dir()), None)
    release.require(developer is not None, f"BLOCKED: Xcode {version} is not installed")
    env = dict(os.environ, DEVELOPER_DIR=str(developer))
    actual = release.run("xcodebuild", "-version", env=env)
    release.require(actual.splitlines() and actual.splitlines()[0] == f"Xcode {version}",
                    f"BLOCKED: expected Xcode {version} at {developer}, got {actual}")
    # Probe the selected toolchain in this process, not just a GITHUB_ENV value
    # which only takes effect in later workflow steps. Aliases are accepted only
    # after the exact version assertion; never silently use the runner default.
    sdk = release.run("xcrun", "--show-sdk-path", env=env)
    if "GITHUB_ENV" in os.environ:
        with open(os.environ["GITHUB_ENV"], "a") as output:
            output.write(f"DEVELOPER_DIR={developer}\n")
    print(f"{actual}\nDeveloper directory: {developer}\nSDK: {sdk}", flush=True)


def toolchain():
    version = release.run("rustc", "--version")
    release.require(version.startswith("rustc 1.99.1 "), "native builds require Rust 1.99.1")
    # Full metadata checks the resolved runtime and bindgen dependency versions.
    data = json.loads(release.run("cargo", "metadata", "--locked", "--all-features", "--format-version", "1"))
    for name in ("uniffi", "uniffi_bindgen", "uniffi_core", "uniffi_macros"):
        versions = {p["version"] for p in data["packages"] if p["name"] == name}
        release.require(versions == {"0.32.2"}, f"{name} must resolve only to 0.32.2: {versions}")


def android_setup():
    # Keep this tuple aligned with the native build script, not the host's
    # preinstalled/default NDK (which changes with runner images).
    sdk = Path(os.environ.get("ANDROID_HOME", os.environ.get("ANDROID_SDK_ROOT", "")))
    release.require(sdk.is_dir() and str(sdk) != ".", "ANDROID_HOME must name the installed SDK")
    # GitHub runners ship sdkmanager under cmdline-tools but not on PATH.
    # Resolve it explicitly instead of assuming PATH configuration.
    candidates = [sdk / "cmdline-tools/latest/bin/sdkmanager",
                  sdk / "cmdline-tools/bin/sdkmanager",
                  Path(shutil.which("sdkmanager") or "")]
    manager = next((str(p) for p in candidates if p and Path(p).is_file()), "sdkmanager")
    subprocess.run([manager, "--licenses"], input="y\n" * 200, text=True, check=True)
    call(manager, "--install", "ndk;30.0.16248370", "platforms;android-36", "build-tools;36.0.0")
    ndk = sdk.resolve() / "ndk/30.0.16248370"
    release.require(ndk.is_dir(), "required NDK r30 was not installed")
    if "GITHUB_ENV" in os.environ:
        with open(os.environ["GITHUB_ENV"], "a") as output:
            output.write(f"ANDROID_NDK_HOME={ndk}\n")
    print(f"ANDROID_NDK_HOME={ndk}")


def stage(args):
    destination = args.destination.resolve()
    artifacts = args.artifacts.resolve()
    sdk = args.sdk
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


def android_test(destination, artifacts, page_size, api_level):
    # Validate the emulator API first: a wrong image must fail before any
    # page-size probing, and the probe below must work on the minimum lane
    # (API 24), where toybox `getconf PAGE_SIZE` is not reliably present.
    actual_api = release.run("adb", "shell", "getprop", "ro.build.version.sdk").strip()
    release.require(actual_api == str(api_level), f"wrong emulator API: expected {api_level}, got {actual_api}")
    # Page size from /proc/self/smaps KernelPageSize lines (e.g. "4 kB" for
    # 4096, "16 kB" for 16384). `cat` exists on every API level; parsing the
    # full set rejects mixed-page images instead of sampling one mapping.
    smaps = release.run("adb", "shell", "cat", "/proc/self/smaps")
    sizes = {line.split(":", 1)[1].strip() for line in smaps.splitlines()
             if line.startswith("KernelPageSize:")}
    release.require(bool(sizes), "no KernelPageSize entries in /proc/self/smaps")
    expected = f"{page_size // 1024} kB"
    release.require(sizes == {expected}, f"wrong emulator page size: expected {expected}, got {sorted(sizes)}")
    print(f"Runtime proof: Android API {actual_api}, KernelPageSize={expected}", flush=True)
    call("adb", "shell", "getprop", "ro.product.cpu.abilist")
    proof = json.loads((destination / "build/sdk-ci-aar.json").read_text())
    release.require(release.digest(proof["path"]) == proof["sha256"], "AAR changed before consumer test")
    env = dict(android_env(artifacts), TINKLE_SDK_TEST_AAR=proof["path"])
    gradlew = destination / "gradlew"
    release.require(gradlew.is_file(),
                    "BLOCKED: Kotlin consumer checkout has no gradlew.")
    # The sample's CI mode consumes this file AAR, never project(':') or jniLibs.
    call("bash", gradlew, "--no-daemon", ":sample:connectedDebugAndroidTest",
         "-PtinkleSdkTestAar=" + proof["path"], cwd=destination, env=env)
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
    manifest = release.load(destination / "sdk-artifact.json")
    published_manifest(manifest)
    release.require(release.remote_tag_revision(manifest["source_tag"]) == manifest["source_revision"],
                    "published core tag differs from SDK source provenance")
    repo = f"tinklehq/tinkle-sdk-{sdk}"
    release.require(os.environ["GITHUB_REPOSITORY"] == repo, "wrong SDK repository")
    sha = release.run("git", "rev-parse", "HEAD", cwd=destination)
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    if "pull_request" in event:
        number = event["pull_request"]["number"]
        prs = [json.loads(release.run("gh", "api", f"repos/{repo}/pulls/{number}"))]
    else:
        prs = json.loads(release.run("gh", "api", f"repos/{repo}/commits/{sha}/pulls"))
    eligible = [pr for pr in prs if pr["merged_at"] and pr["merge_commit_sha"] == sha
                and pr["base"]["ref"] == "main" and pr["head"]["repo"]
                and pr["head"]["repo"]["full_name"] == repo
                and pr["head"]["ref"] == "sdk-release/" + manifest["version"]
                and pr["title"] == "chore: release SDK v" + manifest["version"]]
    release.require(len(eligible) == 1, "tag requires a merged same-repository SDK release PR")
    pr = eligible[0]
    files = json.loads(release.run("gh", "api", f"repos/{repo}/pulls/{pr['number']}/files?per_page=100"))
    release.require(pr["changed_files"] == len(files)
                    and "sdk-artifact.json" in {f["filename"] for f in files}
                    and {f["filename"] for f in files} <= {"sdk-artifact.json", release.BINDINGS[sdk][2]}
                    and all(f["status"] in ("added", "modified") for f in files),
                    "release PR must change only metadata and generated binding")
    release.run("git", "merge-base", "--is-ancestor", sha, "origin/main", cwd=destination)
    with tempfile.TemporaryDirectory() as tmp:
        release.verify(manifest, tmp, [release.BINDINGS[sdk][0]], fetch=True)
        release.promote(manifest, tmp, sdk, destination, check=True)
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
    base = f"https://jitpack.io/com/github/tinklehq/tinkle-sdk-kotlin/{tag}/tinkle-sdk-kotlin-{tag}"
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
         "-PtinkleSdkCoordinate=com.github.tinklehq:tinkle-sdk-kotlin:" + tag,
         cwd=destination, env=env)
    print(f"Resolved com.github.tinklehq:tinkle-sdk-kotlin:{tag}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("toolchain")
    sub.add_parser("android-setup")
    xcode = sub.add_parser("select-xcode")
    xcode.add_argument("--version", choices=("26.6", "27.1"), required=True)
    pins = sub.add_parser("pin-consumers")
    pins.add_argument("--pins", type=Path, default=Path(__file__).with_name("sdk-consumers.json"))
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
    elif args.command == "android-setup":
        android_setup()
    elif args.command == "select-xcode":
        select_xcode(args.version)
    elif args.command == "pin-consumers":
        pin_consumers(args.pins, os.environ.get("PUBLISH") == "true",
                      os.environ.get("SWIFT_REF", ""), os.environ.get("KOTLIN_REF", ""))
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
