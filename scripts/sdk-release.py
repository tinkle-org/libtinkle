#!/usr/bin/env python3
"""SDK artifact contract and distribution CLI (Python 3.11+, standard library only).

See .github/workflows/SDK-DISTRIBUTION.md. All network/mutating operations are
explicit subcommands or --open-pr; importing this module has no side effects.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

REPOSITORY = "tinklehq/libtinkle"
ARTIFACTS = {
    "ios": "TinkleSDKFFI.xcframework.zip",
    "swift_bindings": "swift-bindings.zip",
    "android": "android-native-libraries.zip",
    "kotlin_bindings": "kotlin-bindings.zip",
}
DEBUG_ASSETS = {
    "android_symbols": "android-native-debug-symbols.zip",
    "android_metadata": "android-build-metadata.json",
}
DEBUG_MANIFEST = "sdk-debug-assets.json"
ANDROID_ABIS = {"arm64-v8a", "armeabi-v7a", "x86_64", "x86"}
BINDINGS = {
    "swift": ("swift_bindings", "TinkleSDK.swift", "Sources/TinkleSDK/TinkleSDK.swift"),
    "kotlin": ("kotlin_bindings", "com/tinkle/sdk/tinkle_ffi.kt",
               "src/main/kotlin/com/tinkle/sdk/tinkle_ffi.kt"),
}
SEMVER = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
                    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
                    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"[0-9a-f]{64}\Z")
MAX_DOWNLOAD = 2 * 1024**3
MAX_EXPANDED = 4 * 1024**3
MAX_ENTRIES = 20000
MAX_BINDINGS = 32 * 1024**2


def require(condition, message):
    if not condition:
        raise ValueError(message)


def version_valid(value):
    match = SEMVER.fullmatch(value) if isinstance(value, str) else None
    return bool(match) and all(not (p.isdigit() and len(p) > 1 and p[0] == "0")
                               for p in (match[4] or "").split("."))


def version_order(value):
    # SemVer precedence (build metadata ignored): major/minor/patch numerically,
    # then release > prerelease, with prerelease identifiers compared per spec
    # (numeric < alphanumeric, numeric numerically, alphanumeric lexically,
    # longer prefix-equal sets sort higher).
    require(version_valid(value), "invalid SemVer version")
    match = SEMVER.fullmatch(value)
    assert match is not None  # validated above
    major, minor, patch = int(match[1]), int(match[2]), int(match[3])
    pre = match[4]
    if pre is None:
        return (major, minor, patch, 1, ())
    identifiers = []
    for part in pre.split("."):
        if part.isdigit():
            identifiers.append((0, int(part), ""))
        else:
            identifiers.append((1, 0, part))
    return (major, minor, patch, 0, tuple(identifiers))


def validate(manifest):
    require(isinstance(manifest, dict), "manifest must be an object")
    require(set(manifest) == {"schema_version", "version", "source_revision",
                              "source_repository", "source_tag", "artifacts"},
            "unexpected or missing manifest fields")
    require(type(manifest["schema_version"]) is int and manifest["schema_version"] == 1,
            "unsupported schema_version")
    require(version_valid(manifest["version"]), "invalid SemVer version")
    require(isinstance(manifest["source_revision"], str)
            and SHA.fullmatch(manifest["source_revision"]), "expected exact source SHA")
    require(manifest["source_repository"] == REPOSITORY, "unexpected source repository")
    require(manifest["source_tag"] == "tinkle-ffi-v" + manifest["version"],
            "source tag/version mismatch")
    require(isinstance(manifest["artifacts"], dict)
            and set(manifest["artifacts"]) == set(ARTIFACTS), "incorrect artifact set")
    base = f"https://github.com/{REPOSITORY}/releases/download/{manifest['source_tag']}/"
    for key, filename in ARTIFACTS.items():
        artifact = manifest["artifacts"][key]
        require(isinstance(artifact, dict) and set(artifact) == {"filename", "url", "sha256"},
                f"invalid {key} fields")
        require(artifact["filename"] == filename, f"invalid {key} filename")
        require(artifact["url"] == base + filename, f"noncanonical {key} HTTPS release URL")
        require(isinstance(artifact["sha256"], str) and DIGEST.fullmatch(artifact["sha256"]),
                f"invalid {key} sha256")
    return manifest


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load(path):
    require(Path(path).stat().st_size <= 65536, "manifest too large")
    return validate(json.loads(Path(path).read_text(), object_pairs_hook=unique_object))


def encoded(manifest):
    return (json.dumps(validate(manifest), indent=2, sort_keys=True) + "\n").encode()


def encoded_debug(manifest):
    return (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def run(*args, cwd=None, env=None):
    return subprocess.run([str(arg) for arg in args], cwd=cwd, env=env, check=True,
                          text=True, stdout=subprocess.PIPE).stdout.strip()


def retry(operation, attempts=3, delay=1):
    for attempt in range(attempts):
        try:
            return operation()
        except (subprocess.CalledProcessError, urllib.error.URLError, TimeoutError):
            if attempt == attempts - 1:
                raise
            time.sleep(min(delay * 2 ** attempt, 30))
    raise AssertionError("unreachable retry state")


def workspace_version(root):
    metadata = json.loads(run("cargo", "metadata", "--locked", "--no-deps",
                              "--format-version", "1", cwd=root))
    packages = [p for p in metadata["packages"] if p["name"] == "tinkle-ffi"
                and p["id"] in metadata["workspace_members"]]
    require(len(packages) == 1, "expected one workspace tinkle-ffi package")
    version = packages[0]["version"]
    require(version_valid(version), "invalid Cargo version")
    return version


def build_manifest(root, artifacts):
    version = workspace_version(root)
    revision = run("git", "rev-parse", "HEAD", cwd=root)
    run("git", "diff", "--exit-code", cwd=root)
    run("git", "diff", "--cached", "--exit-code", cwd=root)
    tag = "tinkle-ffi-v" + version
    manifest = {"schema_version": 1, "version": version, "source_revision": revision,
                "source_repository": REPOSITORY, "source_tag": tag, "artifacts": {}}
    for key, filename in ARTIFACTS.items():
        path = Path(artifacts) / filename
        require(path.is_file() and not path.is_symlink(), f"missing regular file: {path}")
        inspect_zip(path)
        manifest["artifacts"][key] = {
            "filename": filename,
            "url": f"https://github.com/{REPOSITORY}/releases/download/{tag}/{filename}",
            "sha256": digest(path),
        }
    return validate(manifest)


class ReleaseRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        parsed = urllib.parse.urlsplit(newurl)
        require(parsed.scheme == "https" and parsed.hostname in {
            "github.com", "release-assets.githubusercontent.com", "objects.githubusercontent.com"
        } and not parsed.username and not parsed.password and parsed.port in (None, 443),
                "untrusted release download redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def download(artifact, directory):
    # Only call with a previously validated manifest entry. Also enforce the URL
    # boundary here so callers cannot accidentally turn this into an arbitrary fetch.
    url = artifact["url"]
    prefix = f"https://github.com/{REPOSITORY}/releases/download/"
    require(url.startswith(prefix) and url.rsplit("/", 1)[-1] in ARTIFACTS.values(),
            "download outside allowed release artifacts")
    tag = url[len(prefix):].split("/")[0]
    require(tag.startswith("tinkle-ffi-v") and version_valid(tag[len("tinkle-ffi-v"):])
            and url == prefix + tag + "/" + artifact["filename"], "invalid download URL")
    target = Path(directory) / artifact["filename"]
    require(not target.is_symlink(), "symlink download destination")

    def fetch():
        opener = urllib.request.build_opener(ReleaseRedirect())
        with opener.open(url, timeout=120) as response, target.open("wb") as output:
            size = 0
            while chunk := response.read(1024 * 1024):
                size += len(chunk)
                require(size <= MAX_DOWNLOAD, "download too large")
                output.write(chunk)
    retry(fetch)
    require(digest(target) == artifact["sha256"], f"checksum mismatch: {target.name}")
    return target


def inspect_zip(path):
    require(Path(path).stat().st_size <= MAX_DOWNLOAD, "archive too large")
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        require(len(entries) <= MAX_ENTRIES, "too many ZIP entries")
        seen = set()
        total = 0
        for entry in entries:
            name = entry.filename
            parts = PurePosixPath(name).parts
            require(name and not name.startswith("/") and "\\" not in name
                    and "\x00" not in entry.orig_filename and ":" not in name
                    and all(p not in ("", ".", "..") for p in name.rstrip("/").split("/"))
                    and parts, f"unsafe archive path: {name!r}")
            normalized = name.rstrip("/").casefold()
            require(normalized not in seen, f"duplicate ZIP entry: {name}")
            seen.add(normalized)
            mode = entry.external_attr >> 16
            require(stat.S_IFMT(mode) in (0, stat.S_IFREG, stat.S_IFDIR),
                    f"symlink or special ZIP entry: {name}")
            require(not entry.flag_bits & 1, "encrypted ZIP unsupported")
            total += entry.file_size
            require(total <= MAX_EXPANDED, "expanded archive too large")
        # Reject a file used as a parent before extracting anything.
        files = {e.filename.rstrip("/").casefold() for e in entries if not e.is_dir()}
        for entry in entries:
            require(not any(str(p).casefold() in files
                            for p in PurePosixPath(entry.filename).parents if str(p) != "."),
                    "ZIP file/directory collision")
        return [entry.filename for entry in entries]


def extract_zip(path, destination):
    inspect_zip(path)
    destination = Path(destination)
    require(not any(p.is_symlink() for p in [destination, *destination.parents]),
            "symlink extraction directory")
    destination = destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path) as archive:
        for entry in archive.infolist():
            target = destination / entry.filename
            require(not any(p.is_symlink() for p in [target, *target.parents]),
                    "symlink extraction target")
            if entry.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                # Reading to EOF also validates ZIP CRCs. Total expansion was bounded above.
                with archive.open(entry) as source, target.open("wb") as output:
                    shutil.copyfileobj(source, output, 1024 * 1024)


def verify(manifest, directory, keys=None, fetch=False):
    validate(manifest)
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    for key in keys or ARTIFACTS:
        artifact = manifest["artifacts"][key]
        path = download(artifact, directory) if fetch else directory / artifact["filename"]
        require(path.is_file() and not path.is_symlink(), f"missing artifact: {path}")
        require(digest(path) == artifact["sha256"], f"checksum mismatch: {path.name}")
        names = set(inspect_zip(path))
        if key in ("swift_bindings", "kotlin_bindings"):
            with zipfile.ZipFile(path) as archive:
                require(sum(entry.file_size for entry in archive.infolist()) <= MAX_BINDINGS,
                        "generated binding archive too large")
        if key == "swift_bindings":
            require(names == {"TinkleSDK.swift", "TinkleSDKFFI.h", "module.modulemap"},
                    "unexpected Swift binding archive layout")
        if key == "kotlin_bindings":
            require({n for n in names if not n.endswith("/")} == {BINDINGS["kotlin"][1]},
                    "unexpected Kotlin binding archive layout")


def validate_debug(debug, manifest):
    validate(manifest)
    require(isinstance(debug, dict) and set(debug) == {
        "schema_version", "version", "source_revision", "source_repository", "source_tag",
        "sdk_release_sha256", "artifacts"}, "invalid debug asset manifest fields")
    require(type(debug["schema_version"]) is int and debug["schema_version"] == 1,
            "unsupported debug asset schema")
    for key in ("version", "source_revision", "source_repository", "source_tag"):
        require(debug[key] == manifest[key], "debug assets/source provenance mismatch")
    require(debug["sdk_release_sha256"] == hashlib.sha256(encoded(manifest)).hexdigest(),
            "debug assets/release manifest checksum mismatch")
    require(isinstance(debug["artifacts"], dict)
            and set(debug["artifacts"]) == set(DEBUG_ASSETS), "incorrect debug asset set")
    for key, filename in DEBUG_ASSETS.items():
        artifact = debug["artifacts"][key]
        require(isinstance(artifact, dict) and set(artifact) == {"filename", "url", "sha256"},
                "invalid debug asset fields")
        require(artifact["filename"] == filename and artifact["url"] ==
                f"https://github.com/{REPOSITORY}/releases/download/{manifest['source_tag']}/{filename}",
                "noncanonical debug asset URL/filename")
        require(isinstance(artifact["sha256"], str) and DIGEST.fullmatch(artifact["sha256"]),
                "invalid debug asset checksum")
    return debug


def debug_metadata(manifest, directory):
    """Tie the producer's per-ABI build IDs/checksums to the shipping ZIP bytes."""
    directory = Path(directory)
    path = directory / DEBUG_ASSETS["android_metadata"]
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 1024**2,
            "missing or oversized Android build metadata")
    metadata = json.loads(path.read_text(), object_pairs_hook=unique_object)
    require(isinstance(metadata, dict)
            and metadata.get("version") == manifest["version"]
            and metadata.get("source_revision") == manifest["source_revision"]
            and metadata.get("profile") == "android-release"
            and metadata.get("release_matrix") is True
            and metadata.get("min_sdk") == 24, "Android build metadata/provenance mismatch")
    require(isinstance(metadata.get("libraries"), dict)
            and set(metadata["libraries"]) == ANDROID_ABIS, "incomplete Android debug ABI matrix")
    symbols = directory / DEBUG_ASSETS["android_symbols"]
    require(symbols.is_file() and not symbols.is_symlink(), "missing Android debug symbols")
    names = inspect_zip(symbols)
    require({name for name in names if not name.endswith("/")} ==
            {f"{abi}/libtinkle_ffi.so.dbg" for abi in ANDROID_ABIS}, "incorrect debug symbol archive layout")
    with zipfile.ZipFile(directory / ARTIFACTS["android"]) as archive:
        for abi in ANDROID_ABIS:
            record = metadata["libraries"][abi]
            require(isinstance(record, dict) and isinstance(record.get("build_id"), str)
                    and SHA.fullmatch(record["build_id"]), "invalid Android GNU build ID")
            member = f"jniLibs/{abi}/libtinkle_ffi.so"
            with archive.open(member) as source:
                checksum = hashlib.sha256()
                while chunk := source.read(1024**2):
                    checksum.update(chunk)
            require(record.get("sha256") == checksum.hexdigest() and type(record.get("bytes")) is int
                    and record["bytes"] == archive.getinfo(member).file_size,
                    f"Android build metadata differs from shipped library: {abi}")


def build_debug_assets(manifest, directory):
    verify(manifest, directory)
    debug_metadata(manifest, directory)
    debug = {key: manifest[key] for key in (
        "schema_version", "version", "source_revision", "source_repository", "source_tag")}
    debug["sdk_release_sha256"] = hashlib.sha256(encoded(manifest)).hexdigest()
    debug["artifacts"] = {
        key: {"filename": filename,
              "url": f"https://github.com/{REPOSITORY}/releases/download/{manifest['source_tag']}/{filename}",
              "sha256": digest(Path(directory) / filename)}
        for key, filename in DEBUG_ASSETS.items()}
    return validate_debug(debug, manifest)


def verify_debug_assets(manifest, directory):
    path = Path(directory) / DEBUG_MANIFEST
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 65536,
            "missing or oversized debug asset manifest")
    debug = validate_debug(json.loads(path.read_text(), object_pairs_hook=unique_object), manifest)
    for artifact in debug["artifacts"].values():
        asset = Path(directory) / artifact["filename"]
        require(asset.is_file() and not asset.is_symlink() and digest(asset) == artifact["sha256"],
                "debug asset checksum mismatch: " + artifact["filename"])
    debug_metadata(manifest, directory)
    return debug


def write_scoped(root, relative, content):
    root_path = Path(root)
    require(not any(p.is_symlink() for p in [root_path, *root_path.parents]),
            "symlink promotion root")
    parts = PurePosixPath(str(relative)).parts
    require(not PurePosixPath(str(relative)).is_absolute() and ".." not in parts
            and all(p not in ("", ".") for p in parts), "unsafe promotion target")
    target = root_path.resolve() / relative
    require(not any(p.is_symlink() for p in [target, *target.parents]),
            f"symlink in promotion target: {relative}")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and target.read_bytes() == content:
        return False
    target.write_bytes(content)
    return True


def promote(manifest, artifacts, sdk, destination, check=False):
    key, source, target = BINDINGS[sdk]
    verify(manifest, artifacts, [key])
    with zipfile.ZipFile(Path(artifacts) / ARTIFACTS[key]) as archive:
        binding = archive.read(source)
    metadata = Path(destination) / "sdk-artifact.json"
    if metadata.exists():
        previous = load(metadata)
        if previous["version"] == manifest["version"]:
            require(previous == manifest, "refusing to rewrite an existing version's provenance")
        else:
            require(version_order(manifest["version"]) > version_order(previous["version"]),
                    f"refusing downgrade from {previous['version']} to {manifest['version']}")
    if check:
        require((Path(destination) / target).read_bytes() == binding, "generated binding drift")
        return False
    changed = write_scoped(destination, target, binding)
    return write_scoped(destination, "sdk-artifact.json", encoded(manifest)) or changed


def same_repository_prs(prs, repo):
    require(isinstance(prs, list) and all(isinstance(pr, dict) for pr in prs), "invalid promotion PR response")
    owner, name = repo.split("/")
    return [pr for pr in prs if pr.get("isCrossRepository") is False
            and (pr.get("headRepositoryOwner") or {}).get("login") == owner
            and (pr.get("headRepository") or {}).get("name") == name]


def open_pr(manifest, artifacts, sdk, repo):
    verify(manifest, artifacts, [BINDINGS[sdk][0]])
    require(repo == f"tinklehq/tinkle-sdk-{sdk}", "unexpected downstream repository")
    require(os.environ.get("SDK_DISTRIBUTION_TOKEN"),
            "BLOCKED: SDK_DISTRIBUTION_TOKEN is required for downstream promotion")
    env = dict(os.environ, GH_TOKEN=os.environ["SDK_DISTRIBUTION_TOKEN"])
    branch = f"sdk-release/{manifest['version']}"
    pr_args = ("gh", "pr", "list", "--repo", repo, "--head", branch, "--base", "main",
               "--state", "all", "--limit", "100", "--json",
               "url,state,mergeCommit,isCrossRepository,headRepositoryOwner,headRepository")
    prs = same_repository_prs(json.loads(retry(lambda: run(*pr_args, env=env))), repo)
    require(not any(p["state"] == "CLOSED" for p in prs), "promotion PR was closed; human review required")
    with tempfile.TemporaryDirectory() as tmp:
        # Resolve system symlinks (e.g. /var -> /private/var on macOS) so the
        # strict symlink-ancestor checks in promote/write_scoped see canonical paths.
        checkout = Path(tmp).resolve() / "sdk"
        retry(lambda: run("gh", "repo", "clone", repo, checkout, "--", "--branch", "main", env=env))
        # Never run downstream build scripts with the distribution credential.
        _, _, target = BINDINGS[sdk]
        for pr in prs:
            if pr["state"] == "MERGED":
                revision = (pr.get("mergeCommit") or {}).get("oid", "")
                require(SHA.fullmatch(revision), "merged promotion PR has no immutable merge commit")
                metadata = json.loads(run("git", "show", revision + ":sdk-artifact.json", cwd=checkout),
                                      object_pairs_hook=unique_object)
                require(validate(metadata) == manifest, "merged promotion has different source provenance")
                with zipfile.ZipFile(Path(artifacts) / ARTIFACTS[BINDINGS[sdk][0]]) as archive:
                    binding = archive.read(BINDINGS[sdk][1])
                require(git_file(checkout, revision, target) == binding,
                        "merged promotion has different generated bindings")
                print(pr["url"])
                return
        run("git", "checkout", "-b", branch, cwd=checkout)
        changed = promote(manifest, artifacts, sdk, checkout)
        if not changed:
            print(f"{repo}: source {manifest['source_revision']} already promoted")
            return
        # gh's credential helper avoids embedding the PAT in a URL or disk config.
        git_env = dict(env, GIT_CONFIG_COUNT="2", GIT_CONFIG_KEY_0="credential.helper",
                       GIT_CONFIG_VALUE_0="", GIT_CONFIG_KEY_1="credential.https://github.com.helper",
                       GIT_CONFIG_VALUE_1="!gh auth git-credential")
        def check_existing_branch():
            run("git", "fetch", "origin", f"refs/heads/{branch}", cwd=checkout, env=git_env)
            for path in (target, "sdk-artifact.json"):
                remote = git_file(checkout, "FETCH_HEAD", path)
                require(remote == (checkout / path).read_bytes(),
                        "existing promotion branch differs; refusing force push")
            base = run("git", "merge-base", "FETCH_HEAD", "origin/main", cwd=checkout)
            changes = set(run("git", "diff", "--name-only", base, "FETCH_HEAD", cwd=checkout).splitlines())
            require(changes <= {target, "sdk-artifact.json"}, "promotion branch has unrelated edits")
        # Inspect immutable proposals before any push. In particular, an old
        # merged/closed branch must never be fast-forwarded by a later replay.
        existing = retry(lambda: run("git", "ls-remote", "--heads", "origin", f"refs/heads/{branch}",
                                     cwd=checkout, env=git_env))
        if existing:
            check_existing_branch()
        else:
            run("git", "add", "--", target, "sdk-artifact.json", cwd=checkout)
            run("git", "-c", "user.name=SDK Distribution", "-c",
                "user.email=sdk-distribution@users.noreply.github.com", "commit", "-m",
                f"chore: release SDK v{manifest['version']}", cwd=checkout)
            try:
                retry(lambda: run("git", "push", "origin", f"HEAD:refs/heads/{branch}",
                                  cwd=checkout, env=git_env))
            except subprocess.CalledProcessError:
                # Covers concurrent identical proposals and an upload whose
                # successful response was lost. Never force an existing ref.
                check_existing_branch()
        if not prs:
            body = (f"Promote {REPOSITORY}@{manifest['source_revision']}\n\n"
                    f"Source tag: `{manifest['source_tag']}`\n\n"
                    "Includes only generated bindings and checksum-pinned sdk-artifact.json.\n"
                    "Merge after SDK CI passes; merging authorizes the SDK v tag. No auto-merge.")
            def ensure_pr():
                existing = same_repository_prs(json.loads(run(*pr_args, env=env)), repo)
                if existing:
                    require(not any(p["state"] == "CLOSED" for p in existing),
                            "promotion PR was closed; human review required")
                    return existing[0]["url"]
                return run("gh", "pr", "create", "--repo", repo, "--base", "main",
                           "--head", branch, "--title", f"chore: release SDK v{manifest['version']}",
                           "--body", body, env=env)
            print(retry(ensure_pr))
        else:
            print(prs[0]["url"])


def git_file(checkout, revision, relative):
    return subprocess.run(["git", "show", revision + ":" + relative], cwd=checkout,
                          check=True, stdout=subprocess.PIPE).stdout


def remote_tag_revision(tag):
    require(tag.startswith("tinkle-ffi-v") and version_valid(tag[len("tinkle-ffi-v"):]),
            "invalid core release tag")
    remote = json.loads(run("gh", "api", f"repos/{REPOSITORY}/git/ref/tags/{tag}"))["object"]
    for _ in range(5):
        if remote["type"] == "commit":
            break
        require(remote["type"] == "tag" and SHA.fullmatch(remote["sha"]), "invalid tag object")
        remote = json.loads(run("gh", "api", f"repos/{REPOSITORY}/git/tags/{remote['sha']}"))["object"]
    require(remote["type"] == "commit" and SHA.fullmatch(remote["sha"]), "invalid core tag revision")
    return remote["sha"]


def authorize_source(root, ref, publish):
    require(ref and not ref.startswith("-") and "\n" not in ref, "invalid source ref")
    revision = run("git", "rev-parse", "--verify", ref + "^{commit}", cwd=root)
    require(SHA.fullmatch(revision), "invalid resolved revision")
    require(run("git", "rev-parse", "HEAD", cwd=root) == revision, "checkout/ref mismatch")
    if publish:
        version = workspace_version(root)
        require(ref == "tinkle-ffi-v" + version, "publishing requires matching tinkle-ffi tag")
        require(run("git", "rev-parse", "refs/tags/" + ref + "^{commit}", cwd=root) == revision,
                "release ref must be an existing tag")
        run("git", "merge-base", "--is-ancestor", revision, "origin/main", cwd=root)
        branch = json.loads(run("gh", "api", f"repos/{REPOSITORY}/branches/main"))
        require(branch["protected"], "release source must belong to protected main")
        require(remote_tag_revision(ref) == revision,
                "remote source tag moved or differs from the tested revision")
    return revision


def publish(manifest, directory, root, debug_assets=False):
    verify(manifest, directory)
    authorize_source(root, manifest["source_tag"], True)
    require(run("git", "rev-parse", "HEAD", cwd=root) == manifest["source_revision"],
            "manifest source mismatch")
    # The release/tag must already exist. Never create or move a source tag here.
    tag = manifest["source_tag"]
    release = json.loads(run("gh", "api", f"repos/{REPOSITORY}/releases/tags/{tag}"))
    require(release["tag_name"] == tag, "release tag mismatch")
    files = [Path(directory) / name for name in ARTIFACTS.values()]
    files.append(Path(directory) / "sdk-release.json")
    require(files[-1].read_bytes() == encoded(manifest), "noncanonical manifest bytes")
    # Never silently discard producer symbols/metadata if they are in the candidate.
    if debug_assets or any((Path(directory) / name).exists()
                           for name in [DEBUG_MANIFEST, *DEBUG_ASSETS.values()]):
        debug = verify_debug_assets(manifest, directory)
        require((Path(directory) / DEBUG_MANIFEST).read_bytes() == encoded_debug(debug),
                "noncanonical debug manifest bytes")
        files.extend(Path(directory) / name for name in DEBUG_ASSETS.values())
        files.append(Path(directory) / DEBUG_MANIFEST)
    for path in files:
        def upload_one():
            current = json.loads(run("gh", "api", f"repos/{REPOSITORY}/releases/tags/{tag}"))
            existing = [a for a in current["assets"] if a["name"] == path.name]
            require(len(existing) <= 1, "duplicate release asset")
            if existing:
                # Use GitHub's digest if available; otherwise download and hash.
                remote_digest = existing[0].get("digest")
                if remote_digest:
                    require(remote_digest == "sha256:" + digest(path),
                            f"immutable release asset mismatch: {path.name}")
                else:
                    with tempfile.TemporaryDirectory() as tmp:
                        run("gh", "release", "download", tag, "--repo", REPOSITORY,
                            "--pattern", path.name, "--dir", tmp)
                        require(digest(Path(tmp) / path.name) == digest(path),
                                f"immutable release asset mismatch: {path.name}")
                return
            run("gh", "release", "upload", tag, path, "--repo", REPOSITORY)
        retry(upload_one)


def release_output(value):
    releases = json.loads(value or "[]")
    require(isinstance(releases, list), "invalid release-plz releases output")
    require(all(isinstance(item, dict) for item in releases), "invalid release-plz release entry")
    found = [r for r in releases if r.get("package_name") == "tinkle-ffi"]
    require(len(found) <= 1, "duplicate tinkle-ffi release")
    if not found:
        return ""
    item = found[0]
    require(version_valid(item["version"]) and item["tag"] == "tinkle-ffi-v" + item["version"],
            "invalid release-plz tag/version")
    return item["tag"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build-manifest")
    build.add_argument("--root", type=Path, default=Path("."))
    build.add_argument("--artifacts", type=Path, required=True)
    debug = commands.add_parser("build-debug-assets")
    debug.add_argument("--manifest", type=Path, required=True)
    debug.add_argument("--artifacts", type=Path, required=True)
    for name in ("verify", "promote", "publish"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--manifest", type=Path, required=True)
        cmd.add_argument("--artifacts", type=Path, required=True)
        if name != "publish":
            cmd.add_argument("--download", action="store_true")
        if name == "verify":
            cmd.add_argument("--keys", nargs="+", choices=ARTIFACTS)
        if name in ("verify", "publish"):
            cmd.add_argument("--debug-assets", action="store_true")
        if name == "promote":
            cmd.add_argument("--sdk", choices=BINDINGS, required=True)
            cmd.add_argument("--destination", type=Path)
            cmd.add_argument("--check", action="store_true")
            cmd.add_argument("--open-pr", action="store_true")
        if name == "publish":
            cmd.add_argument("--root", type=Path, default=Path("."))
    source = commands.add_parser("authorize-source")
    source.add_argument("--root", type=Path, default=Path("."))
    source.add_argument("--ref", required=True)
    source.add_argument("--publish", action="store_true")
    commands.add_parser("release-output")
    unpack = commands.add_parser("extract")
    unpack.add_argument("archive", type=Path)
    unpack.add_argument("destination", type=Path)
    args = parser.parse_args()
    if args.command == "build-manifest":
        manifest = build_manifest(args.root, args.artifacts)
        (args.artifacts / "sdk-release.json").write_bytes(encoded(manifest))
    elif args.command == "build-debug-assets":
        debug = build_debug_assets(load(args.manifest), args.artifacts)
        (args.artifacts / DEBUG_MANIFEST).write_bytes(encoded_debug(debug))
    elif args.command == "authorize-source":
        print(authorize_source(args.root, args.ref, args.publish))
    elif args.command == "release-output":
        print(release_output(os.environ.get("RELEASES", "[]")))
    elif args.command == "extract":
        extract_zip(args.archive, args.destination)
    else:
        manifest = load(args.manifest)
        if args.command == "verify":
            verify(manifest, args.artifacts, args.keys, args.download)
            if args.debug_assets:
                require(not args.download, "debug assets verification requires local release candidate files")
                verify_debug_assets(manifest, args.artifacts)
        elif args.command == "publish":
            publish(manifest, args.artifacts, args.root, args.debug_assets)
        else:
            if args.download:
                verify(manifest, args.artifacts, [BINDINGS[args.sdk][0]], True)
            if args.open_pr:
                require(not args.check and args.destination is None,
                        "--open-pr cannot be combined with --check/--destination")
                open_pr(manifest, args.artifacts, args.sdk, f"tinklehq/tinkle-sdk-{args.sdk}")
            else:
                require(args.destination is not None, "--destination or --open-pr required")
                promote(manifest, args.artifacts, args.sdk, args.destination, args.check)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, zipfile.BadZipFile, subprocess.CalledProcessError) as error:
        sys.exit(f"sdk-release: {error}")
