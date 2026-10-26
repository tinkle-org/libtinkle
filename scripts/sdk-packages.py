#!/usr/bin/env python3
"""Explicit SDK package layout; standard library only, no network or build outputs.

package_files(root, sdk) returns downstream-relative required paths from the local
catalog. snapshot(root, revision, sdk) reads catalog AND blobs from one exact git
commit. Snapshots are dict[str, bytes] with .modes (git 100644/100755 integers).

Layout follows https://github.com/reallyme/crypto: language packages live under
``packages/<sdk>/`` with a single source of truth in this repository.
"""

import json
from pathlib import Path, PurePosixPath
import re
import subprocess

SDKS = ("swift", "kotlin")
CATALOG = "packages/packages.json"
GENERATED = {"Sources/TinkleSDK/TinkleSDK.swift",
             "src/main/kotlin/com/tinkle/sdk/tinkle_ffi.kt",
             "sdk-artifact.json", "sdk-source-files.json"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def safe_path(path):
    require(isinstance(path, str) and bool(path), "invalid SDK package path")
    parts = path.split("/")
    require(all(re.fullmatch(r"[A-Za-z0-9_.-]+", p) and p not in (".", "..")
                and p.casefold() != ".git" for p in parts), "unsafe SDK package path: " + path)
    return path


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate catalog key: " + key)
        result[key] = value
    return result


def catalog(content):
    require(len(content) <= 65536, "SDK package catalog too large")
    data = json.loads(content, object_pairs_hook=unique_object)
    require(isinstance(data, dict) and set(data) == {"schema_version", *SDKS}
            and type(data["schema_version"]) is int and data["schema_version"] == 1,
            "unsupported SDK package catalog")
    for sdk in SDKS:
        paths = data[sdk]
        require(isinstance(paths, list) and paths, "empty SDK package layout: " + sdk)
        seen = set()
        for path in paths:
            safe_path(path)
            parts = PurePosixPath(path).parts
            require(path not in GENERATED and not any(p in {
                ".build", ".gradle", ".kotlin", "__pycache__", "Artifacts", "DerivedData"
            } for p in parts) and not path.endswith(".pyc")
                and not any(p == "build" and parts[max(0, i-2):i] != ("com", "tinkle")
                            for i, p in enumerate(parts)), "non-source SDK package path: " + path)
            folded = path.casefold()
            require(folded not in seen, "duplicate SDK package path: " + path)
            seen.add(folded)
        require(not any(str(parent).casefold() in seen for path in paths
                        for parent in PurePosixPath(path).parents if str(parent) != "."),
                "SDK package file/directory collision")
    return data


def regular_path(root, relative):
    """Reject links at every level, including dangling links; never resolve them."""
    safe_path(relative)
    root = Path(root).absolute()
    target = root / relative
    require(not any(p.is_symlink() for p in [target, *target.parents]),
            "symlink SDK package path: " + relative)
    require(target.is_file(), "missing regular SDK package file: " + relative)
    return target


def package_files(root, sdk):
    require(sdk in SDKS, "unknown SDK")
    paths = catalog(regular_path(root, CATALOG).read_bytes())[sdk]
    for path in paths:
        regular_path(root, f"packages/{sdk}/{path}")
    return tuple(paths)


class Snapshot(dict):
    def __init__(self, files=(), modes=None):
        super().__init__(files)
        self.modes = dict(modes or {})


def git_tree(root, revision):
    require(isinstance(revision, str) and re.fullmatch(r"[0-9a-f]{40}", revision),
            "SDK snapshot requires exact source SHA")
    resolved = subprocess.check_output(
        ["git", "rev-parse", "--verify", revision + "^{commit}"], cwd=root).decode().strip()
    require(resolved == revision, "SDK snapshot requires a commit SHA")
    raw = subprocess.check_output(["git", "ls-tree", "-rz", "--full-tree", revision], cwd=root)
    result = {}
    for record in raw.split(b"\0"):
        if record:
            info, path = record.split(b"\t", 1)
            mode, kind, oid = info.decode("ascii").split()
            result[path.decode("utf-8")] = (int(mode, 8), kind, oid)
    return result


def snapshot(root, revision, sdk):
    require(sdk in SDKS, "unknown SDK")
    tree = git_tree(root, revision)

    def read(path):
        entry = tree.get(path)
        require(entry is not None, "missing SDK source at " + revision + ": " + path)
        assert entry is not None
        mode, kind, oid = entry
        require(mode in (0o100644, 0o100755) and kind == "blob",
                "symlink or special SDK source: " + path)
        return subprocess.check_output(["git", "cat-file", "blob", oid], cwd=root), mode

    paths = catalog(read(CATALOG)[0])[sdk]
    result = Snapshot()
    for path in paths:
        content, mode = read(f"packages/{sdk}/{path}")
        result[path] = content
        result.modes[path] = mode
    return result
