"""Package catalog and git snapshot checks for packages/ layout.

Follows https://github.com/reallyme/crypto: language packages live under
``packages/<sdk>/`` with a single source of truth in this repository.
Uses temp git fixtures so tests run without requiring a real commit.
"""

import importlib.util
import json
import subprocess
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "sdk_packages", ROOT / "scripts/sdk-packages.py")
assert SPEC and SPEC.loader
packages = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(packages)


def git(*args, cwd):
    subprocess.run(["git", *args], cwd=cwd, check=True,
                   capture_output=True)


class PackageLayoutTests(unittest.TestCase):
    def test_catalog_matches_working_tree(self):
        for sdk in packages.SDKS:
            with self.subTest(sdk=sdk):
                paths = packages.package_files(ROOT, sdk)
                self.assertTrue(paths)
                for rel in paths:
                    full = ROOT / "packages" / sdk / rel
                    self.assertTrue(full.is_file(), f"Missing package source: packages/{sdk}/{rel}")
                    self.assertFalse(full.is_symlink())

    def test_catalog_rejects_generated_and_build_outputs(self):
        for bad in ("Sources/TinkleSDK/TinkleSDK.swift",
                    "src/main/kotlin/com/tinkle/sdk/tinkle_ffi.kt",
                    "sdk-artifact.json", "sdk-source-files.json",
                    "build/output.o", ".build/manifest", "sample/build/x"):
            with self.subTest(path=bad):
                data = {"schema_version": 1, "swift": [bad], "kotlin": ["README.md"]}
                if bad.startswith("src/") or bad.startswith("sample/") or bad.startswith("build/"):
                    data = {"schema_version": 1, "swift": ["README.md"], "kotlin": [bad]}
                with self.assertRaises(ValueError):
                    packages.catalog(json.dumps(data).encode())

    def test_safe_path_rejects_traversal_and_git(self):
        for bad in ("", "../escape", "/absolute", "a/../b", ".git/config",
                    "a/.git/b", "a\\b", "a:colon"):
            with self.subTest(path=bad):
                with self.assertRaises(ValueError):
                    packages.safe_path(bad)

    def make_repo(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name).resolve()
        git("init", cwd=root)
        git("config", "user.name", "test", cwd=root)
        git("config", "user.email", "test@example.com", cwd=root)
        catalog = {"schema_version": 1, "swift": ["README.md"], "kotlin": ["README.md"]}
        (root / "packages/swift").mkdir(parents=True)
        (root / "packages/kotlin").mkdir(parents=True)
        (root / "packages/packages.json").write_text(json.dumps(catalog))
        (root / "packages/swift/README.md").write_text("swift\n")
        (root / "packages/kotlin/README.md").write_text("kotlin\n")
        git("add", ".", cwd=root)
        git("commit", "-m", "init", cwd=root)
        sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root).decode().strip()
        return root, sha

    def test_snapshot_reads_exact_commit_not_worktree(self):
        root, sha = self.make_repo()
        snap = packages.snapshot(root, sha, "swift")
        self.assertEqual(snap["README.md"], b"swift\n")
        # Dirty worktree must not affect the committed snapshot.
        (root / "packages/swift/README.md").write_text("dirty\n")
        snap2 = packages.snapshot(root, sha, "swift")
        self.assertEqual(snap2["README.md"], b"swift\n")
        # Non-commit revisions and missing SHAs fail closed.
        with self.assertRaises(ValueError):
            packages.snapshot(root, "main", "swift")
        with self.assertRaises(Exception):
            packages.snapshot(root, "0" * 40, "swift")

    def test_snapshot_modes_preserved(self):
        root, sha = self.make_repo()
        snap = packages.snapshot(root, sha, "swift")
        self.assertEqual(snap.modes["README.md"], 0o100644)


if __name__ == "__main__":
    unittest.main()
