"""Candidate orchestration regressions without Buf, Cargo, or network access."""

from contextlib import redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import call, patch


ROOT = Path(__file__).resolve().parents[1]
BINDINGS = ("swift/TinkleSDK.swift", "swift/TinkleSDKFFI.h", "swift/module.modulemap",
            "kotlin/com/tinkle/sdk/tinkle_ffi.kt")
SPEC = importlib.util.spec_from_file_location("sdk_ci_candidate", ROOT / "scripts/sdk-ci.py")
assert SPEC and SPEC.loader
ci = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci)


class CandidateBindingsTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve() / "repo"
        self.output = self.root.parent / "candidate"
        self.schema = self.root.parent / "schema"
        self.schema.mkdir()
        for name in ("scripts", "crates", ".cargo", "bindings"):
            (self.root / name).mkdir(parents=True)
        self.script = self.root / "scripts/schema-candidate.py"
        shutil.copy2(ROOT / "scripts/schema-candidate.py", self.script)
        for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "uniffi-bindgen.toml"):
            shutil.copy2(ROOT / name, self.root / name)
        (self.root / ".cargo/config.toml").write_text("")
        for relative in BINDINGS:
            path = self.root / "bindings" / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("original " + relative)

    def bindings(self):
        return {name: (self.root / "bindings" / name).read_bytes() for name in BINDINGS}

    def generate(self, missing=None, directory=False, syntra_version="1.0.0-rc.4"):
        def run(command, **kwargs):
            if command[:2] == ["buf", "build"]:
                Path(command[-1]).write_bytes(b"descriptor")
            elif command[:2] == ["buf", "generate"]:
                path = self.output / "prost-candidate/src/tinkle/v1/tinkle.v1.rs"
                path.parent.mkdir(parents=True)
                path.write_text("generated proto")
            elif command[:2] == ["bash", "scripts/generate-bindings.sh"]:
                for relative in BINDINGS:
                    path = self.output / "workspace/bindings" / relative
                    path.write_text("candidate " + relative)
                if missing:
                    path = self.output / "workspace/bindings" / missing
                    path.unlink()
                    if directory:
                        path.mkdir()
            return subprocess.CompletedProcess(command, 0)

        packages = [{"name": "tinkle-org_schema_community_neoeinstein-prost",
                     "manifest_path": str(self.output / "prost-candidate/Cargo.toml")}]
        packages += [{"name": name, "id": name, "version": syntra_version,
                      "source": "registry+https://github.com/rust-lang/crates.io-index"}
                     for name in ("syntra-core", "syntra-runtime", "syntra-net")]
        metadata = {"packages": packages, "resolve": {"nodes": [{"id": "syntra-net", "features": ["tcp"]}]}}
        with patch.object(sys, "argv", [str(self.script), "--schema", str(self.schema),
                                       "--output", str(self.output), "--bindings", "--", "true"]), \
                patch("subprocess.run", side_effect=run), \
                patch("subprocess.check_output", return_value=json.dumps(metadata).encode()), \
                redirect_stdout(io.StringIO()):
            runpy.run_path(str(self.script), run_name="__main__")

    def test_every_missing_or_non_file_source_leaves_all_destinations_untouched(self):
        original = self.bindings()
        for name in BINDINGS:
            for directory in (False, True):
                with self.subTest(name=name, directory=directory):
                    if self.output.exists():
                        shutil.rmtree(self.output)
                    with self.assertRaisesRegex(SystemExit, "candidate generator did not produce") as error:
                        self.generate(name, directory)
                    self.assertIn(name, str(error.exception))
                    self.assertEqual(self.bindings(), original)

    def test_complete_bindings_are_copied_without_changing_manifest_or_lock(self):
        originals = {name: (self.root / name).read_bytes() for name in ("Cargo.toml", "Cargo.lock")}
        self.generate()
        self.assertEqual(self.bindings(), {name: ("candidate " + name).encode() for name in BINDINGS})
        for name, content in originals.items():
            self.assertEqual((self.root / name).read_bytes(), content)

    def test_previous_syntra_release_is_rejected(self):
        """Candidate metadata must keep the exact published RC4 pin."""
        original = self.bindings()
        with self.assertRaisesRegex(SystemExit, "exact published Syntra rc.4"):
            self.generate(syntra_version="1.0.0-rc.3")
        self.assertEqual(self.bindings(), original)


class CandidateCiTests(unittest.TestCase):
    def test_missing_or_mutable_schema_ref_is_rejected_before_any_commands(self):
        for revision in ("", "main", "v1", "a" * 39, "a" * 40 + "\n", "a" * 40 + ",branch=other"):
            with self.subTest(revision=revision), \
                    patch.dict(os.environ, {"SCHEMA_CANDIDATE_REF": revision}), \
                    patch.object(ci, "call") as invoke:
                with self.assertRaisesRegex(ValueError, "full Git SHA"):
                    ci.schema_candidate(Path("unused"))
                invoke.assert_not_called()

    def test_cli_exports_exact_schema_then_uses_existing_candidate_flow(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp).resolve() / "candidate"
            revision = "a" * 40
            with patch.dict(os.environ, {"SCHEMA_CANDIDATE_REF": revision}), \
                    patch.object(sys, "argv", ["sdk-ci.py", "schema-candidate", "--output", str(output)]), \
                    patch.object(ci, "call") as invoke:
                ci.main()
            self.assertEqual(invoke.call_args_list, [
                call("buf", "export", f"https://github.com/tinkle-org/schema.git#ref={revision},depth=0",
                     "--output", output / "schema"),
                call(sys.executable, ci.CORE_ROOT / "scripts/schema-candidate.py", "--schema", output / "schema",
                     "--output", output, "--", "true"),
            ])

    def test_schema_export_failure_never_starts_candidate_build(self):
        with tempfile.TemporaryDirectory() as tmp, \
                patch.dict(os.environ, {"SCHEMA_CANDIDATE_REF": "a" * 40}), \
                patch.object(ci, "call", side_effect=subprocess.CalledProcessError(1, "buf")) as invoke:
            with self.assertRaises(subprocess.CalledProcessError):
                ci.schema_candidate(Path(tmp))
            self.assertEqual(invoke.call_count, 1)


if __name__ == "__main__":
    unittest.main()
