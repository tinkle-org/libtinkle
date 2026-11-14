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
        """Create an isolated candidate workspace with original binding sentinels."""
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve() / "repo"
        self.output = self.root.parent / "candidate"
        self.schema = self.root.parent / "schema"
        self.schema.mkdir()
        for name in ("scripts", "crates", ".cargo", "bindings", "packages"):
            (self.root / name).mkdir(parents=True)
        self.script = self.root / "scripts/schema-candidate.py"
        shutil.copy2(ROOT / "scripts/schema-candidate.py", self.script)
        shutil.copy2(ROOT / "scripts/generate-bindings.sh", self.root / "scripts/generate-bindings.sh")
        for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "uniffi-bindgen.toml"):
            shutil.copy2(ROOT / name, self.root / name)
        (self.root / ".cargo/config.toml").write_text("")
        for relative in BINDINGS:
            path = self.root / "bindings" / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("original " + relative)

    def bindings(self):
        """Snapshot every destination binding so tests can detect partial updates."""
        return {name: (self.root / "bindings" / name).read_bytes() for name in BINDINGS}

    def generate(self, missing=None, directory=False, syntra_version="1.0.0-rc.5",
                 bindings_runner=None):
        """Run candidate generation with stubbed tools and optional broken outputs."""
        def run(command, **kwargs):
            """Emulate Buf and bindgen outputs, or delegate to a supplied generator."""
            if command[:2] == ["buf", "build"]:
                Path(command[-1]).write_bytes(b"descriptor")
            elif command[:2] == ["buf", "generate"]:
                path = self.output / "prost-candidate/src/tinkle/v1/tinkle.v1.rs"
                path.parent.mkdir(parents=True)
                path.write_text("generated proto")
            elif command[:2] == ["bash", "scripts/generate-bindings.sh"]:
                if bindings_runner is not None:
                    return bindings_runner(command, **kwargs)
                if command[-1] == "--check":
                    return subprocess.CompletedProcess(command, 0)
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
        """Validate the complete output set before replacing any checked-in binding."""
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
        """Copy valid candidate bindings without changing the published Cargo pins."""
        originals = {name: (self.root / name).read_bytes() for name in ("Cargo.toml", "Cargo.lock")}
        self.generate()
        self.assertEqual(self.bindings(), {name: ("candidate " + name).encode() for name in BINDINGS})
        for name, content in originals.items():
            self.assertEqual((self.root / name).read_bytes(), content)

    def test_successful_check_without_generation_leaves_bindings_and_pins_unchanged(self):
        """A successful no-op stub checks existing workspace copies; it produces no new bytes."""
        original = self.bindings()
        pins = {name: (self.root / name).read_bytes() for name in ("Cargo.toml", "Cargo.lock")}
        modes = []

        def no_generation(command, **kwargs):
            """Record verification modes without claiming to generate or remove any file."""
            modes.append(command[-1])
            return subprocess.CompletedProcess(command, 0)

        self.generate(bindings_runner=no_generation)
        self.assertEqual(modes, ["--write", "--check"])
        self.assertEqual(self.bindings(), original)
        for name, content in pins.items():
            self.assertEqual((self.root / name).read_bytes(), content)

    def test_real_generator_missing_staged_binding_rejects_existing_destination(self):
        """An old destination is not evidence that bindgen produced a staged file."""
        original = self.bindings()
        generated = self.root.parent / "bindgen-output"
        missing = "kotlin/com/tinkle/sdk/tinkle_ffi.kt"
        for source, destination in (
            ("swift/TinkleSDK.swift", "TinkleSDK.swift"),
            ("swift/TinkleSDKFFI.h", "TinkleSDKFFI.h"),
            ("swift/module.modulemap", "TinkleSDKFFI.modulemap"),
        ):
            target = generated / destination
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / "bindings" / source, target)
        tools = self.root.parent / "bin"
        tools.mkdir()
        cargo = tools / "cargo"
        cargo.write_text(f"""#!{sys.executable}
import json
from pathlib import Path
import shutil
import sys
if sys.argv[1] == "metadata":
    print(json.dumps({{"target_directory": {str(self.output / 'target')!r}}}))
elif sys.argv[1] == "run":
    stage = Path(sys.argv[sys.argv.index("--out-dir") + 1])
    shutil.copytree({str(generated)!r}, stage, dirs_exist_ok=True)
elif sys.argv[1] != "rustc":
    raise SystemExit("unexpected Cargo command")
""")
        cargo.chmod(0o755)
        real_run = subprocess.run
        with patch.dict(os.environ, {"PATH": str(tools) + os.pathsep + os.environ["PATH"]}):
            with self.assertRaises(subprocess.CalledProcessError) as error:
                self.generate(bindings_runner=lambda command, **kwargs: real_run(
                    command, **kwargs, capture_output=True, text=True))
        self.assertEqual(error.exception.returncode, 1)
        self.assertIn("generator did not produce com/tinkle/sdk/tinkle_ffi.kt", error.exception.stderr)
        self.assertEqual(self.bindings(), original)
        self.assertTrue((self.output / "workspace/bindings" / missing).is_file())

    def test_previous_syntra_release_is_rejected(self):
        """Candidate metadata must keep the exact published release pin."""
        original = self.bindings()
        with self.assertRaisesRegex(SystemExit, "exact published Syntra rc.5"):
            self.generate(syntra_version="1.0.0-rc.4")
        self.assertEqual(self.bindings(), original)


class CandidateCiTests(unittest.TestCase):
    def test_output_cannot_overwrite_packages_or_repository_ancestors(self):
        """Reject output overlapping owned package sources or an ancestor of the repo."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            script = root / "scripts/schema-candidate.py"
            script.parent.mkdir(parents=True)
            shutil.copy2(ROOT / "scripts/schema-candidate.py", script)
            for output in (root / "packages", root / "packages/swift", root.parent):
                with self.subTest(output=output), patch.object(sys, "argv", [
                    str(script), "--output", str(output)
                ]), patch("subprocess.run") as invoke:
                    with self.assertRaisesRegex(SystemExit, "separate temporary/build directory"):
                        runpy.run_path(str(script), run_name="__main__")
                    invoke.assert_not_called()

    def test_missing_or_mutable_schema_ref_is_rejected_before_any_commands(self):
        """Reject absent, mutable, or malformed schema revisions before invoking tools."""
        for revision in ("", "main", "v1", "a" * 39, "a" * 40 + "\n", "a" * 40 + ",branch=other"):
            with self.subTest(revision=revision), \
                    patch.dict(os.environ, {"SCHEMA_CANDIDATE_REF": revision}), \
                    patch.object(ci, "call") as invoke:
                with self.assertRaisesRegex(ValueError, "full Git SHA"):
                    ci.schema_candidate(Path("unused"))
                invoke.assert_not_called()

    def test_cli_exports_exact_schema_then_uses_existing_candidate_flow(self):
        """Export the immutable schema revision before invoking the candidate helper."""
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
        """Stop candidate orchestration immediately if schema export fails."""
        with tempfile.TemporaryDirectory() as tmp, \
                patch.dict(os.environ, {"SCHEMA_CANDIDATE_REF": "a" * 40}), \
                patch.object(ci, "call", side_effect=subprocess.CalledProcessError(1, "buf")) as invoke:
            with self.assertRaises(subprocess.CalledProcessError):
                ci.schema_candidate(Path(tmp))
            self.assertEqual(invoke.call_count, 1)


if __name__ == "__main__":
    unittest.main()
