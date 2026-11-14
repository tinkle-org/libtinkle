"""Isolated Rust schema-candidate checks without Buf, Cargo, or network access."""

from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


class SchemaCandidateTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve() / "repo"
        self.output = self.root.parent / "candidate"
        self.schema = self.root.parent / "schema"
        self.schema.mkdir()
        for name in ("scripts", "crates", ".cargo"):
            (self.root / name).mkdir(parents=True)
        self.script = self.root / "scripts/schema-candidate.py"
        shutil.copy2(ROOT / "scripts/schema-candidate.py", self.script)
        for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml"):
            shutil.copy2(ROOT / name, self.root / name)
        (self.root / ".cargo/config.toml").write_text("")

    def generate(self, syntra_version="1.0.0-rc.5", fail_command=False):
        commands = []
        def run(command, **kwargs):
            commands.append((command, kwargs))
            if command[:2] == ["buf", "build"]:
                Path(command[-1]).write_bytes(b"descriptor")
            elif command[:2] == ["buf", "generate"]:
                path = self.output / "prost-candidate/src/tinkle/v1/tinkle.v1.rs"
                path.parent.mkdir(parents=True)
                path.write_text("generated proto")
            elif command == ["true"] and fail_command:
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0)

        packages = [{"name": "tinkle-org_schema_community_neoeinstein-prost",
                     "manifest_path": str(self.output / "prost-candidate/Cargo.toml")}]
        packages += [{"name": name, "id": name, "version": syntra_version,
                      "source": "registry+https://github.com/rust-lang/crates.io-index"}
                     for name in ("syntra-core", "syntra-runtime", "syntra-net")]
        metadata = {"packages": packages, "resolve": {"nodes": [{"id": "syntra-net", "features": ["tcp"]}]}}
        args = [str(self.script), "--schema", str(self.schema), "--output", str(self.output)]
        with patch.object(sys, "argv", [*args, "--", "true"]), \
                patch("subprocess.run", side_effect=run), \
                patch("subprocess.check_output", return_value=json.dumps(metadata).encode()), \
                redirect_stdout(io.StringIO()):
            runpy.run_path(str(self.script), run_name="__main__")
        return commands

    def test_candidate_runs_in_isolated_workspace_without_changing_source_pins(self):
        originals = {name: (self.root / name).read_bytes() for name in ("Cargo.toml", "Cargo.lock")}
        commands = self.generate()
        command, options = commands[-1]
        self.assertEqual(command, ["true"])
        self.assertEqual(options["cwd"], self.output / "workspace")
        self.assertTrue(options["check"])
        self.assertEqual((self.output / "prost-candidate/src/public.binpb").read_bytes(), b"descriptor")
        evidence = json.loads((self.output / "candidate.json").read_text())
        self.assertEqual(evidence["workspace"], str(self.output / "workspace"))
        for name, content in originals.items():
            self.assertEqual((self.root / name).read_bytes(), content)

    def test_command_failure_propagates(self):
        with self.assertRaises(subprocess.CalledProcessError):
            self.generate(fail_command=True)

    def test_dependency_pin_is_enforced(self):
        with self.assertRaisesRegex(SystemExit, "exact published Syntra rc.5"):
            self.generate(syntra_version="1.0.0-rc.4")

    def test_output_cannot_overwrite_sources_or_repository_ancestors(self):
        for output in (self.root / "crates", self.root / "scripts", self.root.parent):
            with self.subTest(output=output), patch.object(sys, "argv", [
                str(self.script), "--output", str(output)
            ]), patch("subprocess.run") as invoke:
                with self.assertRaisesRegex(SystemExit, "separate temporary/build directory"):
                    runpy.run_path(str(self.script), run_name="__main__")
                invoke.assert_not_called()


if __name__ == "__main__":
    unittest.main()
