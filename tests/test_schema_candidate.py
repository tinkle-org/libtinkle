"""Isolated candidate checks without Buf, Cargo, or network; cfg checks use rustc."""

from contextlib import redirect_stdout
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

    def generate(self, syntra_version="1.0.0-rc.5", fail_command=False, room_queries=False,
                 omit_output=None):
        """Run the candidate helper on disposable fixtures with Buf and Cargo mocked."""
        commands = []
        def record(command, **kwargs):
            # Keep only assertions about owned overrides, never a diagnostic copy of the env.
            env = kwargs.pop("env", {})
            options = {"WIRE_ID": "compact-wire-reset-v1", "ASYNC_ACTIONS": "1",
                       "MEDIA_CONTRACT": "singular-thumbnail-compact-wire-reset-v1", "ROOM_QUERIES": "1"}
            for name, expected in options.items():
                key = "TINKLE_SCHEMA_CANDIDATE_" + name
                if key in env:
                    kwargs[name] = env[key] == expected
            commands.append((command, kwargs))

        def run(command, **kwargs):
            record(command, **kwargs)
            if command[:2] == ["buf", "build"] and omit_output != "descriptor":
                Path(command[-1]).write_bytes(b"descriptor")
            elif command[:2] == ["buf", "generate"] and omit_output != "prost":
                path = self.output / "prost-candidate/src/tinkle/v1/tinkle.v1.rs"
                path.parent.mkdir(parents=True, exist_ok=True)
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

        def check_output(command, **kwargs):
            record(command, **kwargs)
            return json.dumps(metadata).encode()

        args = [str(self.script), "--schema", str(self.schema), "--output", str(self.output), "--async-actions"]
        if room_queries:
            args.append("--room-queries")
        with patch.object(sys, "argv", [*args, "--", "true"]), \
                patch("subprocess.run", side_effect=run), \
                patch("subprocess.check_output", side_effect=check_output), \
                redirect_stdout(io.StringIO()):
            runpy.run_path(str(self.script), run_name="__main__")
        return commands

    def test_async_actions_capability_is_explicit_and_isolated(self):
        commands = self.generate()
        self.assertTrue(commands[-1][1].get("ASYNC_ACTIONS"))
        evidence = json.loads((self.output / "candidate.json").read_text())
        self.assertTrue(evidence["async_actions"])
        self.assertEqual(evidence["media_contract"], "singular-thumbnail-compact-wire-reset-v1")
        self.assertTrue(commands[-1][1].get("MEDIA_CONTRACT"))

    def test_reset_identity_overrides_stale_environment(self):
        with patch.dict("os.environ", {
            "TINKLE_SCHEMA_CANDIDATE_WIRE_ID": "stale-candidate",
            "TINKLE_SCHEMA_CANDIDATE_MEDIA_CONTRACT": "singular-thumbnail-v1",
        }):
            commands = self.generate()
        evidence = json.loads((self.output / "candidate.json").read_text())
        self.assertEqual(evidence["wire_identity"], "compact-wire-reset-v1")
        for command, options in commands:
            if command[0] != "buf":
                self.assertTrue(options.get("WIRE_ID"), f"wrong wire identity for {command[:2]}")

    def test_room_queries_require_candidate_opt_in(self):
        key = "TINKLE_SCHEMA_CANDIDATE_ROOM_QUERIES"
        with patch.dict(os.environ, {key: "1"}):
            for command, options in self.generate():
                self.assertNotIn("ROOM_QUERIES", options, f"unexpected opt-in for {command[:2]}")
        commands = self.generate(room_queries=True)
        for command, options in commands:
            if command[0] != "buf":
                self.assertTrue(options.get("ROOM_QUERIES"), f"missing opt-in for {command[:2]}")
        self.assertTrue(json.loads((self.output / "candidate.json").read_text())["room_queries"])

    def test_generator_uses_one_isolated_source_snapshot(self):
        """Both generation operations consume the same frozen source, never the live checkout."""
        (self.schema / "media.proto").write_text("source before generation")
        commands = self.generate()
        inputs = [command[2] for command, _ in commands if command[:2] in (["buf", "build"], ["buf", "generate"])]
        self.assertEqual(inputs, [str(self.output / "schema-source")] * 2)
        self.assertEqual((self.output / "schema-source/media.proto").read_text(), "source before generation")

    def test_source_hash_and_snapshot_exclude_build_and_git_directories(self):
        """Ignored protos/configs are neither traversed for hashing nor copied into generation."""
        (self.schema / "media.proto").write_text("included source")
        (self.schema / "buf.yaml").write_text("included config")
        self.generate()
        before = json.loads((self.output / "candidate.json").read_text())["schema_source_sha256"]
        for parent in (self.schema, self.schema / "nested"):
            for name in (".git", "node_modules", "target", "__pycache__"):
                ignored = parent / name
                ignored.mkdir(parents=True)
                (ignored / "excluded.proto").write_text("not a generation input")
                (ignored / "buf.yaml").write_text("not a generation config")
        original_read = Path.read_bytes

        def read_input(path):
            """Fail if hashing attempts to read any ignored schema input."""
            if path.is_relative_to(self.schema):
                self.assertFalse(set(path.relative_to(self.schema).parts) & {
                    ".git", "node_modules", "target", "__pycache__"
                })
            return original_read(path)

        with patch.object(Path, "read_bytes", read_input):
            self.generate()
        evidence = json.loads((self.output / "candidate.json").read_text())
        self.assertEqual(evidence["schema_source_sha256"], before)
        snapshot = self.output / "schema-source"
        self.assertEqual((snapshot / "media.proto").read_text(), "included source")
        self.assertEqual((snapshot / "buf.yaml").read_text(), "included config")
        for parent in (snapshot, snapshot / "nested"):
            for name in (".git", "node_modules", "target", "__pycache__"):
                self.assertFalse((parent / name).exists())
        (self.schema / "media.proto").write_text("changed included source")
        self.generate()
        self.assertNotEqual(
            json.loads((self.output / "candidate.json").read_text())["schema_source_sha256"], before
        )

    def test_candidate_requires_explicit_actions_before_creating_output(self):
        stderr = io.StringIO()
        with patch.dict("os.environ", {"TINKLE_SCHEMA_CANDIDATE_ASYNC_ACTIONS": "1"}), \
                patch.object(sys, "argv", [str(self.script), "--output", str(self.output)]), \
                patch("subprocess.run") as invoke, patch.object(sys, "stderr", stderr):
            with self.assertRaises(SystemExit) as error:
                runpy.run_path(str(self.script), run_name="__main__")
            self.assertEqual(error.exception.code, 2)
            self.assertIn("required: --async-actions", stderr.getvalue())
            invoke.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_repeat_copy_removes_deleted_sources(self):
        workspace = self.output / "workspace"
        workspace.mkdir(parents=True)
        stale = workspace / "deleted.rs"
        stale.write_text("stale")
        self.generate()
        self.assertFalse(stale.exists())

    def test_repeat_generation_cannot_use_stale_outputs(self):
        for omitted in ("descriptor", "prost"):
            with self.subTest(omitted=omitted):
                self.generate()
                with self.assertRaisesRegex(SystemExit, "candidate .* did not produce"):
                    self.generate(omit_output=omitted)
                self.assertFalse((self.output / "candidate.json").exists())

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
        self.generate()
        with self.assertRaises(subprocess.CalledProcessError):
            self.generate(fail_command=True)
        self.assertFalse((self.output / "candidate.json").exists())

    def test_dependency_pin_is_enforced(self):
        with self.assertRaisesRegex(SystemExit, "exact published Syntra rc.5"):
            self.generate(syntra_version="1.0.0-rc.4")
        self.assertFalse((self.output / "candidate.json").exists())

    def test_output_cannot_overwrite_sources_or_repository_ancestors(self):
        for output in (self.root / "crates", self.root / "scripts", self.root.parent):
            with self.subTest(output=output), patch.object(sys, "argv", [
                str(self.script), "--async-actions", "--output", str(output)
            ]), patch("subprocess.run") as invoke:
                with self.assertRaisesRegex(SystemExit, "separate temporary/build directory"):
                    runpy.run_path(str(self.script), run_name="__main__")
                invoke.assert_not_called()


class SchemaCfgTests(unittest.TestCase):
    """Run the real shared build script to verify fail-closed capability combinations."""

    @classmethod
    def setUpClass(cls):
        """Compile once without Cargo or dependency resolution."""
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.binary = Path(cls.temp.name) / "schema-cfg"
        subprocess.run(["rustc", str(ROOT / "scripts/schema-cfg.rs"), "-o", str(cls.binary)], check=True)

    def configure(self, **variables):
        """Do not inherit candidate flags from the test runner."""
        env = {"TINKLE_SCHEMA_CANDIDATE_" + key: value for key, value in variables.items()}
        return subprocess.run([str(self.binary)], env=env, capture_output=True, text=True)

    def test_published_and_explicit_candidate_have_same_contract(self):
        """Selecting a registry or isolated source does not change the wire contract."""
        published = self.configure()
        self.assertEqual(published.returncode, 0, published.stderr)
        self.assertIn("cargo:rustc-env=TINKLE_SCHEMA_WIRE_ID=compact-wire-reset-v1", published.stdout)
        self.assertIn("cargo:rustc-cfg=tinkle_schema_candidate", published.stdout)
        self.assertIn("cargo:rustc-cfg=tinkle_async_actions", published.stdout)
        candidate = self.configure(WIRE_ID="compact-wire-reset-v1", ASYNC_ACTIONS="1",
                                   MEDIA_CONTRACT="singular-thumbnail-compact-wire-reset-v1")
        self.assertEqual(candidate.returncode, 0, candidate.stderr)
        self.assertNotIn("cargo:rustc-cfg=tinkle_room_queries", candidate.stdout)
        self.assertIn("cargo:rustc-cfg=tinkle_async_actions", candidate.stdout)
        self.assertIn("cargo:rustc-cfg=tinkle_schema_candidate", candidate.stdout)
        self.assertIn("cargo:rustc-env=TINKLE_SCHEMA_WIRE_ID=compact-wire-reset-v1", candidate.stdout)

    def test_invalid_or_incomplete_build_flags_fail_closed(self):
        """Typos and the previous candidate capability cannot fall back to published identity."""
        for variables in (
            {"WIRE_ID": ""}, {"WIRE_ID": "unknown"}, {"ASYNC_ACTIONS": "true"},
            {"ASYNC_ACTIONS": ""}, {"ASYNC_ACTIONS": "1"},
            {"ASYNC_ACTIONS": "1", "MEDIA_CONTRACT": "singular-thumbnail-v1"},
            {"WIRE_ID": "compact-wire-reset-v1", "ASYNC_ACTIONS": "1"},
            {"WIRE_ID": "compact-wire-reset-v1", "ASYNC_ACTIONS": "1", "MEDIA_CONTRACT": "singular-thumbnail-v1"},
            {"MEDIA_CONTRACT": "singular-thumbnail-compact-wire-reset-v1"},
            {"WIRE_ID": "compact-wire-reset-v1", "ASYNC_ACTIONS": "0"},
        ):
            with self.subTest(variables=variables):
                self.assertNotEqual(self.configure(**variables).returncode, 0)

    def test_room_queries_are_published_or_explicitly_selected_for_candidates(self):
        self.assertIn("cargo:rustc-cfg=tinkle_room_queries", self.configure().stdout)
        self.assertNotEqual(self.configure(ROOM_QUERIES="1").returncode, 0)
        candidate = {"WIRE_ID": "compact-wire-reset-v1", "ASYNC_ACTIONS": "1",
                     "MEDIA_CONTRACT": "singular-thumbnail-compact-wire-reset-v1"}
        selected = self.configure(**candidate, ROOM_QUERIES="1")
        self.assertEqual(selected.returncode, 0, selected.stderr)
        self.assertIn("cargo:rustc-cfg=tinkle_room_queries", selected.stdout)
        self.assertNotEqual(self.configure(**candidate, ROOM_QUERIES="true").returncode, 0)

    def test_invalid_flag_diagnostics_do_not_echo_values(self):
        """Test only the intended diagnostic, not environment or raw child output."""
        marker = "synthetic-private-value"
        with patch.dict(os.environ, {"UNRELATED_TEST_CREDENTIAL": marker}):
            result = self.configure(WIRE_ID=marker)
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue("invalid candidate contract" in result.stderr)
        self.assertFalse(marker in result.stdout or marker in result.stderr,
                         "build diagnostics exposed an environment value")

if __name__ == "__main__":
    unittest.main()
