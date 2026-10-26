"""Consumer selection and fresh-checkout preflight, without network/toolchains."""

import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("sdk_ci_consumers", ROOT / "scripts/sdk-ci.py")
assert SPEC and SPEC.loader
ci = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci)


class ConsumerTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.pins = self.root / "pins.json"
        self.data = {"schema_version": 1, "swift": "a" * 40, "kotlin": "b" * 40}
        self.pins.write_text(json.dumps(self.data))

    def tree(self, sdk):
        try:
            names = ci.local_package_files(sdk)
        except ValueError:
            names = ci.CONSUMER_FILES[sdk]
        return {"truncated": False, "tree": [
            {"path": name, "type": "blob", "mode": "100644"}
            for name in names]}

    def test_pr_without_inputs_uses_checked_in_immutable_pins(self):
        self.assertEqual(ci.consumer_refs(self.pins), {"swift": "a" * 40, "kotlin": "b" * 40})
        checked_in = ci.consumer_refs(ROOT / "scripts/sdk-consumers.json")
        for ref in checked_in.values():
            self.assertRegex(ref, r"^[0-9a-f]{40}$")

    def test_explicit_validation_sha_overrides_only_selected_consumer(self):
        self.assertEqual(ci.consumer_refs(self.pins, swift_ref="c" * 40),
                         {"swift": "c" * 40, "kotlin": "b" * 40})
        for value in ("main", "feat/bootstrap-sdk", "v1.0.0", "a" * 39, "A" * 40, "a" * 40 + "\n"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "commit SHA"):
                ci.consumer_refs(self.pins, kotlin_ref=value)

    def test_publish_ignores_pin_file_and_requires_main(self):
        missing = self.root / "absent.json"
        for refs in ({}, {"swift_ref": "main", "kotlin_ref": "main"}):
            self.assertEqual(ci.consumer_refs(missing, publish=True, **refs),
                             {"swift": "main", "kotlin": "main"})
        for value in ("feat/bootstrap-sdk", "b" * 40):
            with self.subTest(value=value), patch.object(ci.release, "run") as command:
                with self.assertRaisesRegex(ValueError, "Release consumers must be main"):
                    ci.pin_consumers(missing, publish=True, kotlin_ref=value)
                command.assert_not_called()

    def test_pin_schema_rejects_missing_unknown_duplicate_or_mutable_values(self):
        invalid = [[], dict(self.data, schema_version=True), dict(self.data, schema_version=2),
                   dict(self.data, repository="other/repo"), {"schema_version": 1, "swift": "a" * 40},
                   dict(self.data, swift="main"), dict(self.data, kotlin=None)]
        for data in invalid:
            with self.subTest(data=data):
                self.pins.write_text(json.dumps(data))
                with self.assertRaises(ValueError):
                    ci.consumer_refs(self.pins)
        self.pins.write_text('{"schema_version":1,"swift":"a","swift":"b","kotlin":"c"}')
        with self.assertRaisesRegex(ValueError, "duplicate JSON"):
            ci.consumer_refs(self.pins)

    def test_pin_preflight_uses_resolved_trees_and_exports_complete_pair(self):
        output = self.root / "outputs"
        for publish in (False, True):
            with self.subTest(publish=publish):
                output.write_text("")
                def command(*args):
                    sdk = "swift" if "tinkle-sdk-swift/" in args[-1] else "kotlin"
                    sha = self.data[sdk]
                    ref = "main" if publish else sha
                    if args[-1].endswith("/commits/" + ref):
                        return json.dumps({"sha": sha})
                    self.assertEqual(args, ("gh", "api",
                        f"repos/tinkle-org/tinkle-sdk-{sdk}/git/trees/{sha}?recursive=1"))
                    return json.dumps(self.tree(sdk))
                with patch.object(ci.release, "run", side_effect=command) as run, \
                        patch.dict(ci.os.environ, {"GITHUB_OUTPUT": str(output)}):
                    self.assertEqual(ci.pin_consumers(self.pins, publish=publish),
                                     {"swift": "a" * 40, "kotlin": "b" * 40})
                self.assertEqual(run.call_count, 4)
                self.assertEqual(output.read_text(), f"swift_sha={'a' * 40}\nkotlin_sha={'b' * 40}\n")

    def test_readme_only_remote_never_exports_partial_pair(self):
        output = self.root / "outputs"
        output.write_text("")
        responses = [{"sha": "a" * 40}, self.tree("swift"), {"sha": "b" * 40},
                     {"truncated": False, "tree": [{"path": "README.md", "type": "blob", "mode": "100644"}]}]
        with patch.object(ci.release, "run", side_effect=map(json.dumps, responses)), \
                patch.dict(ci.os.environ, {"GITHUB_OUTPUT": str(output)}):
            with self.assertRaisesRegex(ValueError, "kotlin.*incomplete.*settings.gradle.kts"):
                ci.pin_consumers(self.pins)
        self.assertEqual(output.read_text(), "")

    def test_invalid_remote_sha_or_incomplete_tree_fails_closed(self):
        for commit in ("main", "A" * 40, "c" * 40, None):
            with self.subTest(commit=commit), patch.object(ci.release, "run", return_value=json.dumps({"sha": commit})):
                with self.assertRaises(ValueError):
                    ci.pin_consumers(self.pins)
        for tree in ({"truncated": True, "tree": []}, {"tree": []}):
            with self.subTest(tree=tree), patch.object(ci.release, "run", side_effect=[
                    json.dumps({"sha": "a" * 40}), json.dumps(tree)]):
                with self.assertRaisesRegex(ValueError, "truncated"):
                    ci.pin_consumers(self.pins)

    def test_symlink_blob_is_not_a_regular_scaffold_file(self):
        tree = self.tree("swift")
        for entry in tree["tree"]:
            if entry["path"] == "Package.swift":
                entry["mode"] = "120000"
        with patch.object(ci.release, "run", side_effect=[json.dumps({"sha": "a" * 40}), json.dumps(tree)]):
            with self.assertRaisesRegex(ValueError, "Package.swift"):
                ci.pin_consumers(self.pins)

    def test_fresh_scaffold_needs_no_metadata_or_binary_artifacts(self):
        for sdk in ci.CONSUMER_FILES:
            try:
                names = ci.local_package_files(sdk)
            except ValueError:
                names = ci.CONSUMER_FILES[sdk]
            destination = self.root / sdk
            for name in names:
                file = destination / name
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text("scaffold")
            ci.consumer_checkout(sdk, destination)
            self.assertFalse((destination / "sdk-artifact.json").exists())
            # Every required file is enforced, including ignored buildSrc sources.
            for name in names:
                with self.subTest(sdk=sdk, name=name):
                    file = destination / name
                    file.unlink()
                    with self.assertRaises(ValueError) as failure:
                        ci.consumer_checkout(sdk, destination)
                    self.assertIn(name, str(failure.exception))
                    file.write_text("scaffold")

    def test_incomplete_checkout_fails_before_artifact_access(self):
        for sdk in ci.CONSUMER_FILES:
            args = SimpleNamespace(sdk=sdk, destination=self.root, artifacts=self.root / "absent",
                                   bootstrap=False, candidate=True, download=False)
            with self.subTest(sdk=sdk), patch.object(ci.release, "load") as load:
                with self.assertRaisesRegex(ValueError, "incomplete"):
                    ci.stage(args)
                load.assert_not_called()

    def test_xcode_selection_probes_and_exports_the_exact_selected_version(self):
        # Named installations take priority; Xcode.app may be a different version.
        applications = self.root / "Applications"
        named = applications / "Xcode_26.6.app/Contents/Developer"
        alias = applications / "Xcode.app/Contents/Developer"
        named.mkdir(parents=True)
        alias.mkdir(parents=True)
        output = self.root / "environment"
        for version, expected in (("26.6", named), ("27.1", alias)):
            with self.subTest(version=version):
                output.write_text("")
                with patch.dict(ci.os.environ, {"GITHUB_ENV": str(output), "DEVELOPER_DIR": "wrong-default"}), \
                        patch.object(ci.release, "run", side_effect=[f"Xcode {version}\nBuild version test", "/sdk"]) as run:
                    ci.select_xcode(version, applications)
                self.assertEqual(output.read_text(), f"DEVELOPER_DIR={expected}\n")
                self.assertEqual([call.args for call in run.call_args_list],
                                 [("xcodebuild", "-version"), ("xcrun", "--show-sdk-path")])
                for call in run.call_args_list:
                    self.assertEqual(call.kwargs["env"]["DEVELOPER_DIR"], str(expected))

    def test_missing_or_wrong_xcode_cannot_silently_use_runner_default(self):
        applications = self.root / "Applications"
        output = self.root / "environment"
        output.write_text("")
        with patch.dict(ci.os.environ, {"GITHUB_ENV": str(output)}), \
                patch.object(ci.release, "run") as run:
            with self.assertRaisesRegex(ValueError, "not installed"):
                ci.select_xcode("27.1", applications)
            run.assert_not_called()
        (applications / "Xcode.app/Contents/Developer").mkdir(parents=True)
        for actual in ("Xcode 26.6\nBuild version test", ""):
            with self.subTest(actual=actual), patch.dict(ci.os.environ, {"GITHUB_ENV": str(output)}), \
                    patch.object(ci.release, "run", return_value=actual) as run:
                with self.assertRaisesRegex(ValueError, "expected Xcode 27.1"):
                    ci.select_xcode("27.1", applications)
                self.assertEqual(run.call_count, 1)
            self.assertEqual(output.read_text(), "")


if __name__ == "__main__":
    unittest.main()
