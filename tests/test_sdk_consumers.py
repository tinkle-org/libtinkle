"""Canonical fresh-checkout validation, without network/toolchains or artifacts."""

from contextlib import redirect_stderr
import importlib.util
import io
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
        self.names = {sdk: ci.local_package_files(sdk) for sdk in ci.packages.SDKS}

    def scaffold(self, sdk, destination):
        for name in self.names[sdk]:
            file = destination / name
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("scaffold")

    def canonical_root(self):
        root = self.root / "core"
        for sdk in self.names:
            self.scaffold(sdk, root / "packages" / sdk)
        catalog = root / ci.packages.CATALOG
        catalog.write_bytes((ROOT / ci.packages.CATALOG).read_bytes())
        return root, catalog

    def assert_checkout_rejected(self, sdk, destination, message):
        with self.assertRaisesRegex(ValueError, message):
            ci.consumer_checkout(sdk, destination)
        with self.assertRaisesRegex(ValueError, message):
            ci.require_consumer_files(sdk, self.names[sdk], destination)

    def assert_stage_rejected(self, sdk, destination, message):
        for mode in ("bootstrap", "candidate", "release"):
            args = SimpleNamespace(sdk=sdk, destination=destination,
                                   artifacts=self.root / "absent", bootstrap=mode == "bootstrap",
                                   candidate=mode == "candidate", download=False)
            with self.subTest(mode=mode), \
                    patch.object(ci.release, "load") as load, \
                    patch.object(ci.release, "verify") as verify, \
                    patch.object(ci.release, "inspect_zip") as inspect, \
                    patch.object(ci.release, "digest") as digest, \
                    patch.object(ci.zipfile, "ZipFile") as archive:
                with self.assertRaisesRegex(ValueError, message):
                    ci.stage(args)
                for operation in (load, verify, inspect, digest, archive):
                    operation.assert_not_called()

    def test_fresh_scaffold_needs_no_metadata_or_binary_artifacts(self):
        for sdk, names in self.names.items():
            destination = self.root / sdk
            self.scaffold(sdk, destination)
            ci.consumer_checkout(sdk, destination)
            ci.require_consumer_files(sdk, names, destination)
            self.assertFalse((destination / "sdk-artifact.json").exists())
            # Every catalog entry is required, including ignored buildSrc sources.
            for name in names:
                with self.subTest(sdk=sdk, name=name):
                    file = destination / name
                    file.unlink()
                    with self.assertRaises(ValueError) as failure:
                        ci.consumer_checkout(sdk, destination)
                    self.assertIn(name, str(failure.exception))
                    with self.assertRaises(ValueError) as failure:
                        ci.require_consumer_files(sdk, set(names) - {name}, destination)
                    self.assertIn(name, str(failure.exception))
                    file.write_text("scaffold")

    def test_missing_or_invalid_local_catalog_fails_closed(self):
        root, catalog = self.canonical_root()
        invalid = (
            (None, "missing regular SDK package file: packages/packages.json"),
            ("{", "Expecting property name"),
            ("[]", "unsupported SDK package catalog"),
            (json.dumps({"schema_version": 2, **self.names}), "unsupported SDK package catalog"),
            ('{"schema_version":1,"swift":[],"swift":[],"kotlin":[]}', "duplicate catalog key"),
            (json.dumps({"schema_version": 1, "swift": [], "kotlin": self.names["kotlin"]}),
             "empty SDK package layout"),
        )
        with patch.object(ci, "CORE_ROOT", root):
            for content, message in invalid:
                if content is None:
                    catalog.unlink()
                else:
                    catalog.write_text(content)
                for sdk in self.names:
                    with self.subTest(message=message, sdk=sdk):
                        destination = root / "packages" / sdk
                        self.assert_checkout_rejected(sdk, destination, message)
                        self.assert_stage_rejected(sdk, destination, message)

    def test_missing_canonical_source_cannot_fall_back_to_consumer_scaffold(self):
        root, _ = self.canonical_root()
        with patch.object(ci, "CORE_ROOT", root):
            for sdk, names in self.names.items():
                destination = self.root / sdk
                self.scaffold(sdk, destination)
                source = root / "packages" / sdk / names[0]
                source.unlink()
                with self.subTest(sdk=sdk):
                    self.assert_checkout_rejected(sdk, destination, "missing regular SDK package file")
                    self.assert_stage_rejected(sdk, destination, "missing regular SDK package file")
                source.write_text("scaffold")

    def test_incomplete_checkout_fails_before_artifact_access(self):
        for sdk in self.names:
            with self.subTest(sdk=sdk):
                self.assert_stage_rejected(sdk, self.root, "incomplete")

    def test_removed_commands_are_rejected_without_execution(self):
        for arguments in (("pin-consumers",), ("pin-consumers", "--pins", "absent.json"),
                          ("select-xcode", "--version", "27.1"), ("android-setup",)):
            stderr = io.StringIO()
            with self.subTest(arguments=arguments), \
                    patch.object(ci.sys, "argv", ["sdk-ci.py", *arguments]), \
                    patch.object(ci.release, "run") as run, \
                    patch.object(ci, "call") as call, \
                    patch.object(ci.subprocess, "run") as process, redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as failure:
                    ci.main()
                self.assertNotEqual(failure.exception.code, 0)
                self.assertIn("invalid choice", stderr.getvalue())
                for operation in (run, call, process):
                    operation.assert_not_called()


if __name__ == "__main__":
    unittest.main()
