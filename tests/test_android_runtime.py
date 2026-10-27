"""Application page-size evidence, including API 24 and x86_64 16 KB emulation."""

import importlib.util
import io
import json
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "android_runtime_ci", Path(__file__).resolve().parents[1] / "scripts/sdk-ci.py")
assert SPEC and SPEC.loader
ci = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci)


def auxv(*entries):
    return b"".join(struct.pack("<QQ", tag, value) for tag, value in entries)


class AndroidRuntimeTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.missing_getconf = subprocess.CalledProcessError(127, ["adb", "shell", "getconf"])

    def test_getconf_reports_application_pages_without_reading_smaps(self):
        # x86_64 ps16k can expose 16384 here while smaps still reports 4 kB.
        for value in (4096, 16384):
            with self.subTest(value=value), patch.object(ci.release, "run", return_value=f"{value}\n") as run, \
                    patch.object(ci.subprocess, "run") as binary:
                self.assertEqual(ci.android_runtime_page_size(), (value, "getconf PAGE_SIZE"))
                run.assert_called_once_with("adb", "shell", "getconf", "PAGE_SIZE")
                binary.assert_not_called()

    def test_getconf_alias_is_only_used_when_first_command_fails(self):
        with patch.object(ci.release, "run", side_effect=[self.missing_getconf, "16384"]) as run, \
                patch.object(ci.subprocess, "run") as binary:
            self.assertEqual(ci.android_runtime_page_size(), (16384, "getconf PAGESIZE"))
            self.assertEqual([call.args for call in run.call_args_list], [
                ("adb", "shell", "getconf", "PAGE_SIZE"),
                ("adb", "shell", "getconf", "PAGESIZE"),
            ])
            binary.assert_not_called()

    def test_missing_getconf_uses_binary_safe_device_auxv(self):
        for pages in (4096, 16384):
            payload = auxv((3, 0x400040), (6, pages), (0, 0))
            with self.subTest(pages=pages), \
                    patch.object(ci.release, "run", side_effect=self.missing_getconf), \
                    patch.object(ci.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, payload)) as binary:
                self.assertEqual(ci.android_runtime_page_size(), (pages, "AT_PAGESZ"))
                binary.assert_called_once_with(["adb", "exec-out", "cat", "/proc/self/auxv"],
                    check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def test_successful_but_invalid_getconf_output_fails_without_fallback(self):
        for value in ("", "undefined", "0", "-4096", "4096\n16384", "16 kB", "16384 garbage"):
            with self.subTest(value=value), patch.object(ci.release, "run", return_value=value) as run, \
                    patch.object(ci.subprocess, "run") as binary:
                with self.assertRaisesRegex(ValueError, "invalid getconf"):
                    ci.android_runtime_page_size()
                self.assertEqual(run.call_count, 1)
                binary.assert_not_called()

    def test_auxv_parser_rejects_malformed_or_ambiguous_evidence(self):
        for payload in (b"", b"\x00" * 15, auxv((6, 4096)), auxv((0, 0)),
                        auxv((6, 0), (0, 0)), auxv((6, 12345), (0, 0)),
                        auxv((6, 4096), (6, 16384), (0, 0)),
                        auxv((6, 4096), (0, 1)), auxv((0, 0), (6, 4096))):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                ci.android_auxv_page_size(payload)
        self.assertEqual(ci.android_auxv_page_size(auxv((6, 4096), (0, 0), (0, 0))), 4096)

    def test_auxv_transport_error_is_not_accepted_as_runtime_proof(self):
        with patch.object(ci.release, "run", side_effect=self.missing_getconf), \
                patch.object(ci.subprocess, "run", side_effect=subprocess.CalledProcessError(1, ["adb"])):
            with self.assertRaises(subprocess.CalledProcessError):
                ci.android_runtime_page_size()

    def test_wrong_abi_fails_before_getconf_or_binary_parser(self):
        with patch.object(ci.release, "run", side_effect=["36", "arm64-v8a"]) as run, \
                patch.object(ci.subprocess, "run") as binary, patch.object(ci, "call") as command:
            with self.assertRaisesRegex(ValueError, "wrong emulator ABI"):
                ci.android_test(self.root, self.root, 16384, 36)
            self.assertEqual(run.call_count, 2)
            binary.assert_not_called()
            command.assert_not_called()

    def test_wrong_numeric_page_size_fails_without_searching_other_probes(self):
        for actual, expected in ((4096, 16384), (16384, 4096)):
            with self.subTest(actual=actual), \
                    patch.object(ci.release, "run", side_effect=["36", "x86_64", str(actual)]) as run, \
                    patch.object(ci.subprocess, "run") as binary, patch.object(ci, "call") as command:
                with self.assertRaisesRegex(ValueError, f"expected {expected}, got {actual}"):
                    ci.android_test(self.root, self.root, expected, 36)
                self.assertEqual(run.call_count, 3)
                binary.assert_not_called()
                command.assert_not_called()

    def test_wrong_auxv_page_size_still_blocks_consumer(self):
        with patch.object(ci.release, "run", side_effect=["36", "x86_64", self.missing_getconf, self.missing_getconf]), \
                patch.object(ci.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, auxv((6, 4096), (0, 0)))), \
                patch.object(ci, "call") as command:
            with self.assertRaisesRegex(ValueError, "expected 16384, got 4096 via AT_PAGESZ"):
                ci.android_test(self.root, self.root, 16384, 36)
            command.assert_not_called()

    def test_correct_runtime_evidence_runs_exact_aar_consumer_and_package_checks(self):
        (self.root / "build").mkdir()
        (self.root / "gradlew").write_text("test wrapper")
        aar = self.root / "candidate.aar"
        aar.write_bytes(b"same AAR throughout test")
        proof = {"path": str(aar), "sha256": ci.release.digest(aar)}
        (self.root / "build/sdk-ci-aar.json").write_text(json.dumps(proof))
        apk = self.root / "sample/build/outputs/apk/debug/consumer.apk"
        apk.parent.mkdir(parents=True)
        apk.write_bytes(b"APK")
        sdk = self.root / "android-sdk"
        sdk.mkdir()
        for api, pages in ((24, 4096), (36, 4096), (36, 16384)):
            responses = [str(api), "x86_64"]
            responses += [self.missing_getconf, self.missing_getconf] if api == 24 else [str(pages)]
            with self.subTest(api=api, pages=pages), \
                    patch.object(ci.release, "run", side_effect=responses), \
                    patch.object(ci.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, auxv((6, pages), (0, 0)))), \
                    patch.object(ci, "android_env", return_value={}), \
                    patch.object(ci.release, "inspect_zip") as inspect, \
                    patch.dict(ci.os.environ, {"ANDROID_HOME": str(sdk)}), patch.object(ci, "call") as command:
                ci.android_test(self.root, self.root, pages, api)
                self.assertEqual(command.call_args_list[1].args, (
                    "bash", self.root / "gradlew", "--no-daemon", ":sample:connectedDebugAndroidTest",
                    "-PtinkleSdkTestAar=" + str(aar)))
                self.assertEqual(command.call_args_list[1].kwargs["env"]["TINKLE_SDK_TEST_AAR"], str(aar))
                inspect.assert_called_once_with(apk)
                self.assertEqual(command.call_args_list[-1].args,
                     (sdk / "build-tools/36.0.0/zipalign", "-c", "-P", "16", "-v", "4", apk))

    def test_instrumentation_failure_collects_evidence_before_propagating(self):
        (self.root / "build").mkdir()
        (self.root / "gradlew").write_text("wrapper")
        aar = self.root / "candidate.aar"
        aar.write_bytes(b"verified candidate")
        (self.root / "build/sdk-ci-aar.json").write_text(json.dumps({
            "path": str(aar), "sha256": ci.release.digest(aar)}))
        failure = subprocess.CalledProcessError(1, ["gradlew", "connectedDebugAndroidTest"])
        with patch.object(ci.release, "run", side_effect=["36", "x86_64", "4096"]), \
                patch.object(ci, "android_env", return_value={}), \
                patch.object(ci, "call", side_effect=[None, failure]), \
                patch.object(ci, "android_diagnostics") as diagnostics:
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                ci.android_test(self.root, self.root, 4096, 36)
            self.assertIs(caught.exception, failure)
            diagnostics.assert_called_once_with(self.root)

    def test_diagnostics_keep_device_causes_and_print_full_xml_failure(self):
        results = self.root / "sample/build/outputs/androidTest-results/connected/debug"
        results.mkdir(parents=True)
        (results / "TEST-smoke.xml").write_text(
            '<testsuite><testcase classname="NativeSmokeTest" name="smoke">'
            '<failure message="Loopback gateway failed">Caused by: Unexpected fixture RPC: GetHistory'
            '\nSuppressed: original assertion</failure></testcase></testsuite>')
        captured = io.StringIO()
        with patch.object(ci.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 0, b"System.err: Caused by: fixture failure\n")) as command, redirect_stdout(captured):
            ci.android_diagnostics(self.root)
        self.assertEqual((self.root / "build/diagnostics/logcat.txt").read_bytes(),
                         b"System.err: Caused by: fixture failure\n")
        self.assertIn("NativeSmokeTest.smoke", captured.getvalue())
        self.assertIn("Caused by: Unexpected fixture RPC: GetHistory", captured.getvalue())
        self.assertIn("Suppressed: original assertion", captured.getvalue())
        self.assertEqual(command.call_args.args[0][-3:], ["System.err:W", "AndroidRuntime:E", "*:S"])
        self.assertEqual(command.call_args.kwargs["timeout"], 20)

    def test_failed_logcat_or_malformed_report_does_not_replace_test_failure(self):
        results = self.root / "sample/build/outputs/androidTest-results/connected"
        results.mkdir(parents=True)
        (results / "broken.xml").write_text("<invalid")
        for failure in (OSError("no adb"), subprocess.TimeoutExpired("adb", 20)):
            with self.subTest(failure=failure), \
                    patch.object(ci.subprocess, "run", side_effect=failure), redirect_stdout(io.StringIO()):
                ci.android_diagnostics(self.root) # Diagnostics must not raise.


if __name__ == "__main__":
    unittest.main()
