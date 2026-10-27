"""Static contracts for the simplified SDK pipeline, not live CI evidence."""

from pathlib import Path
import re
import unittest


WORKFLOWS = Path(__file__).resolve().parents[1] / ".github/workflows"


def jobs(content):
    # These workflows use two-space job keys and six-space step lists.
    blocks = re.split(r"(?m)^  ([\w-]+):\n", content.split("\njobs:\n", 1)[1])
    return dict(zip(blocks[1::2], blocks[2::2]))


def steps(job):
    return re.split(r"(?m)^      - ", job)[1:]


def values(content, key, indent):
    return re.findall(rf"(?m)^{' ' * indent}{re.escape(key)}: (.+)$", content)


class SdkWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflows = {name: (WORKFLOWS / (name + ".yml")).read_text()
                          for name in ("ci", "native-sdk", "release-plz")}
        self.ci = jobs(self.workflows["ci"])
        self.native = jobs(self.workflows["native-sdk"])

    def test_ci_requires_immutable_call_input_and_pins_every_checkout(self):
        header = self.workflows["ci"].split("\njobs:\n", 1)[0]
        call = header.split("  workflow_call:\n", 1)[1].split("\npermissions:", 1)[0]
        self.assertRegex(call, r"(?m)^    inputs:\n      source_sha:\n")
        self.assertEqual(values(call, "type", 8), ["string"])
        self.assertEqual(values(call, "required", 8), ["true"])
        for name, job in self.ci.items():
            with self.subTest(job=name):
                checkouts = [step for step in steps(job) if "uses: actions/checkout@" in step]
                self.assertTrue(checkouts)
                for step in checkouts:
                    self.assertEqual(values(step, "ref", 10),
                                     ["${{ inputs.source_sha || github.sha }}"])
                    self.assertEqual(values(step, "persist-credentials", 10), ["false"])

    def test_original_required_jobs_names_and_unconditional_commands_survive(self):
        names = {"fmt": "rustfmt", "clippy": "clippy (${{ matrix.os }})",
                 "test": "test (${{ matrix.os }})", "wasm": "wasm32 check",
                 "msrv": "msrv (cargo-hack)"}
        commands = {
            "fmt": "cargo fmt --all -- --check",
            "clippy": "cargo clippy --locked --workspace --all-targets --all-features --no-deps -- -D warnings",
            "test": "cargo test --locked --workspace --all-features --verbose",
            "wasm": "cargo check --locked -p syntra-ws -p libtinkle-web --target wasm32-unknown-unknown",
            "msrv": "['cargo', '+' + version, 'check', '--locked', '--offline', '--workspace', '--all-targets']",
        }
        for name, display in names.items():
            with self.subTest(job=name):
                job = self.ci[name]
                self.assertEqual(values(job, "name", 4), [display])
                self.assertEqual(values(job, "if", 4), [])
                command_steps = [step for step in steps(job) if commands[name] in step]
                self.assertEqual(len(command_steps), 1)
                self.assertNotRegex(command_steps[0], r"(?m)^\s*if:")
                if name != "test":
                    self.assertNotRegex(job, r"(?m)^\s*(?:- )?if:")
        for name in ("clippy", "test"):
            self.assertEqual(values(self.ci[name], "os", 8), ["[ubuntu-latest, macos-latest]"])
        self.assertIn("python3 -m unittest discover -s tests -p 'test_*.py' -v", self.ci["test"])
        self.assertIn("bash scripts/generate-bindings.sh --check", self.ci["test"])
        self.assertNotIn("continue-on-error:", self.workflows["ci"])

    def test_main_pushes_keep_per_sha_evidence_and_only_prs_cancel(self):
        prefixes = []
        for name, prefix, source in (("ci", "rust-ci", "source_sha"),
                                     ("native-sdk", "native-sdk", "source_ref")):
            with self.subTest(workflow=name):
                header = self.workflows[name].split("\njobs:\n", 1)[0]
                self.assertIn("  push:\n    branches: [main]\n", header)
                self.assertIn("  pull_request:\n", header)
                self.assertNotRegex(header, r"(?m)^\s*(?:paths|paths-ignore|branches-ignore):")
                concurrency = header.split("\nconcurrency:\n", 1)[1].strip()
                self.assertEqual(concurrency,
                                 f"group: {prefix}-" + "${{ github.event_name }}-"
                                 "${{ github.event_name == 'pull_request' && github.ref || inputs."
                                 + source + " || github.sha }}\n"
                                 "  cancel-in-progress: ${{ github.event_name == 'pull_request' }}")
                prefixes.append(prefix)
        self.assertNotEqual(*prefixes)  # Caller and callee must not cancel each other.

    def test_publish_rust_gate_calls_local_ci_at_exact_resolved_sha(self):
        job = self.native["rust-checks"]
        self.assertEqual(values(job, "needs", 4), ["source"])
        self.assertEqual(values(job, "if", 4), ["needs.source.outputs.publish == 'true'"])
        self.assertEqual(values(job, "uses", 4), ["./.github/workflows/ci.yml"])
        self.assertEqual(values(job, "source_sha", 6), ["${{ needs.source.outputs.sha }}"])
        self.assertNotIn("continue-on-error:", job)

    def test_publish_directly_requires_all_gates_without_success_bypasses(self):
        publish = self.native["publish"]
        self.assertEqual(values(publish, "needs", 4),
                         ["[source, rust-checks, manifest, swift-consumer, kotlin-consumer]"])
        self.assertEqual(values(publish, "if", 4), [
            "needs.source.outputs.publish == 'true' && github.repository == 'tinkle-org/libtinkle' "
            "&& github.event_name != 'pull_request' && (inputs.publish == true || "
            "(github.event_name == 'push' && startsWith(github.ref, 'refs/tags/tinkle-ffi-v'))) "
            "&& (github.ref == 'refs/heads/main' || startsWith(github.ref, 'refs/tags/tinkle-ffi-v'))"])
        self.assertNotIn("always()", publish)
        self.assertNotIn("!cancelled()", publish)
        self.assertNotIn("continue-on-error:", self.workflows["native-sdk"])
        dependencies = {"ios": "source", "android": "source",
                        "manifest": "[source, ios, android]",
                        "swift-consumer": "[source, manifest]",
                        "kotlin-consumer": "[source, manifest]"}
        for name, needs in dependencies.items():
            with self.subTest(job=name):
                job = self.native[name]
                self.assertEqual(values(job, "needs", 4), [needs])
                self.assertEqual(values(job, "if", 4), [])
                for step in steps(job):
                    # Always-uploaded diagnostics must not make validation optional.
                    if "actions/upload-artifact@" not in step:
                        self.assertNotRegex(step, r"(?m)^\s*if:")
        self.assertIn("sdk-release.py publish --manifest artifacts/sdk-release.json "
                      "--artifacts artifacts --debug-assets", publish)

    def test_native_builds_consumers_and_publish_checkout_resolved_sha(self):
        for name, job in self.native.items():
            checkouts = [step for step in steps(job) if "uses: actions/checkout@" in step]
            if name == "rust-checks":
                continue
            with self.subTest(job=name):
                self.assertTrue(checkouts)
                for step in checkouts:
                    expected = ("${{ inputs.source_ref || github.sha }}" if name == "source"
                                else "${{ needs.source.outputs.sha }}")
                    self.assertEqual(values(step, "ref", 10), [expected])
                    self.assertEqual(values(step, "persist-credentials", 10), ["false"])

    def test_release_plz_passes_exact_emitted_tag_and_publish_permission(self):
        release = jobs(self.workflows["release-plz"])
        self.assertEqual(values(release["release-plz"], "native_tag", 6),
                         ["${{ steps.native.outputs.tag }}"])
        self.assertIn("RELEASES: ${{ steps.release.outputs.releases }}", release["release-plz"])
        self.assertIn("'scripts/sdk-release.py', 'release-output'", release["release-plz"])
        caller = release["native"]
        self.assertEqual(values(caller, "needs", 4), ["release-plz"])
        self.assertEqual(values(caller, "if", 4), ["needs.release-plz.outputs.native_tag != ''"])
        self.assertEqual(values(caller, "uses", 4), ["./.github/workflows/native-sdk.yml"])
        self.assertEqual(values(caller, "source_ref", 6), ["${{ needs.release-plz.outputs.native_tag }}"])
        self.assertEqual(values(caller, "publish", 6), ["true"])
        self.assertEqual(values(caller, "contents", 6), ["write"])

    def test_status_polling_and_attestation_are_absent(self):
        for name, content in self.workflows.items():
            with self.subTest(workflow=name):
                self.assertNotIn("sdk-attestation", content)
                self.assertNotRegex(content, r"(?m)^\s*actions:\s*read\b")
                self.assertNotIn("--wait-seconds", content)
                self.assertNotIn("/actions/runs", content)

    def test_maintained_node24_setup_actions_and_exact_toolchain_selection(self):
        for name in ("ios", "swift-consumer"):
            with self.subTest(job=name):
                job = self.native[name]
                self.assertEqual(values(job, "runs-on", 4), ["macos-26"])
                self.assertEqual(values(job, "XCODE_VERSION", 6),
                                 ["${{ needs.source.outputs.publish == 'true' && '27.1' || '26.6' }}"])
                setup = [step for step in steps(job) if "uses: maxim-lobanov/setup-xcode@" in step]
                self.assertEqual(len(setup), 1)
                self.assertIn("setup-xcode@ed7a3b1fda3918c0306d1b724322adc0b8cc0a90", setup[0])
                self.assertEqual(values(setup[0], "xcode-version", 10), ["${{ env.XCODE_VERSION }}"])
                self.assertIn('test "$selected" = "Xcode $XCODE_VERSION"', job)
                self.assertNotIn("DEVELOPER_DIR:", job)
        for name, build in (("android", "bash scripts/build-android.sh"),
                            ("kotlin-consumer", "scripts/sdk-ci.py android-build")):
            with self.subTest(job=name):
                job = self.native[name]
                setup = [step for step in steps(job) if "uses: android-actions/setup-android@" in step]
                self.assertEqual(len(setup), 1)
                self.assertIn("setup-android@be39fa834029ff78f1a44aa3bb0819b8fc2bd8fd", setup[0])
                self.assertEqual(values(setup[0], "packages", 10),
                                 ["'platform-tools ndk;30.0.16248370 platforms;android-36 build-tools;36.0.0'"])
                override = 'echo "ANDROID_NDK_HOME=$ANDROID_HOME/ndk/30.0.16248370" >> "$GITHUB_ENV"'
                self.assertIn(override, job)
                self.assertLess(job.index(setup[0]), job.index(override))
                self.assertLess(job.index(override), job.index(build))
        for content in self.workflows.values():
            actions = re.findall(r"(?m)^\s*(?:- )?uses: ([^\s]+)", content)
            self.assertTrue(actions)
            for action in actions:
                if not action.startswith("./"):
                    self.assertRegex(action, r"^[\w/-]+@[0-9a-f]{40}$")


if __name__ == "__main__":
    unittest.main()
