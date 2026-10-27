"""Inactive downstream workflow contracts; standard library, no GitHub mutation.

Actionlint validates the installed caller/callee layout when available.
These tests protect repository identity, trigger/trust boundaries, and separation
from the actual git-backed package snapshot, without depending on a YAML parser.
"""

import importlib.util
import json
import os
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / ".github/sdk-distribution"
SDKS = ("swift", "kotlin")
SPEC = importlib.util.spec_from_file_location(
    "sdk_distribution_packages", ROOT / "scripts/sdk-packages.py")
assert SPEC and SPEC.loader
packages = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(packages)


def template(sdk, name):
    return (TEMPLATES / sdk / name).read_text()


def steps(content):
    # All template steps use the same six-space indentation. This deliberately
    # extracts only step text, not a partial general-purpose YAML implementation.
    return re.split(r"(?m)^      - ", content)[1:]


def jobs(content):
    blocks = re.split(r"(?m)^  ([\w-]+):\n", content.split("\njobs:\n", 1)[1])
    return dict(zip(blocks[1::2], blocks[2::2]))


def inline_python(content):
    return [textwrap.dedent(script) for script in
            re.findall(r"python3 - <<'PY'\n(.*?)          PY", content, re.S)]


class DistributionTemplateTests(unittest.TestCase):
    def test_templates_are_inactive_and_use_current_identity(self):
        self.assertTrue((TEMPLATES / "README.md").is_file())
        for sdk in SDKS:
            for name in ("ci.yml", "release.yml"):
                with self.subTest(sdk=sdk, name=name):
                    path = TEMPLATES / sdk / name
                    self.assertNotEqual(path.parent, ROOT / ".github/workflows")
                    content = path.read_text()
                    self.assertIn("tinkle-org/libtinkle", content)
                    self.assertNotIn("tinklehq", content.casefold())
                    self.assertNotIn("pull_request_target:", content)
                    self.assertNotIn("workflows: write", content)
                    self.assertNotIn("SDK_DISTRIBUTION_TOKEN", content)
                    self.assertNotIn("secrets.", content)
                    self.assertIn("permissions:\n  contents: read\n", content)

    def test_validation_triggers_cover_pr_main_tags_and_explicit_bootstrap(self):
        for sdk in SDKS:
            with self.subTest(sdk=sdk):
                content = template(sdk, "ci.yml")
                triggers = content.split("\non:\n", 1)[1].split("\npermissions:", 1)[0]
                self.assertIn("  pull_request:\n", triggers)
                self.assertIn("  push:\n    branches: [main]\n", triggers)
                self.assertIn("    tags: ['v*']\n", triggers)
                self.assertIn("  workflow_dispatch:\n    inputs:\n      source_ref:", triggers)
                self.assertNotIn("paths:", triggers)
                self.assertNotIn("paths-ignore:", triggers)
                for name, job in jobs(content).items():
                    if name != "release":
                        self.assertNotIn("contents: write", job)
                        self.assertNotIn("pull-requests: read", job)
                        self.assertNotIn("secrets:", job)
                self.assertNotIn("secrets:", content)
                self.assertIn("Published SDK CI must use its manifest SHA", content)
                self.assertIn("assert bootstrap or data['sha'] == ref", content)
                self.assertIn("if: needs.source.outputs.ready == 'true'", content)
                source = jobs(content)["source"]
                checkout = steps(source)[0]
                self.assertIn("fetch-depth: 2", checkout)
                self.assertNotIn("ref:", checkout)  # Default immutable event SHA.
                for output in ("ready", "core_sha", "bootstrap", "sdk_sha", "release"):
                    self.assertIn(f"{output}: ${{{{ steps.source.outputs.{output} }}}}", source)
                consumer = jobs(content)["consumer"]
                self.assertIn("ref: ${{ needs.source.outputs.sdk_sha }}", consumer)

    def test_validation_concurrency_preserves_push_evidence_and_cancels_only_prs(self):
        for sdk in SDKS:
            with self.subTest(sdk=sdk):
                concurrency = template(sdk, "ci.yml").split("\nconcurrency:\n", 1)[1].split("\njobs:\n", 1)[0]
                self.assertEqual(
                    concurrency,
                    f"  group: {sdk}-sdk-" + "${{ github.event_name }}-"
                    "${{ github.event_name == 'push' && github.sha || github.ref }}\n"
                    "  cancel-in-progress: ${{ github.event_name == 'pull_request' }}")

    def test_release_is_a_local_dependency_after_all_validation(self):
        for sdk in SDKS:
            with self.subTest(sdk=sdk):
                ci = jobs(template(sdk, "ci.yml"))
                release = ci["release"]
                needs = "[source, consumer]" if sdk == "swift" else "[source, consumer, build-logic]"
                self.assertIn("needs: " + needs, release)
                self.assertIn("uses: ./.github/workflows/release.yml", release)
                self.assertIn("source_sha: ${{ needs.source.outputs.core_sha }}", release)
                self.assertIn("sdk_sha: ${{ needs.source.outputs.sdk_sha }}", release)
                self.assertIn("permissions:\n      contents: write\n      pull-requests: read", release)
                self.assertIn(f"github.repository == 'tinkle-org/tinkle-sdk-{sdk}'", release)
                for guard in ("github.event_name == 'push'",
                              "needs.source.outputs.bootstrap == 'false'",
                              "needs.source.outputs.ready == 'true'",
                              "needs.source.outputs.release == 'true'",
                              "(github.ref == 'refs/heads/main' || startsWith(github.ref, 'refs/tags/v'))"):
                    self.assertIn(guard, release)
                # Default success() semantics must reject failed/skipped dependencies.
                for bypass in ("always()", "!cancelled()", "continue-on-error:", "secrets:"):
                    self.assertNotIn(bypass, release)
                self.assertEqual(list(ci)[-1], "release")

    def test_reusable_release_inputs_guards_and_tag_creation(self):
        for sdk in SDKS:
            with self.subTest(sdk=sdk):
                content = template(sdk, "release.yml")
                triggers = content.split("\non:\n", 1)[1].split("\npermissions:", 1)[0]
                self.assertEqual(triggers, "  workflow_call:\n    inputs:\n"
                                 "      source_sha:\n        required: true\n        type: string\n"
                                 "      sdk_sha:\n        required: true\n        type: string")
                for name in (("qualify", "authorize") if sdk == "swift" else ("authorize",)):
                    job = jobs(content)[name]
                    self.assertIn(f"github.repository == 'tinkle-org/tinkle-sdk-{sdk}'", job)
                    self.assertIn("github.event_name == 'push'", job)
                    self.assertIn("(github.ref == 'refs/heads/main' || startsWith(github.ref, 'refs/tags/v'))", job)
                authorize = jobs(content)["authorize"]
                self.assertIn("contents: write", authorize)
                self.assertIn("pull-requests: read", authorize)
                self.assertIn("EVENT_NAME: ${{ github.event_name }}", authorize)
                self.assertIn("SDK_REF: ${{ github.ref }}", authorize)
                self.assertIn('if [[ "$EVENT_NAME" != push ]]; then\n            exit 1\n          fi', authorize)
                guard = f"python3 core/scripts/sdk-ci.py sdk-tag --sdk {sdk} --destination sdk"
                self.assertIn('if [[ "$SDK_REF" == refs/heads/main ]]; then\n            ' + guard + ' --create', authorize)
                self.assertIn('elif [[ "$SDK_REF" == refs/tags/v* ]]; then\n            ' + guard + '\n', authorize)
                for name in ("ci.yml", "release.yml"):
                    workflow = template(sdk, name)
                    for forbidden in ("sdk-attestation.py", "actions: read", "--wait-seconds",
                                      "continue-on-error:", "actions/download-artifact@"):
                        self.assertNotIn(forbidden, workflow)

    def test_release_concurrency_is_distinct_and_commit_specific(self):
        for sdk in SDKS:
            content = template(sdk, "release.yml")
            concurrency = content.split("\nconcurrency:\n", 1)[1].split("\njobs:\n", 1)[0]
            self.assertEqual(concurrency, f"  group: {sdk}-sdk-release-" +
                             "${{ github.ref }}-${{ inputs.sdk_sha }}\n"
                             "  cancel-in-progress: false")

    def test_release_checks_provenance_before_trusted_core_checkout(self):
        for sdk in SDKS:
            content = template(sdk, "release.yml")
            for name in (("qualify", "authorize") if sdk == "swift" else ("authorize",)):
                with self.subTest(sdk=sdk, job=name):
                    job = jobs(content)[name]
                    self.assertLess(job.index("assert branch['protected']"),
                                    job.index("repository: tinkle-org/libtinkle"))
                    self.assertLess(job.index("assert compare['merge_base_commit']['sha'] == sha"),
                                    job.index("repository: tinkle-org/libtinkle"))
                    self.assertIn("SOURCE_SHA: ${{ inputs.source_sha }}", job)
                    self.assertIn("SDK_SHA: ${{ inputs.sdk_sha }}", job)
                    self.assertIn("assert sha == os.environ['SOURCE_SHA']", job)
                    self.assertIn("cwd='sdk', text=True).strip() == os.environ['SDK_SHA']", job)
            for checkout in steps(content):
                if checkout.startswith("uses: actions/checkout@") and "path: sdk" in checkout:
                    self.assertIn("ref: ${{ inputs.sdk_sha }}", checkout)

    def test_consumers_keep_same_artifact_and_binding_checks(self):
        for sdk in SDKS:
            ci = template(sdk, "ci.yml")
            release = template(sdk, "release.yml")
            stage = f"python3 core/scripts/sdk-ci.py stage --sdk {sdk} --destination sdk --artifacts artifacts"
            self.assertIn(stage + " --bootstrap", ci)
            self.assertIn(stage + " --download", ci)
            self.assertIn(stage + " --download", release)
        kotlin = template("kotlin", "ci.yml")
        self.assertIn("android-build --destination sdk --artifacts artifacts", kotlin)
        self.assertIn("sdk/build/outputs/aar/*-release.aar", kotlin)
        self.assertIn("sdk/build/sdk-ci-aar.json", kotlin)
        release = template("kotlin", "release.yml")
        self.assertIn('jitpack --tag "$SDK_TAG" --destination sdk', release)
        self.assertIn("SDK_TAG: v${{ needs.authorize.outputs.version }}", release)
        for output in ("source_sha", "sdk_sha", "version"):
            self.assertIn(output + ": ${{ steps.provenance.outputs.", release)

    def test_core_history_and_checkout_credentials(self):
        for sdk in SDKS:
            for name in ("ci.yml", "release.yml"):
                with self.subTest(sdk=sdk, name=name):
                    checkouts = [step for step in steps(template(sdk, name))
                                 if step.startswith("uses: actions/checkout@")]
                    self.assertTrue(checkouts)
                    core = [step for step in checkouts if "repository: tinkle-org/libtinkle" in step]
                    self.assertTrue(core)
                    for step in checkouts:
                        self.assertIn("persist-credentials: false", step)
                    for step in core:
                        self.assertIn("fetch-depth: 0", step)
                        self.assertIn("ref: ${{", step)
                        self.assertIn("path: core", step)
                        self.assertNotIn("ref: main", step)

    def test_swift_validation_and_release_pins_are_distinct(self):
        for name, version in (("ci.yml", "26.6"), ("release.yml", "27.1")):
            with self.subTest(name=name):
                content = template("swift", name)
                self.assertIn(f"XCODE_VERSION: '{version}'", content)
                self.assertIn('uses: maxim-lobanov/setup-xcode@', content)
                self.assertIn('xcode-version: ${{ env.XCODE_VERSION }}', content)
                self.assertIn('test "$selected" = "Xcode $XCODE_VERSION"', content)
                self.assertNotIn("DEVELOPER_DIR:", content)
                self.assertIn("IPHONEOS_DEPLOYMENT_TARGET: '15.0'", content)
                self.assertNotIn("select-xcode", content)
        release = template("swift", "release.yml")
        self.assertIn("  authorize:\n    needs: qualify\n", release)
        self.assertIn("swift-test --destination sdk", release)

    def test_kotlin_matrix_and_jitpack_dependency(self):
        matrix = """\
          - api: 24
            target: default
            pages: 4096
          - api: 36
            target: google_apis
            pages: 4096
          - api: 36
            target: google_apis_ps16k
            pages: 16384
"""
        for name in ("ci.yml", "release.yml"):
            with self.subTest(name=name):
                content = template("kotlin", name)
                self.assertIn(matrix, content)
                self.assertIn("fail-fast: false", content)
                self.assertIn("--page-size ${{ matrix.pages }}", content)
                self.assertIn("--api-level ${{ matrix.api }}", content)
        self.assertIn("  jitpack:\n    needs: authorize\n", template("kotlin", "release.yml"))
        self.assertIn("-p buildSrc test", template("kotlin", "ci.yml"))

    def test_action_pins_and_evidence_upload_scope(self):
        for sdk in SDKS:
            for name in ("ci.yml", "release.yml"):
                with self.subTest(sdk=sdk, name=name):
                    content = template(sdk, name)
                    actions = re.findall(r"(?m)^\s+uses: ([^\s]+)|^\s+- uses: ([^\s]+)", content)
                    self.assertTrue(actions)
                    for pair in actions:
                        action = next(value for value in pair if value)
                        if action.startswith("./"):
                            self.assertEqual(action, "./.github/workflows/release.yml")
                            self.assertEqual(name, "ci.yml")
                        else:
                            self.assertRegex(action, r"^[\w/-]+@[0-9a-f]{40}$")
                    uploads = [step for step in steps(content) if "actions/upload-artifact@" in step]
                    self.assertTrue(uploads)
                    for step in uploads:
                        self.assertRegex(step, r"if: (always|failure)\(\)")
                        self.assertIn("retention-days: 14", step)
                        paths = re.search(r"(?m)^          path: \|\n((?:            .+\n)+)", step)
                        self.assertIsNotNone(paths)
                        assert paths is not None
                        for path in paths[1].splitlines():
                            path = path.strip()
                            self.assertTrue(path.startswith(("sdk/", "evidence/", "buildSrc/build/", "fixture-tests/build/")))
                            self.assertNotIn("..", path.split("/"))
                            self.assertNotIn("**", path)
                            self.assertNotIn(".git", path.split("/"))
                            self.assertNotIn(".gradle", path.split("/"))
                        if "include-hidden-files: true" in step:
                            self.assertIn("sdk/.build/ci-results.xcresult", paths[1])
                    self.assertNotRegex(content, r"(?m)^\s+(?:env|printenv)\s*$")

    def test_inline_python_is_valid_and_context_is_not_interpolated_into_scripts(self):
        for sdk in SDKS:
            for name in ("ci.yml", "release.yml"):
                with self.subTest(sdk=sdk, name=name):
                    content = template(sdk, name)
                    scripts = inline_python(content)
                    self.assertTrue(scripts)
                    for script in scripts:
                        compile(textwrap.dedent(script), f"{sdk}/{name}", "exec")
                        self.assertNotIn("${{", script)

    @unittest.skipUnless(shutil.which("actionlint"), "actionlint is not installed")
    def test_actionlint_on_installed_caller_and_callee_layout(self):
        # Local workflow calls resolve from .github/workflows, not this inactive
        # source directory. Lint the real downstream layout without installing it.
        for sdk in SDKS:
            with self.subTest(sdk=sdk), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / ".git").mkdir()
                workflows = root / ".github/workflows"
                workflows.mkdir(parents=True)
                for name in ("ci.yml", "release.yml"):
                    (workflows / name).write_text(template(sdk, name))
                result = subprocess.run(["actionlint", ".github/workflows/ci.yml",
                                         ".github/workflows/release.yml"], cwd=root,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_catalog_and_git_snapshots_exclude_workflow_directories(self):
        catalog = packages.catalog((ROOT / packages.CATALOG).read_bytes())
        for sdk in SDKS:
            for path in catalog[sdk]:
                self.assertNotIn(".github", Path(path).parts)

        # Real git fixture: prove the catalog (not directory enumeration) defines
        # the snapshot even when workflow YAML exists beside and inside packages.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def git(*args, input=None):
                return subprocess.check_output(["git", *args], cwd=root, input=input, text=True).strip()

            git("init", "--quiet")
            fixture = {"schema_version": 1, "swift": ["README.md"], "kotlin": ["README.md"]}
            for sdk in SDKS:
                for path in (f"packages/{sdk}/README.md",
                             f"packages/{sdk}/.github/workflows/ci.yml",
                             f".github/sdk-distribution/{sdk}/release.yml",
                             ".github/workflows/ci.yml"):
                    target = root / path
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text("fixture\n")
            (root / packages.CATALOG).write_text(json.dumps(fixture))
            git("add", ".")
            tree = git("write-tree")
            sha = git("-c", "user.name=fixture", "-c", "user.email=fixture@example.com",
                      "commit-tree", tree, input="snapshot fixture\n")
            for sdk in SDKS:
                self.assertEqual(packages.snapshot(root, sha, sdk), {"README.md": b"fixture\n"})


class SourceSelectionTests(unittest.TestCase):
    """Execute the embedded source selector against real root/merge/shallow git trees."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.git("init", "--quiet")
        (self.repo / "README.md").write_text("SDK fixture\n")
        self.source_sha = "a" * 40
        self.manifest = {"source_repository": "tinkle-org/libtinkle",
                         "source_revision": self.source_sha, "version": "1.2.3"}

    def git(self, *args, input=None):
        return subprocess.check_output(["git", *args], cwd=self.repo,
                                       input=input, text=True).strip()

    def commit(self, *parents):
        self.git("add", "--all")
        tree = self.git("write-tree")
        args = ["-c", "user.name=fixture", "-c", "user.email=fixture@example.com",
                "commit-tree", tree]
        for parent in parents:
            args.extend(["-p", parent])
        sha = self.git(*args, input="SDK fixture\n")
        self.git("reset", "--hard", sha)
        return sha

    def write_manifest(self):
        (self.repo / "sdk-artifact.json").write_text(json.dumps(self.manifest))

    def select(self, sdk, event="push", ref="refs/heads/main", source_ref="", repo=None):
        repo = repo or self.repo
        output = self.root / "output"
        output.write_text("")
        real_check_output = subprocess.check_output

        def check_output(command, **kwargs):
            if command[0] == "gh":
                self.assertEqual(command[:2], ["gh", "api"])
                self.assertTrue(command[2].startswith("repos/tinkle-org/libtinkle/commits/"))
                return json.dumps({"sha": self.source_sha})
            return real_check_output(command, **kwargs)

        env = {"GITHUB_EVENT_NAME": event, "GITHUB_REF": ref, "SOURCE_REF": source_ref,
               "GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(self.root / "summary")}
        cwd = Path.cwd()
        try:
            os.chdir(repo)
            with mock.patch.dict(os.environ, env), mock.patch.object(
                    subprocess, "check_output", side_effect=check_output):
                exec(compile(inline_python(jobs(template(sdk, "ci.yml"))["source"])[0],
                             f"{sdk}/ci.yml", "exec"), {})
        finally:
            os.chdir(cwd)
        return dict(line.split("=", 1) for line in output.read_text().splitlines())

    def test_root_manifest_releases_only_for_main_or_version_tag_pushes(self):
        self.write_manifest()
        sha = self.commit()
        cases = (("push", "refs/heads/main", "true"),
                 ("push", "refs/tags/v1.2.3", "true"),
                 ("push", "refs/tags/other", "false"),
                 ("push", "refs/heads/topic", "false"),
                 ("pull_request", "refs/pull/1/merge", "false"),
                 ("pull_request", "refs/heads/main", "false"),
                 ("workflow_dispatch", "refs/heads/main", "false"),
                 ("workflow_dispatch", "refs/tags/v1.2.3", "false"))
        for sdk in SDKS:
            for event, ref, release in cases:
                with self.subTest(sdk=sdk, event=event, ref=ref):
                    outputs = self.select(sdk, event, ref)
                    self.assertEqual(outputs, {"ready": "true", "bootstrap": "false",
                                               "sdk_sha": sha, "core_sha": self.source_sha,
                                               "release": release})

    def test_main_uses_current_first_parent_not_whole_push_range(self):
        self.write_manifest()
        initial = self.commit()
        (self.repo / "README.md").write_text("Documentation change\n")
        docs = self.commit(initial)
        for sdk in SDKS:
            with self.subTest(sdk=sdk, commit="docs"):
                self.assertEqual(self.select(sdk)["release"], "false")
                # An external v* tag is still checked even with unchanged metadata.
                self.assertEqual(self.select(sdk, ref="refs/tags/v1.2.3")["release"], "true")
        self.manifest["version"] = "1.2.4"
        self.write_manifest()
        promoted = self.commit(docs)
        for sdk in SDKS:
            with self.subTest(sdk=sdk, commit="promotion"):
                outputs = self.select(sdk)
                self.assertEqual(outputs["release"], "true")
                self.assertEqual(outputs["sdk_sha"], promoted)

    def test_merge_diff_uses_only_first_parent_even_with_shallow_checkout(self):
        self.write_manifest()
        initial = self.commit()
        self.manifest["version"] = "1.2.4"
        self.write_manifest()
        promotion = self.commit(initial)
        self.git("reset", "--hard", initial)
        (self.repo / "README.md").write_text("Main documentation\n")
        main = self.commit(initial)
        self.write_manifest()
        merged = self.commit(main, promotion)
        clone = self.root / "shallow"
        subprocess.check_output(["git", "clone", "--quiet", "--depth=2", self.repo.as_uri(), str(clone)],
                                stderr=subprocess.STDOUT)
        self.assertTrue((clone / ".git/shallow").is_file())
        for sdk in SDKS:
            with self.subTest(sdk=sdk, first_parent="main"):
                self.assertEqual(self.select(sdk)["release"], "true")
                outputs = self.select(sdk, repo=clone)
                self.assertEqual(outputs["sdk_sha"], merged)
                self.assertEqual(outputs["release"], "true")
        # The same merge tree has no metadata change against the promotion parent.
        self.commit(promotion, main)
        for sdk in SDKS:
            with self.subTest(sdk=sdk, first_parent="promotion"):
                self.assertEqual(self.select(sdk)["release"], "false")

    def test_missing_manifest_and_bootstrap_never_supply_release_evidence(self):
        sha = self.commit()
        for sdk in SDKS:
            with self.subTest(sdk=sdk):
                outputs = self.select(sdk)
                self.assertEqual(outputs, {"ready": "false", "bootstrap": "false",
                                           "sdk_sha": sha, "release": "false"})
                outputs = self.select(sdk, "workflow_dispatch", source_ref="bootstrap/topic")
                self.assertEqual(outputs["bootstrap"], "true")
                self.assertEqual(outputs["ready"], "true")
                self.assertEqual(outputs["release"], "false")
                self.assertEqual(outputs["core_sha"], self.source_sha)
                with self.assertRaisesRegex(AssertionError, "Release push requires SDK provenance"):
                    self.select(sdk, ref="refs/tags/v1.2.3")
        self.assertIn("not release evidence", (self.root / "summary").read_text())

    def test_main_manifest_deletion_fails_instead_of_skipping_release_checks(self):
        self.write_manifest()
        initial = self.commit()
        (self.repo / "sdk-artifact.json").unlink()
        self.commit(initial)
        for sdk in SDKS:
            with self.subTest(sdk=sdk), self.assertRaisesRegex(AssertionError, "Release push requires SDK provenance"):
                self.select(sdk)

    def test_published_manifest_cannot_be_overridden_or_resolve_to_another_sha(self):
        self.write_manifest()
        self.commit()
        for sdk in SDKS:
            with self.subTest(sdk=sdk):
                with self.assertRaisesRegex(AssertionError, "must use its manifest SHA"):
                    self.select(sdk, "workflow_dispatch", source_ref="main")
                self.source_sha = "b" * 40
                with self.assertRaisesRegex(AssertionError, "Published source revision changed"):
                    self.select(sdk)
                self.source_sha = "a" * 40


class ReleaseProvenanceTests(unittest.TestCase):
    def test_tag_entrypoint_creates_only_on_main_push_and_verifies_external_tags(self):
        cases = (("push", "refs/heads/main", True, True),
                 ("push", "refs/tags/v1.2.3", True, False),
                 ("push", "refs/heads/topic", False, False),
                 ("push", "refs/tags/other", False, False),
                 ("pull_request", "refs/heads/main", False, False),
                 ("workflow_dispatch", "refs/heads/main", False, False),
                 ("workflow_dispatch", "refs/tags/v1.2.3", False, False))
        for sdk in SDKS:
            authorize = jobs(template(sdk, "release.yml"))["authorize"]
            tag_step = next(step for step in steps(authorize) if " sdk-tag " in step)
            script = textwrap.dedent(tag_step.split("        run: |\n", 1)[1])
            self.assertNotIn("${{", script)
            for event, ref, allowed, create in cases:
                with self.subTest(sdk=sdk, event=event, ref=ref), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    helper = root / "core/scripts/sdk-ci.py"
                    helper.parent.mkdir(parents=True)
                    helper.write_text("import json, pathlib, sys\n"
                                      "pathlib.Path('tag-args.json').write_text(json.dumps(sys.argv[1:]))\n")
                    env = dict(os.environ, EVENT_NAME=event, SDK_REF=ref)
                    result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", script],
                                            cwd=root, env=env, capture_output=True, text=True)
                    output = root / "tag-args.json"
                    self.assertEqual(result.returncode == 0, allowed, result.stdout + result.stderr)
                    self.assertEqual(output.exists(), allowed)
                    if allowed:
                        args = ["sdk-tag", "--sdk", sdk, "--destination", "sdk"]
                        if create:
                            args.append("--create")
                        self.assertEqual(json.loads(output.read_text()), args)

    def test_supplied_sha_and_protected_ancestry_fail_closed(self):
        source_sha, sdk_sha = "a" * 40, "b" * 40
        for sdk in SDKS:
            for index, script in enumerate(inline_python(template(sdk, "release.yml"))):
                for failure in (None, "source_sha", "sdk_sha", "sdk_head", "repository",
                                "source_format", "sdk_format", "unprotected", "ancestry"):
                    with self.subTest(sdk=sdk, script=index, failure=failure), tempfile.TemporaryDirectory() as tmp:
                        root = Path(tmp)
                        (root / "sdk").mkdir()
                        manifest = {"source_repository": "tinkle-org/libtinkle",
                                    "source_revision": source_sha, "version": "1.2.3"}
                        env = {"SOURCE_SHA": source_sha, "SDK_SHA": sdk_sha,
                               "GITHUB_OUTPUT": str(root / "output")}
                        if failure == "source_sha":
                            env["SOURCE_SHA"] = "c" * 40
                        elif failure == "sdk_sha":
                            env["SDK_SHA"] = "c" * 40
                        elif failure == "repository":
                            manifest["source_repository"] = "untrusted/core"
                        elif failure == "source_format":
                            manifest["source_revision"] = env["SOURCE_SHA"] = "main"
                        elif failure == "sdk_format":
                            env["SDK_SHA"] = "main"
                        (root / "sdk/sdk-artifact.json").write_text(json.dumps(manifest))

                        def check_output(command, **kwargs):
                            if command == ["git", "rev-parse", "HEAD"]:
                                self.assertEqual(kwargs, {"cwd": "sdk", "text": True})
                                return ("c" * 40 if failure == "sdk_head" else sdk_sha) + "\n"
                            if command == ["gh", "api", "repos/tinkle-org/libtinkle/branches/main"]:
                                return json.dumps({"protected": failure != "unprotected"})
                            self.assertEqual(command, ["gh", "api", "repos/tinkle-org/libtinkle/compare/" + source_sha + "...main"])
                            return json.dumps({"merge_base_commit": {"sha": "c" * 40 if failure == "ancestry" else source_sha}})

                        cwd = Path.cwd()
                        try:
                            os.chdir(root)
                            with mock.patch.dict(os.environ, env), mock.patch.object(
                                    subprocess, "check_output", side_effect=check_output):
                                if failure is None:
                                    exec(compile(script, f"{sdk}/release.yml", "exec"), {})
                                    if sdk == "kotlin":
                                        outputs = (root / "output").read_text()
                                        self.assertIn("sha=" + source_sha, outputs)
                                        self.assertIn("sdk_sha=" + sdk_sha, outputs)
                                        self.assertIn("version=1.2.3", outputs)
                                else:
                                    with self.assertRaises(AssertionError):
                                        exec(compile(script, f"{sdk}/release.yml", "exec"), {})
                                    self.assertFalse((root / "output").exists())
                        finally:
                            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
