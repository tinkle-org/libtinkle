"""Execute SDK tag authorization with isolated files, mocked git and HTTP only."""

from contextlib import redirect_stdout
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock
import urllib.error
import zipfile


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("sdk_ci_tags", ROOT / "scripts/sdk-ci.py")
assert SPEC and SPEC.loader
ci = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci)


class SdkTagTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.sha = "b" * 40
        self.base = "d" * 40
        self.source_sha = "a" * 40
        self.tag = "v1.2.3"
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir()
        for key, name in ci.release.ARTIFACTS.items():
            with zipfile.ZipFile(self.artifacts / name, "w") as archive:
                members = ({"TinkleSDK.swift": b"// Swift fixture\n", "TinkleSDKFFI.h": b"// header\n",
                            "module.modulemap": b"module TinkleSDKFFI {}\n"} if key == "swift_bindings"
                           else {"com/tinkle/sdk/tinkle_ffi.kt": b"// Kotlin fixture\n"}
                           if key == "kotlin_bindings" else {"native/payload": b"fixture"})
                for path, content in members.items():
                    archive.writestr(path, content)
        self.valid_manifest = {
            "schema_version": 1, "version": "1.2.3", "source_revision": self.source_sha,
            "source_repository": ci.release.REPOSITORY, "source_tag": "tinkle-ffi-v1.2.3",
            "artifacts": {key: {"filename": name,
                               "url": f"https://github.com/{ci.release.REPOSITORY}/releases/download/tinkle-ffi-v1.2.3/{name}",
                               "sha256": ci.release.digest(self.artifacts / name)}
                          for key, name in ci.release.ARTIFACTS.items()},
        }
        self.snapshot = ci.release.packages.Snapshot({"README.md": b"canonical package\n"})
        self.package_snapshot = self.patch(ci.release, "package_snapshot", return_value=self.snapshot)
        self.run_mock = self.patch(ci.release, "run", side_effect=self.command)
        self.remote_tag = self.patch(ci.release, "remote_tag_revision", return_value=self.source_sha)
        self.authorized_removed = set()
        self.promotion_failure = None
        self.base_failure = False
        # Keep real implementations for the isolated real-git fixture.
        self._real_verify_snapshot_at = ci.release.verify_snapshot_at
        self._real_promotion_deletions = ci.release.promotion_deletions
        self.promotion_deletions = self.patch(ci.release, "promotion_deletions", side_effect=self.deleted)
        self.verify_snapshot = self.patch(ci.release, "verify_snapshot_at", side_effect=self.verified)
        # Keep the real subprocess entrypoint for the isolated real-git fixture.
        # Must be captured before the fail-closed subprocess patch below.
        self._real_run = subprocess.run
        self.opener = mock.Mock()
        self.opener.open.side_effect = self.http
        self.build_opener = self.patch(ci.urllib.request, "build_opener", return_value=self.opener)
        # A new/unmocked process or HTTP path must fail rather than escape this fixture.
        self.patch(ci.subprocess, "run", side_effect=AssertionError("unexpected subprocess"))
        self.patch(ci.urllib.request, "urlopen", side_effect=AssertionError("unexpected HTTP"))
        env = mock.patch.dict(os.environ, {}, clear=True)
        env.start()
        self.addCleanup(env.stop)

    def patch(self, target, name, **kwargs):
        patcher = mock.patch.object(target, name, **kwargs)
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    def deleted(self, checkout, base, manifest, sdk):
        self.trace.append(("promotion_deletions", base))
        self.assertEqual(Path(checkout), self.destination)
        self.assertEqual(base, self.base)
        self.assertEqual(manifest, self.manifest)
        self.assertEqual(sdk, self.sdk)
        if self.promotion_failure:
            raise ValueError(self.promotion_failure)
        return set(self.authorized_removed)

    def verified(self, checkout, revision, expected, removed=()):
        self.trace.append(("verify_snapshot_at", revision, tuple(sorted(set(removed or ())))))
        self.assertEqual(Path(checkout), self.destination)
        self.assertEqual(revision, self.sha)
        self.assertEqual(set(removed or ()), set(self.authorized_removed))
        # Realistic working-tree mirror of release.verify_snapshot_at: every
        # expected byte must be present and every authorized deletion absent.
        for path, content in expected.items():
            target = Path(checkout) / path
            if not target.is_file() or target.read_bytes() != content:
                raise ValueError("promotion snapshot differs: " + path)
        if any((Path(checkout) / path).exists() for path in (removed or ())):
            raise ValueError("promotion retains removed SDK sources")
        return None

    def prepare(self, sdk):
        self.manifest = copy.deepcopy(self.valid_manifest)
        self.sdk = sdk
        self.repo = "tinkle-org/tinkle-sdk-" + sdk
        self.destination = self.root / sdk
        self.destination.mkdir(exist_ok=True)
        self.head = self.sha
        self.published = ci.release.encoded(self.manifest)
        self.refs = [{"ref": "refs/tags/" + self.tag,
                      "object": {"type": "commit", "sha": self.sha}}]
        self.pr = {"number": 17, "merged_at": "2026-01-01T00:00:00Z",
                   "merge_commit_sha": self.sha, "base": {"ref": "main"},
                   "head": {"ref": "sdk-release/1.2.3", "repo": {"full_name": self.repo}},
                   "title": "chore: release SDK v1.2.3"}
        self.prs = [self.pr]
        expected = ci.release.expected_files(self.manifest, self.artifacts, sdk)
        for path, content in expected.items():
            target = self.destination / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            target.chmod(0o644)
        self.files = [{"filename": path, "status": "modified"} for path in expected]
        self.pr["changed_files"] = len(self.files)
        os.environ.clear()
        os.environ.update(GITHUB_REPOSITORY=self.repo, GITHUB_EVENT_NAME="push",
                          GITHUB_REF="refs/heads/main", GITHUB_SHA=self.sha)
        for operation in (self.run_mock, self.remote_tag, self.opener.open,
                          self.build_opener, self.package_snapshot,
                          self.promotion_deletions, self.verify_snapshot):
            operation.reset_mock()
        self.remote_tag.return_value = self.source_sha
        self.api_failure = None
        self.detail_override = None
        self.ancestry_failure = False
        self.base_failure = False
        self.promotion_failure = None
        self.authorized_removed = set()
        self.trace = []

    def http(self, url, timeout):
        self.assertEqual(timeout, 120)
        self.trace.append(("http", url))
        base = f"https://github.com/{ci.release.REPOSITORY}/releases/download/tinkle-ffi-v1.2.3/"
        if url == base + "sdk-release.json":
            return io.BytesIO(self.published)
        key = ci.release.BINDINGS[self.sdk][0]
        self.assertEqual(url, base + ci.release.ARTIFACTS[key])
        return io.BytesIO((self.artifacts / ci.release.ARTIFACTS[key]).read_bytes())

    def command(self, *args, **kwargs):
        self.trace.append(args)
        if args == ("git", "rev-parse", "HEAD"):
            self.assertEqual(kwargs, {"cwd": self.destination})
            return self.head
        if args == ("git", "rev-parse", self.sha + "^1"):
            self.assertEqual(kwargs, {"cwd": self.destination})
            if self.base_failure:
                raise subprocess.CalledProcessError(1, args)
            return self.base
        if args == ("git", "merge-base", "--is-ancestor", self.sha, "origin/main"):
            self.assertEqual(kwargs, {"cwd": self.destination})
            if self.ancestry_failure:
                raise subprocess.CalledProcessError(1, args)
            return ""
        self.assertEqual(kwargs, {})
        responses = {
            ("gh", "api", f"repos/{self.repo}/commits/{self.sha}/pulls"): [
                {key: value for key, value in pr.items() if key != "changed_files"} for pr in self.prs],
            ("gh", "api", f"repos/{self.repo}/pulls/17"): self.detail_override or self.pr,
            ("gh", "api", f"repos/{self.repo}/pulls/17/files?per_page=100"): self.files,
            ("gh", "api", f"repos/{self.repo}/git/matching-refs/tags/{self.tag}"): self.refs,
        }
        if args in responses:
            if self.api_failure == args[2]:
                raise subprocess.CalledProcessError(1, args)
            return json.dumps(responses[args])
        self.assertEqual(args, self.post_command())
        return "{}"

    def post_command(self):
        return ("gh", "api", "--method", "POST", f"repos/{self.repo}/git/refs",
                "-f", "ref=refs/tags/" + self.tag, "-f", "sha=" + self.sha)

    def posts(self):
        return [call.args for call in self.run_mock.call_args_list if "POST" in call.args]

    def authorize(self, create=True):
        output = io.StringIO()
        with redirect_stdout(output):
            result = ci.sdk_tag(self.destination, self.sdk, create)
        self.assertEqual(result, self.tag)
        self.assertEqual(output.getvalue(), self.tag + "\n")

    def assert_rejected(self, message, create=True, error: type[Exception] = ValueError):
        with self.assertRaisesRegex(error, message):
            ci.sdk_tag(self.destination, self.sdk, create)
        self.assertEqual(self.posts(), [])

    def test_wrong_repository_events_and_ref_modes_stop_before_io(self):
        cases = [("GITHUB_REPOSITORY", "other/sdk", True, "wrong SDK repository")]
        for event in ("pull_request", "workflow_dispatch", "workflow_call", ""):
            for create in (True, False):
                cases.append(("GITHUB_EVENT_NAME", event, create, "require push"))
        for ref in ("refs/heads/topic", "refs/tags/v1.2.3", ""):
            cases.append(("GITHUB_REF", ref, True, "creation requires main"))
        for ref in ("refs/heads/main", "refs/tags/tinkle-ffi-v1.2.3", ""):
            cases.append(("GITHUB_REF", ref, False, "verification requires a version tag"))
        for sdk in ("swift", "kotlin"):
            for key, value, create, message in cases:
                with self.subTest(sdk=sdk, key=key, value=value, create=create):
                    self.prepare(sdk)
                    os.environ[key] = value
                    with mock.patch.object(ci.release, "load", wraps=ci.release.load) as load:
                        self.assert_rejected(message, create)
                    load.assert_not_called()
                    self.run_mock.assert_not_called()
                    self.build_opener.assert_not_called()
                    self.remote_tag.assert_not_called()

    def test_event_sha_must_match_checkout_before_manifest_or_network(self):
        for sdk in ("swift", "kotlin"):
            for create in (True, False):
                for event_sha in ("c" * 40, ""):
                    with self.subTest(sdk=sdk, create=create, event_sha=event_sha):
                        self.prepare(sdk)
                        os.environ["GITHUB_SHA"] = event_sha
                        if not create:
                            os.environ["GITHUB_REF"] = "refs/tags/" + self.tag
                        with mock.patch.object(ci.release, "load", wraps=ci.release.load) as load, \
                                mock.patch.object(ci, "published_manifest", wraps=ci.published_manifest) as published:
                            self.assert_rejected("checkout differs from the validated event SHA", create)
                        load.assert_not_called()
                        published.assert_not_called()
                        self.run_mock.assert_called_once_with("git", "rev-parse", "HEAD", cwd=self.destination)
                        self.build_opener.assert_not_called()
                        self.remote_tag.assert_not_called()

    def test_merged_scoped_release_checks_parity_and_existing_tag_without_overwrite(self):
        for sdk in ("swift", "kotlin"):
            with self.subTest(sdk=sdk):
                self.prepare(sdk)
                # A matching-ref prefix response may also contain other version tags.
                self.refs.append({"ref": "refs/tags/v1.2.30", "object": {"type": "tag", "sha": "c" * 40}})
                with mock.patch.object(ci.release, "verify", wraps=ci.release.verify) as verify, \
                        mock.patch.object(ci.release, "promote", wraps=ci.release.promote) as promote, \
                        mock.patch.object(ci.release, "expected_files",
                                          wraps=ci.release.expected_files) as expected_files:
                    self.authorize()
                self.assertEqual(self.posts(), [])
                self.remote_tag.assert_called_once_with(self.manifest["source_tag"])
                self.assertIn(("git", "rev-parse", self.sha + "^1"), self.trace)
                self.assertIn(("promotion_deletions", self.base), self.trace)
                self.assertIn(("verify_snapshot_at", self.sha, ()), self.trace)
                self.assertIn(("git", "merge-base", "--is-ancestor", self.sha, "origin/main"), self.trace)
                self.assertIn(("gh", "api", f"repos/{self.repo}/commits/{self.sha}/pulls"), self.trace)
                self.assertIn(("gh", "api", f"repos/{self.repo}/pulls/17"), self.trace)
                fetch = next(call for call in verify.call_args_list if call.kwargs.get("fetch"))
                self.assertEqual(fetch.args[0], self.manifest)
                self.assertEqual(fetch.args[2], [ci.release.BINDINGS[sdk][0]])
                promote.assert_called_once_with(self.manifest, fetch.args[1], sdk, self.destination, check=True)
                self.package_snapshot.assert_called_with(self.manifest, sdk)
                self.promotion_deletions.assert_called_once_with(self.destination, self.base,
                                                                 self.manifest, sdk)
                # promote(check=True) builds the expected mapping internally, then
                # sdk_tag rebuilds it for the immutable commit verification.
                self.assertEqual(expected_files.call_count, 2)
                self.assertEqual(expected_files.call_args.args, (self.manifest, fetch.args[1], sdk))
                self.verify_snapshot.assert_called_once()
                self.assertEqual(self.trace[0], ("git", "rev-parse", "HEAD"))

    def test_main_creates_only_exact_absent_ref_at_event_sha(self):
        for sdk in ("swift", "kotlin"):
            with self.subTest(sdk=sdk):
                self.prepare(sdk)
                self.refs = [{"ref": "refs/tags/v1.2.30", "object": {"type": "commit", "sha": "c" * 40}}]
                self.authorize()
                self.assertEqual(self.posts(), [self.post_command()])
                self.assertEqual(self.trace[-1], self.post_command())

    def test_tag_push_verifies_but_never_creates_even_when_absent(self):
        for sdk in ("swift", "kotlin"):
            for state in ("matching", "absent", "wrong-version"):
                with self.subTest(sdk=sdk, state=state):
                    self.prepare(sdk)
                    os.environ["GITHUB_REF"] = "refs/tags/" + ("v9.9.9" if state == "wrong-version" else self.tag)
                    if state == "absent":
                        self.refs = []
                        self.assert_rejected("missing SDK tag", create=False)
                    elif state == "wrong-version":
                        self.assert_rejected("SDK tag/version mismatch", create=False)
                    else:
                        self.authorize(create=False)
                    self.assertEqual(self.posts(), [])

    def test_existing_conflicting_annotated_or_duplicate_refs_are_never_overwritten(self):
        for sdk in ("swift", "kotlin"):
            for state in ("moved", "annotated", "duplicate"):
                for create in (True, False):
                    with self.subTest(sdk=sdk, state=state, create=create):
                        self.prepare(sdk)
                        if not create:
                            os.environ["GITHUB_REF"] = "refs/tags/" + self.tag
                        if state == "moved":
                            self.refs[0]["object"]["sha"] = "c" * 40
                        elif state == "annotated":
                            self.refs[0]["object"]["type"] = "tag"
                        else:
                            self.refs *= 2
                        self.assert_rejected("refusing to overwrite", create)

    def test_only_one_merged_same_repo_release_pr_for_exact_event_sha_qualifies(self):
        for sdk in ("swift", "kotlin"):
            for failure in ("missing", "duplicate", "unmerged", "wrong-sha", "wrong-base",
                            "fork", "deleted-fork", "wrong-branch", "wrong-title"):
                with self.subTest(sdk=sdk, failure=failure):
                    self.prepare(sdk)
                    if failure == "missing":
                        self.prs = []
                    elif failure == "duplicate":
                        self.prs *= 2
                    elif failure == "unmerged":
                        self.pr["merged_at"] = None
                    elif failure == "wrong-sha":
                        self.pr["merge_commit_sha"] = "c" * 40
                    elif failure == "wrong-base":
                        self.pr["base"]["ref"] = "topic"
                    elif failure == "fork":
                        self.pr["head"]["repo"]["full_name"] = "fork/" + self.repo.split("/")[1]
                    elif failure == "deleted-fork":
                        self.pr["head"]["repo"] = None
                    elif failure == "wrong-branch":
                        self.pr["head"]["ref"] = "sdk-release/9.9.9"
                    else:
                        self.pr["title"] = "chore: release SDK v9.9.9"
                    self.assert_rejected("merged same-repository SDK release PR")
                    self.assertFalse(any("/files?" in str(call) for call in self.trace))

    def test_selected_pr_detail_is_revalidated_before_using_file_count(self):
        for sdk in ("swift", "kotlin"):
            for field, value in (("number", 18), ("merged_at", None),
                                 ("merge_commit_sha", "c" * 40), ("title", "unrelated")):
                with self.subTest(sdk=sdk, field=field):
                    self.prepare(sdk)
                    self.detail_override = dict(self.pr, **{field: value})
                    self.assert_rejected("release PR changed during authorization")
                    self.assertFalse(any("/files?" in str(call) for call in self.trace))

    def test_release_pr_scope_is_complete_managed_and_includes_provenance(self):
        for sdk in ("swift", "kotlin"):
            for failure in ("truncated", "unrelated", "no-provenance", "renamed"):
                with self.subTest(sdk=sdk, failure=failure):
                    self.prepare(sdk)
                    if failure == "truncated":
                        self.pr["changed_files"] += 1
                    elif failure == "unrelated":
                        self.files.append({"filename": ".github/workflows/ci.yml", "status": "added"})
                    elif failure == "no-provenance":
                        self.files = [file for file in self.files if file["filename"] != "sdk-artifact.json"]
                    else:
                        self.files[0]["status"] = "renamed"
                    if failure != "truncated":
                        self.pr["changed_files"] = len(self.files)
                    self.assert_rejected("change only the managed SDK snapshot")
                    self.assertFalse(any("matching-refs" in str(call) for call in self.trace))

    def test_manifest_provenance_and_published_core_tag_fail_closed(self):
        for sdk in ("swift", "kotlin"):
            for failure, message in (("invalid-local", "expected exact source SHA"),
                                     ("published-drift", "differs from published core release"),
                                     ("core-tag-drift", "core tag differs")):
                with self.subTest(sdk=sdk, failure=failure):
                    self.prepare(sdk)
                    changed = copy.deepcopy(self.manifest)
                    changed["source_revision"] = "main" if failure == "invalid-local" else "c" * 40
                    if failure == "invalid-local":
                        (self.destination / "sdk-artifact.json").write_text(json.dumps(changed))
                    elif failure == "published-drift":
                        self.published = ci.release.encoded(changed)
                    else:
                        self.remote_tag.return_value = "c" * 40
                    self.assert_rejected(message)
                    self.assertFalse(any(call.args[:2] == ("gh", "api") for call in self.run_mock.call_args_list))
                    if failure == "invalid-local":
                        self.build_opener.assert_not_called()

    def test_real_binding_snapshot_inventory_and_checksum_guards_precede_creation(self):
        for sdk in ("swift", "kotlin"):
            for failure, message in (("binding", "generated binding drift"),
                                     ("package", "SDK snapshot drift"),
                                     ("mode", "SDK snapshot mode drift"),
                                     ("inventory", "managed inventory differs"),
                                     ("checksum", "checksum mismatch"),
                                     ("snapshot", "canonical snapshot unavailable")):
                with self.subTest(sdk=sdk, failure=failure):
                    self.prepare(sdk)
                    self.refs = []
                    if failure == "binding":
                        (self.destination / ci.release.BINDINGS[sdk][2]).write_bytes(b"drift")
                    elif failure == "package":
                        (self.destination / "README.md").write_bytes(b"drift")
                    elif failure == "mode":
                        (self.destination / "README.md").chmod(0o755)
                    elif failure == "inventory":
                        (self.destination / ci.release.INVENTORY).write_bytes(b"{}\n")
                    elif failure == "checksum":
                        key = ci.release.BINDINGS[sdk][0]
                        self.manifest["artifacts"][key]["sha256"] = "0" * 64
                        self.published = ci.release.encoded(self.manifest)
                        (self.destination / "sdk-artifact.json").write_bytes(self.published)
                    if failure == "snapshot":
                        with mock.patch.object(ci.release, "package_snapshot",
                                               side_effect=ValueError(message)):
                            self.assert_rejected(message)
                    else:
                        self.assert_rejected(message)
                    self.assertFalse(any("matching-refs" in str(call) for call in self.trace))

    def test_ancestry_and_ref_lookup_errors_cannot_be_treated_as_absent_tag(self):
        for sdk in ("swift", "kotlin"):
            for failure in ("ancestry", "ref-api", "pr-api", "files-api", "manifest-http"):
                with self.subTest(sdk=sdk, failure=failure):
                    self.prepare(sdk)
                    self.refs = []
                    self.ancestry_failure = failure == "ancestry"
                    paths = {"ref-api": f"repos/{self.repo}/git/matching-refs/tags/{self.tag}",
                             "pr-api": f"repos/{self.repo}/commits/{self.sha}/pulls",
                             "files-api": f"repos/{self.repo}/pulls/17/files?per_page=100"}
                    self.api_failure = paths.get(failure)
                    if failure == "manifest-http":
                        with mock.patch.object(self.opener, "open", side_effect=urllib.error.URLError("offline")), \
                                mock.patch.object(ci.release.time, "sleep"):
                            self.assert_rejected("offline", error=urllib.error.URLError)
                    else:
                        self.assert_rejected("non-zero exit status", error=subprocess.CalledProcessError)

    def test_authorized_old_source_deletion_passes_only_as_removed_and_absent(self):
        for sdk in ("swift", "kotlin"):
            with self.subTest(sdk=sdk):
                self.prepare(sdk)
                self.refs = []
                self.authorized_removed = {"OLD.md"}
                self.files.append({"filename": "OLD.md", "status": "removed"})
                self.pr["changed_files"] = len(self.files)
                self.assertFalse((self.destination / "OLD.md").exists())
                self.authorize()
                self.assertEqual(self.posts(), [self.post_command()])
                self.assertIn(("git", "rev-parse", self.sha + "^1"), self.trace)
                self.assertIn(("promotion_deletions", self.base), self.trace)
                self.assertIn(("verify_snapshot_at", self.sha, ("OLD.md",)), self.trace)
                self.promotion_deletions.assert_called_once_with(self.destination, self.base,
                                                                 self.manifest, sdk)

    def test_authorized_deletion_requires_removed_file_status(self):
        for sdk in ("swift", "kotlin"):
            with self.subTest(sdk=sdk):
                self.prepare(sdk)
                self.authorized_removed = {"OLD.md"}
                self.files.append({"filename": "OLD.md", "status": "modified"})
                self.pr["changed_files"] = len(self.files)
                self.assert_rejected("change only the managed SDK snapshot")
                self.assertFalse(any("matching-refs" in str(call) for call in self.trace))
                self.assertFalse(any("verify_snapshot_at" in str(call) for call in self.trace))

    def test_unmanaged_deletions_fail_before_post_even_with_removed_status(self):
        for sdk in ("swift", "kotlin"):
            for failure in ("github-removed", "readme-removed", "unmanaged-removed"):
                with self.subTest(sdk=sdk, failure=failure):
                    self.prepare(sdk)
                    self.refs = []
                    if failure == "github-removed":
                        self.files.append({"filename": ".github/workflows/ci.yml", "status": "removed"})
                    elif failure == "readme-removed":
                        for entry in self.files:
                            if entry["filename"] == "README.md":
                                entry["status"] = "removed"
                                break
                    else:
                        self.files.append({"filename": "UNMANAGED.md", "status": "removed"})
                    self.pr["changed_files"] = len(self.files)
                    self.assert_rejected("change only the managed SDK snapshot")
                    self.assertFalse(any("matching-refs" in str(call) for call in self.trace))
                    self.assertEqual(self.posts(), [])

    def test_stale_retained_authorized_path_fails_before_post(self):
        for sdk in ("swift", "kotlin"):
            with self.subTest(sdk=sdk):
                self.prepare(sdk)
                self.refs = []
                self.authorized_removed = {"OLD.md"}
                self.files.append({"filename": "OLD.md", "status": "removed"})
                self.pr["changed_files"] = len(self.files)
                (self.destination / "OLD.md").write_bytes(b"stale\n")
                self.assert_rejected("promotion retains removed SDK sources")
                self.assertFalse(any("matching-refs" in str(call) for call in self.trace))
                self.assertEqual(self.posts(), [])

    def test_first_parent_and_promotion_provenance_fail_closed_before_post(self):
        for sdk in ("swift", "kotlin"):
            for failure, message in (("base", "non-zero exit status"),
                                     ("promotion", "managed inventory differs")):
                with self.subTest(sdk=sdk, failure=failure):
                    self.prepare(sdk)
                    self.refs = []
                    if failure == "base":
                        self.base_failure = True
                        with self.assertRaisesRegex(subprocess.CalledProcessError,
                                                   "non-zero exit status"):
                            ci.sdk_tag(self.destination, self.sdk, True)
                    else:
                        self.promotion_failure = "managed inventory differs"
                        self.assert_rejected(message)
                    self.assertEqual(self.posts(), [])
                    self.assertFalse(any("matching-refs" in str(call) for call in self.trace))

    def test_real_git_snapshot_absence_proves_mocks_do_not_bypass_security(self):
        # Isolated real-git fixture: the real verify_snapshot_at guard must
        # prove absence at the commit, not trust the mocked working tree.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            self._real_run(["git", "init", "--quiet"], cwd=root, check=True,
                           capture_output=True)
            self._real_run(["git", "config", "user.name", "fixture"], cwd=root,
                           check=True, capture_output=True)
            self._real_run(["git", "config", "user.email", "fixture@example.com"],
                           cwd=root, check=True, capture_output=True)
            (root / "keep.txt").write_text("keep\n")
            (root / "gone.txt").write_text("gone\n")
            self._real_run(["git", "add", "."], cwd=root, check=True, capture_output=True)
            self._real_run(["git", "-c", "user.name=fixture",
                            "-c", "user.email=fixture@example.com",
                            "commit", "-m", "init"], cwd=root, check=True, capture_output=True)
            out = self._real_run(["git", "rev-parse", "HEAD"], cwd=root, check=True,
                                 capture_output=True, text=True)
            sha = out.stdout.strip()
            expected = ci.release.packages.Snapshot({"keep.txt": b"keep\n"})
            expected.modes = {"keep.txt": 0o100644}
            # Absent path passes; retained path fails even though the PR file
            # list is not involved here. Restore the real subprocess entrypoint
            # because the fixture fail-closes all other subprocess use.
            with mock.patch.object(ci.subprocess, "run", self._real_run):
                self._real_verify_snapshot_at(str(root), sha, expected, ["absent.txt"])
                with self.assertRaisesRegex(ValueError, "promotion retains removed"):
                    self._real_verify_snapshot_at(str(root), sha, expected, ["gone.txt"])
                with self.assertRaisesRegex(ValueError, "promotion snapshot differs"):
                    self._real_verify_snapshot_at(str(root), sha,
                                                  ci.release.packages.Snapshot({"missing.txt": b"x\n"}), [])


if __name__ == "__main__":
    unittest.main()
