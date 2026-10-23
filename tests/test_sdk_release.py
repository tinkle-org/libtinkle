"""Distribution security and provenance tests; no network, Rust, or credentials."""

import copy
import importlib.util
import io
import hashlib
import json
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import warnings
import zipfile

SPEC = importlib.util.spec_from_file_location(
    "sdk_release", Path(__file__).resolve().parents[1] / "scripts/sdk-release.py")
assert SPEC and SPEC.loader
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
CI_SPEC = importlib.util.spec_from_file_location(
    "sdk_ci", Path(__file__).resolve().parents[1] / "scripts/sdk-ci.py")
assert CI_SPEC and CI_SPEC.loader
ci = importlib.util.module_from_spec(CI_SPEC)
CI_SPEC.loader.exec_module(ci)


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir()
        for key, name in release.ARTIFACTS.items():
            with zipfile.ZipFile(self.artifacts / name, "w") as archive:
                if key == "swift_bindings":
                    for entry in ("TinkleSDK.swift", "TinkleSDKFFI.h", "module.modulemap"):
                        archive.writestr(entry, "generated " + entry)
                elif key == "kotlin_bindings":
                    archive.writestr("com/tinkle/sdk/tinkle_ffi.kt", "generated Kotlin")
                else:
                    archive.writestr("native/payload", b"native")
        self.manifest = {
            "schema_version": 1, "version": "1.2.3-rc.1", "source_revision": "a" * 40,
            "source_repository": release.REPOSITORY, "source_tag": "tinkle-ffi-v1.2.3-rc.1",
            "artifacts": {key: {"filename": filename,
                "url": f"https://github.com/{release.REPOSITORY}/releases/download/tinkle-ffi-v1.2.3-rc.1/{filename}",
                "sha256": release.digest(self.artifacts / filename)}
                for key, filename in release.ARTIFACTS.items()},
        }

    def test_manifest_round_trip_and_checksums(self):
        metadata = self.root / "manifest.json"
        metadata.write_bytes(release.encoded(self.manifest))
        self.assertEqual(release.load(metadata), self.manifest)
        release.verify(self.manifest, self.artifacts)
        (self.artifacts / release.ARTIFACTS["ios"]).write_bytes(b"different")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            release.verify(self.manifest, self.artifacts)

    def test_reject_manifest_mutations(self):
        changes = [
            ("schema_version", True), ("schema_version", 2), ("version", "01.2.3"),
            ("version", "1.2.3-01"), ("version", "1.2.3\n"),
            ("source_revision", "main"), ("source_revision", "A" * 40),
            ("source_tag", "v1.2.3-rc.1"), ("source_repository", "evil/libtinkle"),
            ("artifacts", {}),
        ]
        for key, value in changes:
            with self.subTest(key=key, value=value):
                manifest = copy.deepcopy(self.manifest)
                manifest[key] = value
                with self.assertRaises(ValueError):
                    release.validate(manifest)

    def test_reject_artifact_urls_and_names(self):
        for field, value in [
            ("url", "http://github.com/tinklehq/libtinkle/releases/download/x/a.zip"),
            ("url", self.manifest["artifacts"]["ios"]["url"] + "?token=x"),
            ("url", self.manifest["artifacts"]["ios"]["url"].replace("tinklehq", "other")),
            ("url", "https://github.com@evil.example/file.zip"),
            ("filename", "../TinkleSDKFFI.xcframework.zip"),
            ("sha256", "A" * 64), ("sha256", "00"),
        ]:
            with self.subTest(field=field, value=value):
                manifest = copy.deepcopy(self.manifest)
                manifest["artifacts"]["ios"][field] = value
                with self.assertRaises(ValueError):
                    release.validate(manifest)

    def test_duplicate_json_keys(self):
        path = self.root / "duplicate.json"
        path.write_text('{"version":"1.0.0","version":"2.0.0"}')
        with self.assertRaisesRegex(ValueError, "duplicate JSON"):
            release.load(path)

    def test_disallowed_download_never_opens_network(self):
        artifact = copy.deepcopy(self.manifest["artifacts"]["ios"])
        artifact["url"] = "https://evil.example/TinkleSDKFFI.xcframework.zip"
        with patch.object(release.urllib.request, "build_opener") as opener:
            with self.assertRaises(ValueError):
                release.download(artifact, self.artifacts)
            opener.assert_not_called()

    def test_allowed_download_hash_and_size_are_verified(self):
        artifact = self.manifest["artifacts"]["ios"]
        payload = (self.artifacts / artifact["filename"]).read_bytes()
        destination = self.root / "downloads"
        destination.mkdir()
        with patch.object(release.urllib.request, "build_opener") as opener:
            opener.return_value.open.side_effect = lambda *a, **kw: io.BytesIO(payload)
            self.assertEqual(release.download(artifact, destination).read_bytes(), payload)
            opener.return_value.open.assert_called_with(artifact["url"], timeout=120)
            changed = dict(artifact, sha256="0" * 64)
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                release.download(changed, destination)
            with patch.object(release, "MAX_DOWNLOAD", 1), self.assertRaisesRegex(ValueError, "too large"):
                release.download(artifact, destination)

    def test_redirect_host_allowlist(self):
        redirect = release.ReleaseRedirect()
        request = release.urllib.request.Request(self.manifest["artifacts"]["ios"]["url"])
        for url in ("http://release-assets.githubusercontent.com/a", "https://evil.example/a",
                    "https://github.com:8443/a", "https://user@github.com/a"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                redirect.redirect_request(request, None, 302, "", {}, url)
        result = redirect.redirect_request(request, None, 302, "", {},
                                           "https://release-assets.githubusercontent.com/a?sig=ok")
        self.assertEqual(result.host, "release-assets.githubusercontent.com")

    def test_archive_rejects_traversal_and_duplicates_before_writing(self):
        for names in (["../escape"], ["/absolute"], ["a/../../escape"], ["a\\b"],
                      ["a/./b"], ["a//b"], ["C:/x"], ["a", "a"], ["A", "a"],
                      ["a", "a/b"]):
            with self.subTest(names=names):
                archive = self.root / "bad.zip"
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    with zipfile.ZipFile(archive, "w") as output:
                        for name in names:
                            output.writestr(name, "bad")
                destination = self.root / "extract"
                with self.assertRaises(ValueError):
                    release.extract_zip(archive, destination)
                self.assertFalse(destination.exists())

    def test_archive_rejects_symlinks_and_bounds(self):
        archive = self.root / "link.zip"
        with zipfile.ZipFile(archive, "w") as output:
            link = zipfile.ZipInfo("link")
            link.create_system = 3
            link.external_attr = (stat.S_IFLNK | 0o777) << 16
            output.writestr(link, "../outside")
        with self.assertRaisesRegex(ValueError, "symlink"):
            release.inspect_zip(archive)
        archive = self.artifacts / release.ARTIFACTS["ios"]
        with patch.object(release, "MAX_EXPANDED", 1), self.assertRaises(ValueError):
            release.inspect_zip(archive)
        with patch.object(release, "MAX_ENTRIES", 0), self.assertRaises(ValueError):
            release.inspect_zip(archive)

    def test_extraction_refuses_existing_symlink(self):
        destination = self.root / "extract"
        destination.mkdir()
        (destination / "native").symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlink"):
            release.extract_zip(self.artifacts / release.ARTIFACTS["ios"], destination)
        self.assertFalse((self.root / "payload").exists())

    def test_promotion_scope_parity_and_idempotence(self):
        for sdk in release.BINDINGS:
            destination = self.root / sdk
            destination.mkdir()
            handwritten = destination / "handwritten.txt"
            handwritten.write_text("keep me")
            self.assertTrue(release.promote(self.manifest, self.artifacts, sdk, destination))
            metadata = destination / "sdk-artifact.json"
            before = metadata.stat().st_mtime_ns
            self.assertFalse(release.promote(self.manifest, self.artifacts, sdk, destination))
            self.assertEqual(before, metadata.stat().st_mtime_ns)
            self.assertEqual(release.load(metadata), self.manifest)
            self.assertEqual(handwritten.read_text(), "keep me")
            target = destination / release.BINDINGS[sdk][2]
            self.assertEqual({str(p.relative_to(destination)) for p in destination.rglob("*")
                              if p.is_file()}, {"handwritten.txt", "sdk-artifact.json",
                                                release.BINDINGS[sdk][2]})
            release.promote(self.manifest, self.artifacts, sdk, destination, check=True)
            target.write_text("drift")
            with self.assertRaisesRegex(ValueError, "drift"):
                release.promote(self.manifest, self.artifacts, sdk, destination, check=True)

    def test_existing_version_provenance_cannot_change(self):
        destination = self.root / "swift"
        release.promote(self.manifest, self.artifacts, "swift", destination)
        manifest = copy.deepcopy(self.manifest)
        manifest["source_revision"] = "b" * 40
        with self.assertRaisesRegex(ValueError, "provenance"):
            release.promote(manifest, self.artifacts, "swift", destination)

    def test_missing_pat_blocks_before_clone(self):
        with patch.dict(release.os.environ, {"SDK_DISTRIBUTION_TOKEN": ""}), \
                patch.object(release, "run") as command:
            with self.assertRaisesRegex(ValueError, "BLOCKED"):
                release.open_pr(self.manifest, self.artifacts, "swift", "tinklehq/tinkle-sdk-swift")
            command.assert_not_called()

    def test_closed_promotion_is_rejected_before_clone_or_push(self):
        with patch.dict(release.os.environ, {"SDK_DISTRIBUTION_TOKEN": "test-placeholder"}), \
                patch.object(release, "run", return_value=json.dumps([self.promotion_pr("CLOSED")])) as command:
            with self.assertRaisesRegex(ValueError, "PR was closed"):
                release.open_pr(self.manifest, self.artifacts, "kotlin", "tinklehq/tinkle-sdk-kotlin")
            self.assertEqual(command.call_count, 1)
            self.assertEqual(command.call_args.args[:3], ("gh", "pr", "list"))

    def test_existing_promotion_is_verified_without_commit_or_push(self):
        calls = []
        binding = b"generated Kotlin"
        def command(*args, **kwargs):
            calls.append(args)
            if args[:3] == ("gh", "pr", "list"):
                return json.dumps([self.promotion_pr("OPEN")])
            if args[:3] == ("gh", "repo", "clone"):
                Path(args[4]).mkdir()
            if args[:2] == ("git", "ls-remote"):
                return "a" * 40 + "\trefs/heads/sdk-release/1.2.3-rc.1"
            if args[:2] == ("git", "merge-base"):
                return "b" * 40
            if args[:2] == ("git", "diff"):
                return "sdk-artifact.json\nsrc/main/kotlin/com/tinkle/sdk/tinkle_ffi.kt"
            return ""
        def remote_file(checkout, revision, relative):
            return release.encoded(self.manifest) if relative == "sdk-artifact.json" else binding
        with patch.dict(release.os.environ, {"SDK_DISTRIBUTION_TOKEN": "test-placeholder"}), \
                patch.object(release, "run", side_effect=command), \
                patch.object(release, "git_file", side_effect=remote_file):
            release.open_pr(self.manifest, self.artifacts, "kotlin", "tinklehq/tinkle-sdk-kotlin")
        self.assertTrue(any(c[:2] == ("git", "fetch") for c in calls))
        self.assertFalse(any("commit" in c or "push" in c or "create" in c for c in calls))

    def test_merged_promotion_replay_checks_historical_provenance(self):
        calls = []
        def command(*args, **kwargs):
            calls.append(args)
            if args[:3] == ("gh", "pr", "list"):
                return json.dumps([self.promotion_pr("MERGED", mergeCommit={"oid": "c" * 40})])
            if args[:3] == ("gh", "repo", "clone"):
                Path(args[4]).mkdir()
                # main has advanced; replay must not attempt a downgrade.
                (Path(args[4]) / "sdk-artifact.json").write_text("newer main release")
            if args[:2] == ("git", "show"):
                return json.dumps(self.manifest)
            return ""
        with patch.dict(release.os.environ, {"SDK_DISTRIBUTION_TOKEN": "test-placeholder"}), \
                patch.object(release, "run", side_effect=command), \
                patch.object(release, "git_file", return_value=b"generated Kotlin"):
            release.open_pr(self.manifest, self.artifacts, "kotlin", "tinklehq/tinkle-sdk-kotlin")
        self.assertFalse(any("commit" in c or "push" in c or "checkout" in c or "create" in c for c in calls))

    def promotion_pr(self, state, **overrides):
        return dict({"state": state, "url": "existing", "isCrossRepository": False,
                     "headRepositoryOwner": {"login": "tinklehq"},
                     "headRepository": {"name": "tinkle-sdk-kotlin"}}, **overrides)

    def test_fork_pr_with_same_release_branch_is_not_a_promotion(self):
        own = self.promotion_pr("OPEN")
        fork = self.promotion_pr("CLOSED", isCrossRepository=True,
                                 headRepositoryOwner={"login": "other"})
        self.assertEqual(release.same_repository_prs([fork, own], "tinklehq/tinkle-sdk-kotlin"), [own])

    def test_release_plz_selects_only_ffi(self):
        self.assertEqual(release.release_output('[{"package_name":"libtinkle"}]'), "")
        output = [{"package_name": "tinkle-ffi", "tag": self.manifest["source_tag"],
                   "version": self.manifest["version"], "prs": []}]
        self.assertEqual(release.release_output(json.dumps(output)), self.manifest["source_tag"])
        output[0]["tag"] = "$(untrusted)"
        with self.assertRaises(ValueError):
            release.release_output(json.dumps(output))

    def test_build_manifest_uses_locked_workspace_metadata(self):
        metadata = {"packages": [{"name": "tinkle-ffi", "version": "1.2.3-rc.1", "id": "ffi"}],
                    "workspace_members": ["ffi"]}
        with patch.object(release, "run", side_effect=[json.dumps(metadata), "a" * 40, "", ""]) as command:
            self.assertEqual(release.build_manifest(self.root, self.artifacts), self.manifest)
            self.assertEqual(command.call_args_list[0].args,
                             ("cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"))

    def test_publish_retries_without_overwriting_assets(self):
        metadata = self.artifacts / "sdk-release.json"
        metadata.write_bytes(release.encoded(self.manifest))
        uploaded = {}
        failed_once = False
        calls = []

        def command(*args, **kwargs):
            nonlocal failed_once
            calls.append(args)
            if args[:2] == ("git", "rev-parse"):
                return self.manifest["source_revision"]
            if args[:2] == ("gh", "api"):
                return json.dumps({"tag_name": self.manifest["source_tag"], "assets": [
                    {"name": name, "digest": "sha256:" + checksum}
                    for name, checksum in uploaded.items()]})
            if args[:3] == ("gh", "release", "upload"):
                path = Path(args[4])
                uploaded[path.name] = release.digest(path)
                if not failed_once:
                    failed_once = True
                    raise release.subprocess.CalledProcessError(1, list(args))
                return ""
            self.fail(f"unexpected command: {args}")

        with patch.object(release, "authorize_source"), patch.object(release, "run", command), \
                patch.object(release.time, "sleep"):
            release.publish(self.manifest, self.artifacts, self.root)
        self.assertEqual(set(uploaded), {*release.ARTIFACTS.values(), "sdk-release.json"})
        self.assertEqual(sum(c[:3] == ("gh", "release", "upload") for c in calls), 5)
        self.assertFalse(any("--clobber" in c or "create" in c for c in calls))

    def test_publish_existing_asset_mismatch_fails_closed(self):
        (self.artifacts / "sdk-release.json").write_bytes(release.encoded(self.manifest))
        def command(*args, **kwargs):
            if args[0] == "git":
                return self.manifest["source_revision"]
            return json.dumps({"tag_name": self.manifest["source_tag"], "assets": [{
                "name": release.ARTIFACTS["ios"], "digest": "sha256:" + "0" * 64}]})
        with patch.object(release, "authorize_source"), patch.object(release, "run", command):
            with self.assertRaisesRegex(ValueError, "immutable release asset mismatch"):
                release.publish(self.manifest, self.artifacts, self.root)

    def test_publish_detects_moved_remote_tag(self):
        revision = self.manifest["source_revision"]
        def command(*args, **kwargs):
            if args[0] == "git":
                return "" if args[1] == "merge-base" else revision
            if args[-1].endswith("branches/main"):
                return '{"protected":true}'
            return json.dumps({"object": {"type": "commit", "sha": "b" * 40}})
        with patch.object(release, "workspace_version", return_value=self.manifest["version"]), \
                patch.object(release, "run", command):
            with self.assertRaisesRegex(ValueError, "remote source tag moved"):
                release.authorize_source(self.root, self.manifest["source_tag"], True)

    def test_unprotected_main_blocks_publication(self):
        revision = self.manifest["source_revision"]
        def command(*args, **kwargs):
            if args[0] == "git":
                return "" if args[1] == "merge-base" else revision
            return '{"protected":false}'
        with patch.object(release, "workspace_version", return_value=self.manifest["version"]), \
                patch.object(release, "run", side_effect=command), \
                patch.object(release, "remote_tag_revision") as remote:
            with self.assertRaisesRegex(ValueError, "protected main"):
                release.authorize_source(self.root, self.manifest["source_tag"], True)
            remote.assert_not_called()

    def test_kotlin_ci_inputs_match_sdk_checksum_contract(self):
        env = ci.android_env(self.artifacts)
        self.assertEqual(env["TINKLE_SDK_ARTIFACT_DIR"], str(self.artifacts.resolve()))
        self.assertEqual(env["TINKLE_SDK_ANDROID_SHA256"], self.manifest["artifacts"]["android"]["sha256"])
        self.assertEqual(env["TINKLE_SDK_KOTLIN_BINDINGS_SHA256"],
                         self.manifest["artifacts"]["kotlin_bindings"]["sha256"])

    def test_sdk_ci_rejects_binding_drift_without_replacing_it(self):
        destination = self.root / "kotlin"
        release.promote(self.manifest, self.artifacts, "kotlin", destination)
        binding = destination / release.BINDINGS["kotlin"][2]
        binding.write_text("drift must remain visible")
        args = SimpleNamespace(sdk="kotlin", destination=destination, artifacts=self.artifacts,
                               bootstrap=False, candidate=False, download=False)
        with self.assertRaisesRegex(ValueError, "binding drift"):
            ci.stage(args)
        self.assertEqual(binding.read_text(), "drift must remain visible")
        args.bootstrap = True
        with self.assertRaisesRegex(ValueError, "bootstrap cannot replace"):
            ci.stage(args)

    def test_candidate_allows_same_version_without_mutating_release_rules(self):
        destination = self.root / "candidate"
        release.promote(self.manifest, self.artifacts, "kotlin", destination)
        candidate = copy.deepcopy(self.manifest)
        candidate["source_revision"] = "b" * 40
        (self.artifacts / "sdk-release.json").write_bytes(release.encoded(candidate))
        args = SimpleNamespace(sdk="kotlin", destination=destination, artifacts=self.artifacts,
                               bootstrap=False, candidate=True, download=False)
        ci.stage(args)
        self.assertEqual(release.load(destination / "sdk-artifact.json"), candidate)
        with self.assertRaisesRegex(ValueError, "provenance"):
            release.promote(self.manifest, self.artifacts, "kotlin", destination)

    def test_android_build_runs_buildsrc_tests_separately(self):
        destination = self.root / "android"
        with patch.object(ci, "call") as command:
            with self.assertRaisesRegex(ValueError, "one final release AAR"):
                ci.android_build(destination, self.artifacts)
            self.assertEqual(command.call_args_list[0].args, ("bash", "gradlew", "-p", "buildSrc", "test"))

    def test_android_runtime_requires_exact_api_and_page_size(self):
        with patch.object(ci.release, "run", side_effect=["4096", "35"]), patch.object(ci, "call") as command:
            with self.assertRaisesRegex(ValueError, "wrong emulator API"):
                ci.android_test(self.root, self.artifacts, 4096, 36)
            command.assert_not_called()
        with patch.object(ci.release, "run", return_value="4096"), patch.object(ci, "call") as command:
            with self.assertRaisesRegex(ValueError, "wrong emulator page size"):
                ci.android_test(self.root, self.artifacts, 16384, 36)
            command.assert_not_called()

    def debug_candidate(self):
        libraries = {}
        with zipfile.ZipFile(self.artifacts / release.ARTIFACTS["android"], "w") as archive:
            for abi in release.ANDROID_ABIS:
                data = ("native " + abi).encode()
                archive.writestr(f"jniLibs/{abi}/libtinkle_ffi.so", data)
                libraries[abi] = {"build_id": "d" * 40, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
        self.manifest["artifacts"]["android"]["sha256"] = release.digest(self.artifacts / release.ARTIFACTS["android"])
        metadata = {"version": self.manifest["version"], "source_revision": self.manifest["source_revision"],
                    "profile": "android-release", "min_sdk": 24, "release_matrix": True, "libraries": libraries}
        (self.artifacts / release.DEBUG_ASSETS["android_metadata"]).write_text(json.dumps(metadata))
        with zipfile.ZipFile(self.artifacts / release.DEBUG_ASSETS["android_symbols"], "w") as archive:
            for abi in release.ANDROID_ABIS:
                archive.writestr(f"{abi}/libtinkle_ffi.so.dbg", "debug " + abi)
        debug = release.build_debug_assets(self.manifest, self.artifacts)
        (self.artifacts / release.DEBUG_MANIFEST).write_bytes(release.encoded_debug(debug))
        (self.artifacts / "sdk-release.json").write_bytes(release.encoded(self.manifest))
        return debug, metadata

    def test_debug_sidecar_preserves_fixed_manifest_and_ties_shipping_bytes(self):
        debug, metadata = self.debug_candidate()
        self.assertEqual(set(self.manifest["artifacts"]), set(release.ARTIFACTS))
        self.assertEqual(release.verify_debug_assets(self.manifest, self.artifacts), debug)
        self.assertEqual(debug["sdk_release_sha256"], release.digest(self.artifacts / "sdk-release.json"))
        metadata["libraries"]["x86_64"]["sha256"] = "0" * 64
        (self.artifacts / release.DEBUG_ASSETS["android_metadata"]).write_text(json.dumps(metadata))
        with self.assertRaisesRegex(ValueError, "differs from shipped"):
            release.build_debug_assets(self.manifest, self.artifacts)

    def test_debug_sidecar_rejects_other_source_and_tampered_symbols(self):
        debug, _ = self.debug_candidate()
        wrong = copy.deepcopy(debug)
        wrong["source_revision"] = "e" * 40
        with self.assertRaisesRegex(ValueError, "provenance"):
            release.validate_debug(wrong, self.manifest)
        (self.artifacts / release.DEBUG_ASSETS["android_symbols"]).write_bytes(b"wrong symbols")
        with self.assertRaisesRegex(ValueError, "debug asset checksum mismatch"):
            release.verify_debug_assets(self.manifest, self.artifacts)

    def test_publish_retains_symbols_metadata_and_debug_checksums(self):
        self.debug_candidate()
        calls = []
        def command(*args, **kwargs):
            calls.append(args)
            if args[0] == "git":
                return self.manifest["source_revision"]
            if args[:2] == ("gh", "api"):
                return json.dumps({"tag_name": self.manifest["source_tag"], "assets": []})
            return ""
        with patch.object(release, "authorize_source"), patch.object(release, "run", side_effect=command):
            release.publish(self.manifest, self.artifacts, self.root, debug_assets=True)
        uploaded = {Path(c[4]).name for c in calls if c[:3] == ("gh", "release", "upload")}
        self.assertEqual(uploaded, {*release.ARTIFACTS.values(), "sdk-release.json",
                                    *release.DEBUG_ASSETS.values(), release.DEBUG_MANIFEST})
        self.assertFalse(any("--clobber" in c for c in calls))

    def test_publish_refuses_partial_debug_assets(self):
        (self.artifacts / "sdk-release.json").write_bytes(release.encoded(self.manifest))
        (self.artifacts / release.DEBUG_ASSETS["android_metadata"]).write_text("{}")
        calls = []
        def command(*args, **kwargs):
            calls.append(args)
            if args[0] == "git":
                return self.manifest["source_revision"]
            return json.dumps({"tag_name": self.manifest["source_tag"], "assets": []})
        with patch.object(release, "authorize_source"), patch.object(release, "run", side_effect=command):
            with self.assertRaisesRegex(ValueError, "missing.*debug asset manifest"):
                release.publish(self.manifest, self.artifacts, self.root)
        self.assertFalse(any(c[:3] == ("gh", "release", "upload") for c in calls))


if __name__ == "__main__":
    unittest.main()
