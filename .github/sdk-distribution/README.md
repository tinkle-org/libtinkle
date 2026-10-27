# Downstream SDK workflow source templates

These are inactive, locally reviewable sources for the workflows owned by
`tinkle-org/tinkle-sdk-swift` and `tinkle-org/tinkle-sdk-kotlin`:

| Source here | Manual destination |
| --- | --- |
| `swift/ci.yml` | `tinkle-org/tinkle-sdk-swift/.github/workflows/ci.yml` |
| `swift/release.yml` | `tinkle-org/tinkle-sdk-swift/.github/workflows/release.yml` |
| `kotlin/ci.yml` | `tinkle-org/tinkle-sdk-kotlin/.github/workflows/ci.yml` |
| `kotlin/release.yml` | `tinkle-org/tinkle-sdk-kotlin/.github/workflows/release.yml` |

GitHub does not execute YAML stored here. No installer or cross-repository
workflow write permission is provided. `packages/packages.json` intentionally
excludes `.github`: package snapshots and `sdk-source-files.json` must never
manage these workflows. Install each `ci.yml`/`release.yml` pair together through
its own human-reviewed downstream PR, outside an `sdk-release/<version>`
promotion PR. No remote rulesets or repository settings have been changed by
this local refactor.

## Baseline inspected read-only

The sources were adapted from each downstream `main` workflow, read using
`gh api repos/tinkle-org/<repo>/contents/.github/workflows/<file> -H
'Accept: application/vnd.github.raw+json'`. The inspected blob SHAs were:

| Repository | `ci.yml` blob | `release.yml` blob |
| --- | --- | --- |
| `tinkle-org/tinkle-sdk-swift` | `2c0ca2a2b1aeeb050d92ef1c09bf0f2278a7650a` | `c9ee8a5cfab4a2397b93b044f35927d583532847` |
| `tinkle-org/tinkle-sdk-kotlin` | `104c94a40468ef3cee087799d517a29816dac498` | `f966f6494c78058d58420412abee8e488e519c15` |

Remote workflows have **not** been updated. Re-read them before installation;
these baseline blobs are review context, not an instruction to overwrite newer
downstream work. Current SHA action pins are listed in the
[distribution contract](../workflows/SDK-DISTRIBUTION.md#action-pins).

## Direct CI-to-release dependencies

`ci.yml` owns PR, all main-push, `v*`-push, and explicit bootstrap validation.
The `source` job emits `ready`, immutable `sdk_sha` and `core_sha`, `bootstrap`,
and `release`. Published metadata pins the core SHA and cannot be overridden by
`source_ref`; that input is only for initial bootstrap. Without metadata or an
explicit bootstrap source, consumers are skipped and the run is not release
evidence. On main, `release` detects `sdk-artifact.json` changes against the
commit's first parent (including merge resolutions), not the whole push range.
Every `v*` push requests verification, even with unchanged metadata, and missing
metadata fails rather than skipping the release checks.

The CI `release` job directly needs Swift `source`/`consumer`, or Kotlin
`source`/`consumer` (all three lanes)/`build-logic`. It runs only on an
own-repository main or `v*` push with `ready=true`, `bootstrap=false`, and
`release=true`; failed/skipped dependencies cannot authorize it. Push concurrency
is commit-specific, PR concurrency is ref-specific, and only PRs cancel older
runs. Release concurrency is separately SDK-SHA-specific and never cancels.

`release.yml` accepts only `workflow_call`, with immutable `source_sha` and
`sdk_sha` supplied by CI. Before checking out or executing core helpers, it
checks the supplied SHAs against the SDK checkout/manifest, core repository
identity, protected main, and core-main ancestry. Swift then qualifies the
consumer again under exactly Xcode 27.1 before `authorize`.

`sdk-tag --create` is allowed only on a main push with checkout SHA matching the
event SHA. External `v*` tags are verification-only. Both paths retain the
published-manifest/checksum, full canonical snapshot/binding/inventory, merged
same-repository scoped release-PR, main-ancestry, and exact tag-target guards;
existing tags are never overwritten. The commit-to-PR association is re-fetched
for authoritative `changed_files`, and only promotion-authorized first-parent
removals with `removed` status are permitted. Kotlin's three JitPack runtime lanes
directly need `authorize`: `GITHUB_TOKEN`-created tags do not spawn another push
workflow. The downloaded final AAR and resolved coordinate retain their digest
and transitive-dependency checks.

`scripts/sdk-attestation.py` is deleted. Authorization now follows this job
graph, without a separate historical main-push Actions API poll or wait/retry
policy. In core, reusable `ci.yml` also accepts `source_sha`; authorized native
publication runs the same required Rust checks through `rust-checks` at
`needs.source.outputs.sha`, and `publish` needs those checks, the manifest, and
both consumers. Standard PR/main Rust CI runs separately once per event.

## Manual installation and review

1. Merge the matching core automation/package changes first. The immutable
   `sdk-artifact.json` `source_revision` must contain the full snapshot/tag
   guards and matching packaging/runtime helpers.
   Old release provenance must not be changed merely to load newer helpers.
2. Re-read downstream workflows with the `gh api` commands above and review a
   local diff against these sources. Open separate downstream workflow PRs
   manually when authorized. Do not put `.github` in the package catalog or
   managed inventory, and do not grant the promotion token Workflows write.
   Install each CI/release pair together: CI calls the local release workflow.
   The four files here are not installed by merging a core PR.
3. Review CI trigger coverage: PRs, all main pushes (no path filters), `v*` tag
   pushes, and manual bootstrap dispatch. Consumer checkouts select the exact
   SDK event SHA and core manifest SHA; core checkouts use `fetch-depth: 0`
   because `package_snapshot` also reads
   previous release catalogs/blobs from git history. Checkout credentials are
   disabled. Validation uses `contents: read`; only the guarded release call
   grants `contents: write` and `pull-requests: read`.
4. Swift validation selects **Xcode 26.6** with SHA-pinned, Node 24
   `maxim-lobanov/setup-xcode`, followed by an exact version assertion.
   Swift release qualification and core publication still require **27.1**;
   missing/wrong versions fail, with no fallback to 26.6 or a runner default.
   Provision a suitable runner before enabling releases. Simulator results prove
   the available current runtime, not execution on the iOS 15 minimum target.
5. Kotlin CI retains build-logic tests and the API 24/4 KB, API 36/4 KB, and API
   36/16 KB consumer lanes. Release JitPack proof remains explicitly dependent
   on tag authorization; a token-created tag does not start another push run.
   SHA-pinned, Node 24 `android-actions/setup-android` installs NDK
   **30.0.16248370**, platform **36**, and build-tools **36.0.0**;
   `ANDROID_NDK_HOME` is persisted through `GITHUB_ENV`. These maintained setup
   actions replace the removed `select-xcode` and `android-setup` helper commands.
6. Review the direct dependencies and provenance guards above before enabling
   releases. Review branch/tag protection and token permissions separately.
   Check actual downstream PR, merge-SHA main-push, release qualification, and
   JitPack results.

`packages/packages.json` is the sole package-source catalog;
`scripts/sdk-consumers.json`, `pin-consumers`, and remote migration are removed.
The SDK helper remains for packaging/runtime checks. Promotion intentionally
keeps `open_pr` in `scripts/sdk-release.py`: version branches are immutable, never
force-updated; closed PRs are rejected and merged PRs are historically
reverified. `peter-evans/create-pull-request` was not adopted because its default
force-with-lease branch rewrites do not preserve that policy. Do not turn
promotion into mutable auto-update.

## Evidence and uploaded data

CI results are diagnostic data, never authorization instructions or executable
release inputs. No release job downloads a CI artifact to obtain a ref, shell
script, environment file, or tag decision. Direct successful job dependencies
and provenance/snapshot guards authorize release. Treat report contents and
filenames as untrusted, including fork PR output.

Uploads are allowlisted test results, AARs/digest records, and (on Swift failure)
toolchain/simulator listings, retained for 14 days. Swift explicitly permits
hidden files only to upload the allowlisted `.build/ci-results.xcresult` bundle.
Do not broaden paths to the workspace, home directory, `.git`, environment dumps,
Gradle credentials/caches, device user data, or unrestricted logcat. Use synthetic
test data and inspect reports before manually sharing them; GitHub log masking
does not sanitize artifact contents. Diagnostic collection may tolerate a failed
probe, but tests, checksum verification, and release gates must still fail.
See GitHub's [secure-use reference](https://docs.github.com/en/actions/reference/security/secure-use).

## Local checks

From the libtinkle root (Python standard library only):

```sh
python3 -B -m unittest discover -s tests -p 'test_*.py' -v
# Focused template checks, including installed-layout linting:
python3 -B -m unittest discover -s tests -p 'test_sdk_distribution_templates.py' -v
actionlint
```

Root `actionlint` checks active workflows. When `actionlint` is installed,
`test_sdk_distribution_templates.py` copies each CI/release pair into a temporary
`.github/workflows/` installed layout and lints both files there, so the local
callee resolves. That lint test is skipped if `actionlint` is absent. These
checks do not prove hosted CI/toolchain availability, simulator/device execution,
release authorization, or successful installation.
