#!/usr/bin/env python3
"""Isolated prost candidate generated from sibling schema; no vendored protos.

The local package has the existing BSR package name/version solely so Cargo's
registry patch can replace that exact dependency in an isolated workspace.
This does not assign a release coordinate to the unpublished schema.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
# Keep hashing and snapshot copying on the same generation-input boundary.
SOURCE_EXCLUSIONS = (".git", "node_modules", "target", "__pycache__")
# Explicit compatibility epoch for the global compact-numbering reset, not a descriptor digest.
WIRE_ID = "compact-wire-reset-v1"
MEDIA_CONTRACT = "singular-thumbnail-compact-wire-reset-v1"
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--schema", type=Path, default=ROOT.parent / "schema")
parser.add_argument("--output", type=Path)
parser.add_argument("--async-actions", action="store_true", required=True,
                    help="opt in to checking an unpublished replacement for the async-actions contract")
parser.add_argument("--room-queries", action="store_true",
                    help="enable typed room query bindings for a candidate that contains both new RPCs")
parser.add_argument("command", nargs=argparse.REMAINDER, help="command executed in the isolated workspace (default: cargo test)")
args = parser.parse_args()
output = (args.output or Path(tempfile.mkdtemp(prefix="libtinkle-schema-candidate-"))).resolve()
if output.is_relative_to(ROOT) or ROOT.is_relative_to(output):
    raise SystemExit("candidate output must be a separate temporary/build directory")
schema = args.schema.resolve(strict=True)
if output.is_relative_to(schema) or schema.is_relative_to(output):
    raise SystemExit("candidate output must not overlap schema sources")
output.mkdir(parents=True, exist_ok=True)
evidence_path = output / "candidate.json"
descriptor = output / "public.binpb"
snapshot = output / "schema-source"
crate = output / "prost-candidate"
workspace = output / "workspace"
# Keep the build cache, but never let a previous run satisfy generation or evidence checks.
for path in (evidence_path, descriptor):
    path.unlink(missing_ok=True)
for path in (snapshot, crate, workspace):
    if path.exists():
        shutil.rmtree(path)

def source_hash(root):
    """Hash generation inputs without traversing directories excluded from the snapshot."""
    digest = hashlib.sha256()
    inputs = []
    for directory, names, files in os.walk(root):
        names[:] = [name for name in names if name not in SOURCE_EXCLUSIONS]
        for name in files:
            path = Path(directory) / name
            if path.is_file() and (path.suffix == ".proto" or name in {"buf.yaml", "buf.lock", "buf.gen.yaml"}):
                inputs.append(path)
    for path in sorted(inputs):
        digest.update(str(path.relative_to(root)).encode() + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()

before = source_hash(schema)
shutil.copytree(schema, snapshot, ignore=shutil.ignore_patterns(*SOURCE_EXCLUSIONS))
if source_hash(snapshot) != before or source_hash(schema) != before:
    raise SystemExit("schema sources changed during snapshot; wait for schema writes to finish")
package_name = "tinkle-org_schema_community_neoeinstein-prost"
pin = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["dependencies"][package_name]["version"].removeprefix("=")
(crate / "src").mkdir(parents=True)
template = output / "buf.gen.yaml"
template.unlink(missing_ok=True)
template.write_text("version: v2\nplugins:\n  - remote: buf.build/community/neoeinstein-prost:v0.5.0\n    revision: 2\n    out: src\n    opt:\n      - bytes=.\n")
subprocess.run(["buf", "build", str(snapshot), "-o", str(descriptor)], check=True)
if not descriptor.is_file() or descriptor.stat().st_size == 0:
    raise SystemExit("candidate Buf build did not produce a non-empty descriptor")
subprocess.run(["buf", "generate", str(snapshot), "--template", str(template), "--output", str(crate)], check=True)
if source_hash(schema) != before:
    raise SystemExit("schema sources changed during generation; wait for schema writes to finish")
generated = crate / "src/tinkle/v1/tinkle.v1.rs"
if not generated.is_file() or generated.stat().st_size == 0:
    raise SystemExit("candidate plugin did not produce a non-empty src/tinkle/v1/tinkle.v1.rs")
shutil.copy2(generated, crate / "src/tinkle.v1.rs")
(crate / "src/public.binpb").write_bytes(descriptor.read_bytes())
(crate / "src/lib.rs").write_text('pub mod tinkle { pub mod v1 { include!("tinkle.v1.rs"); pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("public.binpb"); } }\n')
(crate / "Cargo.toml").write_text(f'[package]\nname = "{package_name}"\nversion = "{pin}"\nedition = "2021"\npublish = false\n[dependencies]\nprost = "=0.14.4"\nprost-types = "=0.14.4"\nbytes = "1"\n')
workspace.mkdir()
for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml"):
    shutil.copy2(ROOT / name, workspace / name)
for name in ("crates", "scripts", ".cargo"):
    shutil.copytree(ROOT / name, workspace / name, dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("target", "__pycache__", ".build", ".gradle", "build", "node_modules"))
config = workspace / ".cargo/config.toml"
with config.open("a") as handle:
    handle.write(f'\n[patch.buf]\n{package_name} = {{ path = {json.dumps(str(crate))} }}\n')
evidence = {"schema_descriptor_sha256": hashlib.sha256(descriptor.read_bytes()).hexdigest(),
             "schema_source_sha256": before, "schema_snapshot": str(snapshot),
            "wire_identity": WIRE_ID, "media_contract": MEDIA_CONTRACT,
             "async_actions": True, "room_queries": args.room_queries,
            "plugin": "buf.build/community/neoeinstein-prost:v0.5.0", "revision": 2,
            "patched_package": package_name, "base_version_only": pin, "workspace": str(workspace)}
env = dict(os.environ, CARGO_TARGET_DIR=str(output / "target"), CARGO_INCREMENTAL="0")
env["TINKLE_SCHEMA_CANDIDATE_WIRE_ID"] = WIRE_ID
env["TINKLE_SCHEMA_CANDIDATE_ASYNC_ACTIONS"] = "1"
env["TINKLE_SCHEMA_CANDIDATE_MEDIA_CONTRACT"] = MEDIA_CONTRACT
env.pop("TINKLE_SCHEMA_CANDIDATE_ROOM_QUERIES", None)
if args.room_queries:
    env["TINKLE_SCHEMA_CANDIDATE_ROOM_QUERIES"] = "1"
subprocess.run(["cargo", "check", "--workspace", "--all-targets", "--keep-going"], cwd=workspace, env=env, check=True)
resolved = json.loads(subprocess.check_output(["cargo", "metadata", "--format-version", "1", "--locked"], cwd=workspace, env=env))
actual = [p for p in resolved["packages"] if p["name"] == package_name]
if len(actual) != 1 or Path(actual[0]["manifest_path"]).resolve() != (crate / "Cargo.toml").resolve():
    raise SystemExit("candidate patch did not replace the BSR prost package")
if any(p["name"] in {"tonic", "tonic-prost", "tinkle-org_schema_community_neoeinstein-tonic"} for p in resolved["packages"]):
    raise SystemExit("native schema candidate unexpectedly depends on tonic")
for name in ("syntra-core", "syntra-runtime", "syntra-net"):
    packages = [p for p in resolved["packages"] if p["name"] == name]
    if len(packages) != 1 or packages[0]["version"] != "1.0.0-rc.5" or packages[0].get("source") != "registry+https://github.com/rust-lang/crates.io-index":
        raise SystemExit("candidate must use the exact published Syntra rc.5 crates")
    if name == "syntra-net":
        node = next(n for n in resolved["resolve"]["nodes"] if n["id"] == packages[0]["id"])
        if node["features"] != ["tcp"]:
            raise SystemExit("candidate must enable syntra-net TCP only")
command = args.command
if command[:1] == ["--"]:
    command = command[1:]
subprocess.run(command or ["cargo", "test", "--locked", "--workspace", "--all-features"], cwd=workspace, env=env, check=True)
evidence_path.write_text(json.dumps(evidence, indent=2) + "\n")
print(f"Unpublished schema candidate evidence: {evidence_path}", flush=True)
