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
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--schema", type=Path, default=ROOT.parent / "schema")
parser.add_argument("--output", type=Path)
parser.add_argument("--bindings", action="store_true", help="regenerate owned Swift/Kotlin bindings from the candidate")
parser.add_argument("command", nargs=argparse.REMAINDER, help="command executed in the isolated workspace (default: cargo test)")
args = parser.parse_args()
output = (args.output or Path(tempfile.mkdtemp(prefix="libtinkle-schema-candidate-"))).resolve()
if output.is_relative_to(ROOT) or ROOT.is_relative_to(output):
    raise SystemExit("candidate output must be a separate temporary/build directory")
output.mkdir(parents=True, exist_ok=True)
schema = args.schema.resolve(strict=True)
package_name = "tinkle-org_schema_community_neoeinstein-prost"
pin = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["dependencies"][package_name]["version"].removeprefix("=")
crate = output / "prost-candidate"
crate.mkdir(exist_ok=True)
(crate / "src").mkdir(exist_ok=True)
template = output / "buf.gen.yaml"
template.write_text("version: v2\nplugins:\n  - remote: buf.build/community/neoeinstein-prost:v0.5.0\n    revision: 2\n    out: src\n    opt:\n      - bytes=.\n")
descriptor = output / "public.binpb"
subprocess.run(["buf", "build", str(schema), "-o", str(descriptor)], check=True)
subprocess.run(["buf", "generate", str(schema), "--template", str(template), "--output", str(crate)], check=True)
generated = crate / "src/tinkle/v1/tinkle.v1.rs"
if not generated.is_file():
    raise SystemExit("candidate plugin did not produce src/tinkle/v1/tinkle.v1.rs")
shutil.copy2(generated, crate / "src/tinkle.v1.rs")
(crate / "src/public.binpb").write_bytes(descriptor.read_bytes())
(crate / "src/lib.rs").write_text('pub mod tinkle { pub mod v1 { include!("tinkle.v1.rs"); pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("public.binpb"); } }\n')
(crate / "Cargo.toml").write_text(f'[package]\nname = "{package_name}"\nversion = "{pin}"\nedition = "2021"\npublish = false\n[dependencies]\nprost = "=0.14.4"\nprost-types = "=0.14.4"\nbytes = "1"\n')
workspace = output / "workspace"
workspace.mkdir(exist_ok=True)
for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "uniffi-bindgen.toml"):
    shutil.copy2(ROOT / name, workspace / name)
for name in ("crates", "scripts", ".cargo", "bindings", "packages"):
    shutil.copytree(ROOT / name, workspace / name, dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("target", "__pycache__", ".build", ".gradle", "build", "node_modules"))
config = workspace / ".cargo/config.toml"
with config.open("a") as handle:
    handle.write(f'\n[patch.buf]\n{package_name} = {{ path = {json.dumps(str(crate))} }}\n')
evidence = {"schema_descriptor_sha256": hashlib.sha256(descriptor.read_bytes()).hexdigest(),
            "plugin": "buf.build/community/neoeinstein-prost:v0.5.0", "revision": 2,
            "patched_package": package_name, "base_version_only": pin, "workspace": str(workspace)}
(output / "candidate.json").write_text(json.dumps(evidence, indent=2) + "\n")
env = dict(os.environ, CARGO_TARGET_DIR=str(output / "target"), CARGO_INCREMENTAL="0")
subprocess.run(["cargo", "check", "--workspace", "--all-targets"], cwd=workspace, env=env, check=True)
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
if args.bindings:
    subprocess.run(["bash", "scripts/generate-bindings.sh", "--write"], cwd=workspace, env=env, check=True)
    subprocess.run(["bash", "scripts/generate-bindings.sh", "--check"], cwd=workspace, env=env, check=True)
    binding_files = ("swift/TinkleSDK.swift", "swift/TinkleSDKFFI.h", "swift/module.modulemap", "kotlin/com/tinkle/sdk/tinkle_ffi.kt")
    for relative in binding_files:
        if not (workspace / "bindings" / relative).is_file():
            raise SystemExit(f"candidate generator did not produce bindings/{relative}")
    for relative in binding_files:
        shutil.copy2(workspace / "bindings" / relative, ROOT / "bindings" / relative)
print(f"Unpublished schema candidate evidence: {output / 'candidate.json'}", flush=True)
