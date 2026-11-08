#!/usr/bin/env python3
"""Run a mobile consumer against a host-only published-SDK TCP fixture.

Builds only a host test executable; packaged release-candidate SDK bytes are
never rebuilt or substituted. The loopback control socket defaults to 48080
for mobile consumers. Host tests can request an OS-assigned port with --port 0;
the consumer receives it in TINKLE_NATIVE_FIXTURE_CONTROL_PORT.
"""
import argparse
import json
import os
from pathlib import Path
import selectors
import subprocess
import sys
import threading

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--port", type=int, default=48080, help="control port; 0 lets the fixture allocate one")
parser.add_argument("command", nargs=argparse.REMAINDER)
args = parser.parse_args()
if not 0 <= args.port <= 65535:
    parser.error("control port must be between 0 and 65535")
if not args.command:
    parser.error("expected consumer command")

root = Path(__file__).resolve().parents[1]
subprocess.run(["cargo", "build", "--locked", "-p", "libtinkle", "--example", "native-fixture"], cwd=root, check=True)
metadata = json.loads(subprocess.check_output(["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], cwd=root))
binary = Path(metadata["target_directory"]) / "debug/examples/native-fixture"
fixture = subprocess.Popen([str(binary), str(args.port)], cwd=root, stdout=subprocess.PIPE, text=True)
output = fixture.stdout
assert output is not None
reader = None
try:
    with selectors.DefaultSelector() as selector:
        selector.register(output, selectors.EVENT_READ)
        ready = output.readline().strip() if selector.select(timeout=10) else ""
        if not ready.isascii() or not ready.isdecimal() or not 0 < int(ready) <= 65535:
            raise RuntimeError("encrypted native fixture failed to become ready")
        port = int(ready)
        if args.port != 0 and port != args.port:
            raise RuntimeError("encrypted native fixture failed to become ready")
    def forward_output(stream):
        for line in stream:
            print(line, end="", flush=True)

    reader = threading.Thread(target=forward_output, args=(output,), daemon=True)
    reader.start()
    env = dict(os.environ, TINKLE_NATIVE_FIXTURE_CONTROL_PORT=str(port))
    result = subprocess.run(args.command, env=env)
    sys.exit(result.returncode)
finally:
    fixture.terminate()
    try:
        fixture.wait(timeout=5)
    except subprocess.TimeoutExpired:
        fixture.kill()
        fixture.wait()
    if reader is not None:
        reader.join()
    output.close()
