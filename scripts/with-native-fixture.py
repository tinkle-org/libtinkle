#!/usr/bin/env python3
"""Run a mobile consumer against a host-only published-SDK TCP fixture.

Builds only a host test executable; packaged release-candidate SDK bytes are
never rebuilt or substituted. The control socket is loopback-only, fixed to
48080 for the iOS simulator and Android emulator host-loopback alias.
"""
import json
import os
from pathlib import Path
import selectors
import subprocess
import sys
import threading

root = Path(__file__).resolve().parents[1]
subprocess.run(["cargo", "build", "--locked", "-p", "libtinkle", "--example", "native-fixture"], cwd=root, check=True)
metadata = json.loads(subprocess.check_output(["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], cwd=root))
binary = Path(metadata["target_directory"]) / "debug/examples/native-fixture"
fixture = subprocess.Popen([str(binary)], cwd=root, stdout=subprocess.PIPE, text=True)
output = fixture.stdout
assert output is not None
reader = None
try:
    with selectors.DefaultSelector() as selector:
        selector.register(output, selectors.EVENT_READ)
        if not selector.select(timeout=10) or output.readline().strip() != "48080":
            raise RuntimeError("encrypted native fixture failed to become ready")
    def forward_output(stream):
        for line in stream:
            print(line, end="", flush=True)

    reader = threading.Thread(target=forward_output, args=(output,), daemon=True)
    reader.start()
    if len(sys.argv) < 2:
        raise RuntimeError("expected consumer command")
    result = subprocess.run(sys.argv[1:], env=os.environ)
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
