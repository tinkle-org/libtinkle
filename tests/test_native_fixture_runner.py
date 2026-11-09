"""Exercise the fixture runner with real pipes and fake host build tools."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class NativeFixtureRunnerTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        (self.root / "scripts").mkdir()
        self.script = self.root / "scripts/with-native-fixture.py"
        shutil.copy2(ROOT / "scripts/with-native-fixture.py", self.script)
        binary = self.root / "host artifacts/native fixture"
        binary.parent.mkdir(parents=True)
        self.binary = binary
        tools = self.root / "bin"
        tools.mkdir()
        self.env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"])
        config = self.root / ".cargo"
        config.mkdir()
        (config / "config.toml").write_text('[build]\ntarget = "wasm32-unknown-unknown"\n')
        self.env["CARGO_BUILD_TARGET"] = "aarch64-linux-android"
        self.artifact = {
            "reason": "compiler-artifact",
            "target": {"name": "native-fixture", "kind": ["example"]},
            "executable": str(self.binary),
        }
        self.cargo_output([self.artifact])

    def cargo_output(self, messages):
        cargo = self.root / "bin/cargo"
        cargo.write_text(
            f"#!{sys.executable}\nimport json, sys\n"
            "assert sys.argv[1:] == ['build', '--locked', '-p', 'libtinkle', '--example', "
            "'native-fixture', '--target', 'host-tuple', '--message-format=json-render-diagnostics']\n"
            "print('non-JSON build output')\n"
            f"for message in {messages!r}: print(json.dumps(message))\n"
        )
        cargo.chmod(0o755)

    def fixture(self, body):
        self.binary.write_text(f"#!{sys.executable}\n" + body)
        self.binary.chmod(0o755)

    def run_consumer(self, body, port=None):
        """Run the wrapper with an optional explicit control-port request."""
        options = [] if port is None else ["--port", str(port)]
        return subprocess.run([sys.executable, str(self.script), *options, sys.executable, "-c", body],
                               cwd=self.root, env=self.env, text=True, capture_output=True, timeout=12)

    def test_dynamic_port_is_allocated_by_fixture_and_forwarded_to_consumer(self):
        """Let the fixture own the socket continuously while advertising its port."""
        self.fixture("import socket, sys\n"
                     "with socket.socket() as server:\n"
                     "    server.bind(('127.0.0.1', int(sys.argv[1])))\n"
                     "    server.listen()\n"
                     "    print(server.getsockname()[1], flush=True)\n"
                     "    connection, _ = server.accept()\n"
                     "    with connection: connection.sendall(b'ready')\n")
        result = self.run_consumer(
            "import os, socket\n"
            "port = int(os.environ['TINKLE_NATIVE_FIXTURE_CONTROL_PORT'])\n"
            "assert 0 < port <= 65535\n"
            "with socket.create_connection(('127.0.0.1', port)) as connection:\n"
            "    with connection.makefile('rb') as response: assert response.read() == b'ready'\n", port=0)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_dynamic_port_rejects_invalid_readiness_before_starting_consumer(self):
        """An allocation request must still receive a valid nonzero TCP port."""
        for port in ("0", "65536", "not-a-port"):
            with self.subTest(port=port):
                self.fixture(f"print({port!r}, flush=True)\n")
                result = self.run_consumer("from pathlib import Path; Path('consumer').touch()", port=0)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("failed to become ready", result.stderr)
                self.assertFalse((self.root / "consumer").exists())

    def test_drains_and_forwards_more_than_a_pipe_buffer_while_consumer_runs(self):
        self.fixture("import signal, sys, time\nfrom pathlib import Path\n"
                     "def stop(*_):\n    print('fixture stopped', flush=True)\n    sys.exit(0)\n"
                     "signal.signal(signal.SIGTERM, stop)\n"
                     "print('48080', flush=True)\n"
                     "for _ in range(1024): print('x' * 1024, flush=True)\n"
                     "Path('drained').touch()\n"
                     "while True: time.sleep(0.01)\n")
        result = self.run_consumer(
            "import sys, time\nfrom pathlib import Path\n"
            "deadline = time.monotonic() + 5\n"
            "while not Path('drained').exists() and time.monotonic() < deadline: time.sleep(0.01)\n"
            "sys.exit(7 if Path('drained').exists() else 99)\n")
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertEqual(result.stdout.count("x" * 1024 + "\n"), 1024)
        self.assertIn("fixture stopped\n", result.stdout)
        self.assertNotIn("48080\n", result.stdout)

    def test_wrong_port_or_eof_prevents_consumer_start(self):
        for body in ("print('48081', flush=True)\n", "pass\n"):
            with self.subTest(body=body):
                self.fixture(body)
                result = self.run_consumer("from pathlib import Path; Path('consumer').touch()")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("failed to become ready", result.stderr)
                self.assertFalse((self.root / "consumer").exists())

    def test_readiness_still_precedes_consumer_and_success_propagates(self):
        self.fixture("import time\nfrom pathlib import Path\n"
                     "time.sleep(0.05)\nPath('ready').touch()\n"
                     "print('48080', flush=True)\n"
                     "while True: time.sleep(0.01)\n")
        result = self.run_consumer("from pathlib import Path; assert Path('ready').exists()")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_launches_reported_host_artifact_not_stale_default_path(self):
        stale = self.root / "target/debug/examples/native-fixture"
        stale.parent.mkdir(parents=True)
        stale.write_text(f"#!{sys.executable}\nraise RuntimeError('stale artifact selected')\n")
        stale.chmod(0o755)
        self.fixture("import time\nprint('48080', flush=True)\nwhile True: time.sleep(0.01)\n")
        unrelated = dict(self.artifact, target={"name": "other", "kind": ["example"]}, executable=str(stale))
        not_example = dict(self.artifact, target={"name": "native-fixture", "kind": ["bin"]}, executable=str(stale))
        self.cargo_output([unrelated, not_example, self.artifact, self.artifact])
        result = self.run_consumer("pass")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_or_ambiguous_artifacts_do_not_start_consumer(self):
        for messages in ([], [self.artifact, dict(self.artifact, executable="another-fixture")]):
            with self.subTest(messages=messages):
                self.cargo_output(messages)
                result = self.run_consumer("from pathlib import Path; Path('consumer').touch()")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("exactly one native-fixture executable", result.stderr)
                self.assertFalse((self.root / "consumer").exists())


if __name__ == "__main__":
    unittest.main()
