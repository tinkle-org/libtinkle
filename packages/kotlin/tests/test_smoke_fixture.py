from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
ENDPOINT_PATH = '/syntra/v1/ws'


class SmokeFixtureTest(unittest.TestCase):
    def test_smoke_checks_connect_with_protocol_endpoint_path(self):
        # syntra-ws rejects endpoints with any other path.
        smoke = (ROOT / 'sample/src/main/kotlin/com/tinkle/sdk/smoke/SmokeChecks.kt').read_text()
        self.assertIn(f'ws://127.0.0.1:${{gateway.port}}{ENDPOINT_PATH}', smoke)
        self.assertNotIn('${gateway.port}"', smoke)


if __name__ == '__main__':
    unittest.main()
