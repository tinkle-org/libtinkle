from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class SmokeFixtureTest(unittest.TestCase):
    def test_smoke_checks_use_native_trust_and_tcp_ports(self):
        smoke = (ROOT / 'sample/src/main/kotlin/com/tinkle/sdk/smoke/SmokeChecks.kt').read_text()
        self.assertIn('gateway.host, gateway.port.toUShort(), null', smoke)
        self.assertIn('gateway.trustSource()', smoke)
        self.assertNotIn('grpcEndpoint', smoke)
        self.assertNotIn('ws://', smoke)


if __name__ == '__main__':
    unittest.main()
