"""Source contracts for the Android host-fixture adapter; no native artifacts needed."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]


class SmokeFixtureTest(unittest.TestCase):
    """Keep the sample's trust, TCP, and JSON control protocol wiring intact."""

    def test_smoke_checks_use_native_trust_and_tcp_ports(self):
        """Require native trust and TCP rather than the removed gRPC/WebSocket path."""
        smoke = (ROOT / 'sample/src/main/kotlin/com/tinkle/sdk/smoke/SmokeChecks.kt').read_text()
        self.assertIn('gateway.host, gateway.port.toUShort(), null', smoke)
        self.assertIn('gateway.trustSource()', smoke)
        self.assertNotIn('grpcEndpoint', smoke)
        self.assertNotIn('ws://', smoke)

    def test_instrumentation_calls_only_kept_target_apk_entry_points(self):
        """R8 must see every SDK/coroutine call, not just the test APK's compile classpath."""
        directory = ROOT / 'sample/src/androidTest/kotlin/com/tinkle/sdk/smoke'
        for source in directory.glob('*.kt'):
            text = source.read_text()
            self.assertNotIn('import kotlinx.coroutines', text, source.name)
            self.assertNotRegex(text, r'import com\.tinkle\.sdk\.(?!smoke\b)', source.name)
            self.assertNotIn('tinkleFfiInit()', text, source.name)
        rules = (ROOT / 'sample/smoke-rules.pro').read_text()
        self.assertIn('-keep class com.tinkle.sdk.smoke.MediaSmokeChecks { public static *; }', rules)

    def test_video_limits_and_callbacks_use_current_native_contract(self):
        """Avoid Kotlin 2.3's inferred-width product cast and the obsolete decoder send hook."""
        smoke = (ROOT / 'sample/src/main/kotlin/com/tinkle/sdk/smoke/VideoSmokeChecks.kt').read_text()
        self.assertIn('FfiVideoDecodeLimits(1024uL * 1024uL,', smoke)
        self.assertNotIn('configureVideoDecoder(object', smoke)
        self.assertEqual(smoke.count('configureNativeMediaPreparer(object : FfiPlatformMediaPreparer'), 2)
        self.assertIn('result.set(AndroidMediaPreparer().prepare(input))', smoke)

    def test_gateway_json_control_requests(self):
        """Pin create, request-log, and close messages and newline-delimited JSON I/O."""
        gateway = (ROOT / 'sample/src/main/kotlin/com/tinkle/sdk/smoke/LoopbackGateway.kt').read_text()
        self.assertRegex(gateway,
                         r'control\s*\(\s*JSONObject\s*\(\s*\)\s*\.\s*put\s*\(\s*"op"\s*,\s*"create"\s*\)'
                         r'\s*\.\s*put\s*\(\s*"deletions"\s*,\s*JSONArray\s*\(\s*deletionTargets\s*\)\s*\)\s*\)')
        self.assertEqual(len(re.findall(
            r'control\s*\(\s*JSONObject\s*\(\s*\)\s*\.\s*put\s*\(\s*"op"\s*,\s*"requests"\s*\)'
            r'\s*\.\s*put\s*\(\s*"id"\s*,\s*id\s*\)\s*\)', gateway)), 2)
        self.assertRegex(gateway,
                         r'override\s+fun\s+close\s*\(\s*\)\s*\{\s*control\s*\(\s*JSONObject\s*\(\s*\)'
                         r'\s*\.\s*put\s*\(\s*"op"\s*,\s*"close"\s*\)\s*\.\s*put\s*\(\s*"id"\s*,\s*id\s*\)\s*\)\s*\}')
        self.assertRegex(gateway, r'fixture\s*\.\s*getLong\s*\(\s*"id"\s*\)')
        self.assertRegex(gateway, r'java\s*\.\s*net\s*\.\s*InetSocketAddress\s*\(\s*host\s*,\s*48080\s*\)')
        self.assertIn('(request.toString() + "\\n").toByteArray()', gateway)
        self.assertIn('socket.getInputStream().bufferedReader().readLine()', gateway)
        self.assertIn('JSONObject(line)', gateway)
        self.assertIn('getJSONArray("requests")', gateway)
        self.assertIn('"target:${row.getString("target_kind")}:${row.getLong("target_id")}" to row.getLong("after_accept_seq")', gateway)
        self.assertIn('getJSONArray("history")', gateway)


if __name__ == '__main__':
    unittest.main()
