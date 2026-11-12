"""Host validation of real MP4 fixture bytes; not Android codec or native SDK evidence."""

import base64
import json
from pathlib import Path
import re
import shutil
import subprocess
import unittest


@unittest.skipUnless(shutil.which("ffprobe") and shutil.which("ffmpeg"), "ffmpeg/ffprobe unavailable")
class VideoFixtureTest(unittest.TestCase):
    def test_mp4_fixture_fully_decodes_four_tagged_avc_frames(self):
        source = Path(__file__).resolve().parents[1] / "sample/src/main/kotlin/com/tinkle/sdk/smoke/VideoSmokeChecks.kt"
        encoded = re.search(r'private const val MP4 = "([A-Za-z0-9+/=]+)"', source.read_text())
        self.assertIsNotNone(encoded)
        media = base64.b64decode(encoded[1], validate=True)
        probe = subprocess.run([
            "ffprobe", "-v", "error", "-count_frames", "-show_streams", "-show_format", "-of", "json", "pipe:0",
        ], input=media, capture_output=True, check=True)
        info = json.loads(probe.stdout)
        video, = info["streams"]
        self.assertEqual(video["codec_name"], "h264")
        self.assertEqual(video["pix_fmt"], "yuv420p")
        self.assertEqual((video["width"], video["height"]), (64, 48))
        self.assertEqual(video["nb_read_frames"], "4")
        self.assertEqual(video["r_frame_rate"], "10/1")
        for key in ("color_space", "color_transfer", "color_primaries"):
            self.assertEqual(video.get(key), "bt709", video)
        self.assertEqual(video["color_range"], "tv")
        self.assertAlmostEqual(float(info["format"]["duration"]), 0.4, places=5)
        decoded = subprocess.run([
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-xerror", "-i", "pipe:0",
            "-f", "rawvideo", "-pix_fmt", "rgba", "pipe:1",
        ], input=media, capture_output=True, check=True)
        self.assertEqual(len(decoded.stdout), 4 * 64 * 48 * 4)
        pixels = decoded.stdout
        self.assertGreater(pixels[0], 200)  # First frame, left: red.
        right = (64 - 1) * 4
        self.assertGreater(pixels[right + 1], pixels[right])  # First frame, right: green.
        next_frame = 64 * 48 * 4
        self.assertGreater(pixels[next_frame + 2], 200)  # Every later frame: blue.


if __name__ == "__main__":
    unittest.main()
