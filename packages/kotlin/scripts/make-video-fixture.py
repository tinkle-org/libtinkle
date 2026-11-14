#!/usr/bin/env python3
"""Reproduce the tiny real AVC MP4 fixture as Base64; requires an existing ffmpeg."""

import base64
from pathlib import Path
import subprocess
import tempfile


def main():
    """Keep the first frame asymmetric and different from every subsequent frame."""
    with tempfile.TemporaryDirectory(prefix="tinkle-video-fixture-") as directory:
        output = Path(directory) / "fixture.mp4"
        filters = (
            "drawbox=x=32:y=0:w=32:h=48:color=green:t=fill,"
            "drawbox=x=0:y=0:w=iw:h=ih:color=blue:t=fill:enable='gte(n,1)',"
            "setparams=range=limited:color_primaries=bt709:color_trc=bt709:colorspace=bt709"
        )
        subprocess.run([
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
            "-i", "color=c=red:s=64x48:r=10:d=0.4", "-vf", filters,
            "-c:v", "libx264", "-profile:v", "baseline", "-pix_fmt", "yuv420p",
            "-bf", "0", "-g", "10", "-color_range", "tv", "-colorspace", "bt709",
            "-color_trc", "bt709", "-color_primaries", "bt709", "-movflags", "+faststart+write_colr",
            str(output),
        ], check=True)
        print(base64.b64encode(output.read_bytes()).decode("ascii"))


if __name__ == "__main__":
    main()
