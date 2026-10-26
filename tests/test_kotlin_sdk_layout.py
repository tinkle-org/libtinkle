"""Source-package layout checks that run without Android, Cargo, or staging."""

import hashlib
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]
SDK = ROOT / "packages/kotlin"


class KotlinSdkLayoutTest(unittest.TestCase):
    def test_fresh_package_has_handwritten_build_and_consumer_inputs(self):
        required = (
            ".gitignore", "README.md", "LICENSE", "build.gradle.kts",
            "settings.gradle.kts", "gradle.properties", "gradlew", "gradlew.bat",
            "gradle/wrapper/gradle-wrapper.jar",
            "gradle/wrapper/gradle-wrapper.properties", "consumer-rules.pro",
            "buildSrc/build.gradle", "buildSrc/settings.gradle",
            "buildSrc/src/main/groovy/com/tinkle/build/PrepareSdkArtifacts.groovy",
            "buildSrc/src/main/groovy/com/tinkle/build/SdkArtifacts.groovy",
            "buildSrc/src/test/groovy/com/tinkle/build/PrepareSdkArtifactsTest.groovy",
            "buildSrc/src/test/groovy/com/tinkle/build/SdkArtifactsTest.groovy",
            "src/main/AndroidManifest.xml", "src/main/kotlin/com/tinkle/sdk/TinkleSdk.kt",
            "sample/build.gradle.kts", "sample/smoke-rules.pro",
            "sample/src/main/AndroidManifest.xml",
            "sample/src/main/kotlin/com/tinkle/sdk/smoke/LoopbackGateway.kt",
            "sample/src/main/kotlin/com/tinkle/sdk/smoke/SmokeChecks.kt",
            "sample/src/androidTest/kotlin/com/tinkle/sdk/smoke/NativeSmokeTest.kt",
            "scripts/verify-consumer.sh",
        )
        for name in required:
            with self.subTest(path=name):
                self.assertTrue((SDK / name).is_file(), f"Missing Kotlin source-package input: {name}")

    def test_generated_binding_has_one_canonical_source(self):
        self.assertTrue((ROOT / "bindings/kotlin/com/tinkle/sdk/tinkle_ffi.kt").is_file())
        self.assertFalse((SDK / "src/main/kotlin/com/tinkle/sdk/tinkle_ffi.kt").exists())
        self.assertFalse((SDK / "sdk-artifact.json").exists())
        self.assertFalse((SDK / ".git").exists())
        self.assertFalse((SDK / ".github").exists())

    def test_ignore_rules_preserve_build_logic_and_exclude_staged_outputs(self):
        paths = (
            "buildSrc/src/main/groovy/com/tinkle/build/SdkArtifacts.groovy",
            "buildSrc/src/test/groovy/com/tinkle/build/SdkArtifactsTest.groovy",
            "buildSrc/build/fixture", "sample/build/fixture", "build/fixture",
            "src/main/kotlin/com/tinkle/sdk/tinkle_ffi.kt",
        )
        result = subprocess.run(
            ["git", "check-ignore", "--no-index", *(f"packages/kotlin/{p}" for p in paths)],
            cwd=ROOT, text=True, capture_output=True, check=False,
        )
        self.assertIn(result.returncode, (0, 1), result.stderr)
        ignored = set(result.stdout.splitlines())
        self.assertNotIn(f"packages/kotlin/{paths[0]}", ignored)
        self.assertNotIn(f"packages/kotlin/{paths[1]}", ignored)
        for path in paths[2:]:
            self.assertIn(f"packages/kotlin/{path}", ignored)

    def test_generated_wrapper_matches_official_gradle_8_13(self):
        digest = hashlib.sha256((SDK / "gradle/wrapper/gradle-wrapper.jar").read_bytes()).hexdigest()
        self.assertEqual(digest, "81a82aaea5abcc8ff68b3dfcb58b3c3c429378efd98e7433460610fecd7ae45f")
        properties = (SDK / "gradle/wrapper/gradle-wrapper.properties").read_text()
        self.assertIn("gradle-8.13-bin.zip", properties)
        self.assertIn("distributionSha256Sum=20f1b1176237254a6fc204d8434196fa11a4cfb387567519c61556e8710aed78", properties)

    def test_sample_consumes_final_aar_and_distribution_uses_current_identity(self):
        sample = (SDK / "sample/build.gradle.kts").read_text()
        self.assertIn('gradleProperty("tinkleSdkTestAar")', sample)
        self.assertIn("implementation(files(suppliedAar.get()))", sample)
        self.assertNotIn("implementation(project(", sample)
        for name in (
            "build.gradle.kts", "settings.gradle.kts", "sample/build.gradle.kts",
            "buildSrc/src/main/groovy/com/tinkle/build/SdkArtifacts.groovy", "README.md",
        ):
            with self.subTest(path=name):
                content = (SDK / name).read_text()
                self.assertNotIn("tinklehq", content)
                self.assertIn("tinkle-org", content)


if __name__ == "__main__":
    unittest.main()
