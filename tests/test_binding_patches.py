"""Exercise the generator's Python postprocessor without Cargo or network access."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/generate-bindings.sh"
POSTPROCESSOR = SCRIPT.read_text().split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
FILES = {
    "TinkleSDK.swift": "bindings/swift/TinkleSDK.swift",
    "TinkleSDKFFI.h": "bindings/swift/TinkleSDKFFI.h",
    "TinkleSDKFFI.modulemap": "bindings/swift/module.modulemap",
    "com/tinkle/sdk/tinkle_ffi.kt": "bindings/kotlin/com/tinkle/sdk/tinkle_ffi.kt",
}
OLD_LIFT = """public static func lift(_ buf: RustBuffer) throws -> SwiftType {
        var reader = createReader(data: Data(rustBuffer: buf))
        let value = try read(from: &reader)
        if hasRemaining(reader) {
            throw UniffiInternalError.incompleteData
        }
        buf.deallocate()
        return value"""
FIXED_LIFT = """public static func lift(_ buf: RustBuffer) throws -> SwiftType {
        defer { buf.deallocate() }
        var reader = createReader(data: Data(rustBuffer: buf))
        let value = try read(from: &reader)
        if hasRemaining(reader) {
            throw UniffiInternalError.incompleteData
        }
        return value"""
OLD_COUNT = """var count: Int {
        get {
            map.count
        }
    }"""
FIXED_COUNT = """var count: Int {
        get {
            lock.withLock { map.count }
        }
    }"""
FRAGMENTS = (
    ("RustBuffer cleanup", OLD_LIFT, FIXED_LIFT),
    ("handle-map locking", OLD_COUNT, FIXED_COUNT),
)


def swift(lift=FIXED_LIFT, count=FIXED_COUNT):
    return f"{lift}\n    }}\n\n{count}\n"


class BindingPatchTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name) / "repo"
        self.stage = Path(temp.name) / "stage"
        for generated, relative in FILES.items():
            source = self.stage / generated
            destination = self.root / relative
            source.parent.mkdir(parents=True, exist_ok=True)
            destination.parent.mkdir(parents=True, exist_ok=True)
            source.write_text(swift() if generated.endswith(".swift") else f"generated {generated}\n")
            destination.write_text("previous binding\n")

    def generate(self, text, mode):
        (self.stage / "TinkleSDK.swift").write_text(text)
        return subprocess.run(
            [sys.executable, "-c", POSTPROCESSOR, str(self.root), str(self.stage), mode],
            capture_output=True, text=True, check=False,
        )

    def bindings(self):
        return {relative: (self.root / relative).read_bytes() for relative in FILES.values()}

    def install_generated(self):
        for generated, relative in FILES.items():
            (self.root / relative).write_bytes((self.stage / generated).read_bytes())

    def assert_rejected(self, text, mode, patch_name):
        before = self.bindings()
        result = self.generate(text, mode)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("unexpected Swift template", result.stderr)
        self.assertIn(patch_name, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.bindings(), before)

    def test_original_templates_are_patched_and_then_check_passes(self):
        original = swift(OLD_LIFT, OLD_COUNT)
        result = self.generate(original, "--write")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.bindings(), {
            relative: (swift() if generated == "TinkleSDK.swift"
                       else f"generated {generated}\n").encode()
            for generated, relative in FILES.items()
        })
        result = self.generate(original, "--check")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_exact_replacements_are_accepted_in_both_modes(self):
        self.install_generated()
        before = self.bindings()
        for mode in ("--write", "--check"):
            with self.subTest(mode=mode):
                result = self.generate(swift(), mode)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.bindings(), before)
                self.assertNotIn("Updated", result.stdout)

    def test_fragments_are_validated_independently(self):
        for text in (swift(OLD_LIFT, FIXED_COUNT), swift(FIXED_LIFT, OLD_COUNT)):
            for mode in ("--write", "--check"):
                with self.subTest(text=text, mode=mode):
                    (self.stage / "TinkleSDK.swift").write_text(swift())
                    self.install_generated()
                    result = self.generate(text, mode)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual((self.root / FILES["TinkleSDK.swift"]).read_text(), swift())

    def test_unknown_or_missing_fragments_cannot_overwrite_bindings(self):
        for name, old, fixed in FRAGMENTS:
            for fragment in ("", old.replace("        ", "          "),
                             fixed.replace("        ", "          ")):
                with self.subTest(patch=name, fragment=fragment):
                    text = swift(OLD_LIFT, OLD_COUNT).replace(old, fragment)
                    self.assert_rejected(text, "--write", name)

    def test_check_rejects_unknown_templates_even_when_bindings_match(self):
        for name, old, fixed in FRAGMENTS:
            for fragment in ("", old.replace("        ", "          "),
                             fixed.replace("        ", "          ")):
                with self.subTest(patch=name, fragment=fragment):
                    text = swift().replace(fixed, fragment)
                    (self.stage / "TinkleSDK.swift").write_text(text)
                    self.install_generated()
                    self.assert_rejected(text, "--check", name)

    def test_ambiguous_duplicate_or_mixed_fragments_are_rejected(self):
        for name, old, fixed in FRAGMENTS:
            for fragment in (old + "\n" + old, fixed + "\n" + fixed,
                             old + "\n" + fixed):
                for mode in ("--write", "--check"):
                    with self.subTest(patch=name, fragment=fragment, mode=mode):
                        self.assert_rejected(swift().replace(fixed, fragment), mode, name)

    def test_whitespace_normalization_precedes_patch_validation(self):
        text = "\r\n".join(line + "  " for line in swift(OLD_LIFT, OLD_COUNT).splitlines())
        result = self.generate(text, "--write")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root / FILES["TinkleSDK.swift"]).read_text(), swift())


if __name__ == "__main__":
    unittest.main()
