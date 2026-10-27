package com.tinkle.build

import groovy.json.JsonOutput
import org.gradle.api.GradleException
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

import static org.junit.Assert.*

class SdkArtifactsTest {
    @Rule public TemporaryFolder temp = new TemporaryFolder()

    File zip(Map<String, String> entries) {
        File file = temp.newFile()
        new ZipOutputStream(new FileOutputStream(file)).withCloseable { zip ->
            entries.each { name, content ->
                zip.putNextEntry(new ZipEntry(name))
                zip.write(content.bytes)
                zip.closeEntry()
            }
        }
        return file
    }

    void rejected(Closure action, String message) {
        try {
            action.call()
            fail('Expected rejection')
        } catch (GradleException error) {
            assertTrue(error.message, error.message.contains(message))
        }
    }

    @Test void tamperedArchiveFailsChecksumBeforeExtraction() {
        File archive = zip(['jniLibs/arm64-v8a/libtinkle_ffi.so': 'original'])
        String digest = SdkArtifacts.sha256(archive)
        archive.append('tamper')
        rejected({ SdkArtifacts.verify(archive, digest) }, 'SHA256 mismatch')
    }

    @Test void traversalAbsoluteAndWindowsPathsAreRejected() {
        ['../outside', '/absolute', 'jniLibs/../../source', 'C:/source', 'jniLibs\\outside', 'jniLibs/./source', 'jniLibs//source'].each { name ->
            File destination = temp.newFolder()
            rejected({ SdkArtifacts.extract(zip([(name): 'bad']), destination, [name].toSet()) }, 'Unsafe ZIP entry')
            assertEquals(0, destination.list().length)
        }
    }

    @Test void incompleteAbiMatrixIsRejected() {
        Set<String> expected = SdkArtifacts.ABIS.collect { "jniLibs/${it}/libtinkle_ffi.so".toString() }.toSet()
        rejected({ SdkArtifacts.extract(zip([(expected.first()): 'elf']), temp.newFolder(), expected) }, 'missing required files/ABIs')
    }

    @Test void extraSourceTreeEntryIsRejected() {
        rejected({ SdkArtifacts.extract(zip(['src/main/kotlin/Injected.kt': 'bad']), temp.newFolder(), [SdkArtifacts.BINDING].toSet()) }, 'Unexpected ZIP file')
    }

    @Test void exactBindingArchiveExtracts() {
        File destination = temp.newFolder()
        Set<String> files = [SdkArtifacts.BINDING].toSet()
        assertEquals(files, SdkArtifacts.extract(zip([(SdkArtifacts.BINDING): 'binding']), destination, files))
        assertEquals('binding', new File(destination, SdkArtifacts.BINDING).text)
    }

    Map validManifest() {
        String version = '1.2.3'
        return [schema_version: 1, version: version, source_revision: 'a' * 40,
            source_repository: 'tinkle-org/libtinkle', source_tag: "tinkle-ffi-v${version}".toString(),
            artifacts: SdkArtifacts.FILES.collectEntries { key, name ->
                [(key): [filename: name, url: "https://github.com/tinkle-org/libtinkle/releases/download/tinkle-ffi-v${version}/${name}".toString(), sha256: 'b' * 64]]
            }]
    }

    File manifest(Map data) {
        File file = temp.newFile()
        file.text = JsonOutput.toJson(data)
        return file
    }

    @Test void releaseUrlsMustMatchExactHostRepositoryVersionAndFilename() {
        assertEquals('1.2.3', SdkArtifacts.manifest(manifest(validManifest())).version)
        ['http://github.com/tinkle-org/libtinkle/releases/download/tinkle-ffi-v1.2.3/android-native-libraries.zip',
         'https://github.com.evil.invalid/tinkle-org/libtinkle/releases/download/tinkle-ffi-v1.2.3/android-native-libraries.zip',
         'https://github.com/tinkle-org/libtinkle/releases/download/tinkle-ffi-v1.2.2/android-native-libraries.zip',
         'https://github.com/tinkle-org/another/releases/download/tinkle-ffi-v1.2.3/android-native-libraries.zip'].each { url ->
            Map data = validManifest()
            data.artifacts.android.url = url
            rejected({ SdkArtifacts.manifest(manifest(data)) }, 'canonical HTTPS')
        }
    }

    @Test void insecureDownloadFailsWithoutNetwork() {
        rejected({ SdkArtifacts.download('http://localhost/file', temp.newFile()) }, 'Unsafe SDK download')
    }
}
