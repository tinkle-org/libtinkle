package com.tinkle.build

import groovy.json.JsonOutput
import org.gradle.api.GradleException
import org.gradle.testfixtures.ProjectBuilder
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

import static org.junit.Assert.*

class PrepareSdkArtifactsTest {
    @Rule public TemporaryFolder temp = new TemporaryFolder()

    void archive(File file, Map<String, String> entries) {
        new ZipOutputStream(new FileOutputStream(file)).withCloseable { zip ->
            entries.each { name, data ->
                zip.putNextEntry(new ZipEntry(name))
                zip.write(data.bytes)
                zip.closeEntry()
            }
        }
    }

    PrepareSdkArtifacts fixture() {
        File root = temp.newFolder()
        def project = ProjectBuilder.builder().withProjectDir(root).build()
        File local = new File(root, 'local').tap { mkdirs() }
        File nativeZip = new File(local, 'android-native-libraries.zip')
        archive(nativeZip, SdkArtifacts.ABIS.collectEntries { [("jniLibs/${it}/libtinkle_ffi.so".toString()): "fixture-${it}"] })
        File bindingZip = new File(local, 'kotlin-bindings.zip')
        archive(bindingZip, [(SdkArtifacts.BINDING): 'same generated binding'])
        File binding = new File(root, 'committed.kt').tap { text = 'same generated binding' }
        def task = project.tasks.create('prepareSdkArtifacts', PrepareSdkArtifacts)
        task.localArtifacts.set(local)
        task.androidSha256.set(SdkArtifacts.sha256(nativeZip))
        task.kotlinSha256.set(SdkArtifacts.sha256(bindingZip))
        task.committedBinding.set(binding)
        task.requestedVersion.set('0.0.0-local')
        task.outputDirectory.set(new File(root, 'build/generated/sdk'))
        return task
    }

    @Test void bootstrapExtractsExactlyFourLibrariesAndDoesNotWriteSources() {
        def task = fixture()
        task.prepare()
        File output = task.outputDirectory.get().asFile
        SdkArtifacts.ABIS.each { abi ->
            assertEquals("fixture-${abi}".toString(), new File(output, "jniLibs/${abi}/libtinkle_ffi.so").text)
        }
        assertEquals('same generated binding', task.committedBinding.get().asFile.text)
        assertFalse(new File(output, SdkArtifacts.BINDING).exists())
        assertFalse(new File(task.project.projectDir, 'src').exists())
    }

    @Test void changedCommittedBindingStopsPackaging() {
        def task = fixture()
        task.committedBinding.get().asFile.text = 'mismatched binding'
        try {
            task.prepare()
            fail('Binding mismatch should fail')
        } catch (GradleException error) {
            assertTrue(error.message, error.message.contains('differs from the pinned'))
        }
        assertFalse(new File(task.outputDirectory.get().asFile, 'jniLibs').exists())
    }

    @Test void realVersionCannotBePublishedWithoutManifest() {
        def task = fixture()
        task.requestedVersion.set('1.2.3')
        try {
            task.prepare()
            fail('Real version needs release provenance')
        } catch (GradleException error) {
            assertTrue(error.message, error.message.contains('only publish 0.0.0-local'))
        }
    }

    @Test void partialMatrixCannotLeavePackageableOutputs() {
        def task = fixture()
        File nativeZip = new File(task.localArtifacts.get().asFile, 'android-native-libraries.zip')
        archive(nativeZip, ['jniLibs/x86_64/libtinkle_ffi.so': 'fixture'])
        task.androidSha256.set(SdkArtifacts.sha256(nativeZip))
        try {
            task.prepare()
            fail('Incomplete release ABI matrix should fail')
        } catch (GradleException error) {
            assertTrue(error.message, error.message.contains('missing required files/ABIs'))
        }
        assertFalse(new File(task.outputDirectory.get().asFile, 'jniLibs').exists())
    }

    void pinManifest(PrepareSdkArtifacts task) {
        Map data = [schema_version: 1, version: '1.2.3', source_revision: 'c' * 40,
            source_repository: 'tinkle-org/libtinkle', source_tag: 'tinkle-ffi-v1.2.3',
            artifacts: SdkArtifacts.FILES.collectEntries { key, name ->
                String hash = key == 'android' ? task.androidSha256.get() :
                    key == 'kotlin_bindings' ? task.kotlinSha256.get() : 'd' * 64
                [(key): [filename: name, sha256: hash,
                    url: "https://github.com/tinkle-org/libtinkle/releases/download/tinkle-ffi-v1.2.3/${name}".toString()]]
            }]
        File manifest = new File(task.project.projectDir, 'sdk-artifact.json')
        manifest.text = JsonOutput.toJson(data)
        task.releaseManifest.set(manifest)
        task.requestedVersion.set('v1.2.3')
    }

    @Test void pinnedReleaseRetainsProvenanceAndAcceptsMatchingVTag() {
        def task = fixture()
        pinManifest(task)
        task.prepare()
        assertEquals(task.releaseManifest.get().asFile.text,
            new File(task.outputDirectory.get().asFile, 'assets/tinkle-sdk/sdk-artifact.json').text)
    }

    @Test void localChecksumCannotOverridePinnedRelease() {
        def task = fixture()
        pinManifest(task)
        task.androidSha256.set('f' * 64)
        try {
            task.prepare()
            fail('Local checksum must not override committed provenance')
        } catch (GradleException error) {
            assertTrue(error.message, error.message.contains('differs from committed manifest'))
        }
    }
}
