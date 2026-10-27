package com.tinkle.build

import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.*
import org.gradle.work.DisableCachingByDefault

/** Revalidates inputs before compiling or publishing; downloads never write src/. */
@DisableCachingByDefault(because = 'Remote artifact integrity is checked on every invocation')
abstract class PrepareSdkArtifacts extends DefaultTask {
    @InputFile @Optional abstract RegularFileProperty getReleaseManifest()
    @InputFile @Optional abstract RegularFileProperty getCommittedBinding()
    @InputDirectory @Optional abstract DirectoryProperty getLocalArtifacts()
    @Input @Optional abstract Property<String> getAndroidSha256()
    @Input @Optional abstract Property<String> getKotlinSha256()
    @Input abstract Property<String> getRequestedVersion()
    @OutputDirectory abstract DirectoryProperty getOutputDirectory()

    PrepareSdkArtifacts() {
        outputs.upToDateWhen { false }
    }

    @TaskAction void prepare() {
        File manifestFile = releaseManifest.orNull?.asFile
        Map manifest = manifestFile?.isFile() ? SdkArtifacts.manifest(manifestFile) : null
        boolean local = localArtifacts.isPresent()
        SdkArtifacts.require(local || manifest != null, 'No sdk-artifact.json. Bootstrap requires -PsdkArtifactDir, -PsdkAndroidSha256 and -PsdkKotlinBindingsSha256; see README.md.')
        SdkArtifacts.require(System.getenv('JITPACK') != 'true' || (!local && manifest != null), 'JitPack requires the committed release manifest, not local bootstrap inputs')
        if (manifest != null) {
            SdkArtifacts.require(requestedVersion.get() in [manifest.version, "v${manifest.version}".toString()], 'Publication tag/version does not match sdk-artifact.json')
            if (androidSha256.isPresent()) SdkArtifacts.require(androidSha256.get() == manifest.artifacts.android.sha256, 'Local Android checksum differs from committed manifest')
            if (kotlinSha256.isPresent()) SdkArtifacts.require(kotlinSha256.get() == manifest.artifacts.kotlin_bindings.sha256, 'Local Kotlin checksum differs from committed manifest')
        } else {
            SdkArtifacts.require(requestedVersion.get() == '0.0.0-local', 'Manifest-free bootstrap may only publish 0.0.0-local')
        }
        SdkArtifacts.require(committedBinding.isPresent(), 'Missing src/main/kotlin/com/tinkle/sdk/tinkle_ffi.kt; stage the same-release generated binding before building')
        File output = outputDirectory.get().asFile
        project.delete(output)
        output.mkdirs()
        File stage = new File(temporaryDir, 'verified')
        project.delete(stage)
        stage.mkdirs()
        ['android', 'kotlin_bindings'].each { key ->
            String filename = SdkArtifacts.FILES[key]
            String expected = manifest != null ? manifest.artifacts[key].sha256 :
                (key == 'android' ? androidSha256.orNull : kotlinSha256.orNull)
            SdkArtifacts.checksum(expected)
            File archive = new File(stage, filename)
            if (local) {
                File source = new File(localArtifacts.get().asFile, filename)
                SdkArtifacts.verify(source, expected)
                java.nio.file.Files.copy(source.toPath(), archive.toPath())
            } else {
                SdkArtifacts.download(manifest.artifacts[key].url, archive)
            }
            SdkArtifacts.verify(archive, expected)
            Set<String> paths = key == 'android' ? SdkArtifacts.ABIS.collect { "jniLibs/${it}/libtinkle_ffi.so".toString() }.toSet() : [SdkArtifacts.BINDING].toSet()
            SdkArtifacts.extract(archive, stage, paths)
        }
        File generated = new File(stage, SdkArtifacts.BINDING)
        SdkArtifacts.require(SdkArtifacts.sha256(generated) == SdkArtifacts.sha256(committedBinding.get().asFile),
            'Committed Kotlin binding differs from the pinned kotlin-bindings.zip. Stage manifest and generated Kotlin together.')
        project.copy { from new File(stage, 'jniLibs'); into new File(output, 'jniLibs') }
        if (manifestFile?.isFile()) project.copy { from manifestFile; into new File(output, 'assets/tinkle-sdk') }
        logger.lifecycle('Verified all four Android ABIs and same-release Kotlin bindings')
    }
}
