import com.tinkle.build.PrepareSdkArtifacts
import com.tinkle.build.SdkArtifacts

plugins {
    id("com.android.library") version "8.13.0"
    id("com.android.application") version "8.13.0" apply false
    id("org.jetbrains.kotlin.android") version "2.3.0"
    `maven-publish`
}

val manifestFile = layout.projectDirectory.file("sdk-artifact.json")
val pinnedManifest = if (manifestFile.asFile.isFile) SdkArtifacts.manifest(manifestFile.asFile) else null
group = "com.github.tinkle-org"
version = providers.gradleProperty("version").orElse(pinnedManifest?.get("version")?.toString() ?: "0.0.0-local").get()

val prepareSdkArtifacts = tasks.register<PrepareSdkArtifacts>("prepareSdkArtifacts") {
    if (manifestFile.asFile.isFile) this.releaseManifest.set(manifestFile)
    val binding = layout.projectDirectory.file("src/main/kotlin/com/tinkle/sdk/tinkle_ffi.kt")
    if (binding.asFile.isFile) committedBinding.set(binding)
    val localPath = providers.gradleProperty("sdkArtifactDir").orElse(providers.environmentVariable("TINKLE_SDK_ARTIFACT_DIR"))
    if (localPath.isPresent) localArtifacts.set(layout.projectDirectory.dir(localPath.get()))
    androidSha256.set(providers.gradleProperty("sdkAndroidSha256").orElse(providers.environmentVariable("TINKLE_SDK_ANDROID_SHA256")))
    kotlinSha256.set(providers.gradleProperty("sdkKotlinBindingsSha256").orElse(providers.environmentVariable("TINKLE_SDK_KOTLIN_BINDINGS_SHA256")))
    // Core CI verifies the candidate manifest and stages both archives.
    val nativeArchive = providers.environmentVariable("TINKLE_SDK_NATIVE_ARCHIVE")
    if (nativeArchive.isPresent) {
        val archive = file(nativeArchive.get())
        require(archive.name == "android-native-libraries.zip") { "Expected the canonical native archive filename" }
        localArtifacts.set(archive.parentFile)
        androidSha256.set(providers.environmentVariable("TINKLE_SDK_NATIVE_SHA256"))
        val kotlinArchive = archive.resolveSibling("kotlin-bindings.zip")
        if (kotlinArchive.isFile && pinnedManifest == null) {
            kotlinSha256.set(SdkArtifacts.sha256(kotlinArchive))
        }
    }
    requestedVersion.set(project.version.toString())
    outputDirectory.set(layout.buildDirectory.dir("generated/sdk"))
}

tasks.register("verifySdkArtifacts") { dependsOn(prepareSdkArtifacts) }

android {
    namespace = "com.tinkle.sdk"
    compileSdk = 36
    buildToolsVersion = "36.0.0"
    defaultConfig {
        minSdk = 24
        consumerProguardFiles("consumer-rules.pro")
        ndk { abiFilters += SdkArtifacts.ABIS }
    }
    sourceSets["main"].jniLibs.srcDir(prepareSdkArtifacts.map { it.outputDirectory.dir("jniLibs") })
    sourceSets["main"].assets.srcDir(prepareSdkArtifacts.map { it.outputDirectory.dir("assets") })
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    packaging { jniLibs.keepDebugSymbols += "**/libtinkle_ffi.so" }
    publishing { singleVariant("release") { withSourcesJar() } }
}

kotlin { jvmToolchain(17) }

dependencies {
    // JNA's Android AAR carries 16 KB-compatible libjnidispatch for all four ABIs.
    api("net.java.dev.jna:jna:5.18.1@aar") { isTransitive = true }
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
}

tasks.named("preBuild") { dependsOn(prepareSdkArtifacts) }
tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    dependsOn(prepareSdkArtifacts)
}

publishing {
    publications {
        register<MavenPublication>("release") {
            artifactId = "tinkle-sdk-kotlin"
            afterEvaluate { from(components["release"]) }
            pom {
                name.set("Tinkle SDK for Android")
                description.set("Kotlin bindings and prebuilt Android libraries for the shared Tinkle Rust engine")
                url.set("https://github.com/tinkle-org/tinkle-sdk-kotlin")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                        distribution.set("repo")
                    }
                }
                scm {
                    url.set("https://github.com/tinkle-org/tinkle-sdk-kotlin")
                    connection.set("scm:git:https://github.com/tinkle-org/tinkle-sdk-kotlin.git")
                }
            }
        }
    }
    repositories { maven { name = "smoke"; url = layout.buildDirectory.dir("maven").get().asFile.toURI() } }
}
