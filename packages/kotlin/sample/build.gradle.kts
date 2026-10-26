plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.tinkle.sdk.smoke"
    compileSdk = 36
    buildToolsVersion = "36.0.0"
    defaultConfig {
        applicationId = "com.tinkle.sdk.smoke"
        minSdk = 24
        targetSdk = 36
        versionCode = 1
        versionName = "1.0"
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        ndk { abiFilters += listOf("arm64-v8a", "armeabi-v7a", "x86_64", "x86") }
    }
    buildTypes {
        debug {
            // connectedDebugAndroidTest still exercises R8 + the release AAR.
            isDebuggable = false
            isMinifyEnabled = true
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "smoke-rules.pro")
        }
        release {
            isMinifyEnabled = true
            signingConfig = signingConfigs.getByName("debug")
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "smoke-rules.pro")
        }
    }
    testBuildType = providers.gradleProperty("smokeBuildType").orElse("debug").get()
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    packaging { jniLibs.useLegacyPackaging = false }
}

kotlin { jvmToolchain(17) }

dependencies {
    val suppliedAar = providers.gradleProperty("tinkleSdkTestAar").orElse(providers.environmentVariable("TINKLE_SDK_TEST_AAR"))
    val publishedCoordinate = providers.gradleProperty("tinkleSdkCoordinate")
    if (suppliedAar.isPresent) {
        implementation(files(suppliedAar.get()))
        // A standalone AAR has no dependency metadata; supply its runtime deps.
        implementation("net.java.dev.jna:jna:5.18.1@aar")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    } else {
        implementation(publishedCoordinate.orElse("com.github.tinkle-org:tinkle-sdk-kotlin:${rootProject.version}").get())
    }
    androidTestImplementation("androidx.test.ext:junit:1.3.0")
    androidTestImplementation("androidx.test:runner:1.7.0")
    // CLASS-retained annotations belong on R8's library classpath, not in the APK.
    androidTestCompileOnly("com.google.errorprone:error_prone_annotations:2.36.0")
}
