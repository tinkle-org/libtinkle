// Compile/JVM evidence only. No native libraries, no bypass of the producer's artifact checks.
// Installed native tests belong to sample and consume the verified final release AAR.
plugins {
    id("com.android.library") version "8.13.2"
    id("org.jetbrains.kotlin.android") version "2.3.0"
}

val sdkPackage = layout.projectDirectory.dir("../..")
val core = layout.projectDirectory.dir("../../../..")
val bindings = providers.gradleProperty("bindingSource").orElse(core.dir("bindings/kotlin").asFile.absolutePath)
android {
    namespace = "com.tinkle.sdk.compilefixture"
    compileSdk = 36
    buildToolsVersion = "36.0.0"
    defaultConfig { minSdk = 24 }
    sourceSets["main"].manifest.srcFile(sdkPackage.file("src/main/AndroidManifest.xml"))
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}
kotlin {
    jvmToolchain(17)
    sourceSets["main"].kotlin.apply {
        srcDirs(sdkPackage.dir("src/main/kotlin"), file(bindings.get()), sdkPackage.dir("sample/src/main/kotlin"))
        exclude("**/SmokeChecks.kt", "**/LoopbackGateway.kt")
    }
    sourceSets["test"].kotlin.srcDir(sdkPackage.dir("src/test/kotlin"))
    sourceSets["androidTest"].kotlin.apply {
        srcDir(sdkPackage.dir("sample/src/androidTest/kotlin"))
        include("**/AndroidVideoDecoderTest.kt")
        include("**/AndroidNativeMediaPreparationTest.kt")
        include("**/AndroidNativeSendingTest.kt")
    }
}
dependencies {
    implementation("net.java.dev.jna:jna:5.18.1@aar")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    testImplementation("junit:junit:4.13.2")
    androidTestImplementation("androidx.test.ext:junit:1.3.0")
    androidTestImplementation("androidx.test:runner:1.7.0")
}
