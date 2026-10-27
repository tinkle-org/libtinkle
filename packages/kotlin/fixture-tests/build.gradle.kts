plugins { kotlin("jvm") version "2.3.0" }

repositories { mavenCentral() }
dependencies { testImplementation("junit:junit:4.13.2") }
kotlin {
    jvmToolchain(17)
    sourceSets.test {
        // Exercise the actual Android fixture, not a second implementation.
        kotlin.srcDir("../sample/src/main/kotlin")
        kotlin.exclude("**/SmokeChecks.kt")
    }
}
