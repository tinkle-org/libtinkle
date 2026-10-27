pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
        maven {
            name = "sdkSmoke"
            url = uri("build/maven")
            content { includeGroup("com.github.tinkle-org") }
        }
        maven {
            url = uri("https://jitpack.io")
            content { includeGroup("com.github.tinkle-org") }
        }
    }
}

rootProject.name = "tinkle-sdk-kotlin"
// Keep the distribution a single-publication root library. Consumer tasks opt in.
if (providers.gradleProperty("includeSample").orNull == "true" ||
    gradle.startParameter.taskNames.any { it.startsWith(":sample:") || it.startsWith("sample:") }) {
    include(":sample")
}
