plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.android) apply false
    alias(libs.plugins.kotlin.compose) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.android.library) apply false
    alias(libs.plugins.maven.publish) apply false
    alias(libs.plugins.dokka)
    alias(libs.plugins.binary.compatibility.validator)
}

// Aggregated API docs for the published modules: ./gradlew dokkaGenerate
dependencies {
    dokka(project(":aquifer-core"))
    dokka(project(":aquifer-persistence-file"))
    dokka(project(":aquifer-android"))
    dokka(project(":aquifer-compose"))
}

dokka {
    moduleName.set("Aquifer")
}

// Public API surface is locked in api/*.api dumps; apiCheck runs as part of `check`.
// After an intentional API change, regenerate with: ./gradlew apiDump
apiValidation {
    ignoredProjects += listOf("sample")
}
