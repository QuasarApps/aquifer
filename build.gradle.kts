import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JavaToolchainService

plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.android) apply false
    alias(libs.plugins.kotlin.compose) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.android.library) apply false
    alias(libs.plugins.maven.publish) apply false
    alias(libs.plugins.detekt) apply false
    alias(libs.plugins.dokka)
    alias(libs.plugins.binary.compatibility.validator)
}

// Static analysis: detekt (with the bundled ktlint formatting rules) on every Kotlin module,
// sharing one config. detekt registers itself into `check`, so `./gradlew build` enforces it.
val detektFormatting = libs.detekt.formatting
subprojects {
    apply(plugin = "io.gitlab.arturbosch.detekt")

    extensions.configure<io.gitlab.arturbosch.detekt.extensions.DetektExtension> {
        buildUponDefaultConfig = true
        config.setFrom(rootProject.layout.projectDirectory.file("config/detekt/detekt.yml"))
    }

    dependencies {
        "detektPlugins"(detektFormatting)
    }
}

// CI-only: run every Test task on a specific JDK launcher so JVM-11 bytecode is proven to
// execute on a real JDK 11 runtime (compilation still targets JVM_11 regardless). Off by
// default — normal builds run tests on the Gradle daemon JDK, unchanged. CI enables it with
// -PtestJvm=11; the JDK is provisioned by setup-java and discovered via
// -Dorg.gradle.java.installations.fromEnv (no foojay, no auto-download).
providers.gradleProperty("testJvm").map(String::toInt).orNull?.let { testJvm ->
    val requested = JavaLanguageVersion.of(testJvm)
    subprojects {
        // Resolve the service per-subproject: java-base (applied by the Kotlin JVM / Android
        // plugins) registers JavaToolchainService on the subproject, not on the root project.
        plugins.withId("java-base") {
            val launcher = extensions.getByType<JavaToolchainService>()
                .launcherFor { languageVersion.set(requested) }
            tasks.withType<Test>().configureEach {
                javaLauncher.set(launcher)
            }
        }
    }
}

// Aggregated API docs for the published modules: ./gradlew dokkaGenerate
dependencies {
    dokka(project(":aquifer-core"))
    dokka(project(":aquifer-test"))
    dokka(project(":aquifer-persistence-file"))
    dokka(project(":aquifer-android"))
    dokka(project(":aquifer-compose"))
    dokka(project(":aquifer-okhttp"))
}

dokka {
    moduleName.set("Aquifer")
}

// Public API surface is locked in api/*.api dumps; apiCheck runs as part of `check`.
// After an intentional API change, regenerate with: ./gradlew apiDump
apiValidation {
    ignoredProjects += listOf("sample")
}
