import com.vanniktech.maven.publish.AndroidSingleVariantLibrary
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlin.android)
    alias(libs.plugins.kotlin.compose)
    alias(libs.plugins.dokka)
    alias(libs.plugins.maven.publish)
}

group = "io.github.quasar-apps"
version = "0.1.0-SNAPSHOT"

android {
    namespace = "io.github.quasarapps.aquifer.compose"
    compileSdk = 35

    defaultConfig {
        minSdk = 21
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    buildFeatures {
        compose = true
    }

    testOptions {
        // Compose runtime calls android.os.Trace during composition; let the stub no-op so
        // molecule-driven JVM unit tests can run compositions without Robolectric.
        unitTests.isReturnDefaultValues = true
    }
}

kotlin {
    explicitApi()
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_11)
    }
}

// Unit tests are variant-independent here; running them once (debug) halves test time.
androidComponents {
    beforeVariants(selector().withBuildType("release")) { variant ->
        variant.enableUnitTest = false
    }
}

dependencies {
    api(project(":aquifer-core"))
    // api, not implementation: Compose State and Lifecycle types appear in public signatures.
    api(libs.androidx.compose.runtime)
    api(libs.androidx.lifecycle.runtime.compose)

    testImplementation(libs.junit4)
    testImplementation(libs.kotlin.test.junit)
    testImplementation(libs.molecule.runtime)
    testImplementation(libs.androidx.lifecycle.runtime.testing)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.turbine)
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()
    coordinates("io.github.quasar-apps", "aquifer-compose", version.toString())
    configure(AndroidSingleVariantLibrary("release", sourcesJar = true, publishJavadocJar = true))

    pom {
        name.set("Aquifer Compose")
        description.set("Jetpack Compose integration for Aquifer: lifecycle-aware state collection and preview helpers.")
        url.set("https://github.com/Quasar-Apps/api-library-example")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }
        developers {
            developer {
                id.set("quasar-apps")
                name.set("Quasar Apps")
                url.set("https://github.com/Quasar-Apps")
            }
        }
        scm {
            url.set("https://github.com/Quasar-Apps/api-library-example")
            connection.set("scm:git:git://github.com/Quasar-Apps/api-library-example.git")
            developerConnection.set("scm:git:ssh://git@github.com/Quasar-Apps/api-library-example.git")
        }
    }
}
