import com.vanniktech.maven.publish.AndroidSingleVariantLibrary
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlin.android)
    alias(libs.plugins.dokka)
    alias(libs.plugins.maven.publish)
}

group = "io.github.quasarapps"
version = "0.1.0-SNAPSHOT"

android {
    namespace = "io.github.quasarapps.aquifer.android"
    compileSdk = 35

    defaultConfig {
        minSdk = 21
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
}

// Unit tests are variant-independent here; running them once (debug) halves Robolectric time.
androidComponents {
    beforeVariants(selector().withBuildType("release")) { variant ->
        variant.enableUnitTest = false
    }
}

kotlin {
    explicitApi()
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_11)
    }
}

dependencies {
    api(project(":aquifer-core"))
    // api, not implementation: Lifecycle appears in public signatures (appForegroundedFlow).
    api(libs.androidx.lifecycle.process)

    testImplementation(libs.junit4)
    testImplementation(libs.kotlin.test.junit)
    testImplementation(libs.robolectric)
    testImplementation(libs.androidx.test.core)
    testImplementation(libs.androidx.lifecycle.runtime.testing)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.turbine)
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()
    coordinates("io.github.quasarapps", "aquifer-android", version.toString())
    configure(AndroidSingleVariantLibrary("release", sourcesJar = true, publishJavadocJar = true))

    pom {
        name.set("Aquifer Android")
        description.set("Android integrations for Aquifer: connectivity- and lifecycle-driven revalidation triggers.")
        url.set("https://github.com/QuasarApps/aquifer")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }
        developers {
            developer {
                id.set("quasarapps")
                name.set("Quasar Apps")
                url.set("https://github.com/QuasarApps")
            }
        }
        scm {
            url.set("https://github.com/QuasarApps/aquifer")
            connection.set("scm:git:git://github.com/QuasarApps/aquifer.git")
            developerConnection.set("scm:git:ssh://git@github.com/QuasarApps/aquifer.git")
        }
    }
}
