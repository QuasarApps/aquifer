import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinJvm
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.sqldelight)
    alias(libs.plugins.dokka)
    alias(libs.plugins.maven.publish)
}

group = "io.github.quasarapps"
version = "0.1.0-SNAPSHOT"

kotlin {
    explicitApi()
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_11)
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

sqldelight {
    databases {
        create("AquiferDatabase") {
            packageName.set("io.github.quasarapps.aquifer.persistence.sqldelight.db")
        }
    }
}

dependencies {
    api(project(":aquifer-core"))
    api(libs.kotlinx.serialization.json)
    // The SQLDelight plugin already puts the `runtime` artifact on this module's `api` configuration,
    // so the generated database and the SqlDriver/SqlSchema types in the public API resolve for
    // consumers without an explicit declaration. The concrete JVM driver is the caller's choice
    // (the README/KDoc examples have the caller supply their own), so it is test-only here.
    testImplementation(libs.sqldelight.sqlite.driver)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    useJUnitPlatform()
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()
    coordinates("io.github.quasarapps", "aquifer-persistence-sqldelight", version.toString())
    configure(KotlinJvm(javadocJar = JavadocJar.Dokka("dokkaGeneratePublicationHtml"), sourcesJar = true))

    pom {
        name.set("Aquifer Persistence (SQLDelight)")
        description.set("SQLDelight SourceOfTruth for Aquifer: batched, enumerable, queryable persistence.")
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
