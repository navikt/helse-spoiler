val javaVersion = "17"
val kotlinVersion = "1.8.10"
val ktorVersion = "2.2.4"
val logbackClassicVersion = "1.4.6"
val logbackEncoderVersion = "7.3"

val junitJupiterVersion = "5.9.1"

plugins {
    kotlin("jvm") version "1.8.10"
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.navikt:rapids-and-rivers:2022092314391663936769.9d5d33074875")

    implementation("org.flywaydb:flyway-core:9.7.0")
    implementation("com.zaxxer:HikariCP:5.0.1")
    implementation("org.postgresql:postgresql:42.5.0")
    implementation("com.github.seratch:kotliquery:1.9.0")

    testImplementation("org.testcontainers:postgresql:1.17.5")

    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = javaVersion
    }

    compileTestKotlin {
        kotlinOptions.jvmTarget = javaVersion
    }

    withType<Jar> {
        archiveBaseName.set("app")

        manifest {
            attributes["Main-Class"] = "no.nav.helse.spoiler.AppKt"
            attributes["Class-Path"] = configurations.runtimeClasspath.get().joinToString(separator = " ") {
                it.name
            }
        }

        doLast {
            configurations.runtimeClasspath.get().forEach {
                val file = File("$buildDir/libs/${it.name}")
                if (!file.exists())
                    it.copyTo(file)
            }
        }
    }
}