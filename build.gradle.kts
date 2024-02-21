val javaVersion = "21"
val ktorVersion = "2.3.7"
val logbackClassicVersion = "1.4.14"
val logbackEncoderVersion = "7.4"

val junitJupiterVersion = "5.10.2"

val flywayVersion = "10.4.1"
val hikariCPVersion = "5.1.0"
val postgresqlVersion = "42.7.1"
val kotliQueryVersion = "1.9.0"
val testContainerPostgresqlVersion = "1.19.3"

plugins {
    kotlin("jvm") version "1.9.22"
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.navikt:rapids-and-rivers:2024010209171704183456.6d035b91ffb4")

    implementation("org.flywaydb:flyway-core:$flywayVersion")
    implementation("org.flywaydb:flyway-database-postgresql:$flywayVersion")
    implementation("com.zaxxer:HikariCP:$hikariCPVersion")
    implementation("org.postgresql:postgresql:42.7.2")
    implementation("com.github.seratch:kotliquery:$kotliQueryVersion")

    testImplementation("org.testcontainers:postgresql:$testContainerPostgresqlVersion")

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

    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
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
                val file = File("${layout.buildDirectory.get()}/libs/${it.name}")
                if (!file.exists()) it.copyTo(file)
            }
        }
    }
}