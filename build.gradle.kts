val junitJupiterVersion = "5.12.1"

val flywayVersion = "11.5.0"
val hikariCPVersion = "6.3.0"
val kotliQueryVersion = "1.9.0"
val rapidsAndRiversVersion = "2025033014191743337188.2f9d6b08d096"
val tbdLibsVersion = "2025.03.30-14.11-a91ce546"
val postgresqlVersion = "42.7.5"

plugins {
    kotlin("jvm") version "2.1.20"
}

repositories {
    val githubPassword: String? by project
    mavenCentral()
    /* ihht. https://github.com/navikt/utvikling/blob/main/docs/teknisk/Konsumere%20biblioteker%20fra%20Github%20Package%20Registry.md
        så plasseres github-maven-repo (med autentisering) før nav-mirror slik at github actions kan anvende førstnevnte.
        Det er fordi nav-mirroret kjører i Google Cloud og da ville man ellers fått unødvendige utgifter til datatrafikk mellom Google Cloud og GitHub
     */
    maven {
        url = uri("https://maven.pkg.github.com/navikt/maven-release")
        credentials {
            username = "x-access-token"
            password = githubPassword
        }
    }
    maven("https://github-package-registry-mirror.gc.nav.no/cached/maven-release")
}

dependencies {
    implementation("com.github.navikt:rapids-and-rivers:$rapidsAndRiversVersion")

    implementation("com.github.navikt.tbd-libs:spurtedu-client:$tbdLibsVersion")

    implementation("org.flywaydb:flyway-database-postgresql:$flywayVersion")
    implementation("com.zaxxer:HikariCP:$hikariCPVersion")
    implementation("org.postgresql:postgresql:$postgresqlVersion")
    implementation("com.github.seratch:kotliquery:$kotliQueryVersion")

    testImplementation("com.github.navikt.tbd-libs:postgres-testdatabaser:$tbdLibsVersion")
    testImplementation("com.github.navikt.tbd-libs:rapids-and-rivers-test:$tbdLibsVersion")

    testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of("21"))
    }
}

tasks {
    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }

        systemProperty("junit.jupiter.execution.parallel.enabled", "true")
        systemProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
        systemProperty("junit.jupiter.execution.parallel.config.strategy", "fixed")
        systemProperty("junit.jupiter.execution.parallel.config.fixed.parallelism", "4")
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
