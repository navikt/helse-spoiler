plugins {
    alias(libs.plugins.sykepenger.deployable)
}

sykepengerDeployable {
    mainClass = "no.nav.helse.spoiler.AppKt"
}

dependencies {
    implementation(libs.rapidsAndRivers)
    implementation(libs.tbdLibs.spurteduClient)

    implementation(libs.flyway.postgresql)
    implementation(libs.hikariCP)
    implementation(libs.postgresql)
    implementation(libs.kotliquery)

    testImplementation(libs.tbdLibs.postgresTestdatabaser)
    testImplementation(libs.tbdLibs.rapidsAndRiversTest)
}
