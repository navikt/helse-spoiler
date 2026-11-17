plugins {
    alias(libs.plugins.sykepenger.deployable)
}

sykepengerDeployable {
    mainClass = "no.nav.helse.spoiler.AppKt"
}

dependencies {
    implementation(libs.rapidsAndRivers)
    implementation(libs.tbdLibs.personPseudoId)

    implementation(libs.flyway.postgresql)
    implementation(libs.hikariCP)
    implementation(libs.postgresql)
    implementation(libs.kotliquery)

    testImplementation(testFixtures(libs.sykepengerLibs.postgres))
    testImplementation(libs.tbdLibs.rapidsAndRiversTest)
}
