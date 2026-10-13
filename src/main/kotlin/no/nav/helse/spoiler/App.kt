package no.nav.helse.spoiler

import com.github.navikt.tbd_libs.personpseudoid.ValkeyPersonPseudoIdProvider
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import no.nav.helse.rapids_rivers.RapidApplication
import org.slf4j.LoggerFactory

internal val log = LoggerFactory.getLogger("no.nav.helse.spoiler")
internal val sikkerlogg = LoggerFactory.getLogger("tjenestekall")

fun main() {
    val env = System.getenv()
    Thread.currentThread().setUncaughtExceptionHandler { _, e ->
        log.error("{}", e.message, e)
    }
    launchApp(env)
}

fun launchApp(env: Map<String, String>) {
    val dataSourceBuilder = DataSourceBuilder(env)
    val dataSource = dataSourceBuilder.getDataSource()
    val overlappendeInfotrygdperiodeEtterInfotrygdendringDao = OverlappendeInfotrygdperiodeEtterInfotrygdendringDao(dataSource)

    val personPseudoIdProvider = ValkeyPersonPseudoIdProvider.fraEnv(instansNavn = "personpseudoid", env = env)

    RapidApplication
        .create(env)
        .apply {
            OverlappendeInfotrygdperioderRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao, personPseudoIdProvider)
            VedtaksperiodeForkastetRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao)
            OppsummeringTilSlackRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao)
        }.apply {
            register(
                object : RapidsConnection.StatusListener {
                    override fun onStartup(rapidsConnection: RapidsConnection) {
                        dataSourceBuilder.migrate()
                    }
                },
            )
        }.start()
}
