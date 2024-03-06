package no.nav.helse.spoiler

import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import org.slf4j.LoggerFactory

internal val log = LoggerFactory.getLogger("no.nav.helse.spoiler")

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

    RapidApplication.create(env).apply {
        InfotrygdendringRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao)
        OverlappendeInfotrygdperiodeEtterInfotrygdendringRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao)
        VedtaksperiodeVenterRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao)
        VedtaksperiodeForkastetRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao)
        OppsummeringTilSlackRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao)
    }.apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                dataSourceBuilder.migrate()
            }
        })
    }.start()
}
