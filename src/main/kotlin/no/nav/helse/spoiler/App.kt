package no.nav.helse.spoiler

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.azure.AzureToken
import com.github.navikt.tbd_libs.azure.AzureTokenProvider
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.spurtedu.SpurteDuClient
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

    val spurteDuClient = SpurteDuClient(
        objectMapper = jacksonObjectMapper().registerModule(JavaTimeModule()),
        tokenProvider = object : AzureTokenProvider {
            override fun bearerToken(scope: String): com.github.navikt.tbd_libs.result_object.Result<AzureToken> {
                TODO("Not yet implemented")
            }

            override fun onBehalfOfToken(scope: String, token: String): com.github.navikt.tbd_libs.result_object.Result<AzureToken> {
                TODO("Not yet implemented")
            }
        }
    )

    RapidApplication.create(env).apply {
        OverlappendeInfotrygdperioderRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao, spurteDuClient)
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
