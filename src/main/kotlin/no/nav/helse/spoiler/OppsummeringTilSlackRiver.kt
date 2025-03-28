package no.nav.helse.spoiler

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import java.time.Year

internal class OppsummeringTilSlackRiver (
    rapidApplication: RapidsConnection,
    private val overlappendeInfotrygdperiodeEtterInfotrygdendringDao: OverlappendeInfotrygdperiodeEtterInfotrygdendringDao

) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            precondition { it.requireValue("@event_name", "identifiser_overlappende_perioder") }
        }.register(this)

        River(rapidApplication).apply {
            precondition {
                it.requireValue("@event_name", "hel_time")
                it.requireValue("time", 9)
                it.requireValue("dagIUke", 1)
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val oppsummering = overlappendeInfotrygdperiodeEtterInfotrygdendringDao.lagOppsummering()
        if (oppsummering.isEmpty()) return lagHyggeligMelding(context)
        val totaltAntall = oppsummering.sumOf { it.antall }
        val perÅr = oppsummering.groupBy { it.år }
            .entries
            .sortedByDescending { it.key }
            .map { (år, verdier) -> år to verdier.sortedByDescending { it.antall }}
        val melding = "Det er totalt $totaltAntall vedtaksperioder med overlapp mot Infotrygd. :sadkek:\n\n" +
                perÅr.joinToString(separator = "\n\n") { (år, verdier) ->
                    "$år ${emojiForÅr(år)}\n${verdier.joinToString(separator = "\n") { verdi ->
                        "\t${"${verdi.antall} stk".padEnd(10, ' ')} ${verdi.tilstand} ${emojiForTilstand(verdi.tilstand)} pga. ${verdi.overlapptype} ${emojiForType(verdi.overlapptype)}"
                    }}"
                }
        context.publish(lagSlackmelding(melding).toJson())
    }

    private fun emojiForÅr(år: Year): String {
        val nå = Year.now()
        val forskjell = nå.value - år.value
        return when (forskjell) {
            0 -> ":angeryer:"
            1 -> ":pepe-hmm:"
            else -> ":old-man:"
        }
    }

    private fun emojiForTilstand(tilstand: String): String {
        if (tilstand == "AVSLUTTET") return ":pepe_meltdown_q:"
        if (tilstand == "AVVENTER_GODKJENNING_REVURDERING") return ":frog-fist:"
        if (tilstand == "AVVENTER_REVURDERING" || tilstand == "AVVENTER_INNTEKTSMELDING") return ":meow_angry:"
        return ""
    }

    private fun emojiForType(type: OppsummeringDto.Overlapptype): String {
        return when (type) {
            OppsummeringDto.Overlapptype.FERIE -> ":beach_with_umbrella:"
            OppsummeringDto.Overlapptype.UTBETALING -> ":pepe_cash:"
        }
    }

    private fun lagHyggeligMelding(context: MessageContext) {
        context.publish(lagSlackmelding("Det er ingen registrerte vedtaksperioder med overlapp mot Infotrygd :yay-frog:").toJson())
    }


    private fun lagSlackmelding(melding: String) = JsonMessage.newMessage("slackmelding", mapOf(
        "melding" to melding
    ))

    private companion object {
        val logger = LoggerFactory.getLogger(OppsummeringTilSlackRiver::class.java)
    }
}
