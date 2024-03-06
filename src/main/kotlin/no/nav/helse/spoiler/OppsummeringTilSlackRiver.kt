package no.nav.helse.spoiler

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory

internal class OppsummeringTilSlackRiver (
    rapidApplication: RapidsConnection,
    private val overlappendeInfotrygdperiodeEtterInfotrygdendringDao: OverlappendeInfotrygdperiodeEtterInfotrygdendringDao

) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            validate { it.demandValue("@event_name", "identifiser_overlappende_perioder") }
        }.register(this)

        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "hel_time")
                it.demandValue("time", "9")
                it.demandValue("ukedag", "MONDAY")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val oppsummering = overlappendeInfotrygdperiodeEtterInfotrygdendringDao.lagOppsummering()
        if (oppsummering.isEmpty()) return lagHyggeligMelding(context)
        val totaltAntall = oppsummering.sumOf { it.antall }
        val perÅr = oppsummering.groupBy { it.år }
        val melding = "Det er totalt $totaltAntall vedtaksperioder med overlapp mot Infotrygd :sadkek:\n" +
                perÅr.entries.joinToString(separator = "\n\n") { (år, verdier) ->
                    "$år\n${verdier.joinToString(separator = "\n") { verdi ->
                        "\t${"${verdi.antall} stk".padEnd(10, ' ')} ${verdi.tilstand}"
                    }}"
                }
        context.publish(lagSlackmelding(melding).toJson())
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
