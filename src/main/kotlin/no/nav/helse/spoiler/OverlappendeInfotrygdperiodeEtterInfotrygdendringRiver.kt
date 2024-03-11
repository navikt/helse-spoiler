package no.nav.helse.spoiler

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDate
import no.nav.helse.rapids_rivers.asLocalDateTime
import java.time.LocalDate
import java.util.UUID

class OverlappendeInfotrygdperiodeEtterInfotrygdendringRiver(
    rapidApplication: RapidsConnection,
    private val overlappendeInfotrygdperiodeEtterInfotrygdendringDao: OverlappendeInfotrygdperiodeEtterInfotrygdendringDao
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "overlappende_infotrygdperiode_etter_infotrygdendring")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("@id") { id -> UUID.fromString(id.asText()) }
                it.require("vedtaksperiodeId") { id -> UUID.fromString(id.asText()) }
                it.require("vedtaksperiodeFom", JsonNode::asLocalDate)
                it.require("vedtaksperiodeTom", JsonNode::asLocalDate)
                it.requireKey("vedtaksperiodetilstand", "fødselsnummer", "aktørId", "organisasjonsnummer")
                it.interestedIn("infotrygdhistorikkHendelseId") { id -> UUID.fromString(id.asText()) }
                it.requireArray("infotrygdperioder") {
                    require("fom", JsonNode::asLocalDate)
                    require("fom", JsonNode::asLocalDate)
                    requireKey("type")
                    interestedIn("organisasjonsnummer")
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        log.info("Mottok overlappende_infotrygdperiode_etter_infotrygdendring-melding")

        val overlappendeInfotrygdperiodeEtterInfotrygdendring = packet.toOverlappendeInfotrygdperiodeEtterInfotrygdendringDto()
        overlappendeInfotrygdperiodeEtterInfotrygdendringDao.lagre(overlappendeInfotrygdperiodeEtterInfotrygdendring)

        log.info("Lagret data fra overlappende_infotrygdperiode_etter_infotrygdendring i databasen")

        val vedtaksperiodetilstand = packet["vedtaksperiodetilstand"].asText()
        if (!erPeriodeTidligereAvsluttet(vedtaksperiodetilstand)) return log.info("Lager ikke alarm etter overlappende Infotrygdperiode i tilstand $vedtaksperiodetilstand")
        val vedtaksperiodeId = packet["vedtaksperiodeId"].let { UUID.fromString(it.asText()) }
        val periode = packet["vedtaksperiodeFom"].asLocalDate() to packet["vedtaksperiodeTom"].asLocalDate()
        val slackmelding = slackmelding(vedtaksperiodeId, vedtaksperiodetilstand, periode)
        context.publish(lagSlackmelding(slackmelding).toJson())
    }

    private fun erPeriodeTidligereAvsluttet(vedtaksperiodetilstand: String) : Boolean {
        return when(vedtaksperiodetilstand) {
            "AVSLUTTET",
            "AVVENTER_REVURDERING",
            "AVVENTER_HISTORIKK_REVURDERING",
            "AVVENTER_VILKÅRSPRØVING_REVURDERING",
            "AVVENTER_SIMULERING_REVURDERING",
            "AVVENTER_GODKJENNING_REVURDERING" -> true
            else -> false
        }
    }

    private fun slackmelding(
        vedtaksperiodeId: UUID,
        vedtaksperiodetilstand: String,
        periode: Pair<LocalDate, LocalDate>
    ) : String {
        return "Huff! Det er lagt inn en overlappende periode i Infotrygd :sadge: Hva skal vi gjøre med dette? \n\n" +
                "Det gjelder vedtaksperiodeId $vedtaksperiodeId i tilstand $vedtaksperiodetilstand for periode ${periode.first} - ${periode.second}"
    }

    private fun lagSlackmelding(melding: String) : JsonMessage {
        return JsonMessage.newMessage("slackmelding", mapOf("melding" to melding))
    }
}
