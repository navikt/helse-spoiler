package no.nav.helse.spoiler

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDate
import no.nav.helse.rapids_rivers.asLocalDateTime
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
                it.rejectValue("vedtaksperiodetilstand", "AVSLUTTET_UTEN_UTBETALING")
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
    }
}
