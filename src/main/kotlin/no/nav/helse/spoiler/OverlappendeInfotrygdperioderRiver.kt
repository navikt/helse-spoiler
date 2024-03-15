package no.nav.helse.spoiler

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import java.time.LocalDate
import java.util.UUID

class OverlappendeInfotrygdperioderRiver(
    rapidApplication: RapidsConnection,
    private val overlappendeInfotrygdperiodeEtterInfotrygdendringDao: OverlappendeInfotrygdperiodeEtterInfotrygdendringDao
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "overlappende_infotrygdperioder")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("@id") { id -> UUID.fromString(id.asText()) }
                it.requireKey("fødselsnummer", "aktørId")
                it.require("infotrygdhistorikkHendelseId") { id -> UUID.fromString(id.asText()) }
                it.requireArray("vedtaksperioder") {
                    require("vedtaksperiodeId") { id -> UUID.fromString(id.asText()) }
                    require("vedtaksperiodeFom", JsonNode::asLocalDate)
                    require("vedtaksperiodeTom", JsonNode::asLocalDate)
                    requireKey("vedtaksperiodetilstand", "organisasjonsnummer")
                    requireArray("infotrygdperioder") {
                        require("fom", JsonNode::asLocalDate)
                        require("fom", JsonNode::asLocalDate)
                        requireKey("type")
                        interestedIn("organisasjonsnummer")
                    }
                }
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        log.error("Forstod ikke overlappende_infotrygdperioder (se securelogs)")
        sikkerlogg.error("Forstod ikke overlappende_infotrygdperioder: ${problems.toExtendedReport()}")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        log.info("Mottok overlappende_infotrygdperiode_etter_infotrygdendring-melding")

        val overlappendeInfotrygdperiodeEtterInfotrygdendring = packet.toOverlappendeInfotrygdperioderDto()
        val nyeOverlappende = overlappendeInfotrygdperiodeEtterInfotrygdendringDao.lagre(packet["fødselsnummer"].asText(), overlappendeInfotrygdperiodeEtterInfotrygdendring)

        log.info("Lagret ${nyeOverlappende.size} nye perioder fra overlappende_infotrygdperioder i databasen")

        var megaslackmelding = "HUFF! Det er lagt inn overlappende periode(r) i Infotrygd :sadge: Hva skal vi gjøre med dette? \n\n"
        var skalPosteSlackMelding = false

        nyeOverlappende.forEach { nyPeriode ->
            if (!erPeriodeTidligereAvsluttet(nyPeriode.vedtaksperiodeTilstand)) return@forEach log.info("Lager ikke alarm etter overlappende Infotrygdperiode i tilstand ${nyPeriode.vedtaksperiodeTilstand}")
            megaslackmelding += slackmelding(nyPeriode.vedtaksperiodeId, nyPeriode.vedtaksperiodeTilstand, nyPeriode.vedtaksperiodeFom to nyPeriode.vedtaksperiodeTom)
            skalPosteSlackMelding = true
        }
        if (!skalPosteSlackMelding) return
        log.info("Publiserer overlappende Infotrygdperiode til Slack")
        context.publish(lagSlackmelding(megaslackmelding).toJson())
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
        return "Og det gjelder vedtaksperiodeId $vedtaksperiodeId i tilstand $vedtaksperiodetilstand for periode ${periode.first} - ${periode.second} \n\n"
    }

    private fun lagSlackmelding(melding: String) : JsonMessage {
        return JsonMessage.newMessage("slackmelding", mapOf("melding" to melding))
    }
}
