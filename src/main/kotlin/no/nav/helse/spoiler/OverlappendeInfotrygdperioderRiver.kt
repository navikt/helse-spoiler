package no.nav.helse.spoiler

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.spurtedu.SkjulRequest
import com.github.navikt.tbd_libs.spurtedu.SpurteDuClient
import no.nav.helse.rapids_rivers.*
import java.time.LocalDate
import java.util.*

class OverlappendeInfotrygdperioderRiver(
    rapidApplication: RapidsConnection,
    private val overlappendeInfotrygdperiodeEtterInfotrygdendringDao: OverlappendeInfotrygdperiodeEtterInfotrygdendringDao,
    private val spurteDuClient: SpurteDuClient?
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "overlappende_infotrygdperioder")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("@id") { id -> UUID.fromString(id.asText()) }
                it.requireKey("fødselsnummer")
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
        val fødselsnummer = packet["fødselsnummer"].asText()
        val nyeOverlappende = overlappendeInfotrygdperiodeEtterInfotrygdendringDao.lagre(fødselsnummer, overlappendeInfotrygdperiodeEtterInfotrygdendring)

        log.info("Lagret ${nyeOverlappende.size} nye perioder fra overlappende_infotrygdperioder i databasen")

        // lager slackmelding om overlapp
        val overskrift = "HUFF! Det er lagt inn overlappende periode(r) i Infotrygd :sadge: Hva skal vi gjøre med dette? \n\n"
        var slackmelding = overskrift
        val overlappendeSomSkalISlackMelding = mutableListOf<OverlappendeInfotrygdperiodeEtterInfotrygdendringDto>()

        nyeOverlappende.forEach { nyPeriode ->
            if (!erPeriodeTidligereAvsluttet(nyPeriode.vedtaksperiodeTilstand)) return@forEach log.info("Lager ikke alarm etter overlappende Infotrygdperiode i tilstand ${nyPeriode.vedtaksperiodeTilstand}")
            val førsteVedtaksperiode = overlappendeSomSkalISlackMelding.isEmpty()
            slackmelding += slackmelding(nyPeriode.vedtaksperiodeId, nyPeriode.vedtaksperiodeTilstand, nyPeriode.vedtaksperiodeFom to nyPeriode.vedtaksperiodeTom, førsteVedtaksperiode)
            overlappendeSomSkalISlackMelding.add(nyPeriode)
        }
        if (overlappendeSomSkalISlackMelding.isEmpty()) return
        if (overlappendeSomSkalISlackMelding.size > 3){
            slackmelding = overskrift + "Og det gjelder mer enn tre vedtaksperioder i perioden ${overlappendeSomSkalISlackMelding.minOf { it.vedtaksperiodeFom }} - ${overlappendeSomSkalISlackMelding.maxOf { it.vedtaksperiodeTom }} \n\n"
        }
        slackmelding += "Her finner du spannerlinken til vedkommende: ${fødselsnummer.spannerUrl(spurteDuClient)?.let { "($it)" }} :hats-off:"
        log.info("Publiserer overlappende Infotrygdperiode til Slack")
        context.publish(lagSlackmelding(slackmelding).toJson())
    }

    companion object {
        private const val SLACKKANAL_OVERLAPPENDE_UTBETALINGER = "C072AFA7DAN"
        private fun String.spannerUrl(spurteDuClient: SpurteDuClient?) = spurteDuClient?.let {
            spannerlink(spurteDuClient, this).let { url ->
                "<$url|spannerlink>"
            }
        }
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
        periode: Pair<LocalDate, LocalDate>,
        first: Boolean
    ) : String {
        val baseText = "vedtaksperiodeId $vedtaksperiodeId i tilstand $vedtaksperiodetilstand for periode ${periode.first} - ${periode.second} \n\n"
        if (first) return "Og det gjelder $baseText"
        return "I tillegg til $baseText"
    }

    private fun lagSlackmelding(melding: String) : JsonMessage {
        return JsonMessage.newMessage("slackmelding", mapOf(
            "channel" to SLACKKANAL_OVERLAPPENDE_UTBETALINGER,
            "melding" to melding
        ))
    }
}
private val objectMapper: ObjectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())
private const val tbdgruppeProd = "c0227409-2085-4eb2-b487-c4ba270986a3"

fun spannerlink(spurteDuClient: SpurteDuClient, fnr: String): String {
    val payload = SkjulRequest.SkjulTekstRequest(
        tekst = objectMapper.writeValueAsString(mapOf(
            "ident" to fnr,
            "identtype" to "FNR"
        )),
        påkrevdTilgang = tbdgruppeProd
    )

    val spurteDuLink = spurteDuClient.skjul(payload)
    return "https://spanner.ansatt.nav.no/person/${spurteDuLink.id}"
}
