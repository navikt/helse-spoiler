package no.nav.helse.spoiler

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.*
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
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
                        requireKey("organisasjonsnummer")
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
        slackmelding += "Her finner du spannerlinken til vedkommende: ${fødselsnummer.spannerUrl?.let { "($it)" }} :hats-off:"
        log.info("Publiserer overlappende Infotrygdperiode til Slack")
        context.publish(lagSlackmelding(slackmelding).toJson())
    }

    companion object {
        private const val SLACKKANAL_OVERLAPPENDE_UTBETALINGER = "C072AFA7DAN"
        private val spurteDuClient = SpurteDuClient()
        private val String.spannerUrl get() = spurteDuClient.utveksleUrl("https://spanner.intern.nav.no/person/${this}")?.let { url ->
            "<$url|spannerlink>"
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

class SpurteDuClient(private val host: String) {
    val objectMapper: ObjectMapper = jacksonObjectMapper()
        .registerModule(JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    constructor() : this(when (System.getenv("NAIS_CLUSTER_NAME")) {
        "prod-gcp" -> "https://spurte-du.ansatt.nav.no"
        else -> "https://spurte-du.intern.dev.nav.no"
    })
    private companion object {
        private const val tbdgruppeProd = "c0227409-2085-4eb2-b487-c4ba270986a3"
    }

    fun utveksleUrl(url: String) = utveksleSpurteDu(objectMapper, mapOf(
        "url" to url,
        "påkrevdTilgang" to tbdgruppeProd
    ))?.let { path ->
        host + path
    }
    private fun utveksleSpurteDu(objectMapper: ObjectMapper, data: Map<String, String>): String? {
        val httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build()

        val jsonInputString = objectMapper.writeValueAsString(data)

        val request = HttpRequest.newBuilder()
            .uri(URI("http://spurtedu/skjul_meg"))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonInputString))
            .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

        return try {
            objectMapper.readTree(response.body()).path("path").asText()
        } catch (err: JsonParseException) {
            null
        }
    }
}
