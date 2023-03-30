package no.nav.helse.spoiler

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.log
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

class OverlappendeInfotrygdperiodeEtterInfotrygdendringRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
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
        val hendelseId = packet["@id"].let { UUID.fromString(it.asText()) }
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val vedtaksperiodeId = packet["vedtaksperiodeId"].let { UUID.fromString(it.asText()) }
        val vedtaksperiodeFom = packet["vedtaksperiodeFom"].asLocalDate()
        val vedtaksperiodeTom = packet["vedtaksperiodeTom"].asLocalDate()
        val vedtaksperiodeTilstand = packet["vedtaksperiodetilstand"].asText()
        val fødelsnummer = packet["fødselsnummer"].asText()
        val aktørId = packet["aktørId"].asText()
        val organisasjonsnummer = packet["organisasjonsnummer"].asText()
        val infotrygdhistorikkHendelseId: UUID? = packet["infotrygdhistorikkHendelseId"].let { UUID.fromString(it.asText()) }

        val infotrygdperioder = packet["infotrygdperioder"]

        log.info("Legger inn data fra overlappende_infotrygdperiode_etter_infotrygdendring i databasen")

        sessionOf(dataSource).use {
            it.transaction { session ->
                @Language("PostgreSQL")
                val statement = """
                     INSERT INTO overlappende_infotrygdperiode_etter_infotrygdendring(id, opprettet, fodselsnummer, aktor_id, organisasjonsnummer, vedtaksperiode_id, vedtaksperiode_fom, vedtaksperiode_tom, vedtaksperiode_tilstand, infotrygdhistorikk_hendelseId)
                     VALUES (:hendelseId, :opprettet, :fodselsnummer, :aktorId, :organisasjonsnummer, :vedtaksperiodeId, :vedtaksperiodeFom, :vedtaksperiodeTom, :vedtaksperiodeTilstand, :infotrygdhistorikkHendelseId)
                     ON CONFLICT DO NOTHING
                """
                session.run(
                    queryOf(
                        statement = statement,
                        paramMap = mapOf(
                            "hendelseId" to hendelseId,
                            "opprettet" to opprettet,
                            "fodselsnummer" to fødelsnummer,
                            "aktorId" to aktørId,
                            "organisasjonsnummer" to organisasjonsnummer,
                            "vedtaksperiodeId" to vedtaksperiodeId,
                            "vedtaksperiodeFom" to vedtaksperiodeFom,
                            "vedtaksperiodeTom" to vedtaksperiodeTom,
                            "vedtaksperiodeTilstand" to vedtaksperiodeTilstand,
                            "infotrygdhistorikkHendelseId" to infotrygdhistorikkHendelseId
                        )
                    ).asExecute
                )

                @Language("PostgreSQL")
                val statement2 = """
                    INSERT INTO overlappende_infotrygd_periode(hendelse_id, fom, tom, type, orgnummer)
                    VALUES ${infotrygdperioder.joinToString { "(?, ?, ?, ?, ?)" }}
                    ON CONFLICT DO NOTHING
                """

                session.run(
                    queryOf(
                        statement = statement2,
                        *infotrygdperioder.flatMap { periode ->
                            listOf(
                                hendelseId,
                                periode.path("fom").asLocalDate(),
                                periode.path("tom").asLocalDate(),
                                periode.path("type").asText(),
                                periode.path("organisasjonsnummer")?.let { orgnummer -> orgnummer.asText() },
                            )
                        }.toTypedArray()
                    ).asExecute
                )
            }
        }
    }
}
