package no.nav.helse.spoiler

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.asLocalDate
import no.nav.helse.rapids_rivers.asLocalDateTime
import no.nav.helse.rapids_rivers.toUUID
import no.nav.helse.spoiler.OverlappendeInfotrygdperiodeEtterInfotrygdendringDto.Infotrygdperiode
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

data class OverlappendeInfotrygdperiodeEtterInfotrygdendringDto(
    val fødelsnummer: String,
    val aktørId: String,
    val hendelseId: UUID,
    val opprettet: LocalDateTime,
    val vedtaksperiodeId: UUID,
    val vedtaksperiodeFom: LocalDate,
    val vedtaksperiodeTom: LocalDate,
    val vedtaksperiodeTilstand: String,
    val organisasjonsnummer: String,
    val infotrygdhistorikkHendelseId: UUID?,
    val infotrygdperioder: List<Infotrygdperiode>
) {

    data class Infotrygdperiode(
        val vedtaksperiodeId: UUID,
        val fom: LocalDate,
        val tom: LocalDate,
        val type: String,
        val orgnummer: String?
    )
}

fun JsonMessage.toOverlappendeInfotrygdperioderDto(): List<OverlappendeInfotrygdperiodeEtterInfotrygdendringDto> {
    val id = this["@id"].asText().toUUID()
    val opprettet = this["@opprettet"].asLocalDateTime()
    val fødelsnummer = this["fødselsnummer"].asText()
    val aktørId = this["aktørId"].asText()
    val infotrygdHendelseId = this["infotrygdhistorikkHendelseId"].asText().toUUID()
    return this["vedtaksperioder"].map { vedtaksperiode ->
        val vedtaksperiodeId = vedtaksperiode.path("vedtaksperiodeId").asText().toUUID()
        OverlappendeInfotrygdperiodeEtterInfotrygdendringDto(
            hendelseId = id,
            opprettet = opprettet,
            vedtaksperiodeId = vedtaksperiodeId,
            vedtaksperiodeFom = vedtaksperiode.path("vedtaksperiodeFom").asLocalDate(),
            vedtaksperiodeTom = vedtaksperiode.path("vedtaksperiodeTom").asLocalDate(),
            vedtaksperiodeTilstand = vedtaksperiode.path("vedtaksperiodetilstand").asText(),
            fødelsnummer = fødelsnummer,
            aktørId = aktørId,
            organisasjonsnummer = vedtaksperiode.path("organisasjonsnummer").asText(),
            infotrygdhistorikkHendelseId = infotrygdHendelseId,
            infotrygdperioder = vedtaksperiode.path("infotrygdperioder").toInfotrygdperioder(vedtaksperiodeId)
        )
    }
}

fun JsonNode.toInfotrygdperioder(vedtaksperiodeId: UUID) = map { periode ->
    Infotrygdperiode(
        vedtaksperiodeId = vedtaksperiodeId,
        fom = periode["fom"].asLocalDate(),
        tom = periode["tom"].asLocalDate(),
        type = periode["type"].asText(),
        orgnummer = periode["orgnummer"]?.asText()
    )
}
