package no.nav.helse.spoiler

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.asLocalDate
import no.nav.helse.rapids_rivers.asLocalDateTime
import no.nav.helse.spoiler.OverlappendeInfotrygdperiodeEtterInfotrygdendringDto.Infotrygdperiode
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID

data class OverlappendeInfotrygdperiodeEtterInfotrygdendringDto(
    val hendelseId: UUID,
    val opprettet: LocalDateTime,
    val vedtaksperiodeId: UUID,
    val vedtaksperiodeFom: LocalDate,
    val vedtaksperiodeTom: LocalDate,
    val vedtaksperiodeTilstand: String,
    val fødelsnummer: String,
    val aktørId: String,
    val organisasjonsnummer: String,
    val infotrygdhistorikkHendelseId: UUID?,
    val infotrygdperioder: List<Infotrygdperiode>
) {

    data class Infotrygdperiode(
        val hendelseId: UUID,
        val fom: LocalDate,
        val tom: LocalDate,
        val type: String,
        val orgnummer: String?
    )
}

fun JsonMessage.toOverlappendeInfotrygdperiodeEtterInfotrygdendringDto(): OverlappendeInfotrygdperiodeEtterInfotrygdendringDto =
    OverlappendeInfotrygdperiodeEtterInfotrygdendringDto(
        hendelseId = this["@id"].let { UUID.fromString(it.asText()) },
        opprettet = this["@opprettet"].asLocalDateTime(),
        vedtaksperiodeId = this["vedtaksperiodeId"].let { UUID.fromString(it.asText()) },
        vedtaksperiodeFom = this["vedtaksperiodeFom"].asLocalDate(),
        vedtaksperiodeTom = this["vedtaksperiodeTom"].asLocalDate(),
        vedtaksperiodeTilstand = this["vedtaksperiodetilstand"].asText(),
        fødelsnummer = this["fødselsnummer"].asText(),
        aktørId = this["aktørId"].asText(),
        organisasjonsnummer = this["organisasjonsnummer"].asText(),
        infotrygdhistorikkHendelseId = this["infotrygdhistorikkHendelseId"].let { UUID.fromString(it.asText()) },
        infotrygdperioder = this["infotrygdperioder"].toInfotrygdperioder(this["@id"].let { UUID.fromString(it.asText()) })
    )

fun JsonNode.toInfotrygdperioder(hendelseId: UUID) = map { periode ->
    Infotrygdperiode(
        hendelseId = hendelseId,
        fom = periode["fom"].asLocalDate(),
        tom = periode["tom"].asLocalDate(),
        type = periode["type"].asText(),
        orgnummer = periode["orgnummer"]?.asText()
    )
}
