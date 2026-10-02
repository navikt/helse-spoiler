package no.nav.helse.spoiler

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDate
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.toUUID
import no.nav.helse.spoiler.OverlappendeInfotrygdperiodeEtterInfotrygdendringDto.Infotrygdperiode
import tools.jackson.databind.JsonNode
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

data class OverlappendeInfotrygdperiodeEtterInfotrygdendringDto(
    val fødelsnummer: String,
    val hendelseId: UUID,
    val opprettet: LocalDateTime,
    val vedtaksperiodeId: UUID,
    val vedtaksperiodeFom: LocalDate,
    val vedtaksperiodeTom: LocalDate,
    val vedtaksperiodeTilstand: String,
    val kanForkastes: Boolean,
    val organisasjonsnummer: String,
    val infotrygdhistorikkHendelseId: UUID?,
    val infotrygdperioder: List<Infotrygdperiode>,
) {
    data class Infotrygdperiode(
        val vedtaksperiodeId: UUID,
        val fom: LocalDate,
        val tom: LocalDate,
        val type: String,
        val orgnummer: String?,
    )
}

fun JsonMessage.toOverlappendeInfotrygdperioderDto(): List<OverlappendeInfotrygdperiodeEtterInfotrygdendringDto> {
    val id = this["@id"].asString().toUUID()
    val opprettet = this["@opprettet"].asLocalDateTime()
    val fødelsnummer = this["fødselsnummer"].asString()
    val infotrygdHendelseId = this["infotrygdhistorikkHendelseId"].asString().toUUID()
    return this["vedtaksperioder"].values().map { vedtaksperiode ->
        val vedtaksperiodeId = vedtaksperiode.path("vedtaksperiodeId").asString().toUUID()
        OverlappendeInfotrygdperiodeEtterInfotrygdendringDto(
            hendelseId = id,
            opprettet = opprettet,
            vedtaksperiodeId = vedtaksperiodeId,
            vedtaksperiodeFom = vedtaksperiode.path("vedtaksperiodeFom").asLocalDate(),
            vedtaksperiodeTom = vedtaksperiode.path("vedtaksperiodeTom").asLocalDate(),
            vedtaksperiodeTilstand = vedtaksperiode.path("vedtaksperiodetilstand").asString(),
            kanForkastes = vedtaksperiode.path("kanForkastes").asBoolean(),
            fødelsnummer = fødelsnummer,
            organisasjonsnummer = vedtaksperiode.path("organisasjonsnummer").asString(),
            infotrygdhistorikkHendelseId = infotrygdHendelseId,
            infotrygdperioder = vedtaksperiode.path("infotrygdperioder").toInfotrygdperioder(vedtaksperiodeId),
        )
    }
}

fun JsonNode.toInfotrygdperioder(vedtaksperiodeId: UUID) =
    values().map { periode ->
        Infotrygdperiode(
            vedtaksperiodeId = vedtaksperiodeId,
            fom = periode["fom"].asLocalDate(),
            tom = periode["tom"].asLocalDate(),
            type = periode["type"].asString(),
            orgnummer = periode["orgnummer"]?.asString(),
        )
    }
