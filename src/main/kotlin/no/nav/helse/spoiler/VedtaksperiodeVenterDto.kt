package no.nav.helse.spoiler

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.asLocalDateTime
import no.nav.helse.rapids_rivers.isMissingOrNull
import java.time.LocalDateTime
import java.util.UUID

data class VedtaksperiodeVenterDto(
    val hendelseId: UUID,
    val fødselsnummer: String,
    val aktørId: String,
    val organisasjonsnummer: String,
    val vedtaksperiodeId: UUID,
    val ventetSiden: LocalDateTime,
    val venterPå: VenterPå
) {
    data class VenterPå (
        internal val hva: String,
        internal val hvorfor: String?
    )
}

fun JsonMessage.toVedtaksperiodeVenterDto() = VedtaksperiodeVenterDto(
    hendelseId = UUID.fromString(this["@id"].asText()),
    fødselsnummer = this["fødselsnummer"].asText(),
    aktørId = this["aktørId"].asText(),
    organisasjonsnummer = this["organisasjonsnummer"].asText(),
    vedtaksperiodeId = UUID.fromString(this["vedtaksperiodeId"].asText()),
    ventetSiden = this["ventetSiden"].asLocalDateTime(),
    venterPå = VedtaksperiodeVenterDto.VenterPå(
        hva = this["venterPå.venteårsak.hva"].asText(),
        hvorfor = this["venterPå.venteårsak.hvorfor"].takeUnless { it.isMissingOrNull() }?.asText()
    )
)
