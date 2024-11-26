package no.nav.helse.spoiler

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.isMissingOrNull
import com.github.navikt.tbd_libs.rapids_and_rivers.toUUID
import java.time.LocalDateTime
import java.util.*


@JsonIgnoreProperties(ignoreUnknown = true)
data class VedtaksperiodeVenterDto(
    val organisasjonsnummer: String,
    val vedtaksperiodeId: UUID,
    val ventetSiden: LocalDateTime,
    val venterPå: VenterPå
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class VenterPå(
        val vedtaksperiodeId: UUID,
        val venteårsak: Venteårsak
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Venteårsak(
        val hva : String,
        val hvorfor: String?
    )
}

fun JsonMessage.toVedtaksperiodeVenterDto() = VedtaksperiodeVenterDto(
    organisasjonsnummer = this["organisasjonsnummer"].asText(),
    vedtaksperiodeId = this["vedtaksperiodeId"].asText().toUUID(),
    ventetSiden = this["ventetSiden"].asLocalDateTime(),
    venterPå = VedtaksperiodeVenterDto.VenterPå(
        vedtaksperiodeId = this["venterPå.vedtaksperiodeId"].asText().toUUID(),
        venteårsak = VedtaksperiodeVenterDto.Venteårsak(
            hva = this["venterPå.venteårsak.hva"].asText(),
            hvorfor = this["venterPå.venteårsak.hvorfor"].takeUnless { it.isMissingOrNull() }?.asText()
        )
    )
)
