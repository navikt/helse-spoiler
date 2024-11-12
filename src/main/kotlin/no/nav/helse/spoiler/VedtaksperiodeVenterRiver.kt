package no.nav.helse.spoiler

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory

internal class VedtaksperiodeVenterRiver (
    rapidApplication: RapidsConnection,
    private val overlappendeInfotrygdperiodeEtterInfotrygdendringDao: OverlappendeInfotrygdperiodeEtterInfotrygdendringDao

) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            validate { it.demandValue("@event_name", "vedtaksperiode_venter") }
            validate { it.rejectValue("@forårsaket_av.event_name", "anmodning_om_forkasting") }
            validate { it.requireKey("venterPå.vedtaksperiodeId") }
            validate { it.requireKey("venterPå.venteårsak.hva") }
            validate { it.interestedIn("venterPå.venteårsak.hvorfor") }
            validate { it.requireKey(
                "@id",
                "fødselsnummer",
                "organisasjonsnummer",
                "vedtaksperiodeId",
                "ventetSiden"
            ) }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        logger.info("Håndterer ikke vedtaksperiode_venter pga. problem: se sikker logg")
        sikkerlogg.info("Håndterer ikke vedtaksperiode_venter pga. problem: {}", problems.toExtendedReport())
    }

    private fun skalAnmodes(vedtaksperiodeVenter: VedtaksperiodeVenterDto): Boolean {
        if (vedtaksperiodeVenter.vedtaksperiodeId != vedtaksperiodeVenter.venterPå.vedtaksperiodeId) return false // Anmoder kun de som selv er en propp
        return vedtaksperiodeVenter.venterPå.hva in setOf("INNTEKTSMELDING", "HJELP") // Anmoder kun de som venter på IM, eller er helt stuck
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val vedtaksperiodeVenter = packet.toVedtaksperiodeVenterDto()
        if (!skalAnmodes(vedtaksperiodeVenter)) return

        val overlappendeInfortrygdperioder = overlappendeInfotrygdperiodeEtterInfotrygdendringDao.finn(vedtaksperiodeVenter.vedtaksperiodeId)
        if (overlappendeInfortrygdperioder.isEmpty()) return

        logger.info("Oppdaget en overlappende infotrydperiode hos en vedtaksperiode som venter på ${vedtaksperiodeVenter.venterPå.hva}, anmoder om å forkaste")

        context.publish(JsonMessage.newMessage("anmodning_om_forkasting", mapOf(
            "fødselsnummer" to vedtaksperiodeVenter.fødselsnummer,
            "organisasjonsnummer" to vedtaksperiodeVenter.organisasjonsnummer,
            "vedtaksperiodeId" to vedtaksperiodeVenter.vedtaksperiodeId,
            "vedtaksperiodeVenter" to mapOf(
                "ventetPåHvem" to vedtaksperiodeVenter.venterPå.vedtaksperiodeId,
                "ventetPåHva" to vedtaksperiodeVenter.venterPå.hva,
                "ventetPåHvorfor" to vedtaksperiodeVenter.venterPå.hvorfor,
                "ventetSiden" to vedtaksperiodeVenter.ventetSiden
            ),
            "infotrygdOverlappHendelseId" to overlappendeInfortrygdperioder
        )).toJson())
    }

    private companion object {
        val logger = LoggerFactory.getLogger(VedtaksperiodeVenterRiver::class.java)
    }
}
