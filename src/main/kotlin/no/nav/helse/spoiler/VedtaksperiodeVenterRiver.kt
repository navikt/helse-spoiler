package no.nav.helse.spoiler

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory

internal class VedtaksperiodeVenterRiver (
    rapidApplication: RapidsConnection,
    private val overlappendeInfotrygdperiodeEtterInfotrygdendringDao: OverlappendeInfotrygdperiodeEtterInfotrygdendringDao

) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            validate { it.demandValue("@event_name", "vedtaksperiode_venter") }
            validate { it.rejectValue("@forårsaket_av.event_name", "anmodning_om_forkasting") }
            validate { it.requireKey("venterPå.venteårsak.hva") }
            validate { it.interestedIn("venterPå.venteårsak.hvorfor") }
            validate { it.requireKey(
                "@id",
                "fødselsnummer",
                "aktørId",
                "organisasjonsnummer",
                "vedtaksperiodeId",
                "ventetSiden"
            ) }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val vedtaksperiodeVenter = packet.toVedtaksperiodeVenterDto()
        val venterPåInntektsmelding = vedtaksperiodeVenter.venterPå.hva == "INNTEKTSMELDING"
        if (!venterPåInntektsmelding) return

        val overlappendeInfortrygdperioder = overlappendeInfotrygdperiodeEtterInfotrygdendringDao.finn(vedtaksperiodeVenter.vedtaksperiodeId)
        if(overlappendeInfortrygdperioder.isEmpty()) return

        logger.info("Oppdaget en overlappende infotrydperiode hos en vedtaksperiode som venter på inntektsmelding, anmoder om å forkaste")

        context.publish(JsonMessage.newMessage("anmodning_om_forkasting", mapOf(
            "fødselsnummer" to vedtaksperiodeVenter.fødselsnummer,
            "aktørId" to vedtaksperiodeVenter.aktørId,
            "organisasjonsnummer" to vedtaksperiodeVenter.organisasjonsnummer,
            "vedtaksperiodeId" to vedtaksperiodeVenter.vedtaksperiodeId,
            "vedtaksperiodeVenter" to mapOf(
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
