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

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val vedtaksperiodeVenter = packet.toVedtaksperiodeVenterDto()

        val overlappendeInfortrygdperioder = overlappendeInfotrygdperiodeEtterInfotrygdendringDao.finn(vedtaksperiodeVenter.vedtaksperiodeId)
        if(overlappendeInfortrygdperioder.isEmpty()) return

        logger.info("Oppdaget en overlappende infotrydperiode hos en vedtaksperiode som er stuck")
    }

    private companion object {
        val logger = LoggerFactory.getLogger(VedtaksperiodeVenterRiver::class.java)
    }
}
