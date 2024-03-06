package no.nav.helse.spoiler

import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

internal class VedtaksperiodeForkastetRiver (
    rapidApplication: RapidsConnection,
    private val overlappendeInfotrygdperiodeEtterInfotrygdendringDao: OverlappendeInfotrygdperiodeEtterInfotrygdendringDao

) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            validate { it.demandValue("@event_name", "vedtaksperiode_forkastet") }
            validate { it.requireKey("vedtaksperiodeId") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val vedtaksperiodeId = packet["vedtaksperiodeId"].asText().toUUID()
        logger.info("sletter $vedtaksperiodeId")
        overlappendeInfotrygdperiodeEtterInfotrygdendringDao.slett(vedtaksperiodeId)
    }

    private companion object {
        val logger = LoggerFactory.getLogger(VedtaksperiodeForkastetRiver::class.java)
    }
}
