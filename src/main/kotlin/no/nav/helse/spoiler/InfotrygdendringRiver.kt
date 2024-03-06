package no.nav.helse.spoiler

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

class InfotrygdendringRiver(
    rapidApplication: RapidsConnection,
    private val overlappendeInfotrygdperiodeEtterInfotrygdendringDao: OverlappendeInfotrygdperiodeEtterInfotrygdendringDao
) : River.PacketListener {
    private companion object {
        private val String.maskertFnr get() = take(6).padEnd(11, '*')
    }
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "infotrygdendring")
                it.requireKey("fødselsnummer")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val fnr = packet["fødselsnummer"].asText()
        log.info("sletter person ${fnr.maskertFnr}")
        overlappendeInfotrygdperiodeEtterInfotrygdendringDao.slettPerson(fnr)
    }
}
