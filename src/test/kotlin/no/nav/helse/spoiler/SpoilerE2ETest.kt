package no.nav.helse.spoiler

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SpoilerE2ETest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val overlappendeInfotrygdperiodeEtterInfotrygdendringDao = OverlappendeInfotrygdperiodeEtterInfotrygdendringDao(dataSource)
    private val testRapid = TestRapid().apply {
        OverlappendeInfotrygdperiodeEtterInfotrygdendringRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao)
        VedtaksperiodeVenterRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao)
    }

    @AfterAll
    fun tearDown() {
        testRapid.stop()
        dataSource.connection.close()
        embeddedPostgres.close()
    }

    @AfterEach
    fun reset() {
        dataSource.resetDatabase()
        testRapid.reset()
    }

    @Test
    fun `anmoder om å forkaste vedtaksperiode`() {
        val hendelseId = UUID.randomUUID()
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperiodeEtterInfotrygdEndring(hendelseId, vedtaksperiodeId))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId))
        val utgående = testRapid.inspektør.message(testRapid.inspektør.size-1)

        assertEquals("anmodning_om_forkasting", utgående["@event_name"].asText())
        assertEquals(vedtaksperiodeId, utgående["vedtaksperiodeId"].asText().let { UUID.fromString(it) })
    }

    @Test
    fun `anmoder ikke om å forkaste vedtaksperiode hvis vedtaksperiode_venter er forårsaket av en anmoding`() {
        val hendelseId = UUID.randomUUID()
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperiodeEtterInfotrygdEndring(hendelseId, vedtaksperiodeId))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, forårsaketAv = "anmodning_om_forkasting"))
        assertTrue(testRapid.inspektør.size == 0)
    }

    @Test
    fun `lagrer i databasen`() {
        val hendelseId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperiodeEtterInfotrygdEndring(hendelseId))
        assertEquals(3, tellOverlappendeInfotrygdperioderEtterInfotrygdEndring(hendelseId))
    }

    @Test
    fun `anmoder ikke om å forkaste vedtaksperiode hvis det er ferie som er registrert i Infotrygd`() {
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperiodeEtterInfotrygdEndring(UUID.randomUUID(), vedtaksperiodeId, "FRIPERIODE"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId))
        assertTrue(testRapid.inspektør.size == 0)
    }

    @Test
    fun `anmoder om å forkaste vedtaksperiode hvis det er arbeidsgiverutbetaling som er registrert i Infotrygd`() {
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperiodeEtterInfotrygdEndring(UUID.randomUUID(), vedtaksperiodeId, "ARBEIDSGIVERUTBETALING"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId))
        val utgående = testRapid.inspektør.message(testRapid.inspektør.size-1)
        assertEquals("anmodning_om_forkasting", utgående["@event_name"].asText())
        assertEquals(vedtaksperiodeId, utgående["vedtaksperiodeId"].asText().let { UUID.fromString(it) })
    }

    @Test
    fun `anmoder om å forkaste vedtaksperiode hvis det er personutbetaling som er registrert i Infotrygd`() {
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperiodeEtterInfotrygdEndring(UUID.randomUUID(), vedtaksperiodeId, "PERSONUTBETALING"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId))
        val utgående = testRapid.inspektør.message(testRapid.inspektør.size-1)
        assertEquals("anmodning_om_forkasting", utgående["@event_name"].asText())
        assertEquals(vedtaksperiodeId, utgående["vedtaksperiodeId"].asText().let { UUID.fromString(it) })
    }

    private fun tellOverlappendeInfotrygdperioderEtterInfotrygdEndring(hendelseId: UUID): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM overlappende_infotrygd_periode WHERE hendelse_id = :hendelse_id"
            requireNotNull(
                session.run(queryOf(query, mapOf("hendelse_id" to hendelseId)).map { row -> row.int(1) }.asSingle)
            )
        }
    }


    @Language("JSON")
    fun overlappendeInfotrygdperiodeEtterInfotrygdEndring(
        hendelseId: UUID,
        vedtaksperiodeId: UUID = UUID.randomUUID()
    ) =
        """
            {
              "@id": "$hendelseId",
              "@event_name": "overlappende_infotrygdperiode_etter_infotrygdendring",
              "organisasjonsnummer": "123",
              "vedtaksperiodeId": "$vedtaksperiodeId",
              "vedtaksperiodeFom": "2023-02-01",
              "vedtaksperiodeTom": "2023-02-12",
              "vedtaksperiodetilstand": "AVVENTER_BLOKKERENDE_PERIODE",
              "infotrygdhistorikkHendelseId": "7ff91df2-fcbe-4657-9171-709917aa043d",
              "infotrygdperioder": [
                {
                  "fom": "2023-01-02",
                  "tom": "2023-02-24",
                  "type": "ARBEIDSGIVERUTBETALING",
                  "organisasjonsnummer": "123"
                },
                {
                  "fom": "2023-01-02",
                  "tom": "2023-02-24",
                  "type": "PERSONUTBETALING",
                  "organisasjonsnummer": "456"
                },
                {
                  "fom": "2023-01-02",
                  "tom": "2023-02-24",
                  "type": "FRIPERIODE"
                }
              ],
              "@opprettet": "2023-03-07T09:58:14.608965521",
              "system_read_count": 0,
              "system_participating_services": [
                {
                  "id": "c12ae2cc-838c-4402-be5c-30179ec63e50",
                  "time": "2023-03-07T09:58:14.608965521",
                  "service": "spleis",
                  "instance": "spleis-74b6b945dd-txzqj",
                  "image": "ghcr.io/navikt/helse-spleis/spleis:9407553"
                }
              ],
              "aktørId": "cafebabe",
              "fødselsnummer": "12345678910"
            }
        """.trimIndent()

    @Language("JSON")
    fun overlappendeInfotrygdperiodeEtterInfotrygdEndring(
        hendelseId: UUID,
        vedtaksperiodeId: UUID = UUID.randomUUID(),
        type: String
    ) =
        """
            {
              "@id": "$hendelseId",
              "@event_name": "overlappende_infotrygdperiode_etter_infotrygdendring",
              "organisasjonsnummer": "123",
              "vedtaksperiodeId": "$vedtaksperiodeId",
              "vedtaksperiodeFom": "2023-02-01",
              "vedtaksperiodeTom": "2023-02-12",
              "vedtaksperiodetilstand": "AVVENTER_BLOKKERENDE_PERIODE",
              "infotrygdhistorikkHendelseId": "7ff91df2-fcbe-4657-9171-709917aa043d",
              "infotrygdperioder": [
                {
                  "fom": "2023-02-01",
                  "tom": "2023-02-12",
                  "type": "$type"
                }
              ],
              "aktørId": "cafebabe",
              "fødselsnummer": "12345678910"
            }
        """.trimIndent()

    @Language("JSON")
    private fun vedtaksperiodeVenter(
        vedtaksperiodeId: UUID,
        venterPåVedtaksperiodeId: UUID = UUID.randomUUID(),
        venterPå: String = "GODKJENNING",
        forårsaketAv: String = "påminnelse"
    ) = """
        {
          "@event_name": "vedtaksperiode_venter",
          "organisasjonsnummer": "123",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "ventetSiden": "2023-03-04T21:34:17.96322",
          "venterTil": "+999999999-12-31T23:59:59.999999999",
          "venterPå": {
            "vedtaksperiodeId": "$venterPåVedtaksperiodeId",
            "organisasjonsnummer": "987654321",
            "venteårsak": {
              "hva": "$venterPå",
              "hvorfor": "TESTOLINI"
            }
          },
          "@id": "${UUID.randomUUID()}",
          "fødselsnummer": "12345678910",
          "aktørId": "cafebabe",
          "@forårsaket_av": {
            "event_name": "$forårsaketAv"
          }
        }
    """

}