package no.nav.helse.spoiler

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OverlappendeInfotrygdperiodeEtterInfotrygdendringRiverTest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val overlappendeInfotrygdperiodeEtterInfotrygdendringDao = OverlappendeInfotrygdperiodeEtterInfotrygdendringDao(dataSource)
    private val river = TestRapid().apply {
        OverlappendeInfotrygdperiodeEtterInfotrygdendringRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao)
    }

    @AfterAll
    fun tearDown() {
        river.stop()
        dataSource.connection.close()
        embeddedPostgres.close()
    }

    @AfterEach
    fun reset() {
        dataSource.resetDatabase()
    }

    @Test
    fun `lagrer i databasen`() {
        val hendelseId = UUID.randomUUID()
        river.sendTestMessage(overlappendeInfotrygdperiodeEtterInfotrygdEndring(hendelseId))
        assertEquals(3, tellOverlappendeInfotrygdperioderEtterInfotrygdEndring(hendelseId))
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
        hendelseId: UUID
    ) =
        """
            {
              "@id": "$hendelseId",
              "@event_name": "overlappende_infotrygdperiode_etter_infotrygdendring",
              "organisasjonsnummer": "123",
              "vedtaksperiodeId": "e7edced9-8aab-4b7a-8eb4-566860192a98",
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

}