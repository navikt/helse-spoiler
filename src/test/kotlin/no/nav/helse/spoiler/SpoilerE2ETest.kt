package no.nav.helse.spoiler

import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.sykepenger.libs.postgres.TestDatabase
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*

private val testDatabase = TestDatabase.start().also { it.migrer() }

class SpoilerE2ETest {
    private lateinit var testRapid: TestRapid
    private val dataSource = testDatabase.dataSource

    @BeforeEach
    fun setup() {
        val overlappendeInfotrygdperiodeEtterInfotrygdendringDao = OverlappendeInfotrygdperiodeEtterInfotrygdendringDao(dataSource)
        testRapid =
            TestRapid().apply {
                OverlappendeInfotrygdperioderRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao, null, "https://speil.intern.nav.no")
            }
    }

    @AfterEach
    fun reset() {
        testDatabase.tøm("overlappende_infotrygd_periode", "overlappende_infotrygdperiode_etter_infotrygdendring")
    }

    @Test
    fun `lagrer i databasen`() {
        val hendelseId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(hendelseId))
        assertEquals(3, tellOverlappendeInfotrygdperioderEtterInfotrygdEndring(hendelseId))
    }

    @Test
    fun `slackmeldingen lenker direkte til vedtaksperioden i speil`() {
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(UUID.randomUUID(), vedtaksperiodeId))

        val slackmelding = testRapid.inspektør.message(testRapid.inspektør.size - 1)
        assertEquals("slackmelding", slackmelding["@event_name"].asString())
        assertTrue(slackmelding["melding"].asString().contains("<https://speil.intern.nav.no/vedtaksperiode/$vedtaksperiodeId|Åpne i Speil>"))
    }

    private fun tellOverlappendeInfotrygdperioderEtterInfotrygdEndring(hendelseId: UUID): Int =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM overlappende_infotrygd_periode p inner join overlappende_infotrygdperiode_etter_infotrygdendring vedtaksperiode on p.vedtaksperiode_id = vedtaksperiode.vedtaksperiode_id WHERE vedtaksperiode.intern_hendelse_id = :hendelse_id"
            requireNotNull(
                session.run(queryOf(query, mapOf("hendelse_id" to hendelseId)).map { row -> row.int(1) }.asSingle),
            )
        }

    @Language("JSON")
    fun overlappendeInfotrygdperioder(
        hendelseId: UUID,
        vedtaksperiodeId: UUID = UUID.randomUUID(),
        kanForkastes: Boolean = false,
        tilstand: String = "AVVENTER_BLOKKERENDE_PERIODE",
    ) = """
        {
          "@id": "$hendelseId",
          "@event_name": "overlappende_infotrygdperioder",
          "infotrygdhistorikkHendelseId": "7ff91df2-fcbe-4657-9171-709917aa043d",
          "vedtaksperioder": [
            {
              "organisasjonsnummer": "123",
              "vedtaksperiodeId": "$vedtaksperiodeId",
              "vedtaksperiodeFom": "2023-02-01",
              "vedtaksperiodeTom": "2023-02-12",
              "vedtaksperiodetilstand": "$tilstand",
              "kanForkastes": $kanForkastes,
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
                  "type": "FRIPERIODE",
                  "organisasjonsnummer": null
                }
              ]
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
          "fødselsnummer": "12345678910"
        }
        """.trimIndent()
}
