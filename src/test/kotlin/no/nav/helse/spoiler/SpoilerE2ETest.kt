package no.nav.helse.spoiler

import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import com.github.navikt.tbd_libs.test_support.CleanupStrategy
import com.github.navikt.tbd_libs.test_support.DatabaseContainers
import com.github.navikt.tbd_libs.test_support.TestDataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*

val databaseContainer = DatabaseContainers.container("spoiler", CleanupStrategy.tables("overlappende_infotrygd_periode,overlappende_infotrygdperiode_etter_infotrygdendring"))

class SpoilerE2ETest {
    private lateinit var testDataSource: TestDataSource
    private lateinit var testRapid: TestRapid
    private val dataSource get() = testDataSource.ds

    @BeforeEach
    fun setup() {
        testDataSource = databaseContainer.nyTilkobling()
        val overlappendeInfotrygdperiodeEtterInfotrygdendringDao = OverlappendeInfotrygdperiodeEtterInfotrygdendringDao(dataSource)
        testRapid = TestRapid().apply {
            OverlappendeInfotrygdperioderRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao, null)
            VedtaksperiodeVenterRiver(this, overlappendeInfotrygdperiodeEtterInfotrygdendringDao)
        }
    }

    @AfterEach
    fun reset() {
        databaseContainer.droppTilkobling(testDataSource)
    }

    @Test
    fun `anmoder om å forkaste vedtaksperiode`() {
        val hendelseId = UUID.randomUUID()
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(hendelseId, vedtaksperiodeId))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId = vedtaksperiodeId, venterPå = "INNTEKTSMELDING"))
        val utgående = testRapid.inspektør.message(testRapid.inspektør.size-1)

        assertEquals("anmodning_om_forkasting", utgående["@event_name"].asText())
        assertEquals(vedtaksperiodeId, utgående["vedtaksperiodeId"].asText().let { UUID.fromString(it) })
    }

    @Test
    fun `anmoder ikke om å forkaste vedtaksperiode i AUU`() {
        val hendelseId = UUID.randomUUID()
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(hendelseId, vedtaksperiodeId, tilstand = "AVSLUTTET_UTEN_UTBETALING"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPå = "INNTEKTSMELDING"))
        val utgående = testRapid.inspektør.message(testRapid.inspektør.size-1)
        assertEquals("slackmelding", utgående["@event_name"].asText())
        assertEquals(1, testRapid.inspektør.size)
    }

    @Test
    fun `anmoder ikke om å forkaste hvis det er noe annet enn inntektsmelding`() {
        val hendelseId = UUID.randomUUID()
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(hendelseId, vedtaksperiodeId, tilstand = "AVSLUTTET_UTEN_UTBETALING"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPå = "GODKJENNING"))
        val utgående = testRapid.inspektør.message(testRapid.inspektør.size-1)
        assertEquals("slackmelding", utgående["@event_name"].asText())
        assertEquals(1, testRapid.inspektør.size)
    }

    @Test
    fun `anmoder ikke om å forkaste vedtaksperiode hvis vedtaksperiode_venter er forårsaket av en anmoding`() {
        val hendelseId = UUID.randomUUID()
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(hendelseId, vedtaksperiodeId, true))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPå = "INNTEKTSMELDING", forårsaketAv = "anmodning_om_forkasting"))
        assertTrue(testRapid.inspektør.size == 0)
    }

    @Test
    fun `lagrer i databasen`() {
        val hendelseId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(hendelseId))
        assertEquals(3, tellOverlappendeInfotrygdperioderEtterInfotrygdEndring(hendelseId))
    }

    @Test
    fun `anmoder ikke om å forkaste vedtaksperiode hvis det er ferie som er registrert i Infotrygd`() {
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(UUID.randomUUID(), vedtaksperiodeId, kanForkastes = true, type  = "FRIPERIODE"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPå = "INNTEKTSMELDING"))
        assertTrue(testRapid.inspektør.size == 0)
    }

    @Test
    fun `anmoder om å forkaste vedtaksperiode hvis det er arbeidsgiverutbetaling som er registrert i Infotrygd`() {
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(UUID.randomUUID(), vedtaksperiodeId, type= "ARBEIDSGIVERUTBETALING"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId = vedtaksperiodeId, venterPå = "INNTEKTSMELDING"))
        val utgående = testRapid.inspektør.message(testRapid.inspektør.size-1)
        assertEquals("anmodning_om_forkasting", utgående["@event_name"].asText())
        assertEquals(vedtaksperiodeId, utgående["vedtaksperiodeId"].asText().let { UUID.fromString(it) })
    }

    @Test
    fun `anmoder om å forkaste vedtaksperiode hvis det er personutbetaling som er registrert i Infotrygd`() {
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(UUID.randomUUID(), vedtaksperiodeId, "PERSONUTBETALING"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId = vedtaksperiodeId, venterPå = "INNTEKTSMELDING"))
        val utgående = testRapid.inspektør.message(testRapid.inspektør.size-1)
        assertEquals("anmodning_om_forkasting", utgående["@event_name"].asText())
        assertEquals(vedtaksperiodeId, utgående["vedtaksperiodeId"].asText().let { UUID.fromString(it) })
    }

    @Test
    fun `anmoder ikke om vi venter på noe som ikke er IM eller Hjelp`() {
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(UUID.randomUUID(), vedtaksperiodeId, true, "PERSONUTBETALING"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId = vedtaksperiodeId, venterPå = "NOE_SPRØTT"))
        assertEquals(0, testRapid.inspektør.size)
    }

    @Test
    fun `anmoder ikke om vi venter på en annen periode som venter på IM`() {
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(UUID.randomUUID(), vedtaksperiodeId, true, "PERSONUTBETALING"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId = UUID.randomUUID(), venterPå = "INNTEKTSMELDING"))
        assertEquals(0, testRapid.inspektør.size)
    }

    @Test
    fun `anmoder ikke om vi venter på en annen periode som venter på Hjelp`() {
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(UUID.randomUUID(), vedtaksperiodeId, true, "PERSONUTBETALING"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId = UUID.randomUUID(), venterPå = "HJELP"))
        assertEquals(0, testRapid.inspektør.size)
    }

    @Test
    fun `anmoder om vi venter på hjelp`() {
        val vedtaksperiodeId = UUID.randomUUID()
        testRapid.sendTestMessage(overlappendeInfotrygdperioder(UUID.randomUUID(), vedtaksperiodeId, "PERSONUTBETALING"))
        testRapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId = vedtaksperiodeId, venterPå = "HJELP"))
        val utgående = testRapid.inspektør.message(testRapid.inspektør.size-1)
        assertEquals("anmodning_om_forkasting", utgående["@event_name"].asText())
        assertEquals(vedtaksperiodeId, utgående["vedtaksperiodeId"].asText().let { UUID.fromString(it) })
    }

    private fun tellOverlappendeInfotrygdperioderEtterInfotrygdEndring(hendelseId: UUID): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM overlappende_infotrygd_periode p inner join overlappende_infotrygdperiode_etter_infotrygdendring vedtaksperiode on p.vedtaksperiode_id = vedtaksperiode.vedtaksperiode_id WHERE vedtaksperiode.intern_hendelse_id = :hendelse_id"
            requireNotNull(
                session.run(queryOf(query, mapOf("hendelse_id" to hendelseId)).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    @Language("JSON")
    fun overlappendeInfotrygdperioder(
        hendelseId: UUID,
        vedtaksperiodeId: UUID = UUID.randomUUID(),
        kanForkastes: Boolean = false,
        tilstand: String = "AVVENTER_BLOKKERENDE_PERIODE"
    ) =
        """
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

    @Language("JSON")
    fun overlappendeInfotrygdperioder(
        hendelseId: UUID,
        vedtaksperiodeId: UUID = UUID.randomUUID(),
        type: String,
        kanForkastes: Boolean = false,
        tilstand: String = "AVVENTER_BLOKKERENDE_PERIODE"
    ) =
        """
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
                    "kanForkastes": $kanForkastes,
                    "vedtaksperiodetilstand": "$tilstand",
                    "infotrygdperioder": [
                        {
                            "fom": "2023-02-01",
                            "tom": "2023-02-12",
                            "type": "$type"
                        }
                    ]      
                  }
              ],
              "fødselsnummer": "12345678910"
            }
        """.trimIndent()

    @Language("JSON")
    private fun vedtaksperiodeVenter(
        vedtaksperiodeId: UUID,
        venterPåVedtaksperiodeId: UUID = UUID.randomUUID(),
        venterPå: String,
        forårsaketAv: String = "påminnelse",
        yrkesaktivitetstype: String = "ARBEIDSTAKER"

        ) = """
        {
          "@event_name": "vedtaksperioder_venter",
          "@id": "${UUID.randomUUID()}",
          "fødselsnummer": "12345678910",
          "@forårsaket_av": {
            "event_name": "$forårsaketAv"
          },
          "vedtaksperioder": [
            {
              "organisasjonsnummer": "123",
              "vedtaksperiodeId": "$vedtaksperiodeId",
              "yrkesaktivitetstype": "$yrkesaktivitetstype",
              "ventetSiden": "2023-03-04T21:34:17.96322",
              "venterTil": "+999999999-12-31T23:59:59.999999999",
              "venterPå": {
                "vedtaksperiodeId": "$venterPåVedtaksperiodeId",
                "organisasjonsnummer": "987654321",
                "venteårsak": {
                  "hva": "$venterPå",
                  "hvorfor": "TESTOLINI"
                }
              }
            }
          ]
        }
    """

}
