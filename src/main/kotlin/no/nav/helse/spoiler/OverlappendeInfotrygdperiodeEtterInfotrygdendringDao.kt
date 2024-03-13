package no.nav.helse.spoiler

import kotliquery.TransactionalSession
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import java.time.Year
import java.util.*
import javax.sql.DataSource

class OverlappendeInfotrygdperiodeEtterInfotrygdendringDao(private val dataSource: DataSource) {

    fun lagre(fødselsnummer: String, overlappendeInfotrygdperioder: List<OverlappendeInfotrygdperiodeEtterInfotrygdendringDto>): List<OverlappendeInfotrygdperiodeEtterInfotrygdendringDto> {
        return sessionOf(dataSource).use {
            it.transaction { session ->
                val eksisterende = session.finnEksisterendeVedtaksperioder(fødselsnummer)
                val nyeOverlapp = overlappendeInfotrygdperioder.filterNot { it.vedtaksperiodeId in eksisterende }

                session.slettPerson(fødselsnummer)

                @Language("PostgreSQL")
                val statement = """
                     INSERT INTO overlappende_infotrygdperiode_etter_infotrygdendring(intern_hendelse_id, opprettet, fodselsnummer, aktor_id, organisasjonsnummer, vedtaksperiode_id, vedtaksperiode_fom, vedtaksperiode_tom, vedtaksperiode_tilstand, infotrygdhistorikk_hendelseId)
                     VALUES (:hendelseId, :opprettet, :fodselsnummer, :aktorId, :organisasjonsnummer, :vedtaksperiodeId, :vedtaksperiodeFom, :vedtaksperiodeTom, :vedtaksperiodeTilstand, :infotrygdhistorikkHendelseId)
                """
                overlappendeInfotrygdperioder.forEach { vedtaksperiode ->
                    session.run(queryOf(
                        statement = statement,
                        paramMap = mapOf(
                            "hendelseId" to vedtaksperiode.hendelseId,
                            "opprettet" to vedtaksperiode.opprettet,
                            "fodselsnummer" to vedtaksperiode.fødelsnummer,
                            "aktorId" to vedtaksperiode.aktørId,
                            "organisasjonsnummer" to vedtaksperiode.organisasjonsnummer,
                            "vedtaksperiodeId" to vedtaksperiode.vedtaksperiodeId,
                            "vedtaksperiodeFom" to vedtaksperiode.vedtaksperiodeFom,
                            "vedtaksperiodeTom" to vedtaksperiode.vedtaksperiodeTom,
                            "vedtaksperiodeTilstand" to vedtaksperiode.vedtaksperiodeTilstand,
                            "infotrygdhistorikkHendelseId" to vedtaksperiode.infotrygdhistorikkHendelseId
                        )).asExecute)

                    @Language("PostgreSQL")
                    val statement2 = """
                    INSERT INTO overlappende_infotrygd_periode(vedtaksperiode_id, fom, tom, type, orgnummer)
                    VALUES ${vedtaksperiode.infotrygdperioder.joinToString { "(?, ?, ?, ?, ?)" }}
            """

                    session.run(queryOf(
                        statement = statement2,
                        *vedtaksperiode.infotrygdperioder.flatMap { periode ->
                            listOf(vedtaksperiode.vedtaksperiodeId, periode.fom, periode.tom, periode.type, periode.orgnummer)
                        }.toTypedArray()
                    ).asExecute)
                }

                nyeOverlapp
            }
        }
    }

    fun lagOppsummering() = sessionOf(dataSource).use {
        it.run(queryOf("""
            select date_part('year', vedtaksperiode_fom), vedtaksperiode_tilstand, count(1), (case when oip.type='FRIPERIODE' then 'FERIE' else 'UTBETALING' end)
            from overlappende_infotrygdperiode_etter_infotrygdendring o
            inner join public.overlappende_infotrygd_periode oip on o.vedtaksperiode_id = oip.vedtaksperiode_id
            group by vedtaksperiode_tilstand,date_part('year', vedtaksperiode_fom), (case when oip.type='FRIPERIODE' then 'FERIE' else 'UTBETALING' end)
        """).map { rad ->
            OppsummeringDto(
                år = Year.of(rad.int(1)),
                tilstand = rad.string(2),
                antall = rad.int(3),
                overlapptype = OppsummeringDto.Overlapptype.valueOf(rad.string(4))
            )
        }.asList)
    }

    private fun TransactionalSession.slettPerson(fnr: String) {
        run(queryOf("delete from overlappende_infotrygdperiode_etter_infotrygdendring where fodselsnummer = ?", fnr).asExecute)
    }
    fun slett(vedtaksperiodeId: UUID) = sessionOf(dataSource).use {
        it.run(queryOf("delete from overlappende_infotrygdperiode_etter_infotrygdendring where vedtaksperiode_id=?", vedtaksperiodeId).asExecute)
    }

    fun finn(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        @Language("PostgreSQL")
        val statement = """
            SELECT vedtaksperiode.intern_hendelse_id FROM overlappende_infotrygdperiode_etter_infotrygdendring vedtaksperiode 
            JOIN overlappende_infotrygd_periode infotrygd on vedtaksperiode.vedtaksperiode_id = infotrygd.vedtaksperiode_id
            WHERE vedtaksperiode.vedtaksperiode_id=?
            AND infotrygd.type in ('ARBEIDSGIVERUTBETALING', 'PERSONUTBETALING')
            AND vedtaksperiode.vedtaksperiode_tilstand != 'AVSLUTTET_UTEN_UTBETALING'
            
        """
        session.run(queryOf(statement, vedtaksperiodeId).map {
            OverlappendeInfotrygdperiodeDto(hendelseId = it.uuid("intern_hendelse_id"))
        }.asList)
    }

    private fun TransactionalSession.finnEksisterendeVedtaksperioder(fødselsnummer: String): Set<UUID> {
        @Language("PostgreSQL")
        val statement = """
            SELECT vedtaksperiode.vedtaksperiode_id FROM overlappende_infotrygdperiode_etter_infotrygdendring vedtaksperiode 
            WHERE vedtaksperiode.fodselsnummer = ?
            
        """
        return run(queryOf(statement, fødselsnummer).map { it.uuid("vedtakskperiode_id") }.asList).toSet()
    }
}

data class OppsummeringDto(
    val år: Year,
    val tilstand: String,
    val antall: Int,
    val overlapptype: Overlapptype
) {
    enum class Overlapptype { FERIE, UTBETALING }
}

data class OverlappendeInfotrygdperiodeDto(
    val hendelseId: UUID // TODO: flere felter?
)