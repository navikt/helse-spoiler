package no.nav.helse.spoiler

import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import java.time.Year
import java.time.YearMonth
import java.util.UUID
import javax.sql.DataSource

class OverlappendeInfotrygdperiodeEtterInfotrygdendringDao(private val dataSource: DataSource) {

    fun lagre(overlappendeInfotrygdperiodeEtterInfotrygdendring: OverlappendeInfotrygdperiodeEtterInfotrygdendringDto) = sessionOf(dataSource).use {
        it.transaction { session ->
            @Language("PostgreSQL")
            val statement = """
                    with eksisterende as (
                        select id from overlappende_infotrygdperiode_etter_infotrygdendring where vedtaksperiode_id=:vedtaksperiodeId
                    ), ins as (
                     INSERT INTO overlappende_infotrygdperiode_etter_infotrygdendring(id, opprettet, fodselsnummer, aktor_id, organisasjonsnummer, vedtaksperiode_id, vedtaksperiode_fom, vedtaksperiode_tom, vedtaksperiode_tilstand, infotrygdhistorikk_hendelseId)
                     VALUES (:hendelseId, :opprettet, :fodselsnummer, :aktorId, :organisasjonsnummer, :vedtaksperiodeId, :vedtaksperiodeFom, :vedtaksperiodeTom, :vedtaksperiodeTilstand, :infotrygdhistorikkHendelseId)
                     ON CONFLICT DO NOTHING
                     RETURNING id
                    )
                    select id from ins
                    union all
                    select id from eksisterende;
                """
            val id = session.run(
                queryOf(
                    statement = statement,
                    paramMap = mapOf(
                        "hendelseId" to overlappendeInfotrygdperiodeEtterInfotrygdendring.hendelseId,
                        "opprettet" to overlappendeInfotrygdperiodeEtterInfotrygdendring.opprettet,
                        "fodselsnummer" to overlappendeInfotrygdperiodeEtterInfotrygdendring.fødelsnummer,
                        "aktorId" to overlappendeInfotrygdperiodeEtterInfotrygdendring.aktørId,
                        "organisasjonsnummer" to overlappendeInfotrygdperiodeEtterInfotrygdendring.organisasjonsnummer,
                        "vedtaksperiodeId" to overlappendeInfotrygdperiodeEtterInfotrygdendring.vedtaksperiodeId,
                        "vedtaksperiodeFom" to overlappendeInfotrygdperiodeEtterInfotrygdendring.vedtaksperiodeFom,
                        "vedtaksperiodeTom" to overlappendeInfotrygdperiodeEtterInfotrygdendring.vedtaksperiodeTom,
                        "vedtaksperiodeTilstand" to overlappendeInfotrygdperiodeEtterInfotrygdendring.vedtaksperiodeTilstand,
                        "infotrygdhistorikkHendelseId" to overlappendeInfotrygdperiodeEtterInfotrygdendring.infotrygdhistorikkHendelseId
                    )
                ).map { rad ->
                    rad.uuid("id")
                }.asSingle
            ) ?: error("Forventet å få id tilbake fra spørring; enten fordi raden finnes fra før eller fordi den er satt inn nå")

            // slette evt. tidligere hendelser først, i tilfelle det var en eksisterende vedtaksperiodeId-rad
            session.run(queryOf("DELETE FROM overlappende_infotrygd_periode where hendelse_id=?", id).asExecute)

            @Language("PostgreSQL")
            val statement2 = """
                    INSERT INTO overlappende_infotrygd_periode(hendelse_id, fom, tom, type, orgnummer)
                    VALUES ${overlappendeInfotrygdperiodeEtterInfotrygdendring.infotrygdperioder.joinToString { "(?, ?, ?, ?, ?)" }}
                    ON CONFLICT DO NOTHING
            """

            session.run(
                queryOf(
                    statement = statement2,
                    *overlappendeInfotrygdperiodeEtterInfotrygdendring.infotrygdperioder.flatMap { periode ->
                        listOf(
                            id,
                            periode.fom,
                            periode.tom,
                            periode.type,
                            periode.orgnummer
                        )
                    }.toTypedArray()
                ).asExecute
            )
        }
    }

    fun lagOppsummering() = sessionOf(dataSource).use {
        it.run(queryOf("""
            select date_part('year', vedtaksperiode_fom), vedtaksperiode_tilstand, count(1), (case when oip.type='FRIPERIODE' then 'FERIE' else 'UTBETALING' end)
            from overlappende_infotrygdperiode_etter_infotrygdendring o
            inner join public.overlappende_infotrygd_periode oip on o.id = oip.hendelse_id
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

    fun slettPerson(fnr: String) = sessionOf(dataSource).use {
        it.run(queryOf("delete from overlappende_infotrygdperiode_etter_infotrygdendring where fodselsnummer = ?", fnr).asExecute)
    }
    fun slett(vedtaksperiodeId: UUID) = sessionOf(dataSource).use {
        it.run(queryOf("delete from overlappende_infotrygdperiode_etter_infotrygdendring where vedtaksperiode_id=?", vedtaksperiodeId).asExecute)
    }

    fun finn(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        @Language("PostgreSQL")
        val statement = """
            SELECT vedtaksperiode.id FROM overlappende_infotrygdperiode_etter_infotrygdendring vedtaksperiode 
            JOIN overlappende_infotrygd_periode infotrygd on vedtaksperiode.id = infotrygd.hendelse_id
            WHERE vedtaksperiode.vedtaksperiode_id=?
            AND infotrygd.type in ('ARBEIDSGIVERUTBETALING', 'PERSONUTBETALING')
            AND vedtaksperiode.vedtaksperiode_tilstand != 'AVSLUTTET_UTEN_UTBETALING'
            
        """
        session.run(
            queryOf(
                statement,
                vedtaksperiodeId
            ).map {
                OverlappendeInfotrygdperiodeDto(hendelseId = it.uuid("id"))
            }.asList
        )
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