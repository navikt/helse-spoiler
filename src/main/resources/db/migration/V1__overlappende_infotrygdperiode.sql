CREATE TABLE overlappende_infotrygdperiode_etter_infotrygdendring
(
    id                            UUID PRIMARY KEY,
    opprettet                     TIMESTAMP NOT NULL,
    fodselsnummer                 VARCHAR   NOT NULL,
    aktor_id                      VARCHAR   NOT NULL,
    organisasjonsnummer           VARCHAR   NOT NULL,
    vedtaksperiode_id             UUID      NOT NULL,
    vedtaksperiode_fom            DATE      NOT NULL,
    vedtaksperiode_tom            DATE      NOT NULL,
    vedtaksperiode_tilstand       VARCHAR   NOT NULL,
    infotrygdhistorikk_hendelseId UUID
);

CREATE TABLE overlappende_infotrygd_periode
(
    id          SERIAL PRIMARY KEY,
    hendelse_id UUID REFERENCES overlappende_infotrygdperiode_etter_infotrygdendring (id),
    fom         DATE    NOT NULL,
    tom         DATE    NOT NULL,
    type        VARCHAR NOT NULL,
    orgnummer   VARCHAR
);

CREATE INDEX overlappende_infotrygd_periode_vedtaksperiode_tilstand_idx ON overlappende_infotrygdperiode_etter_infotrygdendring(vedtaksperiode_tilstand);
CREATE INDEX overlappende_infotrygd_periode_idx ON overlappende_infotrygd_periode(type);
