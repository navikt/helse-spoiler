CREATE INDEX overlappende_infotrygd_periode_vedtaksperiode_id_idx ON overlappende_infotrygdperiode_etter_infotrygdendring(vedtaksperiode_id);

DROP INDEX overlappende_infotrygd_periode_vedtaksperiode_tilstand_idx;
DROP INDEX overlappende_infotrygd_periode_idx;