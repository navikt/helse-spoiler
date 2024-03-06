alter table overlappende_infotrygd_periode
drop constraint overlappende_infotrygd_periode_hendelse_id_fkey,
add constraint overlappende_infotrygd_periode_hendelse_id_fkey
    foreign key (hendelse_id)
        references overlappende_infotrygdperiode_etter_infotrygdendring(id)
        on delete cascade;

CREATE INDEX overlappende_infotrygd_periode_fk_idx ON overlappende_infotrygd_periode(hendelse_id);