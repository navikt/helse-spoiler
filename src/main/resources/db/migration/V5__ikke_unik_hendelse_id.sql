-- setter hendelseid til not-null
alter table overlappende_infotrygdperiode_etter_infotrygdendring
    alter column infotrygdhistorikk_hendelseId set not null;

-- forbereder ny foreign key
alter table overlappende_infotrygd_periode
    add column vedtaksperiode_id uuid references overlappende_infotrygdperiode_etter_infotrygdendring(vedtaksperiode_id) on delete cascade
;
create index overlappende_infotrygd_periode_vedtaksperiode_id_fk_idx on overlappende_infotrygd_periode(vedtaksperiode_id);

-- fyller ny kolonne med data basert på eksisterende FK-relasjon
update overlappende_infotrygd_periode set vedtaksperiode_id=vedtaksperioder.vedtaksperiode_id
from overlappende_infotrygdperiode_etter_infotrygdendring as vedtaksperioder
where vedtaksperioder.id = overlappende_infotrygd_periode.hendelse_id;

-- setter vedtaksperiode_id til not-null
alter table overlappende_infotrygd_periode
    alter column vedtaksperiode_id set not null;

-- fjerner gammel fk
alter table overlappende_infotrygd_periode drop column hendelse_id;

-- fjerner gammel pkey-constraint
alter table overlappende_infotrygdperiode_etter_infotrygdendring
    drop constraint overlappende_infotrygdperiode_etter_infotrygdendring_pkey cascade;

-- lager ny pkey fra uniq index
alter table overlappende_infotrygdperiode_etter_infotrygdendring
    add primary key(vedtaksperiode_id);

-- renamer felt
alter table overlappende_infotrygdperiode_etter_infotrygdendring
    rename column id to intern_hendelse_id;
