--================================================================
-- BRONZE - 5 Tables raws par source
--==============================================================

create table if not exist raw_indeed_jobs(
    id serial         primary key,
    offer_id          varchar(100) unique,
    title             varchar(500),
    company           varchar(300),
    location          varchar(300),
    contract_type     varchar(50),
    salary            varchar(200),
    raw_text          TEXT,
    url               TEXT,
    scrapped_at       timestamp
);

create table if not exist raw_linkedin_jobs(
    id                serial primary key,
    offer_id          varchar(100) unique,
    title             varchar(500),
    company           varchar(300),
    location          varchar(300),
    contract_type     varchar(50),
    salary            varchar(200),
    raw_text          TEXT,
    url               TEXT,
    scrapped_at       timestamp
);

CREATE TABLE IF NOT EXISTS raw_wttj_jobs (
    id            SERIAL PRIMARY KEY,
    offer_id      VARCHAR(100) UNIQUE,
    title         VARCHAR(500),
    company       VARCHAR(300),
    location      VARCHAR(300),
    contract_type VARCHAR(50),
    salary        VARCHAR(200),
    raw_text      TEXT,
    url           TEXT,
    scraped_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_hellowork_jobs (
    id            SERIAL PRIMARY KEY,
    offer_id      VARCHAR(100) UNIQUE,
    title         VARCHAR(500),
    company       VARCHAR(300),
    location      VARCHAR(300),
    contract_type VARCHAR(50),
    salary        VARCHAR(200),
    raw_text      TEXT,
    url           TEXT,
    scraped_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_france_travail_jobs (
    id            SERIAL PRIMARY KEY,
    offer_id      VARCHAR(100) UNIQUE,
    title         VARCHAR(500),
    company       VARCHAR(300),
    location      VARCHAR(300),
    contract_type VARCHAR(50),
    salary        VARCHAR(200),
    raw_text      TEXT,
    url           TEXT,
    scraped_at    TIMESTAMP
);

--================================================================
-- SYLVER - Table unifiée de toute sources
--================================================================

create table if not exists clean_jobs(
    id            SERIAL PRIMARY KEY,
    offer_id      VARCHAR(100) UNIQUE,
    title         VARCHAR(500),
    company       VARCHAR(300),
    location      VARCHAR(300),
    contract_type VARCHAR(50),
    salary        VARCHAR(200),
    raw_text      TEXT,
    url           TEXT,
    source        VARCHAR(50),
    scraped_at    TIMESTAMP,
    created_at    TIMESTAMP DEFAULT NOW ()
)

--==============================================================
-- GOLD - candidatures et lettres générées
--==============================================================

create table if not exists (
    id            serial primary key,
    offer_id      VARCHAR(100) UNIQUE,
    title         VARCHAR(500),
    company       VARCHAR(300),
    url           TEXT,
    source        VARCHAR(50),
    score         INTEGER,
    statut        VARCHAR(50) DEFAULT 'pending',
    created_at    TIMESTAMP DEFAULT NOW()

);


CREATE TABLE IF NOT EXISTS lettres_generees (
    id            SERIAL PRIMARY KEY,
    offer_id      VARCHAR(100),
    lettre_text   TEXT,
    docx_path     TEXT,
    pdf_path      TEXT,
    created_at    TIMESTAMP DEFAULT NOW()
);
