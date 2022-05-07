-- Table Definition ----------------------------------------------

CREATE TABLE devices (
    id SERIAL PRIMARY KEY,
    apns_token character varying(100) UNIQUE,
    sandbox boolean,
    active_until integer,
    grace_period_until integer
);

