-- Table Definition ----------------------------------------------

CREATE TABLE devices (
    id SERIAL PRIMARY KEY,
    apns_token character varying(100) UNIQUE,
    sandbox boolean,
    expires_at timestamp without time zone,
    grace_period_expires_at timestamp without time zone
);

