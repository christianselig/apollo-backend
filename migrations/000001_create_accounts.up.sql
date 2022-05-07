-- Table Definition ----------------------------------------------

CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    username character varying(20) DEFAULT ''::character varying UNIQUE,
    access_token character varying(64) DEFAULT ''::character varying,
    refresh_token character varying(64) DEFAULT ''::character varying,
    expires_at integer DEFAULT 0,
    last_message_id character varying(32) DEFAULT ''::character varying,
    device_count integer DEFAULT 0,
    last_checked_at double precision DEFAULT '0'::double precision,
    last_enqueued_at double precision DEFAULT '0'::double precision,
    account_id character varying(32) DEFAULT ''::character varying,
    last_unstuck_at double precision DEFAULT '0'::double precision
);

-- Indices -------------------------------------------------------

CREATE INDEX accounts_last_checked_at_idx ON accounts(last_checked_at float8_ops);

