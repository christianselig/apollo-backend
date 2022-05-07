-- Table Definition ----------------------------------------------

CREATE TABLE watchers (
    id SERIAL PRIMARY KEY,
    device_id integer REFERENCES devices(id) ON DELETE CASCADE,
    watchee_id integer,
    upvotes integer DEFAULT 0,
    keyword character varying(32) DEFAULT ''::character varying,
    flair character varying(32) DEFAULT ''::character varying,
    domain character varying(32) DEFAULT ''::character varying,
    account_id integer REFERENCES accounts(id) ON DELETE CASCADE,
    created_at double precision DEFAULT '0'::double precision,
    hits integer DEFAULT 0,
    type integer DEFAULT 0,
    last_notified_at double precision DEFAULT '0'::double precision,
    label character varying(64) DEFAULT ''::character varying,
    author character varying(32) DEFAULT ''::character varying,
    subreddit character varying(32) DEFAULT ''::character varying
);

-- Indices -------------------------------------------------------

CREATE INDEX watchers_type_watchee_id_idx ON watchers(type int4_ops,watchee_id int4_ops);

