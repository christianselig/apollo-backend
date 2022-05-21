-- Table Definition ----------------------------------------------

CREATE TABLE watchers (
    id SERIAL PRIMARY KEY,
    created_at timestamp without time zone,
    last_notified_at timestamp without time zone,
    device_id integer REFERENCES devices(id) ON DELETE CASCADE,
    account_id integer REFERENCES accounts(id) ON DELETE CASCADE,
    watchee_id integer,
    upvotes integer DEFAULT 0,
    keyword character varying(32) DEFAULT ''::character varying,
    flair character varying(32) DEFAULT ''::character varying,
    domain character varying(32) DEFAULT ''::character varying,
    hits integer DEFAULT 0,
    type integer DEFAULT 0,
    label character varying(64) DEFAULT ''::character varying,
    author character varying(32) DEFAULT ''::character varying,
    subreddit character varying(32) DEFAULT ''::character varying
);

-- Indices -------------------------------------------------------

CREATE INDEX watchers_type_watchee_id_idx ON watchers(type int4_ops,watchee_id int4_ops);

