-- Table Definition ----------------------------------------------

CREATE TABLE subreddits (
    id SERIAL PRIMARY KEY,
    subreddit_id character varying(32) DEFAULT ''::character varying UNIQUE,
    name character varying(32) DEFAULT ''::character varying,
    last_checked_at double precision DEFAULT '0'::double precision
);

