-- Table Definition ----------------------------------------------

CREATE TABLE subreddits (
    id SERIAL PRIMARY KEY,
    subreddit_id character varying(32) DEFAULT ''::character varying UNIQUE,
    name character varying(32) DEFAULT ''::character varying,
    next_check_at timestamp without time zone
);

