-- Table Definition ----------------------------------------------

CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    reddit_account_id character varying(32) DEFAULT ''::character varying,
    username character varying(20) DEFAULT ''::character varying UNIQUE,
    access_token character varying(64) DEFAULT ''::character varying,
    refresh_token character varying(64) DEFAULT ''::character varying,
    token_expires_at timestamp without time zone,
    last_message_id character varying(32) DEFAULT ''::character varying,
    next_notification_check_at timestamp without time zone,
    next_stuck_notification_check_at timestamp without time zone,
    check_count integer DEFAULT 0
);
