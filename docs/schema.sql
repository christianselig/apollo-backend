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

CREATE TABLE devices (
    id SERIAL PRIMARY KEY,
    apns_token character varying(100) UNIQUE,
    sandbox boolean,
    expires_at timestamp without time zone,
    grace_period_expires_at timestamp without time zone
);

CREATE TABLE devices_accounts (
    id SERIAL PRIMARY KEY,
    account_id integer REFERENCES accounts(id) ON DELETE CASCADE,
    device_id integer REFERENCES devices(id) ON DELETE CASCADE,
    watcher_notifiable boolean DEFAULT true,
    inbox_notifiable boolean DEFAULT true,
    global_mute boolean DEFAULT false
);

CREATE UNIQUE INDEX devices_accounts_account_id_device_id_idx ON devices_accounts(account_id int4_ops,device_id int4_ops);

CREATE TABLE subreddits (
    id SERIAL PRIMARY KEY,
    subreddit_id character varying(32) DEFAULT ''::character varying UNIQUE,
    name character varying(32) DEFAULT ''::character varying,
    next_check_at timestamp without time zone
);

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    user_id character varying(32) DEFAULT ''::character varying UNIQUE,
    name character varying(32) DEFAULT ''::character varying,
    next_check_at timestamp without time zone
);

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

