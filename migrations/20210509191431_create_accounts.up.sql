CREATE TABLE IF NOT EXISTS accounts (
    id SERIAL PRIMARY KEY,
    username character(20),
    access_token character(64),
    refresh_token character(64),
    expires_at integer,
    last_message_id character(32),
    device_count integer,
    last_checked_at integer
);

CREATE UNIQUE INDEX IF NOT EXISTS accounts_pkey ON accounts(id int4_ops);
CREATE UNIQUE INDEX IF NOT EXISTS accounts_username_key ON accounts(username bpchar_ops);
