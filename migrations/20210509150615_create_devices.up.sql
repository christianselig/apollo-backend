CREATE TABLE IF NOT EXISTS devices (
    id SERIAL PRIMARY KEY,
    apns_token character(100) UNIQUE,
    sandbox boolean,
    last_pinged_at integer
);
CREATE UNIQUE INDEX IF NOT EXISTS devices_pkey ON devices(id int4_ops);
CREATE UNIQUE INDEX IF NOT EXISTS devices_apns_token_key ON devices(apns_token bpchar_ops);
