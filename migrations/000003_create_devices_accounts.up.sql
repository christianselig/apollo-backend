-- Table Definition ----------------------------------------------

CREATE TABLE devices_accounts (
    id SERIAL PRIMARY KEY,
    account_id integer REFERENCES accounts(id) ON DELETE CASCADE,
    device_id integer REFERENCES devices(id) ON DELETE CASCADE,
    watcher_notifiable boolean DEFAULT true,
    inbox_notifiable boolean DEFAULT true,
    global_mute boolean DEFAULT false
);

-- Indices -------------------------------------------------------

CREATE UNIQUE INDEX devices_accounts_account_id_device_id_idx ON devices_accounts(account_id int4_ops,device_id int4_ops);

