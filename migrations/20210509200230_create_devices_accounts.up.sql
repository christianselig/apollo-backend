CREATE TABLE IF NOT EXISTS devices_accounts (
    id SERIAL PRIMARY KEY,
    account_id integer,
    device_id integer
);
CREATE UNIQUE INDEX IF NOT EXISTS devices_accounts_pkey ON devices_accounts(id int4_ops);
CREATE UNIQUE INDEX IF NOT EXISTS devices_accounts_account_id_device_id_idx ON devices_accounts(account_id int4_ops,device_id int4_ops);
