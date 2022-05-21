CREATE INDEX accounts_next_notification_check_at_idx ON accounts USING BRIN (next_notification_check_at timestamp_minmax_ops);

