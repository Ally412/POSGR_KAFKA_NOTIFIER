-- When the digest told someone about this alert. NULL means nobody has been told yet, which
-- makes the table its own work queue: no separate outbox, and a row cannot be lost between
-- being stored and being sent.
ALTER TABLE health_alert ADD COLUMN notified_at timestamptz;

-- The digest only ever asks for un-notified rows, and once the backlog is drained that is a
-- vanishing fraction of the table. A partial index keeps the scan proportional to the work.
CREATE INDEX idx_health_alert_unnotified ON health_alert (received_at) WHERE notified_at IS NULL;
