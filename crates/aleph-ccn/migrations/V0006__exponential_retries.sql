ALTER TABLE pending_messages ADD COLUMN next_attempt TIMESTAMPTZ NOT NULL;
DROP INDEX ix_retries_time;
CREATE INDEX ix_next_attempt ON pending_messages (next_attempt ASC);
