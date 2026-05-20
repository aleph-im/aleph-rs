ALTER TABLE pending_txs
    ADD COLUMN next_attempt TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00';

CREATE INDEX ix_pending_txs_next_attempt
    ON pending_txs (next_attempt, tx_hash);
