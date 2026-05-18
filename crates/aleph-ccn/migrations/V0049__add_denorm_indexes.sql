-- Indexes on denormalized columns. Original migration used CONCURRENTLY
-- to avoid table locks during live deployments; refinery wraps each
-- migration in a transaction so CONCURRENTLY is omitted here. The functional
-- result (indexes existing on these columns) is identical.

CREATE INDEX IF NOT EXISTS ix_messages_type_status_time ON messages (type, status, time DESC);
CREATE INDEX IF NOT EXISTS ix_messages_owner_time ON messages (owner, time DESC);
CREATE INDEX IF NOT EXISTS ix_messages_status ON messages (status);
CREATE INDEX IF NOT EXISTS ix_messages_content_ref ON messages (content_ref) WHERE content_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_messages_content_type ON messages (content_type) WHERE content_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_messages_content_key ON messages (content_key) WHERE content_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_messages_content_item_hash ON messages (content_item_hash) WHERE content_item_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_messages_first_confirmed ON messages (first_confirmed_at DESC NULLS FIRST, time DESC, item_hash ASC);
CREATE INDEX IF NOT EXISTS ix_messages_confirmed_height ON messages (first_confirmed_height) WHERE first_confirmed_height IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_messages_reception_time ON messages (reception_time DESC);
CREATE INDEX IF NOT EXISTS ix_messages_payment_type ON messages (payment_type) WHERE payment_type IS NOT NULL;

-- GIN index for tag containment queries (?|) — replaces the old B-tree ix_messages_posts_type_tags
CREATE INDEX IF NOT EXISTS ix_messages_content_tags_gin ON messages USING GIN ((content->'content'->'tags')) WHERE type = 'POST';

-- Drop obsolete indexes superseded by denormalized columns
DROP INDEX IF EXISTS ix_messages_posts_type_tags;

-- Trigger to keep first_confirmed_at/height in sync
CREATE OR REPLACE FUNCTION update_first_confirmed()
RETURNS TRIGGER AS $$
DECLARE
    tx_dt TIMESTAMPTZ;
    tx_ht BIGINT;
BEGIN
    SELECT datetime, height INTO tx_dt, tx_ht
    FROM chain_txs
    WHERE hash = NEW.tx_hash;

    IF tx_dt IS NOT NULL THEN
        UPDATE messages
        SET first_confirmed_at = LEAST(COALESCE(first_confirmed_at, tx_dt), tx_dt),
            first_confirmed_height = LEAST(COALESCE(first_confirmed_height, tx_ht), tx_ht)
        WHERE item_hash = NEW.item_hash;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_first_confirmed
    AFTER INSERT ON message_confirmations
    FOR EACH ROW
    EXECUTE FUNCTION update_first_confirmed();
