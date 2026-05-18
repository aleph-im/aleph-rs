ALTER TABLE pending_messages DROP CONSTRAINT uq_pending_message;
ALTER TABLE pending_messages ADD CONSTRAINT uq_pending_message UNIQUE (sender, item_hash, signature);
