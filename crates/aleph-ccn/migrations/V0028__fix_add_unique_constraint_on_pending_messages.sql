DELETE FROM pending_messages a USING pending_messages b WHERE a.id < b.id AND a.item_hash = b.item_hash;
ALTER TABLE pending_messages ADD CONSTRAINT uq_pending_message UNIQUE (item_hash);
