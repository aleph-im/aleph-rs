ALTER TABLE message_confirmations DROP CONSTRAINT message_confirmations_item_hash_fkey;
ALTER TABLE message_confirmations
    ADD CONSTRAINT message_confirmations_item_hash_fkey
    FOREIGN KEY (item_hash) REFERENCES messages (item_hash) ON DELETE CASCADE;
