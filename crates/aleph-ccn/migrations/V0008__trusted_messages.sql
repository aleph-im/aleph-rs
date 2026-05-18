ALTER TABLE forgotten_messages ALTER COLUMN signature DROP NOT NULL;
ALTER TABLE messages ALTER COLUMN signature DROP NOT NULL;
ALTER TABLE pending_messages ALTER COLUMN signature DROP NOT NULL;

ALTER TABLE pending_messages ADD CONSTRAINT signature_not_null_if_check_message
    CHECK (signature IS NOT NULL OR NOT check_message);
