CREATE INDEX IF NOT EXISTS ix_file_pins_coalesced_ref
ON file_pins (COALESCE(ref, item_hash))
WHERE type = 'message';
