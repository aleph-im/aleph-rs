ALTER TABLE file_pins ADD COLUMN delete_by TIMESTAMPTZ;
CREATE INDEX ix_file_pins_delete_by ON file_pins (delete_by) WHERE delete_by IS NOT NULL;
