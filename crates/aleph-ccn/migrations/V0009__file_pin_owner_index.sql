CREATE INDEX ix_file_pins_owner ON file_pins USING HASH (owner) WHERE owner IS NOT NULL;
