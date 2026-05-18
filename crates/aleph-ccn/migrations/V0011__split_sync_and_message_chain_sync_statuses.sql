-- Add the new type column
ALTER TABLE chains_sync_status ADD COLUMN type VARCHAR;
-- We only support message events on Tezos, every other chain connector fetches sync events
UPDATE chains_sync_status SET type = 'sync' WHERE chain != 'TEZOS';
UPDATE chains_sync_status SET type = 'message' WHERE chain = 'TEZOS';
ALTER TABLE chains_sync_status ALTER COLUMN type SET NOT NULL;

-- Recreate the primary key
ALTER TABLE chains_sync_status DROP CONSTRAINT chains_sync_status_pkey;
ALTER TABLE chains_sync_status ADD CONSTRAINT chains_sync_status_pkey PRIMARY KEY (chain, type);
