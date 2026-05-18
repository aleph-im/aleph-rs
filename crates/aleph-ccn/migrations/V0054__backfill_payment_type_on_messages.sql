-- Backfill payment_type from content JSONB for STORE, PROGRAM, INSTANCE messages.
-- Equivalent to the original batched Python migration; runs as a single UPDATE here.
UPDATE messages
SET payment_type = content->'payment'->>'type'
WHERE payment_type IS NULL
  AND type IN ('STORE', 'PROGRAM', 'INSTANCE')
  AND content->'payment'->>'type' IS NOT NULL;
