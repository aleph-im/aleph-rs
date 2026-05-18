-- Backfill denormalized columns on messages.
-- The original migration ran in batched commits inside Python; refinery runs
-- this as a single transaction. The batching was a performance optimization
-- to reduce WAL/lock pressure on a live database; functionally equivalent
-- with set-based UPDATEs below.

-- Disable trigger — migration 0050 will populate counts from scratch
ALTER TABLE messages DISABLE TRIGGER trg_message_counts;

-- Step 1: Backfill status + reception_time from message_status
UPDATE messages m
SET status = ms.status,
    reception_time = ms.reception_time
FROM message_status ms
WHERE m.item_hash = ms.item_hash
  AND m.status IS NULL;

-- Step 1b: Default orphan messages (no message_status row) to PROCESSED
UPDATE messages
SET status = 'processed',
    reception_time = COALESCE(reception_time, time)
WHERE status IS NULL;

-- Step 2: Backfill promoted JSONB fields
UPDATE messages
SET owner = COALESCE(content->>'address', ''),
    content_type = content->>'type',
    content_ref = content->>'ref',
    content_key = content->>'key',
    content_item_hash = content->>'item_hash'
WHERE owner IS NULL AND content IS NOT NULL;

-- Step 3: Backfill payment_type from account_costs
UPDATE messages m
SET payment_type = ac.payment_type
FROM account_costs ac
WHERE m.item_hash = ac.item_hash
  AND m.payment_type IS NULL;

-- Step 4: Backfill first_confirmed_at + first_confirmed_height
UPDATE messages m
SET first_confirmed_at = sub.earliest,
    first_confirmed_height = sub.height
FROM (
    SELECT mc.item_hash,
           MIN(ct.datetime) AS earliest,
           MIN(ct.height) AS height
    FROM message_confirmations mc
    JOIN chain_txs ct ON mc.tx_hash = ct.hash
    GROUP BY mc.item_hash
) sub
WHERE m.item_hash = sub.item_hash
  AND m.first_confirmed_at IS NULL;

-- Enforce NOT NULL constraints
ALTER TABLE messages ALTER COLUMN status SET NOT NULL;
ALTER TABLE messages ALTER COLUMN reception_time SET NOT NULL;
