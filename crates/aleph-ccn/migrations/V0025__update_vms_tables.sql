-- Update the `payment_type` column in the `vms` table based on the `messages` table.
-- Original migration iterated in Python and JSON-decoded item_content; this is the
-- equivalent set-based SQL using jsonb operators.
UPDATE vms
SET payment_type = COALESCE(
    NULLIF((messages.item_content::jsonb -> 'payment' ->> 'type'), ''),
    'hold'
)
FROM messages
WHERE vms.item_hash = messages.item_hash
  AND messages.item_content IS NOT NULL;

UPDATE vms
SET payment_type = 'hold'
WHERE payment_type IS NULL;
