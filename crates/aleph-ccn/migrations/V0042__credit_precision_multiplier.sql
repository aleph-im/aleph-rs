-- Credit precision multiplier - 10,000x
-- Multiplies all credit values by 10,000 to support new precision:
-- 1 USD = 1,000,000 credits (previously 100 credits).
-- Only entries with message_timestamp < CUTOFF_TIMESTAMP are multiplied.

-- 1. Update credit_history.amount only for entries BEFORE the cutoff
UPDATE credit_history
SET amount = amount * 10000
WHERE message_timestamp < '2026-02-02 00:00:00+00'::timestamptz;

-- 2. Clear credit_balances cache - will be recalculated from history on next access
--    This ensures balances are computed correctly from the updated history
TRUNCATE TABLE credit_balances;

-- 3. Update account_costs.cost_credit for messages created BEFORE the cutoff
--    Join with messages table to get the message creation time
UPDATE account_costs
SET cost_credit = cost_credit * 10000
FROM messages
WHERE account_costs.item_hash = messages.item_hash
  AND messages.time < '2026-02-02 00:00:00+00'::timestamptz;
