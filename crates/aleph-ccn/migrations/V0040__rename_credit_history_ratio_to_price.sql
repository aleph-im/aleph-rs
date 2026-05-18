-- First, add the new price column
ALTER TABLE credit_history ADD COLUMN price DECIMAL;

-- Add the new bonus_amount column
ALTER TABLE credit_history ADD COLUMN bonus_amount BIGINT;

-- Transform data: price calculation depends on payment token
-- Taking into account that bonus_ratio = 1.2
-- For ALEPH token: ratio = (1 / price) * bonus_ratio, therefore price = bonus_ratio / ratio
-- For other tokens: price = 1/ratio
-- Only update rows where payment_method is NOT 'credit_expense' or 'credit_transfer'
-- and where ratio is not null and not zero
UPDATE credit_history
SET price = CASE
    WHEN token = 'ALEPH' THEN ROUND(1.2 / ratio, 18)
    ELSE ROUND(1.0 / ratio, 18)
END
WHERE ratio IS NOT NULL
AND ratio != 0
AND (payment_method IS NULL
     OR (payment_method != 'credit_expense' AND payment_method != 'credit_transfer'));

-- Update bonus_amount for ALEPH token records
UPDATE credit_history
SET bonus_amount = TRUNC(amount / 1.2)
WHERE token = 'ALEPH'
AND amount IS NOT NULL;

-- Drop the old ratio column
ALTER TABLE credit_history DROP COLUMN ratio;
