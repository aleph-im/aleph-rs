-- Composite index on (owner, payment_type) for filtered cost queries
-- This optimizes: WHERE owner = ? AND payment_type = ?
CREATE INDEX ix_account_costs_owner_payment_type
    ON account_costs (owner, payment_type);

-- Index on item_hash for FK lookups and joins with message_confirmations
-- PostgreSQL doesn't auto-create indexes on FK columns
CREATE INDEX ix_account_costs_item_hash
    ON account_costs (item_hash);

-- Index on payment_method for filtering credit expenses
-- Optimizes: WHERE payment_method = 'credit_expense'
CREATE INDEX ix_credit_history_payment_method
    ON credit_history (payment_method);

-- Index on origin for resource-specific credit lookups
-- Optimizes: WHERE origin IN (item_hash1, item_hash2, ...)
CREATE INDEX ix_credit_history_origin
    ON credit_history (origin);
