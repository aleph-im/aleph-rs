-- Replace credit_balances with lot-cache schema.
-- One row per granting credit_history entry, with amount_remaining
-- decremented eagerly by writers. Reads become a simple SUM over
-- still-valid lots.

DROP INDEX IF EXISTS ix_credit_balances_address;
DROP TABLE credit_balances;

CREATE TABLE credit_balances (
    address VARCHAR NOT NULL,
    credit_ref VARCHAR NOT NULL,
    credit_index INTEGER NOT NULL,
    amount_remaining BIGINT NOT NULL,
    expiration_date TIMESTAMPTZ,
    message_timestamp TIMESTAMPTZ NOT NULL,
    last_update TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT credit_balances_pkey PRIMARY KEY (address, credit_ref, credit_index)
);

CREATE INDEX ix_credit_balances_address_order
    ON credit_balances (address, message_timestamp, credit_ref, credit_index);

CREATE INDEX ix_credit_balances_address_active
    ON credit_balances (address)
    WHERE amount_remaining > 0;
