-- Create credit_history table (detailed audit trail)
CREATE TABLE credit_history (
    id BIGSERIAL,
    address VARCHAR NOT NULL,
    amount BIGINT NOT NULL,
    ratio DECIMAL,
    tx_hash VARCHAR,
    token VARCHAR,
    chain VARCHAR,
    provider VARCHAR,
    origin VARCHAR,
    origin_ref VARCHAR,
    payment_method VARCHAR,
    credit_ref VARCHAR NOT NULL,
    credit_index INTEGER NOT NULL,
    expiration_date TIMESTAMPTZ,
    message_timestamp TIMESTAMPTZ NOT NULL,
    last_update TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT credit_history_pkey PRIMARY KEY (credit_ref, credit_index)
);

CREATE INDEX ix_credit_history_address ON credit_history (address);
CREATE INDEX ix_credit_history_message_timestamp ON credit_history (message_timestamp);

-- Create credit_balances table (cached balance summary)
CREATE TABLE credit_balances (
    address VARCHAR NOT NULL,
    balance BIGINT NOT NULL DEFAULT 0,
    last_update TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT credit_balances_pkey PRIMARY KEY (address)
);

CREATE INDEX ix_credit_balances_address ON credit_balances (address);
