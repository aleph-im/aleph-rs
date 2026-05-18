CREATE TABLE balances (
    id BIGINT NOT NULL,
    address VARCHAR NOT NULL,
    chain VARCHAR NOT NULL,
    dapp VARCHAR,
    balance DECIMAL NOT NULL,
    eth_height INTEGER NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ix_balances_address ON balances (address);

ALTER TABLE balances ADD CONSTRAINT balances_address_chain_dapp_uindex
UNIQUE NULLS NOT DISTINCT (address, chain, dapp);
