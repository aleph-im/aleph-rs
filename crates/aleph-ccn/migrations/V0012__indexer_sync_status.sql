CREATE TABLE indexer_sync_status (
    chain VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    start_block_datetime TIMESTAMPTZ NOT NULL,
    end_block_datetime TIMESTAMPTZ NOT NULL,
    start_included BOOLEAN NOT NULL,
    end_included BOOLEAN NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (chain, event_type, start_block_datetime)
);
