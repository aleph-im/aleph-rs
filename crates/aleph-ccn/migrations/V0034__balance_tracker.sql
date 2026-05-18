ALTER TABLE balances
    ADD COLUMN last_update TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE cron_jobs (
    id VARCHAR NOT NULL,
    -- Interval is specified in seconds
    interval INTEGER NOT NULL DEFAULT 24,
    last_run TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO cron_jobs(id, interval, last_run) VALUES ('balance', 3600, '2025-01-01 00:00:00');

INSERT INTO balances(address, chain, balance, eth_height)
SELECT DISTINCT m.sender, 'ETH', 0, 22196000 FROM messages m
INNER JOIN message_status ms ON m.item_hash = ms.item_hash
LEFT JOIN balances b ON m.sender = b.address
WHERE m."type" = 'STORE' AND ms.status = 'processed' AND b.address IS NULL AND m."time" > '2025-04-04T0:0:0.000Z';
