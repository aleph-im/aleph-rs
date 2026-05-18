ALTER TABLE rejected_messages ADD COLUMN tx_hash VARCHAR;
ALTER TABLE rejected_messages ADD FOREIGN KEY (tx_hash) REFERENCES chain_txs (hash);
