-- Drop legacy cost views; account_costs replaces them as a materialized table.
DROP VIEW IF EXISTS costs_view;
DROP VIEW IF EXISTS vm_costs_view;
DROP VIEW IF EXISTS vm_volumes_files_view;

CREATE TABLE account_costs (
    id BIGINT NOT NULL,
    owner VARCHAR NOT NULL,
    item_hash VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    ref VARCHAR,
    payment_type VARCHAR NOT NULL,
    cost_hold DECIMAL NOT NULL DEFAULT 0,
    cost_stream DECIMAL NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    FOREIGN KEY (item_hash) REFERENCES messages (item_hash) ON DELETE CASCADE,
    UNIQUE (owner, item_hash, type, name)
);

-- NOTE: The original Python migration populated `account_costs` by replaying
-- INSTANCE/PROGRAM/STORE messages through aleph.services.cost.get_detailed_costs
-- against a default pricing aggregate. That computation depends on application
-- logic (confidential VM detection, pricing model resolution, volume sizing)
-- that cannot be ported to pure SQL. The Rust runtime recalculates costs on
-- message processing, so this table starts empty.
