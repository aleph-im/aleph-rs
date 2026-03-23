-- Messages
CREATE TABLE IF NOT EXISTS messages (
    item_hash       TEXT PRIMARY KEY,
    type            TEXT NOT NULL,
    chain           TEXT NOT NULL,
    sender          TEXT NOT NULL,
    signature       TEXT NOT NULL,
    item_type       TEXT NOT NULL,
    item_content    TEXT,
    channel         TEXT,
    time            REAL NOT NULL,
    size            INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'processed',
    reception_time  REAL NOT NULL,
    -- Denormalized fields for efficient querying
    owner           TEXT,           -- content.address
    content_type    TEXT,           -- POST content.type
    content_ref     TEXT,           -- content.ref
    content_key     TEXT,           -- AGGREGATE content.key
    content_item_hash TEXT,         -- STORE content.item_hash
    payment_type    TEXT            -- payment.type
);

CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender);
CREATE INDEX IF NOT EXISTS idx_messages_type ON messages(type);
CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel);
CREATE INDEX IF NOT EXISTS idx_messages_time ON messages(time);
CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);
CREATE INDEX IF NOT EXISTS idx_messages_owner ON messages(owner);

-- Aggregates (merged current state)
CREATE TABLE IF NOT EXISTS aggregates (
    address         TEXT NOT NULL,
    key             TEXT NOT NULL,
    content         TEXT NOT NULL,  -- JSON
    time            REAL NOT NULL,
    last_revision_hash TEXT,
    dirty           INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_updated    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (address, key)
);

-- Aggregate elements (individual messages for rebuild)
CREATE TABLE IF NOT EXISTS aggregate_elements (
    item_hash       TEXT PRIMARY KEY,
    address         TEXT NOT NULL,
    key             TEXT NOT NULL,
    content         TEXT NOT NULL,  -- JSON
    time            REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agg_elements_addr_key ON aggregate_elements(address, key);

-- Posts
CREATE TABLE IF NOT EXISTS posts (
    item_hash       TEXT PRIMARY KEY,
    address         TEXT NOT NULL,
    post_type       TEXT NOT NULL,
    ref_            TEXT,
    content         TEXT,  -- JSON
    channel         TEXT,
    time            REAL NOT NULL,
    original_item_hash TEXT,
    latest_amend    TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_posts_address ON posts(address);
CREATE INDEX IF NOT EXISTS idx_posts_channel ON posts(channel);
CREATE INDEX IF NOT EXISTS idx_posts_original ON posts(original_item_hash);

-- Files metadata
CREATE TABLE IF NOT EXISTS files (
    hash            TEXT PRIMARY KEY,
    size            INTEGER NOT NULL,
    file_type       TEXT NOT NULL DEFAULT 'file',
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- File pins (ownership of files)
CREATE TABLE IF NOT EXISTS file_pins (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    file_hash       TEXT NOT NULL,
    owner           TEXT NOT NULL,
    pin_type        TEXT NOT NULL,  -- 'message', 'content', 'grace_period'
    message_hash    TEXT,
    size            INTEGER,
    content_type    TEXT,
    ref_            TEXT,
    delete_by       REAL,           -- Unix timestamp for grace period expiry
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (file_hash) REFERENCES files(hash)
);

CREATE INDEX IF NOT EXISTS idx_file_pins_file ON file_pins(file_hash);
CREATE INDEX IF NOT EXISTS idx_file_pins_owner ON file_pins(owner);
CREATE INDEX IF NOT EXISTS idx_file_pins_message ON file_pins(message_hash);

-- File tags (versioning via ref)
CREATE TABLE IF NOT EXISTS file_tags (
    tag             TEXT PRIMARY KEY,
    owner           TEXT NOT NULL,
    file_hash       TEXT NOT NULL,
    last_updated    REAL NOT NULL
);

-- VMs (programs + instances)
CREATE TABLE IF NOT EXISTS vms (
    item_hash       TEXT PRIMARY KEY,
    vm_type         TEXT NOT NULL,  -- 'program' or 'instance'
    owner           TEXT NOT NULL,
    allow_amend     INTEGER NOT NULL DEFAULT 1,
    replaces        TEXT,
    time            REAL NOT NULL,
    content         TEXT NOT NULL,  -- Full JSON content for queries
    payment_type    TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_vms_owner ON vms(owner);
CREATE INDEX IF NOT EXISTS idx_vms_type ON vms(vm_type);

-- VM machine volumes
CREATE TABLE IF NOT EXISTS vm_volumes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vm_hash         TEXT NOT NULL,
    volume_type     TEXT NOT NULL,
    ref_hash        TEXT,
    use_latest      INTEGER NOT NULL DEFAULT 0,
    size_mib        INTEGER,
    mount           TEXT,
    FOREIGN KEY (vm_hash) REFERENCES vms(item_hash)
);

-- Account costs (per-resource cost tracking)
CREATE TABLE IF NOT EXISTS account_costs (
    owner           TEXT NOT NULL,
    item_hash       TEXT NOT NULL,
    cost_type       TEXT NOT NULL,
    name            TEXT NOT NULL,
    ref_hash        TEXT,
    payment_type    TEXT NOT NULL,
    cost_hold       TEXT NOT NULL DEFAULT '0',
    cost_stream     TEXT NOT NULL DEFAULT '0',
    cost_credit     TEXT NOT NULL DEFAULT '0',
    PRIMARY KEY (owner, item_hash, cost_type)
);

-- Credit balances
CREATE TABLE IF NOT EXISTS credit_balances (
    address         TEXT PRIMARY KEY,
    balance         INTEGER NOT NULL DEFAULT 0
);

-- Credit history (for pre-seeded accounts)
CREATE TABLE IF NOT EXISTS credit_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    address         TEXT NOT NULL,
    amount          INTEGER NOT NULL,
    tx_hash         TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Forgotten messages
CREATE TABLE IF NOT EXISTS forgotten_messages (
    item_hash       TEXT NOT NULL,
    forgotten_by    TEXT NOT NULL,
    reason          TEXT,
    PRIMARY KEY (item_hash, forgotten_by)
);
