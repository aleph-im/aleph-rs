CREATE TABLE programs (
    item_hash VARCHAR NOT NULL,
    owner VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    allow_amend BOOLEAN NOT NULL,
    metadata JSONB,
    variables JSONB,
    http_trigger BOOLEAN NOT NULL,
    message_triggers JSONB,
    persistent BOOLEAN NOT NULL,
    environment_reproducible BOOLEAN NOT NULL,
    environment_internet BOOLEAN NOT NULL,
    environment_aleph_api BOOLEAN NOT NULL,
    environment_shared_cache BOOLEAN NOT NULL,
    resources_vcpus INTEGER NOT NULL,
    resources_memory INTEGER NOT NULL,
    resources_seconds INTEGER NOT NULL,
    cpu_architecture VARCHAR,
    cpu_vendor VARCHAR,
    node_owner VARCHAR,
    node_address_regex VARCHAR,
    replaces VARCHAR,
    FOREIGN KEY (replaces) REFERENCES programs (item_hash),
    PRIMARY KEY (item_hash)
);
CREATE INDEX ix_programs_owner ON programs (owner);

CREATE TABLE program_code_volumes (
    encoding VARCHAR NOT NULL,
    ref VARCHAR,
    use_latest BOOLEAN,
    entrypoint VARCHAR NOT NULL,
    program_hash VARCHAR NOT NULL,
    FOREIGN KEY (program_hash) REFERENCES programs (item_hash),
    PRIMARY KEY (program_hash)
);

CREATE TABLE program_data_volumes (
    encoding VARCHAR NOT NULL,
    ref VARCHAR,
    use_latest BOOLEAN,
    mount VARCHAR NOT NULL,
    program_hash VARCHAR NOT NULL,
    FOREIGN KEY (program_hash) REFERENCES programs (item_hash),
    PRIMARY KEY (program_hash)
);

CREATE TABLE program_export_volumes (
    encoding VARCHAR NOT NULL,
    program_hash VARCHAR NOT NULL,
    FOREIGN KEY (program_hash) REFERENCES programs (item_hash),
    PRIMARY KEY (program_hash)
);

CREATE TABLE program_machine_volumes (
    id INTEGER NOT NULL,
    type VARCHAR NOT NULL,
    program_hash VARCHAR NOT NULL,
    comment VARCHAR,
    mount VARCHAR,
    size_mib INTEGER,
    ref VARCHAR,
    use_latest BOOLEAN,
    persistence VARCHAR,
    name VARCHAR,
    FOREIGN KEY (program_hash) REFERENCES programs (item_hash),
    PRIMARY KEY (id)
);
CREATE INDEX ix_program_machine_volumes_program_hash ON program_machine_volumes (program_hash);

CREATE TABLE program_runtimes (
    ref VARCHAR,
    use_latest BOOLEAN,
    program_hash VARCHAR NOT NULL,
    comment VARCHAR NOT NULL,
    FOREIGN KEY (program_hash) REFERENCES programs (item_hash),
    PRIMARY KEY (program_hash)
);
