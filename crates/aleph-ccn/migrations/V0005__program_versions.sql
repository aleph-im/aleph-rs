-- Add the new "created" column on programs to track the program creation time
ALTER TABLE programs ADD COLUMN created TIMESTAMPTZ;
UPDATE programs SET created = to_timestamp((content->>'time')::float)
    FROM messages WHERE messages.item_hash = programs.item_hash;
ALTER TABLE programs ALTER COLUMN created SET NOT NULL;

-- Update the foreign keys on volume tables to use on delete cascade
ALTER TABLE program_code_volumes DROP CONSTRAINT program_code_volumes_program_hash_fkey;
ALTER TABLE program_code_volumes ADD FOREIGN KEY (program_hash) REFERENCES programs (item_hash) ON DELETE CASCADE;

ALTER TABLE program_data_volumes DROP CONSTRAINT program_data_volumes_program_hash_fkey;
ALTER TABLE program_data_volumes ADD FOREIGN KEY (program_hash) REFERENCES programs (item_hash) ON DELETE CASCADE;

ALTER TABLE program_export_volumes DROP CONSTRAINT program_export_volumes_program_hash_fkey;
ALTER TABLE program_export_volumes ADD FOREIGN KEY (program_hash) REFERENCES programs (item_hash) ON DELETE CASCADE;

ALTER TABLE program_machine_volumes DROP CONSTRAINT program_machine_volumes_program_hash_fkey;
ALTER TABLE program_machine_volumes ADD FOREIGN KEY (program_hash) REFERENCES programs (item_hash) ON DELETE CASCADE;

ALTER TABLE program_runtimes DROP CONSTRAINT program_runtimes_program_hash_fkey;
ALTER TABLE program_runtimes ADD FOREIGN KEY (program_hash) REFERENCES programs (item_hash) ON DELETE CASCADE;

-- Create the program versions table
CREATE TABLE program_versions (
    program_hash VARCHAR NOT NULL,
    owner VARCHAR NOT NULL,
    current_version VARCHAR NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (program_hash)
);

-- Add new error codes for programs and stores
INSERT INTO error_codes(code, description) VALUES
    (200, 'Store reference not found'),
    (201, 'Store update not targeting the original version of the file'),
    (300, 'Program reference not found'),
    (301, 'Program volume reference(s) not found'),
    (302, 'Program update not allowed'),
    (303, 'Program update not targeting the original version of the program');
