-- Rename all common tables to `vm_*`
ALTER TABLE programs RENAME TO vms;
ALTER TABLE program_machine_volumes RENAME TO vm_machine_volumes;
ALTER TABLE program_versions RENAME TO vm_versions;

-- Rename all common columns to `vm_*`
ALTER TABLE vm_machine_volumes RENAME COLUMN program_hash TO vm_hash;
ALTER TABLE vm_versions RENAME COLUMN program_hash TO vm_hash;

-- Rename indexes
ALTER INDEX ix_program_machine_volumes_program_hash RENAME TO ix_vm_machine_volumes_vm_hash;
ALTER INDEX ix_programs_owner RENAME TO ix_vms_owner;

-- Create the instance rootfs table
CREATE TABLE instance_rootfs (
    instance_hash VARCHAR NOT NULL,
    parent_ref VARCHAR NOT NULL,
    parent_use_latest BOOLEAN NOT NULL,
    size_mib INTEGER NOT NULL,
    persistence VARCHAR NOT NULL,
    FOREIGN KEY (instance_hash) REFERENCES vms (item_hash) ON DELETE CASCADE,
    PRIMARY KEY (instance_hash)
);

-- Make program-only columns nullable
ALTER TABLE vms ALTER COLUMN http_trigger DROP NOT NULL;
ALTER TABLE vms ALTER COLUMN persistent DROP NOT NULL;

-- Recreate the cost views (some column names must change)
DROP VIEW costs_view;
DROP VIEW program_costs_view;
DROP VIEW program_volumes_files_view;

CREATE VIEW vm_volumes_files_view(vm_hash, ref, use_latest, type, latest, original, volume_to_use) AS
SELECT volume.program_hash AS vm_hash,
       volume.ref,
       volume.use_latest,
       'code_volume'       AS type,
       tags.file_hash      AS latest,
       originals.file_hash AS original,
       CASE
           WHEN volume.use_latest THEN tags.file_hash
           ELSE originals.file_hash
           END             AS volume_to_use
FROM program_code_volumes volume
         LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
         JOIN file_pins originals ON volume.ref = originals.item_hash
UNION
SELECT volume.program_hash AS vm_hash,
       volume.ref,
       volume.use_latest,
       'data_volume'       AS type,
       tags.file_hash      AS latest,
       originals.file_hash AS original,
       CASE
           WHEN volume.use_latest THEN tags.file_hash
           ELSE originals.file_hash
           END             AS volume_to_use
FROM program_data_volumes volume
         LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
         JOIN file_pins originals ON volume.ref = originals.item_hash
UNION
SELECT volume.program_hash AS vm_hash,
       volume.ref,
       volume.use_latest,
       'runtime'           AS type,
       tags.file_hash      AS latest,
       originals.file_hash AS original,
       CASE
           WHEN volume.use_latest THEN tags.file_hash
           ELSE originals.file_hash
           END             AS volume_to_use
FROM program_runtimes volume
         LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
         JOIN file_pins originals ON volume.ref = originals.item_hash
UNION
SELECT volume.vm_hash,
       volume.ref,
       volume.use_latest,
       'machine_volume'    AS type,
       tags.file_hash      AS latest,
       originals.file_hash AS original,
       CASE
           WHEN volume.use_latest THEN tags.file_hash
           ELSE originals.file_hash
           END             AS volume_to_use
FROM vm_machine_volumes volume
         LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
         JOIN file_pins originals ON volume.ref = originals.item_hash;

CREATE VIEW vm_costs_view AS
SELECT vm_versions.vm_hash,
       vm_versions.owner,
       vms.resources_vcpus,
       vms.resources_memory,
       file_volumes_size.file_volumes_size,
       other_volumes_size.other_volumes_size,
       used_disk.required_disk_space,
       cu.compute_units_required,
       bcp.base_compute_unit_price,
       m.compute_unit_price_multiplier,
       cpm.compute_unit_price,
       free_disk.included_disk_space,
       additional_disk.additional_disk_space,
       adp.disk_price,
       tp.total_price
FROM vm_versions
         JOIN vms on vm_versions.current_version = vms.item_hash
         JOIN (SELECT volume.vm_hash,
                      sum(files.size) AS file_volumes_size
               FROM vm_volumes_files_view volume
                        LEFT JOIN files ON volume.volume_to_use = files.hash
               GROUP BY volume.vm_hash) file_volumes_size
              ON vm_versions.current_version = file_volumes_size.vm_hash
         JOIN (SELECT instance_hash, size_mib * 1024 * 1024 rootfs_size FROM instance_rootfs) rootfs_size
              ON vm_versions.vm_hash = rootfs_size.instance_hash
         JOIN (SELECT vm_hash, SUM(size_mib) * 1024 * 1024 other_volumes_size
               FROM vm_machine_volumes
               GROUP BY vm_hash) other_volumes_size
              ON vm_versions.current_version = other_volumes_size.vm_hash,
     LATERAL (SELECT file_volumes_size + other_volumes_size AS required_disk_space) used_disk,
     LATERAL ( SELECT ceil(GREATEST(ceil(vms.resources_vcpus / 1),
                                    vms.resources_memory / 2000)) AS compute_units_required) cu,
     LATERAL ( SELECT CASE
                          WHEN vms.persistent
                              THEN 20000000000 * cu.compute_units_required
                          ELSE 2000000000 * cu.compute_units_required
                          END AS included_disk_space) free_disk,
     LATERAL ( SELECT GREATEST(file_volumes_size.file_volumes_size +
                               rootfs_size.rootfs_size +
                               other_volumes_size.other_volumes_size -
                               free_disk.included_disk_space,
                               0) AS additional_disk_space) additional_disk,
     LATERAL ( SELECT CASE
                          WHEN vms.persistent THEN 2000
                          ELSE 200
                          END AS base_compute_unit_price) bcp,
     LATERAL ( SELECT 1 + vms.environment_internet::integer AS compute_unit_price_multiplier) m,
     LATERAL ( SELECT cu.compute_units_required * m.compute_unit_price_multiplier::double precision *
                      bcp.base_compute_unit_price::double precision *
                      m.compute_unit_price_multiplier AS compute_unit_price) cpm,
     LATERAL ( SELECT additional_disk.additional_disk_space * 20::double precision /
                      1000000::double precision AS disk_price) adp,
     LATERAL ( SELECT cpm.compute_unit_price + adp.disk_price AS total_price) tp;

CREATE VIEW costs_view AS
SELECT coalesce(vm_prices.owner, storage.owner) address,
       total_vm_cost,
       total_storage_cost,
       total_cost
FROM (SELECT owner, sum(total_price) total_vm_cost FROM vm_costs_view GROUP BY owner) vm_prices
         FULL OUTER JOIN (SELECT owner, sum(f.size) storage_size
                          FROM file_pins
                                   JOIN files f on file_pins.file_hash = f.hash
                          WHERE owner is not null
                          GROUP BY owner) storage ON vm_prices.owner = storage.owner,
     LATERAL (SELECT 3 * storage_size / 1000000 total_storage_cost) sc,
     LATERAL (SELECT coalesce(vm_prices.total_vm_cost, 0) +
                     coalesce(total_storage_cost, 0) AS total_cost ) tc;

-- Add the parent columns for persistent volumes
ALTER TABLE vm_machine_volumes ADD COLUMN parent_ref VARCHAR;
ALTER TABLE vm_machine_volumes ADD COLUMN parent_use_latest BOOLEAN;

-- Add new columns to the vms (ex programs) table
ALTER TABLE vms ADD COLUMN authorized_keys JSONB;
ALTER TABLE vms ADD COLUMN program_type VARCHAR;

-- Update error codes
UPDATE error_codes SET description = 'VM reference not found' WHERE code = 300;
UPDATE error_codes SET description = 'VM volume reference(s) not found' WHERE code = 301;
UPDATE error_codes SET description = 'VM update not allowed' WHERE code = 302;
UPDATE error_codes SET description = 'VM update not targeting the original version of the VM' WHERE code = 303;
INSERT INTO error_codes(code, description) VALUES (304, 'VM volume parent is larger than the child volume');

-- Reprocess failed INSTANCE messages
INSERT INTO pending_messages(item_hash, type, chain, sender, signature, item_type, item_content, time, channel,
                         reception_time, check_message, retries, tx_hash, fetched, next_attempt)
(SELECT rm.item_hash,
        'INSTANCE',
        rm.message ->> 'chain',
        rm.message ->> 'sender',
        rm.message ->> 'signature',
        rm.message ->> 'item_type',
        rm.message ->> 'item_content',
        to_timestamp((rm.message ->> 'time')::numeric),
        rm.message ->> 'channel',
        ms.reception_time,
        true,
        0,
        null,
        false,
        now()
 FROM rejected_messages rm
          JOIN message_status ms on rm.item_hash = ms.item_hash
 WHERE message ->> 'type' = 'INSTANCE');

UPDATE message_status
  SET status = 'pending'
  FROM aleph.public.rejected_messages rm
  WHERE message_status.item_hash = rm.item_hash
    AND rm.message ->> 'type' = 'INSTANCE';

DELETE FROM rejected_messages WHERE message->>'type' = 'INSTANCE';
