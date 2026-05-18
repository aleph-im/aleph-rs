INSERT INTO error_codes(code, description) VALUES
    (504, 'Cannot process a forgotten message');

-- DELETE VMS that have been forgotten (INSTANCE or PROGRAM)
DELETE
    FROM vms v
    WHERE v.item_hash IN
        (SELECT m.item_hash
            FROM messages m
            INNER JOIN forgotten_messages fm ON (m.item_hash = fm.item_hash)
            WHERE m.type = 'INSTANCE' OR m.type = 'PROGRAM');

DELETE
FROM vms v
WHERE v.replaces IN
      (SELECT m.item_hash
       FROM messages m
                INNER JOIN forgotten_messages fm ON (m.item_hash = fm.item_hash)
       WHERE m.type = 'INSTANCE'
          OR m.type = 'PROGRAM');

-- Refresh vm_versions: keep the most recent surviving revision per coalesced ref.
-- (Originally Python looped per vm_hash; the set-based equivalent is below.)
WITH forgotten_vms AS (
    SELECT m.item_hash AS vm_hash
    FROM messages m
    INNER JOIN forgotten_messages fm ON m.item_hash = fm.item_hash
    WHERE m.type = 'INSTANCE' OR m.type = 'PROGRAM'
),
affected_refs AS (
    -- The coalesced ref of every deleted VM (replaces or own hash if no replaces)
    SELECT DISTINCT vm_hash AS ref FROM forgotten_vms
),
latest_per_ref AS (
    SELECT
        COALESCE(v.replaces, v.item_hash) AS ref,
        v.owner,
        v.item_hash,
        v.created
    FROM vms v
    JOIN (
        SELECT COALESCE(replaces, item_hash) AS ref,
               MAX(created) AS created
        FROM vms
        GROUP BY COALESCE(replaces, item_hash)
    ) latest
      ON COALESCE(v.replaces, v.item_hash) = latest.ref
     AND v.created = latest.created
)
DELETE FROM vm_versions vv
USING affected_refs ar
WHERE vv.vm_hash = ar.ref;

WITH forgotten_vms AS (
    SELECT m.item_hash AS vm_hash
    FROM messages m
    INNER JOIN forgotten_messages fm ON m.item_hash = fm.item_hash
    WHERE m.type = 'INSTANCE' OR m.type = 'PROGRAM'
),
affected_refs AS (
    SELECT DISTINCT vm_hash AS ref FROM forgotten_vms
),
latest_per_ref AS (
    SELECT
        COALESCE(v.replaces, v.item_hash) AS ref,
        v.owner,
        v.item_hash,
        v.created
    FROM vms v
    JOIN (
        SELECT COALESCE(replaces, item_hash) AS ref,
               MAX(created) AS created
        FROM vms
        GROUP BY COALESCE(replaces, item_hash)
    ) latest
      ON COALESCE(v.replaces, v.item_hash) = latest.ref
     AND v.created = latest.created
)
INSERT INTO vm_versions (vm_hash, owner, current_version, last_updated)
SELECT lpr.ref, lpr.owner, lpr.item_hash, lpr.created
FROM latest_per_ref lpr
JOIN affected_refs ar ON lpr.ref = ar.ref
ON CONFLICT ON CONSTRAINT program_versions_pkey
DO UPDATE SET current_version = EXCLUDED.current_version,
              last_updated = EXCLUDED.last_updated;

-- DELETE STORE-related file pins / file tags
DELETE
FROM file_pins fp
WHERE fp.item_hash IN (
    SELECT m.item_hash
    FROM messages m
    INNER JOIN forgotten_messages fm ON m.item_hash = fm.item_hash
    WHERE m.type = 'STORE'
);

DELETE
FROM file_tags ft
WHERE ft.tag IN (
    SELECT m.item_hash
    FROM messages m
    INNER JOIN forgotten_messages fm ON m.item_hash = fm.item_hash
    WHERE m.type = 'STORE'
);

-- DELETE MESSAGES referenced by forgotten_messages
DELETE
FROM message_confirmations mc
USING forgotten_messages fm
WHERE mc.item_hash = fm.item_hash;

DELETE
FROM messages m
USING forgotten_messages fm
WHERE m.item_hash = fm.item_hash;
