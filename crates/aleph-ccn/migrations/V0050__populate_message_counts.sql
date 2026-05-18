-- Clear any stale data (safety)
TRUNCATE message_counts;

-- Global: by status only
INSERT INTO message_counts (status, count)
SELECT COALESCE(status, ''), COUNT(*)
FROM messages
GROUP BY status;

-- Per (type, status)
INSERT INTO message_counts (type, status, count)
SELECT COALESCE(type, ''), COALESCE(status, ''), COUNT(*)
FROM messages
GROUP BY type, status;

-- Per (sender, status)
INSERT INTO message_counts (sender, status, count)
SELECT COALESCE(sender, ''), COALESCE(status, ''), COUNT(*)
FROM messages
GROUP BY sender, status;

-- Per (sender, type, status) — for per-address stats
INSERT INTO message_counts (sender, type, status, count)
SELECT COALESCE(sender, ''), COALESCE(type, ''), COALESCE(status, ''), COUNT(*)
FROM messages
GROUP BY sender, type, status;

-- Per (owner, status)
INSERT INTO message_counts (owner, status, count)
SELECT owner, COALESCE(status, ''), COUNT(*)
FROM messages
WHERE owner IS NOT NULL AND owner != ''
GROUP BY owner, status;

-- Re-enable trigger: all future changes are tracked automatically
ALTER TABLE messages ENABLE TRIGGER trg_message_counts;

-- Drop the materialized view superseded by message_counts
DROP MATERIALIZED VIEW IF EXISTS address_stats_mat_view;
