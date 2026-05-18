-- Ensure pg_trgm extension is enabled for text search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create trigram index for substring search on the address column of address_stats_mat_view
CREATE INDEX IF NOT EXISTS idx_address_stats_mat_view_address_trgm
ON address_stats_mat_view
USING gin (lower(address) gin_trgm_ops);

-- Create covering index to optimize queries that need address and nb_messages
CREATE INDEX IF NOT EXISTS idx_address_stats_covering
ON address_stats_mat_view(address)
INCLUDE (nb_messages);
