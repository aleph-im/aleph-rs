CREATE MATERIALIZED VIEW address_stats_mat_view AS
    SELECT sender AS address, type, count(*) AS nb_messages
        FROM messages
        GROUP BY sender, type;

CREATE UNIQUE INDEX ix_address_type ON address_stats_mat_view(address, type);
