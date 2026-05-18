CREATE OR REPLACE VIEW ccn_metric_view AS
WITH json_data AS (SELECT item_hash,
                          jsonb_array_elements(content -> 'content' -> 'metrics' -> 'ccn') as ccn_data
                   FROM messages
                   WHERE channel = 'aleph-scoring'
                     AND sender = '0x4D52380D3191274a04846c89c069E6C3F2Ed94e4')
SELECT item_hash,
       (ccn_data ->> 'measured_at')::float           as measured_at,
       ccn_data ->> 'node_id'                        as node_id,
       (ccn_data ->> 'base_latency')::float          as base_latency,
       (ccn_data ->> 'metrics_latency')::float       as metrics_latency,
       (ccn_data ->> 'aggregate_latency')::float     as aggregate_latency,
       (ccn_data ->> 'base_latency_ipv4')::float     as base_latency_ipv4,
       (ccn_data ->> 'file_download_latency')::float as file_download_latency,
       (ccn_data ->> 'pending_messages')::int        as pending_messages,
       (ccn_data ->> 'eth_height_remaining')::int    as eth_height_remaining
FROM json_data;

CREATE OR REPLACE VIEW crn_metric_view AS
WITH json_data AS (
    SELECT
        item_hash,
        jsonb_array_elements(content->'content'->'metrics'->'crn') as crn_data
    FROM messages
    WHERE channel = 'aleph-scoring' AND sender = '0x4D52380D3191274a04846c89c069E6C3F2Ed94e4'
)
SELECT
    item_hash as item_hash,
    (crn_data->>'measured_at')::float as measured_at,
    crn_data->>'node_id' as node_id,
    (crn_data->>'base_latency')::float as base_latency,
    (crn_data->>'base_latency_ipv4')::float as base_latency_ipv4,
    (crn_data->>'full_check_latency')::float as full_check_latency,
    (crn_data->>'diagnostic_vm_latency')::float as diagnostic_vm_latency
FROM json_data;
