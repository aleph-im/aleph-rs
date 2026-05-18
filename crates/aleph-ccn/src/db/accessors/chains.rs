//! Chain sync accessors. Mirrors `aleph/db/accessors/chains.py`.

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio_postgres::GenericClient;

use aleph_types::chain::Chain;

use crate::AlephResult;
use crate::db::models::chains::{ChainSyncStatusDb, ChainTxDb, IndexerSyncStatusDb};
use crate::toolkit::range::{MultiRange, Range};
use crate::toolkit::timestamp::utc_now;
use crate::types::chain_sync::{ChainEventType, ChainSyncProtocol};

fn chain_to_str(c: &Chain) -> String {
    serde_json::to_value(c)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default()
}

fn event_type_to_str(t: ChainEventType) -> &'static str {
    match t {
        ChainEventType::Message => "message",
        ChainEventType::Sync => "sync",
    }
}

fn sync_protocol_to_str(p: ChainSyncProtocol) -> &'static str {
    match p {
        ChainSyncProtocol::OnChainSync => "aleph",
        ChainSyncProtocol::OffChainSync => "aleph-offchain",
        ChainSyncProtocol::SmartContract => "smart-contract",
    }
}

/// Latest synced height for a `(chain, type)`.
pub async fn get_last_height(
    client: &impl GenericClient,
    chain: Chain,
    sync_type: ChainEventType,
) -> AlephResult<Option<i32>> {
    let row = client
        .query_opt(
            "SELECT height FROM chains_sync_status WHERE chain = $1 AND type = $2",
            &[&chain_to_str(&chain), &event_type_to_str(sync_type)],
        )
        .await?;
    Ok(row.as_ref().map(|r| r.get::<_, i32>(0)))
}

/// Upsert a chain transaction record; conflict on hash is a no-op.
pub async fn upsert_chain_tx(client: &impl GenericClient, tx: &ChainTxDb) -> AlephResult<()> {
    let sql = "INSERT INTO chain_txs(hash, chain, height, datetime, publisher, protocol, \
                                     protocol_version, content) \
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
               ON CONFLICT DO NOTHING";
    client
        .execute(
            sql,
            &[
                &tx.hash,
                &chain_to_str(&tx.chain),
                &tx.height,
                &tx.datetime,
                &tx.publisher,
                &sync_protocol_to_str(tx.protocol),
                &tx.protocol_version,
                &tx.content,
            ],
        )
        .await?;
    Ok(())
}

/// Upsert the latest-synced `(chain, type)` height. Mirrors
/// `upsert_chain_sync_status`.
pub async fn upsert_chain_sync_status(
    client: &impl GenericClient,
    chain: Chain,
    sync_type: ChainEventType,
    height: i32,
    update_datetime: DateTime<Utc>,
) -> AlephResult<()> {
    let sql = "INSERT INTO chains_sync_status(chain, type, height, last_update) \
               VALUES ($1, $2, $3, $4) \
               ON CONFLICT ON CONSTRAINT chains_sync_status_pkey \
               DO UPDATE SET height = EXCLUDED.height, last_update = EXCLUDED.last_update";
    client
        .execute(
            sql,
            &[
                &chain_to_str(&chain),
                &event_type_to_str(sync_type),
                &height,
                &update_datetime,
            ],
        )
        .await?;
    Ok(())
}

/// Indexer sync ranges. Mirrors Python `IndexerMultiRange` dataclass.
#[derive(Debug, Clone)]
pub struct IndexerMultiRange {
    pub chain: Chain,
    pub event_type: ChainEventType,
    pub datetime_multirange: MultiRange<DateTime<Utc>>,
}

impl IndexerMultiRange {
    pub fn iter_ranges(&self) -> impl Iterator<Item = &Range<DateTime<Utc>>> {
        self.datetime_multirange.iter()
    }
}

/// Already-synced datetime multirange for a `(chain, event_type)`.
pub async fn get_indexer_multirange(
    client: &impl GenericClient,
    chain: Chain,
    event_type: ChainEventType,
) -> AlephResult<IndexerMultiRange> {
    let sql = "SELECT chain, event_type, start_block_datetime, end_block_datetime, \
                       start_included, end_included, last_updated \
               FROM indexer_sync_status \
               WHERE chain = $1 AND event_type = $2 \
               ORDER BY start_block_datetime";
    let rows = client
        .query(
            sql,
            &[&chain_to_str(&chain), &event_type_to_str(event_type)],
        )
        .await?;
    let mut multirange: MultiRange<DateTime<Utc>> = MultiRange::default();
    for row in rows.iter() {
        let r = IndexerSyncStatusDb::from_row(row);
        multirange.add_range(r.to_range());
    }
    Ok(IndexerMultiRange {
        chain,
        event_type,
        datetime_multirange: multirange,
    })
}

/// Compute the missing portion of `indexer_multirange` w.r.t. the DB state.
pub async fn get_missing_indexer_datetime_multirange(
    client: &impl GenericClient,
    chain: Chain,
    event_type: ChainEventType,
    indexer_multirange: &MultiRange<DateTime<Utc>>,
) -> AlephResult<MultiRange<DateTime<Utc>>> {
    let db = get_indexer_multirange(client, chain, event_type).await?;
    Ok(indexer_multirange.subtract(&db.datetime_multirange))
}

/// Replace the indexer multirange for `(chain, event_type)`.
pub async fn update_indexer_multirange(
    client: &impl GenericClient,
    indexer_multirange: &IndexerMultiRange,
) -> AlephResult<()> {
    let chain_s = chain_to_str(&indexer_multirange.chain);
    let event_s = event_type_to_str(indexer_multirange.event_type);
    client
        .execute(
            "DELETE FROM indexer_sync_status WHERE chain = $1 AND event_type = $2",
            &[&chain_s, &event_s],
        )
        .await?;
    let update_time = utc_now();
    for range in indexer_multirange.iter_ranges() {
        client
            .execute(
                "INSERT INTO indexer_sync_status(chain, event_type, start_block_datetime, \
                                                 start_included, end_block_datetime, \
                                                 end_included, last_updated) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    &chain_s,
                    &event_s,
                    &range.lower,
                    &range.lower_inc,
                    &range.upper,
                    &range.upper_inc,
                    &update_time,
                ],
            )
            .await?;
    }
    Ok(())
}

/// Add a new datetime range to the indexer multirange, then persist it.
pub async fn add_indexer_range(
    client: &impl GenericClient,
    chain: Chain,
    event_type: ChainEventType,
    datetime_range: Range<DateTime<Utc>>,
) -> AlephResult<()> {
    let mut imr = get_indexer_multirange(client, chain.clone(), event_type).await?;
    imr.datetime_multirange.add_range(datetime_range);
    update_indexer_multirange(client, &imr).await?;
    Ok(())
}

/// Fetch a chain tx by hash.
pub async fn get_chain_tx(
    client: &impl GenericClient,
    hash: &str,
) -> AlephResult<Option<ChainTxDb>> {
    let row = client
        .query_opt(
            "SELECT hash, chain, height, datetime, publisher, protocol, protocol_version, \
                    content \
             FROM chain_txs WHERE hash = $1",
            &[&hash],
        )
        .await?;
    Ok(row.as_ref().map(ChainTxDb::from_row))
}

/// Fetch chain sync status for a `(chain, type)` pair.
pub async fn get_chain_sync_status(
    client: &impl GenericClient,
    chain: Chain,
    sync_type: ChainEventType,
) -> AlephResult<Option<ChainSyncStatusDb>> {
    let row = client
        .query_opt(
            "SELECT chain, type, height, last_update FROM chains_sync_status \
             WHERE chain = $1 AND type = $2",
            &[&chain_to_str(&chain), &event_type_to_str(sync_type)],
        )
        .await?;
    Ok(row.as_ref().map(ChainSyncStatusDb::from_row))
}

/// Helper used by tests / callers wanting to round-trip a raw JSON payload.
pub fn _unused_value_ref() -> &'static Value {
    static NULL: once_cell::sync::Lazy<Value> = once_cell::sync::Lazy::new(|| Value::Null);
    &NULL
}
