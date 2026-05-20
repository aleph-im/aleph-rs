//! BSC connector. Mirrors `aleph/chains/bsc.py`.
//!
//! BSC uses the multichain indexer for chain events (no on-chain log
//! polling), and EVM signatures for verification — so `BscConnector` simply
//! wraps an `AlephIndexerReader` parameterised for the BSC blockchain.

use async_trait::async_trait;
use std::sync::Arc;

use super::abc::ChainReader;
use super::chain_data_service::PendingTxPublisher;
use super::indexer_reader::AlephIndexerReader;
use crate::AlephResult;
use crate::config::Settings;
use crate::db::DbPool;
use crate::types::chain_sync::ChainEventType;
use aleph_types::chain::Chain;

/// BSC reader-only connector (BSC packing is delegated to ETH for now in pyaleph).
pub struct BscConnector {
    indexer_reader: AlephIndexerReader,
    pool: Option<DbPool>,
    pending_tx_publisher: Option<Arc<PendingTxPublisher>>,
}

impl BscConnector {
    pub fn new() -> Self {
        Self {
            indexer_reader: AlephIndexerReader::new(Chain::Bsc),
            pool: None,
            pending_tx_publisher: None,
        }
    }

    pub fn with_services(pool: DbPool, pending_tx_publisher: Arc<PendingTxPublisher>) -> Self {
        Self {
            indexer_reader: AlephIndexerReader::new(Chain::Bsc),
            pool: Some(pool),
            pending_tx_publisher: Some(pending_tx_publisher),
        }
    }
}

impl Default for BscConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ChainReader for BscConnector {
    async fn fetcher(&self, cfg: &Settings) -> AlephResult<()> {
        if let (Some(pool), Some(publisher)) = (&self.pool, &self.pending_tx_publisher) {
            return self
                .indexer_reader
                .run(
                    pool.clone(),
                    publisher.clone(),
                    cfg.aleph.indexer_url.clone(),
                    ChainEventType::Message,
                )
                .await;
        }
        self.indexer_reader
            .fetcher(
                &cfg.aleph.indexer_url,
                &cfg.bsc.sync_contract,
                ChainEventType::Message,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bsc_connector_constructs() {
        let _c = BscConnector::default();
    }
}
