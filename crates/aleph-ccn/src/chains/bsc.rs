//! BSC connector. Mirrors `aleph/chains/bsc.py`.
//!
//! BSC uses the multichain indexer for chain events (no on-chain log
//! polling), and EVM signatures for verification — so `BscConnector` simply
//! wraps an `AlephIndexerReader` parameterised for the BSC blockchain.

use async_trait::async_trait;

use super::abc::ChainReader;
use super::indexer_reader::AlephIndexerReader;
use crate::AlephResult;
use crate::config::Settings;
use crate::types::chain_sync::ChainEventType;
use aleph_types::chain::Chain;

/// BSC reader-only connector (BSC packing is delegated to ETH for now in pyaleph).
pub struct BscConnector {
    indexer_reader: AlephIndexerReader,
}

impl BscConnector {
    pub fn new() -> Self {
        Self {
            indexer_reader: AlephIndexerReader::new(Chain::Bsc),
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
