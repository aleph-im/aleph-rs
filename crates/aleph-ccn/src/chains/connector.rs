//! Chain connector orchestration. Mirrors `aleph/chains/connector.py`.
//!
//! Owns the per-chain reader/writer tasks and fans them out at startup.
//! Each task auto-restarts on failure, mirroring `chain_reader_task` /
//! `chain_writer_task` in pyaleph.

use std::sync::Arc;
use std::time::Duration;

use aleph_types::chain::Chain;
use tokio::task::JoinSet;

use super::abc::{ChainReader, ChainWriter};
use super::bsc::BscConnector;
use super::chain_data_service::{ChainDataService, PendingTxPublisher, TracingPendingTxSink};
use super::ethereum::EthereumConnector;
use super::nuls2::Nuls2Connector;
use super::tezos::TezosConnector;
use crate::AlephResult;
use crate::config::Settings;
use crate::db::DbPool;

/// Top-level orchestrator over the per-chain readers/writers.
///
/// `aleph_types::chain::Chain` does not implement `Hash`/`Ord`; the small
/// number of supported chains means a `Vec<(Chain, ...)>` lookup is fine.
pub struct ChainConnector {
    readers: Vec<(Chain, Arc<dyn ChainReader>)>,
    writers: Vec<(Chain, Arc<dyn ChainWriter>)>,
}

impl ChainConnector {
    pub fn new() -> Self {
        Self {
            readers: Vec::new(),
            writers: Vec::new(),
        }
    }

    /// Constructs the connector from the runtime settings, mirroring
    /// `ChainConnector.new` + `_register_chains` in pyaleph.
    ///
    /// Each per-chain connector is registered regardless of `enabled` state —
    /// `start_all` filters by the enabled flag at run time. This matches the
    /// pyaleph behaviour where `_register_chains` populates a dict and
    /// `chain_event_loop` checks the flags.
    pub async fn from_settings(cfg: &Settings) -> AlephResult<Self> {
        let publisher = Arc::new(PendingTxPublisher::new(Box::new(TracingPendingTxSink)));
        let chain_data_service = Arc::new(ChainDataService::new());
        Self::from_settings_with_services(cfg, None, chain_data_service, publisher).await
    }

    /// Full constructor used by the live runtime — wires in the DB pool +
    /// shared chain-data service + pending-tx publisher.
    pub async fn from_settings_with_services(
        cfg: &Settings,
        pool: Option<DbPool>,
        chain_data_service: Arc<ChainDataService>,
        pending_tx_publisher: Arc<PendingTxPublisher>,
    ) -> AlephResult<Self> {
        let mut me = Self::new();

        if cfg.bsc.enabled {
            let bsc = if let Some(p) = pool.clone() {
                BscConnector::with_services(p, pending_tx_publisher.clone())
            } else {
                BscConnector::default()
            };
            me.readers.push((Chain::Bsc, Arc::new(bsc)));
        }

        // The Ethereum connector implements both reader and writer; construct
        // it only when the operator enabled reading or packing, because its
        // live constructor requires Ethereum RPC/contract settings.
        if cfg.ethereum.enabled || cfg.ethereum.packing_node {
            let eth_base = EthereumConnector::new(cfg).await?;
            let eth = if let Some(p) = pool.clone() {
                eth_base.with_services(p, chain_data_service.clone(), pending_tx_publisher.clone())
            } else {
                eth_base
            };
            let eth: Arc<EthereumConnector> = Arc::new(eth);
            me.readers
                .push((Chain::Ethereum, eth.clone() as Arc<dyn ChainReader>));
            me.writers
                .push((Chain::Ethereum, eth as Arc<dyn ChainWriter>));
        }

        if cfg.tezos.enabled {
            let tezos_base = TezosConnector::new(pending_tx_publisher.clone());
            let tezos = if let Some(p) = pool.clone() {
                tezos_base.with_db(p)
            } else {
                tezos_base
            };
            let tezos = Arc::new(tezos);
            me.readers
                .push((Chain::Tezos, tezos as Arc<dyn ChainReader>));
        }

        if cfg.nuls2.enabled || cfg.nuls2.packing_node {
            let nuls2_base = Nuls2Connector::new(pending_tx_publisher, chain_data_service);
            let nuls2 = if let Some(p) = pool {
                nuls2_base.with_db(p)
            } else {
                nuls2_base
            };
            let nuls2: Arc<Nuls2Connector> = Arc::new(nuls2);
            me.readers
                .push((Chain::Nuls2, nuls2.clone() as Arc<dyn ChainReader>));
            me.writers
                .push((Chain::Nuls2, nuls2 as Arc<dyn ChainWriter>));
        }

        Ok(me)
    }

    pub fn add_reader(&mut self, chain: Chain, reader: Arc<dyn ChainReader>) {
        self.readers.push((chain, reader));
    }

    pub fn add_writer(&mut self, chain: Chain, writer: Arc<dyn ChainWriter>) {
        self.writers.push((chain, writer));
    }

    fn find_reader(&self, chain: &Chain) -> Option<Arc<dyn ChainReader>> {
        self.readers
            .iter()
            .find_map(|(c, r)| (c == chain).then(|| r.clone()))
    }

    fn find_writer(&self, chain: &Chain) -> Option<Arc<dyn ChainWriter>> {
        self.writers
            .iter()
            .find_map(|(c, w)| (c == chain).then(|| w.clone()))
    }

    /// Runs `fetcher` for the given chain with auto-restart on error.
    pub async fn chain_reader_task(&self, chain: Chain, cfg: &Settings) -> AlephResult<()> {
        let reader = self
            .find_reader(&chain)
            .ok_or_else(|| crate::AlephError::Chain(format!("no reader for {chain}")))?;
        loop {
            if let Err(e) = reader.fetcher(cfg).await {
                tracing::error!(%chain, error = %e, "chain reader task failed; restarting in 60s");
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    /// Runs `packer` for the given chain with auto-restart on error.
    pub async fn chain_writer_task(&self, chain: Chain, cfg: &Settings) -> AlephResult<()> {
        let writer = self
            .find_writer(&chain)
            .ok_or_else(|| crate::AlephError::Chain(format!("no writer for {chain}")))?;
        loop {
            if let Err(e) = writer.packer(cfg).await {
                tracing::error!(%chain, error = %e, "chain writer task failed; restarting in 10s");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }

    /// Spawn one reader/writer task per enabled chain. Mirrors
    /// `ChainConnector.chain_event_loop` in pyaleph.
    pub async fn start_all(self: Arc<Self>, cfg: Arc<Settings>) -> AlephResult<()> {
        let cancel = crate::jobs::job_utils::CancelToken::new();
        self.start_all_until_cancel(cfg, cancel).await
    }

    /// Cancellable variant used by the Rust runtime. Tasks are aborted on
    /// shutdown because individual chain fetchers/packers are long-running.
    pub async fn start_all_until_cancel(
        self: Arc<Self>,
        cfg: Arc<Settings>,
        cancel: crate::jobs::job_utils::CancelToken,
    ) -> AlephResult<()> {
        let mut tasks = JoinSet::<AlephResult<()>>::new();

        if cfg.bsc.enabled {
            let me = self.clone();
            let cfg2 = cfg.clone();
            tasks.spawn(async move {
                me.chain_reader_task(Chain::Bsc, &cfg2).await
            });
        }

        if cfg.ethereum.enabled {
            let me = self.clone();
            let cfg2 = cfg.clone();
            tasks.spawn(async move {
                me.chain_reader_task(Chain::Ethereum, &cfg2).await
            });
        }
        if cfg.ethereum.packing_node {
            let me = self.clone();
            let cfg2 = cfg.clone();
            tasks.spawn(async move {
                me.chain_writer_task(Chain::Ethereum, &cfg2).await
            });
        }

        if cfg.tezos.enabled {
            let me = self.clone();
            let cfg2 = cfg.clone();
            tasks.spawn(async move {
                me.chain_reader_task(Chain::Tezos, &cfg2).await
            });
        }

        if cfg.nuls2.enabled {
            let me = self.clone();
            let cfg2 = cfg.clone();
            tasks.spawn(async move {
                me.chain_reader_task(Chain::Nuls2, &cfg2).await
            });
        }
        if cfg.nuls2.packing_node {
            let me = self.clone();
            let cfg2 = cfg.clone();
            tasks.spawn(async move {
                me.chain_writer_task(Chain::Nuls2, &cfg2).await
            });
        }

        if tasks.is_empty() {
            cancel.cancelled().await;
            return Ok(());
        }

        tokio::select! {
            _ = cancel.cancelled() => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                Ok(())
            }
            joined = tasks.join_next() => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                match joined {
                    Some(Ok(result)) => result,
                    Some(Err(err)) => Err(crate::AlephError::Chain(format!("chain task join error: {err}"))),
                    None => Ok(()),
                }
            }
        }
    }
}

impl Default for ChainConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_default_has_no_readers() {
        let c = ChainConnector::default();
        assert!(c.readers.is_empty());
        assert!(c.writers.is_empty());
    }
}
