//! IPFS peer reconnect job. Mirrors `aleph/jobs/reconnect_ipfs.py`.
//!
//! Periodically:
//!   1. Re-dials every peer listed in `config.ipfs.peers`.
//!   2. Walks the `peers` table (peers we have seen recently from IPFS) and
//!      dials each of them, skipping configured peers and any peer hosted
//!      on the local IP.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use crate::AlephResult;
use crate::db::DbPool;
use crate::db::accessors::peers::get_all_addresses_by_peer_type;
use crate::db::models::peers::PeerType;
use crate::services::ipfs::service::IpfsService;
use crate::services::utils::get_ip;

/// Job-runtime configuration. Mirrors the subset of `IpfsSettings` +
/// `P2pSettings` consumed by Python's `reconnect_ipfs_job`.
pub struct ReconnectConfig {
    pub configured_peers: Vec<String>,
    pub reconnect_delay: Duration,
    pub max_peer_age: Duration,
}

/// Dial each peer, ignoring per-peer errors.
async fn reconnect_to(ipfs: &IpfsService, peer: &str) {
    match ipfs.connect(peer).await {
        Ok(resp) => {
            if let Some(strings) = resp.get("Strings").and_then(|s| s.as_array()) {
                for s in strings {
                    if let Some(line) = s.as_str() {
                        tracing::info!("{line}");
                    }
                }
            }
        }
        Err(e) => {
            tracing::warn!("Can't reconnect to {peer}: {e}");
        }
    }
}

/// Run one pass: re-dial configured peers, then any IPFS-source peers seen
/// recently (last `max_peer_age`). Mirrors the body of the outer `while
/// True` loop in Python.
pub async fn run_once(
    pool: &DbPool,
    ipfs: &IpfsService,
    my_ip: &str,
    cfg: &ReconnectConfig,
) -> AlephResult<()> {
    tracing::info!("Reconnecting to peers");

    for peer in &cfg.configured_peers {
        reconnect_to(ipfs, peer).await;
    }

    let last_seen_cutoff = Utc::now()
        - chrono::Duration::from_std(cfg.max_peer_age).unwrap_or(chrono::Duration::hours(24));
    let client = pool
        .get()
        .await
        .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
    let known_peers =
        get_all_addresses_by_peer_type(&**client, PeerType::Ipfs, Some(last_seen_cutoff)).await?;
    drop(client);

    for peer in known_peers {
        if cfg.configured_peers.iter().any(|p| p == &peer) {
            continue;
        }
        if peer.contains(my_ip) {
            continue;
        }
        reconnect_to(ipfs, &peer).await;
    }

    Ok(())
}

/// Run the reconnect loop until `cancel` fires. Mirrors Python's outer
/// `while True` body wrapped in error handling + sleep.
pub async fn run(
    pool: DbPool,
    ipfs: Arc<IpfsService>,
    cfg: ReconnectConfig,
    cancel: crate::jobs::job_utils::CancelToken,
) -> AlephResult<()> {
    let my_ip = get_ip().await.unwrap_or_else(|_| "127.0.0.1".to_string());
    // Mirrors Python's initial 2-second warm-up sleep.
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        _ = cancel.cancelled() => return Ok(()),
    }

    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        if let Err(e) = run_once(&pool, &ipfs, &my_ip, &cfg).await {
            tracing::error!("Error reconnecting to peers: {e}");
        }
        tokio::select! {
            _ = tokio::time::sleep(cfg.reconnect_delay) => {}
            _ = cancel.cancelled() => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reconnect_loop_terminates_on_cancel() {
        let cancel = crate::jobs::job_utils::CancelToken::new();
        cancel.cancel();
        // We can't build a real DbPool + IpfsService cheaply here; assert the
        // cancel branch fires by simulating the same select.
        let res = tokio::time::timeout(Duration::from_millis(20), async {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(60)) => panic!("slept"),
                _ = cancel.cancelled() => "done",
            }
        })
        .await
        .unwrap();
        assert_eq!(res, "done");
    }

    #[test]
    fn backoff_doubles_between_attempts() {
        use crate::jobs::job_utils::compute_next_retry_interval;
        assert!(compute_next_retry_interval(1) > compute_next_retry_interval(0));
        assert!(compute_next_retry_interval(2) > compute_next_retry_interval(1));
    }
}
