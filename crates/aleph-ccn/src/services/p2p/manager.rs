//! Boot-time host wiring. Mirrors `aleph/services/p2p/manager.py`.
//!
//! Sets up the long-running tasks that announce the node to peers and consume
//! the alive topic. The Python function returns a list of coroutines that
//! the caller schedules with `asyncio.gather`. The Rust equivalent returns a
//! list of `JoinHandle<()>`s — one per spawned task — so callers can `await`
//! them all (or `select` on `Ctrl+C`).

use std::sync::Arc;
use std::time::Instant;

use deadpool_postgres::Pool;
use tokio::task::JoinHandle;

use super::jobs::{ApiServerCache, spawn_reconnect_peers, spawn_tidy_http_peers};
use super::protocol::AlephP2PClient;
use crate::AlephResult;
use crate::config::Settings;
use crate::services::ipfs::IpfsService;
use crate::services::peers::allowlist::PeerAllowlist;
use crate::services::peers::monitor::{spawn_monitor_hosts_ipfs, spawn_monitor_hosts_p2p};
use crate::services::peers::publish::spawn_publish_host;

/// `initialize_host` Rust port. Spawns the announcement + monitoring loops
/// and returns the resulting `JoinHandle`s.
///
/// Behaviour parity with the Python version is preserved:
///   - `reconnect_p2p_job` and `tidy_http_peers_job` always run.
///   - When `listen` is true, the node identifies itself, computes its
///     public address, and additionally spawns `publish_host` (P2P + HTTP)
///     and `monitor_hosts_p2p`.
///   - When IPFS is enabled, the function additionally publishes the
///     IPFS multiaddress and monitors the IPFS alive topic.
pub async fn initialize_host<C: AlephP2PClient + 'static>(
    config: &Settings,
    pool: Pool,
    p2p_client: Arc<C>,
    ipfs_service: Arc<IpfsService>,
    peer_allowlist: Arc<PeerAllowlist>,
    cache: Arc<dyn ApiServerCache>,
    host: &str,
    port: u16,
    listen: bool,
) -> AlephResult<Vec<JoinHandle<()>>> {
    let transport_opt = format!("/ip4/{host}/tcp/{port}");
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();

    tasks.push(spawn_reconnect_peers(
        config.p2p.clone(),
        pool.clone(),
        p2p_client.clone(),
    ));
    tasks.push(spawn_tidy_http_peers(
        config.p2p.clone(),
        pool.clone(),
        cache.clone(),
    ));

    if !listen {
        return Ok(tasks);
    }

    let start_time = Instant::now();
    let peer_id = p2p_client.identify().await?.peer_id;
    tracing::info!(
        "Got identify info in {:.3} seconds",
        start_time.elapsed().as_secs_f64()
    );
    tracing::info!("Listening on {transport_opt}/p2p/{peer_id}");

    let start_time = Instant::now();
    let ip = crate::services::utils::get_ip().await.unwrap_or_default();
    tracing::info!(
        "Got IP info in {:.3} seconds",
        start_time.elapsed().as_secs_f64()
    );

    let public_address = format!("/ip4/{ip}/tcp/{port}/p2p/{peer_id}");
    let http_port = config.p2p.http_port;
    let public_http_address = format!("http://{ip}:{http_port}");
    tracing::info!("Probable public on {public_address}");

    tasks.push(spawn_publish_host(
        public_address.clone(),
        p2p_client.clone(),
        ipfs_service.clone(),
        config.p2p.alive_topic.clone(),
        config.ipfs.alive_topic.clone(),
        "P2P".into(),
        config.ipfs.enabled,
    ));
    tasks.push(spawn_publish_host(
        public_http_address.clone(),
        p2p_client.clone(),
        ipfs_service.clone(),
        config.p2p.alive_topic.clone(),
        config.ipfs.alive_topic.clone(),
        "HTTP".into(),
        config.ipfs.enabled,
    ));
    tasks.push(spawn_monitor_hosts_p2p(
        p2p_client.clone(),
        pool.clone(),
        peer_allowlist.clone(),
        config.p2p.alive_topic.clone(),
    ));

    if config.ipfs.enabled {
        tasks.push(spawn_monitor_hosts_ipfs(
            ipfs_service.clone(),
            pool.clone(),
            peer_allowlist.clone(),
            config.ipfs.alive_topic.clone(),
        ));
        match ipfs_service.get_public_address().await {
            Ok(public_ipfs_address) => {
                tasks.push(spawn_publish_host(
                    public_ipfs_address,
                    p2p_client.clone(),
                    ipfs_service.clone(),
                    config.p2p.alive_topic.clone(),
                    config.ipfs.alive_topic.clone(),
                    "IPFS".into(),
                    true,
                ));
            }
            Err(e) => {
                tracing::error!("Can't publish public IPFS address: {e}");
            }
        }
    }

    Ok(tasks)
}

#[cfg(test)]
mod tests {
    #[test]
    fn transport_opt_format_matches_python() {
        let host = "0.0.0.0";
        let port: u16 = 4025;
        assert_eq!(format!("/ip4/{host}/tcp/{port}"), "/ip4/0.0.0.0/tcp/4025");
    }

    #[test]
    fn public_address_format() {
        let ip = "1.2.3.4";
        let port: u16 = 4025;
        let peer_id = "QmABC";
        assert_eq!(
            format!("/ip4/{ip}/tcp/{port}/p2p/{peer_id}"),
            "/ip4/1.2.3.4/tcp/4025/p2p/QmABC"
        );
    }
}
