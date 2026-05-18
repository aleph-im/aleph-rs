//! Peer allowlist. Mirrors `aleph/services/peers/allowlist.py`.
//!
//! Bootstraps from `config.p2p.peers` and merges with peer ids advertised in
//! the `corechannel` aggregate. The merged set is cached for `cache_ttl`
//! seconds.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use tokio::sync::Mutex;

use crate::AlephError;
use crate::AlephResult;
use crate::config::Settings;
use crate::db::accessors::aggregates::get_aggregate_by_key;

pub const CORECHANNEL_KEY: &str = "corechannel";
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(300);

/// Extract the peer id from a multiaddress ending in `/p2p/<peer_id>`. Mirrors
/// the private `_extract_peer_id` in Python.
pub fn extract_peer_id(multiaddress: &str) -> Option<String> {
    let parts: Vec<&str> = multiaddress.split("/p2p/").collect();
    if parts.len() != 2 || parts[1].is_empty() {
        return None;
    }
    Some(parts[1].split('/').next().unwrap_or("").to_string())
}

#[derive(Default)]
struct AllowlistCache {
    ccn_peer_ids: HashSet<String>,
    last_refresh: Option<Instant>,
}

pub struct PeerAllowlist {
    pool: Pool,
    bootstrap_peer_ids: HashSet<String>,
    corechannel_address: String,
    cache_ttl: Duration,
    cache: Mutex<AllowlistCache>,
}

impl PeerAllowlist {
    pub fn new(
        pool: Pool,
        bootstrap_peer_ids: HashSet<String>,
        corechannel_address: String,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            pool,
            bootstrap_peer_ids,
            corechannel_address,
            cache_ttl,
            cache: Mutex::new(AllowlistCache::default()),
        }
    }

    /// Build an allowlist from typed settings. Mirrors `from_config`.
    pub fn from_config(config: &Settings, pool: Pool) -> Arc<Self> {
        let mut bootstrap_peer_ids = HashSet::new();
        for peer_maddr in &config.p2p.peers {
            if let Some(peer_id) = extract_peer_id(peer_maddr) {
                bootstrap_peer_ids.insert(peer_id);
            }
        }
        tracing::info!(
            "Peer allowlist initialized with {} bootstrap peers",
            bootstrap_peer_ids.len()
        );
        Arc::new(Self::new(
            pool,
            bootstrap_peer_ids,
            config.aleph.corechannel.address.clone(),
            Duration::from_secs(config.aleph.corechannel.cache_ttl),
        ))
    }

    /// Pull the corechannel aggregate's peer ids out of postgres. Mirrors
    /// `_refresh_ccn_peer_ids` but propagates DB errors instead of swallowing
    /// them (callers usually log and reuse the previous cache).
    async fn refresh_ccn_peer_ids(&self) -> AlephResult<HashSet<String>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
        let aggregate =
            get_aggregate_by_key(&**client, &self.corechannel_address, CORECHANNEL_KEY, true)
                .await?;
        let Some(aggregate) = aggregate else {
            return Ok(HashSet::new());
        };
        let mut peer_ids = HashSet::new();
        if let Some(nodes) = aggregate.content.get("nodes").and_then(|n| n.as_array()) {
            for node in nodes {
                if let Some(maddr) = node.get("multiaddress").and_then(|v| v.as_str()) {
                    if let Some(peer_id) = extract_peer_id(maddr) {
                        peer_ids.insert(peer_id);
                    }
                }
            }
        }
        tracing::info!(
            "Loaded {} CCN peer IDs from corechannel aggregate",
            peer_ids.len()
        );
        Ok(peer_ids)
    }

    async fn ensure_cache_fresh(&self) {
        let mut guard = self.cache.lock().await;
        let stale = match guard.last_refresh {
            Some(t) => t.elapsed() > self.cache_ttl,
            None => true,
        };
        if stale {
            match self.refresh_ccn_peer_ids().await {
                Ok(set) => guard.ccn_peer_ids = set,
                Err(e) => tracing::warn!("Failed to load corechannel aggregate: {e}"),
            }
            guard.last_refresh = Some(Instant::now());
        }
    }

    /// Return `true` when the peer id is either bootstrapped or present in
    /// the cached corechannel aggregate. Mirrors `PeerAllowlist.is_allowed`.
    pub async fn is_allowed(&self, peer_id: &str) -> bool {
        if self.bootstrap_peer_ids.contains(peer_id) {
            return true;
        }
        self.ensure_cache_fresh().await;
        let guard = self.cache.lock().await;
        guard.ccn_peer_ids.contains(peer_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::time::Duration;
    use tokio_postgres::NoTls;

    // We can't easily spin postgres up here; instead, we run the
    // bootstrap-only path which never queries the DB.
    fn dummy_pool() -> Pool {
        use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
        let mut cfg = tokio_postgres::Config::new();
        cfg.host("127.0.0.1").port(1).user("nobody");
        let mgr = Manager::from_config(
            cfg,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        Pool::builder(mgr).max_size(1).build().unwrap()
    }

    #[test]
    fn extract_peer_id_round_trip() {
        assert_eq!(
            extract_peer_id("/ip4/1.2.3.4/tcp/4025/p2p/QmZkurbY2"),
            Some("QmZkurbY2".to_string())
        );
        assert_eq!(extract_peer_id("/ip4/1.2.3.4/tcp/4025"), None);
        assert_eq!(extract_peer_id("/p2p/"), None);
    }

    #[tokio::test]
    async fn bootstrap_peers_are_always_allowed() {
        let mut peer_ids = HashSet::new();
        peer_ids.insert("QmBoot".to_string());
        let allowlist = PeerAllowlist::new(
            dummy_pool(),
            peer_ids,
            "0xabc".into(),
            Duration::from_secs(300),
        );
        assert!(allowlist.is_allowed("QmBoot").await);
    }

    #[tokio::test]
    async fn unknown_peer_blocked_without_db() {
        let allowlist = PeerAllowlist::new(
            dummy_pool(),
            HashSet::new(),
            "0xabc".into(),
            Duration::from_secs(300),
        );
        // Bootstrap empty + db unreachable → not allowed.
        assert!(!allowlist.is_allowed("QmOther").await);
    }
}
