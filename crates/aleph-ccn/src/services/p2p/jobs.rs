//! Peer-discovery background jobs. Mirrors `aleph/services/p2p/jobs.py`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use deadpool_postgres::Pool;
use tokio::task::JoinHandle;

use super::http::api_get_request;
use super::peers::connect_peer;
use super::protocol::AlephP2PClient;
use crate::AlephError;
use crate::AlephResult;
use crate::config::P2pSettings;
use crate::db::accessors::peers::get_all_addresses_by_peer_type;
use crate::db::models::peers::PeerType;

/// Health/status of a single HTTP peer. Mirrors the `PeerStatus` dataclass.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerStatus {
    pub peer_uri: String,
    pub is_online: bool,
    pub version: Option<serde_json::Value>,
}

/// Trait abstraction over the small slice of `NodeCache` that the HTTP
/// peer-tidy job uses. Lets us test the loop without a Redis dependency.
#[async_trait::async_trait]
pub trait ApiServerCache: Send + Sync {
    async fn has_api_server(&self, uri: &str) -> AlephResult<bool>;
    async fn add_api_server(&self, uri: &str) -> AlephResult<()>;
    async fn remove_api_server(&self, uri: &str) -> AlephResult<()>;
}

/// Read-only view over the API-server set. Used by [`crate::storage::
/// StorageService`] to discover peers when `p2p.clients` includes `"http"`.
/// Mirrors `NodeCache.get_api_servers()` (Redis `SMEMBERS api_servers`).
#[async_trait::async_trait]
pub trait ApiServerLookup: Send + Sync {
    async fn get_api_servers(&self) -> AlephResult<Vec<String>>;
}

/// Reconnect to bootstrap + DB peers on a fixed cadence. Mirrors
/// `reconnect_p2p_job`.
///
/// Spawn this with `tokio::spawn(reconnect_peers_task(...))`.
pub async fn reconnect_peers_task<C, F>(
    p2p_settings: P2pSettings,
    pool: Pool,
    p2p_client: Arc<C>,
    mut should_continue: F,
) where
    C: AlephP2PClient + 'static,
    F: FnMut() -> bool + Send + 'static,
{
    tokio::time::sleep(Duration::from_secs(2)).await;
    let max_peer_age = chrono::Duration::seconds(p2p_settings.max_peer_age as i64);
    while should_continue() {
        if let Err(e) = reconnect_once(&p2p_settings, &pool, &*p2p_client, max_peer_age).await {
            tracing::error!("Error reconnecting to peers: {e}");
        }
        tokio::time::sleep(Duration::from_secs(p2p_settings.reconnect_delay)).await;
    }
}

/// One iteration of the reconnect loop. Extracted so tests can exercise it
/// without sleeping.
pub async fn reconnect_once<C: AlephP2PClient>(
    p2p_settings: &P2pSettings,
    pool: &Pool,
    p2p_client: &C,
    max_peer_age: chrono::Duration,
) -> AlephResult<()> {
    let mut peers: HashSet<String> = p2p_settings.peers.iter().cloned().collect();
    let last_seen = Utc::now() - max_peer_age;
    let client = pool
        .get()
        .await
        .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
    let db_peers =
        get_all_addresses_by_peer_type(&**client, PeerType::P2p, Some(last_seen)).await?;
    peers.extend(db_peers);

    for peer in peers {
        if let Err(e) = connect_peer(p2p_client, &peer).await {
            tracing::debug!("Can't reconnect to {peer}: {e}");
        }
    }
    Ok(())
}

/// Convenience: spawn [`reconnect_peers_task`] and return its handle. Matches
/// the "returns a JoinHandle" convention noted in the task brief.
pub fn spawn_reconnect_peers<C: AlephP2PClient + 'static>(
    p2p_settings: P2pSettings,
    pool: Pool,
    p2p_client: Arc<C>,
) -> JoinHandle<()> {
    tokio::spawn(reconnect_peers_task(p2p_settings, pool, p2p_client, || {
        true
    }))
}

/// Health-check a single HTTP peer. Mirrors `check_peer`.
pub async fn check_peer(peer_uri: &str, timeout: Duration) -> PeerStatus {
    match api_get_request(peer_uri, "version", timeout).await {
        Some(v) => PeerStatus {
            peer_uri: peer_uri.to_string(),
            is_online: true,
            version: Some(v),
        },
        None => PeerStatus {
            peer_uri: peer_uri.to_string(),
            is_online: false,
            version: None,
        },
    }
}

/// Periodically prune unreachable HTTP peers from the API-server cache.
/// Mirrors `tidy_http_peers_job`.
pub async fn tidy_http_peers_task<F>(
    p2p_settings: P2pSettings,
    pool: Pool,
    cache: Arc<dyn ApiServerCache>,
    mut should_continue: F,
) where
    F: FnMut() -> bool + Send + 'static,
{
    let my_ip = match crate::services::utils::get_ip().await {
        Ok(ip) => ip,
        Err(_) => String::new(),
    };
    tokio::time::sleep(Duration::from_secs(2)).await;

    while should_continue() {
        if let Err(e) = tidy_http_peers_once(&pool, cache.as_ref(), &my_ip).await {
            tracing::error!("Error tidying HTTP peers: {e}");
        }
        tokio::time::sleep(Duration::from_secs(p2p_settings.reconnect_delay)).await;
    }
}

pub async fn tidy_http_peers_once(
    pool: &Pool,
    cache: &dyn ApiServerCache,
    my_ip: &str,
) -> AlephResult<()> {
    let client = pool
        .get()
        .await
        .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
    let peers = get_all_addresses_by_peer_type(&**client, PeerType::Http, None).await?;
    drop(client);
    let timeout = Duration::from_secs(1);
    let mut statuses: Vec<PeerStatus> = Vec::new();
    for peer in peers {
        if !my_ip.is_empty() && peer.contains(my_ip) {
            continue;
        }
        statuses.push(check_peer(&peer, timeout).await);
    }
    for status in statuses {
        let in_cache = cache.has_api_server(&status.peer_uri).await?;
        if status.is_online {
            if !in_cache {
                cache.add_api_server(&status.peer_uri).await?;
            }
        } else if in_cache {
            cache.remove_api_server(&status.peer_uri).await?;
        }
    }
    Ok(())
}

pub fn spawn_tidy_http_peers(
    p2p_settings: P2pSettings,
    pool: Pool,
    cache: Arc<dyn ApiServerCache>,
) -> JoinHandle<()> {
    tokio::spawn(tidy_http_peers_task(p2p_settings, pool, cache, || true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[derive(Default)]
    struct MemCache {
        servers: Mutex<HashSet<String>>,
    }

    #[async_trait::async_trait]
    impl ApiServerCache for MemCache {
        async fn has_api_server(&self, uri: &str) -> AlephResult<bool> {
            Ok(self.servers.lock().unwrap().contains(uri))
        }
        async fn add_api_server(&self, uri: &str) -> AlephResult<()> {
            self.servers.lock().unwrap().insert(uri.to_string());
            Ok(())
        }
        async fn remove_api_server(&self, uri: &str) -> AlephResult<()> {
            self.servers.lock().unwrap().remove(uri);
            Ok(())
        }
    }

    #[tokio::test]
    async fn check_peer_returns_online_for_200() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/version"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#""1.0""#))
            .expect(1)
            .mount(&server)
            .await;
        let status = check_peer(&server.uri(), Duration::from_secs(2)).await;
        assert!(status.is_online);
        assert!(status.version.is_some());
    }

    #[tokio::test]
    async fn check_peer_returns_offline_for_500() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/version"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let status = check_peer(&server.uri(), Duration::from_secs(2)).await;
        assert!(!status.is_online);
        assert!(status.version.is_none());
    }

    #[tokio::test]
    async fn check_peer_offline_when_url_invalid() {
        let status = check_peer("http://0.0.0.0:1", Duration::from_millis(200)).await;
        assert!(!status.is_online);
    }

    #[tokio::test]
    async fn mem_cache_round_trips_api_servers() {
        let cache = MemCache::default();
        assert!(!cache.has_api_server("u1").await.unwrap());
        cache.add_api_server("u1").await.unwrap();
        assert!(cache.has_api_server("u1").await.unwrap());
        cache.remove_api_server("u1").await.unwrap();
        assert!(!cache.has_api_server("u1").await.unwrap());
    }
}
