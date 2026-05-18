//! Periodic alive publication. Mirrors `aleph/services/peers/publish.py`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde_json::json;
use tokio::task::JoinHandle;

use crate::VERSION;
use crate::services::ipfs::IpfsService;
use crate::services::p2p::protocol::AlephP2PClient;

/// Publish our multiaddress periodically to both pubsub backends. Mirrors
/// `publish_host`.
///
/// The delay between announcements defaults to 120 seconds (matching the
/// Python signature). `use_ipfs=true` enables IPFS pubsub publication —
/// failures on either backend are logged but never abort the loop.
pub async fn publish_host<C: AlephP2PClient>(
    address: String,
    p2p_client: Arc<C>,
    ipfs_service: Arc<IpfsService>,
    p2p_alive_topic: String,
    ipfs_alive_topic: String,
    peer_type: String,
    use_ipfs: bool,
) {
    publish_host_with_delay(
        address,
        p2p_client,
        ipfs_service,
        p2p_alive_topic,
        ipfs_alive_topic,
        peer_type,
        use_ipfs,
        Duration::from_secs(120),
    )
    .await;
}

/// Same as [`publish_host`] but with a caller-controlled delay. Internal
/// helper used by tests.
pub async fn publish_host_with_delay<C: AlephP2PClient>(
    address: String,
    p2p_client: Arc<C>,
    ipfs_service: Arc<IpfsService>,
    p2p_alive_topic: String,
    ipfs_alive_topic: String,
    peer_type: String,
    use_ipfs: bool,
    delay: Duration,
) {
    tokio::time::sleep(Duration::from_secs(2)).await;
    let msg = json!({
        "address": address,
        "interests": serde_json::Value::Null,
        "peer_type": peer_type,
        "version": VERSION,
    });
    let payload = serde_json::to_vec(&msg).unwrap_or_default();
    loop {
        if use_ipfs {
            let payload_str = String::from_utf8_lossy(&payload).into_owned();
            let pub_fut = ipfs_service.pubsub_publish(&ipfs_alive_topic, &payload_str);
            match tokio::time::timeout(Duration::from_secs(1), pub_fut).await {
                Ok(Ok(())) => tracing::debug!("Published alive on ipfs pubsub"),
                Ok(Err(e)) => tracing::warn!("Can't publish alive on ipfs: {e}"),
                Err(_) => tracing::warn!("Can't publish alive on ipfs: timeout"),
            }
        }
        let pub_fut = p2p_client.publish(Bytes::from(payload.clone()), &p2p_alive_topic, false);
        match tokio::time::timeout(Duration::from_secs(10), pub_fut).await {
            Ok(Ok(())) => tracing::debug!("Published alive on p2p pubsub"),
            Ok(Err(e)) => tracing::warn!("Can't publish alive on p2p: {e}"),
            Err(_) => tracing::warn!("Can't publish alive on p2p: timeout"),
        }
        tokio::time::sleep(delay).await;
    }
}

/// Spawn [`publish_host`] and return its handle.
pub fn spawn_publish_host<C: AlephP2PClient + 'static>(
    address: String,
    p2p_client: Arc<C>,
    ipfs_service: Arc<IpfsService>,
    p2p_alive_topic: String,
    ipfs_alive_topic: String,
    peer_type: String,
    use_ipfs: bool,
) -> JoinHandle<()> {
    tokio::spawn(publish_host(
        address,
        p2p_client,
        ipfs_service,
        p2p_alive_topic,
        ipfs_alive_topic,
        peer_type,
        use_ipfs,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IpfsSettings;
    use crate::services::ipfs::IpfsService;
    use crate::services::p2p::protocol::{Identify, MockP2pClient};

    #[tokio::test]
    async fn publish_host_emits_on_p2p_pubsub() {
        let server = wiremock::MockServer::start().await;
        use wiremock::matchers::method;
        wiremock::Mock::given(method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let url = url::Url::parse(&server.uri()).unwrap();
        let mut s = IpfsSettings::default();
        s.host = url.host_str().unwrap().to_string();
        s.port = url.port().unwrap();
        s.scheme = "http".into();
        let ipfs = Arc::new(IpfsService::new(&s).unwrap());

        let p2p = Arc::new(MockP2pClient::new(Identify {
            peer_id: "QmSelf".into(),
        }));

        // Bound the loop to one tick by aborting after a short delay.
        let p2p_clone = p2p.clone();
        let handle = tokio::spawn(publish_host_with_delay(
            "/ip4/1.2.3.4/tcp/4025/p2p/QmSelf".into(),
            p2p_clone,
            ipfs,
            "ALIVE".into(),
            "ALEPH_ALIVE".into(),
            "P2P".into(),
            false,
            Duration::from_secs(60),
        ));
        tokio::time::sleep(Duration::from_millis(2500)).await;
        handle.abort();

        let pub_log = p2p.published();
        assert!(!pub_log.is_empty(), "should have published at least once");
        let (topic, body, loopback) = &pub_log[0];
        assert_eq!(topic, "ALIVE");
        assert!(!loopback);
        let value: serde_json::Value = serde_json::from_slice(body.as_ref()).unwrap();
        assert_eq!(
            value.get("address").and_then(serde_json::Value::as_str),
            Some("/ip4/1.2.3.4/tcp/4025/p2p/QmSelf")
        );
        assert_eq!(
            value.get("peer_type").and_then(serde_json::Value::as_str),
            Some("P2P")
        );
    }

    #[tokio::test]
    async fn publish_host_handles_failing_publish() {
        let server = wiremock::MockServer::start().await;
        use wiremock::matchers::method;
        wiremock::Mock::given(method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let url = url::Url::parse(&server.uri()).unwrap();
        let mut s = IpfsSettings::default();
        s.host = url.host_str().unwrap().to_string();
        s.port = url.port().unwrap();
        s.scheme = "http".into();
        let ipfs = Arc::new(IpfsService::new(&s).unwrap());
        let p2p = Arc::new(MockP2pClient::new(Identify {
            peer_id: "QmSelf".into(),
        }));
        let handle = tokio::spawn(publish_host_with_delay(
            "address".into(),
            p2p.clone(),
            ipfs,
            "ALIVE".into(),
            "ALEPH_ALIVE".into(),
            "P2P".into(),
            true,
            Duration::from_secs(60),
        ));
        tokio::time::sleep(Duration::from_millis(2500)).await;
        handle.abort();
        // P2P publish itself still succeeds (mock client always returns Ok).
        assert!(!p2p.published().is_empty());
    }
}
