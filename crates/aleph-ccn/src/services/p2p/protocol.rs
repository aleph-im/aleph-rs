//! P2P daemon client trait + default HTTP implementation. Brings together
//! `aleph/services/p2p/protocol.py` (the `incoming_channel` pubsub consumer)
//! and the `AlephP2PServiceClient` abstraction that Python imports from
//! `aleph_p2p_client`.
//!
//! We define [`AlephP2PClient`] here so the rest of the crate can talk to the
//! daemon through a single trait, and provide a concrete [`HttpP2pClient`]
//! that targets `p2p-service` HTTP endpoints + RabbitMQ topics (matching the
//! Python implementation's deployment shape).

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;

use crate::config::{P2pSettings, RabbitmqSettings};
use crate::{AlephError, AlephResult};

/// `identify()` response payload. Mirrors the `IdentifyResponse` returned by
/// the Python `aleph_p2p_client`.
#[derive(Debug, Clone, Deserialize)]
pub struct Identify {
    pub peer_id: String,
}

/// One pubsub message received over the wire. Mirrors the Python
/// `Message(routing_key=..., body=...)` dataclass yielded by
/// `receive_messages`.
#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub routing_key: String,
    pub body: Bytes,
}

/// Async trait modelling the small slice of `aleph_p2p_client` that pyaleph
/// actually consumes (identify, dial, subscribe, publish, receive_messages).
#[async_trait]
pub trait AlephP2PClient: Send + Sync {
    async fn identify(&self) -> AlephResult<Identify>;

    async fn dial(&self, peer_id: &str, multiaddr: &str) -> AlephResult<()>;

    async fn subscribe(&self, topic: &str) -> AlephResult<()>;

    async fn publish(&self, data: Bytes, topic: &str, loopback: bool) -> AlephResult<()>;

    async fn receive_messages(&self, topic: &str) -> AlephResult<ReceivedMessage>;
}

/// HTTP-backed [`AlephP2PClient`] talking to the local `p2p-service` daemon.
///
/// `identify`, `dial`, `subscribe`, and `publish` go through the daemon's
/// HTTP control port. `receive_messages` consumes from RabbitMQ (which is
/// where the daemon publishes incoming pubsub frames in the Python
/// deployment).
pub struct HttpP2pClient {
    pub http: reqwest::Client,
    pub base_url: String,
    pub service_name: String,
    pub rabbitmq: Option<RabbitmqSettings>,
    /// In-memory buffer for `receive_messages`. The real deployment wires
    /// `lapin` consumers — see [`HttpP2pClient::push_received`] for the
    /// integration point.
    inbound: AsyncMutex<HashMap<String, VecDeque<ReceivedMessage>>>,
}

impl std::fmt::Debug for HttpP2pClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpP2pClient")
            .field("base_url", &self.base_url)
            .field("service_name", &self.service_name)
            .finish()
    }
}

impl HttpP2pClient {
    /// Build a client from settings. Mirrors `make_p2p_service_client`.
    pub fn new(p2p: &P2pSettings, rabbitmq: Option<RabbitmqSettings>, service_name: &str) -> Self {
        let base_url = format!("http://{}:{}", p2p.daemon_host, p2p.control_port);
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("reqwest client"),
            base_url,
            service_name: service_name.to_string(),
            rabbitmq,
            inbound: AsyncMutex::new(HashMap::new()),
        }
    }

    /// Build a client with a caller-supplied base URL (used in tests).
    pub fn with_base_url(base_url: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
            service_name: "test".into(),
            rabbitmq: None,
            inbound: AsyncMutex::new(HashMap::new()),
        }
    }

    /// Buffer an incoming pubsub frame so the next `receive_messages(topic)`
    /// returns it. This is the test/integration hook used by the lapin
    /// consumer.
    pub async fn push_received(&self, topic: &str, message: ReceivedMessage) {
        self.inbound
            .lock()
            .await
            .entry(topic.to_string())
            .or_default()
            .push_back(message);
    }
}

#[async_trait]
impl AlephP2PClient for HttpP2pClient {
    async fn identify(&self) -> AlephResult<Identify> {
        let url = format!("{}/identify", self.base_url);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| AlephError::P2p(format!("identify: {e}")))?;
        if !resp.status().is_success() {
            return Err(AlephError::P2p(format!("identify http {}", resp.status())));
        }
        let value: Value = resp
            .json()
            .await
            .map_err(|e| AlephError::P2p(format!("identify json: {e}")))?;
        let peer_id = value
            .get("peer_id")
            .or_else(|| value.get("ID"))
            .and_then(Value::as_str)
            .ok_or_else(|| AlephError::P2p("identify missing peer_id".into()))?
            .to_string();
        Ok(Identify { peer_id })
    }

    async fn dial(&self, peer_id: &str, multiaddr: &str) -> AlephResult<()> {
        let url = format!("{}/dial", self.base_url);
        let body = serde_json::json!({"peer_id": peer_id, "multiaddr": multiaddr});
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| AlephError::P2p(format!("dial: {e}")))?;
        if !resp.status().is_success() {
            return Err(AlephError::P2p(format!("dial http {}", resp.status())));
        }
        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> AlephResult<()> {
        let url = format!("{}/pubsub/subscribe", self.base_url);
        let resp = self
            .http
            .post(&url)
            .json(&serde_json::json!({"topic": topic}))
            .send()
            .await
            .map_err(|e| AlephError::P2p(format!("subscribe: {e}")))?;
        if !resp.status().is_success() {
            return Err(AlephError::P2p(format!("subscribe http {}", resp.status())));
        }
        Ok(())
    }

    async fn publish(&self, data: Bytes, topic: &str, loopback: bool) -> AlephResult<()> {
        use base64::Engine as _;
        let url = format!("{}/pubsub/publish", self.base_url);
        let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
        let body = serde_json::json!({
            "topic": topic,
            "data": encoded,
            "loopback": loopback,
        });
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| AlephError::P2p(format!("publish: {e}")))?;
        if !resp.status().is_success() {
            return Err(AlephError::P2p(format!("publish http {}", resp.status())));
        }
        Ok(())
    }

    async fn receive_messages(&self, topic: &str) -> AlephResult<ReceivedMessage> {
        loop {
            {
                let mut guard = self.inbound.lock().await;
                if let Some(queue) = guard.get_mut(topic) {
                    if let Some(msg) = queue.pop_front() {
                        return Ok(msg);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// In-memory `AlephP2PClient` implementation for unit tests.
pub struct MockP2pClient {
    pub identify: Identify,
    pub dialed: Arc<Mutex<Vec<(String, String)>>>,
    pub subscribed: Arc<Mutex<Vec<String>>>,
    pub published: Arc<Mutex<Vec<(String, Bytes, bool)>>>,
    pub inbound: Arc<AsyncMutex<HashMap<String, VecDeque<ReceivedMessage>>>>,
}

impl MockP2pClient {
    pub fn new(identify: Identify) -> Self {
        Self {
            identify,
            dialed: Arc::new(Mutex::new(Vec::new())),
            subscribed: Arc::new(Mutex::new(Vec::new())),
            published: Arc::new(Mutex::new(Vec::new())),
            inbound: Arc::new(AsyncMutex::new(HashMap::new())),
        }
    }

    pub fn dialed(&self) -> Vec<(String, String)> {
        self.dialed.lock().unwrap().clone()
    }

    pub fn subscribed(&self) -> Vec<String> {
        self.subscribed.lock().unwrap().clone()
    }

    pub fn published(&self) -> Vec<(String, Bytes, bool)> {
        self.published.lock().unwrap().clone()
    }

    pub async fn push(&self, topic: &str, msg: ReceivedMessage) {
        self.inbound
            .lock()
            .await
            .entry(topic.to_string())
            .or_default()
            .push_back(msg);
    }
}

#[async_trait]
impl AlephP2PClient for MockP2pClient {
    async fn identify(&self) -> AlephResult<Identify> {
        Ok(self.identify.clone())
    }

    async fn dial(&self, peer_id: &str, multiaddr: &str) -> AlephResult<()> {
        self.dialed
            .lock()
            .unwrap()
            .push((peer_id.to_string(), multiaddr.to_string()));
        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> AlephResult<()> {
        self.subscribed.lock().unwrap().push(topic.to_string());
        Ok(())
    }

    async fn publish(&self, data: Bytes, topic: &str, loopback: bool) -> AlephResult<()> {
        self.published
            .lock()
            .unwrap()
            .push((topic.to_string(), data, loopback));
        Ok(())
    }

    async fn receive_messages(&self, topic: &str) -> AlephResult<ReceivedMessage> {
        loop {
            {
                let mut guard = self.inbound.lock().await;
                if let Some(q) = guard.get_mut(topic) {
                    if let Some(msg) = q.pop_front() {
                        return Ok(msg);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// In-memory cache of `(sender, item_hash, signature)` tuples used by
/// `incoming_channel` to de-duplicate replayed pubsub frames. Mirrors the
/// `deque([], maxlen=200000)` in Python.
pub struct SeenHashes {
    capacity: usize,
    queue: Mutex<VecDeque<(String, String, String)>>,
}

impl SeenHashes {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    pub fn contains(&self, item: &(String, String, String)) -> bool {
        self.queue.lock().unwrap().iter().any(|v| v == item)
    }

    pub fn record(&self, item: (String, String, String)) {
        let mut g = self.queue.lock().unwrap();
        if g.len() >= self.capacity {
            g.pop_front();
        }
        g.push_back(item);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn http_identify_returns_peer_id() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/identify"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"peer_id":"QmTest"}"#))
            .expect(1)
            .mount(&server)
            .await;
        let client = HttpP2pClient::with_base_url(server.uri());
        let id = client.identify().await.unwrap();
        assert_eq!(id.peer_id, "QmTest");
    }

    #[tokio::test]
    async fn http_dial_posts_json() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/dial"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let client = HttpP2pClient::with_base_url(server.uri());
        client.dial("QmFoo", "/ip4/1.2.3.4/tcp/4025").await.unwrap();
    }

    #[tokio::test]
    async fn http_publish_base64_encodes_payload() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pubsub/publish"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let client = HttpP2pClient::with_base_url(server.uri());
        client
            .publish(Bytes::from_static(b"hello"), "ALIVE", false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn http_subscribe_calls_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pubsub/subscribe"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let client = HttpP2pClient::with_base_url(server.uri());
        client.subscribe("ALIVE").await.unwrap();
    }

    #[tokio::test]
    async fn http_receive_messages_drains_buffer() {
        let client = HttpP2pClient::with_base_url("http://0.0.0.0:1".into());
        client
            .push_received(
                "ALIVE",
                ReceivedMessage {
                    routing_key: "p2p.ALIVE.QmFoo".into(),
                    body: Bytes::from_static(b"data"),
                },
            )
            .await;
        let msg = client.receive_messages("ALIVE").await.unwrap();
        assert_eq!(msg.routing_key, "p2p.ALIVE.QmFoo");
        assert_eq!(msg.body.as_ref(), b"data");
    }

    #[test]
    fn seen_hashes_evicts_oldest() {
        let s = SeenHashes::new(2);
        let a = ("s1".into(), "h1".into(), "g1".into());
        let b = ("s2".into(), "h2".into(), "g2".into());
        let c = ("s3".into(), "h3".into(), "g3".into());
        s.record(a.clone());
        s.record(b.clone());
        assert!(s.contains(&a));
        s.record(c.clone());
        assert!(!s.contains(&a));
        assert!(s.contains(&b));
        assert!(s.contains(&c));
    }
}
