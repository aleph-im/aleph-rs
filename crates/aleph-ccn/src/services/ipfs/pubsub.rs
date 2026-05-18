//! Incoming IPFS pubsub channel. Mirrors `aleph/services/ipfs/pubsub.py`.
//!
//! Subscribes to an IPFS topic and forwards each decoded message to a
//! `MessagePublisher`-like callback. The Python port performs a direct call
//! to `decode_pubsub_message` and the message handler; here we keep the
//! decoding inline and surface the JSON payload to a generic callback so
//! the call-site (which we haven't ported yet) can do whatever it wants.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use serde_json::Value;
use tokio::sync::Mutex;

use super::service::IpfsService;
use crate::AlephResult;

/// Trait implemented by anything that can ingest a pubsub message. Equivalent
/// to `MessagePublisher.add_pending_message` in Python.
#[async_trait::async_trait]
pub trait IpfsPubsubHandler: Send + Sync {
    async fn handle(&self, message: Value) -> AlephResult<()>;
}

/// Subscribe to `topic` and dispatch decoded messages through `handler`.
/// Mirrors `aleph.services.ipfs.pubsub.incoming_channel`.
///
/// The function loops forever (matching the Python `while True:` shape):
/// each pubsub subscription failure triggers a 100 ms backoff and a
/// resubscribe attempt.
pub async fn incoming_channel(
    ipfs_service: Arc<IpfsService>,
    topic: String,
    handler: Arc<dyn IpfsPubsubHandler>,
) {
    let stop_after: Option<usize> = None;
    incoming_channel_with_limit(ipfs_service, topic, handler, stop_after).await;
}

/// Internal variant used by tests to bound the loop count.
async fn incoming_channel_with_limit(
    ipfs_service: Arc<IpfsService>,
    topic: String,
    handler: Arc<dyn IpfsPubsubHandler>,
    max_iterations: Option<usize>,
) {
    let mut iter = 0usize;
    loop {
        match ipfs_service.sub(&topic).await {
            Ok(mut stream) => {
                while let Some(msg) = stream.next().await {
                    match msg {
                        Ok(value) => {
                            if let Err(e) = handler.handle(value).await {
                                tracing::warn!("Invalid IPFS pubsub message: {e}");
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Bad pubsub frame: {e}");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!("Exception in IPFS pubsub, reconnecting in 100 ms... ({e})");
            }
        }

        iter += 1;
        if let Some(limit) = max_iterations {
            if iter >= limit {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// In-memory test helper: records every payload that goes through it.
#[derive(Default)]
pub struct RecordingHandler {
    pub messages: Mutex<Vec<Value>>,
}

#[async_trait::async_trait]
impl IpfsPubsubHandler for RecordingHandler {
    async fn handle(&self, message: Value) -> AlephResult<()> {
        self.messages.lock().await.push(message);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IpfsSettings;
    use crate::services::ipfs::service::IpfsService;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn service_for(server: &MockServer) -> IpfsService {
        let url = url::Url::parse(&server.uri()).unwrap();
        let mut s = IpfsSettings::default();
        s.host = url.host_str().unwrap().to_string();
        s.port = url.port().unwrap();
        s.scheme = "http".into();
        IpfsService::new(&s).unwrap()
    }

    #[tokio::test]
    async fn incoming_channel_dispatches_each_frame() {
        use base64::Engine as _;
        let engine = base64::engine::general_purpose::STANDARD;
        let server = MockServer::start().await;
        let payload = serde_json::json!({
            "from": engine.encode(b"peer1"),
            "data": engine.encode(b"hello"),
            "topicIDs": ["t"],
        });
        let ndjson = format!(
            "{}\n{}\n",
            serde_json::to_string(&payload).unwrap(),
            serde_json::to_string(&payload).unwrap()
        );
        Mock::given(method("POST"))
            .and(path("/api/v0/pubsub/sub"))
            .respond_with(ResponseTemplate::new(200).set_body_string(ndjson))
            .mount(&server)
            .await;

        let service = Arc::new(service_for(&server));
        let handler: Arc<RecordingHandler> = Arc::new(RecordingHandler::default());
        let h: Arc<dyn IpfsPubsubHandler> = handler.clone();
        // Bound the loop to 1 iteration so the test can join.
        incoming_channel_with_limit(service, "t".into(), h, Some(1)).await;
        assert_eq!(handler.messages.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn incoming_channel_recovers_on_subscribe_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pubsub/sub"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let service = Arc::new(service_for(&server));
        let handler: Arc<RecordingHandler> = Arc::new(RecordingHandler::default());
        let h: Arc<dyn IpfsPubsubHandler> = handler.clone();
        // Two iterations to exercise the reconnect path.
        incoming_channel_with_limit(service, "t".into(), h, Some(2)).await;
        assert!(handler.messages.lock().await.is_empty());
    }
}
