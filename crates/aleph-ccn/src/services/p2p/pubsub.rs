//! Thin pubsub publish helper. Mirrors `aleph/services/p2p/pubsub.py`.

use bytes::Bytes;

use super::protocol::AlephP2PClient;
use crate::AlephResult;

/// Publish a message on `topic`. Mirrors `aleph.services.p2p.pubsub.publish`.
///
/// `message` is sent as raw bytes; the Python helper accepts either `bytes`
/// or `str` — we keep `Bytes` for parity since string conversion is trivial
/// at the call site.
pub async fn publish<C: AlephP2PClient + ?Sized>(
    p2p_client: &C,
    topic: &str,
    message: impl Into<Bytes>,
    loopback: bool,
) -> AlephResult<()> {
    let data: Bytes = message.into();
    p2p_client.publish(data, topic, loopback).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::p2p::protocol::{Identify, MockP2pClient};
    use bytes::Bytes;

    #[tokio::test]
    async fn publish_passes_bytes_through() {
        let client = MockP2pClient::new(Identify {
            peer_id: "QmSelf".into(),
        });
        publish(&client, "ALIVE", Bytes::from_static(b"hi"), false)
            .await
            .unwrap();
        let p = client.published();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, "ALIVE");
        assert_eq!(p[0].1.as_ref(), b"hi");
        assert!(!p[0].2);
    }

    #[tokio::test]
    async fn publish_forwards_loopback_flag() {
        let client = MockP2pClient::new(Identify {
            peer_id: "QmSelf".into(),
        });
        publish(&client, "ALIVE", Bytes::from_static(b"hi"), true)
            .await
            .unwrap();
        let p = client.published();
        assert!(p[0].2);
    }
}
