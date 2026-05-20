//! Pubsub message decoding + listener bootstrapping. Mirrors `aleph/network.py`.
//!
//! Two functions:
//! - [`decode_pubsub_message`] — URL-decode the raw pubsub bytes, then
//!   JSON-decode them into a `serde_json::Value`. The Python version does the
//!   same `urllib.parse.unquote` + `json.loads` dance.
//! - [`listener_tasks`] — spawn the long-running pubsub consumer loops (P2P
//!   topic + optional IPFS topic) and return their `JoinHandle`s. In Python
//!   these are returned as coroutines for `asyncio.gather`; in Rust we
//!   pre-spawn them.

use std::sync::Arc;

use futures_util::StreamExt as _;
use lapin::options::{BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use serde_json::Value;
use tokio::task::JoinSet;

use crate::AlephError;
use crate::AlephResult;
use crate::config::Settings;
use crate::handlers::message_handler::MessagePublisher;
use crate::services::ipfs::IpfsService;
use crate::services::ipfs::pubsub::{IpfsPubsubHandler, incoming_channel as incoming_ipfs_channel};
use crate::services::p2p::protocol::{AlephP2PClient, HttpP2pClient, SeenHashes};
use crate::toolkit::rabbitmq::{declare_sub_exchange, make_mq_conn};

/// Decode a single pubsub frame: URL-decode + JSON-decode. Mirrors
/// `aleph.network.decode_pubsub_message` (`urllib.parse.unquote` +
/// `json.loads`).
pub fn decode_pubsub_message(message_data: &[u8]) -> AlephResult<Value> {
    let text = std::str::from_utf8(message_data)
        .map_err(|_| AlephError::InvalidMessage(format!("Data is not UTF-8: {message_data:?}")))?;
    let unquoted = percent_decode(text);
    serde_json::from_str(&unquoted)
        .map_err(|_| AlephError::InvalidMessage(format!("Data is not JSON: {message_data:?}")))
}

/// Minimal percent-decoder. Mirrors `urllib.parse.unquote(s)`:
/// - `%XX` (case-insensitive) → corresponding byte.
/// - Invalid `%`-escapes are left as-is (Python's `unquote` swallows them).
/// - Bytes are interpreted as UTF-8; invalid sequences fall back to the
///   lossy replacement char, matching `unquote(errors='replace')` behaviour.
fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let (h, l) = (bytes[i + 1], bytes[i + 2]);
            match (hex_digit(h), hex_digit(l)) {
                (Some(h), Some(l)) => {
                    out.push((h << 4) | l);
                    i += 3;
                    continue;
                }
                _ => {}
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Glue: forward each decoded IPFS pubsub frame to
/// [`MessagePublisher::add_pending_message`].
struct PublisherHandler {
    publisher: Arc<MessagePublisher>,
    pool: deadpool_postgres::Pool,
    origin: crate::types::message_status::MessageOrigin,
}

async fn add_pending_from_pubsub(
    publisher: &MessagePublisher,
    pool: &deadpool_postgres::Pool,
    message_dict: &Value,
    origin: crate::types::message_status::MessageOrigin,
) -> AlephResult<()> {
    let client = pool
        .get()
        .await
        .map_err(|e| AlephError::Pool(format!("{e}")))?;
    let _ = publisher
        .add_pending_message(
            &**client,
            message_dict,
            chrono::Utc::now(),
            None,
            true,
            Some(origin),
        )
        .await?;
    Ok(())
}

#[async_trait::async_trait]
impl IpfsPubsubHandler for PublisherHandler {
    async fn handle(&self, frame: Value) -> AlephResult<()> {
        // The IPFS daemon delivers `{from, data, seqno, topicIDs}`. The
        // meaningful payload lives in `data` (already base64-decoded by
        // `IpfsService::sub`). Mirrors Python's nested decode.
        let payload = match frame.get("data") {
            Some(Value::String(s)) => s.clone(),
            _ => return Ok(()),
        };
        let message_dict = decode_pubsub_message(payload.as_bytes())?;
        add_pending_from_pubsub(&self.publisher, &self.pool, &message_dict, self.origin).await
    }
}

async fn incoming_p2p_channel(
    config: Arc<Settings>,
    publisher: Arc<MessagePublisher>,
    pool: deadpool_postgres::Pool,
) -> AlephResult<()> {
    let seen_hashes = Arc::new(SeenHashes::new(200_000));
    loop {
        if let Err(e) = run_p2p_channel_once(
            config.clone(),
            publisher.clone(),
            pool.clone(),
            seen_hashes.clone(),
        )
        .await
        {
            tracing::error!("Exception in P2P pubsub, reconnecting in 100 ms... ({e})");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn run_p2p_channel_once(
    config: Arc<Settings>,
    publisher: Arc<MessagePublisher>,
    pool: deadpool_postgres::Pool,
    seen_hashes: Arc<SeenHashes>,
) -> AlephResult<()> {
    let p2p_client = HttpP2pClient::new(&config.p2p, Some(config.rabbitmq.clone()), "aleph-ccn");
    p2p_client.subscribe(&config.aleph.queue_topic).await?;

    let conn = make_mq_conn(&config.p2p, &config.rabbitmq).await?;
    let channel = conn
        .create_channel()
        .await
        .map_err(|e| AlephError::P2p(format!("rabbitmq create channel failed: {e}")))?;
    declare_sub_exchange(&channel, &config.rabbitmq).await?;

    let queue = channel
        .queue_declare(
            "",
            QueueDeclareOptions {
                durable: false,
                exclusive: true,
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| AlephError::P2p(format!("p2p queue_declare failed: {e}")))?;
    let queue_name = queue.name().as_str().to_string();
    let binding = format!("*.{}.*", config.aleph.queue_topic);
    channel
        .queue_bind(
            &queue_name,
            &config.rabbitmq.sub_exchange,
            &binding,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|e| AlephError::P2p(format!("p2p queue_bind failed: {e}")))?;

    let consumer = channel
        .basic_consume(
            &queue_name,
            "aleph-ccn-p2p-listener",
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| AlephError::P2p(format!("p2p consume failed: {e}")))?;
    let mut consumer = consumer;

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.map_err(|e| AlephError::P2p(format!("p2p delivery: {e}")))?;
        let routing_key = delivery.routing_key.as_str();
        let parts: Vec<&str> = routing_key.split('.').collect();
        if parts.len() != 3 {
            tracing::warn!(routing_key, "Received P2P message with invalid routing key");
            continue;
        }

        let message_dict = match decode_pubsub_message(&delivery.data) {
            Ok(message) => message,
            Err(e) => {
                tracing::warn!(routing_key, "Received invalid P2P message: {e}");
                continue;
            }
        };
        if let Some(seen_key) = p2p_dedupe_key(&message_dict) {
            if seen_hashes.contains(&seen_key) {
                continue;
            }
            seen_hashes.record(seen_key);
        }

        if let Err(e) = add_pending_from_pubsub(
            &publisher,
            &pool,
            &message_dict,
            crate::types::message_status::MessageOrigin::P2p,
        )
        .await
        {
            tracing::warn!(routing_key, "Failed to add P2P pending message: {e}");
        }
    }

    Err(AlephError::P2p("p2p consumer ended".into()))
}

fn p2p_dedupe_key(message: &Value) -> Option<(String, String, String)> {
    Some((
        serde_json::to_string(message.get("sender")?).ok()?,
        serde_json::to_string(message.get("item_hash")?).ok()?,
        serde_json::to_string(message.get("signature")?).ok()?,
    ))
}

/// Spawn the pubsub listener loops. Mirrors `aleph.network.listener_tasks`.
/// Returns a `Vec<JoinHandle<()>>` — one task per topic.
///
/// The P2P topic is always listened to; the IPFS topic is added when
/// `config.ipfs.enabled` is true.
pub async fn listener_tasks(
    config: &Settings,
    publisher: Arc<MessagePublisher>,
    pool: deadpool_postgres::Pool,
    ipfs_service: Option<Arc<IpfsService>>,
) -> AlephResult<JoinSet<AlephResult<()>>> {
    let mut handles = JoinSet::new();

    let p2p_config = Arc::new(config.clone());
    let p2p_publisher = publisher.clone();
    let p2p_pool = pool.clone();
    handles.spawn(async move {
        incoming_p2p_channel(p2p_config, p2p_publisher, p2p_pool).await
    });

    // IPFS pubsub channel (optional).
    if config.ipfs.enabled && let Some(ipfs_service) = ipfs_service {
        let topic = config.aleph.queue_topic.clone();
        let handler: Arc<dyn IpfsPubsubHandler> = Arc::new(PublisherHandler {
            publisher: publisher.clone(),
            pool: pool.clone(),
            origin: crate::types::message_status::MessageOrigin::Ipfs,
        });
        let ipfs = ipfs_service.clone();
        handles.spawn(async move {
            incoming_ipfs_channel(ipfs, topic, handler).await;
            Ok(())
        });
    }

    Ok(handles)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_pubsub_message_url_decoded_json() {
        // `urllib.parse.quote('{"a":"b"}')` => `%7B%22a%22%3A%22b%22%7D`.
        let bytes = b"%7B%22a%22%3A%22b%22%7D";
        let v = decode_pubsub_message(bytes).unwrap();
        assert_eq!(v["a"], "b");
    }

    #[test]
    fn decode_pubsub_message_plain_json() {
        let bytes = br#"{"k":1}"#;
        let v = decode_pubsub_message(bytes).unwrap();
        assert_eq!(v["k"], 1);
    }

    #[test]
    fn decode_pubsub_message_double_decode_nested() {
        // Some clients percent-encode the inline `item_content` separately;
        // make sure the outer decode strips the encoding leaving a JSON
        // string we can still parse.
        let bytes = b"%7B%22item_content%22%3A%20%22hello%22%7D";
        let v = decode_pubsub_message(bytes).unwrap();
        assert_eq!(v["item_content"], "hello");
    }

    #[test]
    fn decode_pubsub_message_malformed_bytes_rejected() {
        // Invalid UTF-8 sequence.
        let bytes: &[u8] = &[0xff, 0xfe, 0xfd];
        assert!(matches!(
            decode_pubsub_message(bytes).unwrap_err(),
            AlephError::InvalidMessage(_)
        ));
    }

    #[test]
    fn decode_pubsub_message_not_json_rejected() {
        let bytes = b"this is not json";
        assert!(matches!(
            decode_pubsub_message(bytes).unwrap_err(),
            AlephError::InvalidMessage(_)
        ));
    }
}
