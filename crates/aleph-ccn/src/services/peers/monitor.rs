//! Alive-topic listeners. Mirrors `aleph/services/peers/monitor.py`.
//!
//! Two long-running tasks: one consuming the alive topic over the P2P
//! daemon's pubsub queue, one consuming it over IPFS pubsub. Both write
//! discovered peers into the `peers` table via [`upsert_peer`].

use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::Pool;
use futures_util::StreamExt as _;
use serde_json::Value;
use tokio::task::JoinHandle;

use super::allowlist::PeerAllowlist;
use crate::AlephError;
use crate::AlephResult;
use crate::db::accessors::peers::upsert_peer;
use crate::db::models::peers::PeerType;
use crate::services::ipfs::IpfsService;
use crate::services::p2p::protocol::AlephP2PClient;
use crate::toolkit::timestamp::utc_now;

/// Decode + validate a single `alive` payload. Splits the
/// `handle_incoming_host` Python function into a pure routine so it can be
/// covered with simple tests.
pub struct AliveMessage {
    pub address: String,
    pub peer_type: PeerType,
}

pub fn parse_alive_message(data: &[u8]) -> AlephResult<AliveMessage> {
    let text =
        std::str::from_utf8(data).map_err(|e| AlephError::P2p(format!("alive utf8: {e}")))?;
    let unquoted = url_decode(text);
    let value: Value =
        serde_json::from_str(&unquoted).map_err(|e| AlephError::P2p(format!("alive json: {e}")))?;
    let address = value
        .get("address")
        .and_then(Value::as_str)
        .ok_or_else(|| AlephError::P2p("Bad address".into()))?
        .to_string();
    let peer_type_str = value
        .get("peer_type")
        .and_then(Value::as_str)
        .ok_or_else(|| AlephError::P2p("Bad peer type".into()))?;
    let peer_type = PeerType::try_from(peer_type_str).map_err(AlephError::P2p)?;
    Ok(AliveMessage { address, peer_type })
}

/// Minimal port of `urllib.parse.unquote`. The Python alive messages are JSON
/// strings that may be percent-encoded.
pub fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (
                hex_digit(bytes[i + 1] as char),
                hex_digit(bytes[i + 2] as char),
            ) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_digit(c: char) -> Option<u8> {
    c.to_digit(16).map(|v| v as u8)
}

/// Upsert one alive message into the `peers` table. Mirrors
/// `handle_incoming_host`.
pub async fn handle_incoming_host(
    pool: &Pool,
    peer_allowlist: &PeerAllowlist,
    data: &[u8],
    sender: &str,
    source: PeerType,
) -> AlephResult<()> {
    if !peer_allowlist.is_allowed(sender).await {
        tracing::debug!("Ignoring alive message from unknown peer {sender}");
        return Ok(());
    }
    tracing::debug!("New message received from {sender}");
    let parsed = parse_alive_message(data)?;
    let client = pool
        .get()
        .await
        .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
    upsert_peer(
        &**client,
        sender,
        parsed.peer_type,
        &parsed.address,
        source,
        Some(utc_now()),
    )
    .await?;
    Ok(())
}

/// Subscribe to the P2P alive topic and forward each frame to
/// `handle_incoming_host`. Mirrors `monitor_hosts_p2p`.
pub async fn monitor_hosts_p2p<C: AlephP2PClient>(
    p2p_client: Arc<C>,
    pool: Pool,
    peer_allowlist: Arc<PeerAllowlist>,
    alive_topic: String,
) {
    loop {
        if let Err(e) = p2p_client.subscribe(&alive_topic).await {
            tracing::error!("Failed to subscribe to {alive_topic}: {e}");
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        loop {
            match p2p_client.receive_messages(&alive_topic).await {
                Ok(message) => {
                    let parts: Vec<&str> = message.routing_key.splitn(3, '.').collect();
                    let peer_id = parts.get(2).copied().unwrap_or_default().to_string();
                    if let Err(e) = handle_incoming_host(
                        &pool,
                        &peer_allowlist,
                        &message.body,
                        &peer_id,
                        PeerType::P2p,
                    )
                    .await
                    {
                        tracing::warn!("p2p alive message rejected: {e}");
                    }
                }
                Err(e) => {
                    tracing::error!("p2p alive recv error: {e}");
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Spawn [`monitor_hosts_p2p`] and return its handle.
pub fn spawn_monitor_hosts_p2p<C: AlephP2PClient + 'static>(
    p2p_client: Arc<C>,
    pool: Pool,
    peer_allowlist: Arc<PeerAllowlist>,
    alive_topic: String,
) -> JoinHandle<()> {
    tokio::spawn(monitor_hosts_p2p(
        p2p_client,
        pool,
        peer_allowlist,
        alive_topic,
    ))
}

/// Subscribe to the IPFS alive topic. Mirrors `monitor_hosts_ipfs`.
pub async fn monitor_hosts_ipfs(
    ipfs_service: Arc<IpfsService>,
    pool: Pool,
    peer_allowlist: Arc<PeerAllowlist>,
    alive_topic: String,
) {
    loop {
        match ipfs_service.sub(&alive_topic).await {
            Ok(mut stream) => {
                while let Some(msg) = stream.next().await {
                    match msg {
                        Ok(value) => {
                            let data = value
                                .get("data")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .as_bytes()
                                .to_vec();
                            let sender = value
                                .get("from")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string();
                            if let Err(e) = handle_incoming_host(
                                &pool,
                                &peer_allowlist,
                                &data,
                                &sender,
                                PeerType::Ipfs,
                            )
                            .await
                            {
                                tracing::warn!("ipfs alive message rejected: {e}");
                            }
                        }
                        Err(e) => {
                            tracing::error!("ipfs alive recv error: {e}");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!("Exception in pubsub peers monitoring, resubscribing: {e}");
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Spawn [`monitor_hosts_ipfs`] and return its handle.
pub fn spawn_monitor_hosts_ipfs(
    ipfs_service: Arc<IpfsService>,
    pool: Pool,
    peer_allowlist: Arc<PeerAllowlist>,
    alive_topic: String,
) -> JoinHandle<()> {
    tokio::spawn(monitor_hosts_ipfs(
        ipfs_service,
        pool,
        peer_allowlist,
        alive_topic,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_alive_message_extracts_fields() {
        let json = r#"{"address":"/ip4/1.2.3.4/tcp/4025","interests":null,"peer_type":"P2P","version":"1.0"}"#;
        let parsed = parse_alive_message(json.as_bytes()).unwrap();
        assert_eq!(parsed.address, "/ip4/1.2.3.4/tcp/4025");
        assert_eq!(parsed.peer_type, PeerType::P2p);
    }

    #[test]
    fn parse_alive_message_rejects_bad_peer_type() {
        let json = r#"{"address":"x","peer_type":"OTHER"}"#;
        assert!(parse_alive_message(json.as_bytes()).is_err());
    }

    #[test]
    fn url_decode_percent_encoded() {
        assert_eq!(url_decode("hello%20world"), "hello world");
        assert_eq!(url_decode("%7B%7D"), "{}");
        assert_eq!(url_decode("plain"), "plain");
    }
}
