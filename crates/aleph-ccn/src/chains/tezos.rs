//! Tezos signature verification. Mirrors `aleph/chains/tezos.py`.
//!
//! Supports the three Tezos curves:
//!   - tz1: Ed25519
//!   - tz2: secp256k1 (k256)
//!   - tz3: NIST P-256 (p256)
//!
//! Signature payload is a JSON object:
//!   { "signature": "edsig...", "publicKey": "edpk...",
//!     "signingType": "raw" | "micheline", "dAppUrl": "..." }
//!
//! When `signingType == "micheline"`, the verification buffer is wrapped in
//! a Beacon-style envelope:
//!   `\x05\x01\x00<hex_len_ascii><prefix " "+dapp+" "+iso8601+" "+buffer>`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use blake2::{Blake2bVar, digest::Update, digest::VariableOutput};
use ed25519_dalek::{Signature as EdSignature, Verifier as EdVerifier, VerifyingKey as EdKey};
use k256::ecdsa::{Signature as K256Signature, VerifyingKey as K256Key};
use p256::ecdsa::{Signature as P256Signature, VerifyingKey as P256Key};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::abc::{ChainReader, PendingMessageView, Verifier};
use super::chain_data_service::{PendingChainTx, PendingTxPublisher};
use super::common::verification_buffer;
use crate::AlephResult;
use crate::config::Settings;
use crate::db::DbPool;
use crate::db::accessors::chains::{get_last_height, upsert_chain_sync_status};
use crate::schemas::chains::tezos_indexer_response::SyncStatus;
use crate::toolkit::timestamp::timestamp_to_datetime;
use crate::types::chain_sync::{ChainEventType, ChainSyncProtocol};
use aleph_types::chain::Chain;

const DEFAULT_DAPP_URL: &str = "aleph.im";

#[derive(Deserialize)]
struct TezosSig {
    signature: String,
    #[serde(rename = "publicKey")]
    public_key: String,
    #[serde(default, rename = "signingType")]
    signing_type: Option<String>,
    #[serde(default, rename = "dAppUrl")]
    dapp_url: Option<String>,
}

/// Verifier for Tezos signatures across all three curves.
#[derive(Default, Debug, Clone, Copy)]
pub struct TezosVerifier;

#[async_trait]
impl Verifier for TezosVerifier {
    async fn verify_signature(&self, message: &dyn PendingMessageView) -> AlephResult<bool> {
        let Some(sig_payload) = message.signature() else {
            tracing::warn!(item_hash = %message.item_hash(), "Tezos: missing signature");
            return Ok(false);
        };

        let parsed: TezosSig = match serde_json::from_str(sig_payload) {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!("Tezos: signature field is not JSON deserializable");
                return Ok(false);
            }
        };

        let signing_type = parsed.signing_type.as_deref().unwrap_or("raw");
        let dapp_url = parsed.dapp_url.as_deref().unwrap_or(DEFAULT_DAPP_URL);

        // Decode the public key.
        let pubkey = match TezosPublicKey::decode(&parsed.public_key) {
            Some(pk) => pk,
            None => {
                tracing::warn!("Tezos: bad public key encoding");
                return Ok(false);
            }
        };

        let expected_pkh = pubkey.public_key_hash();
        if expected_pkh != message.sender() {
            tracing::warn!(
                expected = %expected_pkh,
                sender = message.sender(),
                "Tezos: sender does not match public key hash",
            );
            return Ok(false);
        }

        // Build the verification buffer.
        let buffer = verification_buffer(message);
        let signing_buffer: Vec<u8> = match signing_type {
            "raw" => buffer,
            "micheline" => {
                match build_micheline_buffer(&buffer, message.time_seconds(), dapp_url) {
                    Some(b) => b,
                    None => return Ok(false),
                }
            }
            other => {
                tracing::warn!(signing_type = other, "Tezos: unsupported signing type");
                return Ok(false);
            }
        };

        // Decode the signature.
        let signature_bytes = match decode_signature_b58check(&parsed.signature) {
            Some(b) => b,
            None => {
                tracing::warn!("Tezos: bad signature encoding");
                return Ok(false);
            }
        };

        Ok(pubkey.verify(&signing_buffer, &signature_bytes))
    }
}

/// Builds the micheline-style envelope used by Tezos web wallets.
/// Mirrors `micheline_verification_buffer` in pyaleph's `chains/tezos.py`.
fn build_micheline_buffer(buffer: &[u8], time_seconds: f64, dapp_url: &str) -> Option<Vec<u8>> {
    let timestamp = format_iso8601_ms(time_seconds)?;
    let mut payload = Vec::with_capacity(buffer.len() + dapp_url.len() + 64);
    payload.extend_from_slice(b"Tezos Signed Message:");
    payload.push(b' ');
    payload.extend_from_slice(dapp_url.as_bytes());
    payload.push(b' ');
    payload.extend_from_slice(timestamp.as_bytes());
    payload.push(b' ');
    payload.extend_from_slice(buffer);

    let hex_payload = hex::encode(&payload);
    let size_str = hex_payload.len().to_string();

    let mut out = Vec::with_capacity(3 + size_str.len() + payload.len());
    out.push(0x05);
    out.push(0x01);
    out.push(0x00);
    out.extend_from_slice(size_str.as_bytes());
    out.extend_from_slice(&payload);
    Some(out)
}

fn format_iso8601_ms(time_seconds: f64) -> Option<String> {
    use chrono::{DateTime, Utc};
    let secs = time_seconds.trunc() as i64;
    let nanos = ((time_seconds - time_seconds.trunc()) * 1_000_000_000.0).round() as u32;
    let dt = DateTime::<Utc>::from_timestamp(secs, nanos)?;
    let millis = dt.timestamp_subsec_millis();
    Some(format!(
        "{}T{:02}:{:02}:{:02}.{:03}Z",
        dt.format("%Y-%m-%d"),
        dt.format("%H").to_string().parse::<u32>().unwrap(),
        dt.format("%M").to_string().parse::<u32>().unwrap(),
        dt.format("%S").to_string().parse::<u32>().unwrap(),
        millis,
    ))
}

// --- Tezos b58check encoding ------------------------------------------------

/// Tezos public key encoded with its 4-byte algorithm prefix.
enum TezosPublicKey {
    Ed25519([u8; 32]),
    Secp256k1([u8; 33]),
    P256([u8; 33]),
}

impl TezosPublicKey {
    /// Decodes one of edpk / sppk / p2pk.
    fn decode(s: &str) -> Option<Self> {
        let raw = base58check_decode(s)?;
        match raw.as_slice() {
            // edpk: 0d 0f 25 d9
            [0x0d, 0x0f, 0x25, 0xd9, rest @ ..] if rest.len() == 32 => {
                let mut k = [0u8; 32];
                k.copy_from_slice(rest);
                Some(Self::Ed25519(k))
            }
            // sppk: 03 fe e2 56
            [0x03, 0xfe, 0xe2, 0x56, rest @ ..] if rest.len() == 33 => {
                let mut k = [0u8; 33];
                k.copy_from_slice(rest);
                Some(Self::Secp256k1(k))
            }
            // p2pk: 03 b2 8b 7f
            [0x03, 0xb2, 0x8b, 0x7f, rest @ ..] if rest.len() == 33 => {
                let mut k = [0u8; 33];
                k.copy_from_slice(rest);
                Some(Self::P256(k))
            }
            _ => None,
        }
    }

    fn public_key_hash(&self) -> String {
        // PKH = blake2b-160(pub_key) with a curve-specific b58check prefix.
        let (raw_pk, prefix) = match self {
            Self::Ed25519(k) => (k.as_slice(), &[0x06u8, 0xa1, 0x9f][..]), // tz1
            Self::Secp256k1(k) => (k.as_slice(), &[0x06u8, 0xa1, 0xa1][..]), // tz2
            Self::P256(k) => (k.as_slice(), &[0x06u8, 0xa1, 0xa4][..]),    // tz3
        };
        let mut hasher = Blake2bVar::new(20).expect("blake2b 20");
        hasher.update(raw_pk);
        let mut hash = [0u8; 20];
        hasher
            .finalize_variable(&mut hash)
            .expect("finalize blake2b");

        let mut payload = Vec::with_capacity(prefix.len() + 20);
        payload.extend_from_slice(prefix);
        payload.extend_from_slice(&hash);
        base58check_encode(&payload)
    }

    fn verify(&self, message: &[u8], signature: &[u8]) -> bool {
        match self {
            Self::Ed25519(pk) => {
                let key = match EdKey::from_bytes(pk) {
                    Ok(k) => k,
                    Err(_) => return false,
                };
                if signature.len() != 64 {
                    return false;
                }
                let sig_array: [u8; 64] = signature.try_into().unwrap();
                let sig = EdSignature::from_bytes(&sig_array);
                key.verify(message, &sig).is_ok()
            }
            Self::Secp256k1(pk) => {
                // The k256 crate accepts compressed (33) and uncompressed
                // SEC1 encodings.
                let key = match K256Key::from_sec1_bytes(pk) {
                    Ok(k) => k,
                    Err(_) => return false,
                };
                if signature.len() != 64 {
                    return false;
                }
                let sig = match K256Signature::from_slice(signature) {
                    Ok(s) => s,
                    Err(_) => return false,
                };
                // Tezos signs blake2b(msg, 32) — not the raw message.
                let mut hasher = Blake2bVar::new(32).expect("blake2b 32");
                hasher.update(message);
                let mut digest = [0u8; 32];
                hasher
                    .finalize_variable(&mut digest)
                    .expect("finalize blake2b");
                key.verify(&digest, &sig).is_ok()
            }
            Self::P256(pk) => {
                let key = match P256Key::from_sec1_bytes(pk) {
                    Ok(k) => k,
                    Err(_) => return false,
                };
                if signature.len() != 64 {
                    return false;
                }
                let sig = match P256Signature::from_slice(signature) {
                    Ok(s) => s,
                    Err(_) => return false,
                };
                let mut hasher = Blake2bVar::new(32).expect("blake2b 32");
                hasher.update(message);
                let mut digest = [0u8; 32];
                hasher
                    .finalize_variable(&mut digest)
                    .expect("finalize blake2b");
                key.verify(&digest, &sig).is_ok()
            }
        }
    }
}

/// Decodes a Tezos b58check signature, returning the raw 64-byte signature.
/// Strips the algorithm prefix.
fn decode_signature_b58check(s: &str) -> Option<Vec<u8>> {
    let raw = base58check_decode(s)?;
    // Generic signature prefix: sig (04 82 2b)
    // edsig: 09 f5 cd 86 12
    // spsig: 0d 73 65 13 3f
    // p2sig: 36 f0 2c 34
    let trimmed: &[u8] = match raw.as_slice() {
        [0x09, 0xf5, 0xcd, 0x86, 0x12, rest @ ..] => rest,
        [0x0d, 0x73, 0x65, 0x13, 0x3f, rest @ ..] => rest,
        [0x36, 0xf0, 0x2c, 0x34, rest @ ..] => rest,
        [0x04, 0x82, 0x2b, rest @ ..] => rest,
        _ => return None,
    };
    if trimmed.len() != 64 {
        return None;
    }
    Some(trimmed.to_vec())
}

fn base58check_decode(s: &str) -> Option<Vec<u8>> {
    let raw = bs58::decode(s).into_vec().ok()?;
    if raw.len() < 5 {
        return None;
    }
    let (payload, checksum) = raw.split_at(raw.len() - 4);
    let hash1 = Sha256::digest(payload);
    let hash2 = Sha256::digest(hash1);
    if &hash2[..4] != checksum {
        return None;
    }
    Some(payload.to_vec())
}

fn base58check_encode(payload: &[u8]) -> String {
    let hash1 = Sha256::digest(payload);
    let hash2 = Sha256::digest(hash1);
    let mut buf = Vec::with_capacity(payload.len() + 4);
    buf.extend_from_slice(payload);
    buf.extend_from_slice(&hash2[..4]);
    bs58::encode(buf).into_string()
}

// --- TezosConnector --------------------------------------------------------

/// Indexer event payload mirroring `IndexerMessageEvent` from pyaleph.
#[derive(Debug, Clone, Deserialize)]
pub struct TezosIndexerEvent {
    #[serde(rename = "_id", default)]
    pub id: Option<String>,
    pub source: String,
    /// ISO-8601 timestamp emitted by the Tezos indexer.
    pub timestamp: String,
    #[serde(rename = "blockLevel")]
    pub block_level: u64,
    #[serde(rename = "operationHash")]
    pub operation_hash: String,
    #[serde(rename = "type", default)]
    pub event_type: Option<String>,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TezosEventsData {
    #[serde(default)]
    pub events: Vec<TezosIndexerEvent>,
    #[serde(default)]
    pub stats: Option<TezosEventsStats>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TezosEventsStats {
    #[serde(rename = "totalEvents", default)]
    pub total_events: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TezosIndexerResponse {
    pub data: TezosEventsData,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TezosStatusResponse {
    pub data: TezosStatusData,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TezosStatusData {
    #[serde(rename = "indexStatus")]
    pub index_status: TezosStatus,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TezosStatus {
    pub status: SyncStatus,
}

pub fn make_tezos_status_query() -> &'static str {
    "{indexStatus {status}}"
}

/// Build the Tezos indexer GraphQL query. Mirrors `make_graphql_query` in pyaleph.
///
/// Requests `stats { totalEvents }` alongside the events page so the fetcher can
/// drain the full backlog in one pass (advancing `skip` until it reaches the
/// total event count).
pub fn make_tezos_query(sync_contract: &str, event_type: &str, limit: u32, skip: u32) -> String {
    format!(
        "{{\n  indexStatus {{\n    oldestBlock\n    recentBlock\n    status\n  }}\n  stats(address: \"{sync_contract}\") {{\n    totalEvents\n  }}\n  events(limit: {limit}, skip: {skip}, source: \"{sync_contract}\", type: \"{event_type}\") {{\n    _id\n    source\n    timestamp\n    blockLevel\n    operationHash\n    type\n    payload\n  }}\n}}"
    )
}

/// Reader-only Tezos connector polling the Tezos indexer.
pub struct TezosConnector {
    pool: Option<DbPool>,
    http: reqwest::Client,
    pending_tx_publisher: Arc<PendingTxPublisher>,
}

impl TezosConnector {
    pub fn new(pending_tx_publisher: Arc<PendingTxPublisher>) -> Self {
        Self {
            pool: None,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            pending_tx_publisher,
        }
    }

    pub fn with_db(mut self, pool: DbPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Convert one indexer event into a [`PendingChainTx`].
    pub fn event_to_pending_tx(event: &TezosIndexerEvent) -> PendingChainTx {
        let datetime = chrono::DateTime::parse_from_rfc3339(&event.timestamp)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| timestamp_to_datetime(0.0));
        let payload = normalize_tezos_message_payload(&event.payload);
        PendingChainTx {
            hash: event.operation_hash.clone(),
            chain: Chain::Tezos,
            height: event.block_level,
            datetime,
            publisher: event.source.clone(),
            protocol: ChainSyncProtocol::SmartContract,
            protocol_version: 1,
            content: payload,
        }
    }

    /// Read the Tezos indexer status. Pyaleph does not advance the cursor
    /// unless the indexer reports `synced`.
    pub async fn get_indexer_status(&self, indexer_url: &str) -> AlephResult<SyncStatus> {
        let body = serde_json::json!({ "query": make_tezos_status_query() });
        let resp = self.http.post(indexer_url).json(&body).send().await?;
        let parsed: TezosStatusResponse = resp.json().await?;
        Ok(parsed.data.index_status.status)
    }

    /// Pull a single page of events from the Tezos indexer, returning the page
    /// of events together with the indexer-reported total event count
    /// (`stats.totalEvents`). The total drives backlog draining in `fetcher()`.
    pub async fn fetch_events(
        &self,
        indexer_url: &str,
        sync_contract: &str,
        limit: u32,
        skip: u32,
    ) -> AlephResult<(Vec<TezosIndexerEvent>, u64)> {
        let query = make_tezos_query(sync_contract, "MessageEvent", limit, skip);
        let body = serde_json::json!({ "query": query });
        let resp = self.http.post(indexer_url).json(&body).send().await?;
        let parsed: TezosIndexerResponse = resp.json().await?;
        let total_events = parsed.data.stats.unwrap_or_default().total_events;
        Ok((parsed.data.events, total_events))
    }

    /// One-shot poll of the indexer. Publishes every new event via the
    /// `pending_tx_publisher`. Used both by `fetcher()` and tests.
    ///
    /// Returns the published txs alongside the indexer-reported total event
    /// count, so callers can decide whether more pages remain.
    pub async fn poll_once(
        &self,
        indexer_url: &str,
        sync_contract: &str,
        skip: u32,
    ) -> AlephResult<(Vec<PendingChainTx>, u64)> {
        let (events, total_events) = self
            .fetch_events(indexer_url, sync_contract, 100, skip)
            .await?;
        let mut out = Vec::with_capacity(events.len());
        for ev in &events {
            let tx = Self::event_to_pending_tx(ev);
            self.pending_tx_publisher.publish(&tx).await?;
            out.push(tx);
        }
        Ok((out, total_events))
    }
}

fn normalize_tezos_message_payload(payload: &serde_json::Value) -> serde_json::Value {
    let address = payload
        .get("address")
        .or_else(|| payload.get("addr"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let event_type = payload
        .get("type")
        .or_else(|| payload.get("msgtype"))
        .or_else(|| payload.get("message_type"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let content = payload
        .get("content")
        .or_else(|| payload.get("msgcontent"))
        .or_else(|| payload.get("message_content"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let timestamp = payload
        .get("timestamp")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    serde_json::json!({
        "address": address,
        "type": event_type,
        "content": content,
        "timestamp": timestamp,
    })
}

#[async_trait]
impl ChainReader for TezosConnector {
    async fn fetcher(&self, cfg: &Settings) -> AlephResult<()> {
        let indexer_url = cfg.tezos.indexer_url.clone();
        let sync_contract = cfg.tezos.sync_contract.clone();
        loop {
            match self.get_indexer_status(&indexer_url).await {
                Ok(SyncStatus::Synced) => {}
                Ok(status) => {
                    tracing::warn!(
                        status = ?status,
                        "Tezos indexer is not yet synced, waiting until it is"
                    );
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Tezos fetcher: status poll failed");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
            }

            // Read the persisted cursor once at the start of the pass. Mirrors
            // pyaleph's `get_last_height()`; the cursor counts indexer events
            // already drained (it is an event offset, not a block height).
            let mut height = if let Some(pool) = &self.pool {
                match pool.get().await {
                    Ok(client) => get_last_height(&**client, Chain::Tezos, ChainEventType::Message)
                        .await
                        .ok()
                        .flatten()
                        // Avoid an off-by-one at startup (-1 -> 0), like pyaleph.
                        .map(|h| h.max(0) as u32)
                        .unwrap_or(0),
                    Err(e) => {
                        tracing::warn!(error = %e, "Tezos fetcher: could not read sync cursor");
                        0
                    }
                }
            } else {
                0
            };

            // Drain the full backlog in one pass: page the indexer advancing the
            // cursor by the requested limit each time, until we have reached the
            // total event count reported by `stats`. Mirrors the inner loop of
            // pyaleph `fetch_incoming_messages`.
            const LIMIT: u32 = 100;
            let pass_result: AlephResult<()> = loop {
                match self.poll_once(&indexer_url, &sync_contract, height).await {
                    Ok((txs, total_events)) => {
                        tracing::info!(count = txs.len(), "Tezos fetcher: new txs");
                        // Advance by the requested limit, not by the returned
                        // count: the indexer may return fewer rows than asked
                        // for, but the cursor must still progress so subsequent
                        // pages do not re-fetch the same events.
                        height = height.saturating_add(LIMIT);
                        if u64::from(height) >= total_events {
                            // Clamp to the total so we don't overshoot.
                            height = total_events.min(i32::MAX as u64) as u32;
                            break Ok(());
                        }
                    }
                    // Stop the pass on error but keep the cursor we've reached so
                    // progress is persisted (finally-equivalent below).
                    Err(e) => break Err(e),
                }
            };

            // Persist the cursor regardless of how the pass ended, so mid-pass
            // failures still record progress (pyaleph's `finally` block).
            if let Some(pool) = &self.pool {
                match pool.get().await {
                    Ok(client) => {
                        if let Err(e) = upsert_chain_sync_status(
                            &**client,
                            Chain::Tezos,
                            ChainEventType::Message,
                            height.min(i32::MAX as u32) as i32,
                            chrono::Utc::now(),
                        )
                        .await
                        {
                            tracing::warn!(error = %e, "Tezos fetcher: could not persist sync cursor");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Tezos fetcher: could not persist sync cursor");
                    }
                }
            }

            match pass_result {
                Ok(()) => {
                    tracing::info!("Tezos fetcher: processed all transactions, waiting 10 seconds")
                }
                Err(e) => tracing::warn!(error = %e, "Tezos fetcher: poll failed"),
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::abc::SimplePendingMessage;
    use crate::chains::chain_data_service::TracingPendingTxSink;
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;

    #[tokio::test]
    async fn missing_signature_returns_false() {
        let msg = SimplePendingMessage {
            chain: Chain::Tezos,
            sender: "tz1abcdefghijklmnopqrstuvwxyz1234".into(),
            message_type: MessageType::Post,
            item_hash: "h".into(),
            signature: None,
            time_seconds: 0.0,
        };
        assert!(!TezosVerifier.verify_signature(&msg).await.unwrap());
    }

    #[tokio::test]
    async fn invalid_json_returns_false() {
        let msg = SimplePendingMessage {
            chain: Chain::Tezos,
            sender: "tz1abc".into(),
            message_type: MessageType::Post,
            item_hash: "h".into(),
            signature: Some("not-json".into()),
            time_seconds: 0.0,
        };
        assert!(!TezosVerifier.verify_signature(&msg).await.unwrap());
    }

    #[test]
    fn micheline_format_matches_python() {
        // From pyaleph: prefix b"\x05" b"\x01\x00" + payload_size + payload
        // Just sanity-check we can build it without panic.
        let b = build_micheline_buffer(b"abc", 1700000000.5, "aleph.im").unwrap();
        assert_eq!(b[0], 0x05);
        assert_eq!(b[1], 0x01);
        assert_eq!(b[2], 0x00);
    }

    #[tokio::test]
    async fn tezos_indexer_status_decodes_synced() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        let body = serde_json::json!({
            "data": {
                "indexStatus": {
                    "status": "synced"
                }
            }
        });
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&mock)
            .await;

        let publisher = Arc::new(PendingTxPublisher::new(Box::new(TracingPendingTxSink)));
        let connector = TezosConnector::new(publisher);
        let status = connector.get_indexer_status(&mock.uri()).await.unwrap();
        assert_eq!(status, SyncStatus::Synced);
    }

    #[tokio::test]
    async fn tezos_poll_once_decodes_events() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        let body = serde_json::json!({
            "data": {
                "stats": {
                    "totalEvents": 1
                },
                "events": [
                    {
                        "_id": "1",
                        "source": "KT1Foo",
                        "timestamp": "2024-01-02T03:04:05Z",
                        "blockLevel": 42,
                        "operationHash": "ophash1",
                        "type": "MessageEvent",
                        "payload": {
                            "addr": "tz1abc",
                            "msgtype": "STORE_IPFS",
                            "msgcontent": "QmHash",
                            "timestamp": 1704164645.0
                        }
                    }
                ]
            }
        });
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&mock)
            .await;

        let publisher = Arc::new(PendingTxPublisher::new(Box::new(TracingPendingTxSink)));
        let connector = TezosConnector::new(publisher);
        let (txs, total_events) = connector.poll_once(&mock.uri(), "KT1Foo", 0).await.unwrap();
        assert_eq!(total_events, 1);
        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0].hash, "ophash1");
        assert_eq!(txs[0].chain, Chain::Tezos);
        assert_eq!(txs[0].protocol, ChainSyncProtocol::SmartContract);
        assert_eq!(txs[0].height, 42);
        assert_eq!(txs[0].content["address"], "tz1abc");
        assert_eq!(txs[0].content["type"], "STORE_IPFS");
        assert_eq!(txs[0].content["content"], "QmHash");
    }

    #[test]
    fn tezos_event_to_pending_tx_parses_iso_timestamp() {
        let event = TezosIndexerEvent {
            id: Some("1".into()),
            source: "KT1".into(),
            timestamp: "2024-05-05T12:00:00Z".into(),
            block_level: 9,
            operation_hash: "op".into(),
            event_type: Some("MessageEvent".into()),
            payload: serde_json::json!({}),
        };
        let tx = TezosConnector::event_to_pending_tx(&event);
        assert_eq!(tx.height, 9);
        assert_eq!(tx.chain, Chain::Tezos);
        assert_eq!(tx.publisher, "KT1");
    }
}
