//! Ports `tests/message_processing/test_process_pending_txs.py`.
//!
//! The Python tests wire a `PendingTxProcessor` against a mock chain-data
//! service that loads fixture messages from JSON. We mirror that with the
//! [`TxMessageProvider`] and [`PendingMessagePublisher`] traits and assert
//! that:
//! - `handle_pending_tx` calls the publisher once per message returned by the
//!   provider;
//! - it skips publishing for empty TX content;
//! - on-chain provenance is propagated through to the publisher.

mod common;

use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use aleph_ccn::chains::chain_data_service::{ChainDataService, PendingChainTx};
use aleph_ccn::db::accessors::pending_txs::get_pending_txs;
use aleph_ccn::jobs::process_pending_txs::{
    PendingMessagePublisher, TxMessageProvider, handle_pending_tx,
};
use aleph_ccn::types::chain_sync::ChainSyncProtocol;
use aleph_ccn::types::message_status::MessageOrigin;
use aleph_types::chain::Chain;

use common::start_postgres;

struct ListProvider {
    messages: Vec<Value>,
}

#[async_trait]
impl TxMessageProvider for ListProvider {
    async fn get_tx_messages(
        &self,
        _tx: &PendingChainTx,
        _seen_ids: &mut HashSet<String>,
    ) -> aleph_ccn::AlephResult<Vec<Value>> {
        Ok(self.messages.clone())
    }
}

#[derive(Default)]
struct RecordingPublisher {
    calls: Mutex<Vec<(String, Option<String>, bool, MessageOrigin)>>,
}

#[async_trait]
impl PendingMessagePublisher for RecordingPublisher {
    async fn add_pending_message(
        &self,
        message_dict: &Value,
        _reception_time: DateTime<Utc>,
        tx_hash: Option<&str>,
        check_message: bool,
        origin: MessageOrigin,
    ) -> aleph_ccn::AlephResult<()> {
        self.calls.lock().unwrap().push((
            message_dict
                .get("item_hash")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            tx_hash.map(|s| s.to_string()),
            check_message,
            origin,
        ));
        Ok(())
    }
}

fn sample_chain_tx() -> PendingChainTx {
    PendingChainTx {
        hash: "0xf49cb176c1ce4f6eb7b9721303994b05074f8fadc37b5f41ac6f78bdf4b14b6c".into(),
        chain: Chain::Ethereum,
        height: 13_314_512,
        datetime: Utc::now(),
        publisher: "0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC".into(),
        protocol: ChainSyncProtocol::OnChainSync,
        protocol_version: 1,
        content: Value::String("test-data-pending-tx-messages".into()),
    }
}

#[tokio::test]
async fn chain_data_service_provider_preserves_duplicate_item_hashes_for_confirmations() {
    let mut chain_tx = sample_chain_tx();
    chain_tx.content = json!({
        "messages": [
            {
                "item_hash": "deadbeef0001",
                "sender": "0xabc",
                "chain": "ETH",
                "type": "POST",
                "signature": "0xsig",
                "time": 1_700_000_000.0,
                "item_content": "{}",
                "item_type": "inline"
            },
            {
                "item_hash": "deadbeef0001",
                "sender": "0xabc",
                "chain": "ETH",
                "type": "POST",
                "signature": "0xsig",
                "time": 1_700_000_001.0,
                "item_content": "{}",
                "item_type": "inline"
            },
            {
                "item_hash": "deadbeef0002",
                "sender": "0xdef",
                "chain": "ETH",
                "type": "POST",
                "signature": "0xsig2",
                "time": 1_700_000_002.0,
                "item_content": "{}",
                "item_type": "inline"
            }
        ]
    });

    let provider = ChainDataService::new();
    let mut seen = HashSet::new();
    let messages = TxMessageProvider::get_tx_messages(&provider, &chain_tx, &mut seen)
        .await
        .unwrap();

    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0]["item_hash"], "deadbeef0001");
    assert_eq!(messages[1]["item_hash"], "deadbeef0001");
    assert_eq!(messages[2]["item_hash"], "deadbeef0002");
    assert!(seen.is_empty());
}

async fn seed_pending_tx(pool: &aleph_ccn::db::DbPool, chain_tx: &PendingChainTx) {
    let client = pool.get().await.unwrap();
    // chain_tx row.
    let chain_s = serde_json::to_value(&chain_tx.chain)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let proto_s = serde_json::to_value(chain_tx.protocol)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    client
        .execute(
            "INSERT INTO chain_txs(hash, chain, height, datetime, publisher, protocol, protocol_version, content) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
            &[
                &chain_tx.hash,
                &chain_s,
                &(chain_tx.height as i32),
                &chain_tx.datetime,
                &chain_tx.publisher,
                &proto_s,
                &(chain_tx.protocol_version as i32),
                &chain_tx.content,
            ],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO pending_txs(tx_hash) VALUES ($1) ON CONFLICT DO NOTHING",
            &[&chain_tx.hash],
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn handle_pending_tx_publishes_each_message_and_deletes_pending_row() {
    let pg = start_postgres().await;
    let chain_tx = sample_chain_tx();
    seed_pending_tx(&pg.pool, &chain_tx).await;

    let messages = vec![
        json!({"item_hash": "deadbeef0001"}),
        json!({"item_hash": "deadbeef0002"}),
    ];
    let provider = ListProvider {
        messages: messages.clone(),
    };
    let publisher = RecordingPublisher::default();
    let mut seen = HashSet::new();

    handle_pending_tx(&pg.pool, &provider, &publisher, chain_tx.clone(), &mut seen)
        .await
        .unwrap();

    let calls = publisher.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0, "deadbeef0001");
    assert_eq!(calls[1].0, "deadbeef0002");
    for c in calls.iter() {
        assert_eq!(c.1.as_deref(), Some(chain_tx.hash.as_str()));
        // on-chain-sync protocol → check_message = true.
        assert!(c.2);
        assert_eq!(c.3, MessageOrigin::Onchain);
    }

    // pending_txs row removed.
    let client = pg.pool.get().await.unwrap();
    let remaining = get_pending_txs(&**client, 10).await.unwrap();
    assert!(remaining.is_empty(), "pending_txs row must be deleted");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn handle_pending_tx_no_messages_skips_publisher() {
    let pg = start_postgres().await;
    let chain_tx = sample_chain_tx();
    let provider = ListProvider { messages: vec![] };
    let publisher = RecordingPublisher::default();
    let mut seen = HashSet::new();
    handle_pending_tx(&pg.pool, &provider, &publisher, chain_tx, &mut seen)
        .await
        .unwrap();
    assert!(publisher.calls.lock().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn smart_contract_protocol_disables_check_message() {
    let pg = start_postgres().await;
    let mut chain_tx = sample_chain_tx();
    chain_tx.protocol = ChainSyncProtocol::SmartContract;
    chain_tx.hash = "0xsc-hash".into();
    seed_pending_tx(&pg.pool, &chain_tx).await;

    let provider = ListProvider {
        messages: vec![json!({"item_hash": "feed0001"})],
    };
    let publisher = RecordingPublisher::default();
    let mut seen = HashSet::new();
    handle_pending_tx(&pg.pool, &provider, &publisher, chain_tx, &mut seen)
        .await
        .unwrap();
    let calls = publisher.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(!calls[0].2, "smart-contract messages must skip check_message");
}
