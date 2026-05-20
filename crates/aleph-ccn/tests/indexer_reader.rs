mod common;

use aleph_ccn::chains::chain_data_service::{DbPendingTxSink, PendingTxPublisher};
use aleph_ccn::chains::indexer_reader::AlephIndexerReader;
use aleph_ccn::db::accessors::chains::{get_chain_tx, get_indexer_multirange};
use aleph_ccn::types::chain_sync::ChainEventType;
use aleph_types::chain::Chain;
use chrono::{DateTime, Utc};
use serde_json::json;
use wiremock::matchers::{body_string_contains, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

use common::start_postgres;

#[tokio::test]
async fn indexer_reader_uses_account_state_processed_ranges() {
    let pg = start_postgres().await;
    let server = MockServer::start().await;
    let start: DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
    let end: DateTime<Utc> = "2024-01-02T00:00:00Z".parse().unwrap();

    Mock::given(method("POST"))
        .and(body_string_contains("accountState"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "state": [{
                    "processed": [[start, end]]
                }]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(body_string_contains("messageEvents"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "messageEvents": [{
                    "transaction": "0xtx1",
                    "address": "0xcontract",
                    "height": 123,
                    "timestamp": 1704067200000.0,
                    "type": "POST",
                    "content": "QmMessage"
                }]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let reader = AlephIndexerReader::new(Chain::Bsc);
    let publisher = PendingTxPublisher::new(Box::new(DbPendingTxSink::new(pg.pool.clone())));
    let txs = reader
        .fetch_new_events(
            &pg.pool,
            &publisher,
            &server.uri(),
            "0xcontract",
            ChainEventType::Message,
        )
        .await
        .unwrap();

    assert_eq!(txs.len(), 1);
    let client = pg.pool.get().await.unwrap();
    assert!(get_chain_tx(&**client, "0xtx1").await.unwrap().is_some());
    let synced = get_indexer_multirange(&**client, Chain::Bsc, ChainEventType::Message)
        .await
        .unwrap();
    let ranges: Vec<_> = synced.iter_ranges().collect();
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].lower, start);
    assert_eq!(ranges[0].upper, end);
    assert!(ranges[0].upper_inc);
}
