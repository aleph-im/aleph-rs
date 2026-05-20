//! Ports chain sync archive persistence expectations from pyaleph.

mod common;

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use aleph_ccn::chains::chain_data_service::ChainDataService;
use aleph_ccn::config::IpfsSettings;
use aleph_ccn::db::models::messages::MessageDb;
use aleph_ccn::services::cache::local::LocalCache;
use aleph_ccn::services::ipfs::IpfsService;
use aleph_ccn::services::storage::engine::StorageEngine;
use aleph_ccn::services::storage::in_memory::InMemoryStorageEngine;
use aleph_ccn::storage::StorageService;
use aleph_ccn::types::message_status::MessageStatus;
use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use common::start_postgres;

fn message() -> MessageDb {
    MessageDb {
        item_hash: "00".into(),
        r#type: MessageType::Post,
        chain: Chain::Ethereum,
        sender: "0xabc".into(),
        signature: Some("0xsig".into()),
        item_type: ItemType::Inline,
        item_content: Some("{}".into()),
        content: json!({}),
        time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
        channel: None,
        size: 2,
        status_value: MessageStatus::Processed,
        reception_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
        owner: None,
        content_type: None,
        content_ref: None,
        content_key: None,
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: None,
        content_item_hash: None,
        tags: None,
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn prepare_sync_event_payload_upserts_archive_file_row() {
    let pg = start_postgres().await;
    let server = MockServer::start().await;
    let cid = "QmArchiveCidFromTest";
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Hash\":\"{cid}\"}}\n"
        )))
        .expect(1)
        .mount(&server)
        .await;

    let settings = IpfsSettings {
        host: server.address().ip().to_string(),
        port: server.address().port(),
        ..Default::default()
    };
    let ipfs = Arc::new(IpfsService::new(&settings).unwrap());
    let engine = Arc::new(InMemoryStorageEngine::new());
    let cache = Arc::new(LocalCache::new());
    let storage = Arc::new(
        StorageService::new(engine.clone(), ipfs, cache).with_http_p2p_enabled(false),
    );
    let service = ChainDataService::with_storage(storage);
    let client = pg.pool.get().await.unwrap();

    let payload = service
        .prepare_sync_event_payload(&**client, vec![message()])
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload["protocol"], "aleph-offchain");
    assert_eq!(payload["content"], cid);

    let row = client
        .query_one("SELECT hash, size, type FROM files WHERE hash = $1", &[&cid])
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("hash"), cid);
    assert!(row.get::<_, i64>("size") > 0);
    assert_eq!(row.get::<_, String>("type"), "file");

    let local_archive = engine.read(cid).await.unwrap();
    assert!(local_archive.is_some());
}
