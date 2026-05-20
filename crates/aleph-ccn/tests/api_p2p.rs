//! Ports `tests/api/test_p2p.py`.

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use bytes::Bytes;
use chrono::Utc;
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use aleph_ccn::config::IpfsSettings;
use aleph_ccn::db::accessors::messages::{get_message_status, upsert_message_status};
use aleph_ccn::services::ipfs::IpfsService;
use aleph_ccn::services::p2p::protocol::{Identify, MockP2pClient};
use aleph_ccn::services::p2p::protocol::{AlephP2PClient, ReceivedMessage};
use aleph_ccn::types::message_status::MessageStatus;
use aleph_ccn::{AlephError, AlephResult};

use common::{make_app_state, start_postgres};

const P2P_PUB_URI: &str = "/api/v0/p2p/pubsub/pub";

const MESSAGE_DICT_JSON: &str = r#"{
  "chain": "NULS2",
  "item_hash": "4bbcfe7c4775492c2e602d322d68f558891468927b5e0d6cb89ff880134f323e",
  "sender": "NULSd6Hgbhr42Dm5nEgf6foEUT5bgwHesZQJB",
  "type": "STORE",
  "channel": "MYALEPH",
  "item_content": "{\"address\":\"NULSd6Hgbhr42Dm5nEgf6foEUT5bgwHesZQJB\",\"item_type\":\"ipfs\",\"item_hash\":\"QmUDS8mpQmpPyptyUEedHxHMkxo7ueRRiAvrpgvJMpjXwW\",\"time\":1577325086.513}",
  "item_type": "inline",
  "signature": "G7/xlWoMjjOr1NBN4SiZ8USYYVM9Q3JHXChR9hPw9/YSItfAplshWysqYDkvmBZiwbICG0IVB3ilMPJ/ZVgPNlk=",
  "time": 1608297193.717
}"#;

async fn post_json(app: axum::Router, uri: &str, body: Value) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, body)
}

struct FailingP2pClient;

#[async_trait]
impl AlephP2PClient for FailingP2pClient {
    async fn identify(&self) -> AlephResult<Identify> {
        Ok(Identify {
            peer_id: "QmFailingPeer".into(),
        })
    }

    async fn dial(&self, _peer_id: &str, _multiaddr: &str) -> AlephResult<()> {
        Err(AlephError::P2p("dial failed".into()))
    }

    async fn subscribe(&self, _topic: &str) -> AlephResult<()> {
        Err(AlephError::P2p("subscribe failed".into()))
    }

    async fn publish(&self, _data: Bytes, _topic: &str, _loopback: bool) -> AlephResult<()> {
        Err(AlephError::P2p("publish failed".into()))
    }

    async fn receive_messages(&self, _topic: &str) -> AlephResult<ReceivedMessage> {
        Err(AlephError::P2p("receive failed".into()))
    }
}

fn ipfs_service_for_mock(server: &MockServer) -> Arc<IpfsService> {
    let url = url::Url::parse(&server.uri()).unwrap();
    let mut settings = IpfsSettings::default();
    settings.host = url.host_str().unwrap().to_string();
    settings.port = url.port().unwrap();
    settings.scheme = url.scheme().to_string();
    Arc::new(IpfsService::new(&settings).unwrap())
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn pubsub_pub_valid_message_succeeds() {
    let pg = start_postgres().await;
    let p2p = Arc::new(MockP2pClient::new(Identify {
        peer_id: "QmTestPeer".into(),
    }));
    let mut state = make_app_state(pg.pool.clone());
    state.p2p_client = Some(p2p.clone());
    let app = aleph_ccn::web::build_router(state);
    let topic = aleph_ccn::config::Settings::default().aleph.queue_topic;
    let (status, body) = post_json(
        app,
        P2P_PUB_URI,
        json!({"topic": topic, "data": MESSAGE_DICT_JSON}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["status"].as_str(), Some("success"));
    let published = p2p.published();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, topic);
    assert_eq!(published[0].1.as_ref(), MESSAGE_DICT_JSON.as_bytes());
    assert!(published[0].2);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn pubsub_pub_errors() {
    let pg = start_postgres().await;
    let topic = aleph_ccn::config::Settings::default().aleph.queue_topic;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));

    // Invalid topic
    let (status, _) = post_json(
        app.clone(),
        P2P_PUB_URI,
        json!({"topic": "random-topic", "data": MESSAGE_DICT_JSON}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // data is an object (not a stringified JSON)
    let (status, _) = post_json(
        app.clone(),
        P2P_PUB_URI,
        json!({"topic": topic, "data": {"obj": 1}}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // truncated JSON
    let truncated = &MESSAGE_DICT_JSON[..MESSAGE_DICT_JSON.len() - 2];
    let (status, _) = post_json(
        app.clone(),
        P2P_PUB_URI,
        json!({"topic": topic, "data": truncated}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Missing item_content
    let mut v: Value = serde_json::from_str(MESSAGE_DICT_JSON).unwrap();
    if let Some(obj) = v.as_object_mut() {
        obj.remove("item_content");
    }
    let (status, _) = post_json(
        app,
        P2P_PUB_URI,
        json!({"topic": topic, "data": v.to_string()}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn post_message_pending_returns_202() {
    let pg = start_postgres().await;
    let p2p = Arc::new(MockP2pClient::new(Identify {
        peer_id: "QmTestPeer".into(),
    }));
    let mut state = make_app_state(pg.pool.clone());
    state.p2p_client = Some(p2p.clone());
    let app = aleph_ccn::web::build_router(state);
    let message: Value = serde_json::from_str(MESSAGE_DICT_JSON).unwrap();
    let (status, body) = post_json(
        app,
        "/api/v0/messages",
        json!({"sync": false, "message": message}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["message_status"].as_str(), Some("pending"));
    assert_eq!(v["publication_status"]["status"].as_str(), Some("success"));
    let published = p2p.published();
    assert_eq!(published.len(), 1);
    assert_eq!(
        published[0].0,
        aleph_ccn::config::Settings::default().aleph.queue_topic
    );
    assert!(published[0].2);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn post_message_sync_existing_processed_returns_202_pending() {
    let pg = start_postgres().await;
    let p2p = Arc::new(MockP2pClient::new(Identify {
        peer_id: "QmTestPeer".into(),
    }));
    let mut state = make_app_state(pg.pool.clone());
    state.p2p_client = Some(p2p.clone());
    let app = aleph_ccn::web::build_router(state);
    let message: Value = serde_json::from_str(MESSAGE_DICT_JSON).unwrap();
    let item_hash = message["item_hash"].as_str().unwrap();
    let client = pg.pool.get().await.unwrap();
    upsert_message_status(&**client, item_hash, MessageStatus::Processed, Utc::now(), None)
        .await
        .unwrap();

    let (status, body) = post_json(
        app,
        "/api/v0/messages",
        json!({"sync": true, "message": message}),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["message_status"].as_str(), Some("pending"));
    assert_eq!(v["publication_status"]["status"].as_str(), Some("success"));
    assert_eq!(p2p.published().len(), 1);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn post_message_total_publication_failure_returns_500_without_pending_row() {
    let pg = start_postgres().await;
    let ipfs = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pubsub/pub"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&ipfs)
        .await;
    let mut state = make_app_state(pg.pool.clone());
    state.ipfs_service = Some(ipfs_service_for_mock(&ipfs));
    state.p2p_client = Some(Arc::new(FailingP2pClient));
    let app = aleph_ccn::web::build_router(state);
    let message: Value = serde_json::from_str(MESSAGE_DICT_JSON).unwrap();
    let item_hash = message["item_hash"].as_str().unwrap().to_string();

    let (status, body) = post_json(
        app,
        "/api/v0/messages",
        json!({"sync": false, "message": message}),
    )
    .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["publication_status"]["status"].as_str(), Some("error"));
    assert_eq!(v["message_status"], Value::Null);
    let client = pg.pool.get().await.unwrap();
    let pending_count: i64 = client
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM pending_messages WHERE item_hash = $1",
            &[&item_hash],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(pending_count, 0);
    let status_row = get_message_status(&**client, &item_hash).await.unwrap();
    assert!(status_row.is_none());
}
