//! Smoke-test for `/api/ws0/messages`. Boots the production router on a
//! random port, connects via a tungstenite client, publishes a message
//! through `state.message_broadcast`, and asserts the client receives it.
//!
//! The test deliberately uses `history=0` so the DB is never touched.

mod common;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as TgMessage;

use common::dummy_state;

#[tokio::test]
async fn ws_messages_streams_published_payload() {
    let state = dummy_state();
    let publish_tx = state.message_broadcast.clone();
    let app = aleph_ccn::web::build_router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server a moment to start accepting connections.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = format!("ws://{}/api/ws0/messages?history=0", addr);
    let (mut ws, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("ws connect");

    // The handler subscribes synchronously inside the task; publish after a
    // short delay so the broadcast slot is wired up before we send.
    let publish_payload = json!({
        "item_hash": "deadbeef",
        "sender": "0x1",
        "chain": "ETH",
        "channel": "unit-tests",
        "type": "POST",
        "content": {"address": "0x1", "type": "post"},
    });
    let pp = publish_payload.clone();
    tokio::spawn(async move {
        // Wait for the WS handler to subscribe (broadcast tx has at least 1 rx).
        for _ in 0..50 {
            if publish_tx.receiver_count() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // It's fine if send returns Err — the test will fail at receive instead.
        let _ = publish_tx.send(pp);
    });

    let received = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let frame = ws.next().await.expect("ws recv").expect("ws frame");
            match frame {
                TgMessage::Text(t) => return t.to_string(),
                TgMessage::Ping(_) => continue,
                _ => continue,
            }
        }
    })
    .await
    .expect("timed out waiting for ws frame");

    let v: Value = serde_json::from_str(&received).expect("payload json");
    assert_eq!(v["item_hash"], "deadbeef");
    assert_eq!(v["channel"], "unit-tests");

    let _ = ws.send(TgMessage::Close(None)).await;
    server.abort();
}

#[tokio::test]
async fn ws_messages_filter_by_channel_drops_non_matching() {
    let state = dummy_state();
    let publish_tx = state.message_broadcast.clone();
    let app = aleph_ccn::web::build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = format!("ws://{}/api/ws0/messages?history=0&channels=match-me", addr);
    let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.unwrap();

    let tx = publish_tx.clone();
    tokio::spawn(async move {
        for _ in 0..50 {
            if tx.receiver_count() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = tx.send(json!({
            "item_hash": "h1",
            "channel": "other",
            "type": "POST",
            "content": {},
        }));
        let _ = tx.send(json!({
            "item_hash": "h2",
            "channel": "match-me",
            "type": "POST",
            "content": {},
        }));
    });

    let received = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let frame = ws.next().await.unwrap().unwrap();
            if let TgMessage::Text(t) = frame {
                return t.to_string();
            }
        }
    })
    .await
    .expect("timed out");
    let v: Value = serde_json::from_str(&received).unwrap();
    // The "other" message must be skipped — only "match-me" reaches us.
    assert_eq!(v["item_hash"], "h2");

    let _ = ws.send(TgMessage::Close(None)).await;
    server.abort();
}

#[tokio::test]
async fn ws_messages_connection_cap_rejects_with_1013() {
    // Set max_message_connections=1 so the second connect attempt is rejected.
    let mut state = dummy_state();
    let mut settings = (*state.config).clone();
    settings.websocket.max_message_connections = 1;
    state.config = std::sync::Arc::new(settings);
    let app = aleph_ccn::web::build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = format!("ws://{}/api/ws0/messages?history=0", addr);
    let (mut first, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    // Give the first connection time to bump the active counter.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (mut second, _resp) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), second.next())
        .await
        .unwrap()
        .expect("second connection closed")
        .expect("ws frame");
    match frame {
        TgMessage::Close(Some(cf)) => {
            assert_eq!(u16::from(cf.code), 1013);
        }
        other => panic!("expected close frame with 1013, got {other:?}"),
    }

    let _ = first.send(TgMessage::Close(None)).await;
    server.abort();
}
