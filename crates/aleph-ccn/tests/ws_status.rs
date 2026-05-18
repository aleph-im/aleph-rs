//! Smoke-test for `/api/ws0/status`. Boots the production router on a
//! random port, connects via a tungstenite client, and asserts the connect
//! either receives at least one metrics frame OR is rejected when the
//! connection cap is exhausted.
//!
//! The status handler polls `get_metrics`, which requires a working DB pool.
//! `dummy_state()` builds an empty pool, so we bypass the metrics path by
//! validating the connection-cap behaviour and a 1013 rejection.

mod common;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as TgMessage;

use common::dummy_state;

#[tokio::test]
async fn ws_status_connection_cap_rejects_with_1013() {
    let mut state = dummy_state();
    let mut settings = (*state.config).clone();
    settings.websocket.max_status_connections = 1;
    // 1 second heartbeat so the loop is responsive.
    settings.websocket.heartbeat = 1;
    state.config = std::sync::Arc::new(settings);
    let app = aleph_ccn::web::build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = format!("ws://{}/api/ws0/status", addr);
    let (mut first, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (mut second, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), second.next())
        .await
        .unwrap()
        .expect("second connection closed")
        .expect("ws frame");
    match frame {
        TgMessage::Close(Some(cf)) => assert_eq!(u16::from(cf.code), 1013),
        other => panic!("expected 1013 close, got {other:?}"),
    }

    let _ = first.send(TgMessage::Close(None)).await;
    server.abort();
}

#[tokio::test]
async fn ws_status_handshake_succeeds() {
    // Reaching get_metrics requires a real pool; this test only verifies the
    // upgrade handshake. We immediately close the connection after upgrade.
    let state = dummy_state();
    let app = aleph_ccn::web::build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = format!("ws://{}/api/ws0/status", addr);
    let (mut ws, resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("ws connect");
    assert_eq!(resp.status(), http::StatusCode::SWITCHING_PROTOCOLS);
    let _ = ws.send(TgMessage::Close(None)).await;
    server.abort();
}
