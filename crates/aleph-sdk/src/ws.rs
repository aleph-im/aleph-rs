use crate::client::{MessageError, WsMessageFilter};
use aleph_types::message::Message;
use futures_util::StreamExt;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use url::Url;

const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 30_000;
const CHANNEL_BUFFER_SIZE: usize = 100;

/// Builds the websocket URL with query parameters from the filter.
fn build_ws_url(base_url: &Url, filter: &WsMessageFilter) -> Result<Url, MessageError> {
    let scheme = match base_url.scheme() {
        "https" => "wss",
        "http" => "ws",
        s => s,
    };

    let mut ws_url = base_url.clone();
    ws_url
        .set_scheme(scheme)
        .map_err(|_| MessageError::WebsocketConnection("Failed to set scheme".to_string()))?;

    ws_url.set_path("/api/ws0/messages");

    let query = serde_qs::to_string(filter).map_err(|e| {
        MessageError::WebsocketConnection(format!("Failed to serialize filter: {e}"))
    })?;

    if !query.is_empty() {
        ws_url.set_query(Some(&query));
    }

    Ok(ws_url)
}

/// Spawns a background task that manages the websocket connection and returns a receiver stream.
pub async fn subscribe(
    base_url: Url,
    filter: WsMessageFilter,
) -> Result<mpsc::Receiver<Result<Message, MessageError>>, MessageError> {
    let ws_url = build_ws_url(&base_url, &filter)?;

    // Try initial connection to fail fast if URL is invalid
    let (ws_stream, _) = connect_async(ws_url.as_str()).await.map_err(|e| {
        MessageError::WebsocketConnection(format!("Initial connection failed: {e}"))
    })?;

    let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);

    tokio::spawn(run_ws_loop(ws_url, ws_stream, tx));

    Ok(rx)
}

async fn run_ws_loop(
    ws_url: Url,
    initial_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    tx: mpsc::Sender<Result<Message, MessageError>>,
) {
    let mut ws_stream = initial_stream;
    let mut backoff_ms = INITIAL_BACKOFF_MS;

    loop {
        let (_, mut read) = ws_stream.split();

        // Process messages until disconnection
        while let Some(msg_result) = read.next().await {
            match msg_result {
                Ok(WsMessage::Text(text)) => {
                    // Reset backoff on successful message
                    backoff_ms = INITIAL_BACKOFF_MS;

                    let parse_result: Result<Message, _> = serde_json::from_str(&text);
                    let item = match parse_result {
                        Ok(msg) => Ok(msg),
                        Err(e) => Err(MessageError::WebsocketMessage(format!(
                            "Failed to parse message: {e}"
                        ))),
                    };

                    if tx.send(item).await.is_err() {
                        // Receiver dropped, exit the loop
                        return;
                    }
                }
                Ok(WsMessage::Close(_)) => {
                    // Server closed connection, break to reconnect
                    break;
                }
                Ok(_) => {
                    // Ignore ping/pong/binary messages
                }
                Err(e) => {
                    // Connection error, break to reconnect
                    let _ = tx
                        .send(Err(MessageError::WebsocketMessage(format!(
                            "Connection error: {e}"
                        ))))
                        .await;
                    break;
                }
            }
        }

        // Reconnection loop with exponential backoff
        loop {
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);

            match connect_async(ws_url.as_str()).await {
                Ok((new_stream, _)) => {
                    ws_stream = new_stream;
                    break;
                }
                Err(e) => {
                    if tx
                        .send(Err(MessageError::WebsocketConnection(format!(
                            "Reconnection failed: {e}"
                        ))))
                        .await
                        .is_err()
                    {
                        // Receiver dropped
                        return;
                    }
                }
            }
        }
    }
}
