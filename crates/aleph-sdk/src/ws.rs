use crate::client::{MessageError, MessageFilter};
use aleph_types::message::Message;
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{self, Message as WsMessage};
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
};
use url::Url;

const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 30_000;
const CHANNEL_BUFFER_SIZE: usize = 100;

/// Builds the websocket URL with query parameters from the filter.
fn build_ws_url(
    base_url: &Url,
    filter: &MessageFilter,
    history: Option<u32>,
) -> Result<Url, MessageError> {
    let scheme = match base_url.scheme() {
        "https" => "wss",
        "http" => "ws",
        s => s,
    };

    let mut ws_url = base_url.clone();
    ws_url
        .set_scheme(scheme)
        .map_err(|_| MessageError::WebsocketBadScheme)?;

    ws_url.set_path("/api/ws0/messages");

    let mut query = serde_qs::to_string(filter).map_err(MessageError::WebsocketSerializeFilter)?;

    if let Some(history_value) = history {
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str(&format!("history={}", history_value));
    }

    if !query.is_empty() {
        ws_url.set_query(Some(&query));
    }

    Ok(ws_url)
}

/// Spawns a background task that manages the websocket connection and returns a receiver stream.
pub async fn subscribe(
    base_url: Url,
    filter: &MessageFilter,
    history: Option<u32>,
) -> Result<mpsc::Receiver<Result<Message, MessageError>>, MessageError> {
    let ws_url = build_ws_url(&base_url, filter, history)?;

    // Try initial connection to fail fast if URL is invalid
    let (ws_stream, _) = connect(ws_url.as_str())
        .await
        .map_err(|e| MessageError::WebsocketConnect(Box::new(e)))?;

    let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);

    tokio::spawn(run_ws_loop(ws_url, ws_stream, tx));

    Ok(rx)
}

pub(crate) type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Opens a websocket, trusting the system root store for `wss://`.
///
/// The TLS config names its crypto provider: tokio-tungstenite's default
/// connector asks rustls for the process-level one, which panics when both the
/// `ring` and `aws_lc_rs` rustls features are enabled (as they are with
/// `vprogram`).
pub(crate) async fn connect(
    url: &str,
) -> Result<(WsStream, tungstenite::handshake::client::Response), tungstenite::Error> {
    let connector = Connector::Rustls(Arc::new(tls_config()?));
    connect_async_tls_with_config(url, None, false, Some(connector)).await
}

fn tls_config() -> std::io::Result<rustls::ClientConfig> {
    let native = rustls_native_certs::load_native_certs();
    let mut roots = rustls::RootCertStore::empty();
    let (added, _ignored) = roots.add_parsable_certificates(native.certs);
    if added == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "no usable root CA certificates found (errors: {:?})",
                native.errors
            ),
        ));
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(std::io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(config)
}

async fn run_ws_loop(
    ws_url: Url,
    initial_stream: WsStream,
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

                    let item = serde_json::from_str::<Message>(&text)
                        .map_err(MessageError::WebsocketParse);

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
                        .send(Err(MessageError::WebsocketStream(Box::new(e))))
                        .await;
                    break;
                }
            }
        }

        // Reconnection loop with exponential backoff
        loop {
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);

            match connect(ws_url.as_str()).await {
                Ok((new_stream, _)) => {
                    ws_stream = new_stream;
                    break;
                }
                Err(e) => {
                    if tx
                        .send(Err(MessageError::WebsocketConnect(Box::new(e))))
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
