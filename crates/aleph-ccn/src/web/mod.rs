//! HTTP API surface. Mirrors `aleph/web/`.
//!
//! Routes are registered in `controllers::routes`. The router is built from
//! an [`AppState`] that bundles the database pool and active config.

pub mod controllers;

use std::sync::Arc;

use axum::Router;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::AlephResult;
use crate::chains::signature_verifier::SignatureVerifier;
use crate::config::Settings;
use crate::db::DbPool;
use crate::services::cache::local::LocalCache;
use crate::services::ipfs::IpfsService;
use crate::services::storage::engine::StorageEngine;

/// Capacity of the in-process WS-broadcast channel. Each `/api/ws0/messages`
/// subscriber buys a slot; slow subscribers that fall behind the head will
/// receive a `RecvError::Lagged(n)` and be dropped. Sized to comfortably absorb
/// short bursts of processed messages.
const MESSAGE_BROADCAST_CAPACITY: usize = 2048;

/// Shared application state injected into every handler.
#[derive(Clone)]
pub struct AppState {
    pub pool: DbPool,
    pub config: Arc<Settings>,
    pub node_cache: Arc<LocalCache>,
    pub signature_verifier: Arc<SignatureVerifier>,
    pub ipfs_service: Option<Arc<IpfsService>>,
    pub storage_engine: Option<Arc<dyn StorageEngine>>,
    /// In-process broadcast channel used by `/api/ws0/messages` to fan out
    /// freshly-processed messages to every connected WS client. The runtime
    /// (or tests) calls `sender.send(payload)` when a message is processed.
    pub message_broadcast: tokio::sync::broadcast::Sender<serde_json::Value>,
    /// Counter of currently-connected `/api/ws0/messages` clients. Used to
    /// enforce `config.websocket.max_message_connections`.
    pub ws_messages_active: Arc<std::sync::atomic::AtomicU32>,
    /// Counter of currently-connected `/api/ws0/status` clients. Used to
    /// enforce `config.websocket.max_status_connections`.
    pub ws_status_active: Arc<std::sync::atomic::AtomicU32>,
}

impl AppState {
    /// Build a bare AppState from a pool + config. Optional services default to None.
    pub fn new(pool: DbPool, config: Arc<Settings>) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(MESSAGE_BROADCAST_CAPACITY);
        Self {
            pool,
            config,
            node_cache: Arc::new(LocalCache::new()),
            signature_verifier: Arc::new(SignatureVerifier::new()),
            ipfs_service: None,
            storage_engine: None,
            message_broadcast: tx,
            ws_messages_active: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            ws_status_active: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }
}

pub fn build_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    // `/` is owned by `controllers::main` (pyaleph index page); `/api/v0/version`
    // is owned by `controllers::version`. We delegate to the controllers tree.
    Router::new()
        .merge(controllers::routes::router(state.clone()))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

/// Bind + serve the router. Returns when the listener is closed.
pub async fn serve(state: AppState, host: &str, port: u16) -> AlephResult<()> {
    let listener = TcpListener::bind((host, port))
        .await
        .map_err(crate::AlephError::Io)?;
    let app = build_router(state);
    tracing::info!("HTTP API listening on {host}:{port}");
    axum::serve(listener, app)
        .await
        .map_err(crate::AlephError::Io)?;
    Ok(())
}
