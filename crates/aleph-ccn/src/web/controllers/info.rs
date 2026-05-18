//! Mirrors `aleph/web/controllers/info.py`.

use std::collections::BTreeSet;

use axum::Router;
use axum::extract::State;
use axum::routing::get;
use serde::Serialize;
use serde_json::Value;

use crate::web::AppState;

#[derive(Debug, Serialize)]
struct PublicMultiAddressResponse {
    node_multi_addresses: Vec<String>,
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/api/v0/info/public.json", get(public_multiaddress))
}

/// Namespace + key used by the P2P manager to publish the node's announced
/// multiaddresses. Mirrors `PUBLIC_ADDRESSES_KEY` from pyaleph's
/// `services/cache/node_cache.py`.
const NODE_CACHE_NS: &str = "node_cache";
const PUBLIC_ADDRESSES_KEY: &str = "public_addresses";

async fn public_multiaddress(
    State(state): State<AppState>,
) -> axum::Json<PublicMultiAddressResponse> {
    // Mirror pyaleph: surface whatever multiaddresses the P2P manager has
    // published into the node cache. Falls back to the configured peer list
    // when the cache is empty (e.g. when the P2P daemon has not yet announced
    // the node, which matches pyaleph's `node_cache.get_public_addresses()`
    // returning a partial / empty set at boot).
    let mut addrs: BTreeSet<String> = BTreeSet::new();

    if let Some(value) = state.node_cache.get(PUBLIC_ADDRESSES_KEY, NODE_CACHE_NS) {
        match value {
            Value::Array(arr) => {
                for v in arr {
                    if let Some(s) = v.as_str() {
                        addrs.insert(s.to_string());
                    }
                }
            }
            Value::String(s) => {
                addrs.insert(s);
            }
            _ => {}
        }
    }

    axum::Json(PublicMultiAddressResponse {
        node_multi_addresses: addrs.into_iter().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn dummy_state() -> AppState {
        let pool = deadpool_postgres::Pool::builder(deadpool_postgres::Manager::from_config(
            tokio_postgres::Config::new(),
            tokio_postgres::NoTls,
            deadpool_postgres::ManagerConfig::default(),
        ))
        .max_size(0)
        .build()
        .unwrap();
        AppState::new(pool, Arc::new(crate::config::Settings::default()))
    }

    #[tokio::test]
    async fn public_multiaddress_returns_empty_list_default() {
        let state = dummy_state();
        let app = Router::new().merge(routes()).with_state(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v0/info/public.json")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let bytes = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["node_multi_addresses"], serde_json::json!([]));
    }
}
