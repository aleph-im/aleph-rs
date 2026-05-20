//! Version endpoint. Mirrors `aleph/web/controllers/version.py`.

use axum::{Json, Router, routing::get};
use serde::Serialize;

use crate::web::AppState;

#[derive(Debug, Serialize)]
struct VersionResponse {
    version: &'static str,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/version", get(handle))
        .route("/api/v0/version", get(handle))
}

async fn handle() -> Json<VersionResponse> {
    Json(VersionResponse {
        version: crate::VERSION,
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
    async fn version_endpoint_returns_version_payload() {
        let state = dummy_state();
        let app = axum::Router::new().merge(routes()).with_state(state);
        let resp = app.clone()
            .oneshot(
                Request::builder()
                    .uri("/version")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let bytes = to_bytes(resp.into_body(), 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["version"], crate::VERSION);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v0/version")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
}
