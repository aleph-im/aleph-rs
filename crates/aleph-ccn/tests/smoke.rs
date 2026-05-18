//! Smoke tests: the crate builds, default config loads, the version endpoint
//! responds.

use aleph_ccn::config::Settings;

#[test]
fn default_settings_load() {
    let s = Settings::default();
    assert_eq!(s.postgres.port, 5432);
    assert_eq!(s.p2p.http_port, 4024);
    assert_eq!(s.ipfs.port, 5001);
    assert_eq!(s.rabbitmq.port, 5672);
    assert_eq!(s.redis.port, 6379);
}

#[test]
fn default_settings_url_format() {
    let s = Settings::default();
    let url = s.postgres.url();
    assert!(url.starts_with("postgres://"));
    assert!(url.contains("@postgres:5432/aleph"));
}

#[tokio::test]
async fn version_endpoint_works() {
    // Build a router directly with an empty pool — we just want to hit /version.
    // The Pool's manager won't connect until first acquire, so this is cheap.
    use axum::body::Body;
    use http::{Request, StatusCode};
    use tower::ServiceExt;

    // We can't easily construct a deadpool::Pool without a manager, so build the
    // version sub-router directly.
    let app = aleph_ccn::web::controllers::version::routes().with_state(dummy_state());
    let response = app
        .oneshot(Request::get("/version").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));
}

fn dummy_state() -> aleph_ccn::web::AppState {
    // We can't actually connect to Postgres for this smoke test; create a pool
    // that's never used. deadpool's builder lets us configure max_size=0 which
    // never tries to spawn connections.
    use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
    use tokio_postgres::{Config, NoTls};

    let cfg = Config::new();
    let mgr = Manager::from_config(
        cfg,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(mgr).max_size(0).build().unwrap();

    aleph_ccn::web::AppState::new(pool, std::sync::Arc::new(Settings::default()))
}
