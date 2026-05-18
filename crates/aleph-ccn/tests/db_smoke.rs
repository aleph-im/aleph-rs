//! End-to-end migration smoke test.
//!
//! Runs the embedded refinery migration set against a freshly-started Postgres
//! container (testcontainers). Marked `#[ignore]` so CI can opt in explicitly
//! with `cargo test -- --ignored`.

#![cfg(feature = "_e2e")]

use aleph_ccn::config::PostgresSettings;

#[tokio::test]
#[ignore = "requires docker; run with --features _e2e -- --ignored"]
async fn migrations_run_clean_on_empty_postgres() {
    let _ = tracing_subscriber::fmt::try_init();

    // The actual implementation will pull a postgres image via testcontainers.
    // Kept gated behind a feature flag until the dependency lands.
    let cfg = PostgresSettings::default();
    let pool = aleph_ccn::db::connect(&cfg).await.expect("connect");
    aleph_ccn::db::migrate(&pool).await.expect("migrate");
}
