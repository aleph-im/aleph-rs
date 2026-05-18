//! Shared integration-test fixtures. Ports `tests/conftest.py`.
//!
//! Most fixtures are slow (they need a real Postgres) so the helpers here that
//! return a `Pool` are gated behind `#[ignore]`-marked entry points in the
//! callers. Helpers that don't need a live database (sample messages,
//! in-memory storage engine, fake authority lookup) are usable unconditionally.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures_util::stream::BoxStream;
use parking_lot::Mutex;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio_postgres::NoTls;

use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use aleph_ccn::AlephResult;
use aleph_ccn::chains::signature_verifier::SignatureVerifier;
use aleph_ccn::config::{PostgresSettings, Settings};
use aleph_ccn::db::DbPool;
use aleph_ccn::db::accessors::aggregates::{insert_aggregate, insert_aggregate_element};
use aleph_ccn::db::accessors::files::{insert_message_file_pin, upsert_file};
use aleph_ccn::db::accessors::messages::{upsert_message, upsert_message_status};
use aleph_ccn::db::models::messages::MessageDb;
use aleph_ccn::db::models::pending_messages::PendingMessageDb;
use aleph_ccn::handlers::message_handler::{HandlersConfig, MessageHandler};
use aleph_ccn::permissions::{AuthorityLookup, MessageForAuth};
use aleph_ccn::services::storage::engine::StorageEngine;
use aleph_ccn::toolkit::constants::{
    DEFAULT_PRICE_AGGREGATE, DEFAULT_SETTINGS_AGGREGATE, PRICE_AGGREGATE_KEY,
    PRICE_AGGREGATE_OWNER, SETTINGS_AGGREGATE_KEY, SETTINGS_AGGREGATE_OWNER,
};
use aleph_ccn::types::files::FileType;
use aleph_ccn::types::message_status::{MessageOrigin, MessageStatus};
use aleph_ccn::web::AppState;

pub mod fixtures;

// ---------------------------------------------------------------------------
// In-memory storage engine — ports `tests/helpers/in_memory_storage_engine.py`.
// ---------------------------------------------------------------------------

/// Trivial in-memory `StorageEngine` for tests. Mirrors the Python
/// `InMemoryStorageEngine`.
pub struct InMemoryStorageEngine {
    pub files: Mutex<HashMap<String, Bytes>>,
}

impl InMemoryStorageEngine {
    pub fn new() -> Self {
        Self {
            files: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_files(files: HashMap<String, Bytes>) -> Self {
        Self {
            files: Mutex::new(files),
        }
    }
}

#[async_trait]
impl StorageEngine for InMemoryStorageEngine {
    async fn read(&self, filename: &str) -> AlephResult<Option<Bytes>> {
        Ok(self.files.lock().get(filename).cloned())
    }

    async fn read_iterator(
        &self,
        filename: &str,
        _chunk_size: usize,
    ) -> AlephResult<Option<BoxStream<'static, std::io::Result<Bytes>>>> {
        let body = match self.files.lock().get(filename).cloned() {
            Some(b) => b,
            None => return Ok(None),
        };
        let s = futures_util::stream::once(async move { Ok(body) });
        Ok(Some(Box::pin(s)))
    }

    async fn write(&self, filename: &str, content: &[u8]) -> AlephResult<()> {
        self.files
            .lock()
            .insert(filename.to_string(), Bytes::copy_from_slice(content));
        Ok(())
    }

    async fn delete(&self, filename: &str) -> AlephResult<()> {
        self.files.lock().remove(filename);
        Ok(())
    }

    async fn exists(&self, filename: &str) -> AlephResult<bool> {
        Ok(self.files.lock().contains_key(filename))
    }
}

// ---------------------------------------------------------------------------
// In-memory `AuthorityLookup` for permission tests.
// ---------------------------------------------------------------------------

/// Trivial in-memory authority lookup. Mirrors the `mocker.patch` shim used by
/// the Python permission tests — pre-load aggregates / original messages,
/// then exercise `check_sender_authorization` against them.
pub struct StubAuthorityLookup {
    pub aggregates: HashMap<String, Value>,
    pub messages: HashMap<String, FakeAuthMessage>,
}

impl StubAuthorityLookup {
    pub fn new() -> Self {
        Self {
            aggregates: HashMap::new(),
            messages: HashMap::new(),
        }
    }

    pub fn with_security_aggregate(mut self, owner: &str, content: Value) -> Self {
        self.aggregates.insert(owner.to_string(), content);
        self
    }

    pub fn with_original_message(mut self, item_hash: &str, msg: FakeAuthMessage) -> Self {
        self.messages.insert(item_hash.to_string(), msg);
        self
    }
}

#[async_trait]
impl AuthorityLookup for StubAuthorityLookup {
    async fn get_security_aggregate(&self, owner: &str) -> Option<Value> {
        // Case-insensitive lookup so tests can mix-case addresses freely.
        self.aggregates
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(owner))
            .map(|(_, v)| v.clone())
    }

    async fn get_message_by_item_hash(
        &self,
        item_hash: &str,
    ) -> Option<Box<dyn MessageForAuth + Send + Sync>> {
        self.messages
            .get(item_hash)
            .cloned()
            .map(|m| Box::new(m) as Box<dyn MessageForAuth + Send + Sync>)
    }
}

/// Lightweight `MessageForAuth` builder used by permissions tests.
#[derive(Clone, Debug)]
pub struct FakeAuthMessage {
    pub sender: String,
    pub chain: String,
    pub channel: Option<String>,
    pub mtype: MessageType,
    pub content_address: String,
    pub content_type: Option<String>,
    pub content_key: Option<String>,
    pub content_ref: Option<String>,
}

impl FakeAuthMessage {
    pub fn post(sender: &str, address: &str) -> Self {
        Self {
            sender: sender.to_string(),
            chain: "ETH".to_string(),
            channel: None,
            mtype: MessageType::Post,
            content_address: address.to_string(),
            content_type: Some("post".to_string()),
            content_key: None,
            content_ref: None,
        }
    }
}

impl MessageForAuth for FakeAuthMessage {
    fn sender(&self) -> &str {
        &self.sender
    }
    fn chain(&self) -> &str {
        &self.chain
    }
    fn channel(&self) -> Option<&str> {
        self.channel.as_deref()
    }
    fn message_type(&self) -> MessageType {
        self.mtype
    }
    fn content_address(&self) -> &str {
        &self.content_address
    }
    fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }
    fn content_key(&self) -> Option<&str> {
        self.content_key.as_deref()
    }
    fn content_ref(&self) -> Option<&str> {
        self.content_ref.as_deref()
    }
}

// ---------------------------------------------------------------------------
// Postgres testcontainer bootstrap. Mirrors `tests/conftest.py::session_factory`.
// ---------------------------------------------------------------------------

/// Owned Postgres + ready-to-use pool. Two backends:
/// - `Container`: testcontainers (Docker).
/// - `Embedded`: a postgres binary downloaded by `postgresql_embedded`. Used
///   when Docker isn't available — keeps the E2E suite runnable in CI.
pub struct PgFixture {
    _backend: PgBackend,
    pub pool: DbPool,
    pub settings: PostgresSettings,
}

enum PgBackend {
    Container(testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>),
    Embedded(Box<postgresql_embedded::PostgreSQL>),
}

/// Spin up a fresh Postgres, apply all migrations, return a pool. Tries
/// testcontainers first; on failure (no Docker) falls back to an embedded
/// `postgresql_embedded` install.
pub async fn start_postgres() -> PgFixture {
    if let Some(fx) = try_start_via_testcontainers().await {
        return fx;
    }
    start_via_embedded().await
}

async fn try_start_via_testcontainers() -> Option<PgFixture> {
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;

    if !docker_available() {
        return None;
    }
    let image = Postgres::default()
        .with_db_name("aleph")
        .with_user("aleph")
        .with_password("aleph-test");
    let container = image.start().await.ok()?;
    let host = container.get_host().await.ok()?.to_string();
    let port = container.get_host_port_ipv4(5432).await.ok()?;

    let settings = PostgresSettings {
        host,
        port,
        database: "aleph".into(),
        user: "aleph".into(),
        password: "aleph-test".into(),
        pool_size: 10,
        pool_pre_ping: false,
        pool_recycle: 3600,
    };
    let pool = aleph_ccn::db::connect(&settings).await.ok()?;
    aleph_ccn::db::migrate(&pool).await.ok()?;

    Some(PgFixture {
        _backend: PgBackend::Container(container),
        pool,
        settings,
    })
}

async fn start_via_embedded() -> PgFixture {
    use postgresql_embedded::{PostgreSQL, Settings as PgSettings};

    let mut pg_settings = PgSettings::default();
    // Use the zonky binary archive — sidesteps GitHub API rate-limits that
    // block the default Theseus archive in CI. postgresql_archive's `zonky`
    // strategy recognises the github.com/zonkyio prefix and internally fetches
    // the binary from Maven Central.
    pg_settings.releases_url = "https://github.com/zonkyio/embedded-postgres-binaries".into();
    let mut postgres = PostgreSQL::new(pg_settings);
    postgres.setup().await.expect("postgres embedded setup");
    postgres.start().await.expect("postgres embedded start");
    postgres
        .create_database("aleph")
        .await
        .expect("create database");

    // Use the random superuser credentials that postgresql_embedded generated
    // — they're stored on `postgres.settings()`.
    let pg = postgres.settings();
    let settings = PostgresSettings {
        host: pg.host.clone(),
        port: pg.port,
        database: "aleph".into(),
        user: pg.username.clone(),
        password: pg.password.clone(),
        pool_size: 10,
        pool_pre_ping: false,
        pool_recycle: 3600,
    };
    let pool = aleph_ccn::db::connect(&settings).await.expect("connect");
    aleph_ccn::db::migrate(&pool).await.expect("migrate");

    PgFixture {
        _backend: PgBackend::Embedded(Box::new(postgres)),
        pool,
        settings,
    }
}

/// Build an `AppState` against a live pool. Mirrors `ccn_test_aiohttp_app`.
pub fn make_app_state(pool: DbPool) -> AppState {
    let settings = Arc::new(Settings::default());
    let state = AppState::new(pool, settings);
    AppState {
        storage_engine: Some(Arc::new(InMemoryStorageEngine::new()) as Arc<dyn StorageEngine>),
        ..state
    }
}

/// Construct an HTTP router carrying just the controllers we actually exercise
/// in tests. Uses the production `aleph_ccn::web::build_router` now that the
/// axum 0.8 route patterns have been resolved.
pub fn build_app(state: AppState) -> Router {
    aleph_ccn::web::build_router(state)
}

/// Build a router that exposes the messages controller (and friends). Uses
/// the production router directly.
pub fn build_messages_app(state: AppState) -> Router {
    aleph_ccn::web::build_router(state)
}

// ---------------------------------------------------------------------------
// Dummy-state helper (no DB) for tests that only exercise routing layer.
// ---------------------------------------------------------------------------

/// Build an `AppState` whose pool is never used (max_size=0). Useful for
/// /version and other handler-level smoke tests.
pub fn dummy_state() -> AppState {
    use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
    let cfg = tokio_postgres::Config::new();
    let mgr = Manager::from_config(
        cfg,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(mgr).max_size(0).build().unwrap();
    AppState::new(pool, Arc::new(Settings::default()))
}

// ---------------------------------------------------------------------------
// Sample fixtures shared by many tests
// ---------------------------------------------------------------------------

/// Build a sample `PendingMessageDb` (mirrors `fixture_instance_message`).
pub fn sample_pending_message() -> PendingMessageDb {
    let content = json!({
        "address": "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
        "time": 1619017773.8950517,
        "type": "test",
    });
    let item_content = serde_json::to_string(&content).unwrap();
    PendingMessageDb {
        id: 0,
        item_hash: "734a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26".into(),
        r#type: MessageType::Post,
        chain: Chain::Ethereum,
        sender: "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba".into(),
        signature: Some("0xdeadbeef".into()),
        item_type: ItemType::Inline,
        item_content: Some(item_content),
        content: Some(content),
        time: ts(1619017773),
        channel: None,
        reception_time: ts(1619017774),
        check_message: false,
        next_attempt: dt(2023, 1, 1),
        retries: 0,
        tx_hash: None,
        fetched: true,
        origin: Some("p2p".into()),
    }
}

pub fn ts(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).unwrap()
}

pub fn dt(y: i32, m: u32, d: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap()
}

/// Insert a sample processed message into the live DB. Returns the row.
pub async fn insert_processed_message(pool: &DbPool, message: MessageDb) -> AlephResult<MessageDb> {
    let client = pool.get().await.unwrap();
    upsert_message(&**client, &message).await?;
    upsert_message_status(
        &**client,
        &message.item_hash,
        MessageStatus::Processed,
        message.reception_time,
        None,
    )
    .await?;
    Ok(message)
}

/// Insert the default settings + price aggregates. Mirrors the two
/// `fixture_*_aggregate_in_db` Python fixtures.
pub async fn insert_default_aggregates(pool: &DbPool) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    let creation = dt(2025, 1, 31);
    insert_aggregate_element(
        &**client,
        "7b74b9c5f73e7a0713dbe83a377b1d321ffb4a5411ea3df49790a9720b93a5bF",
        PRICE_AGGREGATE_KEY,
        PRICE_AGGREGATE_OWNER,
        &DEFAULT_PRICE_AGGREGATE,
        creation,
    )
    .await?;
    insert_aggregate(
        &**client,
        PRICE_AGGREGATE_KEY,
        PRICE_AGGREGATE_OWNER,
        &DEFAULT_PRICE_AGGREGATE,
        creation,
        "7b74b9c5f73e7a0713dbe83a377b1d321ffb4a5411ea3df49790a9720b93a5bF",
    )
    .await?;

    insert_aggregate_element(
        &**client,
        "a319a7216d39032212c2f11028a21efaac4e5f78254baa34001483c7af22b7a4",
        SETTINGS_AGGREGATE_KEY,
        SETTINGS_AGGREGATE_OWNER,
        &DEFAULT_SETTINGS_AGGREGATE,
        creation,
    )
    .await?;
    insert_aggregate(
        &**client,
        SETTINGS_AGGREGATE_KEY,
        SETTINGS_AGGREGATE_OWNER,
        &DEFAULT_SETTINGS_AGGREGATE,
        creation,
        "a319a7216d39032212c2f11028a21efaac4e5f78254baa34001483c7af22b7a4",
    )
    .await?;
    Ok(())
}

/// Insert a user balance row. Mirrors `user_balance` fixture.
pub async fn insert_user_balance(pool: &DbPool, address: &str, balance: u64) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    let chain_s: String = "ETH".into();
    let bal = Decimal::from(balance);
    let now = Utc::now();
    // V0059 made `balances.id` an IDENTITY column, so we omit it.
    client
        .execute(
            "INSERT INTO balances(address, chain, dapp, balance, eth_height, last_update) \
             VALUES ($1, $2, NULL, $3, 0, $4) \
             ON CONFLICT ON CONSTRAINT balances_address_chain_dapp_uindex \
             DO UPDATE SET balance = EXCLUDED.balance",
            &[&address, &chain_s, &bal, &now],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    Ok(())
}

/// Seed an `aleph_balance` row for an arbitrary chain. Mirrors
/// `AlephBalanceDb(...)` fixtures in the Python tests.
pub async fn seed_aleph_balance(
    pool: &DbPool,
    address: &str,
    chain: &str,
    balance: Decimal,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    let chain_s = chain.to_string();
    let now = Utc::now();
    client
        .execute(
            "INSERT INTO balances(address, chain, dapp, balance, eth_height, last_update) \
             VALUES ($1, $2, NULL, $3, 0, $4)",
            &[&address, &chain_s, &balance, &now],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    Ok(())
}

/// Insert an aggregate (with one element) for a `(owner, key)` pair. Used by
/// tests that need an existing aggregate to update / forget.
pub async fn seed_aggregate(
    pool: &DbPool,
    owner: &str,
    key: &str,
    content: Value,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    let creation = dt(2023, 1, 1);
    // Stable, content-derived "item hash" so re-runs are idempotent.
    let item_hash = format!("{:0>64x}", (owner.len() + key.len()) as u64);
    insert_aggregate_element(&**client, &item_hash, key, owner, &content, creation).await?;
    insert_aggregate(&**client, key, owner, &content, creation, &item_hash).await?;
    Ok(())
}

/// Insert a `(hash, size)` row in `files`. Mirrors `StoredFileDb(...)`.
pub async fn seed_file(pool: &DbPool, hash: &str, size: i64) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    upsert_file(&**client, hash, size, FileType::File).await?;
    Ok(())
}

/// Insert a `message`-type file pin. Mirrors `MessageFilePinDb`.
pub async fn seed_file_pin(
    pool: &DbPool,
    file_hash: &str,
    owner: &str,
    item_hash: &str,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    insert_message_file_pin(&**client, file_hash, Some(owner), item_hash, None, Utc::now()).await?;
    Ok(())
}

/// Build a `PendingMessageDb` from the supplied parts. Helper for tests that
/// drive the full pipeline.
pub fn build_pending_message(
    item_hash: &str,
    item_type: ItemType,
    message_type: MessageType,
    sender: &str,
    chain: aleph_types::chain::Chain,
    content: Option<Value>,
    item_content: Option<String>,
) -> PendingMessageDb {
    let time = ts(1_700_000_000);
    PendingMessageDb {
        id: 0,
        item_hash: item_hash.into(),
        r#type: message_type,
        chain,
        sender: sender.into(),
        signature: None,
        item_type,
        item_content,
        content,
        time,
        channel: None,
        reception_time: time,
        check_message: false,
        next_attempt: dt(2023, 1, 1),
        retries: 0,
        tx_hash: None,
        fetched: true,
        origin: Some("p2p".into()),
    }
}

/// Wire a `MessageHandler` against an in-memory storage engine, an
/// "always-allow" authority lookup and the supplied [`HandlersConfig`].
/// Mirrors the Python `message_processor` fixture.
pub fn build_message_handler(_pool: DbPool) -> Arc<MessageHandler> {
    let storage: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::new());
    let lookup: Arc<dyn AuthorityLookup> = Arc::new(StubAuthorityLookup::new());
    let cfg = HandlersConfig {
        balances_addresses: vec!["0xbalances".into()],
        balances_post_type: "balances".into(),
        credit_balances_addresses: vec!["0xcredits".into()],
        credit_balances_post_types: vec!["aleph_credit_distribution".into()],
        credit_balances_channels: Vec::new(),
        storage_grace_period_hours: 24,
        max_unauthenticated_upload_file_size: 25 * 1024 * 1024,
        ipfs_enabled: false,
        store_files: true,
        ipfs_stat_timeout: 5,
        api_servers: Vec::new(),
    };
    let signature_verifier = Arc::new(SignatureVerifier::new());
    Arc::new(MessageHandler::new(
        signature_verifier,
        storage,
        None,
        lookup,
        &cfg,
    ))
}

/// Variant that exposes the InMemoryStorageEngine. Tests that need to seed
/// file contents reuse the engine for both `Arc<dyn StorageEngine>` and as
/// the concrete type.
pub fn build_message_handler_with_storage(
    _pool: DbPool,
    storage: Arc<InMemoryStorageEngine>,
) -> Arc<MessageHandler> {
    let storage_dyn: Arc<dyn StorageEngine> = storage.clone();
    let lookup: Arc<dyn AuthorityLookup> = Arc::new(StubAuthorityLookup::new());
    let cfg = HandlersConfig {
        balances_addresses: vec!["0xbalances".into()],
        balances_post_type: "balances".into(),
        credit_balances_addresses: vec!["0xcredits".into()],
        credit_balances_post_types: vec!["aleph_credit_distribution".into()],
        credit_balances_channels: Vec::new(),
        storage_grace_period_hours: 24,
        max_unauthenticated_upload_file_size: 25 * 1024 * 1024,
        ipfs_enabled: false,
        store_files: true,
        ipfs_stat_timeout: 5,
        api_servers: Vec::new(),
    };
    let signature_verifier = Arc::new(SignatureVerifier::new());
    Arc::new(MessageHandler::new(
        signature_verifier,
        storage_dyn,
        None,
        lookup,
        &cfg,
    ))
}

/// Convenience: insert a `pending_messages` row.
pub async fn insert_pending_row(
    pool: &DbPool,
    pending: &mut PendingMessageDb,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    let r#type_s = serde_json::to_value(&pending.r#type)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let chain_s = serde_json::to_value(&pending.chain)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let item_type_s = serde_json::to_value(pending.item_type)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let channel_s: Option<String> = pending
        .channel
        .as_ref()
        .and_then(|c| serde_json::to_value(c).ok())
        .and_then(|v| v.as_str().map(|s| s.to_string()));
    let row = client
        .query_one(
            "INSERT INTO pending_messages(item_hash, type, chain, sender, signature, item_type, \
                                            item_content, content, time, channel, reception_time, \
                                            check_message, next_attempt, retries, tx_hash, fetched, origin) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) \
             RETURNING id",
            &[
                &pending.item_hash,
                &r#type_s,
                &chain_s,
                &pending.sender,
                &pending.signature,
                &item_type_s,
                &pending.item_content,
                &pending.content,
                &pending.time,
                &channel_s,
                &pending.reception_time,
                &pending.check_message,
                &pending.next_attempt,
                &pending.retries,
                &pending.tx_hash,
                &pending.fetched,
                &pending.origin,
            ],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    pending.id = row.get(0);
    Ok(())
}

/// Re-export so tests can `use common::MessageOrigin`.
pub use aleph_ccn::types::message_status::MessageOrigin as ReExportedMessageOrigin;
// Silence unused-warning when MessageOrigin only re-exported.
#[allow(dead_code)]
fn _use_message_origin(_: MessageOrigin) {}

// ---------------------------------------------------------------------------
// Marker for tests that need docker. Use as `if skip_if_no_docker() { return; }`
// ---------------------------------------------------------------------------

pub fn docker_available() -> bool {
    // Cheap probe — testcontainers also checks this, but doing it here lets us
    // skip the test up-front rather than panic during container start-up.
    std::process::Command::new("docker")
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

