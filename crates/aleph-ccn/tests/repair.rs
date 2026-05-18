//! Integration tests for `repair_node`. Mirrors `tests/test_repair.py` —
//! specifically the file-size fix-up path.

mod common;

use std::sync::Arc;
use std::time::Duration;

use aleph_ccn::db::accessors::files::get_file;
use aleph_ccn::repair::fix_file_sizes;
use aleph_ccn::services::ipfs::IpfsService;
use aleph_ccn::services::ipfs::common::IpfsEndpoint;
use aleph_ccn::services::p2p::jobs::ApiServerLookup;
use aleph_ccn::services::storage::engine::StorageEngine;
use aleph_ccn::services::storage::in_memory::InMemoryStorageEngine;
use aleph_ccn::storage::StorageService;
use aleph_ccn::types::files::FileType;
use aleph_ccn::AlephResult;

use common::start_postgres;

struct EmptyApiServers;

#[async_trait::async_trait]
impl ApiServerLookup for EmptyApiServers {
    async fn get_api_servers(&self) -> AlephResult<Vec<String>> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn fix_file_sizes_patches_negative_size_rows() {
    let pg = start_postgres().await;
    let pool = pg.pool.clone();

    // Use a 64-char hex hash so item_type_from_hash classifies it as STORAGE
    // (= local engine); the file content is 11 bytes.
    let hash = "0".repeat(63) + "1";
    let content = b"hello world";

    let engine: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::new());
    engine.write(&hash, content).await.unwrap();

    {
        let client = pool.get().await.unwrap();
        client
            .execute(
                "INSERT INTO files(hash, size, type) VALUES ($1, -1, $2)",
                &[&hash, &"file"],
            )
            .await
            .unwrap();
    }

    let ipfs = Arc::new(IpfsService::from_parts(
        reqwest::Client::new(),
        None,
        IpfsEndpoint {
            scheme: "http".into(),
            host: "127.0.0.1".into(),
            port: 1,
            timeout: Duration::from_millis(1),
        },
        IpfsEndpoint {
            scheme: "http".into(),
            host: "127.0.0.1".into(),
            port: 1,
            timeout: Duration::from_millis(1),
        },
    ));
    let cache: Arc<dyn ApiServerLookup> = Arc::new(EmptyApiServers);
    let storage_service = StorageService::new(engine, ipfs, cache)
        .with_ipfs_enabled(false)
        .with_http_p2p_enabled(false);

    fix_file_sizes(&pool, &storage_service, false).await.unwrap();

    let client = pool.get().await.unwrap();
    let row = get_file(&**client, &hash).await.unwrap().unwrap();
    assert_eq!(row.size, content.len() as i64);
    assert_eq!(row.r#type, FileType::File);
}
