//! Integration tests for the garbage collector. Mirrors
//! `tests/services/test_garbage_collector.py`.
//!
//! Requires a live Postgres backend (docker or `postgresql_embedded`); the
//! tests early-return when neither is available.

mod common;

use std::sync::Arc;

use chrono::Utc;

use aleph_ccn::db::accessors::files::{
    file_pin_exists, get_file, get_unpinned_files, insert_grace_period_file_pin,
    insert_message_file_pin, upsert_file,
};
use aleph_ccn::db::accessors::messages::upsert_message_status;
use aleph_ccn::services::storage::engine::StorageEngine;
use aleph_ccn::services::storage::garbage_collector::GarbageCollector;
use aleph_ccn::services::storage::in_memory::InMemoryStorageEngine;
use aleph_ccn::types::files::FileType;
use aleph_ccn::types::message_status::MessageStatus;

use common::{start_postgres};

#[tokio::test]
#[ignore = "requires docker/embedded postgres; run with --ignored"]
async fn collect_deletes_orphan_files_and_keeps_pinned_ones() {
    let pg = start_postgres().await;
    let pool = pg.pool.clone();
    let engine: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::new());

    // Hash A is pinned by a message — must survive.
    // Hash B is unpinned — must be deleted.
    // Hash C has a grace-period pin that already expired — must be deleted.
    let pinned = "a".repeat(64);
    let orphan = "b".repeat(64);
    let grace_expired = "c".repeat(64);

    let now = Utc::now();
    {
        let client = pool.get().await.unwrap();
        for h in [&pinned, &orphan, &grace_expired] {
            upsert_file(&**client, h, 4, FileType::File).await.unwrap();
        }
        insert_message_file_pin(&**client, &pinned, Some("0xowner"), "msg-hash-1", None, now)
            .await
            .unwrap();
        // grace-period pin that has already expired
        insert_grace_period_file_pin(
            &**client,
            &grace_expired,
            now - chrono::Duration::days(2),
            now - chrono::Duration::days(1),
            Some("msg-hash-2"),
            Some("0xowner"),
            None,
        )
        .await
        .unwrap();
    }
    for h in [&pinned, &orphan, &grace_expired] {
        engine.write(h, b"data").await.unwrap();
    }

    let gc = GarbageCollector::new(pool.clone(), engine.clone(), None, 24);
    gc.collect(now).await.unwrap();

    let client = pool.get().await.unwrap();
    assert!(get_file(&**client, &pinned).await.unwrap().is_some());
    assert!(get_file(&**client, &orphan).await.unwrap().is_none());
    assert!(get_file(&**client, &grace_expired).await.unwrap().is_none());
    assert!(engine.exists(&pinned).await.unwrap());
    assert!(!engine.exists(&orphan).await.unwrap());
    assert!(!engine.exists(&grace_expired).await.unwrap());
    // Sanity: no more unpinned files left.
    let leftover = get_unpinned_files(&**client).await.unwrap();
    assert!(leftover.is_empty());
}

#[tokio::test]
#[ignore = "requires docker/embedded postgres; run with --ignored"]
async fn removing_messages_become_removed_when_resources_are_gone() {
    let pg = start_postgres().await;
    let pool = pg.pool.clone();
    let engine: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::new());

    let item_hash = "d".repeat(64);
    let store_item_hash = "e".repeat(64);
    let pinned_item_hash = "f".repeat(64);
    let pinned_file_hash = "1".repeat(64);

    let now = Utc::now();
    {
        let client = pool.get().await.unwrap();
        // A non-STORE REMOVING message — should transition unconditionally.
        client
            .execute(
                "INSERT INTO messages(item_hash, type, chain, sender, signature, item_type, \
                 item_content, content, time, channel, size, status, reception_time, owner, \
                 content_type, content_ref, content_key, content_item_hash, first_confirmed_at, \
                 first_confirmed_height, payment_type, tags) \
                 VALUES ($1, 'POST', 'ETH', '0xs', NULL, 'inline', NULL, '{}'::jsonb, $2, NULL, 0, \
                 'removing', $2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
                &[&item_hash, &now],
            )
            .await
            .unwrap();
        upsert_message_status(&**client, &item_hash, MessageStatus::Removing, now, None)
            .await
            .unwrap();

        // A STORE REMOVING message that has no remaining file pin — should
        // transition.
        client
            .execute(
                "INSERT INTO messages(item_hash, type, chain, sender, signature, item_type, \
                 item_content, content, time, channel, size, status, reception_time, owner, \
                 content_type, content_ref, content_key, content_item_hash, first_confirmed_at, \
                 first_confirmed_height, payment_type, tags) \
                 VALUES ($1, 'STORE', 'ETH', '0xs', NULL, 'inline', NULL, '{}'::jsonb, $2, NULL, \
                 0, 'removing', $2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
                &[&store_item_hash, &now],
            )
            .await
            .unwrap();
        upsert_message_status(
            &**client,
            &store_item_hash,
            MessageStatus::Removing,
            now,
            None,
        )
        .await
        .unwrap();

        // A STORE REMOVING message that STILL has a file pin — must NOT
        // transition (resources_deleted=false branch).
        client
            .execute(
                "INSERT INTO messages(item_hash, type, chain, sender, signature, item_type, \
                 item_content, content, time, channel, size, status, reception_time, owner, \
                 content_type, content_ref, content_key, content_item_hash, first_confirmed_at, \
                 first_confirmed_height, payment_type, tags) \
                 VALUES ($1, 'STORE', 'ETH', '0xs', NULL, 'inline', NULL, '{}'::jsonb, $2, NULL, \
                 0, 'removing', $2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
                &[&pinned_item_hash, &now],
            )
            .await
            .unwrap();
        upsert_message_status(
            &**client,
            &pinned_item_hash,
            MessageStatus::Removing,
            now,
            None,
        )
        .await
        .unwrap();
        upsert_file(&**client, &pinned_file_hash, 4, FileType::File)
            .await
            .unwrap();
        insert_message_file_pin(
            &**client,
            &pinned_file_hash,
            Some("0xowner"),
            &pinned_item_hash,
            None,
            now,
        )
        .await
        .unwrap();
    }

    let gc = GarbageCollector::new(pool.clone(), engine.clone(), None, 24);
    gc.check_and_update_removing_messages().await.unwrap();

    let client = pool.get().await.unwrap();
    let status_of = |ih: String| {
        let pool = pool.clone();
        async move {
            let c = pool.get().await.unwrap();
            let row = c
                .query_one(
                    "SELECT status FROM message_status WHERE item_hash = $1",
                    &[&ih],
                )
                .await
                .unwrap();
            row.get::<_, String>(0)
        }
    };
    drop(client);

    assert_eq!(status_of(item_hash.clone()).await, "removed");
    assert_eq!(status_of(store_item_hash.clone()).await, "removed");
    assert_eq!(status_of(pinned_item_hash.clone()).await, "removing");

    // Ensure the still-pinned file is still pinned.
    let client = pool.get().await.unwrap();
    assert!(file_pin_exists(&**client, &pinned_item_hash).await.unwrap());

    // The denormalized `messages.status` column should also be updated for the
    // transitioned rows.
    let row = client
        .query_one(
            "SELECT status FROM messages WHERE item_hash = $1",
            &[&item_hash],
        )
        .await
        .unwrap();
    let status: String = row.get(0);
    assert_eq!(status, "removed");
}
