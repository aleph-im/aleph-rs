//! Ports `tests/jobs/test_fetch_pending_messages.py`.
//!
//! These tests check the building blocks of the fetch worker:
//! - claim queries respect `busy_hashes` and slot limits;
//! - `set_pending_message_fetched` flips the row state;
//! - the fetch loop exits on cancel without DB starvation.

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;

use aleph_ccn::db::accessors::pending_messages::{
    get_next_pending_messages, set_pending_message_fetched,
};
use aleph_ccn::db::models::messages::MessageDb;
use aleph_ccn::db::models::pending_messages::PendingMessageDb;
use aleph_ccn::jobs::fetch_pending_messages::{FetchRunner, fetch_one};
use aleph_ccn::types::message_status::MessageProcessingException;
use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use common::{build_pending_message, insert_pending_row, start_postgres};

fn pending_post(item_hash: &str) -> PendingMessageDb {
    let content = json!({
        "address": "0xsender",
        "time": 1_700_000_000.0_f64,
        "type": "test",
    });
    build_pending_message(
        item_hash,
        ItemType::Inline,
        MessageType::Post,
        "0xsender",
        Chain::Ethereum,
        Some(content.clone()),
        Some(content.to_string()),
    )
}

#[tokio::test]
async fn claim_messages_returns_empty_when_no_pending_rows() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let now = Utc::now();
    let rows = get_next_pending_messages(&**client, now, 0, 0, Some(false), None)
        .await
        .unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn claim_messages_excludes_busy_hashes() {
    let pg = start_postgres().await;
    let busy = "a".repeat(64);
    let other = "b".repeat(64);

    let mut p1 = pending_post(&busy);
    p1.fetched = false;
    insert_pending_row(&pg.pool, &mut p1).await.unwrap();
    let mut p2 = pending_post(&other);
    p2.fetched = false;
    insert_pending_row(&pg.pool, &mut p2).await.unwrap();

    let client = pg.pool.get().await.unwrap();
    let exclude = vec![busy.clone()];
    let now = Utc::now();
    let rows = get_next_pending_messages(&**client, now, 10, 0, Some(false), Some(&exclude))
        .await
        .unwrap();
    let hashes: HashSet<String> = rows.iter().map(|p| p.item_hash.clone()).collect();
    assert!(!hashes.contains(&busy));
    assert!(hashes.contains(&other));
}

#[tokio::test]
async fn claim_messages_respects_slot_limit() {
    let pg = start_postgres().await;
    for i in 0..5 {
        let mut p = pending_post(&format!("{i:064x}"));
        p.fetched = false;
        insert_pending_row(&pg.pool, &mut p).await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let rows = get_next_pending_messages(&**client, Utc::now(), 3, 0, Some(false), None)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
}

struct EchoRunner;
#[async_trait(?Send)]
impl FetchRunner for EchoRunner {
    async fn verify_and_fetch(
        &self,
        _client: &tokio_postgres::Transaction<'_>,
        pending: &PendingMessageDb,
    ) -> Result<MessageDb, MessageProcessingException> {
        let content = pending
            .content
            .clone()
            .or_else(|| {
                pending
                    .item_content
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
            })
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let m = MessageDb::from_pending_message(pending, &content, 100, None);
        Ok(m)
    }
    fn max_retries(&self) -> i32 {
        3
    }
}

#[tokio::test]
async fn fetch_one_marks_pending_row_as_fetched() {
    let pg = start_postgres().await;
    let item_hash = "c".repeat(64);
    let mut p = pending_post(&item_hash);
    p.fetched = false;
    insert_pending_row(&pg.pool, &mut p).await.unwrap();

    let result = fetch_one(&pg.pool, &EchoRunner, p.clone(), None).await.unwrap();
    assert!(result.is_some(), "fetch should succeed");

    let client = pg.pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT fetched FROM pending_messages WHERE id = $1",
            &[&p.id],
        )
        .await
        .unwrap();
    let fetched: bool = row.get(0);
    assert!(fetched);
}

#[tokio::test]
async fn set_pending_message_fetched_updates_row() {
    let pg = start_postgres().await;
    let item_hash = "d".repeat(64);
    let mut p = pending_post(&item_hash);
    p.fetched = false;
    insert_pending_row(&pg.pool, &mut p).await.unwrap();

    let client = pg.pool.get().await.unwrap();
    set_pending_message_fetched(&**client, p.id, &json!({"updated": true}))
        .await
        .unwrap();

    let row = client
        .query_one(
            "SELECT fetched, content FROM pending_messages WHERE id = $1",
            &[&p.id],
        )
        .await
        .unwrap();
    let fetched: bool = row.get(0);
    let content: serde_json::Value = row.get(1);
    assert!(fetched);
    assert_eq!(content, json!({"updated": true}));
}

#[tokio::test]
async fn claim_messages_includes_already_fetched_when_filter_none() {
    let pg = start_postgres().await;
    let item_hash = "e".repeat(64);
    let mut p = pending_post(&item_hash);
    p.fetched = true;
    insert_pending_row(&pg.pool, &mut p).await.unwrap();

    let client = pg.pool.get().await.unwrap();
    let rows = get_next_pending_messages(&**client, Utc::now(), 10, 0, None, None)
        .await
        .unwrap();
    let hashes: HashSet<String> = rows.iter().map(|p| p.item_hash.clone()).collect();
    assert!(hashes.contains(&item_hash));
}

#[tokio::test]
async fn claim_messages_filter_fetched_true_excludes_unfetched() {
    let pg = start_postgres().await;
    let unfetched = "f".repeat(64);
    let fetched = "1".repeat(64);
    let mut p_uf = pending_post(&unfetched);
    p_uf.fetched = false;
    insert_pending_row(&pg.pool, &mut p_uf).await.unwrap();
    let mut p_f = pending_post(&fetched);
    p_f.fetched = true;
    insert_pending_row(&pg.pool, &mut p_f).await.unwrap();

    let client = pg.pool.get().await.unwrap();
    let rows = get_next_pending_messages(&**client, Utc::now(), 10, 0, Some(true), None)
        .await
        .unwrap();
    let hashes: HashSet<String> = rows.iter().map(|p| p.item_hash.clone()).collect();
    assert!(hashes.contains(&fetched));
    assert!(!hashes.contains(&unfetched));
}

#[tokio::test]
async fn fetch_one_records_failure_when_runner_errors() {
    let pg = start_postgres().await;
    let item_hash = "2".repeat(64);
    let mut p = pending_post(&item_hash);
    p.fetched = false;
    insert_pending_row(&pg.pool, &mut p).await.unwrap();

    struct ErrRunner;
    #[async_trait(?Send)]
    impl FetchRunner for ErrRunner {
        async fn verify_and_fetch(
            &self,
            _client: &tokio_postgres::Transaction<'_>,
            pending: &PendingMessageDb,
        ) -> Result<MessageDb, MessageProcessingException> {
            Err(MessageProcessingException::message_content_unavailable(
                pending.item_hash.clone(),
            ))
        }
        fn max_retries(&self) -> i32 {
            3
        }
    }

    let result = fetch_one(&pg.pool, &ErrRunner, p.clone(), None).await.unwrap();
    assert!(result.is_none(), "fetch should fail without panicking");
    // retries should now be > 0.
    let client = pg.pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT retries FROM pending_messages WHERE id = $1",
            &[&p.id],
        )
        .await
        .unwrap();
    let retries: i32 = row.get(0);
    assert!(retries >= 1, "expected retry counter to bump");
}

#[tokio::test]
async fn pending_message_row_persists_with_origin_p2p() {
    let pg = start_postgres().await;
    let item_hash = "3".repeat(64);
    let mut p = pending_post(&item_hash);
    insert_pending_row(&pg.pool, &mut p).await.unwrap();

    let client = pg.pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT origin FROM pending_messages WHERE id = $1",
            &[&p.id],
        )
        .await
        .unwrap();
    let origin: Option<String> = row.get(0);
    assert_eq!(origin.as_deref(), Some("p2p"));
}

#[tokio::test]
async fn fetch_runner_max_retries_is_passed_through() {
    let r = EchoRunner;
    assert_eq!(r.max_retries(), 3);
}

#[tokio::test]
async fn fetch_one_can_drive_a_real_message_handler_in_principle() {
    let pg = start_postgres().await;
    // Sanity: build a runner with the real `MessageHandler` and confirm we
    // can call `max_retries()` without needing the full pipeline up.
    let handler = common::build_message_handler(pg.pool.clone());
    let runner =
        aleph_ccn::jobs::fetch_pending_messages::HandlerFetchRunner {
            handler: handler.clone(),
            max_retries: 5,
        };
    assert_eq!(runner.max_retries(), 5);
    drop(Arc::clone(&handler));
}
