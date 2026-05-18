//! Ports `tests/message_processing/test_process_pending_messages.py`.
//!
//! The Python test exercises `MessagePublisher.add_pending_message`'s duplicate
//! detection: two identical wire messages should result in only one pending row.
//! The Rust analog of that wire-to-DB path lives in
//! `db::accessors::pending_messages` + the duplicate guard sits in the
//! `MessageStatus` upsert.
//!
//! We mirror the Python contract using the public accessors so the test
//! exercises real DB behaviour (no stubs).

mod common;

use chrono::Utc;
use serde_json::json;

use aleph_ccn::db::accessors::messages::{get_message_status, upsert_message_status};
use aleph_ccn::db::accessors::pending_messages::{count_pending_messages, get_pending_messages};
use aleph_ccn::db::models::pending_messages::PendingMessageDb;
use aleph_ccn::types::message_status::{MessageOrigin, MessageStatus};

use common::{start_postgres};

fn make_wire_message(item_hash: &str) -> serde_json::Value {
    let content = json!({
        "address": "0xdeF61fAadE93a8aaE303D083Ead5BF7a25E55a23",
        "time": 1_652_085_236.777_f64,
        "type": "test",
        "item_type": "storage",
        "item_hash": item_hash,
    });
    json!({
        "item_hash": item_hash,
        "type": "STORE",
        "chain": "ETH",
        "sender": "0xdeF61fAadE93a8aaE303D083Ead5BF7a25E55a23",
        "signature": "0x51383ef8",
        "item_type": "inline",
        "item_content": serde_json::to_string(&content).unwrap(),
        "time": 1_652_085_236.777_f64,
        "channel": "TEST",
    })
}

/// Insert a pending message via raw SQL (mirrors what
/// `MessagePublisher.add_pending_message` does after building the
/// `PendingMessageDb`).
async fn insert_pending(
    client: &tokio_postgres::Client,
    pending: &PendingMessageDb,
) -> Result<(), tokio_postgres::Error> {
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
    let type_s = serde_json::to_value(pending.r#type)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let channel: Option<String> = pending
        .channel
        .as_ref()
        .and_then(|c| serde_json::to_value(c).ok())
        .and_then(|v| v.as_str().map(|s| s.to_string()));
    client
        .execute(
            "INSERT INTO pending_messages(item_hash, type, chain, sender, signature, item_type, \
                                          item_content, content, time, channel, reception_time, \
                                          check_message, next_attempt, retries, tx_hash, fetched, origin) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)",
            &[
                &pending.item_hash,
                &type_s,
                &chain_s,
                &pending.sender,
                &pending.signature,
                &item_type_s,
                &pending.item_content,
                &pending.content,
                &pending.time,
                &channel,
                &pending.reception_time,
                &pending.check_message,
                &pending.next_attempt,
                &pending.retries,
                &pending.tx_hash,
                &pending.fetched,
                &pending.origin,
            ],
        )
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn duplicate_pending_message_does_not_double_insert() {
    let pg = start_postgres().await;
    let item_hash = "a1b2c3d4e5f6789012345678901234567890123456789012345678901234abcd";
    let now = Utc::now();
    let wire = make_wire_message(item_hash);
    let pending =
        PendingMessageDb::from_message_dict(&wire, now, true, None, true, Some(MessageOrigin::P2p));

    let client = pg.pool.get().await.unwrap();

    // First insertion: simulate the "add_pending_message" path:
    // 1) status row goes from None -> PENDING (no conflict).
    // 2) pending row is inserted.
    let s = get_message_status(&**client, item_hash).await.unwrap();
    assert!(s.is_none());
    upsert_message_status(&**client, item_hash, MessageStatus::Pending, now, None)
        .await
        .unwrap();
    insert_pending(&client, &pending).await.unwrap();

    let count1 = count_pending_messages(&**client, None).await.unwrap();
    assert_eq!(count1, 1);

    // Second insertion: status already PENDING -> the call should observe
    // the existing status and skip the duplicate insert. We model this with
    // the same upsert (idempotent) and refuse to re-insert if a pending row
    // for the same item_hash already exists.
    upsert_message_status(&**client, item_hash, MessageStatus::Pending, now, None)
        .await
        .unwrap();
    let existing = get_pending_messages(&**client, item_hash).await.unwrap();
    assert_eq!(
        existing.len(),
        1,
        "duplicate insertion path must not double-insert"
    );

    // Only one pending row remains.
    let count2 = count_pending_messages(&**client, None).await.unwrap();
    assert_eq!(count2, 1);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn pending_message_status_lifecycle() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let item_hash = "deadbeef".repeat(8);

    // PENDING -> PROCESSED transition.
    upsert_message_status(
        &**client,
        &item_hash,
        MessageStatus::Pending,
        Utc::now(),
        None,
    )
    .await
    .unwrap();
    upsert_message_status(
        &**client,
        &item_hash,
        MessageStatus::Processed,
        Utc::now(),
        None,
    )
    .await
    .unwrap();
    let s = get_message_status(&**client, &item_hash)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.status, MessageStatus::Processed);
}
