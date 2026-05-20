//! Ports `tests/jobs/test_balance_job.py`.
//!
//! The `BalanceCronJob`:
//! - marks messages for removal when the wallet balance is insufficient AND
//!   the message's confirmation height is past the cutoff;
//! - leaves messages alone when their confirmation height is below the cutoff;
//! - recovers REMOVING messages when the balance is sufficient again.

mod common;

use std::str::FromStr;

use chrono::{Duration, TimeZone, Utc};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use aleph_ccn::db::accessors::messages::get_message_status;
use aleph_ccn::db::models::cron_jobs::CronJobDb;
use aleph_ccn::db::models::messages::MessageDb;
use aleph_ccn::jobs::cron::balance_job::BalanceCronJob;
use aleph_ccn::jobs::cron::credit_balance_job::CreditBalanceCronJob;
use aleph_ccn::jobs::cron::cron_job::CronJob;
use aleph_ccn::toolkit::constants::{MiB, STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT};
use aleph_ccn::types::channel::Channel;
use aleph_ccn::types::message_status::MessageStatus;
use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use common::{insert_processed_message, start_postgres};

async fn seed_balance(
    pool: &aleph_ccn::db::DbPool,
    address: &str,
    balance: &str,
) {
    let client = pool.get().await.unwrap();
    let bal = Decimal::from_str(balance).unwrap();
    let now = Utc::now();
    // V0059 made `balances.id` an IDENTITY column; omit it.
    client
        .execute(
            "INSERT INTO balances(address, chain, dapp, balance, eth_height, last_update) \
             VALUES ($1, 'ETH', NULL, $2, $3, $4)",
            &[
                &address,
                &bal,
                &(STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT as i32),
                &now,
            ],
        )
        .await
        .unwrap();
}

async fn seed_store_message(
    pool: &aleph_ccn::db::DbPool,
    item_hash: &str,
    sender: &str,
    file_hash: &str,
    status: MessageStatus,
    size_mib: u64,
) {
    let now = Utc::now();
    let content = json!({
        "address": sender,
        "time": now.timestamp() as f64,
        "hashes": [],
        "type": "TEST",
        "item_hash": file_hash,
        "item_type": "ipfs",
    });
    let m = MessageDb {
        item_hash: item_hash.into(),
        r#type: MessageType::Store,
        chain: Chain::Ethereum,
        sender: sender.into(),
        signature: Some(format!("sig_{}", &item_hash[..8])),
        item_type: ItemType::Ipfs,
        item_content: None,
        content,
        time: now,
        channel: Some(Channel::from("TEST".to_string())),
        size: (size_mib * MiB) as i32,
        status_value: MessageStatus::Processed,
        reception_time: now,
        owner: Some(sender.into()),
        content_type: Some("TEST".into()),
        content_ref: None,
        content_key: None,
        content_item_hash: Some(file_hash.into()),
        first_confirmed_at: None,
        first_confirmed_height: Some(STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT + 1000),
        payment_type: None,
        tags: None,
    };
    insert_processed_message(pool, m).await.unwrap();
    let client = pool.get().await.unwrap();
    if status == MessageStatus::Removing {
        // Move status to REMOVING explicitly.
        client
            .execute(
                "UPDATE message_status SET status = 'removing' WHERE item_hash = $1",
                &[&item_hash.to_string()],
            )
            .await
            .unwrap();
    }
    // Stored file row.
    client
        .execute(
            "INSERT INTO files(hash, size, type) VALUES ($1, $2, 'file') \
             ON CONFLICT DO NOTHING",
            &[&file_hash.to_string(), &((size_mib * MiB) as i64)],
        )
        .await
        .unwrap();
    // Pin row.
    if status == MessageStatus::Processed {
        client
            .execute(
                "INSERT INTO file_pins(file_hash, type, owner, item_hash, created) \
                 VALUES ($1, 'message', $2, $3, NOW())",
                &[
                    &file_hash.to_string(),
                    &sender.to_string(),
                    &item_hash.to_string(),
                ],
            )
            .await
            .unwrap();
    } else {
        client
            .execute(
                "INSERT INTO file_pins(file_hash, type, owner, item_hash, created, delete_by) \
                 VALUES ($1, 'grace_period', $2, $3, NOW(), NOW() + INTERVAL '24 hours')",
                &[
                    &file_hash.to_string(),
                    &sender.to_string(),
                    &item_hash.to_string(),
                ],
            )
            .await
            .unwrap();
    }
}

async fn seed_message_cost(
    pool: &aleph_ccn::db::DbPool,
    owner: &str,
    item_hash: &str,
    cost: &str,
) {
    let client = pool.get().await.unwrap();
    let cost_d = Decimal::from_str(cost).unwrap();
    // V0059 made `account_costs.id` an IDENTITY column; omit it.
    client
        .execute(
            "INSERT INTO account_costs(owner, item_hash, type, name, payment_type, cost_hold, cost_stream, cost_credit) \
             VALUES ($1, $2, 'STORAGE', 'store', 'hold', $3, 0, 0)",
            &[&owner.to_string(), &item_hash.to_string(), &cost_d],
        )
        .await
        .unwrap();
}

async fn seed_credit_message_cost(
    pool: &aleph_ccn::db::DbPool,
    owner: &str,
    item_hash: &str,
    cost: &str,
) {
    let client = pool.get().await.unwrap();
    let cost_d = Decimal::from_str(cost).unwrap();
    client
        .execute(
            "INSERT INTO account_costs(owner, item_hash, type, name, payment_type, cost_hold, cost_stream, cost_credit) \
             VALUES ($1, $2, 'STORAGE', 'store', 'credit', 0, 0, $3)",
            &[&owner.to_string(), &item_hash.to_string(), &cost_d],
        )
        .await
        .unwrap();
}

async fn seed_credit_balance(pool: &aleph_ccn::db::DbPool, address: &str, amount: i64) {
    let client = pool.get().await.unwrap();
    let now = Utc::now();
    client
        .execute(
            "INSERT INTO credit_history(address, amount, credit_ref, credit_index, payment_method, message_timestamp) \
             VALUES ($1, $2, 'credit-balance-job-test', 0, 'credit_distribution', $3)",
            &[&address.to_string(), &amount, &now],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO credit_balances(address, credit_ref, credit_index, amount_remaining, message_timestamp) \
             VALUES ($1, 'credit-balance-job-test', 0, $2, $3)",
            &[&address.to_string(), &amount, &now],
        )
        .await
        .unwrap();
}

async fn seed_chain_confirmation(
    pool: &aleph_ccn::db::DbPool,
    item_hash: &str,
    height: i64,
) {
    let client = pool.get().await.unwrap();
    let tx_hash = format!("0xtx_{}_{}", &item_hash[..8], height);
    client
        .execute(
            "INSERT INTO chain_txs(hash, chain, height, datetime, publisher, protocol, protocol_version, content) \
             VALUES ($1, 'ETH', $2, NOW(), '0xpub', 'aleph-offchain', 1, '\"x\"'::jsonb) \
             ON CONFLICT DO NOTHING",
            &[&tx_hash, &(height as i32)],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO message_confirmations(item_hash, tx_hash) VALUES ($1, $2) \
             ON CONFLICT DO NOTHING",
            &[&item_hash.to_string(), &tx_hash],
        )
        .await
        .unwrap();
}

async fn seed_cron_job(pool: &aleph_ccn::db::DbPool, id: &str, now: chrono::DateTime<Utc>) -> CronJobDb {
    let client = pool.get().await.unwrap();
    let last_run = now - Duration::hours(1);
    client
        .execute(
            "INSERT INTO cron_jobs(id, interval, last_run) VALUES ($1, 1, $2) \
             ON CONFLICT (id) DO UPDATE SET last_run = EXCLUDED.last_run",
            &[&id.to_string(), &last_run],
        )
        .await
        .unwrap();
    CronJobDb {
        id: id.into(),
        interval: 1,
        last_run,
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn balance_job_marks_messages_for_removal() {
    let pg = start_postgres().await;
    let now = Utc::now();
    let cron = seed_cron_job(&pg.pool, "balance_check_remove", now).await;

    let wallet = "0xtestaddress1";
    let message_hash = "abcd1234".repeat(4);
    let file_hash = "1234".repeat(16);

    seed_balance(&pg.pool, wallet, "10.0").await;
    seed_store_message(
        &pg.pool,
        &message_hash,
        wallet,
        &file_hash,
        MessageStatus::Processed,
        30,
    )
    .await;
    seed_message_cost(&pg.pool, wallet, &message_hash, "15.0").await;
    seed_chain_confirmation(
        &pg.pool,
        &message_hash,
        STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT + 1000,
    )
    .await;

    let job = BalanceCronJob::new(0);
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        job.run(now, &cron, &*tx).await.unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let status = get_message_status(&**client, &message_hash).await.unwrap().unwrap();
    assert_eq!(status.status, MessageStatus::Removing);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn balance_job_ignores_messages_below_cutoff_height() {
    let pg = start_postgres().await;
    let now = Utc::now();
    let cron = seed_cron_job(&pg.pool, "balance_check_below", now).await;

    let wallet = "0xtestaddress2";
    let message_hash = "bcde2345".repeat(4);
    let file_hash = "1234".repeat(16);

    seed_balance(&pg.pool, wallet, "5.0").await;
    // Override the confirmation height to be BELOW the cutoff.
    let now2 = Utc::now();
    let content = json!({
        "address": wallet,
        "time": now2.timestamp() as f64,
        "type": "TEST",
        "item_hash": file_hash,
        "item_type": "ipfs",
    });
    let m = MessageDb {
        item_hash: message_hash.clone(),
        r#type: MessageType::Store,
        chain: Chain::Ethereum,
        sender: wallet.into(),
        signature: Some("sig".into()),
        item_type: ItemType::Ipfs,
        item_content: None,
        content,
        time: now2,
        channel: Some(Channel::from("TEST".to_string())),
        size: (30 * MiB) as i32,
        status_value: MessageStatus::Processed,
        reception_time: now2,
        owner: Some(wallet.into()),
        content_type: Some("TEST".into()),
        content_ref: None,
        content_key: None,
        content_item_hash: Some(file_hash.clone()),
        first_confirmed_at: None,
        first_confirmed_height: Some(STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT - 1000),
        payment_type: None,
        tags: None,
    };
    insert_processed_message(&pg.pool, m).await.unwrap();
    seed_message_cost(&pg.pool, wallet, &message_hash, "10.0").await;
    seed_chain_confirmation(
        &pg.pool,
        &message_hash,
        STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT - 1000,
    )
    .await;

    let job = BalanceCronJob::new(0);
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        job.run(now, &cron, &*tx).await.unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let status = get_message_status(&**client, &message_hash).await.unwrap().unwrap();
    // Without `account_costs` row we'd skip - here we *do* add a cost row but
    // since the height ends up below the cutoff via the dependency on `height`
    // in account_costs (not message.first_confirmed_height), the test asserts
    // PROCESSED stays.
    // NB: `get_total_costs_for_address_grouped_by_message` uses the
    // `height` column on account_costs which we did not set (defaults to 0).
    // 0 < STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT → the job leaves it alone.
    assert_eq!(status.status, MessageStatus::Processed);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn balance_job_recovers_messages_with_sufficient_balance() {
    let pg = start_postgres().await;
    let now = Utc::now();
    let cron = seed_cron_job(&pg.pool, "balance_check_recovery", now).await;

    let wallet = "0xtestaddress3";
    let message_hash = "cdef3456".repeat(4);
    let file_hash = "1234".repeat(16);

    seed_balance(&pg.pool, wallet, "50.0").await;
    seed_store_message(
        &pg.pool,
        &message_hash,
        wallet,
        &file_hash,
        MessageStatus::Removing,
        30,
    )
    .await;
    seed_message_cost(&pg.pool, wallet, &message_hash, "20.0").await;
    seed_chain_confirmation(
        &pg.pool,
        &message_hash,
        STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT + 2000,
    )
    .await;

    let job = BalanceCronJob::new(0);
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        job.run(now, &cron, &*tx).await.unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let status = get_message_status(&**client, &message_hash).await.unwrap().unwrap();
    assert_eq!(status.status, MessageStatus::Processed);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn credit_balance_job_uses_fractional_daily_costs() {
    let pg = start_postgres().await;
    let now = Utc::now();
    let cron = seed_cron_job(&pg.pool, "credit_balance_fractional", now).await;

    let wallet = "0xcreditfractional";
    let message_hash = "cafe1234".repeat(4);
    let file_hash = "beef".repeat(16);

    seed_credit_balance(&pg.pool, wallet, 10).await;
    seed_store_message(
        &pg.pool,
        &message_hash,
        wallet,
        &file_hash,
        MessageStatus::Processed,
        30,
    )
    .await;
    seed_credit_message_cost(&pg.pool, wallet, &message_hash, "0.5").await;
    seed_chain_confirmation(
        &pg.pool,
        &message_hash,
        STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT + 1000,
    )
    .await;

    let job = CreditBalanceCronJob::new(25 * 1024 * 1024);
    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    job.run(now, &cron, &tx).await.unwrap();
    tx.commit().await.unwrap();

    let status = get_message_status(&**pg.pool.get().await.unwrap(), &message_hash)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.status, MessageStatus::Removing);
}
