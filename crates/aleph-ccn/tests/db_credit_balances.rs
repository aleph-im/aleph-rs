//! Ports `tests/db/test_credit_balances.py`. Selected scenarios — the Python
//! suite has 45 tests; we cover ~30 hitting every accessor in
//! `aleph_ccn::db::accessors::balances`.

mod common;

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::str::FromStr;

use aleph_ccn::db::accessors::balances::{
    count_address_credit_history, get_address_credit_history, get_consumed_credits_by_resource,
    get_credit_balance, get_credit_balance_with_details, get_resource_consumed_credits,
    get_total_consumed_credits, update_credit_balances_distribution,
    update_credit_balances_expense, update_credit_balances_transfer,
    validate_credit_transfer_balance, CreditHistoryFilters,
};
use aleph_ccn::types::sort_order::{SortByCreditHistory, SortOrder};

use common::fixtures::{insert_credit_history_with_lot, CreditHistoryRow};
use common::{start_postgres};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const MUL: i64 = 10_000;

/// pre-credit-precision-cutoff (2026-02-02) timestamp so the 10000x multiplier
/// applies.
fn pre_cutoff(seconds_offset: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2023, 6, 15, 0, 0, 0).unwrap() + ChronoDuration::seconds(seconds_offset)
}

fn ms(dt: DateTime<Utc>) -> i64 {
    dt.timestamp() * 1000
}

fn dist_entry(address: &str, amount: i64, expiration_ms: Option<i64>) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("address".into(), Value::String(address.into()));
    m.insert("amount".into(), Value::Number(amount.into()));
    m.insert("price".into(), Value::String("1.0".into()));
    m.insert("tx_hash".into(), Value::String("0xdist".into()));
    m.insert("provider".into(), Value::String("test_provider".into()));
    if let Some(e) = expiration_ms {
        m.insert("expiration".into(), Value::Number(e.into()));
    }
    Value::Object(m)
}

// ---------------------------------------------------------------------------
// Distribution / expense / transfer write paths
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn distribution_inserts_history_with_multiplier() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    let credits = vec![json!({
        "address": "0x123",
        "amount": 1000,
        "price": "0.5",
        "tx_hash": "0xabc123",
        "provider": "test_provider",
        "expiration": ms(Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap()),
        "origin": "test_origin",
        "ref": "test_ref",
        "payment_method": "test_payment",
    })];
    update_credit_balances_distribution(&**client, &credits, "TEST_TOKEN", "ETH", "msg_hash_123", ts)
        .await
        .unwrap();

    let row = client
        .query_one(
            "SELECT address, amount, price, tx_hash, token, chain, provider, origin, origin_ref, \
                    payment_method, credit_ref, credit_index, expiration_date, message_timestamp \
             FROM credit_history WHERE credit_ref = $1",
            &[&"msg_hash_123"],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("address"), "0x123");
    assert_eq!(row.get::<_, i64>("amount"), 1000 * MUL);
    assert_eq!(
        row.get::<_, Option<Decimal>>("price"),
        Some(Decimal::from_str("0.5").unwrap())
    );
    assert_eq!(row.get::<_, Option<String>>("tx_hash").as_deref(), Some("0xabc123"));
    assert_eq!(row.get::<_, Option<String>>("token").as_deref(), Some("TEST_TOKEN"));
    assert_eq!(row.get::<_, Option<String>>("chain").as_deref(), Some("ETH"));
    assert_eq!(row.get::<_, Option<String>>("provider").as_deref(), Some("test_provider"));
    assert_eq!(row.get::<_, Option<String>>("origin").as_deref(), Some("test_origin"));
    assert_eq!(row.get::<_, Option<String>>("origin_ref").as_deref(), Some("test_ref"));
    assert_eq!(row.get::<_, Option<String>>("payment_method").as_deref(), Some("test_payment"));
    assert_eq!(row.get::<_, i32>("credit_index"), 0);
    assert!(row.get::<_, Option<DateTime<Utc>>>("expiration_date").is_some());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn expense_inserts_negative_amount_with_aleph_provider() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    let credits = vec![json!({"address": "0x456", "amount": 500, "ref": "expense_ref"})];
    update_credit_balances_expense(&**client, &credits, "expense_msg_789", ts)
        .await
        .unwrap();
    let row = client
        .query_one(
            "SELECT amount, provider, payment_method, origin_ref FROM credit_history \
             WHERE credit_ref = $1",
            &[&"expense_msg_789"],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>("amount"), -500 * MUL);
    assert_eq!(row.get::<_, Option<String>>("provider").as_deref(), Some("ALEPH"));
    assert_eq!(row.get::<_, Option<String>>("payment_method").as_deref(), Some("credit_expense"));
    assert_eq!(row.get::<_, Option<String>>("origin_ref").as_deref(), Some("expense_ref"));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn expense_with_execution_and_node_id_maps_origin_and_tx_hash() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    let credits = vec![json!({
        "address": "0x456",
        "amount": 500,
        "ref": "expense_ref",
        "execution_id": "exec_12345",
        "node_id": "node_67890",
        "price": "0.001",
    })];
    update_credit_balances_expense(&**client, &credits, "expense_msg_with_fields", ts)
        .await
        .unwrap();
    let row = client
        .query_one(
            "SELECT origin, tx_hash, price FROM credit_history WHERE credit_ref = $1",
            &[&"expense_msg_with_fields"],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<String>>("origin").as_deref(), Some("exec_12345"));
    assert_eq!(row.get::<_, Option<String>>("tx_hash").as_deref(), Some("node_67890"));
    assert_eq!(row.get::<_, Option<Decimal>>("price"), Some(Decimal::from_str("0.001").unwrap()));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn transfer_creates_recipient_and_sender_rows() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let dist_ts = pre_cutoff(0);
    let xfer_ts = pre_cutoff(86400 * 2);
    let exp_dt = Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap();
    let exp_ms_v = ms(exp_dt);

    update_credit_balances_distribution(
        &**client,
        &[dist_entry("0xsender", 300, Some(exp_ms_v))],
        "ALEPH",
        "ETH",
        "dist_for_transfer_test",
        dist_ts,
    )
    .await
    .unwrap();

    update_credit_balances_transfer(
        &**client,
        &[json!({"address": "0x789", "amount": 250, "expiration": exp_ms_v})],
        "0xsender",
        &["0xwhitelisted".to_string()],
        "transfer_msg_456",
        xfer_ts,
    )
    .await
    .unwrap();

    let rows = client
        .query(
            "SELECT address, amount, payment_method, origin, expiration_date FROM credit_history \
             WHERE credit_ref = $1 ORDER BY credit_index",
            &[&"transfer_msg_456"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let recipient = rows
        .iter()
        .find(|r| r.get::<_, i64>("amount") == 250 * MUL)
        .unwrap();
    let sender = rows
        .iter()
        .find(|r| r.get::<_, i64>("amount") == -(250 * MUL))
        .unwrap();
    assert_eq!(recipient.get::<_, String>("address"), "0x789");
    assert_eq!(
        recipient.get::<_, Option<String>>("payment_method").as_deref(),
        Some("credit_transfer")
    );
    assert_eq!(recipient.get::<_, Option<String>>("origin").as_deref(), Some("0xsender"));
    assert_eq!(recipient.get::<_, Option<DateTime<Utc>>>("expiration_date"), Some(exp_dt));
    assert_eq!(sender.get::<_, String>("address"), "0xsender");
    assert_eq!(sender.get::<_, Option<String>>("origin").as_deref(), Some("0x789"));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn whitelisted_sender_transfer_does_not_create_sender_row() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    let credits = vec![json!({"address": "0xrecipient", "amount": 500, "expiration": 1700000000000_i64})];
    update_credit_balances_transfer(
        &**client,
        &credits,
        "0xwhitelisted",
        &["0xwhitelisted".to_string(), "0xother".to_string()],
        "whitelist_transfer_123",
        ts,
    )
    .await
    .unwrap();
    let rows = client
        .query(
            "SELECT address, amount FROM credit_history WHERE credit_ref = $1",
            &[&"whitelist_transfer_123"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("address"), "0xrecipient");
    assert_eq!(rows[0].get::<_, i64>("amount"), 500 * MUL);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn balance_validation_with_sufficient_and_insufficient_amounts() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    update_credit_balances_distribution(
        &**client,
        &[dist_entry("0xlow", 500, Some(ms(Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap())))],
        "TEST",
        "ETH",
        "low_init",
        ts,
    )
    .await
    .unwrap();
    assert!(validate_credit_transfer_balance(&**client, "0xlow", 5_000_000).await.unwrap());
    assert!(validate_credit_transfer_balance(&**client, "0xlow", 4_000_000).await.unwrap());
    assert!(!validate_credit_transfer_balance(&**client, "0xlow", 6_000_000).await.unwrap());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn expired_credits_excluded_from_balance() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    let expired = ms(Utc::now() - ChronoDuration::days(1));
    let valid = ms(Utc::now() + ChronoDuration::days(1));
    update_credit_balances_distribution(
        &**client,
        &[
            dist_entry("0xexpired", 800, Some(expired)),
            dist_entry("0xexpired", 200, Some(valid)),
        ],
        "TEST",
        "ETH",
        "exp_test",
        ts,
    )
    .await
    .unwrap();
    let bal = get_credit_balance(&**client, "0xexpired", None).await.unwrap();
    assert_eq!(bal, 200 * MUL);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn multiple_recipients_single_transfer() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    let credits = vec![
        json!({"address": "0xa", "amount": 300, "expiration": 1700000000000_i64}),
        json!({"address": "0xb", "amount": 200}),
        json!({"address": "0xc", "amount": 150, "expiration": 1800000000000_i64}),
    ];
    update_credit_balances_transfer(
        &**client,
        &credits,
        "0xmulti",
        &[],
        "multi_transfer",
        ts,
    )
    .await
    .unwrap();
    let rows = client
        .query(
            "SELECT amount FROM credit_history WHERE credit_ref = $1",
            &[&"multi_transfer"],
        )
        .await
        .unwrap();
    // 3 recipients + 3 sender debits = 6 rows
    assert_eq!(rows.len(), 6);
    let positives: Vec<i64> = rows
        .iter()
        .map(|r| r.get::<_, i64>("amount"))
        .filter(|a| *a > 0)
        .collect();
    assert_eq!(positives.len(), 3);
    assert!(positives.contains(&(300 * MUL)));
    assert!(positives.contains(&(200 * MUL)));
    assert!(positives.contains(&(150 * MUL)));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn self_transfer_records_both_entries() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    update_credit_balances_transfer(
        &**client,
        &[json!({"address": "0xself", "amount": 250, "expiration": 1700000000000_i64})],
        "0xself",
        &[],
        "self_transfer_test",
        ts,
    )
    .await
    .unwrap();
    let rows = client
        .query(
            "SELECT amount, address FROM credit_history WHERE credit_ref = $1",
            &[&"self_transfer_test"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let amounts: Vec<i64> = rows.iter().map(|r| r.get("amount")).collect();
    assert!(amounts.contains(&(250 * MUL)));
    assert!(amounts.contains(&(-(250 * MUL))));
    for row in &rows {
        assert_eq!(row.get::<_, String>("address"), "0xself");
    }
}

// ---------------------------------------------------------------------------
// Expiration capping
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn transfer_expiration_propagated_to_recipient() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let exp_x = Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap();
    let dist_ts = pre_cutoff(0);
    let xfer_ts = pre_cutoff(86400 * 7);
    update_credit_balances_distribution(
        &**client,
        &[dist_entry("0xB", 300, Some(ms(exp_x)))],
        "ALEPH",
        "ETH",
        "dist_prop_1",
        dist_ts,
    )
    .await
    .unwrap();
    update_credit_balances_transfer(
        &**client,
        &[json!({"address": "0xC", "amount": 200, "expiration": Value::Null})],
        "0xB",
        &[],
        "xfer_prop_1",
        xfer_ts,
    )
    .await
    .unwrap();
    let row = client
        .query_one(
            "SELECT expiration_date FROM credit_history WHERE credit_ref = $1 AND amount > 0",
            &[&"xfer_prop_1"],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<DateTime<Utc>>>("expiration_date"), Some(exp_x));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn transfer_later_expiration_capped_to_source() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let exp_x = Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap();
    let exp_y = Utc.with_ymd_and_hms(2101, 1, 1, 0, 0, 0).unwrap();
    let dist_ts = pre_cutoff(0);
    let xfer_ts = pre_cutoff(86400 * 7);
    update_credit_balances_distribution(
        &**client,
        &[dist_entry("0xB", 300, Some(ms(exp_x)))],
        "ALEPH",
        "ETH",
        "dist_cap_1",
        dist_ts,
    )
    .await
    .unwrap();
    update_credit_balances_transfer(
        &**client,
        &[json!({"address": "0xC", "amount": 200, "expiration": ms(exp_y)})],
        "0xB",
        &[],
        "xfer_cap_1",
        xfer_ts,
    )
    .await
    .unwrap();
    let row = client
        .query_one(
            "SELECT expiration_date FROM credit_history WHERE credit_ref = $1 AND amount > 0",
            &[&"xfer_cap_1"],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<DateTime<Utc>>>("expiration_date"), Some(exp_x));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn transfer_earlier_expiration_kept() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let exp_x = Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap();
    let exp_z = Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap();
    let dist_ts = pre_cutoff(0);
    let xfer_ts = pre_cutoff(86400 * 7);
    update_credit_balances_distribution(
        &**client,
        &[dist_entry("0xB", 300, Some(ms(exp_x)))],
        "ALEPH",
        "ETH",
        "dist_early_1",
        dist_ts,
    )
    .await
    .unwrap();
    update_credit_balances_transfer(
        &**client,
        &[json!({"address": "0xC", "amount": 200, "expiration": ms(exp_z)})],
        "0xB",
        &[],
        "xfer_early_1",
        xfer_ts,
    )
    .await
    .unwrap();
    let row = client
        .query_one(
            "SELECT expiration_date FROM credit_history WHERE credit_ref = $1 AND amount > 0",
            &[&"xfer_early_1"],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<DateTime<Utc>>>("expiration_date"), Some(exp_z));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn whitelisted_sender_expiration_not_constrained() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let exp_y = Utc.with_ymd_and_hms(2101, 1, 1, 0, 0, 0).unwrap();
    let ts = pre_cutoff(86400 * 7);
    update_credit_balances_transfer(
        &**client,
        &[json!({"address": "0xC", "amount": 100, "expiration": ms(exp_y)})],
        "0xwhitelisted",
        &["0xwhitelisted".to_string()],
        "xfer_whitelist_1",
        ts,
    )
    .await
    .unwrap();
    let row = client
        .query_one(
            "SELECT expiration_date FROM credit_history WHERE credit_ref = $1 AND amount > 0",
            &[&"xfer_whitelist_1"],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<DateTime<Utc>>>("expiration_date"), Some(exp_y));
}

// ---------------------------------------------------------------------------
// get_resource_consumed_credits / get_total_consumed_credits / get_consumed_credits_by_resource
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_resource_consumed_credits_no_records() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let n = get_resource_consumed_credits(&**client, "nonexistent").await.unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_resource_consumed_credits_uses_absolute_values_and_filter() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    update_credit_balances_expense(
        &**client,
        &[json!({"address": "0xu", "amount": 250, "ref": "resource_abs"})],
        "expense_msg",
        ts,
    )
    .await
    .unwrap();
    client
        .execute(
            "UPDATE credit_history SET origin = $1 WHERE credit_ref = $2",
            &[&"resource_abs", &"expense_msg"],
        )
        .await
        .unwrap();
    let n = get_resource_consumed_credits(&**client, "resource_abs").await.unwrap();
    assert_eq!(n, 250 * MUL);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_resource_consumed_credits_filters_by_payment_method() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    // distribution (not counted)
    update_credit_balances_distribution(
        &**client,
        &[dist_entry("0xu", 500, Some(1700000000000_i64.max(1)))],
        "TEST",
        "ETH",
        "dist_for_rcc",
        ts,
    )
    .await
    .unwrap();
    // transfer (not counted)
    update_credit_balances_transfer(
        &**client,
        &[json!({"address": "0xu", "amount": 200})],
        "0xsender",
        &[],
        "transfer_for_rcc",
        ts,
    )
    .await
    .unwrap();
    // expense (counted)
    update_credit_balances_expense(
        &**client,
        &[json!({"address": "0xu", "amount": 150, "ref": "resource_789"})],
        "expense_for_rcc",
        ts,
    )
    .await
    .unwrap();
    for msg in ["dist_for_rcc", "transfer_for_rcc", "expense_for_rcc"] {
        client
            .execute(
                "UPDATE credit_history SET origin = $1 WHERE credit_ref = $2",
                &[&"resource_789", &msg],
            )
            .await
            .unwrap();
    }
    let n = get_resource_consumed_credits(&**client, "resource_789").await.unwrap();
    assert_eq!(n, 150 * MUL);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_total_consumed_credits_for_address_only() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    update_credit_balances_expense(
        &**client,
        &[json!({"address": "0xu", "amount": 100})],
        "expense_one",
        ts,
    )
    .await
    .unwrap();
    update_credit_balances_expense(
        &**client,
        &[json!({"address": "0xu", "amount": 250})],
        "expense_two",
        ts,
    )
    .await
    .unwrap();
    let n = get_total_consumed_credits(&**client, Some("0xu"), None).await.unwrap();
    assert_eq!(n, 350 * MUL);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_consumed_credits_by_resource_groups_per_resource() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    update_credit_balances_expense(
        &**client,
        &[json!({"address": "0xu", "amount": 300})],
        "ex_a",
        ts,
    )
    .await
    .unwrap();
    update_credit_balances_expense(
        &**client,
        &[json!({"address": "0xu", "amount": 150, "ref": "vol_hash"})],
        "ex_b",
        ts,
    )
    .await
    .unwrap();
    // Set origin for the first row only
    client
        .execute(
            "UPDATE credit_history SET origin = $1 WHERE credit_ref = $2",
            &[&"instance_hash_1", &"ex_a"],
        )
        .await
        .unwrap();
    let map = get_consumed_credits_by_resource(
        &**client,
        &["instance_hash_1".to_string(), "vol_hash".to_string()],
    )
    .await
    .unwrap();
    assert_eq!(map["instance_hash_1"], 300 * MUL);
    assert_eq!(map["vol_hash"], 150 * MUL);
}

// ---------------------------------------------------------------------------
// get_credit_balance_with_details
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn credit_balance_details_non_expiring_only() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 3, 2, 0, 0, 0).unwrap();
    for (amount, ref_, t_off) in [(1000, "d1_a", 0_i64), (2000, "d1_b", 3600)] {
        let row = CreditHistoryRow::new(
            "0xdetails1",
            amount,
            ref_,
            ts + ChronoDuration::seconds(t_off),
        );
        insert_credit_history_with_lot(&pg.pool, &row).await.unwrap();
    }
    let (total, details) = get_credit_balance_with_details(&**client, "0xdetails1", Some(now))
        .await
        .unwrap();
    assert_eq!(total, 3000);
    assert_eq!(details.len(), 1);
    assert!(details[0].expiration_date.is_none());
    assert_eq!(details[0].amount, 3000);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn credit_balance_details_mixed_expiration_groups() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 3, 2, 0, 0, 0).unwrap();
    let exp1 = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let exp2 = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
    let entries = [
        (1000_i64, "d2_a", 0_i64, None),
        (500, "d2_b", 3600, Some(exp1)),
        (300, "d2_c", 7200, Some(exp2)),
    ];
    for (amount, ref_, offset, exp) in entries {
        let row = CreditHistoryRow::new("0xdetails2", amount, ref_, ts + ChronoDuration::seconds(offset))
            .with_expiration(exp);
        insert_credit_history_with_lot(&pg.pool, &row).await.unwrap();
    }
    let (total, details) = get_credit_balance_with_details(&**client, "0xdetails2", Some(now))
        .await
        .unwrap();
    assert_eq!(total, 1800);
    assert_eq!(details.len(), 3);
    assert!(details[0].expiration_date.is_none());
    assert_eq!(details[0].amount, 1000);
    assert_eq!(details[1].expiration_date, Some(exp1));
    assert_eq!(details[1].amount, 500);
    assert_eq!(details[2].expiration_date, Some(exp2));
    assert_eq!(details[2].amount, 300);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn credit_balance_details_no_history() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let (total, details) = get_credit_balance_with_details(&**client, "0xno_history", None)
        .await
        .unwrap();
    assert_eq!(total, 0);
    assert!(details.is_empty());
}

// ---------------------------------------------------------------------------
// credit_history filter + sort tests
// ---------------------------------------------------------------------------

async fn seed_filter_history(pg: &aleph_ccn::db::DbPool, address: &str) -> DateTime<Utc> {
    let ts = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let exp = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let with_exp = CreditHistoryRow::new(address, 100, "ref_exp", ts)
        .with_expiration(Some(exp))
        .with_payment_method("credit_distribution");
    let no_exp = CreditHistoryRow::new(address, 200, "ref_noexp", ts)
        .with_payment_method("credit_distribution");
    insert_credit_history_with_lot(pg, &with_exp).await.unwrap();
    insert_credit_history_with_lot(pg, &no_exp).await.unwrap();
    ts
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn has_expiration_filter_true() {
    let pg = start_postgres().await;
    seed_filter_history(&pg.pool, "0xfilter").await;
    let client = pg.pool.get().await.unwrap();
    let filters = CreditHistoryFilters {
        has_expiration: Some(true),
        ..Default::default()
    };
    let rows = get_address_credit_history(
        &**client,
        "0xfilter",
        1,
        20,
        &filters,
        SortByCreditHistory::MessageTimestamp,
        SortOrder::Descending,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].credit_ref, "ref_exp");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn has_expiration_filter_false() {
    let pg = start_postgres().await;
    seed_filter_history(&pg.pool, "0xfilter2").await;
    let client = pg.pool.get().await.unwrap();
    let filters = CreditHistoryFilters {
        has_expiration: Some(false),
        ..Default::default()
    };
    let rows = get_address_credit_history(
        &**client,
        "0xfilter2",
        1,
        20,
        &filters,
        SortByCreditHistory::MessageTimestamp,
        SortOrder::Descending,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].credit_ref, "ref_noexp");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn exclude_payment_method_filter() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let address = "0xexclude";
    for (amount, ref_, pm) in [
        (100, "ref_dist", "credit_distribution"),
        (-50, "ref_expense", "credit_expense"),
        (75, "ref_transfer", "credit_transfer"),
    ] {
        let row = CreditHistoryRow::new(address, amount, ref_, ts).with_payment_method(pm);
        insert_credit_history_with_lot(&pg.pool, &row).await.unwrap();
    }
    let excludes = ["credit_expense".to_string()];
    let filters = CreditHistoryFilters {
        exclude_payment_method: Some(&excludes),
        ..Default::default()
    };
    let rows = get_address_credit_history(
        &**client,
        address,
        1,
        20,
        &filters,
        SortByCreditHistory::MessageTimestamp,
        SortOrder::Descending,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    let refs: std::collections::HashSet<_> = rows.into_iter().map(|r| r.credit_ref).collect();
    assert!(refs.contains("ref_dist"));
    assert!(refs.contains("ref_transfer"));

    let count_total = count_address_credit_history(&**client, address, &CreditHistoryFilters::default())
        .await
        .unwrap();
    assert_eq!(count_total, 3);
    let count_filtered = count_address_credit_history(&**client, address, &filters)
        .await
        .unwrap();
    assert_eq!(count_filtered, 2);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn sort_by_amount_ascending_and_descending() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let address = "0xsort";
    for (amount, ref_, offset) in [(300_i64, "sort_a", 0_i64), (100, "sort_b", 3600), (200, "sort_c", 7200)] {
        let row = CreditHistoryRow::new(address, amount, ref_, ts + ChronoDuration::seconds(offset));
        insert_credit_history_with_lot(&pg.pool, &row).await.unwrap();
    }
    let asc = get_address_credit_history(
        &**client,
        address,
        1,
        20,
        &CreditHistoryFilters::default(),
        SortByCreditHistory::Amount,
        SortOrder::Ascending,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    let amounts: Vec<i64> = asc.iter().map(|r| r.amount).collect();
    assert_eq!(amounts, vec![100, 200, 300]);

    let desc = get_address_credit_history(
        &**client,
        address,
        1,
        20,
        &CreditHistoryFilters::default(),
        SortByCreditHistory::Amount,
        SortOrder::Descending,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    let amounts: Vec<i64> = desc.iter().map(|r| r.amount).collect();
    assert_eq!(amounts, vec![300, 200, 100]);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn sort_by_expiration_nulls_last() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let address = "0xnull";
    let entries: [(i64, &str, i64, Option<DateTime<Utc>>); 3] = [
        (100, "null_a", 0, None),
        (
            200,
            "null_b",
            3600,
            Some(Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap()),
        ),
        (
            300,
            "null_c",
            7200,
            Some(Utc.with_ymd_and_hms(2026, 4, 1, 0, 0, 0).unwrap()),
        ),
    ];
    for (amount, ref_, offset, exp) in entries {
        let row = CreditHistoryRow::new(address, amount, ref_, ts + ChronoDuration::seconds(offset))
            .with_expiration(exp);
        insert_credit_history_with_lot(&pg.pool, &row).await.unwrap();
    }
    let asc = get_address_credit_history(
        &**client,
        address,
        1,
        20,
        &CreditHistoryFilters::default(),
        SortByCreditHistory::ExpirationDate,
        SortOrder::Ascending,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    let refs: Vec<&str> = asc.iter().map(|r| r.credit_ref.as_str()).collect();
    assert_eq!(refs, vec!["null_c", "null_b", "null_a"]);

    let desc = get_address_credit_history(
        &**client,
        address,
        1,
        20,
        &CreditHistoryFilters::default(),
        SortByCreditHistory::ExpirationDate,
        SortOrder::Descending,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    let refs: Vec<&str> = desc.iter().map(|r| r.credit_ref.as_str()).collect();
    assert_eq!(refs, vec!["null_b", "null_c", "null_a"]);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn count_address_credit_history_matches_basic() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    for i in 0..3 {
        let row = CreditHistoryRow::new("0xcnt", 100, &format!("cnt_{i}"), ts);
        insert_credit_history_with_lot(&pg.pool, &row).await.unwrap();
    }
    let n = count_address_credit_history(&**client, "0xcnt", &CreditHistoryFilters::default())
        .await
        .unwrap();
    assert_eq!(n, 3);
}

// ---------------------------------------------------------------------------
// get_credit_balance via update_credit_balances_* pipelines
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn balance_fix_doesnt_affect_valid_credits() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let dist_ts = pre_cutoff(0);
    let exp = Utc::now() + ChronoDuration::days(365);
    update_credit_balances_distribution(
        &**client,
        &[dist_entry("0xv", 1000, Some(ms(exp)))],
        "TEST",
        "ETH",
        "valid_credits_msg",
        dist_ts,
    )
    .await
    .unwrap();
    let bal = get_credit_balance(&**client, "0xv", None).await.unwrap();
    assert_eq!(bal, 1000 * MUL);

    let exp_ts = pre_cutoff(3600);
    update_credit_balances_expense(
        &**client,
        &[json!({"address": "0xv", "amount": 300})],
        "valid_expense_msg",
        exp_ts,
    )
    .await
    .unwrap();
    let bal = get_credit_balance(&**client, "0xv", None).await.unwrap();
    assert_eq!(bal, 700 * MUL);

    let xfer_ts = pre_cutoff(7200);
    update_credit_balances_transfer(
        &**client,
        &[json!({"address": "0xother", "amount": 200, "expiration": ms(exp)})],
        "0xv",
        &[],
        "valid_transfer_msg",
        xfer_ts,
    )
    .await
    .unwrap();
    let bal = get_credit_balance(&**client, "0xv", None).await.unwrap();
    assert_eq!(bal, 500 * MUL);
}

// ---------------------------------------------------------------------------
// Highest-value missing scenarios — zero-amount, FIFO, cache invalidation.
// Ports tests/db/test_credit_balances.py::{test_zero_amount_edge_case,
// test_fifo_scenario_1_..., test_fifo_scenario_2_...,
// test_cache_invalidation_on_credit_expiration}.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn zero_amount_transfer_creates_both_entries() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let ts = pre_cutoff(0);
    update_credit_balances_transfer(
        &**client,
        &[json!({"address": "0xzero_recipient", "amount": 0, "expiration": 1700000000000_i64})],
        "0xzero_sender",
        &[],
        "zero_amount_transfer",
        ts,
    )
    .await
    .unwrap();

    let rows = client
        .query(
            "SELECT address, amount FROM credit_history WHERE credit_ref = $1",
            &[&"zero_amount_transfer"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "transfer should yield recipient + sender rows even for 0");
    for r in &rows {
        assert_eq!(r.get::<_, i64>("amount"), 0);
    }
    let addrs: std::collections::HashSet<String> = rows.iter().map(|r| r.get("address")).collect();
    assert!(addrs.contains("0xzero_recipient"));
    assert!(addrs.contains("0xzero_sender"));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn fifo_scenario_1_non_expiring_first_equals_0_remaining() {
    // Setup mirrors the Python test:
    // - 1000 non-expiring credits at T1 (FIRST)
    // - 1000 expiring credits at T2 (SECOND, expire at base-300s)
    // - 1500 expense at T3 (before expiration)
    // Expected final balance at base: 0.
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let base = Utc.timestamp_opt(1_686_830_400, 0).unwrap();
    let expiration_ms = (base.timestamp() - 300) * 1000;
    let t1 = base - ChronoDuration::seconds(3600);
    let t2 = base - ChronoDuration::seconds(1800);
    let t3 = base - ChronoDuration::seconds(600);

    let address = "0xcorner_case_user";
    update_credit_balances_distribution(
        &**client,
        &[json!({
            "address": address, "amount": 1000, "price": "1.0",
            "tx_hash": "0xno_expiry", "provider": "test_provider",
        })],
        "TEST",
        "ETH",
        "no_expiry_credits",
        t1,
    )
    .await
    .unwrap();
    update_credit_balances_distribution(
        &**client,
        &[json!({
            "address": address, "amount": 1000, "price": "1.0",
            "tx_hash": "0xwith_expiry", "provider": "test_provider",
            "expiration": expiration_ms,
        })],
        "TEST",
        "ETH",
        "expiring_credits",
        t2,
    )
    .await
    .unwrap();
    update_credit_balances_expense(
        &**client,
        &[json!({"address": address, "amount": 1500, "ref": "big_expense"})],
        "big_expense_msg",
        t3,
    )
    .await
    .unwrap();

    let balance = get_credit_balance(&**client, address, Some(base)).await.unwrap();
    assert_eq!(balance, 0, "non-expiring consumed first, expiring remainder is now expired");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn fifo_scenario_2_expiring_first_equals_500_remaining() {
    // Setup mirrors the Python test:
    // - 1000 expiring credits at T1 (FIRST, expire at base-300s)
    // - 1000 non-expiring credits at T2 (SECOND)
    // - 1500 expense at T3 (before expiration)
    // Expected final balance at base: 500 (with multiplier => 5_000_000).
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let base = Utc.timestamp_opt(1_686_830_400, 0).unwrap();
    let expiration_ms = (base.timestamp() - 300) * 1000;
    let t1 = base - ChronoDuration::seconds(3600);
    let t2 = base - ChronoDuration::seconds(1800);
    let t3 = base - ChronoDuration::seconds(600);

    let address = "0xscenario2_user";
    update_credit_balances_distribution(
        &**client,
        &[json!({
            "address": address, "amount": 1000, "price": "1.0",
            "tx_hash": "0xexpiry_first", "provider": "test_provider",
            "expiration": expiration_ms,
        })],
        "TEST",
        "ETH",
        "expiring_credits_first",
        t1,
    )
    .await
    .unwrap();
    update_credit_balances_distribution(
        &**client,
        &[json!({
            "address": address, "amount": 1000, "price": "1.0",
            "tx_hash": "0xno_expiry_second", "provider": "test_provider",
        })],
        "TEST",
        "ETH",
        "no_expiry_credits_second",
        t2,
    )
    .await
    .unwrap();
    update_credit_balances_expense(
        &**client,
        &[json!({"address": address, "amount": 1500, "ref": "big_expense_scenario2"})],
        "big_expense_msg_scenario2",
        t3,
    )
    .await
    .unwrap();

    let balance = get_credit_balance(&**client, address, Some(base)).await.unwrap();
    assert_eq!(balance, 500 * MUL);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn cache_invalidation_on_credit_expiration() {
    // T1: insert credit with expiration X.
    // T2 (< X): read balance -> 1000 * MUL.
    // T3 (> X): read balance -> 0 (lot is filtered by the SQL cutoff; the
    // underlying row is not mutated by the read).
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let base = Utc.timestamp_opt(1_686_830_400, 0).unwrap();
    let credit_time = base - ChronoDuration::seconds(3600);
    let cache_time = base - ChronoDuration::seconds(1800);
    let expiration_ms = (base.timestamp() - 300) * 1000;

    let address = "0xcache_bug_user";
    update_credit_balances_distribution(
        &**client,
        &[json!({
            "address": address, "amount": 1000, "price": "1.0",
            "tx_hash": "0xcache_test", "provider": "test_provider",
            "expiration": expiration_ms,
        })],
        "TEST",
        "ETH",
        "cache_expiration_test",
        credit_time,
    )
    .await
    .unwrap();

    let before = get_credit_balance(&**client, address, Some(cache_time)).await.unwrap();
    assert_eq!(before, 1000 * MUL);

    let lot_amount: i64 = client
        .query_one(
            "SELECT amount_remaining::bigint FROM credit_balances WHERE address = $1",
            &[&address],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(lot_amount, 1000 * MUL);

    let after = get_credit_balance(&**client, address, Some(base)).await.unwrap();
    assert_eq!(after, 0);

    // The lot stays in the table — the cutoff is applied server-side on each read.
    let lot_amount_after: i64 = client
        .query_one(
            "SELECT amount_remaining::bigint FROM credit_balances WHERE address = $1",
            &[&address],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(lot_amount_after, 1000 * MUL);
}
