//! Ports `tests/db/test_cost.py`. The original Python test ran a full
//! `get_total_and_detailed_costs` pipeline; the Rust counterpart exercises the
//! `get_total_cost_for_address` accessor directly by seeding a handful of
//! account_costs rows.

mod common;

use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use serde_json::json;
use std::str::FromStr;

use aleph_ccn::db::accessors::cost::get_total_cost_for_address;
use aleph_ccn::db::models::account_costs::PaymentType;
use aleph_types::message::MessageType;

use common::fixtures::build_message;
use common::{insert_processed_message, start_postgres};

#[tokio::test]
async fn get_total_cost_for_address_sums_hold_costs() {
    let pg = start_postgres().await;

    // Seed the FK target — a processed message owned by the sender.
    let sender = "0xowner";
    let item_hash = "734a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26";
    let m = build_message(
        item_hash,
        sender,
        MessageType::Instance,
        None,
        json!({"address": sender}),
        Some(format!(r#"{{"address":"{sender}"}}"#)),
        Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
    );
    insert_processed_message(&pg.pool, m).await.unwrap();

    let client = pg.pool.get().await.unwrap();
    let hold_a = Decimal::from_str("1000").unwrap();
    let hold_b = Decimal::from_str("1.8").unwrap();
    // V0059 made `account_costs.id` an IDENTITY column; omit it.
    client
        .execute(
            "INSERT INTO account_costs(owner, item_hash, type, name, ref, payment_type, \
                                        cost_hold, cost_stream, cost_credit) \
             VALUES ($1, $2, $3, $4, NULL, $5, $6, 0, 0)",
            &[
                &sender,
                &item_hash,
                &"EXECUTION",
                &"main",
                &"hold",
                &hold_a,
            ],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO account_costs(owner, item_hash, type, name, ref, payment_type, \
                                        cost_hold, cost_stream, cost_credit) \
             VALUES ($1, $2, $3, $4, NULL, $5, $6, 0, 0)",
            &[
                &sender,
                &item_hash,
                &"STORAGE",
                &"vol",
                &"hold",
                &hold_b,
            ],
        )
        .await
        .unwrap();

    let total = get_total_cost_for_address(&**client, sender, Some(PaymentType::Hold))
        .await
        .unwrap();
    assert_eq!(total, Decimal::from_str("1001.8").unwrap());
}
