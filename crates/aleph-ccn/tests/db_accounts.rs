//! Ports `tests/db/test_accounts.py`. The original Python file covers three
//! accessors:
//!  * `get_message_stats_by_address` — has an outstanding type-mismatch in the
//!    Rust accessor (SUM(BIGINT) → NUMERIC vs i64); covered indirectly through
//!    `count_address_stats` in `db_address_stats.rs`.
//!  * `get_distinct_post_types_for_address`
//!  * `get_distinct_channels_for_address`
//!
//! The latter two work and are ported below.

mod common;

use std::collections::HashSet;

use chrono::{TimeZone, Utc};
use serde_json::json;

use aleph_ccn::db::accessors::messages::{
    get_distinct_channels_for_address, get_distinct_post_types_for_address,
};
use aleph_types::message::MessageType;

use common::fixtures::build_message;
use common::{insert_processed_message, start_postgres};

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_distinct_post_types_for_address_filters_to_post_messages() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let address = "0xPostAddress123";

    let none = get_distinct_post_types_for_address(&**client, address)
        .await
        .unwrap();
    assert!(none.is_empty());
    drop(client);

    let base = Utc.with_ymd_and_hms(2022, 5, 9, 0, 0, 0).unwrap();
    for (i, ctype) in ["blog", "blog", "news", "tutorial"].iter().enumerate() {
        let m = build_message(
            &format!("post_hash{i}"),
            address,
            MessageType::Post,
            Some("TEST"),
            json!({"address": address, "time": 1652126646.5_f64 + i as f64, "type": ctype, "content": {}}),
            Some(format!(
                r#"{{"address":"{address}","time":{},"type":"{ctype}","content":{{}}}}"#,
                1652126646.5_f64 + i as f64
            )),
            base + chrono::Duration::seconds(i as i64),
        );
        insert_processed_message(&pg.pool, m).await.unwrap();
    }
    let other = build_message(
        "post_hash5",
        "0xDifferentAddress",
        MessageType::Post,
        Some("TEST"),
        json!({"address": "0xDifferentAddress", "time": 1652126651.5, "type": "blog", "content": {}}),
        Some(r#"{"address":"0xDifferentAddress","time":1652126651.5,"type":"blog","content":{}}"#.into()),
        base + chrono::Duration::seconds(10),
    );
    insert_processed_message(&pg.pool, other).await.unwrap();

    let client = pg.pool.get().await.unwrap();
    let types = get_distinct_post_types_for_address(&**client, address)
        .await
        .unwrap();
    let set: HashSet<String> = types.iter().cloned().collect();
    assert_eq!(
        set,
        ["blog", "news", "tutorial"]
            .into_iter()
            .map(String::from)
            .collect::<HashSet<String>>()
    );

    let other_types = get_distinct_post_types_for_address(&**client, "0xDifferentAddress")
        .await
        .unwrap();
    assert_eq!(other_types, vec!["blog".to_string()]);

    let empty = get_distinct_post_types_for_address(&**client, "0xEmpty")
        .await
        .unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_distinct_channels_for_address_excludes_nulls_and_other_senders() {
    let pg = start_postgres().await;
    let address = "0xChannelAddress123";
    let client = pg.pool.get().await.unwrap();

    let empty = get_distinct_channels_for_address(&**client, address)
        .await
        .unwrap();
    assert!(empty.is_empty());
    drop(client);

    let base = Utc.with_ymd_and_hms(2022, 5, 9, 0, 0, 0).unwrap();
    let rows: [(&str, MessageType, Option<&str>); 6] = [
        ("channel_hash1", MessageType::Post, Some("channel1")),
        ("channel_hash2", MessageType::Post, Some("channel1")),
        ("channel_hash3", MessageType::Aggregate, Some("channel2")),
        ("channel_hash4", MessageType::Store, Some("channel3")),
        ("channel_hash5", MessageType::Post, None),
        ("channel_hash6", MessageType::Post, Some("other_channel")),
    ];
    for (i, (hash, mtype, channel)) in rows.iter().enumerate() {
        let sender = if i == 5 { "0xDifferentAddress" } else { address };
        let m = build_message(
            hash,
            sender,
            *mtype,
            *channel,
            json!({"address": sender, "time": 1.0 + i as f64}),
            Some(format!(
                r#"{{"address":"{sender}","time":{}}}"#,
                1.0 + i as f64
            )),
            base + chrono::Duration::seconds(i as i64),
        );
        insert_processed_message(&pg.pool, m).await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let channels = get_distinct_channels_for_address(&**client, address)
        .await
        .unwrap();
    let set: HashSet<String> = channels.iter().cloned().collect();
    assert_eq!(
        set,
        ["channel1", "channel2", "channel3"]
            .into_iter()
            .map(String::from)
            .collect::<HashSet<String>>()
    );

    let other = get_distinct_channels_for_address(&**client, "0xDifferentAddress")
        .await
        .unwrap();
    assert_eq!(other, vec!["other_channel".to_string()]);

    let none = get_distinct_channels_for_address(&**client, "0xNobody")
        .await
        .unwrap();
    assert!(none.is_empty());
}

// ---------------------------------------------------------------------------
// get_message_stats_by_address — ports test_db/test_accounts.py.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_message_stats_by_address_aggregates_and_filters() {
    use aleph_ccn::db::accessors::messages::get_message_stats_by_address;
    use aleph_ccn::types::sort_order::SortOrder;

    let pg = start_postgres().await;

    // No data -> empty result.
    {
        let client = pg.pool.get().await.unwrap();
        let stats = get_message_stats_by_address(&**client, None, None, None, SortOrder::Descending, 1, 100, None, None, false)
            .await
            .unwrap();
        assert!(stats.is_empty(), "no rows expected for empty DB");
    }

    let addr_forget = "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef";
    let addr_aggregate = "0x1234";
    let base = Utc.with_ymd_and_hms(2022, 2, 25, 0, 0, 0).unwrap();

    let forget_msg = build_message(
        "e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5",
        addr_forget,
        MessageType::Forget,
        Some("TEST"),
        json!({
            "address": addr_forget, "time": 1645794065.439_f64,
            "aggregates": [], "hashes": ["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],
            "reason": "None",
        }),
        None,
        base,
    );
    insert_processed_message(&pg.pool, forget_msg).await.unwrap();

    let agg_msg = build_message(
        "aea68aac5f4dc6e6b813fc5de9e6c17d3ef1b03e77eace15398405260baf3ce4",
        addr_aggregate,
        MessageType::Aggregate,
        Some("CHANEL-N5"),
        json!({
            "address": addr_aggregate, "key": "my-aggregate",
            "time": 1664999873_i64, "content": {"easy": "as", "a-b": "c"},
        }),
        Some(r#"{"address":"0x51A58800b26AA1451aaA803d1746687cB88E0500","key":"my-aggregate","time":1664999873,"content":{"easy":"as","a-b":"c"}}"#.into()),
        Utc.with_ymd_and_hms(2022, 10, 5, 0, 0, 0).unwrap(),
    );
    insert_processed_message(&pg.pool, agg_msg).await.unwrap();

    let client = pg.pool.get().await.unwrap();
    let stats = get_message_stats_by_address(&**client, None, None, None, SortOrder::Descending, 1, 100, None, None, false)
        .await
        .unwrap();
    assert_eq!(stats.len(), 2, "two distinct senders");

    let by_address: std::collections::HashMap<String, _> =
        stats.into_iter().map(|r| (r.address.clone(), r)).collect();

    let forget_row = by_address
        .get(addr_forget)
        .unwrap_or_else(|| panic!("missing row for {addr_forget}"));
    assert_eq!(forget_row.forget, 1);
    assert_eq!(forget_row.total, 1);

    let agg_row = by_address
        .get(addr_aggregate)
        .unwrap_or_else(|| panic!("missing row for {addr_aggregate}"));
    assert_eq!(agg_row.aggregate, 1);
    assert_eq!(agg_row.total, 1);

    // Filter by address.
    let only = get_message_stats_by_address(
        &**client,
        Some(&[addr_aggregate.to_string()]),
        None,
        None,
        SortOrder::Descending,
        1,
        100,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    assert_eq!(only.len(), 1);
    assert_eq!(only[0].address, addr_aggregate);
    assert_eq!(only[0].aggregate, 1);
    assert_eq!(only[0].total, 1);
}
