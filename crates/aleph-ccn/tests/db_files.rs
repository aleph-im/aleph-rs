//! Ports `tests/db/test_files.py`.

mod common;

use chrono::{TimeZone, Utc};

use aleph_ccn::db::accessors::files::{
    get_file_tag, is_pinned_file, refresh_file_tag, upsert_file_tag,
};
use aleph_ccn::types::files::FileTag;

use common::fixtures::{insert_file_pin_row, insert_file_row};
use common::{start_postgres};

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn is_pinned_file_reflects_pin_rows() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let file_hash = "QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWd";
    insert_file_row(&pg.pool, file_hash, 27, "file").await.unwrap();

    assert!(!is_pinned_file(&**client, file_hash).await.unwrap());

    insert_file_pin_row(
        &pg.pool,
        file_hash,
        "tx",
        None,
        None,
        Some("1234"),
        None,
        Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
    )
    .await
    .unwrap();
    assert!(is_pinned_file(&**client, file_hash).await.unwrap());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn upsert_file_tag_writes_then_replaces_on_newer_only() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();

    let original_hash = "QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWd";
    let new_hash = "QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWe";
    insert_file_row(&pg.pool, original_hash, 32, "file")
        .await
        .unwrap();
    insert_file_row(&pg.pool, new_hash, 413, "file")
        .await
        .unwrap();

    let tag = FileTag::from("aleph/custom-tag".to_string());
    let owner = "aleph";
    let original_dt = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    upsert_file_tag(&**client, &tag, owner, original_hash, original_dt)
        .await
        .unwrap();

    let row = get_file_tag(&**client, &tag).await.unwrap().expect("tag");
    assert_eq!(row.file_hash, original_hash);
    assert_eq!(row.owner, owner);
    assert_eq!(row.last_updated, original_dt);

    let new_dt = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
    upsert_file_tag(&**client, &tag, owner, new_hash, new_dt)
        .await
        .unwrap();
    let row = get_file_tag(&**client, &tag).await.unwrap().expect("tag");
    assert_eq!(row.file_hash, new_hash);

    // Older update — no-op.
    upsert_file_tag(&**client, &tag, owner, original_hash, original_dt)
        .await
        .unwrap();
    let row = get_file_tag(&**client, &tag).await.unwrap().expect("tag");
    assert_eq!(row.file_hash, new_hash);
    assert_eq!(row.last_updated, new_dt);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn refresh_file_tag_uses_latest_message_pin() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();

    let f1 = "QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWd";
    let f2 = "QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWe";
    insert_file_row(&pg.pool, f1, 123, "file").await.unwrap();
    insert_file_row(&pg.pool, f2, 678, "file").await.unwrap();

    let owner = "aleph";
    let tag = "4d1052267dfb2aff9d7b5a70cd004e100fe2fccfb492b24e3dcd1b8da9f3ae73";
    let first_created = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let second_created = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();

    // first pin: item_hash == tag, no ref.
    insert_file_pin_row(
        &pg.pool,
        f1,
        "message",
        Some(owner),
        Some(tag),
        None,
        None,
        first_created,
    )
    .await
    .unwrap();
    // second pin: item_hash differs but ref == tag.
    insert_file_pin_row(
        &pg.pool,
        f2,
        "message",
        Some(owner),
        Some("e5eb60fd1adfc4d3b9dc7c16ab00e20a50cd690fdf0108fb8e7899a94c578770"),
        None,
        Some(tag),
        second_created,
    )
    .await
    .unwrap();

    let ft = FileTag::from(tag.to_string());
    refresh_file_tag(&**client, &ft).await.unwrap();
    let row = get_file_tag(&**client, &ft).await.unwrap().expect("tag");
    assert_eq!(row.file_hash, f2);
    assert_eq!(row.last_updated, second_created);
}
