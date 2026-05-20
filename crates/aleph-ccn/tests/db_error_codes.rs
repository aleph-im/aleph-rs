//! Ports `tests/db/test_error_codes.py`.

mod common;

use std::collections::HashSet;

use aleph_ccn::types::message_status::ErrorCode;

use common::start_postgres;

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn all_error_codes_are_mapped_in_db() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let rows = client
        .query("SELECT code FROM error_codes", &[])
        .await
        .unwrap();
    let db_codes = rows
        .iter()
        .map(|row| row.get::<_, i32>("code"))
        .collect::<HashSet<_>>();

    for error_code in ErrorCode::ALL {
        assert!(
            db_codes.contains(&error_code.as_i32()),
            "missing error code in DB: {}",
            error_code.as_i32()
        );
    }

    for db_code in db_codes {
        ErrorCode::try_from(db_code).unwrap_or_else(|_| {
            panic!("DB error code is not mapped in ErrorCode enum: {db_code}")
        });
    }
}
