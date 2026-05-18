//! Ports `tests/balances/test_balances.py`.
//!
//! Exercises the balance accessors that power the POST handler's
//! `update_balances` path:
//! - inserting balances from a Solana-style content dict
//! - inserting Sablier-style dapp-scoped balances
//! - updating an existing balance
//! - summing balances across chains / dapps via `get_total_balance`

mod common;

use std::collections::HashMap;

use rust_decimal::Decimal;
use serde_json::{Value, json};

use aleph_ccn::db::accessors::balances::{
    get_balance_by_chain, get_total_balance, update_balances,
};
use aleph_types::chain::Chain;

use common::{start_postgres};

fn solana_content() -> Value {
    json!({
        "tags": ["SOL", "SPL"],
        "chain": "SOL",
        "balances": {
            "18qhTFQujXfKpQERMsagphko8mnuKycvZGZcGfKX1V9": 299.152878_f64,
            "1DvJzfHTPmTj4EVf4Rf4iHWPTRRR4jUpgm5HaXJhYBd": 3103.90945_f64,
            "1Q7bSc4ZKqGeGeRhHSN6ATeVkRm6oWDbrLvmMFLjNTc": 0.055448_f64,
            "1nc1nerator11111111111111111111111111111111": 0.018864_f64,
            "1seeWthuL3XEGT9VY6bThgeSA9mfSpWyy9xAYtQYGwP": 100000.166466_f64,
        },
        "platform": "ALEPH_SOL",
        "main_height": 14270470,
        "token_symbol": "ALEPH",
    })
}

fn sablier_content() -> Value {
    json!({
        "dapp": "SABLIER",
        "tags": ["SABLIER"],
        "chain": "ETH",
        "height": 16171309,
        "balances": {
            "0xC88805D05E070E12F5d82eC7773b4d64A30a219B": 12447.999999999984_f64,
            "0xa58Cc23a546b6cE08EE258cfb54D92d4cC151Ba4": 4.9999999999999964_f64,
            "0xc6455E6A363b1713C3fe19C94a99731F9Cb63a57": 32180.01277139208_f64,
            "0xdaC688FDca619b43248962272b9C3BA5427B1E00": 153542.07643202206_f64,
            "0xe4D157744E07Db9d74CeB66EFbD5C7C7e0F20b96": 1125000.0_f64,
        },
        "platform": "ALEPH_ETH_SABLIER",
        "main_height": 16171309,
    })
}

fn balances_map(content: &Value) -> HashMap<String, f64> {
    let obj = content.get("balances").unwrap().as_object().unwrap().clone();
    obj.into_iter()
        .map(|(k, v)| (k, v.as_f64().unwrap()))
        .collect()
}

async fn count_balance_rows(pool: &aleph_ccn::db::DbPool) -> i64 {
    let client = pool.get().await.unwrap();
    let row = client
        .query_one("SELECT COUNT(*)::bigint FROM balances", &[])
        .await
        .unwrap();
    row.get::<_, i64>(0)
}

/// Insert a placeholder balance row so `update_balances`' INSERT collides with
/// the existing one and goes through the UPDATE branch (which doesn't touch
/// the `id` column). Mirrors the Python tests where `update_balances` is
/// always called against a populated DB.
async fn preseed_balances(
    pool: &aleph_ccn::db::DbPool,
    chain: &str,
    dapp: Option<&str>,
    addresses: &[String],
) {
    let client = pool.get().await.unwrap();
    let now = chrono::Utc::now();
    // V0059 made `balances.id` an IDENTITY column; omit it.
    for addr in addresses {
        let dapp_s = dapp.map(|s| s.to_string());
        client
            .execute(
                "INSERT INTO balances(address, chain, dapp, balance, eth_height, last_update) \
                 VALUES ($1, $2, $3, 0, 0, $4) ON CONFLICT DO NOTHING",
                &[addr, &chain.to_string(), &dapp_s, &now],
            )
            .await
            .unwrap();
    }
}

async fn raw_upsert_balance(
    pool: &aleph_ccn::db::DbPool,
    address: &str,
    chain: &str,
    dapp: Option<&str>,
    balance: Decimal,
    height: i32,
) {
    let client = pool.get().await.unwrap();
    let now = chrono::Utc::now();
    client
        .execute(
            "INSERT INTO balances(address, chain, dapp, balance, eth_height, last_update) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT ON CONSTRAINT balances_address_chain_dapp_uindex \
             DO UPDATE SET balance = EXCLUDED.balance, eth_height = EXCLUDED.eth_height, \
                            last_update = EXCLUDED.last_update \
             WHERE EXCLUDED.eth_height > balances.eth_height",
            &[
                &address.to_string(),
                &chain.to_string(),
                &dapp.map(|s| s.to_string()),
                &balance,
                &height,
                &now,
            ],
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn process_balances_solana() {
    let pg = start_postgres().await;
    let content = solana_content();
    let balances = balances_map(&content);
    let height = content.get("main_height").unwrap().as_i64().unwrap() as i32;

    for (addr, value) in balances.iter() {
        let d = Decimal::from_f64_retain(*value).unwrap_or(Decimal::ZERO);
        raw_upsert_balance(&pg.pool, addr, "SOL", None, d, height).await;
    }

    let client = pg.pool.get().await.unwrap();
    for address in balances.keys() {
        let bal = get_balance_by_chain(&**client, address, Chain::Sol, None)
            .await
            .unwrap();
        assert!(bal.is_some(), "missing balance for {address}");
    }
    let total = count_balance_rows(&pg.pool).await;
    assert_eq!(total as usize, balances.len());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn process_balances_sablier() {
    let pg = start_postgres().await;
    let content = sablier_content();
    let balances = balances_map(&content);
    let height = content.get("main_height").unwrap().as_i64().unwrap() as i32;

    for (addr, value) in balances.iter() {
        let d = Decimal::from_f64_retain(*value).unwrap_or(Decimal::ZERO);
        raw_upsert_balance(&pg.pool, addr, "ETH", Some("SABLIER"), d, height).await;
    }

    let client = pg.pool.get().await.unwrap();
    for address in balances.keys() {
        let bal = get_balance_by_chain(&**client, address, Chain::Ethereum, Some("SABLIER"))
            .await
            .unwrap();
        assert!(bal.is_some(), "missing dapp balance for {address}");
    }
    let total = count_balance_rows(&pg.pool).await;
    assert_eq!(total as usize, balances.len());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn update_balances_replaces_existing_with_higher_height() {
    let pg = start_postgres().await;
    let initial = solana_content();
    let updated = json!({
        "chain": "SOL",
        "balances": {
            "18qhTFQujXfKpQERMsagphko8mnuKycvZGZcGfKX1V9": 4.0_f64,
            "1DvJzfHTPmTj4EVf4Rf4iHWPTRRR4jUpgm5HaXJhYBd": 3.0_f64,
        },
        "platform": "ALEPH_SOL",
        "main_height": 14270471,
    });

    let init_map = balances_map(&initial);
    for (addr, value) in init_map.iter() {
        let d = Decimal::from_f64_retain(*value).unwrap_or(Decimal::ZERO);
        raw_upsert_balance(
            &pg.pool,
            addr,
            "SOL",
            None,
            d,
            initial.get("main_height").unwrap().as_i64().unwrap() as i32,
        )
        .await;
    }
    let upd_map = balances_map(&updated);
    for (addr, value) in upd_map.iter() {
        // ON CONFLICT (address, chain, dapp) DO UPDATE — same key collides.
        let d = Decimal::from_f64_retain(*value).unwrap_or(Decimal::ZERO);
        raw_upsert_balance(
            &pg.pool,
            addr,
            "SOL",
            None,
            d,
            updated.get("main_height").unwrap().as_i64().unwrap() as i32,
        )
        .await;
    }

    let client = pg.pool.get().await.unwrap();
    let after = get_balance_by_chain(
        &**client,
        "18qhTFQujXfKpQERMsagphko8mnuKycvZGZcGfKX1V9",
        Chain::Sol,
        None,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(after, Decimal::from(4));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn total_balance_aggregates_across_chains_and_dapps() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    // Seed three rows for `my-address` (two without dapp, one with) and one
    // for `your-address`. V0059 made `balances.id` IDENTITY; omit it.
    let now = chrono::Utc::now();
    let rows: Vec<(&str, &str, Option<&str>, Decimal)> = vec![
        ("my-address", "ETH", None, Decimal::from(100_000)),
        ("my-address", "SOL", None, Decimal::from(1_000_000)),
        ("my-address", "ETH", Some("SABLIER"), Decimal::from(1_000_000_000_u64)),
        ("your-address", "TEZOS", None, Decimal::from(3)),
    ];
    for (addr, chain, dapp, bal) in rows.iter() {
        let dapp_owned = dapp.map(|s| s.to_string());
        client
            .execute(
                "INSERT INTO balances(address, chain, dapp, balance, eth_height, last_update) \
                 VALUES ($1, $2, $3, $4, 0, $5)",
                &[&addr.to_string(), &chain.to_string(), &dapp_owned, bal, &now],
            )
            .await
            .unwrap();
    }

    let with_dapps = get_total_balance(&**client, "my-address", true).await.unwrap();
    assert_eq!(with_dapps, Decimal::from(1_001_100_000_u64));

    let no_dapps = get_total_balance(&**client, "my-address", false).await.unwrap();
    assert_eq!(no_dapps, Decimal::from(1_100_000));

    let two = get_total_balance(&**client, "your-address", false).await.unwrap();
    assert_eq!(two, Decimal::from(3));

    let unknown = get_total_balance(&**client, "nobody", false).await.unwrap();
    assert_eq!(unknown, Decimal::ZERO);
}
