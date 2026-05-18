//! Balance + credit-history accessors. Mirrors `aleph/db/accessors/balances.py`.

use std::collections::HashMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tokio_postgres::GenericClient;
use tokio_postgres::types::ToSql;

use aleph_types::chain::Chain;

use crate::AlephResult;
use crate::db::models::balances::AlephCreditHistoryDb;
use crate::toolkit::constants::{CREDIT_PRECISION_CUTOFF_TIMESTAMP, CREDIT_PRECISION_MULTIPLIER};
use crate::toolkit::timestamp::{timestamp_to_datetime, utc_now};
use crate::types::sort_order::{SortByCreditHistory, SortOrder};

fn chain_to_str(c: &Chain) -> String {
    serde_json::to_value(c)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default()
}

/// Apply the precision multiplier to pre-cutoff amounts. Mirrors
/// `_apply_credit_precision_multiplier`.
fn apply_credit_precision_multiplier(amount: i64, message_timestamp: DateTime<Utc>) -> i64 {
    let cutoff = timestamp_to_datetime(CREDIT_PRECISION_CUTOFF_TIMESTAMP as f64);
    if message_timestamp < cutoff {
        amount.saturating_mul(CREDIT_PRECISION_MULTIPLIER)
    } else {
        amount
    }
}

/// Fetch the balance for `(address, chain, dapp)`.
pub async fn get_balance_by_chain(
    client: &impl GenericClient,
    address: &str,
    chain: Chain,
    dapp: Option<&str>,
) -> AlephResult<Option<Decimal>> {
    let chain_s = chain_to_str(&chain);
    let row =
        match dapp {
            Some(d) => client
                .query_opt(
                    "SELECT balance FROM balances WHERE address = $1 AND chain = $2 AND dapp = $3",
                    &[&address, &chain_s, &d],
                )
                .await?,
            None => {
                client
                    .query_opt(
                        "SELECT balance FROM balances WHERE address = $1 AND chain = $2 \
                     AND dapp IS NULL",
                        &[&address, &chain_s],
                    )
                    .await?
            }
        };
    Ok(row.as_ref().map(|r| r.get::<_, Decimal>(0)))
}

/// Filters accepted by [`get_balances_by_chain`].
#[derive(Debug, Clone, Default)]
pub struct BalanceFilters<'a> {
    pub chains: Option<&'a [String]>,
    pub page: i64,
    pub pagination: i64,
    pub min_balance: i64,
    pub after_address: Option<&'a str>,
    pub cursor_mode: bool,
}

/// Row returned by [`get_balances_by_chain`].
#[derive(Debug, Clone)]
pub struct BalancesByChainRow {
    pub address: String,
    pub balance: Decimal,
    pub chain: String,
}

fn build_balances_by_chain_query(
    f: &BalanceFilters<'_>,
    count_only: bool,
) -> (String, Vec<Box<dyn ToSql + Sync + Send>>) {
    let mut sql = if count_only {
        String::from("SELECT COUNT(*) FROM balances")
    } else {
        String::from("SELECT address, balance, chain FROM balances")
    };
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut wheres: Vec<String> = Vec::new();
    if let Some(chains) = f.chains {
        if !chains.is_empty() {
            params.push(Box::new(chains.to_vec()));
            wheres.push(format!("chain = ANY(${})", params.len()));
        }
    }
    if f.min_balance > 0 {
        params.push(Box::new(Decimal::from(f.min_balance)));
        wheres.push(format!("balance >= ${}", params.len()));
    }
    if let Some(addr) = f.after_address {
        params.push(Box::new(addr.to_string()));
        wheres.push(format!("address > ${}", params.len()));
    }
    if !wheres.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&wheres.join(" AND "));
    }
    if !count_only {
        sql.push_str(" ORDER BY address ASC");
        if f.after_address.is_some() || f.cursor_mode {
            if f.pagination > 0 {
                params.push(Box::new(f.pagination + 1));
                sql.push_str(&format!(" LIMIT ${}", params.len()));
            }
        } else {
            params.push(Box::new((f.page - 1).max(0) * f.pagination.max(0)));
            sql.push_str(&format!(" OFFSET ${}", params.len()));
            if f.pagination > 0 {
                params.push(Box::new(f.pagination));
                sql.push_str(&format!(" LIMIT ${}", params.len()));
            }
        }
    }
    (sql, params)
}

/// Paginated `(address, balance, chain)` rows.
pub async fn get_balances_by_chain(
    client: &impl GenericClient,
    filters: &BalanceFilters<'_>,
) -> AlephResult<Vec<BalancesByChainRow>> {
    let (sql, params) = build_balances_by_chain_query(filters, false);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    Ok(rows
        .into_iter()
        .map(|r| BalancesByChainRow {
            address: r.get("address"),
            balance: r.get("balance"),
            chain: r.get("chain"),
        })
        .collect())
}

/// Count rows matching `filters` over `balances`.
pub async fn count_balances_by_chain(
    client: &impl GenericClient,
    filters: &BalanceFilters<'_>,
) -> AlephResult<i64> {
    let mut f = filters.clone();
    f.pagination = 0;
    f.cursor_mode = false;
    f.after_address = None;
    let (sql, params) = build_balances_by_chain_query(&f, true);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let row = client.query_one(&sql, &param_refs).await?;
    Ok(row.get::<_, i64>(0))
}

/// Sum of balances for an address.
pub async fn get_total_balance(
    client: &impl GenericClient,
    address: &str,
    include_dapps: bool,
) -> AlephResult<Decimal> {
    let sql = if include_dapps {
        "SELECT COALESCE(SUM(balance), 0) FROM balances WHERE address = $1"
    } else {
        "SELECT COALESCE(SUM(balance), 0) FROM balances WHERE address = $1 AND dapp IS NULL"
    };
    let row = client.query_one(sql, &[&address]).await?;
    Ok(row.get::<_, Decimal>(0))
}

/// Total balance plus a per-chain breakdown. Mirrors `get_total_detailed_balance`.
pub async fn get_total_detailed_balance(
    client: &impl GenericClient,
    address: &str,
    chain: Option<&str>,
    include_dapps: bool,
) -> AlephResult<(Decimal, HashMap<String, Decimal>)> {
    if let Some(c) = chain {
        let sql = if include_dapps {
            "SELECT COALESCE(SUM(balance), 0) FROM balances WHERE address = $1 AND chain = $2"
        } else {
            "SELECT COALESCE(SUM(balance), 0) FROM balances \
             WHERE address = $1 AND chain = $2 AND dapp IS NULL"
        };
        let row = client.query_one(sql, &[&address, &c]).await?;
        return Ok((row.get::<_, Decimal>(0), HashMap::new()));
    }
    let dapp_filter = if include_dapps {
        ""
    } else {
        " AND dapp IS NULL"
    };
    let by_chain_sql = format!(
        "SELECT chain, COALESCE(SUM(balance), 0) AS balance FROM balances \
         WHERE address = $1{dapp_filter} GROUP BY chain"
    );
    let rows = client.query(&by_chain_sql, &[&address]).await?;
    let mut map = HashMap::new();
    for r in rows {
        let c: String = r.get("chain");
        let b: Decimal = r.get("balance");
        map.insert(c, b);
    }
    let total_sql =
        format!("SELECT COALESCE(SUM(balance), 0) FROM balances WHERE address = $1{dapp_filter}");
    let row = client.query_one(&total_sql, &[&address]).await?;
    Ok((row.get::<_, Decimal>(0), map))
}

/// Bulk-update balances. Mirrors `update_balances` via a UNNEST upsert.
pub async fn update_balances(
    client: &impl GenericClient,
    chain: Chain,
    dapp: Option<&str>,
    eth_height: i32,
    balances: &HashMap<String, f64>,
) -> AlephResult<()> {
    if balances.is_empty() {
        return Ok(());
    }
    let last_update = utc_now();
    let chain_s = chain_to_str(&chain);
    let addresses: Vec<String> = balances.keys().cloned().collect();
    let bal_values: Vec<Decimal> = balances
        .values()
        .map(|v| Decimal::from_f64_retain(*v).unwrap_or(Decimal::ZERO))
        .collect();
    let sql = "INSERT INTO balances(address, chain, dapp, balance, eth_height, last_update) \
               SELECT a.address, $2::varchar, $3, a.balance, $4, $5 \
               FROM UNNEST($1::varchar[], $6::numeric[]) AS a(address, balance) \
               ON CONFLICT ON CONSTRAINT balances_address_chain_dapp_uindex \
               DO UPDATE SET balance = EXCLUDED.balance, \
                             eth_height = EXCLUDED.eth_height, \
                             last_update = CASE \
                                 WHEN EXCLUDED.balance <> balances.balance \
                                 THEN EXCLUDED.last_update \
                                 ELSE balances.last_update END \
               WHERE EXCLUDED.eth_height > balances.eth_height";
    client
        .execute(
            sql,
            &[
                &addresses,
                &chain_s,
                &dapp,
                &eth_height,
                &last_update,
                &bal_values,
            ],
        )
        .await?;
    Ok(())
}

/// Distinct addresses with `last_update >= last_update`. Mirrors
/// `get_updated_balance_accounts`.
pub async fn get_updated_balance_accounts(
    client: &impl GenericClient,
    last_update: DateTime<Utc>,
) -> AlephResult<Vec<String>> {
    let sql = "SELECT DISTINCT address FROM balances WHERE last_update >= $1";
    let rows = client.query(sql, &[&last_update]).await?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Per-expiration breakdown of remaining credit, used by callers that need
/// fine-grained reporting.
#[derive(Debug, Clone)]
pub struct CreditBalanceDetail {
    pub expiration_date: Option<DateTime<Utc>>,
    pub amount: i64,
}

/// Insert one cache row representing a granting credit_history entry.
/// Mirrors `_insert_credit_lot`.
pub async fn insert_credit_lot(
    client: &impl GenericClient,
    address: &str,
    credit_ref: &str,
    credit_index: i32,
    amount: i64,
    expiration_date: Option<DateTime<Utc>>,
    message_timestamp: DateTime<Utc>,
) -> AlephResult<()> {
    let sql = "INSERT INTO credit_balances(address, credit_ref, credit_index, amount_remaining, \
                                            expiration_date, message_timestamp) \
               VALUES ($1, $2, $3, $4, $5, $6) \
               ON CONFLICT(address, credit_ref, credit_index) DO NOTHING";
    client
        .execute(
            sql,
            &[
                &address,
                &credit_ref,
                &credit_index,
                &amount,
                &expiration_date,
                &message_timestamp,
            ],
        )
        .await?;
    Ok(())
}

/// Consume a credit amount from the address's still-valid lots in emission order.
///
/// Returns `(consumed_amount, source_expiration)` per touched lot. Mirrors
/// `_consume_address_credits` (a write that locks rows `FOR UPDATE` and
/// decrements in place).
pub async fn consume_address_credits(
    client: &impl GenericClient,
    address: &str,
    amount: i64,
    message_timestamp: DateTime<Utc>,
) -> AlephResult<Vec<(i64, Option<DateTime<Utc>>)>> {
    if amount <= 0 {
        return Ok(Vec::new());
    }
    // Lock and select still-valid lots ordered by emission.
    let sql = "SELECT credit_ref, credit_index, amount_remaining, expiration_date \
               FROM credit_balances \
               WHERE address = $1 AND amount_remaining > 0 \
                 AND (expiration_date IS NULL OR expiration_date > $2) \
               ORDER BY message_timestamp ASC, credit_ref ASC, credit_index ASC \
               FOR UPDATE";
    let rows = client.query(sql, &[&address, &message_timestamp]).await?;

    let mut consumed: Vec<(i64, Option<DateTime<Utc>>)> = Vec::new();
    let mut remaining = amount;
    for row in rows {
        if remaining <= 0 {
            break;
        }
        let credit_ref: String = row.get("credit_ref");
        let credit_index: i32 = row.get("credit_index");
        let amount_remaining: i64 = row.get("amount_remaining");
        let expiration_date: Option<DateTime<Utc>> = row.get("expiration_date");
        let take = amount_remaining.min(remaining);
        // Decrement in place.
        client
            .execute(
                "UPDATE credit_balances SET amount_remaining = amount_remaining - $1 \
                 WHERE address = $2 AND credit_ref = $3 AND credit_index = $4",
                &[&take, &address, &credit_ref, &credit_index],
            )
            .await?;
        remaining -= take;
        consumed.push((take, expiration_date));
    }
    Ok(consumed)
}

/// Cap each consumed portion's expiration at min(source, requested), merging
/// adjacent portions. Mirrors `_compute_transfer_entries_by_expiration`.
pub fn compute_transfer_entries_by_expiration(
    consumed_lots: &[(i64, Option<DateTime<Utc>>)],
    requested_expiration: Option<DateTime<Utc>>,
) -> Vec<(i64, Option<DateTime<Utc>>)> {
    let mut result: Vec<(i64, Option<DateTime<Utc>>)> = Vec::new();
    for (consumed, source_exp) in consumed_lots {
        let effective_exp: Option<DateTime<Utc>> = match (source_exp, requested_expiration) {
            (None, req) => req,
            (Some(src), None) => Some(*src),
            (Some(src), Some(req)) => Some((*src).min(req)),
        };
        if let Some(last) = result.last_mut() {
            if last.1 == effective_exp {
                last.0 += consumed;
                continue;
            }
        }
        result.push((*consumed, effective_exp));
    }
    result
}

/// Sum still-valid lots for an address. Mirrors `get_credit_balance`.
pub async fn get_credit_balance(
    client: &impl GenericClient,
    address: &str,
    now: Option<DateTime<Utc>>,
) -> AlephResult<i64> {
    let sql = "SELECT COALESCE(SUM(amount_remaining), 0)::bigint AS total \
               FROM credit_balances \
               WHERE address = $1 \
                 AND (expiration_date IS NULL OR expiration_date > $2)";
    let cutoff = now.unwrap_or_else(utc_now);
    let row = client.query_one(sql, &[&address, &cutoff]).await?;
    let v: i64 = row.get(0);
    Ok(v.max(0))
}

/// Detailed credit balance for an address (per-expiration breakdown).
pub async fn get_credit_balance_with_details(
    client: &impl GenericClient,
    address: &str,
    now: Option<DateTime<Utc>>,
) -> AlephResult<(i64, Vec<CreditBalanceDetail>)> {
    let cutoff = now.unwrap_or_else(utc_now);
    let sql = "SELECT expiration_date, COALESCE(SUM(amount_remaining), 0)::bigint AS amount \
               FROM credit_balances \
               WHERE address = $1 AND amount_remaining > 0 \
                 AND (expiration_date IS NULL OR expiration_date > $2) \
               GROUP BY expiration_date";
    let rows = client.query(sql, &[&address, &cutoff]).await?;
    let mut details: Vec<CreditBalanceDetail> = rows
        .into_iter()
        .map(|r| CreditBalanceDetail {
            expiration_date: r.get("expiration_date"),
            amount: r.get::<_, i64>("amount"),
        })
        .collect();
    // Sort: non-expiring first, then by expiration ascending.
    details.sort_by(|a, b| match (a.expiration_date, b.expiration_date) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(x), Some(y)) => x.cmp(&y),
    });
    let total: i64 = details.iter().map(|d| d.amount).sum::<i64>().max(0);
    Ok((total, details))
}

/// Paginated `(address, balance)` pairs for addresses with `balance >= min_balance`.
pub async fn get_credit_balances(
    client: &impl GenericClient,
    page: i64,
    pagination: i64,
    min_balance: i64,
    after_address: Option<&str>,
    cursor_mode: bool,
) -> AlephResult<Vec<(String, i64)>> {
    let mut sql = String::from(
        "SELECT address, \
                COALESCE(SUM(amount_remaining) FILTER ( \
                    WHERE expiration_date IS NULL OR expiration_date > now() \
                ), 0)::bigint AS balance \
         FROM credit_balances",
    );
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    if let Some(addr) = after_address {
        params.push(Box::new(addr.to_string()));
        sql.push_str(&format!(" WHERE address > ${}", params.len()));
    }
    sql.push_str(" GROUP BY address");
    params.push(Box::new(min_balance));
    sql.push_str(&format!(
        " HAVING COALESCE(SUM(amount_remaining) FILTER ( \
        WHERE expiration_date IS NULL OR expiration_date > now() \
    ), 0)::bigint >= ${}",
        params.len()
    ));
    sql.push_str(" ORDER BY address ASC");

    if after_address.is_some() || cursor_mode {
        if pagination > 0 {
            params.push(Box::new(pagination + 1));
            sql.push_str(&format!(" LIMIT ${}", params.len()));
        }
    } else {
        params.push(Box::new((page - 1).max(0) * pagination.max(0)));
        sql.push_str(&format!(" OFFSET ${}", params.len()));
        if pagination > 0 {
            params.push(Box::new(pagination));
            sql.push_str(&format!(" LIMIT ${}", params.len()));
        }
    }

    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.get::<_, String>("address"), r.get::<_, i64>("balance")))
        .collect())
}

/// Count addresses matching the credit-balance filter.
pub async fn count_credit_balances(
    client: &impl GenericClient,
    min_balance: i64,
) -> AlephResult<i64> {
    let sql = "SELECT COUNT(*) FROM ( \
                   SELECT address FROM credit_balances \
                   GROUP BY address \
                   HAVING COALESCE(SUM(amount_remaining) FILTER ( \
                       WHERE expiration_date IS NULL OR expiration_date > now() \
                   ), 0)::bigint >= $1 \
               ) AS sub";
    let row = client.query_one(sql, &[&min_balance]).await?;
    Ok(row.get::<_, i64>(0))
}

const CREDIT_HISTORY_COLS: &str = "id, address, amount, price, bonus_amount, tx_hash, token, \
    chain, provider, origin, origin_ref, payment_method, credit_ref, credit_index, \
    expiration_date, message_timestamp, last_update";

fn insert_credit_history_row(
    sql: &mut String,
    params: &mut Vec<Box<dyn ToSql + Sync + Send>>,
    address: &str,
    amount: i64,
    credit_ref: &str,
    credit_index: i32,
    message_timestamp: DateTime<Utc>,
    last_update: DateTime<Utc>,
    price: Option<Decimal>,
    bonus_amount: Option<i64>,
    tx_hash: Option<&str>,
    expiration_date: Option<DateTime<Utc>>,
    token: Option<&str>,
    chain: Option<&str>,
    origin: Option<&str>,
    provider: Option<&str>,
    origin_ref: Option<&str>,
    payment_method: Option<&str>,
) {
    let start_idx = params.len() + 1;
    params.push(Box::new(address.to_string()));
    params.push(Box::new(amount));
    params.push(Box::new(credit_ref.to_string()));
    params.push(Box::new(credit_index));
    params.push(Box::new(message_timestamp));
    params.push(Box::new(last_update));
    params.push(Box::new(price));
    params.push(Box::new(bonus_amount));
    params.push(Box::new(tx_hash.map(|s| s.to_string())));
    params.push(Box::new(expiration_date));
    params.push(Box::new(token.map(|s| s.to_string())));
    params.push(Box::new(chain.map(|s| s.to_string())));
    params.push(Box::new(origin.map(|s| s.to_string())));
    params.push(Box::new(provider.map(|s| s.to_string())));
    params.push(Box::new(origin_ref.map(|s| s.to_string())));
    params.push(Box::new(payment_method.map(|s| s.to_string())));
    let placeholders = (0..16)
        .map(|i| format!("${}", start_idx + i))
        .collect::<Vec<_>>()
        .join(", ");
    if !sql.is_empty() {
        sql.push_str(", ");
    }
    sql.push_str(&format!("({placeholders})"));
}

async fn bulk_insert_credit_history(
    client: &impl GenericClient,
    rows: Vec<(
        String,                // address
        i64,                   // amount
        String,                // credit_ref
        i32,                   // credit_index
        DateTime<Utc>,         // message_timestamp
        DateTime<Utc>,         // last_update
        Option<Decimal>,       // price
        Option<i64>,           // bonus_amount
        Option<String>,        // tx_hash
        Option<DateTime<Utc>>, // expiration_date
        Option<String>,        // token
        Option<String>,        // chain
        Option<String>,        // origin
        Option<String>,        // provider
        Option<String>,        // origin_ref
        Option<String>,        // payment_method
    )>,
) -> AlephResult<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut values_sql = String::new();
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    for r in &rows {
        insert_credit_history_row(
            &mut values_sql,
            &mut params,
            &r.0,
            r.1,
            &r.2,
            r.3,
            r.4,
            r.5,
            r.6,
            r.7,
            r.8.as_deref(),
            r.9,
            r.10.as_deref(),
            r.11.as_deref(),
            r.12.as_deref(),
            r.13.as_deref(),
            r.14.as_deref(),
            r.15.as_deref(),
        );
    }
    let sql = format!(
        "INSERT INTO credit_history(address, amount, credit_ref, credit_index, \
                                    message_timestamp, last_update, price, bonus_amount, \
                                    tx_hash, expiration_date, token, chain, origin, provider, \
                                    origin_ref, payment_method) \
         VALUES {values_sql} \
         ON CONFLICT ON CONSTRAINT credit_history_pkey DO NOTHING"
    );
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    client.execute(&sql, &param_refs).await?;
    Ok(())
}

/// Distinct addresses with credit-history updates since `last_update`.
pub async fn get_updated_credit_balance_accounts(
    client: &impl GenericClient,
    last_update: DateTime<Utc>,
) -> AlephResult<Vec<String>> {
    let sql = "SELECT DISTINCT address FROM credit_history WHERE last_update >= $1";
    let rows = client.query(sql, &[&last_update]).await?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Convert a JSON credit entry shape into typed fields the way the Python
/// `update_credit_balances_*` functions do.
fn parse_amount(v: &serde_json::Value) -> i64 {
    if let Some(s) = v.as_str() {
        s.parse::<i64>().unwrap_or(0)
    } else if let Some(n) = v.as_i64() {
        n
    } else if let Some(f) = v.as_f64() {
        f as i64
    } else {
        0
    }
}

fn parse_decimal_option(v: &serde_json::Value) -> Option<Decimal> {
    if v.is_null() {
        return None;
    }
    if let Some(s) = v.as_str() {
        if s.is_empty() {
            return None;
        }
        return Decimal::from_str(s).ok();
    }
    if let Some(n) = v.as_f64() {
        return Decimal::from_f64_retain(n);
    }
    None
}

fn parse_str_or_empty(v: &serde_json::Value) -> String {
    v.as_str().unwrap_or("").to_string()
}

fn parse_expiration(v: Option<&serde_json::Value>) -> Option<DateTime<Utc>> {
    let ts = v
        .and_then(|x| x.as_i64())
        .or_else(|| v.and_then(|x| x.as_f64()).map(|f| f as i64));
    ts.filter(|t| *t != 0)
        .map(|t| timestamp_to_datetime((t as f64) / 1000.0))
}

/// Apply a distribution message. Mirrors `update_credit_balances_distribution`.
pub async fn update_credit_balances_distribution(
    client: &impl GenericClient,
    credits_list: &[serde_json::Value],
    token: &str,
    chain: &str,
    message_hash: &str,
    message_timestamp: DateTime<Utc>,
) -> AlephResult<()> {
    let last_update = utc_now();
    let mut rows: Vec<_> = Vec::new();
    for (index, entry) in credits_list.iter().enumerate() {
        let index = index as i32;
        let address = entry
            .get("address")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let raw_amount = entry.get("amount").map(parse_amount).unwrap_or(0);
        let amount = apply_credit_precision_multiplier(raw_amount, message_timestamp);
        let price = entry
            .get("price")
            .and_then(parse_decimal_option_ref)
            .or_else(|| {
                entry.get("price").and_then(|v| {
                    if v.is_null() {
                        None
                    } else {
                        parse_decimal_option(v).into()
                    }
                })
            });
        let tx_hash = entry
            .get("tx_hash")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let provider = entry
            .get("provider")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let expiration_date = parse_expiration(entry.get("expiration"));
        let origin = entry.get("origin").map(parse_str_or_empty);
        let origin_ref = entry.get("ref").map(parse_str_or_empty);
        let payment_method = entry.get("payment_method").map(parse_str_or_empty);
        let bonus_amount = entry.get("bonus_amount").and_then(|v| v.as_i64());

        insert_credit_lot(
            client,
            &address,
            message_hash,
            index,
            amount,
            expiration_date,
            message_timestamp,
        )
        .await?;

        rows.push((
            address,
            amount,
            message_hash.to_string(),
            index,
            message_timestamp,
            last_update,
            price,
            bonus_amount,
            tx_hash,
            expiration_date,
            Some(token.to_string()),
            Some(chain.to_string()),
            origin,
            provider,
            origin_ref,
            payment_method,
        ));
    }
    bulk_insert_credit_history(client, rows).await?;
    Ok(())
}

/// `parse_decimal_option_ref` helper kept separate so we can swap it out if
/// inputs evolve. Mirrors Python's `Decimal(credit_entry["price"])` —
/// returns None when missing or unparsable.
fn parse_decimal_option_ref(v: &serde_json::Value) -> Option<Decimal> {
    parse_decimal_option(v)
}

/// Apply an expense message. Mirrors `update_credit_balances_expense`.
pub async fn update_credit_balances_expense(
    client: &impl GenericClient,
    credits_list: &[serde_json::Value],
    message_hash: &str,
    message_timestamp: DateTime<Utc>,
) -> AlephResult<()> {
    let last_update = utc_now();
    let mut rows: Vec<_> = Vec::new();
    for (index, entry) in credits_list.iter().enumerate() {
        let index = index as i32;
        let address = entry
            .get("address")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let raw_amount = entry.get("amount").map(parse_amount).unwrap_or(0);
        let amount = apply_credit_precision_multiplier(raw_amount, message_timestamp);
        let origin_ref = entry.get("ref").map(parse_str_or_empty);
        let origin = entry.get("execution_id").map(parse_str_or_empty);
        let tx_hash = entry
            .get("node_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let price = entry.get("price").and_then(parse_decimal_option_ref);

        consume_address_credits(client, &address, amount, message_timestamp).await?;

        rows.push((
            address,
            -amount,
            message_hash.to_string(),
            index,
            message_timestamp,
            last_update,
            price,
            None,
            tx_hash,
            None,
            None,
            None,
            origin,
            Some("ALEPH".to_string()),
            origin_ref,
            Some("credit_expense".to_string()),
        ));
    }
    bulk_insert_credit_history(client, rows).await?;
    Ok(())
}

/// Apply a transfer message. Mirrors `update_credit_balances_transfer`.
pub async fn update_credit_balances_transfer(
    client: &impl GenericClient,
    credits_list: &[serde_json::Value],
    sender_address: &str,
    whitelisted_addresses: &[String],
    message_hash: &str,
    message_timestamp: DateTime<Utc>,
) -> AlephResult<()> {
    let last_update = utc_now();
    let is_whitelisted = whitelisted_addresses.iter().any(|w| w == sender_address);
    let mut rows: Vec<_> = Vec::new();
    let mut index: i32 = 0;
    for entry in credits_list {
        let recipient = entry
            .get("address")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let raw_amount = entry.get("amount").map(parse_amount).unwrap_or(0);
        let amount = apply_credit_precision_multiplier(raw_amount, message_timestamp);
        let requested_exp = parse_expiration(entry.get("expiration"));

        let mut entries: Vec<(i64, Option<DateTime<Utc>>)> = if is_whitelisted {
            vec![(amount, requested_exp)]
        } else {
            let consumed =
                consume_address_credits(client, sender_address, amount, message_timestamp).await?;
            let mut e = compute_transfer_entries_by_expiration(&consumed, requested_exp);
            if e.is_empty() {
                e.push((amount, requested_exp));
            }
            e
        };

        for (entry_amount, entry_expiration) in entries.drain(..) {
            insert_credit_lot(
                client,
                &recipient,
                message_hash,
                index,
                entry_amount,
                entry_expiration,
                message_timestamp,
            )
            .await?;
            rows.push((
                recipient.clone(),
                entry_amount,
                message_hash.to_string(),
                index,
                message_timestamp,
                last_update,
                None,
                None,
                None,
                entry_expiration,
                None,
                None,
                Some(sender_address.to_string()),
                Some("ALEPH".to_string()),
                None,
                Some("credit_transfer".to_string()),
            ));
            index += 1;
        }

        if !is_whitelisted {
            rows.push((
                sender_address.to_string(),
                -amount,
                message_hash.to_string(),
                index,
                message_timestamp,
                last_update,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(recipient.clone()),
                Some("ALEPH".to_string()),
                None,
                Some("credit_transfer".to_string()),
            ));
            index += 1;
        }
    }
    bulk_insert_credit_history(client, rows).await?;
    Ok(())
}

/// Whether the sender has enough credit balance for `total_transfer_amount`.
pub async fn validate_credit_transfer_balance(
    client: &impl GenericClient,
    sender_address: &str,
    total_transfer_amount: i64,
) -> AlephResult<bool> {
    let bal = get_credit_balance(client, sender_address, None).await?;
    Ok(bal >= total_transfer_amount)
}

const NULLABLE_SORT_COLUMNS: &[SortByCreditHistory] = &[
    SortByCreditHistory::ExpirationDate,
    SortByCreditHistory::Origin,
    SortByCreditHistory::TxHash,
    SortByCreditHistory::Provider,
];

fn sort_by_to_column(sb: SortByCreditHistory) -> &'static str {
    match sb {
        SortByCreditHistory::MessageTimestamp => "message_timestamp",
        SortByCreditHistory::ExpirationDate => "expiration_date",
        SortByCreditHistory::PaymentMethod => "payment_method",
        SortByCreditHistory::Amount => "amount",
        SortByCreditHistory::Origin => "origin",
        SortByCreditHistory::TxHash => "tx_hash",
        SortByCreditHistory::Provider => "provider",
    }
}

/// Filters for credit history listing/counting.
#[derive(Debug, Clone, Default)]
pub struct CreditHistoryFilters<'a> {
    pub tx_hash: Option<&'a str>,
    pub token: Option<&'a str>,
    pub chain: Option<&'a str>,
    pub provider: Option<&'a str>,
    pub origin: Option<&'a str>,
    pub origin_ref: Option<&'a str>,
    pub payment_method: Option<&'a str>,
    pub has_expiration: Option<bool>,
    pub exclude_payment_method: Option<&'a [String]>,
}

fn append_credit_history_filters(
    sql: &mut String,
    params: &mut Vec<Box<dyn ToSql + Sync + Send>>,
    f: &CreditHistoryFilters<'_>,
) {
    let mut wheres: Vec<String> = Vec::new();
    if let Some(v) = f.tx_hash {
        params.push(Box::new(v.to_string()));
        wheres.push(format!("tx_hash = ${}", params.len()));
    }
    if let Some(v) = f.token {
        params.push(Box::new(v.to_string()));
        wheres.push(format!("token = ${}", params.len()));
    }
    if let Some(v) = f.chain {
        params.push(Box::new(v.to_string()));
        wheres.push(format!("chain = ${}", params.len()));
    }
    if let Some(v) = f.provider {
        params.push(Box::new(v.to_string()));
        wheres.push(format!("provider = ${}", params.len()));
    }
    if let Some(v) = f.origin {
        params.push(Box::new(v.to_string()));
        wheres.push(format!("origin = ${}", params.len()));
    }
    if let Some(v) = f.origin_ref {
        params.push(Box::new(v.to_string()));
        wheres.push(format!("origin_ref = ${}", params.len()));
    }
    if let Some(v) = f.payment_method {
        params.push(Box::new(v.to_string()));
        wheres.push(format!("payment_method = ${}", params.len()));
    }
    if let Some(true) = f.has_expiration {
        wheres.push("expiration_date IS NOT NULL".to_string());
    } else if let Some(false) = f.has_expiration {
        wheres.push("expiration_date IS NULL".to_string());
    }
    if let Some(excl) = f.exclude_payment_method {
        if !excl.is_empty() {
            params.push(Box::new(excl.to_vec()));
            wheres.push(format!("payment_method <> ALL(${})", params.len()));
        }
    }
    for w in wheres {
        sql.push_str(" AND ");
        sql.push_str(&w);
    }
}

/// Paginated credit-history listing for an address. Mirrors
/// `get_address_credit_history`.
pub async fn get_address_credit_history(
    client: &impl GenericClient,
    address: &str,
    page: i64,
    pagination: i64,
    filters: &CreditHistoryFilters<'_>,
    sort_by: SortByCreditHistory,
    sort_order: SortOrder,
    after_sort_value: Option<&str>,
    after_credit_ref: Option<&str>,
    after_credit_index: Option<i32>,
    cursor_mode: bool,
) -> AlephResult<Vec<AlephCreditHistoryDb>> {
    let mut sql = format!(
        "SELECT {cols} FROM credit_history WHERE address = $1",
        cols = CREDIT_HISTORY_COLS
    );
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(address.to_string())];
    append_credit_history_filters(&mut sql, &mut params, filters);

    let col = sort_by_to_column(sort_by);
    let is_desc = sort_order == SortOrder::Descending;
    let is_nullable = NULLABLE_SORT_COLUMNS.contains(&sort_by);

    if let Some(_cr) = after_credit_ref {
        // Two regimes: NULL sort value (only valid for nullable cols), and non-null.
        match (after_sort_value, is_nullable) {
            (None, true) => {
                let cmp = if is_desc { "<" } else { ">" };
                let cr = after_credit_ref.unwrap_or("").to_string();
                params.push(Box::new(cr));
                let cr_idx = params.len();
                let ci = after_credit_index.unwrap_or(0);
                params.push(Box::new(ci));
                let ci_idx = params.len();
                sql.push_str(&format!(
                    " AND ({col} IS NULL AND (credit_ref {cmp} ${cr} OR \
                       (credit_ref = ${cr} AND credit_index {cmp} ${ci})))",
                    col = col,
                    cmp = cmp,
                    cr = cr_idx,
                    ci = ci_idx,
                ));
            }
            (Some(asv), _) => {
                let cmp = if is_desc { "<" } else { ">" };
                params.push(Box::new(asv.to_string()));
                let v_idx = params.len();
                let cr = after_credit_ref.unwrap_or("").to_string();
                params.push(Box::new(cr));
                let cr_idx = params.len();
                let ci = after_credit_index.unwrap_or(0);
                params.push(Box::new(ci));
                let ci_idx = params.len();
                let nulls_branch = if is_nullable {
                    format!(" OR {col} IS NULL", col = col)
                } else {
                    String::new()
                };
                sql.push_str(&format!(
                    " AND (({col} {cmp} ${v} OR \
                       ({col} = ${v} AND \
                        (credit_ref {cmp} ${cr} OR \
                         (credit_ref = ${cr} AND credit_index {cmp} ${ci})))){nulls})",
                    col = col,
                    cmp = cmp,
                    v = v_idx,
                    cr = cr_idx,
                    ci = ci_idx,
                    nulls = nulls_branch,
                ));
            }
            _ => {}
        }
    }

    // ORDER BY
    let direction = if is_desc { "DESC" } else { "ASC" };
    let nulls = if is_nullable { " NULLS LAST" } else { "" };
    sql.push_str(&format!(
        " ORDER BY {col} {direction}{nulls}, credit_ref {direction}, credit_index {direction}"
    ));

    if after_credit_ref.is_some() || cursor_mode {
        if pagination > 0 {
            params.push(Box::new(pagination + 1));
            sql.push_str(&format!(" LIMIT ${}", params.len()));
        }
    } else if pagination > 0 {
        params.push(Box::new((page - 1).max(0) * pagination));
        sql.push_str(&format!(" OFFSET ${}", params.len()));
        params.push(Box::new(pagination));
        sql.push_str(&format!(" LIMIT ${}", params.len()));
    }

    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    Ok(rows.iter().map(AlephCreditHistoryDb::from_row).collect())
}

/// Count credit history entries for an address with optional filters.
pub async fn count_address_credit_history(
    client: &impl GenericClient,
    address: &str,
    filters: &CreditHistoryFilters<'_>,
) -> AlephResult<i64> {
    let mut sql = String::from("SELECT COUNT(credit_ref) FROM credit_history WHERE address = $1");
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(address.to_string())];
    append_credit_history_filters(&mut sql, &mut params, filters);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let row = client.query_one(&sql, &param_refs).await?;
    Ok(row.get::<_, i64>(0))
}

/// Sum of credits consumed (positive), optionally filtered by address/item_hash.
pub async fn get_total_consumed_credits(
    client: &impl GenericClient,
    address: Option<&str>,
    item_hash: Option<&str>,
) -> AlephResult<i64> {
    let mut sql = String::from(
        "SELECT COALESCE(SUM(ABS(amount)), 0)::bigint FROM credit_history \
         WHERE payment_method = 'credit_expense'",
    );
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    if let Some(addr) = address {
        params.push(Box::new(addr.to_string()));
        sql.push_str(&format!(" AND address = ${}", params.len()));
    }
    if let Some(ih) = item_hash {
        params.push(Box::new(ih.to_string()));
        sql.push_str(&format!(
            " AND COALESCE(NULLIF(origin, ''), origin_ref) = ${}",
            params.len()
        ));
    }
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let row = client.query_one(&sql, &param_refs).await?;
    Ok(row.get::<_, i64>(0))
}

/// Consumed credits for a specific resource (convenience wrapper).
pub async fn get_resource_consumed_credits(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<i64> {
    get_total_consumed_credits(client, None, Some(item_hash)).await
}

/// Per-resource consumed-credits mapping.
pub async fn get_consumed_credits_by_resource(
    client: &impl GenericClient,
    item_hashes: &[String],
) -> AlephResult<HashMap<String, i64>> {
    if item_hashes.is_empty() {
        return Ok(HashMap::new());
    }
    let sql = "SELECT COALESCE(NULLIF(origin, ''), origin_ref) AS resource_hash, \
                      COALESCE(SUM(ABS(amount)), 0)::bigint AS consumed_credits \
               FROM credit_history \
               WHERE payment_method = 'credit_expense' \
                 AND COALESCE(NULLIF(origin, ''), origin_ref) = ANY($1) \
               GROUP BY resource_hash";
    let rows = client.query(sql, &[&item_hashes.to_vec()]).await?;
    let mut out = HashMap::new();
    for r in rows {
        let key: Option<String> = r.try_get("resource_hash").ok().flatten();
        let v: i64 = r.get("consumed_credits");
        if let Some(k) = key {
            out.insert(k, v);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn precision_multiplier_applied_before_cutoff() {
        let pre = timestamp_to_datetime(CREDIT_PRECISION_CUTOFF_TIMESTAMP as f64 - 100.0);
        assert_eq!(
            apply_credit_precision_multiplier(2, pre),
            2 * CREDIT_PRECISION_MULTIPLIER
        );
    }

    #[test]
    fn precision_multiplier_passthrough_after_cutoff() {
        let post = timestamp_to_datetime(CREDIT_PRECISION_CUTOFF_TIMESTAMP as f64);
        assert_eq!(apply_credit_precision_multiplier(2, post), 2);
    }

    #[test]
    fn compute_transfer_caps_expirations() {
        let src = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let req = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
        // All effective expirations resolve to `req` (req < src and None -> req),
        // so adjacent entries collapse into a single bucket. Mirrors Python.
        let entries = compute_transfer_entries_by_expiration(
            &[(100, Some(src)), (50, Some(src)), (10, None)],
            Some(req),
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (160, Some(req)));
    }

    #[test]
    fn compute_transfer_splits_distinct_expirations() {
        let exp_a = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let exp_b = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        // Distinct effective expirations do not merge.
        let entries =
            compute_transfer_entries_by_expiration(&[(100, Some(exp_a)), (50, Some(exp_b))], None);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (100, Some(exp_a)));
        assert_eq!(entries[1], (50, Some(exp_b)));
    }
}
