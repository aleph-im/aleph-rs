//! `account_costs` accessors. Mirrors `aleph/db/accessors/cost.py`.

use rust_decimal::Decimal;
use tokio_postgres::GenericClient;

use crate::AlephResult;
use crate::db::models::account_costs::{AccountCostsDb, PaymentType};
use crate::toolkit::costs::{format_cost, format_cost_str};
use crate::types::cost::CostType;

fn payment_type_value(p: PaymentType) -> &'static str {
    p.as_value_str()
}

/// Get the total cost (single Decimal) for an address, filtered by payment
/// type. Mirrors `get_total_cost_for_address`.
pub async fn get_total_cost_for_address(
    client: &impl GenericClient,
    address: &str,
    payment_type: Option<PaymentType>,
) -> AlephResult<Decimal> {
    let payment_type = payment_type.unwrap_or(PaymentType::Hold);
    let summary = get_costs_summary(client, Some(address), None, Some(payment_type)).await?;
    let raw = match payment_type {
        PaymentType::Superfluid => &summary.total_cost_stream,
        PaymentType::Credit => &summary.total_cost_credit,
        PaymentType::Hold => &summary.total_cost_hold,
    };
    let parsed: Decimal = raw.parse().unwrap_or(Decimal::ZERO);
    Ok(format_cost(parsed, None))
}

/// Result row of [`get_total_costs_for_address_grouped_by_message`].
#[derive(Debug, Clone)]
pub struct AccountCostsGrouped {
    pub item_hash: String,
    pub height: i32,
    pub total: Decimal,
    pub id_min: i64,
}

/// Group totals by `(item_hash, height)`, ordered by smallest cost id ascending.
///
/// Mirrors `get_total_costs_for_address_grouped_by_message`.
pub async fn get_total_costs_for_address_grouped_by_message(
    client: &impl GenericClient,
    address: &str,
    payment_type: Option<PaymentType>,
) -> AlephResult<Vec<AccountCostsGrouped>> {
    let payment_type = payment_type.unwrap_or(PaymentType::Hold);
    let cost_col = match payment_type {
        PaymentType::Hold => "cost_hold",
        PaymentType::Superfluid => "cost_stream",
        PaymentType::Credit => "cost_credit",
    };
    let payment_s = payment_type_value(payment_type);
    let sql = format!(
        "SELECT ac.item_hash AS item_hash, ct.height AS height, \
                SUM(ac.{cost_col}) AS total, MIN(ac.id) AS id_min \
         FROM account_costs ac \
         JOIN message_confirmations mc ON mc.item_hash = ac.item_hash \
         JOIN chain_txs ct ON mc.tx_hash = ct.hash \
         WHERE ac.owner = $1 AND ac.payment_type = $2 \
         GROUP BY ac.item_hash, ct.height \
         ORDER BY id_min ASC"
    );
    let rows = client.query(&sql, &[&address, &payment_s]).await?;
    Ok(rows
        .into_iter()
        .map(|row| AccountCostsGrouped {
            item_hash: row.get("item_hash"),
            height: row.get("height"),
            total: row.get("total"),
            id_min: row.get("id_min"),
        })
        .collect())
}

const ACCOUNT_COST_COLS: &str =
    "id, owner, item_hash, type, name, ref, payment_type, cost_hold, cost_stream, cost_credit";

/// All cost rows attached to a message.
pub async fn get_message_costs(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Vec<AccountCostsDb>> {
    let sql = format!(
        "SELECT {cols} FROM account_costs WHERE item_hash = $1",
        cols = ACCOUNT_COST_COLS
    );
    let rows = client.query(&sql, &[&item_hash]).await?;
    Ok(rows.iter().map(AccountCostsDb::from_row).collect())
}

const DB_SIZED_COST_TYPES: &[&str] = &[
    "STORAGE",
    "EXECUTION_PROGRAM_VOLUME_CODE",
    "EXECUTION_PROGRAM_VOLUME_RUNTIME",
    "EXECUTION_PROGRAM_VOLUME_DATA",
    "EXECUTION_VOLUME_INMUTABLE",
];

/// Cost rows for a message paired with file sizes resolved through file_pins → files.
///
/// Mirrors `get_message_costs_with_file_sizes`. `file_size` is `None` for cost
/// types not in `DB_SIZED_COST_TYPES`.
pub async fn get_message_costs_with_file_sizes(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Vec<(AccountCostsDb, Option<i64>)>> {
    let sized_types: Vec<String> = DB_SIZED_COST_TYPES.iter().map(|s| s.to_string()).collect();
    let sql = "SELECT ac.id, ac.owner, ac.item_hash, ac.type, ac.name, ac.ref, \
                       ac.payment_type, ac.cost_hold, ac.cost_stream, ac.cost_credit, \
                       sf.size AS file_size \
               FROM account_costs ac \
               LEFT JOIN file_pins fp \
                   ON fp.item_hash = ac.ref \
                  AND fp.type = 'message' \
                  AND ac.type = ANY($2) \
               LEFT JOIN files sf ON sf.hash = fp.file_hash \
               WHERE ac.item_hash = $1";
    let rows = client.query(sql, &[&item_hash, &sized_types]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                AccountCostsDb::from_row(r),
                r.try_get::<_, Option<i64>>("file_size").ok().flatten(),
            )
        })
        .collect())
}

/// Upsert one or more account-cost rows. Mirrors
/// `make_costs_upsert_query` followed by execution.
pub async fn upsert_costs(
    client: &impl GenericClient,
    costs: &[AccountCostsDb],
) -> AlephResult<()> {
    for cost in costs {
        let sql = "INSERT INTO account_costs(owner, item_hash, type, name, ref, payment_type, \
                                              cost_hold, cost_stream, cost_credit) \
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                   ON CONFLICT ON CONSTRAINT account_costs_owner_item_hash_type_name_key \
                   DO UPDATE SET cost_hold = EXCLUDED.cost_hold, \
                                 cost_stream = EXCLUDED.cost_stream, \
                                 cost_credit = EXCLUDED.cost_credit";
        let type_s = cost_type_value(cost.r#type);
        client
            .execute(
                sql,
                &[
                    &cost.owner,
                    &cost.item_hash,
                    &type_s,
                    &cost.name,
                    &cost.r#ref,
                    &payment_type_value(cost.payment_type),
                    &cost.cost_hold,
                    &cost.cost_stream,
                    &cost.cost_credit,
                ],
            )
            .await?;
    }
    Ok(())
}

fn cost_type_value(t: CostType) -> &'static str {
    t.as_value_str()
}

/// Delete cost rows attached to a message hash.
pub async fn delete_costs_for_message(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<()> {
    client
        .execute(
            "DELETE FROM account_costs WHERE item_hash = $1",
            &[&item_hash],
        )
        .await?;
    Ok(())
}

/// Delete cost rows attached to messages that are forgotten or removed.
pub async fn delete_costs_for_forgotten_and_deleted_messages(
    client: &impl GenericClient,
) -> AlephResult<()> {
    let sql = "DELETE FROM account_costs \
               USING message_status ms \
               WHERE account_costs.item_hash = ms.item_hash \
                 AND (ms.status = 'forgotten' OR ms.status = 'removed')";
    client.execute(sql, &[]).await?;
    Ok(())
}

/// Aggregated cost summary, mirroring `get_costs_summary`.
#[derive(Debug, Clone)]
pub struct CostsSummary {
    pub total_cost_hold: String,
    pub total_cost_stream: String,
    pub total_cost_credit: String,
    pub resource_count: i64,
}

/// Get the aggregated cost summary for an address, item or payment type.
pub async fn get_costs_summary(
    client: &impl GenericClient,
    address: Option<&str>,
    item_hash: Option<&str>,
    payment_type: Option<PaymentType>,
) -> AlephResult<CostsSummary> {
    let mut sql = String::from(
        "SELECT \
            SUM(cost_hold) AS total_cost_hold, \
            SUM(cost_stream) AS total_cost_stream, \
            SUM(cost_credit) AS total_cost_credit, \
            COUNT(DISTINCT item_hash) AS resource_count \
         FROM account_costs",
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    let mut wheres: Vec<String> = Vec::new();
    if let Some(addr) = address {
        params.push(Box::new(addr.to_string()));
        wheres.push(format!("owner = ${}", params.len()));
    }
    if let Some(ih) = item_hash {
        params.push(Box::new(ih.to_string()));
        wheres.push(format!("item_hash = ${}", params.len()));
    }
    if let Some(pt) = payment_type {
        params.push(Box::new(payment_type_value(pt)));
        wheres.push(format!("payment_type = ${}", params.len()));
    }
    if !wheres.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&wheres.join(" AND "));
    }
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let row = client.query_one(&sql, &param_refs).await?;
    let hold: Option<Decimal> = row.get("total_cost_hold");
    let stream: Option<Decimal> = row.get("total_cost_stream");
    let credit: Option<Decimal> = row.get("total_cost_credit");
    let count: i64 = row.get("resource_count");
    Ok(CostsSummary {
        total_cost_hold: format_cost_str(hold.unwrap_or(Decimal::ZERO), None),
        total_cost_stream: format_cost_str(stream.unwrap_or(Decimal::ZERO), None),
        total_cost_credit: format_cost_str(credit.unwrap_or(Decimal::ZERO), None),
        resource_count: count,
    })
}

/// One row in [`get_resources_with_costs`].
#[derive(Debug, Clone)]
pub struct ResourceCostRow {
    pub item_hash: String,
    pub owner: String,
    pub payment_type: String,
    pub cost_hold: Decimal,
    pub cost_stream: Decimal,
    pub cost_credit: Decimal,
}

/// Paginated list of resources with aggregated costs.
pub async fn get_resources_with_costs(
    client: &impl GenericClient,
    address: Option<&str>,
    item_hash: Option<&str>,
    payment_type: Option<PaymentType>,
    page: i64,
    pagination: i64,
) -> AlephResult<Vec<ResourceCostRow>> {
    let mut sql = String::from(
        "SELECT item_hash, owner, payment_type, \
                SUM(cost_hold) AS cost_hold, \
                SUM(cost_stream) AS cost_stream, \
                SUM(cost_credit) AS cost_credit \
         FROM account_costs",
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    let mut wheres: Vec<String> = Vec::new();
    if let Some(addr) = address {
        params.push(Box::new(addr.to_string()));
        wheres.push(format!("owner = ${}", params.len()));
    }
    if let Some(ih) = item_hash {
        params.push(Box::new(ih.to_string()));
        wheres.push(format!("item_hash = ${}", params.len()));
    }
    if let Some(pt) = payment_type {
        params.push(Box::new(payment_type_value(pt)));
        wheres.push(format!("payment_type = ${}", params.len()));
    }
    if !wheres.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&wheres.join(" AND "));
    }
    sql.push_str(" GROUP BY item_hash, owner, payment_type ORDER BY item_hash");
    if page > 1 && pagination > 0 {
        params.push(Box::new((page - 1) * pagination));
        sql.push_str(&format!(" OFFSET ${}", params.len()));
    } else if page == 1 {
        // page=1 still emits OFFSET 0 in Python; harmless to skip in Rust.
    } else if page > 1 {
        // Python: offset always applied even if pagination=0; with no LIMIT this means OFFSET applied.
        params.push(Box::new((page - 1) * pagination.max(1)));
        sql.push_str(&format!(" OFFSET ${}", params.len()));
    }
    if pagination > 0 {
        params.push(Box::new(pagination));
        sql.push_str(&format!(" LIMIT ${}", params.len()));
    }
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    Ok(rows
        .into_iter()
        .map(|r| ResourceCostRow {
            item_hash: r.get("item_hash"),
            owner: r.get("owner"),
            payment_type: r.get("payment_type"),
            cost_hold: r.get("cost_hold"),
            cost_stream: r.get("cost_stream"),
            cost_credit: r.get("cost_credit"),
        })
        .collect())
}

/// Count distinct resources matching the filters.
pub async fn count_resources_with_costs(
    client: &impl GenericClient,
    address: Option<&str>,
    item_hash: Option<&str>,
    payment_type: Option<PaymentType>,
) -> AlephResult<i64> {
    let mut sql = String::from("SELECT COUNT(DISTINCT item_hash) FROM account_costs");
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    let mut wheres: Vec<String> = Vec::new();
    if let Some(addr) = address {
        params.push(Box::new(addr.to_string()));
        wheres.push(format!("owner = ${}", params.len()));
    }
    if let Some(ih) = item_hash {
        params.push(Box::new(ih.to_string()));
        wheres.push(format!("item_hash = ${}", params.len()));
    }
    if let Some(pt) = payment_type {
        params.push(Box::new(payment_type_value(pt)));
        wheres.push(format!("payment_type = ${}", params.len()));
    }
    if !wheres.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&wheres.join(" AND "));
    }
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let row = client.query_one(&sql, &param_refs).await?;
    Ok(row.get::<_, i64>(0))
}
