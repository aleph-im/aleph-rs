//! Mirrors `aleph/web/controllers/accounts.py`.

use std::collections::HashMap;

use aleph_types::chain::Chain;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};

use crate::db::accessors::address_stats::count_address_stats;
use crate::db::accessors::balances::{
    BalanceFilters, CreditHistoryFilters, count_address_credit_history, count_balances_by_chain,
    count_credit_balances, get_address_credit_history, get_balances_by_chain, get_credit_balance,
    get_credit_balance_with_details, get_credit_balances, get_resource_consumed_credits,
    get_total_detailed_balance,
};
use crate::db::accessors::cost::get_total_cost_for_address;
use crate::db::accessors::files::{get_address_files_for_api, get_address_files_stats};
use crate::db::accessors::messages::{
    get_distinct_channels_for_address, get_distinct_post_types_for_address,
    get_message_stats_by_address,
};
use crate::schemas::addresses_query_params::AddressesQueryParams;
use crate::toolkit::cursor::{
    decode_address_cursor, decode_address_stats_cursor, decode_credit_history_sort_cursor,
    decode_message_cursor, encode_address_cursor, encode_address_stats_cursor,
    encode_credit_history_sort_cursor, encode_message_cursor,
};
use crate::types::sort_order::{SortByCreditHistory, SortByMessageType, SortOrder};
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::utils::{get_db, json_text_response, validate_cursor_pagination};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/v0/addresses/stats.json", get(addresses_stats_v0))
        .route("/api/v1/addresses/stats.json", get(addresses_stats_v1))
        .route(
            "/api/v0/addresses/{address}/balance",
            get(get_account_balance),
        )
        .route("/api/v0/balances", get(get_chain_balances))
        .route("/api/v0/credit_balances", get(get_credit_balances_handler))
        .route("/api/v0/addresses/{address}/files", get(get_account_files))
        .route(
            "/api/v0/addresses/{address}/post_types",
            get(get_account_post_types),
        )
        .route(
            "/api/v0/addresses/{address}/channels",
            get(get_account_channels),
        )
        .route(
            "/api/v0/addresses/{address}/credit_history",
            get(get_account_credit_history),
        )
        .route(
            "/api/v0/messages/{item_hash}/consumed_credits",
            get(get_resource_consumed_credits_handler),
        )
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

fn make_stats_dict(rows: &[crate::db::accessors::messages::AddressStatsRow]) -> Map<String, Value> {
    let mut out = Map::new();
    for row in rows {
        out.insert(
            row.address.clone(),
            json!({
                "messages": row.total,
                "aggregate": row.aggregate,
                "forget": row.forget,
                "instance": row.instance,
                "post": row.post,
                "program": row.program,
                "store": row.store,
            }),
        );
    }
    out
}

/// Extract the `addresses[]` query parameter values from a raw axum
/// key-value list. Supports both repeated-key (`addresses[]=a&addresses[]=b`)
/// and comma-separated (`addresses[]=a,b`) encodings — pyaleph's aiohttp
/// MultiDict accepts the same.
fn collect_addresses_brackets(pairs: &[(String, String)]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (k, v) in pairs {
        if k == "addresses[]" {
            if v.contains(',') {
                out.extend(v.split(',').map(|s| s.to_string()));
            } else {
                out.push(v.clone());
            }
        }
    }
    out
}

async fn addresses_stats_v0(
    State(state): State<AppState>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> WebResult<Response> {
    // pyaleph reads `addresses[]` (the aiohttp MultiDict). We accept both
    // the repeated-key form (`?addresses[]=a&addresses[]=b`) and the
    // legacy comma-separated form (`?addresses[]=a,b`).
    let addresses = collect_addresses_brackets(&pairs);
    let client = get_db(&state).await?;
    let rows = get_message_stats_by_address(
        &**client,
        Some(&addresses),
        None,
        None,
        SortOrder::Descending,
        1,
        0,
        None,
        None,
        false,
    )
    .await?;
    let data = make_stats_dict(&rows);
    let body = json!({ "data": Value::Object(data) });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

async fn addresses_stats_v1(
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let value = raw_value(&raw);
    let q: AddressesQueryParams =
        serde_json::from_value(value).map_err(|e| WebError::Unprocessable(e.to_string()))?;
    q.validate().map_err(WebError::Unprocessable)?;
    let client = get_db(&state).await?;

    let cursor = q.cursor.as_deref();

    if let Some(cursor_val) = cursor {
        let pagination_per_page = validate_cursor_pagination(Some(cursor_val), q.pagination)?;
        let (sort_value_json, after_address) = if cursor_val.is_empty() {
            (None, None)
        } else {
            let (v, a) = decode_address_stats_cursor(cursor_val)
                .map_err(|e| WebError::Unprocessable(e.to_string()))?;
            (Some(v), Some(a))
        };
        let after_sort_value = sort_value_json.as_ref().and_then(|v| v.as_i64());
        let rows = get_message_stats_by_address(
            &**client,
            None,
            q.address_contains.as_deref(),
            Some(q.sort_by),
            q.sort_order,
            1,
            pagination_per_page,
            after_sort_value,
            after_address.as_deref(),
            true,
        )
        .await?;
        let has_more = (rows.len() as i64) > pagination_per_page;
        let mut rows = rows;
        if has_more {
            rows.truncate(pagination_per_page as usize);
        }
        let next_cursor = if has_more && !rows.is_empty() {
            let last = rows.last().unwrap();
            let v = sort_value(&last, q.sort_by);
            Some(encode_address_stats_cursor(v, &last.address))
        } else {
            None
        };
        let data = make_stats_dict(&rows);
        let body = json!({
            "data": Value::Object(data),
            "pagination_per_page": pagination_per_page,
            "next_cursor": next_cursor,
        });
        return Ok(json_text_response(StatusCode::OK, body.to_string()));
    }

    let rows = get_message_stats_by_address(
        &**client,
        None,
        q.address_contains.as_deref(),
        Some(q.sort_by),
        q.sort_order,
        q.page,
        q.pagination,
        None,
        None,
        false,
    )
    .await?;
    let total = count_address_stats(&**client, q.address_contains.as_deref()).await?;
    let data = make_stats_dict(&rows);
    let body = json!({
        "data": Value::Object(data),
        "pagination_per_page": q.pagination,
        "pagination_page": q.page,
        "pagination_total": total,
        "pagination_item": "addresses",
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

fn sort_value(
    row: &crate::db::accessors::messages::AddressStatsRow,
    sort_by: SortByMessageType,
) -> Value {
    json!(match sort_by {
        SortByMessageType::Aggregate => row.aggregate,
        SortByMessageType::Forget => row.forget,
        SortByMessageType::Instance => row.instance,
        SortByMessageType::Post => row.post,
        SortByMessageType::Program => row.program,
        SortByMessageType::Store => row.store,
        SortByMessageType::Total => row.total,
    })
}

// ---------------------------------------------------------------------------
// Balance
// ---------------------------------------------------------------------------

/// Serialise a decimal as a string to preserve precision across the wire.
/// pyaleph relies on `python-json`'s `Decimal` -> string fallback; we mirror it.
fn decimal_str(d: Decimal) -> Value {
    Value::String(d.normalize().to_string())
}

async fn get_account_balance(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let chain = raw.get("chain").cloned();
    let include_credit_details = raw
        .get("include_credit_details")
        .map(|s| matches!(s.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let client = get_db(&state).await?;
    let chain_s = chain.as_deref();
    let (balance, details) =
        get_total_detailed_balance(&**client, &address, chain_s, false).await?;
    let total_cost = get_total_cost_for_address(&**client, &address, None).await?;
    let (credits, credit_details) = if include_credit_details {
        let (c, d) = get_credit_balance_with_details(&**client, &address, None).await?;
        (c, Some(d))
    } else {
        let c = get_credit_balance(&**client, &address, None).await?;
        (c, None)
    };

    // `details` is always emitted (defaults to {}). pyaleph builds the dict
    // unconditionally and the SDK expects the key to exist.
    let mut details_map: Map<String, Value> = Map::new();
    for (k, v) in details {
        details_map.insert(k, decimal_str(v));
    }

    let mut body = Map::new();
    body.insert("address".into(), Value::String(address.clone()));
    body.insert("balance".into(), decimal_str(balance));
    body.insert("locked_amount".into(), decimal_str(total_cost));
    body.insert("credit_balance".into(), json!(credits));
    body.insert("details".into(), Value::Object(details_map));
    // `credit_balance_details` is always present; null when the caller did not
    // request the breakdown.
    let credit_details_v = match credit_details {
        Some(d) => {
            let arr: Vec<Value> = d
                .iter()
                .map(|x| {
                    json!({
                        "expiration_date": x.expiration_date.map(|t| t.to_rfc3339()),
                        "amount": x.amount,
                    })
                })
                .collect();
            Value::Array(arr)
        }
        None => Value::Null,
    };
    body.insert("credit_balance_details".into(), credit_details_v);
    Ok(json_text_response(
        StatusCode::OK,
        Value::Object(body).to_string(),
    ))
}

// ---------------------------------------------------------------------------
// Chain balances
// ---------------------------------------------------------------------------

async fn get_chain_balances(
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let chains_csv = raw.get("chains").cloned();
    let pagination = raw
        .get("pagination")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(100);
    if pagination < 0 {
        return Err(WebError::Unprocessable("pagination must be >= 0".into()));
    }
    let page = raw
        .get("page")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(1);
    if page < 1 {
        return Err(WebError::Unprocessable("page must be >= 1".into()));
    }
    let min_balance = raw
        .get("min_balance")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let cursor = raw.get("cursor").cloned();
    let chains_vec: Vec<String> = chains_csv
        .as_deref()
        .map(|v| v.split(',').map(|s| s.to_string()).collect())
        .unwrap_or_default();

    let client = get_db(&state).await?;

    if let Some(cursor_val) = cursor.as_deref() {
        let pagination_per_page = validate_cursor_pagination(Some(cursor_val), pagination)?;
        let after_address = if cursor_val.is_empty() {
            None
        } else {
            Some(
                decode_address_cursor(cursor_val)
                    .map_err(|e| WebError::Unprocessable(e.to_string()))?,
            )
        };
        let filters = BalanceFilters {
            chains: if chains_vec.is_empty() {
                None
            } else {
                Some(&chains_vec)
            },
            page: 1,
            pagination: pagination_per_page,
            min_balance,
            after_address: after_address.as_deref(),
            cursor_mode: true,
        };
        let mut balances = get_balances_by_chain(&**client, &filters).await?;
        let has_more = (balances.len() as i64) > pagination_per_page;
        if has_more {
            balances.truncate(pagination_per_page as usize);
        }
        let formatted: Vec<Value> = balances
            .iter()
            .map(|b| {
                json!({
                    "address": b.address,
                    "balance": decimal_str(b.balance),
                    "chain": b.chain,
                })
            })
            .collect();
        let next_cursor = if has_more && !balances.is_empty() {
            let last = balances.last().unwrap();
            Some(encode_address_cursor(&last.address))
        } else {
            None
        };
        let body = json!({
            "balances": formatted,
            "pagination_per_page": pagination_per_page,
            "next_cursor": next_cursor,
        });
        return Ok(json_text_response(StatusCode::OK, body.to_string()));
    }

    let filters = BalanceFilters {
        chains: if chains_vec.is_empty() {
            None
        } else {
            Some(&chains_vec)
        },
        page,
        pagination,
        min_balance,
        after_address: None,
        cursor_mode: false,
    };
    let balances = get_balances_by_chain(&**client, &filters).await?;
    let total = count_balances_by_chain(&**client, &filters).await?;
    let formatted: Vec<Value> = balances
        .iter()
        .map(|b| {
            json!({
                "address": b.address,
                "balance": decimal_str(b.balance),
                "chain": b.chain,
            })
        })
        .collect();
    let body = json!({
        "balances": formatted,
        "pagination_per_page": pagination,
        "pagination_page": page,
        "pagination_total": total,
        "pagination_item": "balances",
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

// ---------------------------------------------------------------------------
// Credit balances
// ---------------------------------------------------------------------------

async fn get_credit_balances_handler(
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let pagination = raw
        .get("pagination")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(100);
    let page = raw
        .get("page")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(1);
    if page < 1 {
        return Err(WebError::Unprocessable("page must be >= 1".into()));
    }
    let min_balance = raw
        .get("min_balance")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let cursor = raw.get("cursor").cloned();
    let client = get_db(&state).await?;

    if let Some(cursor_val) = cursor.as_deref() {
        let pagination_per_page = validate_cursor_pagination(Some(cursor_val), pagination)?;
        let after_address = if cursor_val.is_empty() {
            None
        } else {
            Some(
                decode_address_cursor(cursor_val)
                    .map_err(|e| WebError::Unprocessable(e.to_string()))?,
            )
        };
        let rows = get_credit_balances(
            &**client,
            1,
            pagination_per_page,
            min_balance,
            after_address.as_deref(),
            true,
        )
        .await?;
        let has_more = (rows.len() as i64) > pagination_per_page;
        let mut rows = rows;
        if has_more {
            rows.truncate(pagination_per_page as usize);
        }
        let formatted: Vec<Value> = rows
            .iter()
            .map(|(a, c)| json!({"address": a, "credits": c}))
            .collect();
        let next_cursor = if has_more && !rows.is_empty() {
            let (last_addr, _) = rows.last().unwrap();
            Some(encode_address_cursor(last_addr))
        } else {
            None
        };
        let body = json!({
            "credit_balances": formatted,
            "pagination_per_page": pagination_per_page,
            "next_cursor": next_cursor,
        });
        return Ok(json_text_response(StatusCode::OK, body.to_string()));
    }

    let rows = get_credit_balances(&**client, page, pagination, min_balance, None, false).await?;
    let total = count_credit_balances(&**client, min_balance).await?;
    let formatted: Vec<Value> = rows
        .iter()
        .map(|(a, c)| json!({"address": a, "credits": c}))
        .collect();
    let body = json!({
        "credit_balances": formatted,
        "pagination_per_page": pagination,
        "pagination_page": page,
        "pagination_total": total,
        "pagination_item": "credit_balances",
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

async fn get_account_files(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let pagination = raw
        .get("pagination")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(100);
    if pagination < 0 {
        return Err(WebError::Unprocessable("pagination must be >= 0".into()));
    }
    let page = raw
        .get("page")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(1);
    if page < 1 {
        return Err(WebError::Unprocessable("page must be >= 1".into()));
    }
    let sort_order = match raw.get("sort_order").and_then(|s| s.parse::<i32>().ok()) {
        Some(1) => SortOrder::Ascending,
        _ => SortOrder::Descending,
    };
    let cursor = raw.get("cursor").cloned();
    let file_hash = raw.get("file_hash").cloned();

    let client = get_db(&state).await?;

    if let Some(cursor_val) = cursor.as_deref() {
        let pagination_per_page = validate_cursor_pagination(Some(cursor_val), pagination)?;
        let (after_time, after_hash) = if cursor_val.is_empty() {
            (None, None)
        } else {
            let (t, h) = decode_message_cursor(cursor_val)
                .map_err(|e| WebError::Unprocessable(e.to_string()))?;
            (Some(t), Some(h))
        };
        let rows = get_address_files_for_api(
            &**client,
            &address,
            pagination_per_page,
            1,
            sort_order,
            after_time,
            after_hash.as_deref(),
            true,
            file_hash.as_deref(),
        )
        .await?;
        let (_nb, total_size) = get_address_files_stats(&**client, &address).await?;
        let has_more = (rows.len() as i64) > pagination_per_page;
        let mut rows = rows;
        if has_more {
            rows.truncate(pagination_per_page as usize);
        }
        if rows.is_empty() {
            return Err(WebError::NotFound("No files found for this address".into()));
        }
        let files: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "file_hash": r.file_hash,
                    "size": r.size,
                    "type": serde_json::to_value(r.r#type).unwrap(),
                    "created": r.created.to_rfc3339(),
                    "item_hash": r.item_hash,
                })
            })
            .collect();
        let next_cursor = if has_more && !rows.is_empty() {
            let last = rows.last().unwrap();
            Some(encode_message_cursor(last.created, &last.item_hash))
        } else {
            None
        };
        let body = json!({
            "address": address,
            "total_size": total_size,
            "files": files,
            "pagination_per_page": pagination_per_page,
            "next_cursor": next_cursor,
        });
        return Ok(json_text_response(StatusCode::OK, body.to_string()));
    }

    let rows = get_address_files_for_api(
        &**client,
        &address,
        pagination,
        page,
        sort_order,
        None,
        None,
        false,
        file_hash.as_deref(),
    )
    .await?;
    let (nb_files, total_size) = get_address_files_stats(&**client, &address).await?;
    if rows.is_empty() {
        return Err(WebError::NotFound("No files found for this address".into()));
    }
    let files: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "file_hash": r.file_hash,
                "size": r.size,
                "type": serde_json::to_value(r.r#type).unwrap(),
                "created": r.created.to_rfc3339(),
                "item_hash": r.item_hash,
            })
        })
        .collect();
    let body = json!({
        "address": address,
        "total_size": total_size,
        "files": files,
        "pagination_page": page,
        "pagination_total": nb_files,
        "pagination_per_page": pagination,
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

// ---------------------------------------------------------------------------
// Credit history
// ---------------------------------------------------------------------------

async fn get_account_credit_history(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let pagination = raw
        .get("pagination")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    if pagination < 0 {
        return Err(WebError::Unprocessable("pagination must be >= 0".into()));
    }
    let page = raw
        .get("page")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(1);
    if page < 1 {
        return Err(WebError::Unprocessable("page must be >= 1".into()));
    }
    let sort_by_str = raw
        .get("sort_by")
        .cloned()
        .unwrap_or_else(|| "message_timestamp".to_string());
    let sort_by: SortByCreditHistory =
        serde_json::from_value(Value::String(sort_by_str.clone()))
            .map_err(|_| WebError::Unprocessable(format!("Invalid sort_by: {sort_by_str}")))?;
    let sort_order = match raw.get("sort_order").and_then(|s| s.parse::<i32>().ok()) {
        Some(1) => SortOrder::Ascending,
        _ => SortOrder::Descending,
    };
    let has_expiration = raw
        .get("has_expiration")
        .and_then(|s| match s.to_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        });
    let exclude_pm_csv = raw.get("exclude_payment_method").cloned();
    let exclude_pm_vec: Option<Vec<String>> = exclude_pm_csv
        .as_deref()
        .map(|s| s.split(',').map(|x| x.to_string()).collect());

    let filters = CreditHistoryFilters {
        tx_hash: raw.get("tx_hash").map(|s| s.as_str()),
        token: raw.get("token").map(|s| s.as_str()),
        chain: raw.get("chain").map(|s| s.as_str()),
        provider: raw.get("provider").map(|s| s.as_str()),
        origin: raw.get("origin").map(|s| s.as_str()),
        origin_ref: raw.get("origin_ref").map(|s| s.as_str()),
        payment_method: raw.get("payment_method").map(|s| s.as_str()),
        has_expiration,
        exclude_payment_method: exclude_pm_vec.as_deref(),
    };

    let cursor = raw.get("cursor").cloned();
    let client = get_db(&state).await?;

    if let Some(cursor_val) = cursor.as_deref() {
        let pagination_per_page = validate_cursor_pagination(Some(cursor_val), pagination)?;
        let cursor_decoded = if cursor_val.is_empty() {
            None
        } else {
            Some(
                decode_credit_history_sort_cursor(cursor_val)
                    .map_err(|e| WebError::Unprocessable(e.to_string()))?,
            )
        };

        let (after_sort_value, after_credit_ref, after_credit_index) = if let Some(c) =
            cursor_decoded.as_ref()
        {
            if c.sort_by != sort_by_str {
                return Err(WebError::Unprocessable(format!(
                    "Cursor sort field mismatch: cursor was created with sort_by={}, but request has sort_by={}",
                    c.sort_by, sort_by_str
                )));
            }
            if c.sort_order != i32::from(sort_order) as i64 {
                return Err(WebError::Unprocessable(format!(
                    "Cursor sort order mismatch: cursor was created with sort_order={}, but request has sort_order={}",
                    c.sort_order,
                    i32::from(sort_order)
                )));
            }
            let sv: Option<String> = match &c.sort_value {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                Value::Null => None,
                other => Some(other.to_string()),
            };
            (sv, Some(c.credit_ref.clone()), Some(c.credit_index as i32))
        } else {
            (None, None, None)
        };

        let rows = get_address_credit_history(
            &**client,
            &address,
            1,
            pagination_per_page,
            &filters,
            sort_by,
            sort_order,
            after_sort_value.as_deref(),
            after_credit_ref.as_deref(),
            after_credit_index,
            true,
        )
        .await?;

        if rows.is_empty() {
            return Err(WebError::NotFound(
                "No credit history found for this address".into(),
            ));
        }
        let has_more = (rows.len() as i64) > pagination_per_page;
        let mut rows = rows;
        if has_more {
            rows.truncate(pagination_per_page as usize);
        }
        let history: Vec<Value> = rows.iter().map(credit_history_to_dict).collect();
        let next_cursor = if has_more && !rows.is_empty() {
            let last = rows.last().unwrap();
            let v = credit_sort_value(last, sort_by);
            Some(encode_credit_history_sort_cursor(
                &sort_by_str,
                v,
                i32::from(sort_order) as i64,
                &last.credit_ref,
                last.credit_index as i64,
            ))
        } else {
            None
        };
        let body = json!({
            "address": address,
            "credit_history": history,
            "pagination_per_page": pagination_per_page,
            "next_cursor": next_cursor,
        });
        return Ok(json_text_response(StatusCode::OK, body.to_string()));
    }

    let rows = get_address_credit_history(
        &**client, &address, page, pagination, &filters, sort_by, sort_order, None, None, None,
        false,
    )
    .await?;
    if rows.is_empty() {
        return Err(WebError::NotFound(
            "No credit history found for this address".into(),
        ));
    }
    let total = count_address_credit_history(&**client, &address, &filters).await?;
    let history: Vec<Value> = rows.iter().map(credit_history_to_dict).collect();
    let body = json!({
        "address": address,
        "credit_history": history,
        "pagination_page": page,
        "pagination_total": total,
        "pagination_per_page": pagination,
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

fn credit_history_to_dict(e: &crate::db::models::balances::AlephCreditHistoryDb) -> Value {
    json!({
        "amount": e.amount,
        "price": e.price.map(|d| d.to_string()),
        "bonus_amount": e.bonus_amount,
        "tx_hash": e.tx_hash,
        "token": e.token,
        "chain": e.chain,
        "provider": e.provider,
        "origin": e.origin,
        "origin_ref": e.origin_ref,
        "payment_method": e.payment_method,
        "credit_ref": e.credit_ref,
        "credit_index": e.credit_index,
        "expiration_date": e.expiration_date.map(|t| t.to_rfc3339()),
        "message_timestamp": e.message_timestamp.to_rfc3339(),
    })
}

fn credit_sort_value(
    e: &crate::db::models::balances::AlephCreditHistoryDb,
    sort_by: SortByCreditHistory,
) -> Value {
    match sort_by {
        SortByCreditHistory::MessageTimestamp => json!(e.message_timestamp.to_rfc3339()),
        SortByCreditHistory::ExpirationDate => e
            .expiration_date
            .map(|d| Value::String(d.to_rfc3339()))
            .unwrap_or(Value::Null),
        SortByCreditHistory::PaymentMethod => e
            .payment_method
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
        SortByCreditHistory::Amount => json!(e.amount),
        SortByCreditHistory::Origin => e.origin.clone().map(Value::String).unwrap_or(Value::Null),
        SortByCreditHistory::TxHash => e.tx_hash.clone().map(Value::String).unwrap_or(Value::Null),
        SortByCreditHistory::Provider => {
            e.provider.clone().map(Value::String).unwrap_or(Value::Null)
        }
    }
}

// ---------------------------------------------------------------------------
// Resource credits / post types / channels
// ---------------------------------------------------------------------------

async fn get_resource_consumed_credits_handler(
    State(state): State<AppState>,
    Path(item_hash): Path<String>,
) -> WebResult<Response> {
    if item_hash.is_empty() {
        return Err(WebError::Unprocessable(
            "Item hash must be specified.".into(),
        ));
    }
    let client = get_db(&state).await?;
    let consumed = get_resource_consumed_credits(&**client, &item_hash).await?;
    let body = json!({
        "item_hash": item_hash,
        "consumed_credits": consumed,
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

async fn get_account_post_types(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> WebResult<Response> {
    let client = get_db(&state).await?;
    let types = get_distinct_post_types_for_address(&**client, &address).await?;
    let body = json!({ "address": address, "post_types": types });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

async fn get_account_channels(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> WebResult<Response> {
    let client = get_db(&state).await?;
    let channels = get_distinct_channels_for_address(&**client, &address).await?;
    let body = json!({ "address": address, "channels": channels });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

fn raw_value(raw: &HashMap<String, String>) -> Value {
    let mut m = Map::new();
    for (k, v) in raw {
        m.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(m)
}

#[allow(dead_code)]
fn _types() {
    let _ = std::marker::PhantomData::<Chain>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn decimal_str_emits_string_representation() {
        // The wire format MUST be a JSON string — pyaleph emits Decimals via
        // its custom JSON encoder which serialises them as quoted strings.
        let d = Decimal::from_str("1.23456789").unwrap();
        let v = decimal_str(d);
        assert!(matches!(v, Value::String(_)));
        assert_eq!(v.as_str(), Some("1.23456789"));
    }

    #[test]
    fn decimal_str_normalises_trailing_zeroes() {
        // pyaleph's Decimal('1.0') -> '1', mirrored by `.normalize()`.
        let d = Decimal::from_str("1.0").unwrap();
        let v = decimal_str(d);
        assert_eq!(v.as_str(), Some("1"));
    }

    #[test]
    fn addresses_brackets_repeated_keys() {
        // `?addresses[]=a&addresses[]=b` — the canonical aiohttp encoding.
        let pairs = vec![
            ("addresses[]".to_string(), "a".to_string()),
            ("addresses[]".to_string(), "b".to_string()),
        ];
        let got = collect_addresses_brackets(&pairs);
        assert_eq!(got, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn addresses_brackets_csv_form() {
        // Legacy `?addresses[]=a,b` form is preserved for backwards compat.
        let pairs = vec![("addresses[]".to_string(), "a,b".to_string())];
        let got = collect_addresses_brackets(&pairs);
        assert_eq!(got, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn addresses_brackets_mixed() {
        let pairs = vec![
            ("addresses[]".to_string(), "a,b".to_string()),
            ("addresses[]".to_string(), "c".to_string()),
            ("other".to_string(), "ignored".to_string()),
        ];
        let got = collect_addresses_brackets(&pairs);
        assert_eq!(got, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    }
}
