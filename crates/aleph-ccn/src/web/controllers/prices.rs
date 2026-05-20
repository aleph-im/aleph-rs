//! Mirrors `aleph/web/controllers/prices.py`. All endpoints are wired through
//! to `crate::services::cost` and `crate::services::cost_validation`.

use std::collections::HashMap;

use aleph_types::item_hash::ItemHash;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::middleware::from_fn_with_state;
use axum::response::Response;
use axum::routing::{get, post};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::db::accessors::balances::{
    get_consumed_credits_by_resource, get_total_consumed_credits,
};
use crate::db::accessors::cost::{
    count_resources_with_costs, get_costs_summary, get_message_costs,
    get_message_costs_with_file_sizes, get_resources_with_costs,
};
use crate::db::accessors::messages::get_message_status;
use crate::db::models::account_costs::PaymentType;
use crate::services::cost::{
    CostContent, CostContentKind, get_cost_component_size_mib, get_payment_type,
    get_total_and_detailed_costs,
};
use crate::toolkit::costs::{CostInput, format_cost_str};
use crate::types::message_status::MessageStatus;
use crate::web::AppState;
use crate::web::controllers::auth::require_auth_token;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::utils::{get_db, json_text_response};

pub fn routes(state: AppState) -> Router<AppState> {
    // `recalculate_*` mutates cost rows for arbitrary messages, so pyaleph
    // protects both endpoints with `@require_auth_token`. We mirror that by
    // splitting the router and layering the middleware only on those routes.
    let public = Router::new()
        .route("/api/v0/costs", get(get_costs))
        .route("/api/v0/price/{item_hash}", get(message_price))
        .route("/api/v0/price/estimate", post(message_price_estimate))
        .route(
            "/api/v0/price/estimate/instance",
            post(instance_cost_estimate),
        );

    // `route_layer` applies the middleware *only* to the matched routes
    // below — unmatched fall-through traffic (404s) skips the auth check.
    let protected = Router::new()
        .route("/api/v0/price/recalculate", post(recalculate_message_costs))
        .route(
            "/api/v0/price/{item_hash}/recalculate",
            post(recalculate_message_costs_with_hash),
        )
        .route_layer(from_fn_with_state(state, require_auth_token));

    public.merge(protected)
}

fn payment_type_label(p: PaymentType) -> &'static str {
    match p {
        PaymentType::Hold => "hold",
        PaymentType::Superfluid => "superfluid",
        PaymentType::Credit => "credit",
    }
}

fn parse_payment_type(s: Option<&str>) -> WebResult<PaymentType> {
    match s.unwrap_or("credit") {
        "hold" => Ok(PaymentType::Hold),
        "superfluid" => Ok(PaymentType::Superfluid),
        "credit" => Ok(PaymentType::Credit),
        other => Err(WebError::Unprocessable(format!(
            "Invalid payment_type: {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// /api/v0/price/{item_hash}
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct PriceQuery {
    #[serde(default)]
    include_size: Option<String>,
}

async fn message_price(
    State(state): State<AppState>,
    Path(item_hash): Path<String>,
    Query(q): Query<PriceQuery>,
) -> WebResult<Response> {
    // Reject unknown hash format up-front.
    ItemHash::try_from(item_hash.as_str())
        .map_err(|_| WebError::BadRequest(format!("Invalid message hash: {item_hash}")))?;

    let include_size = q
        .include_size
        .as_deref()
        .map(|s| matches!(s.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    let client = get_db(&state).await?;
    let status_db = get_message_status(&**client, &item_hash)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("Message not found with hash: {item_hash}")))?;
    match status_db.status {
        MessageStatus::Pending => return Err(WebError::Internal("Message still pending".into())),
        MessageStatus::Rejected => {
            return Err(WebError::NotFound("This message was rejected".into()));
        }
        MessageStatus::Forgotten => {
            return Err(WebError::Gone("This message has been forgotten".into()));
        }
        MessageStatus::Removed => {
            return Err(WebError::Gone("This message has been removed".into()));
        }
        MessageStatus::Processed | MessageStatus::Removing => {}
    }

    // Iterate over stored cost rows.
    let cost_rows: Vec<crate::db::models::account_costs::AccountCostsDb> = if include_size {
        // get_message_costs_with_file_sizes returns (cost, Option<i64>).
        let with_sizes = get_message_costs_with_file_sizes(&**client, &item_hash).await?;
        with_sizes.into_iter().map(|(c, _)| c).collect()
    } else {
        get_message_costs(&**client, &item_hash).await?
    };

    if cost_rows.is_empty() {
        return Err(WebError::NotFound(format!(
            "No cost data for message: {item_hash}"
        )));
    }

    let payment_type = cost_rows[0].payment_type;
    let charged_address = cost_rows[0].owner.clone();
    let mut hold_total = Decimal::ZERO;
    let mut stream_total = Decimal::ZERO;
    let mut credit_total = Decimal::ZERO;
    let mut detail: Vec<Value> = Vec::new();
    for cost in &cost_rows {
        hold_total += cost.cost_hold;
        stream_total += cost.cost_stream;
        credit_total += cost.cost_credit;
        detail.push(json!({
            "type": cost.r#type.as_value_str(),
            "name": cost.name,
            "cost_hold": format_cost_str(cost.cost_hold, None),
            "cost_stream": format_cost_str(cost.cost_stream, None),
            "cost_credit": format_cost_str(cost.cost_credit, None),
            "size_mib": Value::Null,
        }));
    }
    let total = match payment_type {
        PaymentType::Hold => hold_total,
        PaymentType::Superfluid => stream_total,
        PaymentType::Credit => credit_total,
    };
    let body = json!({
        "required_tokens": total.to_string().parse::<f64>().unwrap_or(0.0),
        "payment_type": payment_type_label(payment_type),
        "cost": format_cost_str(total, None),
        "detail": detail,
        "charged_address": charged_address,
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

// ---------------------------------------------------------------------------
// /api/v0/price/estimate{,/instance}
// ---------------------------------------------------------------------------

/// Render the `EstimatedCostsResponse` shape from a content view + cost rows.
async fn build_estimated_response<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    content: &CostContent<'_>,
    total: Decimal,
    cost_rows: Vec<crate::db::models::account_costs::AccountCostsDb>,
) -> WebResult<Response> {
    let payment_type = get_payment_type(content);
    let mut detail: Vec<Value> = Vec::with_capacity(cost_rows.len());
    for cost in &cost_rows {
        let size_mib = get_cost_component_size_mib(Some(client), cost, Some(content)).await?;
        detail.push(json!({
            "type": cost.r#type.as_value_str(),
            "name": cost.name,
            "cost_hold": format_cost_str(CostInput::Decimal(cost.cost_hold), None),
            "cost_stream": format_cost_str(CostInput::Decimal(cost.cost_stream), None),
            "cost_credit": format_cost_str(CostInput::Decimal(cost.cost_credit), None),
            "size_mib": size_mib,
        }));
    }
    let body = json!({
        "required_tokens": total.to_string().parse::<f64>().unwrap_or(0.0),
        "payment_type": payment_type_label(payment_type),
        "cost": format_cost_str(CostInput::Decimal(total), None),
        "detail": detail,
        "charged_address": content.address(),
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

#[derive(Debug, Deserialize)]
struct PriceEstimateRequest {
    /// pyaleph wraps the candidate message under "message".
    #[serde(default)]
    message: Option<Value>,
    /// Some clients send the message dict at the top level. Accept either.
    #[serde(flatten)]
    rest: HashMap<String, Value>,
}

async fn message_price_estimate(
    State(state): State<AppState>,
    axum::Json(body): axum::Json<Value>,
) -> WebResult<Response> {
    let req: PriceEstimateRequest = serde_json::from_value(body.clone())
        .map_err(|e| WebError::Unprocessable(format!("Invalid request body: {e}")))?;
    let message = req
        .message
        .or_else(|| Some(Value::Object(req.rest.into_iter().collect())))
        .ok_or_else(|| WebError::Unprocessable("missing 'message' field".into()))?;

    // The candidate message must carry a `content` field that describes an
    // executable / store payload — we look at `content` first, fall back to the
    // top-level body for bare content payloads.
    let content_json = message.get("content").cloned().unwrap_or(message.clone());

    let kind = if content_json.get("rootfs").is_some() {
        CostContentKind::Instance
    } else if content_json.get("code").is_some() {
        CostContentKind::Program
    } else if content_json.get("item_hash").is_some() {
        CostContentKind::Store
    } else {
        return Err(WebError::Unprocessable(
            "Unable to determine content kind for estimation".into(),
        ));
    };
    let content = CostContent::new(kind, &content_json);
    let item_hash = message
        .get("item_hash")
        .and_then(|v| v.as_str())
        .unwrap_or("estimate");

    let client = get_db(&state).await?;
    let (total, costs) = get_total_and_detailed_costs(&**client, &content, item_hash)
        .await
        .map_err(|e| WebError::NotFound(e.to_string()))?;
    build_estimated_response(&**client, &content, total, costs).await
}

async fn instance_cost_estimate(
    State(state): State<AppState>,
    axum::Json(body): axum::Json<Value>,
) -> WebResult<Response> {
    if !body.is_object() {
        return Err(WebError::BadRequest("Invalid JSON body".into()));
    }
    // Body is the instance content directly (CostEstimationInstanceContent).
    let content = CostContent::new(CostContentKind::Instance, &body);
    let client = get_db(&state).await?;
    let (total, costs) = get_total_and_detailed_costs(&**client, &content, "estimate")
        .await
        .map_err(|e| WebError::NotFound(e.to_string()))?;
    build_estimated_response(&**client, &content, total, costs).await
}

async fn recalculate_message_costs(State(state): State<AppState>) -> WebResult<Response> {
    // Full chronological walk: re-price every processed STORE/PROGRAM/INSTANCE
    // message using the pricing model that was in force at message time.
    // Mirrors pyaleph's `recalculate_message_costs`.
    let client = get_db(&state).await?;
    let rows = client
        .query(
            "SELECT item_hash, content, time FROM messages \
             WHERE status = 'PROCESSED' AND type IN ('STORE', 'PROGRAM', 'INSTANCE') \
             ORDER BY time ASC",
            &[],
        )
        .await
        .map_err(WebError::from)?;
    let total_messages = rows.len() as i64;

    if rows.is_empty() {
        return Ok(json_text_response(
            StatusCode::OK,
            json!({
                "message": "No messages found for cost recalculation",
                "recalculated_count": 0,
                "total_messages": 0,
            })
            .to_string(),
        ));
    }

    // Build the pricing timeline (timestamp, model). We also need the matching
    // aggregate value for the in-process cost cache — we walk the merged
    // aggregate history in lockstep with the model timeline.
    let timeline = crate::services::pricing_utils::get_pricing_timeline(&**client)
        .await
        .map_err(|e| WebError::Internal(format!("pricing timeline: {e}")))?;
    let pricing_changes_found = timeline.len() as i64;

    let history = crate::services::pricing_utils::get_pricing_aggregate_history(&**client)
        .await
        .map_err(|e| WebError::Internal(format!("pricing history: {e}")))?;

    // Pre-compute the merged aggregate value at each timeline step. Slot 0 is
    // the default aggregate (matching `dt.datetime.min`); slot i+1 is the
    // merged view of the first i+1 revisions.
    use crate::db::accessors::aggregates::merge_aggregate_elements;
    use crate::toolkit::constants::DEFAULT_PRICE_AGGREGATE;
    let mut aggregate_values: Vec<Value> = Vec::with_capacity(timeline.len());
    aggregate_values.push(DEFAULT_PRICE_AGGREGATE.clone());
    let mut so_far = Vec::<&crate::db::models::aggregates::AggregateElementDb>::new();
    for el in history.iter() {
        so_far.push(el);
        let merged = merge_aggregate_elements(so_far.iter().copied());
        aggregate_values.push(Value::Object(merged));
    }

    let mut current_idx: usize = 0;
    let mut recalculated_count: u64 = 0;
    let mut errors: Vec<Value> = Vec::new();

    for row in rows {
        let item_hash: String = row.get(0);
        let content_value: serde_json::Value = row.get(1);
        let msg_time: chrono::DateTime<chrono::Utc> = row.get(2);

        // Advance to the latest pricing segment whose start time is <= msg_time.
        while current_idx + 1 < timeline.len() && timeline[current_idx + 1].0 <= msg_time {
            current_idx += 1;
        }
        // Pin the active pricing aggregate in the cost engine's local cache so
        // `get_total_and_detailed_costs` resolves prices against the segment
        // that was in force at message time.
        crate::services::cost::invalidate_aggregate_cache();
        if current_idx < aggregate_values.len() {
            crate::services::cache::local::GLOBAL_CACHE.set(
                "price",
                aggregate_values[current_idx].clone(),
                "cost_aggregates",
            );
        }

        match recompute_costs_for_row(&**client, &item_hash, &content_value).await {
            Ok(true) => recalculated_count += 1,
            Ok(false) => {
                // Content was un-priceable (no rootfs/code/item_hash) — pyaleph
                // logs a warning and continues; we record nothing.
            }
            Err(e) => {
                errors.push(json!({
                    "item_hash": item_hash,
                    "error": format!("{e:?}"),
                }));
            }
        }
    }
    // Drop the pinned pricing aggregate so subsequent endpoints see the
    // current value again.
    crate::services::cost::invalidate_aggregate_cache();

    let mut body = json!({
        "message": "Cost recalculation completed with historical pricing",
        "recalculated_count": recalculated_count,
        "total_messages": total_messages,
        "pricing_changes_found": pricing_changes_found,
    });
    if !errors.is_empty() {
        body["errors"] = Value::Array(errors);
    }
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

async fn recalculate_message_costs_with_hash(
    State(state): State<AppState>,
    Path(item_hash): Path<String>,
) -> WebResult<Response> {
    let client = get_db(&state).await?;
    let row = client
        .query_opt(
            "SELECT content FROM messages WHERE item_hash = $1 AND status = 'PROCESSED'",
            &[&item_hash],
        )
        .await
        .map_err(WebError::from)?
        .ok_or_else(|| WebError::NotFound(format!("Message not found: {item_hash}")))?;
    let content_value: serde_json::Value = row.get(0);
    let updated = recompute_costs_for_row(&**client, &item_hash, &content_value).await?;
    Ok(json_text_response(
        StatusCode::OK,
        json!({ "updated": if updated { 1 } else { 0 } }).to_string(),
    ))
}

/// Recompute & persist costs for a single message row. Returns true if rows
/// were upserted (i.e. the content was priceable).
async fn recompute_costs_for_row<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    item_hash: &str,
    content_value: &Value,
) -> WebResult<bool> {
    let Some(content) = CostContent::from_value(content_value) else {
        return Ok(false);
    };
    match get_total_and_detailed_costs(client, &content, item_hash).await {
        Ok((_total, costs)) => {
            crate::db::accessors::cost::delete_costs_for_message(client, item_hash)
                .await
                .map_err(WebError::from)?;
            crate::db::accessors::cost::upsert_costs(client, &costs)
                .await
                .map_err(WebError::from)?;
            Ok(true)
        }
        Err(_) => Ok(false),
    }
}

// ---------------------------------------------------------------------------
// /api/v0/costs
// ---------------------------------------------------------------------------

async fn get_costs(
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let address = raw.get("address").cloned();
    let item_hash = raw.get("item_hash").cloned();
    let payment_type = parse_payment_type(raw.get("payment_type").map(|s| s.as_str()))?;
    let include_details = raw
        .get("include_details")
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(0);
    if !(0..=2).contains(&include_details) {
        return Err(WebError::Unprocessable(
            "include_details must be in [0, 2]".into(),
        ));
    }
    if include_details >= 2 && address.is_none() && item_hash.is_none() {
        return Err(WebError::Unprocessable(
            "include_details=2 requires at least one of 'address' or 'item_hash' filters to avoid fetching breakdowns for all resources".into(),
        ));
    }
    let pagination = raw
        .get("pagination")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(100);
    if !(10..=1000).contains(&pagination) {
        return Err(WebError::Unprocessable(
            "pagination must be in [10, 1000]".into(),
        ));
    }
    let page = raw
        .get("page")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(1);
    if page < 1 {
        return Err(WebError::Unprocessable("page must be >= 1".into()));
    }
    let include_size = raw
        .get("include_size")
        .map(|s| matches!(s.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    let client = get_db(&state).await?;

    let summary = get_costs_summary(
        &**client,
        address.as_deref(),
        item_hash.as_deref(),
        Some(payment_type),
    )
    .await?;
    let total_consumed =
        get_total_consumed_credits(&**client, address.as_deref(), item_hash.as_deref()).await?;

    let summary_v = json!({
        "total_consumed_credits": total_consumed,
        "total_cost_hold": summary.total_cost_hold,
        "total_cost_stream": summary.total_cost_stream,
        "total_cost_credit": summary.total_cost_credit,
        "resource_count": summary.resource_count,
    });
    let filters_v = json!({
        "address": address,
        "item_hash": item_hash,
        "payment_type": payment_type_label(payment_type),
    });
    let mut body = json!({
        "summary": summary_v,
        "filters": filters_v,
    });

    if include_details >= 1 {
        let resources = get_resources_with_costs(
            &**client,
            address.as_deref(),
            item_hash.as_deref(),
            Some(payment_type),
            page,
            pagination,
        )
        .await?;
        let item_hashes: Vec<String> = resources.iter().map(|r| r.item_hash.clone()).collect();
        let consumed_map = get_consumed_credits_by_resource(&**client, &item_hashes).await?;
        let mut formatted: Vec<Value> = Vec::with_capacity(resources.len());
        for row in &resources {
            let mut item = json!({
                "item_hash": row.item_hash,
                "owner": row.owner,
                "payment_type": row.payment_type,
                "consumed_credits": consumed_map.get(&row.item_hash).copied().unwrap_or(0),
                "cost_hold": format_cost_str(CostInput::Decimal(row.cost_hold), None),
                "cost_stream": format_cost_str(CostInput::Decimal(row.cost_stream), None),
                "cost_credit": format_cost_str(CostInput::Decimal(row.cost_credit), None),
            });
            if include_details >= 2 {
                let cost_items: Vec<(
                    crate::db::models::account_costs::AccountCostsDb,
                    Option<i64>,
                )> = if include_size {
                    get_message_costs_with_file_sizes(&**client, &row.item_hash).await?
                } else {
                    get_message_costs(&**client, &row.item_hash)
                        .await?
                        .into_iter()
                        .map(|c| (c, None))
                        .collect()
                };
                let mut detail: Vec<Value> = Vec::new();
                for (cost, file_size) in cost_items {
                    let size_mib = file_size.map(|b| (b as f64) / (1024.0 * 1024.0));
                    detail.push(json!({
                        "type": cost.r#type.as_value_str(),
                        "name": cost.name,
                        "cost_hold": format_cost_str(cost.cost_hold, None),
                        "cost_stream": format_cost_str(cost.cost_stream, None),
                        "cost_credit": format_cost_str(cost.cost_credit, None),
                        "size_mib": size_mib,
                    }));
                }
                item["detail"] = Value::Array(detail);
            }
            formatted.push(item);
        }
        let total_resources = count_resources_with_costs(
            &**client,
            address.as_deref(),
            item_hash.as_deref(),
            Some(payment_type),
        )
        .await?;
        body["resources"] = Value::Array(formatted);
        body["pagination_page"] = json!(page);
        body["pagination_total"] = json!(total_resources);
        body["pagination_per_page"] = json!(pagination);
        body["pagination_item"] = json!("resources");
    }
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reproduce the JSON envelope built by `recalculate_message_costs` for
    /// the "no messages" fast-path. The endpoint contract is brittle (clients
    /// inspect every field) so we assert the exact shape here.
    #[test]
    fn recalc_empty_response_shape() {
        let body = json!({
            "message": "No messages found for cost recalculation",
            "recalculated_count": 0,
            "total_messages": 0,
        });
        let s = body.to_string();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["recalculated_count"], 0);
        assert_eq!(v["total_messages"], 0);
        assert_eq!(v["message"], "No messages found for cost recalculation");
    }

    /// Full-shape variant: a non-empty walk emits `pricing_changes_found` and
    /// the same envelope fields. `errors` is optional and only present when
    /// at least one row failed.
    #[test]
    fn recalc_full_response_shape() {
        let body = json!({
            "message": "Cost recalculation completed with historical pricing",
            "recalculated_count": 3u64,
            "total_messages": 5i64,
            "pricing_changes_found": 2i64,
            "errors": [{"item_hash": "h", "error": "boom"}],
        });
        assert!(body.get("message").is_some());
        assert!(body.get("recalculated_count").is_some());
        assert!(body.get("total_messages").is_some());
        assert!(body.get("pricing_changes_found").is_some());
        let errs = body["errors"].as_array().unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0]["item_hash"], "h");
    }

    #[test]
    fn costs_payment_type_parser_matches_pyaleph_default_and_validation() {
        assert_eq!(parse_payment_type(None).unwrap(), PaymentType::Credit);
        assert_eq!(parse_payment_type(Some("credit")).unwrap(), PaymentType::Credit);
        assert_eq!(parse_payment_type(Some("hold")).unwrap(), PaymentType::Hold);
        assert!(parse_payment_type(Some("bogus")).is_err());
    }
}
