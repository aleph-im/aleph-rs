//! Mirrors `aleph/web/controllers/aggregates.py`.

use std::collections::HashMap;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::db::accessors::aggregates::{
    AggregatesByOwner, AggregatesQuery, count_aggregates, get_aggregates_by_owner,
    get_aggregates_with_last_revision, get_dirty_aggregate_keys_for_owner, refresh_aggregate,
};
use crate::schemas::messages_query_params::{
    DEFAULT_MESSAGES_PER_PAGE, DEFAULT_PAGE, LIST_FIELD_SEPARATOR,
};
use crate::toolkit::cursor::{decode_aggregate_cursor, encode_aggregate_cursor};
use crate::types::sort_order::{SortByAggregate, SortOrder};
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::utils::{get_db, json_text_response, validate_cursor_pagination};

const DEFAULT_LIMIT: i64 = 1000;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/v0/aggregates.json", get(view_aggregates_list))
        .route("/api/v0/aggregates", get(view_aggregates_list))
        // axum 0.8 forbids literal suffixes on a path parameter; we capture the
        // full last segment and strip the `.json` extension in the handler so the
        // pyaleph URL surface (`/api/v0/aggregates/<address>.json`) is preserved.
        .route("/api/v0/aggregates/{address_json}", get(address_aggregate))
}

#[derive(Debug, Deserialize)]
struct AggregatesQueryParams {
    #[serde(default)]
    keys: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    with_info: Option<String>,
    #[serde(default)]
    value_only: Option<String>,
}

fn parse_bool(s: Option<&str>) -> bool {
    match s {
        Some(v) => matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"),
        None => false,
    }
}

fn split_csv(s: Option<&str>) -> Option<Vec<String>> {
    s.map(|v| {
        v.split(LIST_FIELD_SEPARATOR)
            .map(|x| x.to_string())
            .collect()
    })
}

async fn address_aggregate(
    State(state): State<AppState>,
    Path(address_json): Path<String>,
    Query(raw): Query<AggregatesQueryParams>,
) -> WebResult<Response> {
    let address = address_json
        .strip_suffix(".json")
        .ok_or_else(|| WebError::NotFound(format!("Unknown route: {address_json}")))?
        .to_string();
    let keys = split_csv(raw.keys.as_deref());
    let _limit = raw.limit.unwrap_or(DEFAULT_LIMIT);
    let with_info = parse_bool(raw.with_info.as_deref());
    let value_only = parse_bool(raw.value_only.as_deref());

    let client = get_db(&state).await?;
    // Mirror pyaleph: scan dirty aggregates for the address and refresh each
    // before reading. Drops the previous stale view of an aggregate as soon as
    // a writer marks it dirty.
    let dirty_keys = get_dirty_aggregate_keys_for_owner(&**client, &address).await?;
    for key in &dirty_keys {
        if let Err(e) = refresh_aggregate(&**client, &address, key).await {
            tracing::warn!(?e, owner = %address, key = %key, "refresh_aggregate failed");
        }
    }
    let result = get_aggregates_by_owner(&**client, &address, with_info, keys.as_deref()).await?;

    let is_empty = match &result {
        AggregatesByOwner::Plain(v) => v.is_empty(),
        AggregatesByOwner::WithInfo(v) => v.is_empty(),
    };
    if is_empty {
        return Err(WebError::NotFound(
            "No aggregate found for this address".into(),
        ));
    }

    if value_only {
        if let (AggregatesByOwner::Plain(rows), Some(keys)) = (&result, &keys) {
            if keys.len() == 1 {
                let target = &keys[0];
                for row in rows {
                    if &row.key == target {
                        return Ok(json_text_response(StatusCode::OK, row.content.to_string()));
                    }
                }
                return Err(WebError::NotFound(
                    "No aggregate found for this address".into(),
                ));
            }
        }
    }

    let mut data = Map::new();
    let mut info = Map::new();
    match result {
        AggregatesByOwner::Plain(rows) => {
            for row in rows {
                data.insert(row.key, row.content);
            }
        }
        AggregatesByOwner::WithInfo(rows) => {
            for row in rows {
                data.insert(row.key.clone(), row.content);
                info.insert(
                    row.key,
                    json!({
                        "created": row.created.to_rfc3339(),
                        "last_updated": row.last_updated.to_rfc3339(),
                        "original_item_hash": row.original_item_hash,
                        "last_update_item_hash": row.last_update_item_hash,
                    }),
                );
            }
        }
    }

    let body = json!({
        "address": address,
        "data": Value::Object(data),
        "info": Value::Object(info),
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

#[derive(Debug, Deserialize)]
struct AggregatesListQueryParams {
    #[serde(default)]
    keys: Option<String>,
    #[serde(default)]
    addresses: Option<String>,
    #[serde(default, rename = "sortBy", alias = "sort_by")]
    sort_by: Option<String>,
    #[serde(default, rename = "sortOrder", alias = "sort_order")]
    sort_order: Option<i32>,
    #[serde(default)]
    pagination: Option<i64>,
    #[serde(default)]
    page: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
}

async fn view_aggregates_list(
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let p = AggregatesListQueryParams {
        keys: raw.get("keys").cloned(),
        addresses: raw.get("addresses").cloned(),
        sort_by: raw.get("sortBy").or_else(|| raw.get("sort_by")).cloned(),
        sort_order: raw
            .get("sortOrder")
            .or_else(|| raw.get("sort_order"))
            .and_then(|s| s.parse::<i32>().ok()),
        pagination: raw.get("pagination").and_then(|s| s.parse::<i64>().ok()),
        page: raw.get("page").and_then(|s| s.parse::<i64>().ok()),
        cursor: raw.get("cursor").cloned(),
    };
    let sort_by = match p.sort_by.as_deref() {
        Some("creation_time") => SortByAggregate::CreationTime,
        _ => SortByAggregate::LastModified,
    };
    let sort_order = match p.sort_order {
        Some(1) => SortOrder::Ascending,
        Some(-1) | None => SortOrder::Descending,
        Some(other) => {
            return Err(WebError::Unprocessable(format!(
                "Invalid sortOrder: {other}"
            )));
        }
    };
    let pagination = p.pagination.unwrap_or(DEFAULT_MESSAGES_PER_PAGE);
    if !(1..=500).contains(&pagination) {
        return Err(WebError::Unprocessable(
            "pagination must be in [1, 500]".into(),
        ));
    }
    let page = p.page.unwrap_or(DEFAULT_PAGE);
    if page < 1 {
        return Err(WebError::Unprocessable("page must be >= 1".into()));
    }
    let keys = split_csv(p.keys.as_deref());
    let addresses = split_csv(p.addresses.as_deref());
    let cursor = p.cursor.clone();

    let client = get_db(&state).await?;

    if let Some(cursor_val) = cursor.as_deref() {
        let pagination_per_page = validate_cursor_pagination(Some(cursor_val), pagination)?;
        let (after_time, after_key, after_owner) = if cursor_val.is_empty() {
            (None, None, None)
        } else {
            let (t, k, o) = decode_aggregate_cursor(cursor_val)
                .map_err(|e| WebError::Unprocessable(e.to_string()))?;
            (Some(t), Some(k), Some(o))
        };
        let q = AggregatesQuery {
            keys: keys.clone(),
            addresses: addresses.clone(),
            sort_by,
            sort_order,
            page,
            pagination: pagination_per_page,
            after_time,
            after_key,
            after_owner,
            cursor_mode: true,
        };
        let mut aggregates = get_aggregates_with_last_revision(&**client, &q).await?;
        let has_more = (aggregates.len() as i64) > pagination_per_page;
        if has_more {
            aggregates.truncate(pagination_per_page as usize);
        }
        let next_cursor = if has_more && !aggregates.is_empty() {
            let (last, last_rev) = aggregates.last().unwrap();
            // Match pyaleph: for LAST_MODIFIED sort the cursor anchor is the
            // last revision time, not the aggregate creation time. For
            // CREATION_TIME sort, both columns coincide via the create-row.
            let cursor_time = match sort_by {
                SortByAggregate::LastModified => *last_rev,
                SortByAggregate::CreationTime => last.creation_datetime,
            };
            Some(encode_aggregate_cursor(
                cursor_time,
                &last.key,
                &last.owner,
            ))
        } else {
            None
        };
        let aggs: Vec<Value> = aggregates
            .iter()
            .map(|(a, last_rev)| {
                json!({
                    "address": a.owner,
                    "key": a.key,
                    "content": a.content,
                    "created": a.creation_datetime.to_rfc3339(),
                    "last_updated": last_rev.to_rfc3339(),
                })
            })
            .collect();
        let body = json!({
            "aggregates": aggs,
            "pagination_per_page": pagination_per_page,
            "next_cursor": next_cursor,
        });
        return Ok(json_text_response(StatusCode::OK, body.to_string()));
    }

    let q = AggregatesQuery {
        keys: keys.clone(),
        addresses: addresses.clone(),
        sort_by,
        sort_order,
        page,
        pagination,
        after_time: None,
        after_key: None,
        after_owner: None,
        cursor_mode: false,
    };
    let aggregates = get_aggregates_with_last_revision(&**client, &q).await?;
    let total = count_aggregates(&**client, keys.as_deref(), addresses.as_deref()).await?;
    let aggs: Vec<Value> = aggregates
        .iter()
        .map(|(a, last_rev)| {
            json!({
                "address": a.owner,
                "key": a.key,
                "content": a.content,
                "created": a.creation_datetime.to_rfc3339(),
                "last_updated": last_rev.to_rfc3339(),
            })
        })
        .collect();
    let body = json!({
        "aggregates": aggs,
        "pagination_per_page": pagination,
        "pagination_page": page,
        "pagination_total": total,
        "pagination_item": "aggregates",
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}
