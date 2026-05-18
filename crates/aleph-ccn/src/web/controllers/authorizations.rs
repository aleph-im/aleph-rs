//! Mirrors `aleph/web/controllers/authorizations.py`.

use std::collections::HashMap;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use serde_json::{Map, Value, json};

use crate::db::accessors::authorizations::{
    AuthFilter, filter_authorizations, get_granted_authorizations, get_received_authorizations,
    paginate_authorizations,
};
use crate::schemas::messages_query_params::LIST_FIELD_SEPARATOR;
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::utils::{get_db, json_text_response};

pub fn routes() -> Router<AppState> {
    Router::new()
        // axum 0.8 forbids literal suffixes on path params: capture the segment
        // and strip `.json` in the handlers below.
        .route(
            "/api/v0/authorizations/granted/{address_json}",
            get(view_granted),
        )
        .route(
            "/api/v0/authorizations/received/{address_json}",
            get(view_received),
        )
}

#[derive(Debug, Default)]
struct AuthQuery {
    channels: Option<Vec<String>>,
    types: Option<Vec<String>>,
    post_types: Option<Vec<String>>,
    chains: Option<Vec<String>>,
    aggregate_keys: Option<Vec<String>>,
    pagination: i64,
    page: i64,
    party: Option<String>,
}

fn split_csv(s: Option<&String>) -> Option<Vec<String>> {
    s.map(|v| {
        v.split(LIST_FIELD_SEPARATOR)
            .map(|x| x.to_string())
            .collect()
    })
}

fn parse_query(raw: &HashMap<String, String>, party_key: &str) -> WebResult<AuthQuery> {
    let pagination = raw
        .get("pagination")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(20);
    if !(1..=500).contains(&pagination) {
        return Err(WebError::Unprocessable(
            "pagination must be in [1, 500]".into(),
        ));
    }
    let page = raw
        .get("page")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(1);
    if page < 1 {
        return Err(WebError::Unprocessable("page must be >= 1".into()));
    }
    Ok(AuthQuery {
        channels: split_csv(raw.get("channels")),
        types: split_csv(raw.get("types")),
        post_types: split_csv(raw.get("postTypes")).or_else(|| split_csv(raw.get("post_types"))),
        chains: split_csv(raw.get("chains")),
        aggregate_keys: split_csv(raw.get("aggregateKeys"))
            .or_else(|| split_csv(raw.get("aggregate_keys"))),
        pagination,
        page,
        party: raw.get(party_key).cloned(),
    })
}

fn build_grouped_from_content(content: &Value) -> HashMap<String, Vec<Value>> {
    let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();
    if let Some(arr) = content.get("authorizations").and_then(|v| v.as_array()) {
        for auth in arr {
            if let Some(obj) = auth.as_object() {
                if let Some(addr) = obj.get("address").and_then(|v| v.as_str()) {
                    if addr.is_empty() {
                        continue;
                    }
                    let mut entry = obj.clone();
                    entry.remove("address");
                    grouped
                        .entry(addr.to_string())
                        .or_default()
                        .push(Value::Object(entry));
                }
            }
        }
    }
    grouped
}

async fn view_granted(
    State(state): State<AppState>,
    Path(address_json): Path<String>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let address = address_json
        .strip_suffix(".json")
        .ok_or_else(|| WebError::NotFound(format!("Unknown route: {address_json}")))?
        .to_string();
    let q = parse_query(&raw, "grantee")?;
    let client = get_db(&state).await?;
    // Mirror pyaleph: refresh any dirty aggregates for the owner so the
    // security aggregate served reflects all merged revisions.
    let dirty_keys =
        crate::db::accessors::aggregates::get_dirty_aggregate_keys_for_owner(&**client, &address)
            .await?;
    for key in &dirty_keys {
        if let Err(e) =
            crate::db::accessors::aggregates::refresh_aggregate(&**client, &address, key).await
        {
            tracing::warn!(?e, owner = %address, key = %key, "refresh_aggregate failed");
        }
    }
    let content = get_granted_authorizations(&**client, &address).await?;
    let mut grouped = match content {
        Some(c) => build_grouped_from_content(&c),
        None => HashMap::new(),
    };
    if let Some(grantee) = &q.party {
        grouped.retain(|k, _| k == grantee);
    }
    let filter = AuthFilter {
        channels: q.channels.as_deref(),
        types: q.types.as_deref(),
        post_types: q.post_types.as_deref(),
        chains: q.chains.as_deref(),
        aggregate_keys: q.aggregate_keys.as_deref(),
    };
    let filtered = filter_authorizations(&grouped, &filter);
    let (page_map, total) = paginate_authorizations(&filtered, q.page, q.pagination);
    let auths: Map<String, Value> = page_map
        .into_iter()
        .map(|(k, v)| (k, Value::Array(v)))
        .collect();
    let body = json!({
        "authorizations": Value::Object(auths),
        "pagination_page": q.page,
        "pagination_per_page": q.pagination,
        "pagination_total": total,
        "pagination_item": "authorizations",
        "address": address,
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

async fn view_received(
    State(state): State<AppState>,
    Path(address_json): Path<String>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let address = address_json
        .strip_suffix(".json")
        .ok_or_else(|| WebError::NotFound(format!("Unknown route: {address_json}")))?
        .to_string();
    let q = parse_query(&raw, "granter")?;
    let client = get_db(&state).await?;
    let rows = get_received_authorizations(&**client, &address).await?;
    let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();
    for (owner, auths) in rows {
        grouped.insert(owner, auths);
    }
    if let Some(granter) = &q.party {
        grouped.retain(|k, _| k == granter);
    }
    let filter = AuthFilter {
        channels: q.channels.as_deref(),
        types: q.types.as_deref(),
        post_types: q.post_types.as_deref(),
        chains: q.chains.as_deref(),
        aggregate_keys: q.aggregate_keys.as_deref(),
    };
    let filtered = filter_authorizations(&grouped, &filter);
    let (page_map, total) = paginate_authorizations(&filtered, q.page, q.pagination);
    let auths: Map<String, Value> = page_map
        .into_iter()
        .map(|(k, v)| (k, Value::Array(v)))
        .collect();
    let body = json!({
        "authorizations": Value::Object(auths),
        "pagination_page": q.page,
        "pagination_per_page": q.pagination,
        "pagination_total": total,
        "pagination_item": "authorizations",
        "address": address,
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}
