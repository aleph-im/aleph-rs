//! Mirrors `aleph/web/controllers/posts.py`.

use std::collections::HashMap;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use chrono::DateTime;
use serde_json::{Map, Value, json};

use crate::db::accessors::posts::{
    MergedPost, MergedPostV0, PostFilters, count_matching_posts, get_matching_posts,
    get_matching_posts_legacy,
};
use crate::schemas::messages_query_params::{
    DEFAULT_MESSAGES_PER_PAGE, DEFAULT_PAGE, LIST_FIELD_SEPARATOR,
};
use crate::toolkit::cursor::{decode_message_cursor, encode_message_cursor};
use crate::types::sort_order::{SortBy, SortOrder};
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::utils::{get_db, json_text_response, validate_cursor_pagination};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/v0/posts.json", get(view_posts_list_v0))
        // axum 0.8: strip `.json` in the handler.
        .route(
            "/api/v0/posts/page/{page_json}",
            get(view_posts_list_v0_paged),
        )
        .route("/api/v1/posts.json", get(view_posts_list_v1))
        .route(
            "/api/v1/posts/page/{page_json}",
            get(view_posts_list_v1_paged),
        )
}

fn split_csv(s: Option<&String>) -> Option<Vec<String>> {
    s.map(|v| {
        v.split(LIST_FIELD_SEPARATOR)
            .map(|x| x.to_string())
            .collect()
    })
}

#[derive(Debug)]
struct PostQuery {
    addresses: Option<Vec<String>>,
    hashes: Option<Vec<String>>,
    refs: Option<Vec<String>>,
    post_types: Option<Vec<String>>,
    tags: Option<Vec<String>>,
    channels: Option<Vec<String>>,
    start_date: f64,
    end_date: f64,
    pagination: i64,
    page: i64,
    sort_by: SortBy,
    sort_order: SortOrder,
    cursor: Option<String>,
}

fn parse_post_query(raw: &HashMap<String, String>) -> WebResult<PostQuery> {
    let addresses = split_csv(raw.get("addresses"));
    let hashes = split_csv(raw.get("hashes"));
    let refs = split_csv(raw.get("refs"));
    let post_types = split_csv(raw.get("types"));
    let tags = split_csv(raw.get("tags"));
    let channels = split_csv(raw.get("channels"));
    let start_date = raw
        .get("startDate")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let end_date = raw
        .get("endDate")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    if start_date < 0.0 {
        return Err(WebError::Unprocessable("startDate must be >= 0".into()));
    }
    if end_date < 0.0 {
        return Err(WebError::Unprocessable("endDate must be >= 0".into()));
    }
    if start_date > 0.0 && end_date > 0.0 && end_date < start_date {
        return Err(WebError::Unprocessable(
            "end date cannot be lower than start date.".into(),
        ));
    }
    let pagination = raw
        .get("pagination")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(DEFAULT_MESSAGES_PER_PAGE);
    if pagination < 0 {
        return Err(WebError::Unprocessable("pagination must be >= 0".into()));
    }
    let page = raw
        .get("page")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(DEFAULT_PAGE);
    if page < 1 {
        return Err(WebError::Unprocessable("page must be >= 1".into()));
    }
    let sort_by = match raw.get("sortBy").map(|s| s.as_str()) {
        Some("tx-time") => SortBy::TxTime,
        _ => SortBy::Time,
    };
    let sort_order = match raw.get("sortOrder").and_then(|s| s.parse::<i32>().ok()) {
        Some(1) => SortOrder::Ascending,
        _ => SortOrder::Descending,
    };
    let cursor = raw.get("cursor").cloned();
    if cursor.is_some() && sort_by == SortBy::TxTime {
        return Err(WebError::Unprocessable(
            "Cursor pagination is not supported with tx-time sort order.".into(),
        ));
    }
    Ok(PostQuery {
        addresses,
        hashes,
        refs,
        post_types,
        tags,
        channels,
        start_date,
        end_date,
        pagination,
        page,
        sort_by,
        sort_order,
        cursor,
    })
}

fn to_filters(q: &PostQuery) -> PostFilters {
    PostFilters {
        hashes: q.hashes.clone(),
        addresses: q.addresses.clone(),
        refs: q.refs.clone(),
        post_types: q.post_types.clone(),
        tags: q.tags.clone(),
        channels: q.channels.clone(),
        start_date: if q.start_date > 0.0 {
            Some(q.start_date)
        } else {
            None
        },
        end_date: if q.end_date > 0.0 {
            Some(q.end_date)
        } else {
            None
        },
        sort_by: Some(q.sort_by),
        sort_order: Some(q.sort_order),
        page: q.page,
        pagination: q.pagination,
        after_time: None,
        after_hash: None,
        cursor_mode: false,
    }
}

fn merged_post_to_dict(p: &MergedPost) -> Value {
    json!({
        "item_hash": p.item_hash,
        "content": p.content,
        "original_item_hash": p.original_item_hash,
        "original_type": p.original_type,
        "address": p.owner,
        "ref": p.r#ref,
        "channel": serde_json::to_value(&p.channel).unwrap(),
        "created": p.created.to_rfc3339(),
        "last_updated": p.last_updated.to_rfc3339(),
    })
}

fn merged_post_v0_to_dict(p: &MergedPostV0, confirmations: Vec<Value>) -> Value {
    let mut out = Map::new();
    out.insert("chain".into(), serde_json::to_value(&p.chain).unwrap());
    out.insert("item_hash".into(), json!(p.item_hash));
    out.insert("sender".into(), json!(p.owner));
    out.insert("type".into(), serde_json::to_value(&p.r#type).unwrap());
    out.insert("channel".into(), serde_json::to_value(&p.channel).unwrap());
    out.insert("confirmed".into(), json!(!confirmations.is_empty()));
    out.insert("content".into(), p.content.clone());
    out.insert("item_content".into(), json!(p.item_content));
    out.insert(
        "item_type".into(),
        serde_json::to_value(&p.item_type).unwrap(),
    );
    out.insert("signature".into(), json!(p.signature));
    out.insert("size".into(), json!(p.size));
    out.insert("time".into(), json!(p.time));
    out.insert("confirmations".into(), Value::Array(confirmations));
    out.insert("original_item_hash".into(), json!(p.original_item_hash));
    out.insert("original_signature".into(), json!(p.original_signature));
    out.insert("original_type".into(), json!(p.original_type));
    out.insert("hash".into(), json!(p.original_item_hash));
    out.insert("address".into(), json!(p.owner));
    out.insert("ref".into(), json!(p.r#ref));
    Value::Object(out)
}

async fn fetch_post_confirmations(
    client: &impl tokio_postgres::GenericClient,
    item_hashes: &[String],
) -> WebResult<HashMap<String, Vec<Value>>> {
    if item_hashes.is_empty() {
        return Ok(HashMap::new());
    }
    let sql = "SELECT mc.item_hash, ct.chain, ct.hash, ct.height \
               FROM message_confirmations mc \
               JOIN chain_txs ct ON mc.tx_hash = ct.hash \
               WHERE mc.item_hash = ANY($1)";
    let rows = client
        .query(sql, &[&item_hashes.to_vec()])
        .await
        .map_err(|e| WebError::Internal(format!("db: {e}")))?;
    let mut out: HashMap<String, Vec<Value>> = HashMap::new();
    for row in rows {
        let ih: String = row.get("item_hash");
        let chain: String = row.get("chain");
        let hash: String = row.get("hash");
        let height: i64 = row.get("height");
        out.entry(ih).or_default().push(json!({
            "chain": chain,
            "hash": hash,
            "height": height,
        }));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// V0
// ---------------------------------------------------------------------------

async fn view_posts_list_v0(
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    do_list_v0(state, raw, None).await
}

async fn view_posts_list_v0_paged(
    State(state): State<AppState>,
    Path(page_json): Path<String>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let page = page_json
        .strip_suffix(".json")
        .ok_or_else(|| WebError::NotFound(format!("Unknown route: {page_json}")))?;
    let page_n: i64 = page
        .parse::<i64>()
        .map_err(|_| WebError::BadRequest(format!("Invalid page value in path: {page}")))?;
    if page_n < 1 {
        return Err(WebError::Unprocessable(
            "Page number must be greater than 1.".into(),
        ));
    }
    do_list_v0(state, raw, Some(page_n)).await
}

async fn do_list_v0(
    state: AppState,
    raw: HashMap<String, String>,
    url_page: Option<i64>,
) -> WebResult<Response> {
    let mut q = parse_post_query(&raw)?;
    if let Some(p) = url_page {
        q.page = p;
    }
    let pagination_per_page = q.pagination;
    let cursor = q.cursor.clone();
    let client = get_db(&state).await?;

    if let Some(cursor_val) = cursor.as_deref() {
        let pagination = validate_cursor_pagination(Some(cursor_val), pagination_per_page)?;
        let (after_time, after_hash) = if cursor_val.is_empty() {
            (None, None)
        } else {
            let (t, h) = decode_message_cursor(cursor_val)
                .map_err(|e| WebError::Unprocessable(e.to_string()))?;
            (Some(t), Some(h))
        };
        let mut filters = to_filters(&q);
        filters.pagination = pagination;
        filters.after_time = after_time;
        filters.after_hash = after_hash;
        filters.cursor_mode = true;
        let mut results = get_matching_posts_legacy(&**client, &filters).await?;
        let has_more = (results.len() as i64) > pagination;
        if has_more {
            results.truncate(pagination as usize);
        }
        let item_hashes: Vec<String> = results.iter().map(|p| p.item_hash.clone()).collect();
        let confs = fetch_post_confirmations(&**client, &item_hashes).await?;
        let posts: Vec<Value> = results
            .iter()
            .map(|p| {
                merged_post_v0_to_dict(p, confs.get(&p.item_hash).cloned().unwrap_or_default())
            })
            .collect();
        let next_cursor: Option<String> = if has_more && !results.is_empty() {
            let last = results.last().unwrap();
            Some(encode_message_cursor(
                last.last_updated,
                &last.original_item_hash,
            ))
        } else {
            None
        };
        let body = json!({
            "posts": posts,
            "pagination_per_page": pagination,
            "next_cursor": next_cursor,
        });
        return Ok(json_text_response(StatusCode::OK, body.to_string()));
    }

    let filters = to_filters(&q);
    let mut count_f = filters.clone();
    count_f.pagination = 0;
    count_f.page = 1;
    let total = count_matching_posts(&**client, Some(&count_f)).await?;
    let results = get_matching_posts_legacy(&**client, &filters).await?;
    let item_hashes: Vec<String> = results.iter().map(|p| p.item_hash.clone()).collect();
    let confs = fetch_post_confirmations(&**client, &item_hashes).await?;
    let posts: Vec<Value> = results
        .iter()
        .map(|p| merged_post_v0_to_dict(p, confs.get(&p.item_hash).cloned().unwrap_or_default()))
        .collect();
    let body = json!({
        "posts": posts,
        "pagination": {
            "page": q.page,
            "per_page": q.pagination,
            "total_count": total,
        },
        "pagination_page": q.page,
        "pagination_total": total,
        "pagination_per_page": q.pagination,
        "pagination_item": "posts",
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

// ---------------------------------------------------------------------------
// V1
// ---------------------------------------------------------------------------

async fn view_posts_list_v1(
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    do_list_v1(state, raw, None).await
}

async fn view_posts_list_v1_paged(
    State(state): State<AppState>,
    Path(page_json): Path<String>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let page = page_json
        .strip_suffix(".json")
        .ok_or_else(|| WebError::NotFound(format!("Unknown route: {page_json}")))?;
    let page_n: i64 = page
        .parse::<i64>()
        .map_err(|_| WebError::BadRequest(format!("Invalid page value in path: {page}")))?;
    if page_n < 1 {
        return Err(WebError::Unprocessable(
            "Page number must be greater than 1.".into(),
        ));
    }
    do_list_v1(state, raw, Some(page_n)).await
}

async fn do_list_v1(
    state: AppState,
    raw: HashMap<String, String>,
    url_page: Option<i64>,
) -> WebResult<Response> {
    let mut q = parse_post_query(&raw)?;
    if let Some(p) = url_page {
        q.page = p;
    }
    let pagination_per_page = q.pagination;
    let cursor = q.cursor.clone();
    let client = get_db(&state).await?;

    if let Some(cursor_val) = cursor.as_deref() {
        let pagination = validate_cursor_pagination(Some(cursor_val), pagination_per_page)?;
        let (after_time, after_hash) = if cursor_val.is_empty() {
            (None, None)
        } else {
            let (t, h) = decode_message_cursor(cursor_val)
                .map_err(|e| WebError::Unprocessable(e.to_string()))?;
            (Some(t), Some(h))
        };
        let mut filters = to_filters(&q);
        filters.pagination = pagination;
        filters.after_time = after_time;
        filters.after_hash = after_hash;
        filters.cursor_mode = true;
        let mut results = get_matching_posts(&**client, &filters).await?;
        let has_more = (results.len() as i64) > pagination;
        if has_more {
            results.truncate(pagination as usize);
        }
        let posts: Vec<Value> = results.iter().map(merged_post_to_dict).collect();
        let next_cursor: Option<String> = if has_more && !results.is_empty() {
            let last = results.last().unwrap();
            Some(encode_message_cursor(
                last.last_updated,
                &last.original_item_hash,
            ))
        } else {
            None
        };
        let body = json!({
            "posts": posts,
            "pagination_per_page": pagination,
            "next_cursor": next_cursor,
        });
        return Ok(json_text_response(StatusCode::OK, body.to_string()));
    }

    let filters = to_filters(&q);
    let mut count_f = filters.clone();
    count_f.pagination = 0;
    count_f.page = 1;
    let total = count_matching_posts(&**client, Some(&count_f)).await?;
    let results = get_matching_posts(&**client, &filters).await?;
    let posts: Vec<Value> = results.iter().map(merged_post_to_dict).collect();
    let body = json!({
        "posts": posts,
        "pagination": {
            "page": q.page,
            "per_page": q.pagination,
            "total_count": total,
        },
        "pagination_page": q.page,
        "pagination_total": total,
        "pagination_per_page": q.pagination,
        "pagination_item": "posts",
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

// Keep DateTime in scope for cursor decoding.
#[allow(dead_code)]
fn _types() {
    let _ = std::marker::PhantomData::<DateTime<chrono::Utc>>;
}
