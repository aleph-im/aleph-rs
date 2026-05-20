//! Mirrors `aleph/web/controllers/messages.py`.
//!
//! Handles `/api/v0/messages*` and `/api/v0/messages/page/{page}.json`.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{CloseCode, CloseFrame, Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value, json};
use tokio_postgres::types::ToSql;

use crate::db::accessors::messages::{
    MessageFilters, MessageHashesFilters, count_matching_hashes, count_matching_messages,
    get_forgotten_message, get_matching_hashes, get_matching_messages, get_message_by_item_hash,
    get_message_status, get_rejected_message,
};
use crate::db::accessors::pending_messages::get_pending_messages;
use crate::schemas::messages_query_params::{
    MessageHashesQueryParams, MessageQueryParams, WsMessageQueryParams,
};
use crate::toolkit::cursor::{decode_message_cursor, encode_message_cursor};
use crate::types::message_status::MessageStatus;
use crate::types::sort_order::SortBy;
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::utils::{
    datetime_to_timestamp, get_db, json_text_response, validate_cursor_pagination,
};

/// WebSocket close code returned when the per-process WS connection cap is
/// hit. Matches RFC 6455 `1013 Try Again Later`.
const WS_TRY_AGAIN_LATER: CloseCode = 1013;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/v0/messages.json", get(view_messages_list))
        .route(
            // axum 0.8 forbids literal suffixes on path params; strip `.json` below.
            "/api/v0/messages/page/{page_json}",
            get(view_messages_list_paged),
        )
        .route("/api/v0/messages/hashes", get(view_message_hashes))
        .route("/api/v0/messages/{item_hash}", get(view_message))
        .route(
            "/api/v0/messages/{item_hash}/content",
            get(view_message_content),
        )
        .route(
            "/api/v0/messages/{item_hash}/status",
            get(view_message_status),
        )
        // WebSocket subscription stream. Pre-loads history then streams new
        // messages from the in-process broadcast channel. Mirrors `messages_ws`.
        .route("/api/ws0/messages", get(messages_ws))
}

// ---------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------

fn raw_params_from_map(map: HashMap<String, String>) -> serde_json::Value {
    // Pyaleph relies on pydantic's permissive coercion: query strings like
    // `pagination=10` parse into typed integer fields. axum surfaces them as
    // plain strings, so we promote numeric literals to JSON numbers before
    // handing the map to serde_json. Strings that parse as integers become
    // `Number(i64)`, strings that parse as floats become `Number(f64)`,
    // everything else stays a `String`.
    let mut out = Map::new();
    for (k, v) in map {
        let is_bool_field = matches!(k.as_str(), "excludeContent" | "exclude_content");
        if is_bool_field && v.eq_ignore_ascii_case("true") {
            out.insert(k, serde_json::Value::Bool(true));
        } else if is_bool_field && v.eq_ignore_ascii_case("false") {
            out.insert(k, serde_json::Value::Bool(false));
        } else if let Ok(n) = v.parse::<i64>() {
            out.insert(k, serde_json::Value::Number(n.into()));
        } else if let Ok(n) = v.parse::<f64>()
            && let Some(num) = serde_json::Number::from_f64(n)
        {
            out.insert(k, serde_json::Value::Number(num));
        } else {
            out.insert(k, serde_json::Value::String(v));
        }
    }
    serde_json::Value::Object(out)
}

fn parse_message_query(raw: HashMap<String, String>) -> WebResult<MessageQueryParams> {
    let value = raw_params_from_map(raw);
    let params: MessageQueryParams =
        serde_json::from_value(value).map_err(|e| WebError::Unprocessable(e.to_string()))?;
    params.validate().map_err(WebError::Unprocessable)?;
    Ok(params)
}

fn filters_from_query(q: &MessageQueryParams) -> MessageFilters {
    let mut f = MessageFilters::new();
    f.hashes = q
        .base
        .hashes
        .as_ref()
        .map(|v| v.iter().map(|h| h.to_string()).collect());
    f.addresses = q.base.addresses.clone();
    f.owners = q.base.owners.clone();
    f.refs = q.base.refs.clone();
    f.chains = q.base.chains.as_ref().map(|v| {
        v.iter()
            .map(|c| {
                serde_json::to_value(c)
                    .ok()
                    .and_then(|x| x.as_str().map(|s| s.to_string()))
                    .unwrap_or_default()
            })
            .collect()
    });
    f.message_type = q.base.message_type;
    f.message_types = q.base.message_types.clone();
    f.message_statuses = q.base.message_statuses.clone();
    if q.base.start_date > 0.0 {
        f.start_date = Some(q.base.start_date);
    }
    if q.base.end_date > 0.0 {
        f.end_date = Some(q.base.end_date);
    }
    if q.base.start_block > 0 {
        f.start_block = Some(q.base.start_block);
    }
    if q.base.end_block > 0 {
        f.end_block = Some(q.base.end_block);
    }
    f.content_hashes = q
        .base
        .content_hashes
        .as_ref()
        .map(|v| v.iter().map(|h| h.to_string()).collect());
    f.content_keys = q
        .base
        .content_keys
        .as_ref()
        .map(|v| v.iter().map(|h| h.to_string()).collect());
    f.content_types = q.base.content_types.clone();
    f.tags = q.base.tags.clone();
    f.channels = q.base.channels.clone();
    f.payment_types = q.base.payment_types.as_ref().map(|v| {
        v.iter()
            .map(|p| {
                serde_json::to_value(p)
                    .ok()
                    .and_then(|x| x.as_str().map(|s| s.to_string()))
                    .unwrap_or_default()
            })
            .collect()
    });
    f.sort_by = q.base.sort_by;
    f.sort_order = q.base.sort_order;
    f.page = q.page;
    f.pagination = q.pagination;
    f.include_confirmations = true;
    f
}

/// Format a single message row into the Python-API JSON shape.
fn message_to_dict(
    m: &crate::db::models::messages::MessageDb,
    confirmations: &[(String, String, i64)],
    exclude_content: bool,
) -> Value {
    m.to_api_value(confirmations, exclude_content)
}

/// Fetch on-chain confirmations for a list of item hashes in one query.
async fn fetch_confirmations(
    client: &impl tokio_postgres::GenericClient,
    item_hashes: &[String],
) -> WebResult<HashMap<String, Vec<(String, String, i64)>>> {
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
    let mut out: HashMap<String, Vec<(String, String, i64)>> = HashMap::new();
    for row in rows {
        let ih: String = row.get("item_hash");
        let chain: String = row.get("chain");
        let hash: String = row.get("hash");
        let height: i64 = row.get("height");
        out.entry(ih).or_default().push((chain, hash, height));
    }
    Ok(out)
}

async fn view_messages_list(
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    do_messages_list(state, raw, None).await
}

async fn view_messages_list_paged(
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
    do_messages_list(state, raw, Some(page_n)).await
}

async fn do_messages_list(
    state: AppState,
    raw: HashMap<String, String>,
    url_page: Option<i64>,
) -> WebResult<Response> {
    let mut q = parse_message_query(raw.clone())?;
    if let Some(p) = url_page {
        q.page = p;
    }
    let exclude_content = q.base.exclude_content;
    let pagination_per_page = q.pagination;
    let cursor_str = q.cursor.clone();

    let client = get_db(&state).await?;

    if let Some(cursor_val) = cursor_str.as_deref() {
        let pagination = validate_cursor_pagination(Some(cursor_val), pagination_per_page)?;
        let (after_time, after_hash) = if cursor_val.is_empty() {
            (None, None)
        } else {
            let (t, h) = decode_message_cursor(cursor_val)
                .map_err(|e| WebError::Unprocessable(e.to_string()))?;
            (Some(t), Some(h))
        };
        let mut f = filters_from_query(&q);
        f.after_time = after_time;
        f.after_hash = after_hash;
        f.cursor_mode = true;
        f.pagination = pagination;
        let messages = get_matching_messages(&**client, &f).await?;
        let has_more = (messages.len() as i64) > pagination;
        let mut messages = messages;
        if has_more {
            messages.truncate(pagination as usize);
        }
        let item_hashes: Vec<String> = messages.iter().map(|m| m.item_hash.clone()).collect();
        let confs = fetch_confirmations(&**client, &item_hashes).await?;
        let formatted: Vec<Value> = messages
            .iter()
            .map(|m| {
                let empty = Vec::new();
                let cs = confs.get(&m.item_hash).unwrap_or(&empty);
                message_to_dict(m, cs, exclude_content)
            })
            .collect();
        let next_cursor: Option<String> = if has_more && !messages.is_empty() {
            let last = messages.last().unwrap();
            Some(encode_message_cursor(last.time, &last.item_hash))
        } else {
            None
        };
        let body = json!({
            "messages": formatted,
            "pagination_per_page": pagination,
            "next_cursor": next_cursor,
        });
        return Ok(json_text_response(StatusCode::OK, body.to_string()));
    }

    // Legacy page mode
    let pagination_page = q.page;
    let f = filters_from_query(&q);
    let messages = get_matching_messages(&**client, &f).await?;
    let item_hashes: Vec<String> = messages.iter().map(|m| m.item_hash.clone()).collect();
    let confs = fetch_confirmations(&**client, &item_hashes).await?;

    let total = if pagination_per_page > 0 && (messages.len() as i64) < pagination_per_page {
        (pagination_page - 1) * pagination_per_page + messages.len() as i64
    } else {
        count_matching_messages(&**client, &f).await?
    };

    let formatted: Vec<Value> = messages
        .iter()
        .map(|m| {
            let empty = Vec::new();
            let cs = confs.get(&m.item_hash).unwrap_or(&empty);
            message_to_dict(m, cs, exclude_content)
        })
        .collect();
    let body = json!({
        "messages": formatted,
        "pagination_per_page": pagination_per_page,
        "pagination_page": pagination_page,
        "pagination_total": total,
        "pagination_item": "messages",
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

// ---------------------------------------------------------------------------
// Single message + status
// ---------------------------------------------------------------------------

async fn view_message(
    State(state): State<AppState>,
    Path(item_hash): Path<String>,
) -> WebResult<Response> {
    if item_hash.is_empty() {
        return Err(WebError::BadRequest(format!(
            "Invalid message hash: {item_hash}"
        )));
    }
    let client = get_db(&state).await?;
    let status_db = get_message_status(&**client, &item_hash)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("Message {item_hash} not found")))?;
    let body = build_message_with_status(&**client, &status_db).await?;
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

async fn view_message_content(
    State(state): State<AppState>,
    Path(item_hash): Path<String>,
) -> WebResult<Response> {
    if item_hash.is_empty() {
        return Err(WebError::BadRequest(format!(
            "Invalid message hash: {item_hash}"
        )));
    }
    let client = get_db(&state).await?;
    let status_db = get_message_status(&**client, &item_hash)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("Message {item_hash} not found")))?;
    if status_db.status != MessageStatus::Processed {
        return Err(WebError::Unprocessable(format!(
            "Message {item_hash} is not processed (status: {:?})",
            status_db.status
        )));
    }
    let msg = get_message_by_item_hash(&**client, &item_hash)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("Message {item_hash} not found")))?;
    let mut content = msg.content.clone();
    if msg.r#type == aleph_types::message::MessageType::Post
        && let Some(inner) = content.get("content").cloned()
    {
        content = inner;
    }
    Ok(json_text_response(StatusCode::OK, content.to_string()))
}

async fn view_message_status(
    State(state): State<AppState>,
    Path(item_hash): Path<String>,
) -> WebResult<Response> {
    if item_hash.is_empty() {
        return Err(WebError::BadRequest(format!(
            "Invalid message hash: {item_hash}"
        )));
    }
    let client = get_db(&state).await?;
    let s = get_message_status(&**client, &item_hash)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("Message {item_hash} not found")))?;
    let body = json!({
        "status": serde_json::to_value(s.status).unwrap(),
        "item_hash": s.item_hash,
        "reception_time": s.reception_time.to_rfc3339(),
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

/// Build the MessageWithStatus JSON object for the requested item hash.
async fn build_message_with_status(
    client: &impl tokio_postgres::GenericClient,
    status_db: &crate::db::models::messages::MessageStatusDb,
) -> WebResult<Value> {
    let item_hash = &status_db.item_hash;
    let reception_time = status_db.reception_time.to_rfc3339();
    match status_db.status {
        MessageStatus::Pending => {
            let pending = get_pending_messages(client, item_hash).await?;
            let messages: Vec<Value> = pending.iter().map(pending_message_to_value).collect();
            Ok(json!({
                "status": "pending",
                "item_hash": item_hash,
                "reception_time": reception_time,
                "messages": messages,
            }))
        }
        MessageStatus::Processed | MessageStatus::Removing | MessageStatus::Removed => {
            let msg = get_message_by_item_hash(client, item_hash)
                .await?
                .ok_or_else(|| WebError::NotFound("Message body missing".into()))?;
            let confs = fetch_confirmations(client, &[item_hash.clone()]).await?;
            let empty = Vec::new();
            let cs = confs.get(item_hash).unwrap_or(&empty);
            let message_dict = message_to_dict(&msg, cs, false);
            let status_s = serde_json::to_value(status_db.status).unwrap();
            let mut out = json!({
                "status": status_s,
                "item_hash": item_hash,
                "reception_time": reception_time,
                "message": message_dict,
            });
            if status_db.status == MessageStatus::Removing
                || status_db.status == MessageStatus::Removed
            {
                out["reason"] = json!("balance_insufficient");
            }
            Ok(out)
        }
        MessageStatus::Forgotten => {
            let fm = get_forgotten_message(client, item_hash)
                .await?
                .ok_or_else(|| WebError::NotFound("Forgotten message not found".into()))?;
            Ok(json!({
                "status": "forgotten",
                "item_hash": item_hash,
                "reception_time": reception_time,
                "message": forgotten_message_to_value(&fm),
                "forgotten_by": fm.forgotten_by,
            }))
        }
        MessageStatus::Rejected => {
            let rj = get_rejected_message(client, item_hash)
                .await?
                .ok_or_else(|| WebError::NotFound("Rejected message not found".into()))?;
            let mut out = json!({
                "status": "rejected",
                "item_hash": item_hash,
                "reception_time": reception_time,
                "message": rj.message,
                "error_code": rj.error_code.as_i32(),
            });
            if let Some(d) = rj.details {
                out["details"] = d;
            }
            Ok(out)
        }
    }
}

fn pending_message_to_value(m: &crate::db::models::pending_messages::PendingMessageDb) -> Value {
    json!({
        "sender": m.sender,
        "chain": serde_json::to_value(&m.chain).unwrap(),
        "signature": m.signature,
        "type": serde_json::to_value(m.r#type).unwrap(),
        "item_content": m.item_content,
        "item_type": serde_json::to_value(m.item_type).unwrap(),
        "item_hash": m.item_hash,
        "time": datetime_to_timestamp(m.time),
        "channel": serde_json::to_value(&m.channel).unwrap(),
        "reception_time": m.reception_time.to_rfc3339(),
    })
}

fn forgotten_message_to_value(m: &crate::db::models::messages::ForgottenMessageDb) -> Value {
    json!({
        "sender": m.sender,
        "chain": serde_json::to_value(&m.chain).unwrap(),
        "signature": m.signature,
        "type": serde_json::to_value(m.r#type).unwrap(),
        "item_type": serde_json::to_value(m.item_type).unwrap(),
        "item_hash": m.item_hash,
        "time": datetime_to_timestamp(m.time),
        "channel": serde_json::to_value(&m.channel).unwrap(),
    })
}

// ---------------------------------------------------------------------------
// Hashes
// ---------------------------------------------------------------------------

async fn view_message_hashes(
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> WebResult<Response> {
    let raw_value = raw_params_from_map(raw);
    let q: MessageHashesQueryParams =
        serde_json::from_value(raw_value).map_err(|e| WebError::Unprocessable(e.to_string()))?;
    q.validate().map_err(WebError::Unprocessable)?;
    let client = get_db(&state).await?;
    let f = MessageHashesFilters {
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
        status: q.status,
        sort_order: q.sort_order,
        page: q.page,
        pagination: q.pagination,
        hash_only: q.hash_only,
    };
    let hashes = get_matching_hashes(&**client, &f).await?;
    let total = count_matching_hashes(&**client, &f).await?;
    let formatted: Vec<Value> = if f.hash_only {
        hashes.into_iter().map(|h| json!(h.item_hash)).collect()
    } else {
        hashes
            .into_iter()
            .map(|h| {
                json!({
                    "item_hash": h.item_hash,
                    "status": h.status.map(|s| serde_json::to_value(s).unwrap()),
                    "reception_time": h.reception_time.map(|t| t.to_rfc3339()),
                })
            })
            .collect()
    };
    let body = json!({
        "hashes": formatted,
        "pagination_per_page": q.pagination,
        "pagination_page": q.page,
        "pagination_total": total,
        "pagination_item": "hashes",
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

// ---------------------------------------------------------------------------
// WebSocket: `/api/ws0/messages`
// ---------------------------------------------------------------------------

fn parse_ws_query(raw: HashMap<String, String>) -> WebResult<WsMessageQueryParams> {
    // `WsMessageQueryParams` has typed integer/float fields (history,
    // pagination, page, start_date, ...). axum's `Query<HashMap<String,
    // String>>` yields raw strings, so we promote numeric values into JSON
    // `Number`s before deserialising — mirroring pyaleph's pydantic coercion.
    let mut map = Map::new();
    for (k, v) in raw {
        if let Ok(n) = v.parse::<i64>() {
            map.insert(k, Value::Number(n.into()));
        } else if let Ok(n) = v.parse::<f64>() {
            if let Some(num) = serde_json::Number::from_f64(n) {
                map.insert(k, Value::Number(num));
            } else {
                map.insert(k, Value::String(v));
            }
        } else {
            map.insert(k, Value::String(v));
        }
    }
    let value = Value::Object(map);
    let params: WsMessageQueryParams =
        serde_json::from_value(value).map_err(|e| WebError::Unprocessable(e.to_string()))?;
    params.validate().map_err(WebError::Unprocessable)?;
    Ok(params)
}

/// Build [`MessageFilters`] from a [`WsMessageQueryParams`]. Mirrors the
/// `find_filters = query_params.model_dump(exclude_none=True)` shape used by
/// `_send_history_to_ws`.
fn filters_from_ws_query(q: &WsMessageQueryParams) -> MessageFilters {
    let mut f = MessageFilters::new();
    f.hashes = q
        .base
        .hashes
        .as_ref()
        .map(|v| v.iter().map(|h| h.to_string()).collect());
    f.addresses = q.base.addresses.clone();
    f.owners = q.base.owners.clone();
    f.refs = q.base.refs.clone();
    f.chains = q.base.chains.as_ref().map(|v| {
        v.iter()
            .map(|c| {
                serde_json::to_value(c)
                    .ok()
                    .and_then(|x| x.as_str().map(|s| s.to_string()))
                    .unwrap_or_default()
            })
            .collect()
    });
    f.message_type = q.base.message_type;
    f.message_types = q.base.message_types.clone();
    f.content_hashes = q
        .base
        .content_hashes
        .as_ref()
        .map(|v| v.iter().map(|h| h.to_string()).collect());
    f.content_keys = q
        .base
        .content_keys
        .as_ref()
        .map(|v| v.iter().map(|h| h.to_string()).collect());
    f.content_types = q.base.content_types.clone();
    f.tags = q.base.tags.clone();
    f.channels = q.base.channels.clone();
    f.include_confirmations = true;
    f
}

/// Apply the same filter predicates as `_send_to_client`'s
/// `message_matches_filters` to a streaming JSON payload. Returns `true` if
/// the payload should be forwarded to the client.
fn json_matches_filters(payload: &Value, q: &WsMessageQueryParams) -> bool {
    let base = &q.base;
    let get_str = |field: &str| {
        payload
            .get(field)
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };

    if let Some(t) = base.message_type {
        let mt = get_str("type");
        let want = serde_json::to_value(t)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string));
        if mt != want {
            return false;
        }
    }

    if let Some(hashes) = &base.hashes {
        let h = get_str("item_hash");
        if !hashes.iter().any(|x| Some(x.to_string()) == h) {
            return false;
        }
    }
    if let Some(addrs) = &base.addresses {
        let sender = get_str("sender");
        if !addrs.iter().any(|a| Some(a.clone()) == sender) {
            return false;
        }
    }
    if let Some(owners) = &base.owners {
        let content_addr = payload
            .pointer("/content/address")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        if !owners.iter().any(|a| Some(a.clone()) == content_addr) {
            return false;
        }
    }
    if let Some(refs) = &base.refs {
        let r = payload
            .pointer("/content/ref")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        if !refs.iter().any(|a| Some(a.clone()) == r) {
            return false;
        }
    }
    if let Some(chains) = &base.chains {
        let c = get_str("chain");
        let want: Vec<String> = chains
            .iter()
            .filter_map(|x| {
                serde_json::to_value(x)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
            })
            .collect();
        if !want.iter().any(|x| Some(x.clone()) == c) {
            return false;
        }
    }
    if let Some(channels) = &base.channels {
        let c = get_str("channel");
        if !channels.iter().any(|a| Some(a.clone()) == c) {
            return false;
        }
    }
    if let Some(content_hashes) = &base.content_hashes {
        let c = payload
            .pointer("/content/item_hash")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        if !content_hashes.iter().any(|h| Some(h.to_string()) == c) {
            return false;
        }
    }
    if let Some(content_types) = &base.content_types {
        let c = payload
            .pointer("/content/type")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        if !content_types.iter().any(|a| Some(a.clone()) == c) {
            return false;
        }
    }
    if let Some(tags) = &base.tags {
        let msg_tags: Vec<String> = payload
            .pointer("/content/content/tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if tags.iter().all(|t| !msg_tags.contains(t)) {
            return false;
        }
    }
    true
}

/// `/api/ws0/messages` entry point. Negotiates the WS upgrade, enforces the
/// global connection cap, pre-loads history, then streams new messages from
/// `state.message_broadcast`.
async fn messages_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_messages_ws(socket, state, raw))
}

async fn handle_messages_ws(mut socket: WebSocket, state: AppState, raw: HashMap<String, String>) {
    let cap = state.config.websocket.max_message_connections;
    // Reserve a slot first so we can reject cleanly without acquiring DB.
    let active = &state.ws_messages_active;
    let prev = active.fetch_add(1, Ordering::SeqCst);
    if prev >= cap {
        active.fetch_sub(1, Ordering::SeqCst);
        let _ = socket
            .send(WsMessage::Close(Some(CloseFrame {
                code: WS_TRY_AGAIN_LATER,
                reason: "Too many connections".into(),
            })))
            .await;
        return;
    }
    // Guard releases the slot on every exit path.
    struct ActiveGuard<'a>(&'a std::sync::atomic::AtomicU32);
    impl Drop for ActiveGuard<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let _guard = ActiveGuard(active);

    let query = match parse_ws_query(raw) {
        Ok(q) => q,
        Err(e) => {
            let _ = socket
                .send(WsMessage::Close(Some(CloseFrame {
                    code: 1002, // protocol error: invalid query params
                    reason: format!("{e:?}").into(),
                })))
                .await;
            return;
        }
    };
    let exclude_content = query.base.exclude_content;

    // Subscribe to the broadcast BEFORE history send so we don't miss any
    // messages that arrive during the catch-up read.
    let mut rx = state.message_broadcast.subscribe();

    if let Some(history) = query.history {
        if history > 0 {
            if let Err(e) =
                send_history(&mut socket, &state, &query, history, exclude_content).await
            {
                tracing::info!(?e, "messages_ws: failed to send history");
                return;
            }
        }
    }

    // Heartbeat: ping every `heartbeat` seconds. aiohttp's server-side
    // heartbeat sends frames itself; axum surfaces the same control via
    // explicit Ping messages.
    let hb = Duration::from_secs(state.config.websocket.heartbeat.max(1));
    let mut hb_interval = tokio::time::interval(hb);
    hb_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Tick once immediately and discard so the first beat isn't fired at t=0.
    hb_interval.tick().await;

    loop {
        tokio::select! {
            recv = rx.recv() => {
                match recv {
                    Ok(payload) => {
                        if !json_matches_filters(&payload, &query) {
                            continue;
                        }
                        let mut to_send = payload;
                        if exclude_content && let Some(obj) = to_send.as_object_mut() {
                            obj.remove("content");
                        }
                        let s = to_send.to_string();
                        if socket.send(WsMessage::Text(s.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        // Slow client: skip the gap, keep streaming. pyaleph
                        // disconnects on overflow; we deliberately keep the
                        // connection so cheap clients don't get torn down.
                        tracing::debug!(skipped = n, "messages_ws: client lagging");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = hb_interval.tick() => {
                if socket.send(WsMessage::Ping(Vec::new().into())).await.is_err() {
                    break;
                }
            }
            msg = socket.recv() => match msg {
                Some(Ok(WsMessage::Close(_))) | None => break,
                Some(Err(_)) => break,
                _ => {}
            }
        }
    }
}

async fn send_history(
    socket: &mut WebSocket,
    state: &AppState,
    query: &WsMessageQueryParams,
    history: i64,
    exclude_content: bool,
) -> Result<(), String> {
    let client = get_db(state).await.map_err(|e| format!("{e:?}"))?;
    let mut f = filters_from_ws_query(query);
    f.pagination = history;
    let messages = get_matching_messages(&**client, &f)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let item_hashes: Vec<String> = messages.iter().map(|m| m.item_hash.clone()).collect();
    let confs = fetch_confirmations(&**client, &item_hashes)
        .await
        .map_err(|e| format!("{e:?}"))?;
    // Match pyaleph: history is rendered oldest-first (server reverses the
    // query result before sending).
    for m in messages.iter().rev() {
        let empty = Vec::new();
        let cs = confs.get(&m.item_hash).unwrap_or(&empty);
        let payload = message_to_dict(m, cs, exclude_content);
        let s = payload.to_string();
        socket
            .send(WsMessage::Text(s.into()))
            .await
            .map_err(|e| format!("{e}"))?;
    }
    Ok(())
}

// Keep type imports referenced for `WebError` mapping etc.
#[allow(dead_code)]
fn _types() {
    let _ = (
        std::marker::PhantomData::<DateTime<Utc>>,
        std::marker::PhantomData::<SortBy>,
    );
    fn _phantom_sql<T: ToSql>(_: &T) {}
}

// Note: Router-level tests for these handlers are blocked on the
// `Box<dyn ToSql + Sync>` -> `Box<dyn ToSql + Sync + Send>` foundation
// fix in `db/accessors`. Once that lands, this module's `mod tests` can
// be reinstated. The handler bodies above contain the iso-functional
// translation of the Python controller's request flow.

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ws_q(extra: Value) -> WsMessageQueryParams {
        let mut base = serde_json::Map::new();
        if let Some(obj) = extra.as_object() {
            for (k, v) in obj {
                base.insert(k.clone(), v.clone());
            }
        }
        serde_json::from_value(Value::Object(base)).unwrap()
    }

    #[test]
    fn ws_filter_channel_matches() {
        let q = ws_q(json!({"channels": "match-me", "history": 0}));
        let payload = json!({"item_hash": "h", "channel": "match-me"});
        assert!(json_matches_filters(&payload, &q));
    }

    #[test]
    fn ws_filter_channel_rejects_mismatch() {
        let q = ws_q(json!({"channels": "match-me", "history": 0}));
        let payload = json!({"item_hash": "h", "channel": "other"});
        assert!(!json_matches_filters(&payload, &q));
    }

    #[test]
    fn ws_filter_addresses_matches_sender() {
        let q = ws_q(json!({"addresses": "0xabc", "history": 0}));
        let payload = json!({"sender": "0xabc"});
        assert!(json_matches_filters(&payload, &q));
    }

    #[test]
    fn ws_filter_owners_uses_content_address() {
        let q = ws_q(json!({"owners": "0xowner", "history": 0}));
        let payload = json!({"sender": "x", "content": {"address": "0xowner"}});
        assert!(json_matches_filters(&payload, &q));
    }

    #[test]
    fn ws_filter_tags_any_match() {
        let q = ws_q(json!({"tags": "alpha,beta", "history": 0}));
        let payload = json!({"content": {"content": {"tags": ["gamma", "beta"]}}});
        assert!(json_matches_filters(&payload, &q));
    }

    #[test]
    fn ws_filter_tags_no_overlap_rejected() {
        let q = ws_q(json!({"tags": "alpha,beta", "history": 0}));
        let payload = json!({"content": {"content": {"tags": ["gamma"]}}});
        assert!(!json_matches_filters(&payload, &q));
    }
}
