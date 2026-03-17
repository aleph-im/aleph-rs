use actix_web::{HttpResponse, Responder, web};
use serde::{Deserialize, Serialize};

use crate::api::AppState;
use crate::db::messages::{self, MessageFilter, StoredMessage};
use crate::handlers::{self, IncomingMessage};

// ---------------------------------------------------------------------------
// POST /api/v0/messages
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct PostMessageRequest {
    #[serde(default)]
    pub sync: bool,
    pub message: IncomingMessage,
}

#[derive(Serialize)]
struct PublicationStatus {
    status: &'static str,
    failed: Vec<String>,
}

#[derive(Serialize)]
struct PostMessageResponse {
    publication_status: PublicationStatus,
    message_status: &'static str,
}

pub async fn post_message(
    state: web::Data<AppState>,
    body: web::Json<PostMessageRequest>,
) -> impl Responder {
    let req = body.into_inner();
    let sync = req.sync;
    let msg = req.message;
    let db = state.db.clone();
    let file_store = state.file_store.clone();

    let result = tokio::task::spawn_blocking(move || {
        handlers::process_message_with_store(&db, &msg, Some(&file_store))
    })
    .await
    .unwrap();

    let pub_status = PublicationStatus {
        status: "success",
        failed: vec![],
    };

    match result {
        Ok(()) => {
            let (status_code, message_status) = if sync {
                (200, "processed")
            } else {
                (202, "pending")
            };

            HttpResponse::build(actix_web::http::StatusCode::from_u16(status_code).unwrap()).json(
                PostMessageResponse {
                    publication_status: pub_status,
                    message_status,
                },
            )
        }
        Err(ref e) => {
            // Return 422 for rejection
            HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "publication_status": { "status": "success", "failed": [] },
                "message_status": "rejected",
                "error": {
                    "code": e.error_code(),
                    "message": e.message(),
                }
            }))
        }
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/messages.json  +  GET /api/v0/messages/page/{page}.json
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct MessageQuery {
    pub pagination: Option<u32>,
    pub page: Option<u32>,
    #[serde(rename = "msgType")]
    pub msg_type: Option<String>,
    #[serde(rename = "msgTypes")]
    pub msg_types: Option<String>,
    #[serde(rename = "msgStatuses")]
    pub msg_statuses: Option<String>,
    pub addresses: Option<String>,
    pub owners: Option<String>,
    pub refs: Option<String>,
    #[serde(rename = "contentHashes")]
    pub content_hashes: Option<String>,
    #[serde(rename = "contentKeys")]
    pub content_keys: Option<String>,
    #[serde(rename = "contentTypes")]
    pub content_types: Option<String>,
    pub chains: Option<String>,
    pub channels: Option<String>,
    pub hashes: Option<String>,
    pub tags: Option<String>,
    #[serde(rename = "startDate")]
    pub start_date: Option<f64>,
    #[serde(rename = "endDate")]
    pub end_date: Option<f64>,
    #[serde(rename = "sortBy")]
    pub sort_by: Option<String>,
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i32>,
}

fn split_csv(s: &Option<String>) -> Vec<String> {
    match s {
        Some(v) if !v.is_empty() => v.split(',').map(|s| s.trim().to_string()).collect(),
        _ => vec![],
    }
}

fn query_to_filter(q: &MessageQuery, page_override: Option<u32>) -> MessageFilter {
    let per_page = q.pagination.unwrap_or(20);
    let page = page_override.or(q.page).unwrap_or(1).max(1);

    // Merge msgType and msgTypes
    let mut msg_types = split_csv(&q.msg_types);
    if let Some(ref single) = q.msg_type
        && !single.is_empty()
        && !msg_types.contains(single)
    {
        msg_types.push(single.clone());
    }

    let statuses = if q.msg_statuses.is_some() {
        split_csv(&q.msg_statuses)
    } else {
        vec!["processed".into(), "removing".into()]
    };

    MessageFilter {
        statuses,
        message_types: msg_types,
        addresses: split_csv(&q.addresses),
        owners: split_csv(&q.owners),
        refs: split_csv(&q.refs),
        content_hashes: split_csv(&q.content_hashes),
        content_keys: split_csv(&q.content_keys),
        content_types: split_csv(&q.content_types),
        chains: split_csv(&q.chains),
        channels: split_csv(&q.channels),
        hashes: split_csv(&q.hashes),
        tags: split_csv(&q.tags),
        start_date: q.start_date,
        end_date: q.end_date,
        sort_by: q.sort_by.clone().unwrap_or_else(|| "time".into()),
        sort_order: q.sort_order.unwrap_or(-1),
        page,
        per_page,
    }
}

#[derive(Serialize)]
struct MessageResponse {
    sender: String,
    chain: String,
    signature: String,
    #[serde(rename = "type")]
    message_type: String,
    item_content: Option<String>,
    item_type: String,
    item_hash: String,
    time: f64,
    channel: Option<String>,
    content: serde_json::Value,
    size: i64,
    confirmed: bool,
    confirmations: Vec<()>,
}

fn stored_to_response(msg: &StoredMessage) -> MessageResponse {
    let content: serde_json::Value =
        serde_json::from_str(&msg.content).unwrap_or(serde_json::Value::Null);
    MessageResponse {
        sender: msg.sender.clone(),
        chain: msg.chain.clone(),
        signature: msg.signature.clone(),
        message_type: msg.message_type.clone(),
        item_content: msg.item_content.clone(),
        item_type: msg.item_type.clone(),
        item_hash: msg.item_hash.clone(),
        time: msg.time,
        channel: msg.channel.clone(),
        content,
        size: msg.size,
        confirmed: false,
        confirmations: vec![],
    }
}

pub async fn list_messages(
    state: web::Data<AppState>,
    query: web::Query<MessageQuery>,
) -> impl Responder {
    let filter = query_to_filter(&query, None);
    let per_page = filter.per_page;
    let page = filter.page;
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| messages::query_messages(conn, &filter))
    })
    .await
    .unwrap();

    match result {
        Ok((msgs, total)) => {
            let messages: Vec<MessageResponse> = msgs.iter().map(stored_to_response).collect();
            HttpResponse::Ok().json(serde_json::json!({
                "messages": messages,
                "pagination_page": page,
                "pagination_total": total,
                "pagination_per_page": per_page,
                "pagination_item": "messages"
            }))
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

pub async fn list_messages_page(
    state: web::Data<AppState>,
    path: web::Path<u32>,
    query: web::Query<MessageQuery>,
) -> impl Responder {
    let page = path.into_inner();
    let filter = query_to_filter(&query, Some(page));
    let per_page = filter.per_page;
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| messages::query_messages(conn, &filter))
    })
    .await
    .unwrap();

    match result {
        Ok((msgs, total)) => {
            let messages: Vec<MessageResponse> = msgs.iter().map(stored_to_response).collect();
            HttpResponse::Ok().json(serde_json::json!({
                "messages": messages,
                "pagination_page": page,
                "pagination_total": total,
                "pagination_per_page": per_page,
                "pagination_item": "messages"
            }))
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/messages/{hash}
// ---------------------------------------------------------------------------

pub async fn get_message(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let hash = path.into_inner();
    let db = state.db.clone();
    let hash2 = hash.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            let msg = messages::get_message_by_hash(conn, &hash2)?;
            let forgotten_by = messages::get_forgotten_by(conn, &hash2)?;
            Ok::<_, rusqlite::Error>((msg, forgotten_by))
        })
    })
    .await
    .unwrap();

    match result {
        Ok((Some(msg), forgotten_by)) => {
            let message_resp = stored_to_response(&msg);
            match msg.status.as_str() {
                "processed" | "removing" => HttpResponse::Ok().json(serde_json::json!({
                    "status": msg.status,
                    "item_hash": msg.item_hash,
                    "reception_time": msg.reception_time,
                    "message": message_resp,
                })),
                "pending" => HttpResponse::Ok().json(serde_json::json!({
                    "status": "pending",
                    "item_hash": msg.item_hash,
                    "reception_time": msg.reception_time,
                    "message": message_resp,
                })),
                "forgotten" => {
                    // Strip content from forgotten messages
                    let mut resp = stored_to_response(&msg);
                    resp.content = serde_json::Value::Null;
                    resp.item_content = None;
                    HttpResponse::Ok().json(serde_json::json!({
                        "status": "forgotten",
                        "item_hash": msg.item_hash,
                        "reception_time": msg.reception_time,
                        "message": resp,
                        "forgotten_by": forgotten_by,
                    }))
                }
                "rejected" => HttpResponse::Ok().json(serde_json::json!({
                    "status": "rejected",
                    "item_hash": msg.item_hash,
                    "reception_time": msg.reception_time,
                    "message": message_resp,
                    "error_code": 1,
                })),
                _ => HttpResponse::Ok().json(serde_json::json!({
                    "status": msg.status,
                    "item_hash": msg.item_hash,
                    "reception_time": msg.reception_time,
                    "message": message_resp,
                })),
            }
        }
        Ok((None, _)) => HttpResponse::NotFound().json(serde_json::json!({
            "error": "Message not found"
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/messages/{hash}/status
// ---------------------------------------------------------------------------

pub async fn get_message_status(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let hash = path.into_inner();
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| messages::get_message_by_hash(conn, &hash))
    })
    .await
    .unwrap();

    match result {
        Ok(Some(msg)) => HttpResponse::Ok().json(serde_json::json!({
            "status": msg.status,
            "item_hash": msg.item_hash,
            "reception_time": msg.reception_time,
        })),
        Ok(None) => HttpResponse::NotFound().json(serde_json::json!({
            "error": "Message not found"
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/messages/{hash}/content
// ---------------------------------------------------------------------------

pub async fn get_message_content(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let hash = path.into_inner();
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| messages::get_message_by_hash(conn, &hash))
    })
    .await
    .unwrap();

    match result {
        Ok(Some(msg)) => {
            if msg.status != "processed" {
                return HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Message not processed"
                }));
            }
            if msg.message_type != "POST" {
                return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                    "error": "Content endpoint only available for POST messages"
                }));
            }
            // Return content.content (the user-provided content body)
            let parsed: serde_json::Value =
                serde_json::from_str(&msg.content).unwrap_or(serde_json::Value::Null);
            if let Some(content) = parsed.get("content") {
                HttpResponse::Ok().json(content)
            } else {
                HttpResponse::Ok().json(serde_json::Value::Null)
            }
        }
        Ok(None) => HttpResponse::NotFound().json(serde_json::json!({
            "error": "Message not found"
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/messages/hashes
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct HashesQuery {
    pub status: Option<String>,
    pub pagination: Option<u32>,
    pub page: Option<u32>,
    #[serde(rename = "startDate")]
    pub start_date: Option<f64>,
    #[serde(rename = "endDate")]
    pub end_date: Option<f64>,
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i32>,
}

pub async fn list_hashes(
    state: web::Data<AppState>,
    query: web::Query<HashesQuery>,
) -> impl Responder {
    let per_page = query.pagination.unwrap_or(20);
    let page = query.page.unwrap_or(1).max(1);
    let statuses = if let Some(ref s) = query.status {
        s.split(',').map(|v| v.trim().to_string()).collect()
    } else {
        vec!["processed".into(), "removing".into()]
    };

    let filter = MessageFilter {
        statuses,
        start_date: query.start_date,
        end_date: query.end_date,
        sort_order: query.sort_order.unwrap_or(-1),
        sort_by: "time".into(),
        page,
        per_page,
        ..Default::default()
    };

    let db = state.db.clone();
    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| messages::query_message_hashes(conn, &filter))
    })
    .await
    .unwrap();

    match result {
        Ok((hashes, total)) => HttpResponse::Ok().json(serde_json::json!({
            "hashes": hashes,
            "pagination_page": page,
            "pagination_total": total,
            "pagination_per_page": per_page,
            "pagination_item": "hashes"
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{AppState, configure_routes};
    use crate::config::HephConfig;
    use crate::db::Db;
    use crate::files::FileStore;
    use actix_web::{App, test};
    use std::sync::Arc;

    use aleph_types::account::{Account, EvmAccount, sign_message};
    use aleph_types::chain::Chain;
    use aleph_types::item_hash::{AlephItemHash, ItemHash};
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use aleph_types::message::unsigned::UnsignedMessage;
    use aleph_types::timestamp::Timestamp;

    fn make_test_state(tmpdir: &tempfile::TempDir) -> web::Data<AppState> {
        let db = Arc::new(Db::open_in_memory().unwrap());
        let file_store = Arc::new(FileStore::new(&tmpdir.path().join("files")).unwrap());
        let config = HephConfig {
            port: 4024,
            host: "127.0.0.1".into(),
            data_dir: None,
            accounts: vec![],
            balance: 1_000_000_000,
            log_level: "info".into(),
        };
        web::Data::new(AppState {
            db,
            file_store,
            config,
        })
    }

    fn sign_test_post(key: &[u8; 32], time: f64) -> (serde_json::Value, String) {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let addr_str = account.address().as_str().to_string();
        let item_content = format!(
            r#"{{"type":"test","address":"{}","time":{},"content":{{"body":"Hello"}}}}"#,
            addr_str, time
        );
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));

        let unsigned = UnsignedMessage {
            message_type: MessageType::Post,
            item_type: ItemType::Inline,
            item_content: item_content.clone(),
            item_hash: item_hash.clone(),
            time: Timestamp::from(time),
            channel: None,
        };

        let pending = sign_message(&account, unsigned).unwrap();
        let hash_str = pending.item_hash.to_string();

        let msg_json = serde_json::json!({
            "chain": pending.chain,
            "sender": pending.sender.as_str(),
            "signature": pending.signature.as_str(),
            "type": "POST",
            "item_type": "inline",
            "item_content": pending.item_content,
            "item_hash": hash_str,
            "time": time,
        });

        (msg_json, hash_str)
    }

    #[actix_web::test]
    async fn test_post_and_get_message() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let (msg_json, hash_str) = sign_test_post(&[1u8; 32], 1_700_000_000.0);

        // POST message with sync=true
        let req = test::TestRequest::post()
            .uri("/api/v0/messages")
            .set_json(serde_json::json!({
                "sync": true,
                "message": msg_json,
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200, "POST should return 200 for sync");

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["message_status"], "processed");
        assert_eq!(body["publication_status"]["status"], "success");

        // GET /api/v0/messages/{hash}
        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/messages/{hash_str}"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "processed");
        assert_eq!(body["item_hash"], hash_str);
        assert!(body["message"]["sender"].is_string());
        assert_eq!(body["message"]["confirmed"], false);

        // GET /api/v0/messages/{hash}/status
        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/messages/{hash_str}/status"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "processed");

        // GET /api/v0/messages/{hash}/content
        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/messages/{hash_str}/content"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["body"], "Hello");
    }

    #[actix_web::test]
    async fn test_post_async_returns_202() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let (msg_json, _hash_str) = sign_test_post(&[2u8; 32], 1_700_000_001.0);

        let req = test::TestRequest::post()
            .uri("/api/v0/messages")
            .set_json(serde_json::json!({
                "sync": false,
                "message": msg_json,
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 202, "POST with sync=false should return 202");

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["message_status"], "pending");
    }

    #[actix_web::test]
    async fn test_get_missing_message_404() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/messages/nonexistent_hash")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 404);
    }

    #[actix_web::test]
    async fn test_list_messages() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        // Post two messages
        let (msg1, _) = sign_test_post(&[3u8; 32], 1_700_000_002.0);
        let (msg2, _) = sign_test_post(&[4u8; 32], 1_700_000_003.0);

        for msg in [&msg1, &msg2] {
            let req = test::TestRequest::post()
                .uri("/api/v0/messages")
                .set_json(serde_json::json!({ "sync": true, "message": msg }))
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert!(resp.status().is_success());
        }

        // List messages
        let req = test::TestRequest::get()
            .uri("/api/v0/messages.json")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["pagination_total"], 2);
        assert_eq!(body["messages"].as_array().unwrap().len(), 2);
        assert_eq!(body["pagination_item"], "messages");
    }

    #[actix_web::test]
    async fn test_list_hashes() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let (msg1, hash1) = sign_test_post(&[5u8; 32], 1_700_000_004.0);
        let req = test::TestRequest::post()
            .uri("/api/v0/messages")
            .set_json(serde_json::json!({ "sync": true, "message": msg1 }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let req = test::TestRequest::get()
            .uri("/api/v0/messages/hashes")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        let hashes = body["hashes"].as_array().unwrap();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0], hash1);
    }

    #[actix_web::test]
    async fn test_content_endpoint_rejects_non_post() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);
        let db = state.db.clone();

        // Insert a non-POST message directly into the DB
        db.with_conn(|conn| {
            messages::insert_message(
                conn,
                &messages::InsertMessage {
                    item_hash: "agg_hash",
                    message_type: aleph_types::message::MessageType::Aggregate,
                    chain: "ETH",
                    sender: "0xtest",
                    signature: "0xsig",
                    item_type: "inline",
                    item_content: Some("{}"),
                    content_json: "{}",
                    channel: None,
                    time: 1000.0,
                    size: 2,
                    status: aleph_types::message::MessageStatus::Processed,
                    reception_time: 1000.0,
                    owner: None,
                    content_type: None,
                    content_ref: None,
                    content_key: None,
                    content_item_hash: None,
                    payment_type: None,
                },
            )
        })
        .unwrap();

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/messages/agg_hash/content")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 422);
    }
}
