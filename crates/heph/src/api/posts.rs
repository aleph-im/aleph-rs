use actix_web::{HttpResponse, Responder, web};
use serde::Deserialize;

use crate::api::AppState;
use crate::db::posts::{self, PostFilter};

// ---------------------------------------------------------------------------
// Query params shared by v0 and v1
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct PostQuery {
    pub addresses: Option<String>,
    pub hashes: Option<String>,
    pub refs: Option<String>,
    pub types: Option<String>,
    pub tags: Option<String>,
    pub channels: Option<String>,
    #[serde(rename = "startDate")]
    pub start_date: Option<f64>,
    #[serde(rename = "endDate")]
    pub end_date: Option<f64>,
    pub pagination: Option<u32>,
    pub page: Option<u32>,
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

fn query_to_filter(q: &PostQuery) -> (PostFilter, u32, u32) {
    let per_page = q.pagination.unwrap_or(20).min(500);
    let page = q.page.unwrap_or(1).max(1);
    let filter = PostFilter {
        addresses: split_csv(&q.addresses),
        hashes: split_csv(&q.hashes),
        refs: split_csv(&q.refs),
        types: split_csv(&q.types),
        channels: split_csv(&q.channels),
        start_date: q.start_date,
        end_date: q.end_date,
        sort_by: q.sort_by.clone().unwrap_or_else(|| "time".into()),
        sort_order: q.sort_order.unwrap_or(-1),
        page,
        per_page,
    };
    (filter, page, per_page)
}

// ---------------------------------------------------------------------------
// GET /api/v0/posts.json  (spec 9.18) — V0 legacy format
// ---------------------------------------------------------------------------

pub async fn list_posts_v0(
    state: web::Data<AppState>,
    query: web::Query<PostQuery>,
) -> impl Responder {
    let (filter, page, per_page) = query_to_filter(&query);
    let db = state.db.clone();

    let result =
        tokio::task::spawn_blocking(move || db.with_conn(|conn| posts::query_posts(conn, &filter)))
            .await
            .unwrap();

    match result {
        Ok((posts_with_msgs, total)) => {
            let posts_out: Vec<serde_json::Value> = posts_with_msgs
                .iter()
                .map(|pwm| {
                    let effective = &pwm.msg;
                    let original = pwm.original_msg.as_ref().unwrap_or(&pwm.msg);

                    let content: serde_json::Value =
                        serde_json::from_str(&effective.content).unwrap_or(serde_json::Value::Null);
                    // Extract content.content (the user's body)
                    let content_body = content
                        .get("content")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);

                    let original_content: serde_json::Value =
                        serde_json::from_str(&original.content).unwrap_or(serde_json::Value::Null);
                    let original_type = original_content
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    let ref_val = pwm
                        .post
                        .ref_
                        .as_deref()
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null);

                    serde_json::json!({
                        "chain": effective.chain,
                        "item_hash": effective.item_hash,
                        "sender": effective.sender,
                        "type": effective.message_type,
                        "channel": effective.channel,
                        "confirmed": false,
                        "content": content_body,
                        "item_content": effective.item_content,
                        "item_type": effective.item_type,
                        "signature": effective.signature,
                        "size": effective.size,
                        "time": effective.time,
                        "confirmations": [],
                        "original_item_hash": original.item_hash,
                        "original_signature": original.signature,
                        "original_type": original_type,
                        "hash": original.item_hash,
                        "address": pwm.post.address,
                        "ref": ref_val,
                    })
                })
                .collect();

            HttpResponse::Ok().json(serde_json::json!({
                "posts": posts_out,
                "pagination_page": page,
                "pagination_total": total,
                "pagination_per_page": per_page,
                "pagination_item": "posts",
            }))
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v1/posts.json  (spec 9.19) — V1 clean format
// ---------------------------------------------------------------------------

pub async fn list_posts_v1(
    state: web::Data<AppState>,
    query: web::Query<PostQuery>,
) -> impl Responder {
    let (filter, page, per_page) = query_to_filter(&query);
    let db = state.db.clone();

    let result =
        tokio::task::spawn_blocking(move || db.with_conn(|conn| posts::query_posts(conn, &filter)))
            .await
            .unwrap();

    match result {
        Ok((posts_with_msgs, total)) => {
            let posts_out: Vec<serde_json::Value> = posts_with_msgs
                .iter()
                .map(|pwm| {
                    let effective = &pwm.msg;
                    let original = pwm.original_msg.as_ref().unwrap_or(&pwm.msg);

                    let content: serde_json::Value =
                        serde_json::from_str(&effective.content).unwrap_or(serde_json::Value::Null);
                    let content_body = content
                        .get("content")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);

                    let original_content: serde_json::Value =
                        serde_json::from_str(&original.content).unwrap_or(serde_json::Value::Null);
                    let original_type = original_content
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    let ref_val = pwm
                        .post
                        .ref_
                        .as_deref()
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null);

                    // Use the post's created_at (from posts table) if available; else fall back to message time.
                    // For v1 we use ISO8601 strings. Since posts table has created_at, we'd need it.
                    // We'll format time as ISO8601 for now (the posts table stores created_at but we
                    // didn't join it in query_posts; use time as fallback).
                    let time_str = format_time_iso(original.time);
                    let last_updated_str = format_time_iso(effective.time);

                    serde_json::json!({
                        "item_hash": effective.item_hash,
                        "content": content_body,
                        "original_item_hash": original.item_hash,
                        "original_type": original_type,
                        "address": pwm.post.address,
                        "ref": ref_val,
                        "channel": pwm.post.channel,
                        "created": time_str,
                        "last_updated": last_updated_str,
                    })
                })
                .collect();

            HttpResponse::Ok().json(serde_json::json!({
                "posts": posts_out,
                "pagination_page": page,
                "pagination_total": total,
                "pagination_per_page": per_page,
                "pagination_item": "posts",
            }))
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

/// Format a Unix timestamp (f64) as an ISO 8601 string.
fn format_time_iso(ts: f64) -> String {
    use chrono::{TimeZone, Utc};
    let secs = ts as i64;
    let nanos = ((ts - secs as f64) * 1_000_000_000.0) as u32;
    match Utc.timestamp_opt(secs, nanos) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),
        _ => ts.to_string(),
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
    use aleph_types::account::{Account, EvmAccount, sign_message};
    use aleph_types::chain::Chain;
    use aleph_types::item_hash::{AlephItemHash, ItemHash};
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use aleph_types::message::unsigned::UnsignedMessage;
    use aleph_types::timestamp::Timestamp;
    use std::sync::Arc;

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
            corechannel: std::sync::Mutex::new(crate::corechannel::CoreChannelState::new()),
        })
    }

    fn sign_post_msg(key: &[u8; 32], time: f64) -> (serde_json::Value, String) {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let addr = account.address().as_str().to_string();
        let ic = format!(
            r#"{{"type":"article","address":"{}","time":{},"content":{{"body":"Hello"}}}}"#,
            addr, time
        );
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(ic.as_bytes()));
        let unsigned = UnsignedMessage {
            message_type: MessageType::Post,
            item_type: ItemType::Inline,
            item_content: ic.clone(),
            item_hash: item_hash.clone(),
            time: Timestamp::from(time),
            channel: None,
        };
        let pending = sign_message(&account, unsigned).unwrap();
        let hash_str = pending.item_hash.to_string();
        let msg = serde_json::json!({
            "chain": pending.chain,
            "sender": pending.sender.as_str(),
            "signature": pending.signature.as_str(),
            "type": "POST",
            "item_type": "inline",
            "item_content": pending.item_content,
            "item_hash": hash_str,
            "time": time,
        });
        (msg, hash_str)
    }

    fn sign_amend_msg(key: &[u8; 32], time: f64, ref_hash: &str) -> (serde_json::Value, String) {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let addr = account.address().as_str().to_string();
        let ic = format!(
            r#"{{"type":"amend","ref":"{}","address":"{}","time":{},"content":{{"body":"Amended"}}}}"#,
            ref_hash, addr, time
        );
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(ic.as_bytes()));
        let unsigned = UnsignedMessage {
            message_type: MessageType::Post,
            item_type: ItemType::Inline,
            item_content: ic.clone(),
            item_hash: item_hash.clone(),
            time: Timestamp::from(time),
            channel: None,
        };
        let pending = sign_message(&account, unsigned).unwrap();
        let hash_str = pending.item_hash.to_string();
        let msg = serde_json::json!({
            "chain": pending.chain,
            "sender": pending.sender.as_str(),
            "signature": pending.signature.as_str(),
            "type": "POST",
            "item_type": "inline",
            "item_content": pending.item_content,
            "item_hash": hash_str,
            "time": time,
        });
        (msg, hash_str)
    }

    #[actix_web::test]
    async fn test_list_posts_v0_basic() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let (msg, hash_str) = sign_post_msg(&[50u8; 32], 1_700_000_000.0);
        let req = test::TestRequest::post()
            .uri("/api/v0/messages")
            .set_json(serde_json::json!({ "sync": true, "message": msg }))
            .to_request();
        let r = test::call_service(&app, req).await;
        assert!(r.status().is_success());

        let req = test::TestRequest::get()
            .uri("/api/v0/posts.json")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["pagination_total"], 1);
        assert_eq!(body["pagination_item"], "posts");
        let posts = body["posts"].as_array().unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0]["item_hash"], hash_str);
        assert_eq!(posts[0]["original_item_hash"], hash_str);
        assert_eq!(posts[0]["confirmed"], false);
    }

    #[actix_web::test]
    async fn test_list_posts_v0_with_amend() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let (orig_msg, orig_hash) = sign_post_msg(&[51u8; 32], 1_700_000_000.0);
        let req = test::TestRequest::post()
            .uri("/api/v0/messages")
            .set_json(serde_json::json!({ "sync": true, "message": orig_msg }))
            .to_request();
        let r = test::call_service(&app, req).await;
        assert!(r.status().is_success());

        let (amend_msg, amend_hash) = sign_amend_msg(&[51u8; 32], 1_700_000_001.0, &orig_hash);
        let req = test::TestRequest::post()
            .uri("/api/v0/messages")
            .set_json(serde_json::json!({ "sync": true, "message": amend_msg }))
            .to_request();
        let r = test::call_service(&app, req).await;
        assert!(r.status().is_success());

        let req = test::TestRequest::get()
            .uri("/api/v0/posts.json")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        let posts = body["posts"].as_array().unwrap();
        // Should return 1 post (the original), but with amend data
        assert_eq!(posts.len(), 1);
        // effective item_hash should be the amend's hash
        assert_eq!(posts[0]["item_hash"], amend_hash);
        // original hash preserved
        assert_eq!(posts[0]["original_item_hash"], orig_hash);
        assert_eq!(posts[0]["hash"], orig_hash);
    }

    #[actix_web::test]
    async fn test_list_posts_v1_basic() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let (msg, hash_str) = sign_post_msg(&[52u8; 32], 1_700_000_000.0);
        let req = test::TestRequest::post()
            .uri("/api/v0/messages")
            .set_json(serde_json::json!({ "sync": true, "message": msg }))
            .to_request();
        let r = test::call_service(&app, req).await;
        assert!(r.status().is_success());

        let req = test::TestRequest::get()
            .uri("/api/v1/posts.json")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["pagination_item"], "posts");
        let posts = body["posts"].as_array().unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0]["item_hash"], hash_str);
        assert!(posts[0]["created"].is_string());
        assert!(posts[0]["last_updated"].is_string());
    }
}
