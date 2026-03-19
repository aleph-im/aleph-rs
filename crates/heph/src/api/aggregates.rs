use actix_web::{HttpResponse, Responder, web};
use serde::Deserialize;

use crate::api::AppState;
use crate::db::aggregates::{self, AggregateFilter};

// ---------------------------------------------------------------------------
// GET /api/v0/aggregates/{address}.json  (spec 9.8)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct GetAggregateQuery {
    pub keys: Option<String>,
    pub limit: Option<usize>,
    pub with_info: Option<bool>,
    pub value_only: Option<bool>,
}

pub async fn get_aggregates_for_address(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<GetAggregateQuery>,
) -> impl Responder {
    let address = path.into_inner();
    let keys_csv = query.keys.clone();
    let limit = query.limit.unwrap_or(1000);
    let with_info = query.with_info.unwrap_or(false);
    let value_only = query.value_only.unwrap_or(false);
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            let key_strings: Vec<String> = keys_csv
                .as_deref()
                .map(|s| s.split(',').map(|k| k.trim().to_string()).collect())
                .unwrap_or_default();
            let key_refs: Vec<&str> = key_strings.iter().map(|s| s.as_str()).collect();
            let keys_opt = if key_refs.is_empty() {
                None
            } else {
                Some(key_refs.as_slice())
            };
            aggregates::get_aggregates_for_address(conn, &address, keys_opt, limit)
        })
    })
    .await
    .unwrap();

    match result {
        Ok(records) => {
            if records.is_empty() {
                return HttpResponse::NotFound().json(serde_json::json!({
                    "error": "No aggregates found for this address"
                }));
            }

            // value_only: single key, return value directly
            if value_only && records.len() == 1 {
                let val: serde_json::Value =
                    serde_json::from_str(&records[0].content).unwrap_or(serde_json::Value::Null);
                return HttpResponse::Ok().json(val);
            }

            let mut data = serde_json::Map::new();
            let mut info_map = serde_json::Map::new();

            for rec in &records {
                let content: serde_json::Value =
                    serde_json::from_str(&rec.content).unwrap_or(serde_json::Value::Null);
                data.insert(rec.key.clone(), content);

                if with_info {
                    info_map.insert(
                        rec.key.clone(),
                        serde_json::json!({
                            "created": rec.created_at,
                            "last_updated": rec.last_updated,
                            "original_item_hash": rec.last_revision_hash,
                            "last_update_item_hash": rec.last_revision_hash,
                        }),
                    );
                }
            }

            let address_val = records[0].address.clone();

            if with_info {
                HttpResponse::Ok().json(serde_json::json!({
                    "address": address_val,
                    "data": data,
                    "info": info_map,
                }))
            } else {
                HttpResponse::Ok().json(serde_json::json!({
                    "address": address_val,
                    "data": data,
                }))
            }
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/aggregates.json  and  GET /api/v0/aggregates  (spec 9.9)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct ListAggregatesQuery {
    pub keys: Option<String>,
    pub addresses: Option<String>,
    #[serde(rename = "sortBy")]
    pub sort_by: Option<String>,
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i32>,
    pub pagination: Option<u32>,
    pub page: Option<u32>,
}

fn split_csv(s: &Option<String>) -> Vec<String> {
    match s {
        Some(v) if !v.is_empty() => v.split(',').map(|s| s.trim().to_string()).collect(),
        _ => vec![],
    }
}

pub async fn list_aggregates(
    state: web::Data<AppState>,
    query: web::Query<ListAggregatesQuery>,
) -> impl Responder {
    let per_page = query.pagination.unwrap_or(20).min(500);
    let page = query.page.unwrap_or(1).max(1);
    let filter = AggregateFilter {
        addresses: split_csv(&query.addresses),
        keys: split_csv(&query.keys),
        sort_by: query
            .sort_by
            .clone()
            .unwrap_or_else(|| "last_modified".into()),
        sort_order: query.sort_order.unwrap_or(-1),
        page,
        per_page,
    };
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| aggregates::query_aggregates(conn, &filter))
    })
    .await
    .unwrap();

    match result {
        Ok((records, total)) => {
            let aggregates_out: Vec<serde_json::Value> = records
                .iter()
                .map(|rec| {
                    let content: serde_json::Value =
                        serde_json::from_str(&rec.content).unwrap_or(serde_json::Value::Null);
                    serde_json::json!({
                        "address": rec.address,
                        "key": rec.key,
                        "content": content,
                        "created": rec.created_at,
                        "last_updated": rec.last_updated,
                    })
                })
                .collect();

            HttpResponse::Ok().json(serde_json::json!({
                "aggregates": aggregates_out,
                "pagination_per_page": per_page,
                "pagination_page": page,
                "pagination_total": total,
                "pagination_item": "aggregates",
            }))
        }
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

    fn sign_aggregate_msg(
        key: &[u8; 32],
        agg_key: &str,
        content_json: &str,
        time: f64,
    ) -> serde_json::Value {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let addr = account.address().as_str().to_string();
        let ic = format!(
            r#"{{"key":"{}","address":"{}","time":{},"content":{}}}"#,
            agg_key, addr, time, content_json
        );
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(ic.as_bytes()));
        let unsigned = UnsignedMessage {
            message_type: MessageType::Aggregate,
            item_type: ItemType::Inline,
            item_content: ic.clone(),
            item_hash: item_hash.clone(),
            time: Timestamp::from(time),
            channel: None,
        };
        let pending = sign_message(&account, unsigned).unwrap();
        serde_json::json!({
            "chain": pending.chain,
            "sender": pending.sender.as_str(),
            "signature": pending.signature.as_str(),
            "type": "AGGREGATE",
            "item_type": "inline",
            "item_content": pending.item_content,
            "item_hash": pending.item_hash.to_string(),
            "time": time,
        })
    }

    fn addr_for_key(key: &[u8; 32]) -> String {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        account.address().as_str().to_string()
    }

    #[actix_web::test]
    async fn test_get_aggregates_for_address() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);
        let key = [100u8; 32];
        let addr = addr_for_key(&key);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        // Submit aggregate via HTTP
        let msg = sign_aggregate_msg(&key, "profile", r#"{"name":"Alice"}"#, 1_700_000_000.0);
        let req = test::TestRequest::post()
            .uri("/api/v0/messages")
            .set_json(serde_json::json!({ "sync": true, "message": msg }))
            .to_request();
        let r = test::call_service(&app, req).await;
        assert!(r.status().is_success());

        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/aggregates/{addr}.json"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["address"], addr);
        assert_eq!(body["data"]["profile"]["name"], "Alice");
    }

    #[actix_web::test]
    async fn test_get_aggregates_with_keys_filter() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);
        let key = [101u8; 32];
        let addr = addr_for_key(&key);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let msg1 = sign_aggregate_msg(&key, "profile", r#"{"name":"Alice"}"#, 1_700_000_000.0);
        let msg2 = sign_aggregate_msg(&key, "settings", r#"{"theme":"dark"}"#, 1_700_000_001.0);
        for msg in [msg1, msg2] {
            let req = test::TestRequest::post()
                .uri("/api/v0/messages")
                .set_json(serde_json::json!({ "sync": true, "message": msg }))
                .to_request();
            let r = test::call_service(&app, req).await;
            assert!(r.status().is_success());
        }

        // Only request "profile" key
        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/aggregates/{addr}.json?keys=profile"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert!(body["data"]["profile"].is_object());
        assert!(body["data"]["settings"].is_null());
    }

    #[actix_web::test]
    async fn test_get_aggregates_with_info() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);
        let key = [102u8; 32];
        let addr = addr_for_key(&key);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let msg = sign_aggregate_msg(&key, "profile", r#"{"name":"Bob"}"#, 1_700_000_000.0);
        let req = test::TestRequest::post()
            .uri("/api/v0/messages")
            .set_json(serde_json::json!({ "sync": true, "message": msg }))
            .to_request();
        let r = test::call_service(&app, req).await;
        assert!(r.status().is_success());

        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/aggregates/{addr}.json?with_info=true"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert!(body["info"].is_object());
        assert!(body["info"]["profile"]["created"].is_string());
        assert!(body["info"]["profile"]["last_updated"].is_string());
    }

    #[actix_web::test]
    async fn test_get_aggregates_404_for_missing() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/aggregates/0xdeadbeef.json")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 404);
    }

    #[actix_web::test]
    async fn test_list_aggregates() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);
        let key = [103u8; 32];

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let msg = sign_aggregate_msg(&key, "profile", r#"{"name":"Carol"}"#, 1_700_000_000.0);
        let req = test::TestRequest::post()
            .uri("/api/v0/messages")
            .set_json(serde_json::json!({ "sync": true, "message": msg }))
            .to_request();
        let r = test::call_service(&app, req).await;
        assert!(r.status().is_success());

        let req = test::TestRequest::get()
            .uri("/api/v0/aggregates.json")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["pagination_total"], 1);
        assert_eq!(body["pagination_item"], "aggregates");
        let aggs = body["aggregates"].as_array().unwrap();
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0]["key"], "profile");
        assert_eq!(aggs[0]["content"]["name"], "Carol");
    }
}
