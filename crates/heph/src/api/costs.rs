use actix_web::{HttpResponse, Responder, web};
use serde::Deserialize;

use crate::api::AppState;
use crate::db::costs;
use crate::db::messages;
use crate::handlers::{IncomingMessage, ProcessingError, compute_cost_records, validate};

// ---------------------------------------------------------------------------
// GET /api/v0/messages/{hash}/consumed_credits  (spec 9.24)
// ---------------------------------------------------------------------------

pub async fn get_consumed_credits(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let item_hash = path.into_inner();
    let hash_clone = item_hash.clone();
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| costs::get_costs_for_item(conn, &hash_clone))
    })
    .await
    .unwrap();

    match result {
        Ok(records) => {
            let consumed: f64 = records
                .iter()
                .filter_map(|r| r.cost_credit.parse::<f64>().ok())
                .sum();
            // Round to integer credits
            let consumed_credits = consumed.round() as i64;
            HttpResponse::Ok().json(serde_json::json!({
                "item_hash": item_hash,
                "consumed_credits": consumed_credits,
            }))
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/costs  (spec 9.25)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct CostsQuery {
    pub address: Option<String>,
    pub item_hash: Option<String>,
    pub payment_type: Option<String>,
    pub include_details: Option<u8>,
    pub include_size: Option<bool>,
    pub pagination: Option<u32>,
    pub page: Option<u32>,
}

pub async fn list_costs(
    state: web::Data<AppState>,
    query: web::Query<CostsQuery>,
) -> impl Responder {
    let address = query.address.clone();
    let item_hash_filter = query.item_hash.clone();
    let payment_type = query
        .payment_type
        .clone()
        .unwrap_or_else(|| "credit".into());
    let include_details = query.include_details.unwrap_or(0);
    let per_page = query.pagination.unwrap_or(100) as i64;
    let page = query.page.unwrap_or(1).max(1) as i64;
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            // Build query dynamically
            let mut clauses: Vec<String> = Vec::new();
            let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

            if let Some(ref addr) = address {
                params.push(Box::new(addr.clone()));
                clauses.push(format!("owner = ?{}", params.len()));
            }
            if let Some(ref ih) = item_hash_filter {
                params.push(Box::new(ih.clone()));
                clauses.push(format!("item_hash = ?{}", params.len()));
            }

            let where_sql = if clauses.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", clauses.join(" AND "))
            };

            // Summary aggregation
            let summary_sql = format!(
                "SELECT
                    COALESCE(SUM(CAST(cost_credit AS REAL)), 0.0),
                    COALESCE(SUM(CAST(cost_hold AS REAL)), 0.0),
                    COALESCE(SUM(CAST(cost_stream AS REAL)), 0.0),
                    COUNT(DISTINCT item_hash)
                 FROM account_costs{where_sql}"
            );
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p.as_ref()).collect();

            let (total_credit, total_hold, total_stream, resource_count): (f64, f64, f64, i64) =
                conn.query_row(&summary_sql, param_refs.as_slice(), |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })?;

            // For include_details >= 1, also get the resource list
            let resources = if include_details >= 1 {
                let offset = (page - 1) * per_page;
                // Rebuild params for the second query (Box<dyn ToSql> is not Clone)
                let mut params_r: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
                if let Some(ref addr) = address {
                    params_r.push(Box::new(addr.clone()));
                }
                if let Some(ref ih) = item_hash_filter {
                    params_r.push(Box::new(ih.clone()));
                }
                let resources_sql = format!(
                    "SELECT item_hash, owner, cost_type, name, ref_hash, payment_type,
                            cost_hold, cost_stream, cost_credit
                     FROM account_costs{where_sql}
                     ORDER BY item_hash ASC LIMIT ?{} OFFSET ?{}",
                    params_r.len() + 1,
                    params_r.len() + 2
                );
                params_r.push(Box::new(per_page));
                params_r.push(Box::new(offset));

                let param_refs3: Vec<&dyn rusqlite::types::ToSql> =
                    params_r.iter().map(|p| p.as_ref()).collect();
                let mut stmt = conn.prepare(&resources_sql)?;
                stmt.query_map(param_refs3.as_slice(), |row| {
                    Ok(serde_json::json!({
                        "item_hash": row.get::<_, String>(0)?,
                        "owner": row.get::<_, String>(1)?,
                        "cost_type": row.get::<_, String>(2)?,
                        "name": row.get::<_, String>(3)?,
                        "ref_hash": row.get::<_, Option<String>>(4)?,
                        "payment_type": row.get::<_, String>(5)?,
                        "cost_hold": row.get::<_, String>(6)?,
                        "cost_stream": row.get::<_, String>(7)?,
                        "cost_credit": row.get::<_, String>(8)?,
                    }))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
            } else {
                vec![]
            };

            Ok::<_, rusqlite::Error>((
                total_credit,
                total_hold,
                total_stream,
                resource_count,
                resources,
            ))
        })
    })
    .await
    .unwrap();

    match result {
        Ok((total_credit, total_hold, total_stream, resource_count, resources)) => {
            let summary = serde_json::json!({
                "total_consumed_credits": total_credit.round() as i64,
                "total_cost_hold": format!("{total_hold:.6}"),
                "total_cost_stream": format!("{total_stream:.6}"),
                "total_cost_credit": format!("{total_credit:.6}"),
                "resource_count": resource_count,
            });

            let filters = serde_json::json!({
                "address": query.address,
                "item_hash": query.item_hash,
                "payment_type": payment_type,
            });

            if include_details >= 1 {
                HttpResponse::Ok().json(serde_json::json!({
                    "summary": summary,
                    "filters": filters,
                    "resources": resources,
                }))
            } else {
                HttpResponse::Ok().json(serde_json::json!({
                    "summary": summary,
                    "filters": filters,
                }))
            }
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/price/{hash}  (spec 9.26)
// ---------------------------------------------------------------------------

pub async fn get_price(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let item_hash = path.into_inner();
    let hash_clone = item_hash.clone();
    let hash_clone2 = item_hash.clone();
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            // Check message exists
            let msg = messages::get_message_by_hash(conn, &hash_clone)?;
            let cost_records = costs::get_costs_for_item(conn, &hash_clone2)?;
            Ok::<_, rusqlite::Error>((msg, cost_records))
        })
    })
    .await
    .unwrap();

    match result {
        Ok((None, _)) => HttpResponse::NotFound().json(serde_json::json!({
            "error": "Message not found"
        })),
        Ok((Some(msg), records)) => {
            if msg.status == "forgotten" {
                return HttpResponse::Gone().json(serde_json::json!({
                    "error": "Message has been forgotten"
                }));
            }

            let total_cost: f64 = records
                .iter()
                .filter_map(|r| r.cost_credit.parse::<f64>().ok())
                .sum();

            let charged_address = msg.sender.clone();
            let payment_type = msg.payment_type.clone().unwrap_or_else(|| "credit".into());

            let detail: Vec<serde_json::Value> = records
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "name": r.name,
                        "cost_type": r.cost_type,
                        "cost_credit": r.cost_credit,
                    })
                })
                .collect();

            HttpResponse::Ok().json(serde_json::json!({
                "required_tokens": 0.0,
                "payment_type": payment_type,
                "cost": format!("{total_cost:.6}"),
                "detail": detail,
                "charged_address": charged_address,
            }))
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// POST /api/v0/price/estimate  (spec 9.27)
// ---------------------------------------------------------------------------

fn bad_request(msg: impl Into<String>) -> HttpResponse {
    HttpResponse::BadRequest().json(serde_json::json!({ "error": msg.into() }))
}

/// Estimate the cost of a message before submission.
///
/// Mirrors the per-message cost calculation in
/// `handlers::compute_cost_records` so the estimate matches what would be
/// recorded on submission. STORE/PROGRAM/INSTANCE are billed (per-second
/// credit cost, in `cost_credit`); POST/AGGREGATE/FORGET return zero with an
/// empty detail array.
///
/// Inline messages are validated and parsed in-process. Non-inline (storage)
/// messages fall back to the local `FileStore`, matching the
/// `process_message_with_store` flow used at submission time.
pub async fn estimate_price(
    state: web::Data<AppState>,
    body: web::Json<serde_json::Value>,
) -> impl Responder {
    let msg_value = match body.get("message").cloned() {
        Some(v) if !v.is_null() => v,
        _ => return bad_request("missing 'message' field"),
    };

    let msg: IncomingMessage = match serde_json::from_value(msg_value) {
        Ok(m) => m,
        Err(e) => return bad_request(format!("invalid message: {e}")),
    };

    let content = match validate::validate_format(&msg) {
        Ok(c) => c,
        Err(ProcessingError::ContentUnavailable(_)) => {
            // Non-inline (storage/IPFS) — try to resolve via local FileStore,
            // matching what `process_message_with_store` does at submission.
            let item_hash_str = msg.item_hash.to_string();
            match state.file_store.read(&item_hash_str) {
                Ok(raw) => match validate::validate_fetched_content(&msg, &raw) {
                    Ok(c) => c,
                    Err(e) => return bad_request(e.message().to_string()),
                },
                Err(_) => {
                    return bad_request(format!(
                        "content for {item_hash_str} not available locally; \
                         upload it first or send an inline message",
                    ));
                }
            }
        }
        Err(e) => return bad_request(e.message().to_string()),
    };

    let item_hash = msg.item_hash.to_string();
    let charged_address = content.address.as_str().to_string();
    let records = compute_cost_records(&msg, &content, &item_hash);

    let total_cost: f64 = records
        .iter()
        .filter_map(|r| r.cost_credit.parse::<f64>().ok())
        .sum();
    // `format!("{:.6}", -0.0)` yields "-0.000000"; collapse signed zero so
    // free-message and zero-cost responses round-trip as "0.000000".
    let total_cost = if total_cost == 0.0 { 0.0 } else { total_cost };

    let detail: Vec<serde_json::Value> = records
        .iter()
        .map(|r| {
            serde_json::json!({
                "name": r.name,
                "cost_type": r.cost_type,
                "cost_credit": r.cost_credit,
            })
        })
        .collect();

    HttpResponse::Ok().json(serde_json::json!({
        "required_tokens": 0.0,
        "payment_type": "credit",
        "cost": format!("{total_cost:.6}"),
        "detail": detail,
        "charged_address": charged_address,
    }))
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
    use crate::db::costs::AccountCostRecord;
    use crate::files::FileStore;
    use actix_web::{App, test};
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

    #[actix_web::test]
    async fn test_get_consumed_credits_no_costs() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/messages/nonexistent_hash/consumed_credits")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["item_hash"], "nonexistent_hash");
        assert_eq!(body["consumed_credits"], 0);
    }

    #[actix_web::test]
    async fn test_get_consumed_credits_with_costs() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let hash = "aaaa1111111111111111111111111111111111111111111111111111111111111111";
        state
            .db
            .with_conn(|conn| {
                costs::insert_account_costs(
                    conn,
                    &[AccountCostRecord {
                        owner: "0xowner".to_string(),
                        item_hash: hash.to_string(),
                        cost_type: "STORAGE".to_string(),
                        name: "file".to_string(),
                        ref_hash: None,
                        payment_type: "credit".to_string(),
                        cost_hold: "0".to_string(),
                        cost_stream: "0".to_string(),
                        cost_credit: "750".to_string(),
                    }],
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
            .uri(&format!("/api/v0/messages/{hash}/consumed_credits"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["consumed_credits"], 750);
    }

    #[actix_web::test]
    async fn test_list_costs_empty() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get().uri("/api/v0/costs").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["summary"]["resource_count"], 0);
        assert_eq!(body["summary"]["total_consumed_credits"], 0);
        assert_eq!(body["filters"]["payment_type"], "credit");
    }

    // -----------------------------------------------------------------------
    // estimate_price tests
    // -----------------------------------------------------------------------

    use aleph_types::account::{Account, EvmAccount, sign_message};
    use aleph_types::chain::Chain;
    use aleph_types::item_hash::{AlephItemHash, ItemHash};
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use aleph_types::message::unsigned::UnsignedMessage;
    use aleph_types::timestamp::Timestamp;

    fn sign_inline(
        key: &[u8; 32],
        msg_type: MessageType,
        item_content: String,
    ) -> serde_json::Value {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));
        let unsigned = UnsignedMessage {
            message_type: msg_type,
            item_type: ItemType::Inline,
            item_content: item_content.clone(),
            item_hash,
            time: Timestamp::from(1_000.0),
            channel: None,
        };
        let pending = sign_message(&account, unsigned).unwrap();
        serde_json::to_value(&pending).unwrap()
    }

    fn addr_for_key(key: &[u8; 32]) -> String {
        EvmAccount::new(Chain::Ethereum, key)
            .unwrap()
            .address()
            .as_str()
            .to_string()
    }

    async fn post_estimate(
        state: &web::Data<AppState>,
        message: serde_json::Value,
    ) -> (u16, serde_json::Value) {
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let req = test::TestRequest::post()
            .uri("/api/v0/price/estimate")
            .set_json(serde_json::json!({ "message": message }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        let status = resp.status().as_u16();
        let body: serde_json::Value = test::read_body_json(resp).await;
        (status, body)
    }

    /// Regression: STORE on a fresh address must report a non-zero per-second
    /// cost matching `calculate_store_cost`, with a populated `detail` array
    /// (the previous stub returned `{cost:"0", detail:[]}` for everything).
    #[actix_web::test]
    async fn test_estimate_store_returns_storage_cost() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let key = [90u8; 32];
        let addr = addr_for_key(&key);
        let file_hash = "a".repeat(64);
        let size: u64 = 100 * 1024 * 1024;
        let item_content = format!(
            r#"{{"address":"{}","time":1000.0,"item_type":"storage","item_hash":"{}","size":{}}}"#,
            addr, file_hash, size
        );
        let msg = sign_inline(&key, MessageType::Store, item_content);

        let (status, body) = post_estimate(&state, msg).await;
        assert_eq!(status, 200);

        let expected_per_second = crate::cost::calculate_store_cost(size);
        let expected_cost_str = format!("{expected_per_second:.6}");
        assert_eq!(body["cost"], expected_cost_str);
        assert_eq!(body["payment_type"], "credit");
        assert_eq!(body["charged_address"], addr);
        let detail = body["detail"].as_array().expect("detail must be an array");
        assert_eq!(detail.len(), 1, "STORE must produce one cost detail entry");
        assert_eq!(detail[0]["cost_type"], "STORAGE");
    }

    /// STORE without an explicit `size` falls back to the 25-MiB minimum.
    #[actix_web::test]
    async fn test_estimate_store_no_size_uses_minimum() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let key = [91u8; 32];
        let addr = addr_for_key(&key);
        let file_hash = "b".repeat(64);
        let item_content = format!(
            r#"{{"address":"{}","time":1000.0,"item_type":"storage","item_hash":"{}"}}"#,
            addr, file_hash
        );
        let msg = sign_inline(&key, MessageType::Store, item_content);

        let (status, body) = post_estimate(&state, msg).await;
        assert_eq!(status, 200);

        let expected = crate::cost::calculate_store_cost(0);
        assert_eq!(body["cost"], format!("{expected:.6}"));
        assert!(body["detail"].as_array().unwrap().len() == 1);
    }

    /// POST is free — empty detail and zero cost.
    #[actix_web::test]
    async fn test_estimate_post_is_free() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let key = [92u8; 32];
        let addr = addr_for_key(&key);
        let item_content = format!(
            r#"{{"type":"test","address":"{}","time":1000.0,"content":{{"body":"Hi"}}}}"#,
            addr
        );
        let msg = sign_inline(&key, MessageType::Post, item_content);

        let (status, body) = post_estimate(&state, msg).await;
        assert_eq!(status, 200);
        assert_eq!(body["cost"], "0.000000");
        assert_eq!(body["detail"].as_array().unwrap().len(), 0);
        assert_eq!(body["charged_address"], addr);
    }

    /// Missing `message` field returns 400 instead of a default-zero estimate.
    #[actix_web::test]
    async fn test_estimate_missing_message_returns_400() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let req = test::TestRequest::post()
            .uri("/api/v0/price/estimate")
            .set_json(serde_json::json!({}))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 400);
    }
}
