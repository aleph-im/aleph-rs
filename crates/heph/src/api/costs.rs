use actix_web::{HttpResponse, Responder, web};
use serde::Deserialize;

use crate::api::AppState;
use crate::db::costs;
use crate::db::messages;

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

pub async fn estimate_price(
    _state: web::Data<AppState>,
    body: web::Json<serde_json::Value>,
) -> impl Responder {
    // Very basic estimation: return zeros for now.
    // Full implementation would parse message type and run cost calculation.
    let message = body
        .get("message")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let msg_type = message
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    // For free messages (POST, AGGREGATE, FORGET), cost is 0.
    let cost = match msg_type.to_uppercase().as_str() {
        "POST" | "AGGREGATE" | "FORGET" => 0.0f64,
        _ => 0.0, // Other types: simplified — actual cost would require size info
    };

    HttpResponse::Ok().json(serde_json::json!({
        "required_tokens": 0.0,
        "payment_type": "credit",
        "cost": format!("{cost:.6}"),
        "detail": [],
        "charged_address": message.get("address").and_then(|v| v.as_str()).unwrap_or(""),
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
}
