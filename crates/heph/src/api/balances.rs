use actix_web::{HttpResponse, Responder, web};
use serde::Deserialize;

use crate::api::AppState;
use crate::db::balances;

// ---------------------------------------------------------------------------
// GET /api/v0/addresses/{address}/balance  (spec 9.20)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct BalanceQuery {
    pub chain: Option<String>,
}

pub async fn get_balance(
    state: web::Data<AppState>,
    path: web::Path<String>,
    _query: web::Query<BalanceQuery>,
) -> impl Responder {
    let address = path.into_inner();
    let addr_clone = address.clone();
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| balances::get_credit_balance(conn, &addr_clone))
    })
    .await
    .unwrap();

    match result {
        Ok(credit) => HttpResponse::Ok().json(serde_json::json!({
            "address": address,
            "balance": 0.0,
            "details": {},
            "locked_amount": 0.0,
            "credit_balance": credit.unwrap_or(0),
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/balances  (spec 9.21)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct ListBalancesQuery {
    pub chains: Option<String>,
    pub pagination: Option<u32>,
    pub page: Option<u32>,
    pub min_balance: Option<f64>,
}

pub async fn list_balances(
    _state: web::Data<AppState>,
    query: web::Query<ListBalancesQuery>,
) -> impl Responder {
    let per_page = query.pagination.unwrap_or(100);
    let page = query.page.unwrap_or(1).max(1);

    HttpResponse::Ok().json(serde_json::json!({
        "balances": [],
        "pagination_per_page": per_page,
        "pagination_page": page,
        "pagination_total": 0,
        "pagination_item": "balances",
    }))
}

// ---------------------------------------------------------------------------
// GET /api/v0/credit_balances  (spec 9.22)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct ListCreditBalancesQuery {
    pub pagination: Option<u32>,
    pub page: Option<u32>,
    pub min_balance: Option<i64>,
}

pub async fn list_credit_balances(
    state: web::Data<AppState>,
    query: web::Query<ListCreditBalancesQuery>,
) -> impl Responder {
    let per_page = query.pagination.unwrap_or(100) as i64;
    let page = query.page.unwrap_or(1).max(1) as i64;
    let min_balance = query.min_balance.unwrap_or(0);
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            let total: i64 = conn.query_row(
                "SELECT COUNT(*) FROM credit_balances WHERE balance >= ?1",
                rusqlite::params![min_balance],
                |r| r.get(0),
            )?;

            let offset = (page - 1) * per_page;
            let mut stmt = conn.prepare(
                "SELECT address, balance FROM credit_balances WHERE balance >= ?1
                 ORDER BY balance DESC LIMIT ?2 OFFSET ?3",
            )?;
            let rows = stmt
                .query_map(rusqlite::params![min_balance, per_page, offset], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            Ok::<_, rusqlite::Error>((rows, total))
        })
    })
    .await
    .unwrap();

    match result {
        Ok((rows, total)) => {
            let credit_balances: Vec<serde_json::Value> = rows
                .iter()
                .map(|(addr, credits)| serde_json::json!({ "address": addr, "credits": credits }))
                .collect();

            HttpResponse::Ok().json(serde_json::json!({
                "credit_balances": credit_balances,
                "pagination_per_page": per_page,
                "pagination_page": page,
                "pagination_total": total,
                "pagination_item": "credit_balances",
            }))
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/addresses/{address}/credit_history  (spec 9.23)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct CreditHistoryQuery {
    pub pagination: Option<u32>,
    pub page: Option<u32>,
}

pub async fn get_credit_history(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<CreditHistoryQuery>,
) -> impl Responder {
    let address = path.into_inner();
    let addr_clone = address.clone();
    let per_page = query.pagination.unwrap_or(100) as i64;
    let page = query.page.unwrap_or(1).max(1) as i64;
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            let total: i64 = conn.query_row(
                "SELECT COUNT(*) FROM credit_history WHERE address = ?1",
                rusqlite::params![addr_clone],
                |r| r.get(0),
            )?;

            let offset = (page - 1) * per_page;
            let mut stmt = conn.prepare(
                "SELECT amount, tx_hash, created_at FROM credit_history
                 WHERE address = ?1 ORDER BY id DESC LIMIT ?2 OFFSET ?3",
            )?;
            let rows = stmt
                .query_map(rusqlite::params![addr_clone, per_page, offset], |row| {
                    Ok(serde_json::json!({
                        "amount": row.get::<_, i64>(0)?,
                        "tx_hash": row.get::<_, Option<String>>(1)?,
                        "created_at": row.get::<_, String>(2)?,
                    }))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            Ok::<_, rusqlite::Error>((rows, total))
        })
    })
    .await
    .unwrap();

    match result {
        Ok((rows, total)) => HttpResponse::Ok().json(serde_json::json!({
            "address": address,
            "credit_history": rows,
            "pagination_page": page,
            "pagination_total": total,
            "pagination_per_page": per_page,
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

    #[actix_web::test]
    async fn test_get_balance_with_seeded_address() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        state
            .db
            .with_conn(|conn| balances::set_credit_balance(conn, "0xtest_addr", 5000))
            .unwrap();

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/addresses/0xtest_addr/balance")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["address"], "0xtest_addr");
        assert_eq!(body["credit_balance"], 5000);
        assert_eq!(body["balance"], 0.0);
    }

    #[actix_web::test]
    async fn test_get_balance_for_unknown_address() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/addresses/0xunknown/balance")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["credit_balance"], 0);
    }

    #[actix_web::test]
    async fn test_list_balances_empty() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/balances")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["balances"].as_array().unwrap().len(), 0);
        assert_eq!(body["pagination_total"], 0);
        assert_eq!(body["pagination_item"], "balances");
    }

    #[actix_web::test]
    async fn test_list_credit_balances() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        state
            .db
            .with_conn(|conn| {
                balances::set_credit_balance(conn, "0xaddr1", 1000)?;
                balances::set_credit_balance(conn, "0xaddr2", 2000)?;
                Ok::<_, rusqlite::Error>(())
            })
            .unwrap();

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/credit_balances")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["pagination_total"], 2);
        assert_eq!(body["pagination_item"], "credit_balances");
        let cbs = body["credit_balances"].as_array().unwrap();
        assert_eq!(cbs.len(), 2);
    }

    #[actix_web::test]
    async fn test_get_credit_history() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        state
            .db
            .with_conn(|conn| {
                balances::insert_credit_history(conn, "0xowner", 1000, Some("0xtx1"))?;
                balances::insert_credit_history(conn, "0xowner", 500, None)?;
                Ok::<_, rusqlite::Error>(())
            })
            .unwrap();

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/addresses/0xowner/credit_history")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["address"], "0xowner");
        assert_eq!(body["pagination_total"], 2);
        let history = body["credit_history"].as_array().unwrap();
        assert_eq!(history.len(), 2);
    }
}
