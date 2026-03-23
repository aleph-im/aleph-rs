use actix_web::{HttpResponse, Responder, web};
use serde::Deserialize;
use std::collections::HashMap;

use crate::api::AppState;

// ---------------------------------------------------------------------------
// GET /api/v0/addresses/stats.json  (spec 9.28)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct StatsQuery {
    // Query param is `addresses[]` — actix-web picks it up as a repeated param.
    // We accept a CSV fallback too via a single `addresses` param.
    pub addresses: Option<String>,
}

/// Parse `key=value` pairs from a percent-encoded query string.
/// Handles both repeated `addresses[]` params and CSV in a single `addresses` param.
fn parse_addresses_from_query(query_str: &str) -> Vec<String> {
    let mut address_list: Vec<String> = Vec::new();
    for pair in query_str.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = if let Some(pos) = pair.find('=') {
            (&pair[..pos], &pair[pos + 1..])
        } else {
            (pair, "")
        };
        // Decode percent-encoding simply (replace + and %XX)
        let key = percent_decode(k);
        let val = percent_decode(v);
        if key == "addresses[]" || key == "addresses" {
            for addr in val.split(',') {
                let a = addr.trim().to_string();
                if !a.is_empty() {
                    address_list.push(a);
                }
            }
        }
    }
    address_list
}

fn percent_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'+' {
            result.push(' ');
            i += 1;
        } else if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h1), Some(h2)) = (
                (bytes[i + 1] as char).to_digit(16),
                (bytes[i + 2] as char).to_digit(16),
            ) {
                let byte = ((h1 << 4) | h2) as u8;
                result.push(byte as char);
                i += 3;
            } else {
                result.push('%');
                i += 1;
            }
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    result
}

pub async fn get_stats(
    state: web::Data<AppState>,
    _query: web::Query<StatsQuery>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let address_list = parse_addresses_from_query(req.query_string());
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            let sql = if address_list.is_empty() {
                "SELECT owner, type, COUNT(*) FROM messages WHERE status='processed' AND owner IS NOT NULL GROUP BY owner, type".to_string()
            } else {
                let placeholders: Vec<String> = address_list
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("?{}", i + 1))
                    .collect();
                format!(
                    "SELECT owner, type, COUNT(*) FROM messages WHERE status='processed' AND owner IN ({}) GROUP BY owner, type",
                    placeholders.join(",")
                )
            };

            let mut stmt = conn.prepare(&sql)?;

            let rows: Vec<(String, String, i64)> = if address_list.is_empty() {
                stmt.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
            } else {
                let params: Vec<&dyn rusqlite::types::ToSql> =
                    address_list.iter().map(|a| a as &dyn rusqlite::types::ToSql).collect();
                stmt.query_map(params.as_slice(), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
            };

            Ok::<_, rusqlite::Error>(rows)
        })
    })
    .await
    .unwrap();

    match result {
        Ok(rows) => {
            let mut data: HashMap<String, serde_json::Map<String, serde_json::Value>> =
                HashMap::new();

            for (owner, msg_type, count) in rows {
                let entry = data.entry(owner).or_insert_with(|| {
                    let mut m = serde_json::Map::new();
                    m.insert("messages".to_string(), serde_json::json!(0i64));
                    m.insert("aggregate".to_string(), serde_json::json!(0i64));
                    m.insert("forget".to_string(), serde_json::json!(0i64));
                    m.insert("instance".to_string(), serde_json::json!(0i64));
                    m.insert("post".to_string(), serde_json::json!(0i64));
                    m.insert("program".to_string(), serde_json::json!(0i64));
                    m.insert("store".to_string(), serde_json::json!(0i64));
                    m
                });

                // Increment the total messages count
                let total = entry.get("messages").and_then(|v| v.as_i64()).unwrap_or(0) + count;
                entry.insert("messages".to_string(), serde_json::json!(total));

                // Increment the type-specific count
                let type_key = msg_type.to_lowercase();
                let type_count = entry.get(&type_key).and_then(|v| v.as_i64()).unwrap_or(0) + count;
                entry.insert(type_key, serde_json::json!(type_count));
            }

            let data_val: serde_json::Value = serde_json::Value::Object(
                data.into_iter()
                    .map(|(k, v)| (k, serde_json::Value::Object(v)))
                    .collect(),
            );

            HttpResponse::Ok().json(serde_json::json!({ "data": data_val }))
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/addresses/{address}/files  (spec 9.29)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct FilesQuery {
    pub pagination: Option<u32>,
    pub page: Option<u32>,
    pub sort_order: Option<i32>,
}

pub async fn get_files(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<FilesQuery>,
) -> impl Responder {
    let address = path.into_inner();
    let addr_clone = address.clone();
    let per_page = query.pagination.unwrap_or(100) as i64;
    let page = query.page.unwrap_or(1).max(1) as i64;
    let sort_dir = if query.sort_order.unwrap_or(-1) >= 0 {
        "ASC"
    } else {
        "DESC"
    };
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            let total: i64 = conn.query_row(
                "SELECT COUNT(*) FROM file_pins WHERE owner = ?1 AND pin_type != 'grace_period'",
                rusqlite::params![addr_clone],
                |r| r.get(0),
            )?;

            let total_size: i64 = conn.query_row(
                "SELECT COALESCE(SUM(fp.size), 0) FROM file_pins fp WHERE fp.owner = ?1 AND fp.pin_type != 'grace_period'",
                rusqlite::params![addr_clone],
                |r| r.get(0),
            )?;

            let offset = (page - 1) * per_page;
            let sql = format!(
                "SELECT fp.file_hash, fp.size, fp.pin_type, fp.created_at, fp.message_hash
                 FROM file_pins fp
                 WHERE fp.owner = ?1 AND fp.pin_type != 'grace_period'
                 ORDER BY fp.created_at {sort_dir} LIMIT ?2 OFFSET ?3"
            );

            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt
                .query_map(rusqlite::params![addr_clone, per_page, offset], |row| {
                    Ok(serde_json::json!({
                        "file_hash": row.get::<_, String>(0)?,
                        "size": row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                        "type": row.get::<_, String>(2)?,
                        "created": row.get::<_, String>(3)?,
                        "item_hash": row.get::<_, Option<String>>(4)?,
                    }))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            Ok::<_, rusqlite::Error>((rows, total, total_size))
        })
    })
    .await
    .unwrap();

    match result {
        Ok((rows, total, total_size)) => HttpResponse::Ok().json(serde_json::json!({
            "address": address,
            "total_size": total_size,
            "files": rows,
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
// GET /api/v0/addresses/{address}/post_types  (spec 9.30)
// ---------------------------------------------------------------------------

pub async fn get_post_types(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let address = path.into_inner();
    let addr_clone = address.clone();
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT post_type FROM posts WHERE address = ?1 ORDER BY post_type ASC",
            )?;
            let types = stmt
                .query_map(rusqlite::params![addr_clone], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<_, rusqlite::Error>(types)
        })
    })
    .await
    .unwrap();

    match result {
        Ok(types) => HttpResponse::Ok().json(serde_json::json!({
            "address": address,
            "post_types": types,
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/addresses/{address}/channels  (spec 9.31)
// ---------------------------------------------------------------------------

pub async fn get_channels(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let address = path.into_inner();
    let addr_clone = address.clone();
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT channel FROM messages
                 WHERE sender = ?1 AND channel IS NOT NULL
                 ORDER BY channel ASC",
            )?;
            let channels = stmt
                .query_map(rusqlite::params![addr_clone], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<_, rusqlite::Error>(channels)
        })
    })
    .await
    .unwrap();

    match result {
        Ok(channels) => HttpResponse::Ok().json(serde_json::json!({
            "address": address,
            "channels": channels,
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/channels/list.json  (spec 9.32)
// ---------------------------------------------------------------------------

pub async fn list_channels(state: web::Data<AppState>) -> impl Responder {
    let db = state.db.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT channel FROM messages WHERE channel IS NOT NULL ORDER BY channel ASC",
            )?;
            let channels = stmt
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<_, rusqlite::Error>(channels)
        })
    })
    .await
    .unwrap();

    match result {
        Ok(channels) => HttpResponse::Ok().json(serde_json::json!({
            "channels": channels,
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/authorizations/granted/{address}.json  (spec 9.33)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct AuthorizationsQuery {
    pub pagination: Option<u32>,
    pub page: Option<u32>,
}

pub async fn get_granted_authorizations(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<AuthorizationsQuery>,
) -> impl Responder {
    let address = path.into_inner();
    let addr_clone = address.clone();
    let per_page = query.pagination.unwrap_or(20) as i64;
    let page = query.page.unwrap_or(1).max(1) as i64;
    let db = state.db.clone();

    // Read the "security" aggregate for this address
    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            let row = conn.query_row(
                "SELECT content FROM aggregates WHERE address = ?1 AND key = 'security'",
                rusqlite::params![addr_clone],
                |row| row.get::<_, String>(0),
            );
            match row {
                Ok(content) => Ok(Some(content)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e),
            }
        })
    })
    .await
    .unwrap();

    match result {
        Ok(Some(content)) => {
            let parsed: serde_json::Value =
                serde_json::from_str(&content).unwrap_or(serde_json::Value::Null);

            // Security aggregate content is a map of grantee -> [auth entries]
            // Parse and return with pagination
            let empty_obj = serde_json::Map::new();
            let auth_map = parsed.as_object().unwrap_or(&empty_obj);

            let all_grantees: Vec<_> = auth_map.iter().collect();
            let total = all_grantees.len() as i64;
            let offset = ((page - 1) * per_page) as usize;
            let limit = per_page as usize;

            let mut authorizations = serde_json::Map::new();
            for (grantee, auths) in all_grantees.iter().skip(offset).take(limit) {
                authorizations.insert((*grantee).clone(), (*auths).clone());
            }

            HttpResponse::Ok().json(serde_json::json!({
                "authorizations": authorizations,
                "pagination_page": page,
                "pagination_per_page": per_page,
                "pagination_total": total,
                "pagination_item": "authorizations",
                "address": address,
            }))
        }
        Ok(None) => {
            // No security aggregate — return empty authorizations
            HttpResponse::Ok().json(serde_json::json!({
                "authorizations": {},
                "pagination_page": page,
                "pagination_per_page": per_page,
                "pagination_total": 0,
                "pagination_item": "authorizations",
                "address": address,
            }))
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/authorizations/received/{address}.json  (spec 9.34)
// ---------------------------------------------------------------------------

pub async fn get_received_authorizations(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<AuthorizationsQuery>,
) -> impl Responder {
    let address = path.into_inner();
    let addr_clone = address.clone();
    let per_page = query.pagination.unwrap_or(20) as i64;
    let page = query.page.unwrap_or(1).max(1) as i64;
    let db = state.db.clone();

    // Scan all "security" aggregates and find entries where addr appears as grantee
    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| {
            let mut stmt =
                conn.prepare("SELECT address, content FROM aggregates WHERE key = 'security'")?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<_, rusqlite::Error>(rows)
        })
    })
    .await
    .unwrap();

    match result {
        Ok(rows) => {
            // Find all (granter_address, auth_entries) where our address appears as a grantee key
            let mut received: Vec<serde_json::Value> = Vec::new();

            for (granter, content) in rows {
                let parsed: serde_json::Value =
                    serde_json::from_str(&content).unwrap_or(serde_json::Value::Null);

                if let Some(auth_map) = parsed.as_object()
                    && let Some(auths) = auth_map.get(&addr_clone)
                {
                    received.push(serde_json::json!({
                        "granter": granter,
                        "authorizations": auths,
                    }));
                }
            }

            let total = received.len() as i64;
            let offset = ((page - 1) * per_page) as usize;
            let limit = per_page as usize;
            let page_items: Vec<_> = received.into_iter().skip(offset).take(limit).collect();

            HttpResponse::Ok().json(serde_json::json!({
                "authorizations": page_items,
                "pagination_page": page,
                "pagination_per_page": per_page,
                "pagination_total": total,
                "pagination_item": "authorizations",
                "address": address,
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
    use crate::db::messages::{self, InsertMessage};
    use crate::files::FileStore;
    use actix_web::{App, test};
    use aleph_types::message::{MessageStatus, MessageType};
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

    fn insert_test_msg(
        db: &Db,
        item_hash: &str,
        msg_type: MessageType,
        sender: &str,
        owner: &str,
        channel: Option<&str>,
    ) {
        db.with_conn(|conn| {
            messages::insert_message(
                conn,
                &InsertMessage {
                    item_hash,
                    message_type: msg_type,
                    chain: "ETH",
                    sender,
                    signature: "0xsig",
                    item_type: "inline",
                    item_content: Some("{}"),
                    channel,
                    time: 1_700_000_000.0,
                    size: 100,
                    status: MessageStatus::Processed,
                    reception_time: 1_700_000_000.0,
                    owner: Some(owner),
                    content_type: None,
                    content_ref: None,
                    content_key: None,
                    content_item_hash: None,
                    payment_type: None,
                },
            )
        })
        .unwrap();
    }

    #[actix_web::test]
    async fn test_get_stats_after_messages() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        insert_test_msg(
            &state.db,
            "hash1",
            MessageType::Post,
            "0xsender1",
            "0xowner1",
            None,
        );
        insert_test_msg(
            &state.db,
            "hash2",
            MessageType::Post,
            "0xsender1",
            "0xowner1",
            None,
        );
        insert_test_msg(
            &state.db,
            "hash3",
            MessageType::Aggregate,
            "0xsender1",
            "0xowner1",
            None,
        );

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/addresses/stats.json")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        let data = &body["data"];
        let owner_stats = &data["0xowner1"];
        assert_eq!(owner_stats["messages"], 3);
        assert_eq!(owner_stats["post"], 2);
        assert_eq!(owner_stats["aggregate"], 1);
    }

    #[actix_web::test]
    async fn test_list_channels() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        insert_test_msg(
            &state.db,
            "h1",
            MessageType::Post,
            "0xsender",
            "0xowner",
            Some("chan-a"),
        );
        insert_test_msg(
            &state.db,
            "h2",
            MessageType::Post,
            "0xsender",
            "0xowner",
            Some("chan-b"),
        );
        insert_test_msg(
            &state.db,
            "h3",
            MessageType::Post,
            "0xsender",
            "0xowner",
            Some("chan-a"),
        ); // dup

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/channels/list.json")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        let channels = body["channels"].as_array().unwrap();
        assert_eq!(channels.len(), 2);
        // Should be distinct and sorted
        assert!(channels.contains(&serde_json::json!("chan-a")));
        assert!(channels.contains(&serde_json::json!("chan-b")));
    }

    #[actix_web::test]
    async fn test_get_address_channels() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        insert_test_msg(
            &state.db,
            "h1",
            MessageType::Post,
            "0xalice",
            "0xalice",
            Some("my-app"),
        );
        insert_test_msg(
            &state.db,
            "h2",
            MessageType::Post,
            "0xalice",
            "0xalice",
            Some("test"),
        );
        // Bob should not appear
        insert_test_msg(
            &state.db,
            "h3",
            MessageType::Post,
            "0xbob",
            "0xbob",
            Some("other"),
        );

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/addresses/0xalice/channels")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["address"], "0xalice");
        let channels = body["channels"].as_array().unwrap();
        assert_eq!(channels.len(), 2);
        assert!(channels.contains(&serde_json::json!("my-app")));
        assert!(channels.contains(&serde_json::json!("test")));
    }

    #[actix_web::test]
    async fn test_get_granted_authorizations_no_security_aggregate() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/api/v0/authorizations/granted/0xnoauth.json")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["pagination_total"], 0);
        assert_eq!(body["address"], "0xnoauth");
        assert!(body["authorizations"].as_object().unwrap().is_empty());
    }
}
