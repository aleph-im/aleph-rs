use actix_multipart::Multipart;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json;
use sha2::{Digest, Sha256};

use crate::api::AppState;
use crate::db::files;
use crate::db::messages;
use crate::handlers::IncomingMessage;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Metadata submitted alongside a file upload for authenticated storage.
#[derive(Debug, Deserialize, Serialize)]
pub struct StorageMetadata {
    pub message: IncomingMessage,
    #[serde(default)]
    pub sync: bool,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn is_valid_hex(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_hexdigit())
}

// ---------------------------------------------------------------------------
// GET /api/v0/storage/raw/{hash}   (also HEAD)   spec 9.10
// ---------------------------------------------------------------------------

pub async fn get_raw(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let hash = path.into_inner();

    if !is_valid_hex(&hash) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Invalid file hash"
        }));
    }

    let file_store = state.file_store.clone();
    if !file_store.exists(&hash) {
        return HttpResponse::NotFound().json(serde_json::json!({
            "error": "File not found"
        }));
    }

    match file_store.read(&hash) {
        Ok(data) => HttpResponse::Ok()
            .content_type("application/octet-stream")
            .body(data),
        Err(_) => HttpResponse::NotFound().json(serde_json::json!({
            "error": "File not found"
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/storage/{hash}   spec 9.11
// ---------------------------------------------------------------------------

pub async fn get_base64(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let hash = path.into_inner();

    if !is_valid_hex(&hash) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Invalid file hash"
        }));
    }

    let file_store = state.file_store.clone();
    match file_store.read(&hash) {
        Ok(data) => {
            let encoded = BASE64.encode(&data);
            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "hash": hash,
                "engine": "storage",
                "content": encoded
            }))
        }
        Err(_) => HttpResponse::NotFound().json(serde_json::json!({
            "error": "File not found"
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/storage/metadata/{hash}   spec 9.12
// ---------------------------------------------------------------------------

pub async fn get_metadata(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let hash = path.into_inner();

    if !is_valid_hex(&hash) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Invalid file hash"
        }));
    }

    let db = state.db.clone();
    let hash2 = hash.clone();

    let result =
        tokio::task::spawn_blocking(move || db.with_conn(|conn| files::get_file(conn, &hash2)))
            .await
            .unwrap();

    match result {
        Ok(Some(rec)) => {
            let size = state.file_store.size(&rec.hash).unwrap_or(0) as i64;
            let download_url = format!("/api/v0/storage/raw/{}", rec.hash);
            HttpResponse::Ok().json(serde_json::json!({
                "file_hash": rec.hash,
                "type": rec.file_type,
                "size": size,
                "download_url": download_url
            }))
        }
        Ok(None) => HttpResponse::NotFound().json(serde_json::json!({
            "error": "File not found"
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v0/storage/by-message-hash/{hash}   spec 9.13
// ---------------------------------------------------------------------------

pub async fn get_by_message_hash(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let msg_hash = path.into_inner();

    if !is_valid_hex(&msg_hash) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Invalid message hash"
        }));
    }

    let db = state.db.clone();
    let msg_hash2 = msg_hash.clone();

    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| messages::get_message_by_hash(conn, &msg_hash2))
    })
    .await
    .unwrap();

    match result {
        Ok(Some(msg)) => {
            // Must be a STORE message.
            if msg.message_type.to_uppercase() != "STORE" {
                return HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Message is not a STORE message"
                }));
            }

            let file_hash = match msg.content_item_hash {
                Some(ref fh) => fh.clone(),
                None => {
                    return HttpResponse::NotFound().json(serde_json::json!({
                        "error": "STORE message has no file hash"
                    }));
                }
            };

            let owner = msg.owner.unwrap_or_default();
            let ref_ = msg.content_ref.unwrap_or_default();
            let download_url = format!("/api/v0/storage/raw/{file_hash}");

            let size = state.file_store.size(&file_hash).unwrap_or(0) as i64;

            HttpResponse::Ok().json(serde_json::json!({
                "ref": ref_,
                "owner": owner,
                "file_hash": file_hash,
                "download_url": download_url,
                "size": size
            }))
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
// Shared helper: look up a file tag and return the metadata response
// ---------------------------------------------------------------------------

async fn file_info_by_tag(state: &web::Data<AppState>, tag: &str) -> HttpResponse {
    let db = state.db.clone();
    let tag_owned = tag.to_string();

    let tag_result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| files::get_file_tag(conn, &tag_owned))
    })
    .await
    .unwrap();

    let tag_rec = match tag_result {
        Ok(Some(t)) => t,
        Ok(None) => {
            return HttpResponse::NotFound().json(serde_json::json!({
                "error": "File tag not found"
            }));
        }
        Err(e) => {
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": e.to_string()
            }));
        }
    };

    let file_hash = tag_rec.file_hash.clone();
    let owner = tag_rec.owner.clone();

    let size = state.file_store.size(&file_hash).unwrap_or(0) as i64;

    let download_url = format!("/api/v0/storage/raw/{file_hash}");

    HttpResponse::Ok().json(serde_json::json!({
        "ref": tag_rec.tag,
        "owner": owner,
        "file_hash": file_hash,
        "download_url": download_url,
        "size": size
    }))
}

// ---------------------------------------------------------------------------
// GET /api/v0/storage/by-ref/{ref_}   spec 9.14
// ---------------------------------------------------------------------------

pub async fn get_by_ref(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let ref_ = path.into_inner();
    file_info_by_tag(&state, &ref_).await
}

// ---------------------------------------------------------------------------
// GET /api/v0/storage/by-ref/{address}/{ref_}   spec 9.14
// ---------------------------------------------------------------------------

pub async fn get_by_ref_with_address(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (address, ref_) = path.into_inner();
    let tag = format!("{}:{}", address.to_lowercase(), ref_);
    file_info_by_tag(&state, &tag).await
}

// ---------------------------------------------------------------------------
// GET /api/v0/storage/count/{hash}   spec 9.15
// ---------------------------------------------------------------------------

pub async fn get_pin_count(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let hash = path.into_inner();

    if !is_valid_hex(&hash) {
        return HttpResponse::BadRequest().body("Invalid file hash");
    }

    let db = state.db.clone();
    let result = tokio::task::spawn_blocking(move || {
        db.with_conn(|conn| files::count_active_pins(conn, &hash))
    })
    .await
    .unwrap();

    match result {
        Ok(count) => HttpResponse::Ok().body(count.to_string()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// POST /api/v0/storage/add_file   spec 9.16
//
// Actix-web dispatches based on extractors. We use a single handler that
// inspects the Content-Type header and handles both multipart and raw binary.
// For multipart, we take `Multipart` as an extractor directly.
// ---------------------------------------------------------------------------

const MAX_SIZE_NO_META: usize = 25 * 1024 * 1024; // 25 MiB
const MAX_SIZE_WITH_META: usize = 100 * 1024 * 1024; // 100 MiB

pub async fn add_file(
    state: web::Data<AppState>,
    req: HttpRequest,
    payload: web::Payload,
) -> impl Responder {
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if content_type.starts_with("multipart/form-data") {
        let multipart = Multipart::new(req.headers(), payload);
        return add_file_multipart(state, multipart).await;
    }

    // application/octet-stream or similar — raw binary body
    let mut data = Vec::new();
    let mut stream = payload.into_inner();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                data.extend_from_slice(&bytes);
                if data.len() > MAX_SIZE_NO_META {
                    return HttpResponse::PayloadTooLarge().json(serde_json::json!({
                        "error": "File too large (max 25 MiB without metadata)"
                    }));
                }
            }
            Err(e) => {
                return HttpResponse::BadRequest().json(serde_json::json!({
                    "error": e.to_string()
                }));
            }
        }
    }

    let file_store = state.file_store.clone();
    match tokio::task::spawn_blocking(move || file_store.write(&data))
        .await
        .unwrap()
    {
        Ok(hash) => HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "hash": hash
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string()
        })),
    }
}

async fn add_file_multipart(state: web::Data<AppState>, mut multipart: Multipart) -> HttpResponse {
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut metadata_str: Option<String> = None;

    while let Some(item) = multipart.next().await {
        let mut field = match item {
            Ok(f) => f,
            Err(_) => continue,
        };

        let field_name = field
            .content_disposition()
            .and_then(|cd| cd.get_name())
            .unwrap_or("")
            .to_string();

        let mut buf = Vec::new();
        while let Some(chunk) = field.next().await {
            match chunk {
                Ok(bytes) => buf.extend_from_slice(&bytes),
                Err(_) => break,
            }
        }

        match field_name.as_str() {
            "file" => file_bytes = Some(buf),
            "metadata" => {
                metadata_str = String::from_utf8(buf).ok();
            }
            _ => {}
        }
    }

    let has_meta = metadata_str.is_some();
    let max_size = if has_meta {
        MAX_SIZE_WITH_META
    } else {
        MAX_SIZE_NO_META
    };

    let data = match file_bytes {
        Some(b) => b,
        None => {
            return HttpResponse::BadRequest().json(serde_json::json!({
                "error": "No file field in multipart body"
            }));
        }
    };

    if data.len() > max_size {
        return HttpResponse::PayloadTooLarge().json(serde_json::json!({
            "error": "File too large"
        }));
    }

    if let Some(meta_str) = metadata_str {
        // ── Authenticated path ──────────────────────────────────────────
        // (a) Deserialize metadata.
        let meta: StorageMetadata = match serde_json::from_str(&meta_str) {
            Ok(m) => m,
            Err(e) => {
                return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                    "error": format!("could not decode metadata: {e}")
                }));
            }
        };

        let msg = &meta.message;

        // (b) Validate message_type == STORE and item_type == Inline.
        if msg.message_type != aleph_types::message::MessageType::Store {
            return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "error": "metadata message must be a STORE message"
            }));
        }
        if msg.item_type != aleph_types::message::item_type::ItemType::Inline {
            return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "error": "metadata message must have inline item_type"
            }));
        }

        // (c) Verify signature.
        if let Err(_e) = crate::handlers::verify_signature(msg) {
            return HttpResponse::Forbidden().json(serde_json::json!({
                "error": "invalid signature"
            }));
        }

        // (d) Parse item_content as StoreContent.
        let item_content_str = match &msg.item_content {
            Some(s) => s.clone(),
            None => {
                return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                    "error": "store message content needed"
                }));
            }
        };

        let content = match aleph_types::message::MessageContent::deserialize_with_type(
            aleph_types::message::MessageType::Store,
            item_content_str.as_bytes(),
        ) {
            Ok(c) => c,
            Err(e) => {
                return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                    "error": format!("invalid store message content: {e}")
                }));
            }
        };

        let store_content = match &content.content {
            aleph_types::message::MessageContentEnum::Store(s) => s,
            _ => {
                return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                    "error": "content is not a STORE message"
                }));
            }
        };

        // (e) Compute SHA-256 of uploaded file bytes, check it matches file_hash.
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let computed_hash = hex::encode(hasher.finalize());
        let expected_hash = store_content.file_hash().to_string();

        if computed_hash != expected_hash {
            return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "error": format!(
                    "file hash does not match ({computed_hash} != {expected_hash})"
                )
            }));
        }

        // (f) Check balance.
        let db_balance = state.db.clone();
        let msg_balance = msg.clone();
        let content_balance = content.clone();
        let balance_result = tokio::task::spawn_blocking(move || {
            crate::handlers::check_balance_public(&db_balance, &msg_balance, &content_balance)
        })
        .await
        .unwrap();

        if let Err(_e) = balance_result {
            return HttpResponse::PaymentRequired().json(serde_json::json!({
                "error": "insufficient balance"
            }));
        }

        // (g) Store file.
        let file_store = state.file_store.clone();
        let hash = match tokio::task::spawn_blocking(move || file_store.write(&data))
            .await
            .unwrap()
        {
            Ok(h) => h,
            Err(e) => {
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": e.to_string()
                }));
            }
        };

        // (h) Process the STORE message (insert message + file pin + cost records).
        let db = state.db.clone();
        let fs = state.file_store.clone();
        let msg_clone = msg.clone();
        let result = tokio::task::spawn_blocking(move || {
            crate::handlers::process_message_with_store(&db, &msg_clone, Some(&fs))
        })
        .await
        .unwrap();

        if let Err(e) = result {
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("message processing failed: {e}")
            }));
        }

        // (i) Return success.
        HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "hash": hash
        }))
    } else {
        // ── Unauthenticated path ────────────────────────────────────────
        let file_store = state.file_store.clone();
        let hash = match tokio::task::spawn_blocking(move || file_store.write(&data))
            .await
            .unwrap()
        {
            Ok(h) => h,
            Err(e) => {
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": e.to_string()
                }));
            }
        };

        HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "hash": hash
        }))
    }
}

// ---------------------------------------------------------------------------
// POST /api/v0/storage/add_json   spec 9.17
// ---------------------------------------------------------------------------

pub async fn add_json(state: web::Data<AppState>, body: web::Bytes) -> impl Responder {
    // Validate it's parseable JSON.
    if serde_json::from_slice::<serde_json::Value>(&body).is_err() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Invalid JSON body"
        }));
    }

    let data = body.to_vec();
    let file_store = state.file_store.clone();

    match tokio::task::spawn_blocking(move || file_store.write(&data))
        .await
        .unwrap()
    {
        Ok(hash) => HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "hash": hash
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
            corechannel: std::sync::Mutex::new(crate::corechannel::CoreChannelState::new()),
        })
    }

    // -----------------------------------------------------------------------
    // Test 1: Upload via add_json, download via raw — round trip
    // -----------------------------------------------------------------------

    #[actix_web::test]
    async fn test_add_json_and_download_raw() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let json_body = serde_json::json!({"hello": "world", "number": 42});
        let json_bytes = serde_json::to_vec(&json_body).unwrap();

        // POST /api/v0/storage/add_json
        let req = test::TestRequest::post()
            .uri("/api/v0/storage/add_json")
            .set_payload(json_bytes.clone())
            .insert_header(("content-type", "application/json"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200, "add_json should return 200");

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "success");
        let hash = body["hash"].as_str().unwrap().to_string();
        assert_eq!(hash.len(), 64, "hash should be 64 hex chars");

        // GET /api/v0/storage/raw/{hash}
        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/storage/raw/{hash}"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let raw = test::read_body(resp).await;
        assert_eq!(
            raw.to_vec(),
            json_bytes,
            "downloaded bytes should match uploaded bytes"
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: Get metadata for uploaded file
    // -----------------------------------------------------------------------

    #[actix_web::test]
    async fn test_get_metadata_for_uploaded_file() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let json_body = b"{\"key\":\"value\"}";

        // Upload
        let req = test::TestRequest::post()
            .uri("/api/v0/storage/add_json")
            .set_payload(json_body.as_ref())
            .insert_header(("content-type", "application/json"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        let body: serde_json::Value = test::read_body_json(resp).await;
        let hash = body["hash"].as_str().unwrap().to_string();

        // Insert into DB files table so metadata works
        state
            .db
            .with_conn(|conn| {
                crate::db::files::upsert_file(conn, &hash, json_body.len() as i64, "file")
            })
            .unwrap();

        // GET metadata
        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/storage/metadata/{hash}"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["file_hash"], hash);
        assert_eq!(body["size"], json_body.len() as i64);
        assert_eq!(body["type"], "file");
        assert!(body["download_url"].as_str().unwrap().contains(&hash));
    }

    // -----------------------------------------------------------------------
    // Test 3: Get base64 encoding
    // -----------------------------------------------------------------------

    #[actix_web::test]
    async fn test_get_base64_encoding() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let content = b"test base64 content";

        // Upload raw
        let req = test::TestRequest::post()
            .uri("/api/v0/storage/add_file")
            .set_payload(content.as_ref())
            .insert_header(("content-type", "application/octet-stream"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = test::read_body_json(resp).await;
        let hash = body["hash"].as_str().unwrap().to_string();

        // GET base64
        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/storage/{hash}"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "success");
        assert_eq!(body["hash"], hash);
        assert_eq!(body["engine"], "storage");

        let decoded = BASE64
            .decode(body["content"].as_str().unwrap())
            .expect("content should be valid base64");
        assert_eq!(decoded, content);
    }

    // -----------------------------------------------------------------------
    // Test 4: Pin count returns correct number
    // -----------------------------------------------------------------------

    #[actix_web::test]
    async fn test_pin_count() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let hash = "abcd1234".repeat(8); // 64 hex chars
        // Insert file and a couple of pins.
        state.db.with_conn(|conn| {
            crate::db::files::upsert_file(conn, &hash, 64, "file").unwrap();
            crate::db::files::insert_file_pin(
                conn,
                &crate::db::files::InsertFilePin {
                    file_hash: &hash,
                    owner: "0xOwner",
                    pin_type: "message",
                    message_hash: Some("mh1"),
                    size: None,
                    content_type: None,
                    ref_: None,
                },
            )
            .unwrap();
            crate::db::files::insert_file_pin(
                conn,
                &crate::db::files::InsertFilePin {
                    file_hash: &hash,
                    owner: "0xOwner2",
                    pin_type: "message",
                    message_hash: Some("mh2"),
                    size: None,
                    content_type: None,
                    ref_: None,
                },
            )
            .unwrap();
        });

        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/storage/count/{hash}"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);

        let body = test::read_body(resp).await;
        let count: i64 = std::str::from_utf8(&body).unwrap().trim().parse().unwrap();
        assert_eq!(count, 2);
    }

    // -----------------------------------------------------------------------
    // Test 5: 404 for non-existent file
    // -----------------------------------------------------------------------

    #[actix_web::test]
    async fn test_404_for_nonexistent_file() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let nonexistent = "0".repeat(64);

        // raw
        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/storage/raw/{nonexistent}"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 404);

        // base64
        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/storage/{nonexistent}"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 404);

        // metadata
        let req = test::TestRequest::get()
            .uri(&format!("/api/v0/storage/metadata/{nonexistent}"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 404);
    }

    // -----------------------------------------------------------------------
    // Authenticated upload tests
    // -----------------------------------------------------------------------

    use aleph_types::account::{Account, EvmAccount, sign_message};
    use aleph_types::chain::Chain;
    use aleph_types::item_hash::{AlephItemHash, ItemHash};
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use aleph_types::message::unsigned::UnsignedMessage;
    use aleph_types::timestamp::Timestamp;
    use sha2::{Digest, Sha256};

    /// Build a valid multipart body with `file` and optional `metadata` fields.
    fn build_multipart_body(file_bytes: &[u8], metadata: Option<&str>) -> (Vec<u8>, String) {
        let boundary = "----TestBoundary12345";
        let mut body = Vec::new();

        // File field.
        body.extend_from_slice(
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"test.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n").as_bytes()
        );
        body.extend_from_slice(file_bytes);
        body.extend_from_slice(b"\r\n");

        // Metadata field.
        if let Some(meta) = metadata {
            body.extend_from_slice(
                format!("--{boundary}\r\nContent-Disposition: form-data; name=\"metadata\"\r\nContent-Type: application/json\r\n\r\n").as_bytes()
            );
            body.extend_from_slice(meta.as_bytes());
            body.extend_from_slice(b"\r\n");
        }

        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

        let content_type = format!("multipart/form-data; boundary={boundary}");
        (body, content_type)
    }

    /// Build a signed STORE metadata JSON string for authenticated upload.
    fn build_store_metadata(key: &[u8; 32], file_hash: &str) -> String {
        build_store_metadata_with_size(key, file_hash, None).0
    }

    /// Build a signed STORE metadata JSON string, optionally including a `size` field.
    /// Returns (metadata_json, sender_address).
    fn build_store_metadata_with_size(
        key: &[u8; 32],
        file_hash: &str,
        size: Option<u64>,
    ) -> (String, String) {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let addr_str = account.address().as_str().to_string();

        let item_content = if let Some(sz) = size {
            format!(
                r#"{{"address":"{}","time":1700000000.0,"item_type":"storage","item_hash":"{}","size":{}}}"#,
                addr_str, file_hash, sz
            )
        } else {
            format!(
                r#"{{"address":"{}","time":1700000000.0,"item_type":"storage","item_hash":"{}"}}"#,
                addr_str, file_hash
            )
        };
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));

        let unsigned = UnsignedMessage {
            message_type: MessageType::Store,
            item_type: ItemType::Inline,
            item_content: item_content.clone(),
            item_hash: item_hash.clone(),
            time: Timestamp::from(1_700_000_000.0),
            channel: None,
        };

        let pending = sign_message(&account, unsigned).unwrap();

        // Serialize the PendingMessage and wrap in StorageMetadata.
        let msg_json = serde_json::to_value(&pending).unwrap();
        let meta = serde_json::json!({
            "message": msg_json,
            "sync": false
        });
        (serde_json::to_string(&meta).unwrap(), addr_str)
    }

    #[actix_web::test]
    async fn test_authenticated_upload_succeeds() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let file_data = b"hello authenticated upload";
        let mut hasher = Sha256::new();
        hasher.update(file_data);
        let file_hash = hex::encode(hasher.finalize());

        let metadata = build_store_metadata(&[10u8; 32], &file_hash);
        let (body, content_type) = build_multipart_body(file_data, Some(&metadata));

        let req = test::TestRequest::post()
            .uri("/api/v0/storage/add_file")
            .insert_header(("content-type", content_type))
            .set_payload(body)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200, "authenticated upload should return 200");

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "success");
        assert_eq!(body["hash"], file_hash);

        // Verify the file was stored.
        assert!(state.file_store.exists(&file_hash));
    }

    #[actix_web::test]
    async fn test_authenticated_upload_bad_signature_returns_403() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let file_data = b"bad signature test";
        let mut hasher = Sha256::new();
        hasher.update(file_data);
        let file_hash = hex::encode(hasher.finalize());

        let metadata_str = build_store_metadata(&[11u8; 32], &file_hash);
        // Corrupt the signature by replacing it.
        let mut meta_val: serde_json::Value = serde_json::from_str(&metadata_str).unwrap();
        meta_val["message"]["signature"] = serde_json::json!(
            "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef00"
        );
        let corrupted_metadata = serde_json::to_string(&meta_val).unwrap();

        let (body, content_type) = build_multipart_body(file_data, Some(&corrupted_metadata));

        let req = test::TestRequest::post()
            .uri("/api/v0/storage/add_file")
            .insert_header(("content-type", content_type))
            .set_payload(body)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            403,
            "bad signature should return 403 Forbidden"
        );

        // File should NOT be stored.
        assert!(
            !state.file_store.exists(&file_hash),
            "file should not be stored when signature is invalid"
        );
    }

    #[actix_web::test]
    async fn test_authenticated_upload_hash_mismatch_returns_422() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let file_data = b"hash mismatch test";
        let mut hasher = Sha256::new();
        hasher.update(file_data);
        let actual_file_hash = hex::encode(hasher.finalize());

        // Use a wrong file hash in the metadata.
        let wrong_file_hash = "0".repeat(64);
        let metadata = build_store_metadata(&[12u8; 32], &wrong_file_hash);
        let (body, content_type) = build_multipart_body(file_data, Some(&metadata));

        let req = test::TestRequest::post()
            .uri("/api/v0/storage/add_file")
            .insert_header(("content-type", content_type))
            .set_payload(body)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            422,
            "hash mismatch should return 422 Unprocessable Entity"
        );

        // File should NOT be stored.
        assert!(
            !state.file_store.exists(&actual_file_hash),
            "file should not be stored when hash does not match"
        );
    }

    #[actix_web::test]
    async fn test_authenticated_upload_insufficient_balance_returns_402() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        // Create a non-empty file so storage cost is non-zero.
        let file_data = vec![0u8; 1024 * 1024]; // 1 MiB
        let mut hasher = Sha256::new();
        hasher.update(&file_data);
        let file_hash = hex::encode(hasher.finalize());

        // Build metadata WITH a size field so cost is non-zero.
        let file_size = file_data.len() as u64;
        let (metadata, sender_addr) =
            build_store_metadata_with_size(&[13u8; 32], &file_hash, Some(file_size));

        // Pre-seed the sender's credit balance to 0 so the check fails.
        state
            .db
            .with_conn(|conn| crate::db::balances::set_credit_balance(conn, &sender_addr, 0))
            .unwrap();

        let (body, content_type) = build_multipart_body(&file_data, Some(&metadata));

        let req = test::TestRequest::post()
            .uri("/api/v0/storage/add_file")
            .insert_header(("content-type", content_type))
            .set_payload(body)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            402,
            "insufficient balance should return 402 Payment Required"
        );

        // File should NOT be stored.
        assert!(
            !state.file_store.exists(&file_hash),
            "file should not be stored when balance is insufficient"
        );
    }
}
