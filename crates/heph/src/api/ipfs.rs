//! Stub /api/v0/ipfs/add_car handler.
//!
//! Validates the wire contract pyaleph implements (multipart shape,
//! signature, CAR header, root match against metadata.item_hash) but does
//! NOT contact a real IPFS daemon. Used for end-to-end SDK / CLI tests in
//! CI without depending on pyaleph or a kubo container. Full heph IPFS
//! support is a separate body of work.

use actix_multipart::Multipart;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use aleph_sdk::car::{InvalidCarFile, read_carv1_root};
use futures_util::StreamExt;
use std::io::Write;

use crate::api::AppState;
use crate::api::storage::StorageMetadata;

pub(crate) const MAX_UPLOAD_CAR_SIZE: usize = 4 * 1024 * 1024 * 1024; // 4 GiB

pub async fn add_car(
    _state: web::Data<AppState>,
    req: HttpRequest,
    payload: web::Payload,
) -> impl Responder {
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if !content_type.starts_with("multipart/form-data") {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Expected Content-Type: multipart/form-data"
        }));
    }

    let mut multipart = Multipart::new(req.headers(), payload);
    let mut tmp_car: Option<tempfile::NamedTempFile> = None;
    let mut car_byte_count: usize = 0;
    let mut metadata_str: Option<String> = None;

    while let Some(item) = multipart.next().await {
        let mut field = match item {
            Ok(f) => f,
            Err(_) => continue,
        };
        let name = field
            .content_disposition()
            .and_then(|cd| cd.get_name())
            .unwrap_or("")
            .to_string();
        match name.as_str() {
            "file" => {
                let mut t = match tempfile::NamedTempFile::new() {
                    Ok(t) => t,
                    Err(e) => {
                        return HttpResponse::InternalServerError().json(serde_json::json!({
                            "error": format!("tempfile: {e}")
                        }));
                    }
                };
                while let Some(chunk) = field.next().await {
                    let bytes = match chunk {
                        Ok(b) => b,
                        Err(e) => {
                            return HttpResponse::BadRequest().json(serde_json::json!({
                                "error": e.to_string()
                            }));
                        }
                    };
                    car_byte_count += bytes.len();
                    if car_byte_count > MAX_UPLOAD_CAR_SIZE {
                        return HttpResponse::PayloadTooLarge().json(serde_json::json!({
                            "error": "File too large"
                        }));
                    }
                    if let Err(e) = t.write_all(&bytes) {
                        return HttpResponse::InternalServerError().json(serde_json::json!({
                            "error": format!("tempfile write: {e}")
                        }));
                    }
                }
                tmp_car = Some(t);
            }
            "metadata" => {
                let mut buf = Vec::new();
                while let Some(chunk) = field.next().await {
                    match chunk {
                        Ok(b) => buf.extend_from_slice(&b),
                        Err(_) => break,
                    }
                }
                metadata_str = String::from_utf8(buf).ok();
            }
            _ => {}
        }
    }

    let tmp_car = match tmp_car {
        Some(t) => t,
        None => {
            return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "error": "Missing 'file' in multipart form"
            }));
        }
    };
    let meta_str = match metadata_str {
        Some(s) => s,
        None => {
            return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "error": "metadata is required for CAR upload"
            }));
        }
    };

    let meta: StorageMetadata = match serde_json::from_str(&meta_str) {
        Ok(m) => m,
        Err(e) => {
            return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "error": format!("Could not decode metadata: {e}")
            }));
        }
    };
    let msg = &meta.message;

    if msg.message_type != aleph_types::message::MessageType::Store {
        return HttpResponse::UnprocessableEntity().json(serde_json::json!({
            "error": "metadata message must be a STORE message"
        }));
    }

    if crate::handlers::verify_signature(msg).is_err() {
        return HttpResponse::Forbidden().json(serde_json::json!({
            "error": "invalid signature"
        }));
    }

    let item_content_str = match &msg.item_content {
        Some(s) => s.clone(),
        None => {
            return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "error": "Store message content needed"
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
                "error": format!("Invalid store message content: {e}")
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

    match store_content.file_hash() {
        aleph_types::item_hash::ItemHash::Ipfs(_) => {}
        other => {
            return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "error": format!(
                    "Expected item_type=ipfs in STORE message, got {}",
                    match other {
                        aleph_types::item_hash::ItemHash::Native(_) => "storage",
                        aleph_types::item_hash::ItemHash::Ipfs(_) => unreachable!(),
                    }
                )
            }));
        }
    }

    let metadata_item_hash = store_content.file_hash().to_string();

    let car_path = tmp_car.path().to_path_buf();
    let car_root = match read_carv1_root(&car_path) {
        Ok(r) => r,
        Err(InvalidCarFile::Io(e)) => {
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("read CAR: {e}")
            }));
        }
        Err(e) => {
            return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "error": format!("Invalid CAR file: {e}")
            }));
        }
    };

    if car_root != metadata_item_hash {
        return HttpResponse::UnprocessableEntity().json(serde_json::json!({
            "error": format!(
                "Root CID does not match ({} != {})",
                car_root, metadata_item_hash
            )
        }));
    }

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "hash": car_root,
        "size": car_byte_count,
    }))
}

#[cfg(test)]
mod tests {
    use crate::api::{AppState, configure_routes};
    use crate::config::HephConfig;
    use crate::db::Db;
    use crate::files::FileStore;
    use actix_web::{App, test, web};
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
    async fn add_car_rejects_wrong_content_type() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let state = make_test_state(&tmpdir);
        let app = test::init_service(App::new().app_data(state).configure(configure_routes)).await;
        let req = test::TestRequest::post()
            .uri("/api/v0/ipfs/add_car")
            .insert_header(("content-type", "application/octet-stream"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 400);
    }
}
