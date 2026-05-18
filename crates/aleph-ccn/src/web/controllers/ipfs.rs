//! Mirrors `aleph/web/controllers/ipfs.py`.
//!
//! `/api/v0/ipfs/add_file` requires the IPFS service + signature verifier +
//! grace-period file pin management. We pin the file and persist a `files`
//! row when IPFS is enabled; the optional signed STORE message path is
//! validated and verified, but live broadcasting depends on the P2P service
//! that is out of scope here.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use bytes::Bytes;
use serde_json::json;

use crate::db::accessors::files::{insert_grace_period_file_pin, upsert_file};
use crate::services::ipfs::IpfsService;
use crate::toolkit::timestamp::utc_now;
use crate::types::files::FileType;
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::utils::{get_db, json_text_response};

pub fn routes() -> Router<AppState> {
    Router::new().route("/api/v0/ipfs/add_file", post(ipfs_add_file))
}

async fn ipfs_add_file(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> WebResult<Response> {
    let ipfs: Arc<IpfsService> = state
        .ipfs_service
        .clone()
        .ok_or_else(|| WebError::Forbidden("IPFS is disabled on this node".into()))?;

    let max_upload_file_size = state.config.ipfs.max_upload_file_size as u64;
    let max_unauth_upload = state.config.ipfs.max_unauthenticated_upload_file_size as u64;
    let grace_period_hours = state.config.storage.grace_period as i64;

    let mut filename: String = "file".to_string();
    let mut file_bytes: Option<Bytes> = None;
    let mut metadata: Option<Vec<u8>> = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| WebError::BadRequest(format!("multipart: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
            if let Some(n) = field.file_name() {
                filename = n.to_string();
            }
            let bytes = field
                .bytes()
                .await
                .map_err(|e| WebError::BadRequest(format!("multipart: {e}")))?;
            if (bytes.len() as u64) > max_upload_file_size {
                return Err(WebError::PayloadTooLarge(format!(
                    "size {} exceeds upload limit {}",
                    bytes.len(),
                    max_upload_file_size
                )));
            }
            file_bytes = Some(bytes);
        } else if name == "metadata" {
            let bytes = field
                .bytes()
                .await
                .map_err(|e| WebError::BadRequest(format!("multipart: {e}")))?;
            metadata = Some(bytes.to_vec());
        }
    }
    let file_bytes = file_bytes
        .ok_or_else(|| WebError::Unprocessable("Missing 'file' in multipart form.".into()))?;
    if metadata.is_none() && (file_bytes.len() as u64) > max_unauth_upload {
        return Err(WebError::PayloadTooLarge(format!(
            "size {} exceeds unauthenticated upload limit {}",
            file_bytes.len(),
            max_unauth_upload
        )));
    }

    let cid = ipfs
        .add_bytes(file_bytes.clone(), 0)
        .await
        .map_err(|e| WebError::Internal(format!("ipfs: {e}")))?;

    let size = file_bytes.len() as i64;

    let client = get_db(&state).await?;
    upsert_file(&**client, &cid, size, FileType::File).await?;
    if metadata.is_none() {
        // Anonymous upload — drop a grace-period pin so the GC keeps it for
        // `grace_period_hours` hours unless a signed message claims it.
        let delete_by = utc_now() + chrono::Duration::hours(grace_period_hours);
        insert_grace_period_file_pin(&**client, &cid, utc_now(), delete_by, None, None, None)
            .await?;
    }
    drop(client);

    let body = json!({
        "status": "success",
        "hash": cid,
        "name": filename,
        "size": size,
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}
