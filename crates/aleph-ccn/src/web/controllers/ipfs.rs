//! Mirrors `aleph/web/controllers/ipfs.py`.
//!
//! `/api/v0/ipfs/add_file` requires the IPFS service + signature verifier +
//! grace-period file pin management. We pin the file and persist a `files`
//! row when IPFS is enabled; the optional signed STORE message path is
//! validated, verified, and queued for normal pending-message processing.

use std::sync::Arc;
use std::time::Duration;
use std::{path::PathBuf, process};

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::extract::multipart::Field;
use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use aleph_types::message::item_type::ItemType;
use bytes::Bytes;
use serde_json::json;
use tokio::io::AsyncWriteExt;

use crate::db::accessors::files::{insert_grace_period_file_pin, upsert_file};
use crate::services::ipfs::car::read_carv1_root_from_path;
use crate::services::ipfs::IpfsService;
use crate::toolkit::timestamp::utc_now;
use crate::types::files::FileType;
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::storage::{StoreMetadata, verify_store_metadata};
use crate::web::controllers::utils::{broadcast_and_process_message, get_db, json_text_response};

pub fn routes(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/api/v0/ipfs/add_file", post(ipfs_add_file))
        .route(
            "/api/v0/ipfs/add_car",
            post(ipfs_add_car).layer(DefaultBodyLimit::max(car_body_limit(&state))),
        )
}

fn car_body_limit(state: &AppState) -> usize {
    const MULTIPART_METADATA_HEADROOM: u64 = 1024 * 1024;
    let configured = state
        .config
        .ipfs
        .max_upload_car_size
        .saturating_add(MULTIPART_METADATA_HEADROOM);
    usize::try_from(configured).unwrap_or(usize::MAX)
}

struct TempUpload {
    path: PathBuf,
    size: u64,
}

impl Drop for TempUpload {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

async fn create_upload_temp_file(extension: &str) -> WebResult<(PathBuf, tokio::fs::File)> {
    let pid = process::id();
    for _ in 0..16 {
        let nonce = rand::random::<u64>();
        let path = std::env::temp_dir().join(format!("aleph-ccn-upload-{pid}-{nonce}{extension}"));
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => return Ok((path, file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(WebError::Internal(format!("temp upload file: {e}"))),
        }
    }
    Err(WebError::Internal(
        "could not create unique upload temp file".into(),
    ))
}

async fn stream_field_to_temp_file(
    mut field: Field<'_>,
    limit: u64,
    extension: &str,
) -> WebResult<TempUpload> {
    let (path, mut file) = create_upload_temp_file(extension).await?;
    let mut size = 0u64;
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|e| WebError::BadRequest(format!("multipart: {e}")))?
    {
        size = size.saturating_add(chunk.len() as u64);
        if size > limit {
            drop(file);
            let _ = tokio::fs::remove_file(&path).await;
            return Err(WebError::PayloadTooLarge(format!(
                "size {size} exceeds upload limit {limit}"
            )));
        }
        file.write_all(&chunk)
            .await
            .map_err(|e| WebError::Internal(format!("temp upload write: {e}")))?;
    }
    file.flush()
        .await
        .map_err(|e| WebError::Internal(format!("temp upload flush: {e}")))?;
    Ok(TempUpload { path, size })
}

async fn apply_orphan_ipfs_grace_pin(
    client: &impl tokio_postgres::GenericClient,
    cid: &str,
    fallback_size: i64,
    file_type: FileType,
    grace_period_hours: i64,
) {
    if let Err(e) = async {
        upsert_file(client, cid, fallback_size, file_type).await?;
        let delete_by = utc_now() + chrono::Duration::hours(grace_period_hours);
        insert_grace_period_file_pin(client, cid, utc_now(), delete_by, None, None, None).await?;
        Ok::<(), crate::AlephError>(())
    }
    .await
    {
        tracing::error!(?e, cid, "failed to apply orphan IPFS grace pin");
    }
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
    let stat_timeout = Duration::from_secs(state.config.ipfs.stat_timeout);

    let mut filename: String = "file".to_string();
    let mut file_upload: Option<TempUpload> = None;
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
            file_upload = Some(stream_field_to_temp_file(field, max_upload_file_size, ".bin").await?);
        } else if name == "metadata" {
            let bytes = field
                .bytes()
                .await
                .map_err(|e| WebError::BadRequest(format!("multipart: {e}")))?;
            metadata = Some(bytes.to_vec());
        }
    }
    let file_upload = file_upload
        .ok_or_else(|| WebError::Unprocessable("Missing 'file' in multipart form.".into()))?;
    if metadata.is_none() && file_upload.size > max_unauth_upload {
        return Err(WebError::PayloadTooLarge(format!(
            "size {} exceeds unauthenticated upload limit {}",
            file_upload.size,
            max_unauth_upload
        )));
    }
    let parsed_metadata = match metadata.as_deref() {
        Some(raw) => Some(StoreMetadata::from_bytes(raw)?),
        None => None,
    };
    let expected_hash = if let Some(meta) = parsed_metadata.as_ref() {
        Some(
            verify_store_metadata(
                &state,
                meta,
                None,
                file_upload.size as usize,
                ItemType::Ipfs,
            )
            .await?,
        )
    } else {
        None
    };

    let cid = ipfs
        .add_file_path(&file_upload.path, 0)
        .await
        .map_err(|e| WebError::Internal(format!("ipfs: {e}")))?;
    if let Some(expected_hash) = expected_hash.as_deref()
        && expected_hash != cid
    {
        let client = get_db(&state).await?;
        apply_orphan_ipfs_grace_pin(
            &**client,
            &cid,
            file_upload.size as i64,
            FileType::File,
            grace_period_hours,
        )
        .await;
        return Err(WebError::Unprocessable(format!(
            "File hash does not match ({cid} != {expected_hash})"
        )));
    }

    let client = get_db(&state).await?;
    let size = match ipfs.stat(&cid, stat_timeout).await {
        Ok(stat) => stat.size as i64,
        Err(e) => {
            tracing::warn!(?e, cid, "ipfs_add_file: failed to stat uploaded file");
            apply_orphan_ipfs_grace_pin(
                &**client,
                &cid,
                file_upload.size as i64,
                FileType::File,
                grace_period_hours,
            )
            .await;
            return Err(WebError::GatewayTimeout(format!("ipfs stat: {e}")));
        }
    };
    if let Err(e) = async {
        upsert_file(&**client, &cid, size, FileType::File).await?;
        if parsed_metadata.is_none() {
            // Anonymous upload — drop a grace-period pin so the GC keeps it for
            // `grace_period_hours` hours unless a signed message claims it.
            let delete_by = utc_now() + chrono::Duration::hours(grace_period_hours);
            insert_grace_period_file_pin(&**client, &cid, utc_now(), delete_by, None, None, None)
                .await?;
        }
        Ok::<(), crate::AlephError>(())
    }
    .await
    {
        apply_orphan_ipfs_grace_pin(&**client, &cid, size, FileType::File, grace_period_hours)
            .await;
        return Err(WebError::from(e));
    }

    let body = json!({
        "status": "success",
        "hash": cid,
        "name": filename,
        "size": size,
    });
    let mut status_code = StatusCode::OK;
    if let Some(meta) = parsed_metadata {
        let (broadcast_status_code, _) =
            broadcast_and_process_message(&state, &**client, &meta.message_dict, meta.sync)
                .await?;
        status_code = broadcast_status_code;
    }
    Ok(json_text_response(status_code, body.to_string()))
}

async fn ipfs_add_car(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> WebResult<Response> {
    let ipfs: Arc<IpfsService> = state
        .ipfs_service
        .clone()
        .ok_or_else(|| WebError::Forbidden("IPFS is disabled on this node".into()))?;

    let max_upload_car_size = state.config.ipfs.max_upload_car_size as u64;
    let grace_period_hours = state.config.storage.grace_period as i64;
    let stat_timeout = Duration::from_secs(state.config.ipfs.stat_timeout);

    let mut car_upload: Option<TempUpload> = None;
    let mut metadata: Option<Vec<u8>> = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| WebError::BadRequest(format!("multipart: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
            car_upload = Some(stream_field_to_temp_file(field, max_upload_car_size, ".car").await?);
        } else if name == "metadata" {
            let bytes = field
                .bytes()
                .await
                .map_err(|e| WebError::BadRequest(format!("multipart: {e}")))?;
            metadata = Some(bytes.to_vec());
        }
    }

    let car_upload = car_upload
        .ok_or_else(|| WebError::Unprocessable("Missing 'file' in multipart form.".into()))?;
    let metadata =
        metadata.ok_or_else(|| WebError::Unprocessable("metadata is required for CAR upload".into()))?;
    let parsed_metadata = StoreMetadata::from_bytes(&metadata)?;
    let car_root = read_carv1_root_from_path(&car_upload.path).await.map_err(WebError::from)?;
    verify_store_metadata(
        &state,
        &parsed_metadata,
        Some(&car_root),
        car_upload.size as usize,
        ItemType::Ipfs,
    )
    .await?;

    let imported_roots = ipfs
        .dag_import_path(&car_upload.path, true)
        .await
        .map_err(|e| WebError::BadGateway(format!("Failed to import CAR into IPFS: {e}")))?;
    if imported_roots.len() != 1 || imported_roots.first() != Some(&car_root) {
        let kubo_root = imported_roots
            .first()
            .cloned()
            .unwrap_or_else(|| "<none>".to_string());
        return Err(WebError::Unprocessable(format!(
            "Imported root does not match expected ({kubo_root} != {car_root}); CAR header declared a root that does not correspond to the imported DAG"
        )));
    }

    let stat = match ipfs.stat(&car_root, stat_timeout).await {
        Ok(stat) => stat,
        Err(e) => {
            let client = get_db(&state).await?;
            apply_orphan_ipfs_grace_pin(
                &**client,
                &car_root,
                car_upload.size as i64,
                FileType::Directory,
                grace_period_hours,
            )
            .await;
            return Err(WebError::GatewayTimeout(format!("ipfs stat: {e}")));
        }
    };

    let client = get_db(&state).await?;
    if let Err(e) = upsert_file(&**client, &car_root, stat.size as i64, FileType::Directory).await {
        apply_orphan_ipfs_grace_pin(
            &**client,
            &car_root,
            stat.size as i64,
            FileType::Directory,
            grace_period_hours,
        )
        .await;
        return Err(WebError::from(e));
    }

    let body = json!({
        "status": "success",
        "hash": car_root,
        "size": stat.size,
    });
    let (status_code, _) = broadcast_and_process_message(
        &state,
        &**client,
        &parsed_metadata.message_dict,
        parsed_metadata.sync,
    )
    .await?;

    Ok(json_text_response(status_code, body.to_string()))
}
