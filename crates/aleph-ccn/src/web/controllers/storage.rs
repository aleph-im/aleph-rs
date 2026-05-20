//! Mirrors `aleph/web/controllers/storage.py`. All endpoints are wired,
//! including the multipart upload pipeline (raw + form-data + optional STORE
//! metadata).

use axum::Router;
use axum::body::Body;
use axum::extract::multipart::Field;
use axum::extract::{FromRequest, Multipart, Path, State};
use axum::http::{StatusCode, header};
use axum::response::Response;
use axum::routing::{get, post};
use aleph_types::message::item_type::ItemType;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use serde_json::{Value, json};

use crate::db::accessors::files::{
    count_file_pins, get_file, get_file_tag, get_message_file_pin, insert_grace_period_file_pin,
    upsert_file,
};
use crate::schemas::pending_messages::parse_message as parse_pending_message;
use crate::services::cost::{
    CostContent, CostContentKind, get_payment_type, get_total_and_detailed_costs,
};
use crate::services::cost_validation::{BalanceValidation, validate_balance_for_payment};
use crate::services::ipfs::IpfsService;
use crate::types::files::{FileTag, FileType};
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::utils::{broadcast_and_process_message, get_db, json_text_response};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/v0/ipfs/add_json", post(add_ipfs_json))
        .route("/api/v0/storage/add_json", post(add_storage_json))
        .route("/api/v0/storage/add_file", post(storage_add_file))
        .route("/api/v0/storage/{file_hash}", get(get_hash))
        .route("/api/v0/storage/raw/{file_hash}", get(get_raw_hash))
        .route(
            "/api/v0/storage/by-message-hash/{message_hash}",
            get(get_file_metadata_by_message_hash),
        )
        .route(
            "/api/v0/storage/by-ref/{ref}",
            get(get_file_metadata_by_ref),
        )
        .route(
            "/api/v0/storage/by-ref/{address}/{ref}",
            get(get_file_metadata_by_ref_addr),
        )
        .route(
            "/api/v0/storage/metadata/{file_hash}",
            get(get_file_metadata),
        )
        .route("/api/v0/storage/count/{hash}", get(get_file_pins_count))
}

// ---------------------------------------------------------------------------
// JSON upload helpers
// ---------------------------------------------------------------------------

async fn add_ipfs_json(State(state): State<AppState>, body: Bytes) -> WebResult<Response> {
    let ipfs = state
        .ipfs_service
        .clone()
        .ok_or_else(|| WebError::Forbidden("IPFS is disabled on this node".into()))?;
    let value: serde_json::Value =
        serde_json::from_slice(&body).map_err(|e| WebError::Unprocessable(e.to_string()))?;
    let hash = ipfs
        .add_json(&value)
        .await
        .map_err(|e| WebError::Internal(format!("ipfs: {e}")))?;
    let client = get_db(&state).await?;
    upsert_file(&**client, &hash, body.len() as i64, FileType::File).await?;
    insert_upload_grace_pin(&**client, &state, &hash).await?;
    let resp = json!({ "status": "success", "hash": hash });
    Ok(json_text_response(StatusCode::OK, resp.to_string()))
}

async fn add_storage_json(State(state): State<AppState>, body: Bytes) -> WebResult<Response> {
    let engine = state
        .storage_engine
        .clone()
        .ok_or_else(|| WebError::Internal("storage engine not configured".into()))?;
    let value: serde_json::Value =
        serde_json::from_slice(&body).map_err(|e| WebError::Unprocessable(e.to_string()))?;
    let canonical = serde_json::to_vec(&value).map_err(|e| WebError::Internal(e.to_string()))?;
    let hash = sha256_hex(&canonical);
    engine
        .write(&hash, &canonical)
        .await
        .map_err(|e| WebError::Internal(format!("storage: {e}")))?;
    let client = get_db(&state).await?;
    upsert_file(&**client, &hash, canonical.len() as i64, FileType::File).await?;
    insert_upload_grace_pin(&**client, &state, &hash).await?;
    let resp = json!({ "status": "success", "hash": hash });
    Ok(json_text_response(StatusCode::OK, resp.to_string()))
}

async fn insert_upload_grace_pin(
    client: &impl tokio_postgres::GenericClient,
    state: &AppState,
    hash: &str,
) -> WebResult<()> {
    let now = crate::toolkit::timestamp::utc_now();
    let delete_by = now + chrono::Duration::hours(state.config.storage.grace_period as i64);
    insert_grace_period_file_pin(client, hash, now, delete_by, None, None, None).await?;
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

async fn read_multipart_field_limited(mut field: Field<'_>, limit: usize) -> WebResult<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|e| WebError::BadRequest(format!("Multipart error: {e}")))?
    {
        let next_len = out.len().saturating_add(chunk.len());
        if next_len > limit {
            return Err(WebError::PayloadTooLarge(format!(
                "size {next_len} exceeds upload limit {limit}"
            )));
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Multipart file upload (depends on signature_verifier + storage engine)
// ---------------------------------------------------------------------------

/// Handle `POST /api/v0/storage/add_file`. Supports both `multipart/form-data`
/// (with optional `metadata` part containing a STORE message) and the
/// "raw" path where the body is the file bytes. Mirrors
/// `aleph/web/controllers/storage.py::storage_add_file` — including signature
/// verification, balance check, and pending-message insertion (the broadcast
/// path pyaleph performs).
async fn storage_add_file(
    State(state): State<AppState>,
    request: axum::extract::Request,
) -> WebResult<Response> {
    let engine = state
        .storage_engine
        .clone()
        .ok_or_else(|| WebError::Internal("storage engine not configured".into()))?;
    let max_file_size = state.config.storage.max_file_size as usize;
    let max_unauth = state.config.storage.max_unauthenticated_upload_file_size as usize;

    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let (file_bytes, metadata_bytes): (Vec<u8>, Option<Vec<u8>>) =
        if content_type.starts_with("multipart/form-data") {
            let mut multipart = Multipart::from_request(request, &())
                .await
                .map_err(|e| WebError::BadRequest(format!("Invalid multipart body: {e}")))?;
            let mut file_bytes: Option<Vec<u8>> = None;
            let mut metadata_bytes: Option<Vec<u8>> = None;
            while let Some(field) = multipart
                .next_field()
                .await
                .map_err(|e| WebError::BadRequest(format!("Multipart error: {e}")))?
            {
                match field.name() {
                    Some("file") => {
                        let data = read_multipart_field_limited(field, max_file_size).await?;
                        file_bytes = Some(data);
                    }
                    Some("metadata") => {
                        metadata_bytes = Some(
                            field
                                .bytes()
                                .await
                                .map_err(|e| WebError::BadRequest(e.to_string()))?
                                .to_vec(),
                        );
                    }
                    _ => {}
                }
            }
            (
                file_bytes.ok_or_else(|| {
                    WebError::BadRequest("No 'file' field in multipart request".into())
                })?,
                metadata_bytes,
            )
        } else {
            // Raw upload — body is the file bytes.
            let bytes = axum::body::to_bytes(request.into_body(), max_unauth + 1)
                .await
                .map_err(|e| WebError::PayloadTooLarge(e.to_string()))?;
            (bytes.to_vec(), None)
        };

    let limit = if metadata_bytes.is_some() {
        max_file_size
    } else {
        max_unauth
    };
    if file_bytes.len() > limit {
        return Err(WebError::PayloadTooLarge(format!(
            "{} exceeds max file size {limit}",
            file_bytes.len()
        )));
    }

    // Parse metadata (if any) up-front so we can perform auth/balance checks
    // *before* writing the file. Mirrors pyaleph's `_check_and_add_file`.
    let parsed_metadata: Option<StoreMetadata> = match metadata_bytes.as_deref() {
        None => None,
        Some(raw) => {
            let value: Value = serde_json::from_slice(raw).map_err(|e| {
                WebError::Unprocessable(format!("Could not decode metadata: {e}"))
            })?;
            Some(StoreMetadata::from_value(value)?)
        }
    };

    let hash = sha256_hex(&file_bytes);

    // Auth + balance check, if metadata was attached. Mirrors pyaleph's
    // `_verify_message_signature` + `_verify_user_balance`.
    let has_metadata = parsed_metadata.is_some();
    if let Some(meta) = parsed_metadata.as_ref() {
        verify_store_metadata(&state, meta, Some(&hash), file_bytes.len(), ItemType::Storage)
            .await?;
    }

    engine
        .write(&hash, &file_bytes)
        .await
        .map_err(|e| WebError::Internal(format!("storage: {e}")))?;

    let client = get_db(&state).await?;
    upsert_file(&**client, &hash, file_bytes.len() as i64, FileType::File).await?;
    if !has_metadata {
        insert_upload_grace_pin(&**client, &state, &hash).await?;
    }

    let response = json!({
        "status": "success",
        "name": hash,
        "hash": hash,
    });

    let mut status_code = StatusCode::OK;
    if let Some(meta) = parsed_metadata {
        let (broadcast_status_code, _) =
            broadcast_and_process_message(&state, &**client, &meta.message_dict, meta.sync)
                .await?;
        status_code = broadcast_status_code;
    }

    Ok(json_text_response(status_code, response.to_string()))
}

/// Parsed `metadata` part of a STORE upload. Mirrors pyaleph's
/// `StorageMetadata` schema.
pub(crate) struct StoreMetadata {
    /// The raw JSON dict of the STORE message, kept around so we can insert it
    /// into `pending_messages`.
    pub(crate) message_dict: Value,
    /// Whether the client wants synchronous broadcast.
    pub(crate) sync: bool,
}

impl StoreMetadata {
    pub(crate) fn from_value(v: Value) -> WebResult<Self> {
        let obj = v
            .as_object()
            .ok_or_else(|| WebError::Unprocessable("metadata must be an object".into()))?;
        let message = obj.get("message").cloned().ok_or_else(|| {
            WebError::Unprocessable("metadata.message is required".into())
        })?;
        let sync = obj.get("sync").and_then(|v| v.as_bool()).unwrap_or(false);
        Ok(Self {
            message_dict: message,
            sync,
        })
    }

    pub(crate) fn from_bytes(raw: &[u8]) -> WebResult<Self> {
        let value: Value = serde_json::from_slice(raw)
            .map_err(|e| WebError::Unprocessable(format!("Could not decode metadata: {e}")))?;
        Self::from_value(value)
    }
}

/// Verify the signature on `meta.message` and that the sender can afford the
/// upload at `file_size`. Mirrors `_verify_message_signature` +
/// `_verify_user_balance` from pyaleph.
pub(crate) async fn verify_store_metadata(
    state: &AppState,
    meta: &StoreMetadata,
    expected_file_hash: Option<&str>,
    file_size: usize,
    expected_item_type: ItemType,
) -> WebResult<String> {
    // 1. Parse + validate the wire payload as a STORE PendingMessage.
    let parsed = parse_pending_message(meta.message_dict.clone())
        .map_err(|e| WebError::Unprocessable(format!("Invalid STORE metadata: {e}")))?;
    let pending_store = match parsed {
        crate::schemas::pending_messages::BasePendingMessage::Store(s) => s,
        _ => {
            return Err(WebError::Unprocessable(
                "metadata.message must be a STORE message".into(),
            ));
        }
    };
    let content = pending_store.content.as_ref().ok_or_else(|| {
        WebError::Unprocessable("Store message content needed".into())
    })?;
    // 2. The content's `item_hash` must match the uploaded file hash when it
    //    is already known. IPFS uploads only know the final CID after pinning,
    //    so callers can defer this check and use the returned hash.
    let content_item_hash = content.file_hash().to_string();
    if let Some(file_hash) = expected_file_hash
        && content_item_hash != file_hash
    {
        return Err(WebError::Unprocessable(format!(
            "File hash does not match ({file_hash} != {content_item_hash})"
        )));
    }
    let content_item_type = crate::schemas::base_messages::item_type_from_hash(&content_item_hash)
        .map_err(|e| WebError::Unprocessable(format!("Invalid STORE file hash: {e}")))?;
    if content_item_type != expected_item_type {
        return Err(WebError::Unprocessable(format!(
            "Unsupported STORE item type for this endpoint: {content_item_type:?}"
        )));
    }
    // 3. Signature verification — delegates to the chain dispatcher.
    let view = crate::chains::abc::SimplePendingMessage {
        chain: pending_store.chain.clone(),
        sender: pending_store.sender.clone(),
        message_type: pending_store.message_type,
        item_hash: pending_store.item_hash.clone(),
        signature: pending_store.signature.clone(),
        time_seconds: pending_store.time.timestamp() as f64
            + (pending_store.time.timestamp_subsec_nanos() as f64) / 1e9,
    };
    state
        .signature_verifier
        .verify_signature(&view)
        .await
        .map_err(|_| WebError::Forbidden("Invalid signature on STORE metadata".into()))?;

    // 4. Balance check. Mirrors pyaleph's `_verify_user_balance`, which runs
    //    for every authenticated upload regardless of file size.
    let charged_address =
        store_content_address(&meta.message_dict).unwrap_or_else(|| pending_store.sender.clone());
    use crate::toolkit::constants::MIB;
    let mib = MIB as usize;
    let estimated_size_mib = file_size.div_ceil(mib) as i64;
    let client = get_db(state).await?;
    let mut cost_content_value = serde_json::to_value(content)
        .map_err(|e| WebError::Internal(format!("store content serialization: {e}")))?;
    let cost_content_obj = cost_content_value
        .as_object_mut()
        .ok_or_else(|| WebError::Unprocessable("Invalid store message content".into()))?;
    cost_content_obj.insert("address".into(), Value::String(charged_address.clone()));
    cost_content_obj.insert(
        "estimated_size_mib".into(),
        Value::Number(serde_json::Number::from(estimated_size_mib)),
    );
    let cost_content = CostContent::new(CostContentKind::Store, &cost_content_value);
    let payment_type = get_payment_type(&cost_content);
    let (message_cost, _) =
        get_total_and_detailed_costs(&**client, &cost_content, "").await.map_err(|e| {
            WebError::Internal(format!("storage cost estimation failed: {e}"))
        })?;
    let validation =
        validate_balance_for_payment(&**client, &charged_address, message_cost, payment_type)
            .await
            .map_err(WebError::from)?;
    if let BalanceValidation::Invalid(exception) = validation {
        return Err(WebError::PaymentRequired(exception.to_string()));
    }
    Ok(content_item_hash)
}

fn store_content_address(message_dict: &Value) -> Option<String> {
    if let Some(address) = message_dict
        .get("content")
        .and_then(|content| content.get("address"))
        .and_then(Value::as_str)
    {
        return Some(address.to_owned());
    }
    message_dict
        .get("item_content")
        .and_then(Value::as_str)
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|content| {
            content
                .get("address")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
}

// ---------------------------------------------------------------------------
// Read endpoints
// ---------------------------------------------------------------------------

fn item_type_for_hash(hash: &str) -> WebResult<&'static str> {
    // QmXXX (base58, 46 chars, leading Qm/12) is IPFSv0;
    // bafy... is CIDv1 (IPFS); 64-hex is storage.
    if hash.starts_with("Qm") && hash.len() == 46 {
        return Ok("ipfs");
    }
    if hash.starts_with("bafy") {
        return Ok("ipfs");
    }
    if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok("storage");
    }
    Err(WebError::BadRequest("Invalid hash provided".into()))
}

async fn get_hash(
    State(state): State<AppState>,
    Path(file_hash): Path<String>,
) -> WebResult<Response> {
    let engine_kind = item_type_for_hash(&file_hash)?;
    let max_file_size = state.config.storage.max_file_size as i64;

    let client = get_db(&state).await?;
    let meta = get_file(&**client, &file_hash).await?;
    let meta = meta.ok_or_else(|| WebError::NotFound("Not found".into()))?;
    if meta.size > max_file_size {
        return Err(WebError::PayloadTooLarge(format!(
            "{} exceeds max file size {}",
            meta.size, max_file_size
        )));
    }
    drop(client);

    let content = read_hash_content(&state, &file_hash, engine_kind).await?;
    let encoded = B64.encode(content);
    let resp = json!({
        "status": "success",
        "hash": file_hash,
        "engine": engine_kind,
        "content": encoded,
    });
    Ok(json_text_response(StatusCode::OK, resp.to_string()))
}

async fn read_hash_content(
    state: &AppState,
    file_hash: &str,
    engine_kind: &str,
) -> WebResult<Bytes> {
    if engine_kind == "storage" {
        let engine = state
            .storage_engine
            .clone()
            .ok_or_else(|| WebError::NotFound(format!("No file found for hash {file_hash}")))?;
        match engine
            .read(file_hash)
            .await
            .map_err(|e| WebError::Internal(format!("storage: {e}")))?
        {
            Some(b) => Ok(b),
            None => Err(WebError::NotFound(format!(
                "No file found for hash {file_hash}"
            ))),
        }
    } else {
        let ipfs = state
            .ipfs_service
            .clone()
            .ok_or_else(|| WebError::NotFound(format!("No file found for hash {file_hash}")))?;
        ipfs_cat(&ipfs, file_hash).await
    }
}

async fn ipfs_cat(ipfs: &IpfsService, cid: &str) -> WebResult<Bytes> {
    ipfs.cat(cid)
        .await
        .map_err(|e| WebError::NotFound(format!("ipfs: {e}")))
}

async fn get_raw_hash(
    State(state): State<AppState>,
    Path(file_hash): Path<String>,
) -> WebResult<Response> {
    let engine_kind = item_type_for_hash(&file_hash)?;
    let client = get_db(&state).await?;
    let meta = get_file(&**client, &file_hash).await?;
    let meta = meta.ok_or_else(|| WebError::NotFound("Not found".into()))?;
    let size = meta.size;
    let is_directory = meta.r#type == FileType::Directory;
    let content_type = if is_directory {
        "application/x-tar"
    } else {
        "application/octet-stream"
    };
    drop(client);

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header("Accept-Ranges", "none");
    if !is_directory {
        builder = builder.header(header::CONTENT_LENGTH, size.to_string());
    }
    let body = raw_hash_body(&state, &file_hash, engine_kind, is_directory).await?;
    builder
        .body(body)
        .map_err(|e| WebError::Internal(e.to_string()))
}

async fn raw_hash_body(
    state: &AppState,
    file_hash: &str,
    engine_kind: &str,
    is_directory: bool,
) -> WebResult<Body> {
    if engine_kind == "storage" {
        let engine = state
            .storage_engine
            .clone()
            .ok_or_else(|| WebError::NotFound(format!("No file found for hash {file_hash}")))?;
        let stream = engine
            .read_iterator(file_hash, 64 * 1024)
            .await
            .map_err(|e| WebError::Internal(format!("storage: {e}")))?
            .ok_or_else(|| WebError::NotFound(format!("No file found for hash {file_hash}")))?;
        return Ok(Body::from_stream(stream));
    }

    let ipfs = state
        .ipfs_service
        .clone()
        .ok_or_else(|| WebError::NotFound(format!("No file found for hash {file_hash}")))?;
    let stream = if is_directory {
        ipfs.get_ipfs_directory_iterator(file_hash).await
    } else {
        ipfs.get_ipfs_content_iterator(file_hash).await
    }
    .map_err(|e| WebError::NotFound(format!("ipfs: {e}")))?;
    Ok(Body::from_stream(stream))
}

async fn get_file_metadata_by_message_hash(
    State(state): State<AppState>,
    Path(message_hash): Path<String>,
) -> WebResult<Response> {
    let client = get_db(&state).await?;
    let pin = get_message_file_pin(&**client, &message_hash)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("No file found for message {message_hash}")))?;
    let file = get_file(&**client, &pin.file_hash)
        .await?
        .ok_or_else(|| WebError::NotFound("Underlying file not found".into()))?;
    let ref_value = pin.r#ref.unwrap_or_else(|| message_hash.clone());
    let body = json!({
        "ref": ref_value,
        "owner": pin.owner.unwrap_or_default(),
        "file_hash": pin.file_hash,
        "download_url": format!("/api/v0/storage/raw/{}", pin.file_hash),
        "size": file.size,
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

async fn get_file_metadata_by_ref(
    State(state): State<AppState>,
    Path(reference): Path<String>,
) -> WebResult<Response> {
    if item_type_for_hash(&reference).is_err() {
        return Err(WebError::BadRequest(
            "address is required for user-defined ref".into(),
        ));
    }
    metadata_by_ref(&state, &reference, None).await
}

async fn get_file_metadata_by_ref_addr(
    State(state): State<AppState>,
    Path((address, reference)): Path<(String, String)>,
) -> WebResult<Response> {
    let address = if item_type_for_hash(&reference).is_ok() {
        None
    } else {
        Some(address.as_str())
    };
    metadata_by_ref(&state, &reference, address).await
}

async fn metadata_by_ref(
    state: &AppState,
    reference: &str,
    address: Option<&str>,
) -> WebResult<Response> {
    let tag_str = if let Some(addr) = address {
        format!("{}/{}", addr, reference)
    } else {
        reference.to_string()
    };
    let tag = FileTag::from(tag_str.clone());
    let client = get_db(state).await?;
    let tagged = get_file_tag(&**client, &tag)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("No file found for tag {tag_str}")))?;
    let file = get_file(&**client, &tagged.file_hash)
        .await?
        .ok_or_else(|| WebError::NotFound("Underlying file not found".into()))?;
    let final_ref = if let Some((_, after)) = tag_str.split_once('/') {
        after.to_string()
    } else {
        tag_str
    };
    let body = json!({
        "ref": final_ref,
        "owner": tagged.owner,
        "file_hash": tagged.file_hash,
        "download_url": format!("/api/v0/storage/raw/{}", tagged.file_hash),
        "size": file.size,
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

async fn get_file_metadata(
    State(state): State<AppState>,
    Path(file_hash): Path<String>,
) -> WebResult<Response> {
    let client = get_db(&state).await?;
    let file = get_file(&**client, &file_hash)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("No file found for hash {file_hash}")))?;
    let body = json!({
        "file_hash": file.hash,
        "type": serde_json::to_value(file.r#type).unwrap(),
        "size": file.size,
        "download_url": format!("/api/v0/storage/raw/{}", file.hash),
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

async fn get_file_pins_count(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> WebResult<Response> {
    if hash.is_empty() {
        return Err(WebError::BadRequest("No hash provided".into()));
    }
    let client = get_db(&state).await?;
    let count = count_file_pins(&**client, &hash).await?;
    Ok(json_text_response(StatusCode::OK, count.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_content_address_prefers_content_address() {
        let message = json!({
            "sender": "sender",
            "content": {"address": "owner"},
            "item_content": "{\"address\":\"legacy-owner\"}"
        });
        assert_eq!(store_content_address(&message).as_deref(), Some("owner"));
    }

    #[test]
    fn store_content_address_reads_legacy_item_content_address() {
        let message = json!({
            "sender": "sender",
            "item_content": "{\"address\":\"legacy-owner\"}"
        });
        assert_eq!(
            store_content_address(&message).as_deref(),
            Some("legacy-owner")
        );
    }
}
