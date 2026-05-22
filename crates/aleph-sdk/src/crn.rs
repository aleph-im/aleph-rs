use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use aleph_types::account::{Account, SignError};
use aleph_types::item_hash::ItemHash;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use futures_util::{SinkExt, Stream, StreamExt};
use p256::ecdsa::{SigningKey, signature::Signer};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use url::Url;

/// Response from POST /control/allocation/notify.
#[derive(Debug, Clone, Deserialize)]
pub struct AllocationResponse {
    /// Whether zero VMs failed (i.e. `failing` is empty).
    pub success: bool,
    /// Whether the requested VM started successfully.
    pub successful: bool,
    pub failing: Vec<String>,
    pub errors: HashMap<String, String>,
}

/// A log entry from the stream_logs WebSocket.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    #[serde(rename = "type")]
    pub log_type: LogType,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogType {
    Stdout,
    Stderr,
    System,
}

/// Options for `CrnClient::create_backup`. Defaults to `false` for both fields.
#[derive(Debug, Clone, Default)]
pub struct CreateBackupOpts {
    /// Include persistent volumes in the backup archive.
    pub include_volumes: bool,
    /// Skip the QEMU guest agent filesystem freeze. Faster, less consistent.
    pub skip_fsfreeze: bool,
}

/// Backup metadata returned by the CRN once a backup is complete.
///
/// `expires_at` is kept as a `String` (ISO 8601) rather than a typed timestamp
/// so the SDK doesn't take a transitive dep on chrono/time for one passthrough
/// field. Callers needing typed time parse at the call site.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    pub backup_id: String,
    pub size: u64,
    pub checksum: String,
    pub expires_at: String,
    pub download_url: String,
    #[serde(default)]
    pub volumes: Vec<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Result of `POST /control/machine/<vm>/backup`. 200 -> Complete; 202 -> Started.
#[derive(Debug, Clone)]
pub enum CreateBackup {
    Started,
    Complete(BackupMetadata),
}

/// Result of `GET /control/machine/<vm>/backup`. 202 -> InProgress; 200 -> Complete; 404 -> NotFound.
#[derive(Debug, Clone)]
pub enum BackupStatus {
    InProgress,
    Complete(BackupMetadata),
    NotFound,
}

/// Response from POST /control/machine/<vm>/restore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreResponse {
    pub status: String,
    pub vm_hash: String,
    #[serde(default)]
    pub old_rootfs_backup: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Networking info for a running VM, returned by `/about/executions/list`.
///
/// `ipv6` is a CIDR like `"fc00:1:2:3:1:abcd:1234:5670/124"` - the prefix
/// assigned to the VM's interface, not the address you `ssh` to directly.
#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionNetworking {
    #[serde(default)]
    pub ipv4: Option<String>,
    #[serde(default)]
    pub ipv6: Option<String>,
}

/// A running VM execution on a CRN, as returned by `/about/executions/list`.
/// Only the fields used by aleph-cli are modeled.
#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionInfo {
    #[serde(default)]
    pub networking: Option<ExecutionNetworking>,
}

/// Subset of the `/v2/about/executions/list` response shape on a CRN.
///
/// Only the fields actually consumed by `aleph instance port-forward list`
/// are modeled. Unknown fields on the wire are silently ignored so CRN
/// response shape evolution does not break us.
#[derive(Debug, Clone, Deserialize)]
pub struct ActiveVmList(pub HashMap<ItemHash, ActiveVm>);

#[derive(Debug, Clone, Deserialize)]
pub struct ActiveVm {
    #[serde(default)]
    pub networking: Option<ActiveVmNetworking>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ActiveVmNetworking {
    #[serde(default)]
    pub mapped_ports: BTreeMap<u16, MappedPort>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MappedPort {
    pub host: u16,
    /// Forward-compat for fields the CRN may add (e.g. protocol filters).
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum CrnError {
    #[error(transparent)]
    Http(#[from] reqwest::Error),

    #[error("CRN returned HTTP {status}: {body}")]
    Api { status: u16, body: String },

    #[error("WebSocket error: {0}")]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),

    #[error("Signing error: {0}")]
    Sign(#[from] SignError),

    #[error("VM not found: {0}")]
    VmNotFound(ItemHash),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Payment required: {0}")]
    PaymentRequired(String),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<tokio_tungstenite::tungstenite::Error> for CrnError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        CrnError::WebSocket(Box::new(e))
    }
}

fn p256_pubkey_to_jwk(key: &p256::ecdsa::VerifyingKey) -> serde_json::Value {
    let point = key.to_encoded_point(false); // uncompressed
    let x = URL_SAFE_NO_PAD.encode(point.x().unwrap());
    let y = URL_SAFE_NO_PAD.encode(point.y().unwrap());
    serde_json::json!({
        "kty": "EC",
        "crv": "P-256",
        "x": x,
        "y": y,
    })
}

fn sign_operation(ephemeral_key: &SigningKey, domain: &str, method: &str, path: &str) -> String {
    let time = Utc::now().format("%Y-%m-%dT%H:%M:%S.%6fZ").to_string();

    let payload = serde_json::json!({
        "time": time,
        "method": method,
        "path": path,
        "domain": domain,
    });

    let payload_bytes = serde_json::to_vec(&payload).unwrap();
    let payload_hex = hex::encode(&payload_bytes);
    let signature: p256::ecdsa::Signature = ephemeral_key.sign(&payload_bytes);
    let sig_hex = hex::encode(signature.to_bytes());

    serde_json::to_string(&serde_json::json!({
        "payload": payload_hex,
        "signature": sig_hex,
    }))
    .unwrap()
}

pub struct CrnClient {
    http_client: reqwest::Client,
    crn_url: Url,
    domain: String,
    ephemeral_key: SigningKey,
    signed_pubkey_header: String,
}

impl CrnClient {
    pub fn new(account: &impl Account, crn_url: Url) -> Result<Self, CrnError> {
        let domain = crn_url
            .host_str()
            .ok_or_else(|| CrnError::Api {
                status: 0,
                body: format!("CRN URL has no host: {crn_url}"),
            })?
            .to_string();

        let ephemeral_key = SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng);
        let signed_pubkey_header = build_signed_pubkey_header(account, &domain, &ephemeral_key)?;

        Ok(Self {
            http_client: reqwest::Client::new(),
            crn_url,
            domain,
            ephemeral_key,
            signed_pubkey_header,
        })
    }

    pub async fn start_instance(&self, vm_id: &ItemHash) -> Result<AllocationResponse, CrnError> {
        let url = self
            .crn_url
            .join("/control/allocation/notify")
            .expect("valid path");

        let response = self
            .http_client
            .post(url)
            .json(&serde_json::json!({ "instance": vm_id.to_string() }))
            .send()
            .await?;

        let status = response.status().as_u16();
        match status {
            // 200 = all ok, 207 = partial success, 503 = all failed.
            // All three return structured JSON with failure details.
            200 | 207 | 503 => {
                let body = response.text().await?;
                Ok(serde_json::from_str(&body)?)
            }
            402 => {
                let body = response.text().await?;
                Err(CrnError::PaymentRequired(body))
            }
            _ => {
                let body = response.text().await?;
                Err(CrnError::Api { status, body })
            }
        }
    }

    fn auth_headers(&self, method: &str, path: &str) -> [(&'static str, String); 2] {
        let signed_op = sign_operation(&self.ephemeral_key, &self.domain, method, path);
        [
            ("X-SignedPubKey", self.signed_pubkey_header.clone()),
            ("X-SignedOperation", signed_op),
        ]
    }

    async fn perform_operation(&self, vm_id: &ItemHash, operation: &str) -> Result<(), CrnError> {
        let path = format!("/control/machine/{vm_id}/{operation}");
        let url = self.crn_url.join(&path).expect("valid path");
        let headers = self.auth_headers("POST", &path);

        let mut request = self.http_client.post(url);
        for (name, value) in &headers {
            request = request.header(*name, value);
        }

        let response = request.send().await?;
        let status = response.status().as_u16();
        match status {
            200 => Ok(()),
            402 => Err(CrnError::PaymentRequired(response.text().await?)),
            403 => Err(CrnError::Unauthorized(response.text().await?)),
            404 => Err(CrnError::VmNotFound(vm_id.clone())),
            _ => Err(CrnError::Api {
                status,
                body: response.text().await?,
            }),
        }
    }

    pub async fn stop_instance(&self, vm_id: &ItemHash) -> Result<(), CrnError> {
        self.perform_operation(vm_id, "stop").await
    }

    pub async fn reboot_instance(&self, vm_id: &ItemHash) -> Result<(), CrnError> {
        self.perform_operation(vm_id, "reboot").await
    }

    pub async fn erase_instance(&self, vm_id: &ItemHash) -> Result<(), CrnError> {
        self.perform_operation(vm_id, "erase").await
    }

    /// Ask the CRN to re-read the sender's `port-forwarding` aggregate and apply
    /// it to `vm_id` immediately. The CRN normally refreshes on its own schedule;
    /// this is the explicit prod.
    ///
    /// The endpoint is unauthenticated (no `X-SignedOperation` header). It is
    /// safe for anyone to ask the CRN to refresh; the actual configuration
    /// comes from the per-sender aggregate stored on the CCN.
    ///
    /// 2xx -> `Ok(())`. 404 -> [`CrnError::VmNotFound`]. Other statuses
    /// -> [`CrnError::Api`] with the body.
    pub async fn update_instance_config(&self, vm_id: &ItemHash) -> Result<(), CrnError> {
        let path = format!("/control/machine/{vm_id}/update");
        let url = self.crn_url.join(&path).expect("valid path");

        let response = self.http_client.post(url).send().await?;
        let status = response.status().as_u16();
        match status {
            200..=299 => Ok(()),
            404 => Err(CrnError::VmNotFound(vm_id.clone())),
            _ => Err(CrnError::Api {
                status,
                body: response.text().await?,
            }),
        }
    }

    /// List the VMs currently active on this CRN, indexed by item hash.
    ///
    /// Calls `GET /v2/about/executions/list`. The v1 fallback that the Python
    /// SDK does (on 404) is intentionally not implemented here - users on
    /// v1-only CRNs will see `external_port` as `N/A` in `aleph instance
    /// port-forward list`, same graceful degradation as an unreachable CRN.
    ///
    /// No auth; this endpoint is public.
    pub async fn get_active_vms(&self) -> Result<ActiveVmList, CrnError> {
        let url = self
            .crn_url
            .join("/v2/about/executions/list")
            .expect("valid path");
        let response = self.http_client.get(url).send().await?;
        let status = response.status().as_u16();
        if !(200..=299).contains(&status) {
            return Err(CrnError::Api {
                status,
                body: response.text().await?,
            });
        }
        Ok(response.json::<ActiveVmList>().await?)
    }

    pub async fn create_backup(
        &self,
        vm_id: &ItemHash,
        opts: CreateBackupOpts,
    ) -> Result<CreateBackup, CrnError> {
        let path = format!("/control/machine/{vm_id}/backup");
        let url = self.crn_url.join(&path).expect("valid path");
        let headers = self.auth_headers("POST", &path);

        let mut request = self.http_client.post(url);
        for (name, value) in &headers {
            request = request.header(*name, value);
        }
        let mut query: Vec<(&str, &str)> = Vec::new();
        if opts.include_volumes {
            query.push(("include_volumes", "true"));
        }
        if opts.skip_fsfreeze {
            query.push(("skip_fsfreeze", "true"));
        }
        if !query.is_empty() {
            request = request.query(&query);
        }

        let response = request.send().await?;
        let status = response.status().as_u16();
        match status {
            202 => Ok(CreateBackup::Started),
            200 => Ok(CreateBackup::Complete(response.json().await?)),
            402 => Err(CrnError::PaymentRequired(response.text().await?)),
            403 => Err(CrnError::Unauthorized(response.text().await?)),
            404 => Err(CrnError::VmNotFound(vm_id.clone())),
            _ => Err(CrnError::Api {
                status,
                body: response.text().await?,
            }),
        }
    }

    pub async fn expire_instance(
        &self,
        vm_id: &ItemHash,
        timeout: Duration,
    ) -> Result<(), CrnError> {
        let path = format!("/control/machine/{vm_id}/expire");
        let url = self.crn_url.join(&path).expect("valid path");
        let headers = self.auth_headers("POST", &path);

        let mut request = self.http_client.post(url);
        for (name, value) in &headers {
            request = request.header(*name, value);
        }

        let response = request
            .json(&serde_json::json!({ "timeout": timeout.as_secs_f64() }))
            .send()
            .await?;

        let status = response.status().as_u16();
        match status {
            200 => Ok(()),
            402 => Err(CrnError::PaymentRequired(response.text().await?)),
            403 => Err(CrnError::Unauthorized(response.text().await?)),
            404 => Err(CrnError::VmNotFound(vm_id.clone())),
            _ => Err(CrnError::Api {
                status,
                body: response.text().await?,
            }),
        }
    }

    pub async fn stream_logs(
        &self,
        vm_id: &ItemHash,
    ) -> Result<impl Stream<Item = Result<LogEntry, CrnError>>, CrnError> {
        let path = format!("/control/machine/{vm_id}/stream_logs");

        // Build WebSocket URL
        let mut ws_url = self.crn_url.clone();
        let scheme = match ws_url.scheme() {
            "https" => "wss",
            _ => "ws",
        };
        ws_url.set_scheme(scheme).expect("valid scheme");
        ws_url.set_path(&path);

        // Build auth message
        let signed_op = sign_operation(&self.ephemeral_key, &self.domain, "GET", &path);
        let signed_pubkey: serde_json::Value = serde_json::from_str(&self.signed_pubkey_header)?;
        let signed_op_value: serde_json::Value = serde_json::from_str(&signed_op)?;

        let auth_message = serde_json::json!({
            "auth": {
                "X-SignedPubKey": signed_pubkey,
                "X-SignedOperation": signed_op_value,
            }
        });

        // Connect
        let (mut ws_stream, _) = tokio_tungstenite::connect_async(ws_url.as_str()).await?;

        // Send auth
        ws_stream
            .send(WsMessage::text(serde_json::to_string(&auth_message)?))
            .await?;

        // Read the auth response
        let auth_response = ws_stream.next().await;
        match auth_response {
            Some(Ok(WsMessage::Text(text))) => {
                let value: serde_json::Value = serde_json::from_str(&text)?;
                if value["status"] == "failed" {
                    let reason = value["reason"].as_str().unwrap_or("unknown").to_string();
                    return Err(CrnError::Unauthorized(reason));
                }
                // "connected" — proceed
            }
            Some(Ok(msg)) => {
                return Err(CrnError::Api {
                    status: 0,
                    body: format!("unexpected auth response: {msg}"),
                });
            }
            Some(Err(e)) => return Err(CrnError::WebSocket(Box::new(e))),
            None => {
                return Err(CrnError::Api {
                    status: 0,
                    body: "WebSocket closed before auth response".into(),
                });
            }
        }

        // Return a stream that yields LogEntry items
        Ok(async_stream::stream! {
            while let Some(msg) = ws_stream.next().await {
                match msg {
                    Ok(WsMessage::Text(text)) => {
                        match serde_json::from_str::<LogEntry>(&text) {
                            Ok(entry) => yield Ok(entry),
                            Err(e) => yield Err(CrnError::Json(e)),
                        }
                    }
                    Ok(WsMessage::Close(_)) => break,
                    Ok(_) => {} // ignore ping/pong/binary
                    Err(e) => {
                        yield Err(CrnError::WebSocket(Box::new(e)));
                        break;
                    }
                }
            }
        })
    }
}

/// Fetch the list of running VM executions from a CRN's
/// `/about/executions/list` endpoint. Unauthenticated.
///
/// Keys of the returned map are VM item hashes (hex-encoded).
pub async fn fetch_executions(
    http: &reqwest::Client,
    crn_url: &Url,
) -> Result<HashMap<String, ExecutionInfo>, CrnError> {
    let mut url = crn_url.clone();
    url.set_path("/about/executions/list");
    let resp = http.get(url).send().await?;
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(CrnError::Api { status, body });
    }
    Ok(resp.json().await?)
}

fn build_signed_pubkey_header(
    account: &impl Account,
    domain: &str,
    ephemeral_key: &SigningKey,
) -> Result<String, CrnError> {
    let jwk = p256_pubkey_to_jwk(ephemeral_key.verifying_key());
    let expires = (Utc::now() + chrono::Duration::hours(24))
        .format("%Y-%m-%dT%H:%M:%S.%6fZ")
        .to_string();

    let payload = serde_json::json!({
        "pubkey": jwk,
        "alg": "ECDSA",
        "domain": domain,
        "address": account.address().to_string(),
        "expires": expires,
        "chain": account.chain().to_string(),
    });

    // The Python SDK signs the raw JSON bytes (not the hex string).
    // encode_defunct(hexstr=H) decodes hex -> JSON bytes, then sign_raw
    // applies EIP-191 to those bytes. Our sign_raw also applies EIP-191
    // internally, so we pass the JSON bytes directly.
    let payload_json = serde_json::to_string(&payload).unwrap();
    let payload_hex = hex::encode(payload_json.as_bytes());
    let signature = account.sign_raw(payload_json.as_bytes())?;

    Ok(serde_json::to_string(&serde_json::json!({
        "sender": account.address().to_string(),
        "payload": payload_hex,
        "signature": signature.as_str(),
        "content": { "domain": domain },
    }))
    .unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::chain::Chain;

    #[test]
    fn deserialize_allocation_response_success() {
        let json = r#"{
            "success": true,
            "successful": true,
            "failing": [],
            "errors": {}
        }"#;
        let resp: AllocationResponse = serde_json::from_str(json).unwrap();
        assert!(resp.success);
        assert!(resp.successful);
        assert!(resp.failing.is_empty());
        assert!(resp.errors.is_empty());
    }

    #[test]
    fn deserialize_allocation_response_partial_failure() {
        let json = r#"{
            "success": false,
            "successful": false,
            "failing": ["abc123"],
            "errors": {"abc123": "RuntimeError('boom')"}
        }"#;
        let resp: AllocationResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.success);
        assert_eq!(resp.failing, vec!["abc123"]);
        assert_eq!(resp.errors["abc123"], "RuntimeError('boom')");
    }

    #[test]
    fn deserialize_log_entry_stdout() {
        let json = r#"{"type": "stdout", "message": "hello world"}"#;
        let entry: LogEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.log_type, LogType::Stdout);
        assert_eq!(entry.message, "hello world");
    }

    #[test]
    fn deserialize_log_entry_system() {
        let json = r#"{"type": "system", "message": "VM is starting"}"#;
        let entry: LogEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.log_type, LogType::System);
    }

    #[test]
    fn deserialize_backup_metadata_minimal() {
        let json = r#"{
            "backup_id": "abc_123",
            "size": 12345,
            "checksum": "sha256:deadbeef",
            "expires_at": "2026-05-24T12:00:00.000000Z",
            "download_url": "https://crn.example/control/machine/abc/backup/abc_123?signature=x&expires=1"
        }"#;
        let meta: BackupMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(meta.backup_id, "abc_123");
        assert_eq!(meta.size, 12345);
        assert_eq!(meta.checksum, "sha256:deadbeef");
        assert!(meta.volumes.is_empty());
        assert!(meta.extra.is_empty());
    }

    #[test]
    fn deserialize_backup_metadata_with_volumes_and_extra() {
        let json = r#"{
            "backup_id": "abc_123",
            "size": 12345,
            "checksum": "sha256:deadbeef",
            "expires_at": "2026-05-24T12:00:00.000000Z",
            "download_url": "https://crn.example/path",
            "volumes": ["data", "cache"],
            "future_field": "ignored-but-preserved"
        }"#;
        let meta: BackupMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(meta.volumes, vec!["data".to_string(), "cache".to_string()]);
        assert_eq!(meta.extra["future_field"], "ignored-but-preserved");
        assert_eq!(meta.extra.len(), 1, "extra should only contain unknown fields, not absorb known ones");
    }

    #[test]
    fn p256_pubkey_to_jwk_format() {
        let signing_key = SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng);
        let verifying_key = signing_key.verifying_key();
        let jwk = p256_pubkey_to_jwk(verifying_key);

        assert_eq!(jwk["kty"], "EC");
        assert_eq!(jwk["crv"], "P-256");
        // x and y are base64url-no-pad strings of 32 bytes -> 43 chars
        assert_eq!(jwk["x"].as_str().unwrap().len(), 43);
        assert_eq!(jwk["y"].as_str().unwrap().len(), 43);
        // Must not contain padding
        assert!(!jwk["x"].as_str().unwrap().contains('='));
    }

    #[test]
    fn sign_operation_header_structure() {
        let signing_key = SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng);
        let domain = "node.example.com";
        let method = "POST";
        let path = "/control/machine/abc123/stop";

        let header = sign_operation(&signing_key, domain, method, path);

        let parsed: serde_json::Value = serde_json::from_str(&header).unwrap();
        let payload_hex = parsed["payload"].as_str().unwrap();
        let sig_hex = parsed["signature"].as_str().unwrap();

        // Payload is hex-encoded JSON
        let payload_bytes = hex::decode(payload_hex).unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).unwrap();
        assert_eq!(payload["method"], "POST");
        assert_eq!(payload["path"], "/control/machine/abc123/stop");
        assert_eq!(payload["domain"], "node.example.com");
        assert!(payload["time"].as_str().unwrap().ends_with("Z"));

        // Signature is hex-encoded and non-empty
        assert!(!sig_hex.is_empty());
        let sig_bytes = hex::decode(sig_hex).unwrap();
        assert!(!sig_bytes.is_empty());
    }

    #[test]
    fn sign_operation_signature_verifies() {
        use p256::ecdsa::{Signature, signature::Verifier};

        let signing_key = SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng);
        let verifying_key = signing_key.verifying_key();
        let header = sign_operation(&signing_key, "example.com", "POST", "/test");

        let parsed: serde_json::Value = serde_json::from_str(&header).unwrap();
        let payload_hex = parsed["payload"].as_str().unwrap();
        let sig_hex = parsed["signature"].as_str().unwrap();

        let payload_bytes = hex::decode(payload_hex).unwrap();
        let sig_bytes = hex::decode(sig_hex).unwrap();
        let signature = Signature::from_der(&sig_bytes)
            .or_else(|_| Signature::from_slice(&sig_bytes))
            .unwrap();

        verifying_key.verify(&payload_bytes, &signature).unwrap();
    }

    #[test]
    fn update_instance_config_url_format() {
        // We can't easily mock reqwest here without bringing in wiremock; this
        // pin documents the URL path that the method uses. The runtime test
        // would otherwise just duplicate what the implementation says.
        let base = Url::parse("https://crn.example.com").unwrap();
        let vm_id_str = "1111111111111111111111111111111111111111111111111111111111111111";
        let joined = base
            .join(&format!("/control/machine/{vm_id_str}/update"))
            .unwrap();
        assert_eq!(
            joined.path(),
            "/control/machine/1111111111111111111111111111111111111111111111111111111111111111/update"
        );
    }

    #[test]
    fn deserialize_active_vm_list_full_shape() {
        let json = serde_json::json!({
            "1111111111111111111111111111111111111111111111111111111111111111": {
                "networking": {
                    "mapped_ports": {
                        "80":  { "host": 24001 },
                        "443": { "host": 24002, "protocol": "tcp" }
                    }
                },
                "irrelevant_field": "ignored"
            }
        });
        let list: ActiveVmList = serde_json::from_value(json).unwrap();
        assert_eq!(list.0.len(), 1);
        let (_, vm) = list.0.iter().next().unwrap();
        let net = vm.networking.as_ref().unwrap();
        assert_eq!(net.mapped_ports.get(&80).unwrap().host, 24001);
        assert_eq!(net.mapped_ports.get(&443).unwrap().host, 24002);
        assert_eq!(
            net.mapped_ports.get(&443).unwrap().extra.get("protocol"),
            Some(&serde_json::json!("tcp"))
        );
    }

    #[test]
    fn deserialize_active_vm_list_missing_networking() {
        let json = serde_json::json!({
            "1111111111111111111111111111111111111111111111111111111111111111": {}
        });
        let list: ActiveVmList = serde_json::from_value(json).unwrap();
        let (_, vm) = list.0.iter().next().unwrap();
        assert!(vm.networking.is_none());
    }

    #[test]
    fn deserialize_active_vm_list_empty() {
        let json = serde_json::json!({});
        let list: ActiveVmList = serde_json::from_value(json).unwrap();
        assert!(list.0.is_empty());
    }

    #[cfg(feature = "account-evm")]
    #[test]
    fn crn_client_new_succeeds() {
        use aleph_types::account::EvmAccount;

        let account = EvmAccount::new(Chain::Ethereum, &[1u8; 32]).unwrap();
        let url = Url::parse("https://node.example.com").unwrap();
        let client = CrnClient::new(&account, url).unwrap();

        assert_eq!(client.domain, "node.example.com");
    }

    #[cfg(feature = "account-evm")]
    #[test]
    fn build_signed_pubkey_header_structure() {
        use aleph_types::account::EvmAccount;

        let account = EvmAccount::new(Chain::Ethereum, &[1u8; 32]).unwrap();
        let domain = "node.example.com";
        let signing_key = SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng);

        let header = build_signed_pubkey_header(&account, domain, &signing_key).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&header).unwrap();
        assert_eq!(parsed["sender"], account.address().to_string());
        assert!(!parsed["payload"].as_str().unwrap().is_empty());
        assert!(parsed["signature"].as_str().unwrap().starts_with("0x"));
        assert_eq!(parsed["content"]["domain"], domain);

        // The payload is hex-encoded JSON -- decode and verify structure
        let payload_hex = parsed["payload"].as_str().unwrap();
        let payload_bytes = hex::decode(payload_hex).unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).unwrap();
        assert_eq!(payload["alg"], "ECDSA");
        assert_eq!(payload["domain"], domain);
        assert_eq!(payload["address"], account.address().to_string());
        assert_eq!(payload["chain"], "ETH");
        assert!(payload["pubkey"]["kty"] == "EC");
        assert!(payload["expires"].as_str().unwrap().ends_with("Z"));
    }

    #[test]
    fn deserialize_restore_response() {
        let json = r#"{
            "status": "restored",
            "vm_hash": "abc123",
            "old_rootfs_backup": "/var/lib/aleph/backups/abc123-old.qcow2"
        }"#;
        let resp: RestoreResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "restored");
        assert_eq!(resp.vm_hash, "abc123");
        assert_eq!(
            resp.old_rootfs_backup.as_deref(),
            Some("/var/lib/aleph/backups/abc123-old.qcow2")
        );
    }

    #[cfg(feature = "account-evm")]
    #[tokio::test]
    async fn create_backup_returns_started_on_202() {
        use aleph_types::account::EvmAccount;
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let vm = "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99";
        Mock::given(method("POST"))
            .and(path(format!("/control/machine/{vm}/backup")))
            .and(query_param("include_volumes", "true"))
            .and(query_param("skip_fsfreeze", "true"))
            .respond_with(ResponseTemplate::new(202).set_body_string(""))
            .mount(&server)
            .await;

        let account = EvmAccount::new(Chain::Ethereum, &[1u8; 32]).unwrap();
        let url = Url::parse(&server.uri()).unwrap();
        let client = CrnClient::new(&account, url).unwrap();
        let result = client
            .create_backup(
                &vm.parse().unwrap(),
                CreateBackupOpts {
                    include_volumes: true,
                    skip_fsfreeze: true,
                },
            )
            .await
            .unwrap();
        assert!(matches!(result, CreateBackup::Started));
    }

    #[cfg(feature = "account-evm")]
    #[tokio::test]
    async fn create_backup_returns_complete_on_200() {
        use aleph_types::account::EvmAccount;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let vm = "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99";
        Mock::given(method("POST"))
            .and(path(format!("/control/machine/{vm}/backup")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "backup_id": "abc_1",
                "size": 100,
                "checksum": "sha256:deadbeef",
                "expires_at": "2026-05-24T12:00:00.000000Z",
                "download_url": "https://crn.example/path"
            })))
            .mount(&server)
            .await;

        let account = EvmAccount::new(Chain::Ethereum, &[1u8; 32]).unwrap();
        let url = Url::parse(&server.uri()).unwrap();
        let client = CrnClient::new(&account, url).unwrap();
        let result = client
            .create_backup(&vm.parse().unwrap(), CreateBackupOpts::default())
            .await
            .unwrap();
        match result {
            CreateBackup::Complete(meta) => {
                assert_eq!(meta.backup_id, "abc_1");
                assert_eq!(meta.size, 100);
            }
            CreateBackup::Started => panic!("expected Complete"),
        }
    }
}
