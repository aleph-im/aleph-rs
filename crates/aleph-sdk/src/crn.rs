use std::collections::HashMap;
use std::time::Duration;

use aleph_types::account::{Account, SignError};
use aleph_types::item_hash::ItemHash;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use futures_util::{SinkExt, Stream, StreamExt};
use p256::ecdsa::{SigningKey, signature::Signer};
use serde::Deserialize;
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
}
