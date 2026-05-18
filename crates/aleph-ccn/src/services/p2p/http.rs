//! Direct HTTP client to other CCNs. Mirrors `aleph/services/p2p/http.py`.
//!
//! While libp2p remains unstable, peers exchange content over plain HTTP via
//! their `/api/v0/...` REST endpoints. The Python version keeps one
//! `aiohttp.ClientSession` per timeout and a 5-connection-per-host pool;
//! `reqwest::Client` already pools by default so we keep a single shared
//! client per crate instance and pass the timeout per call.

use std::sync::OnceLock;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use serde_json::Value;

fn shared_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .pool_max_idle_per_host(5)
            .build()
            .expect("reqwest client build")
    })
}

/// In-place uniform shuffle of `v`. Uses `rand::thread_rng()` so we get an
/// unbiased permutation backed by the standard RNG — the previous custom
/// xorshift suffered from modulo bias.
fn fisher_yates_shuffle<T>(v: &mut [T]) {
    use rand::seq::SliceRandom;
    let mut rng = rand::thread_rng();
    v.shuffle(&mut rng);
}

/// Perform a GET against `<base_uri>/api/v0/<method>` and return the decoded
/// JSON body. Mirrors `api_get_request`.
///
/// Returns `None` on any network / decode failure, matching Python.
pub async fn api_get_request(base_uri: &str, method: &str, timeout: Duration) -> Option<Value> {
    let uri = format!("{}/api/v0/{}", base_uri.trim_end_matches('/'), method);
    let client = shared_client();
    let resp = match client.get(&uri).timeout(timeout).send().await {
        Ok(r) => r,
        Err(_) => return None,
    };
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<Value>().await.ok()
}

/// Pull a stored item from `<base_uri>` and base64-decode the payload.
/// Mirrors `get_peer_hash_content`.
pub async fn get_peer_hash_content(
    base_uri: &str,
    item_hash: &str,
    timeout: Duration,
) -> Option<Bytes> {
    let path = format!("storage/{item_hash}");
    let item = api_get_request(base_uri, &path, timeout).await?;
    if item.get("status").and_then(Value::as_str)? != "success" {
        return None;
    }
    let content = item.get("content").and_then(Value::as_str)?;
    base64::engine::general_purpose::STANDARD
        .decode(content)
        .ok()
        .map(Bytes::from)
}

/// Try each API server in random order until one returns a payload.
/// Mirrors `request_hash`.
pub async fn request_hash(
    api_servers: &[String],
    item_hash: &str,
    timeout: Duration,
) -> Option<Bytes> {
    if api_servers.is_empty() {
        return None;
    }
    let mut shuffled = api_servers.to_vec();
    fisher_yates_shuffle(&mut shuffled);
    for uri in shuffled {
        if let Some(content) = get_peer_hash_content(&uri, item_hash, timeout).await {
            return Some(content);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn api_get_request_returns_json_on_200() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/version"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"version":"1.0"}"#))
            .expect(1)
            .mount(&server)
            .await;
        let v = api_get_request(&server.uri(), "version", Duration::from_secs(2)).await;
        assert_eq!(
            v.unwrap().get("version").and_then(Value::as_str),
            Some("1.0")
        );
    }

    #[tokio::test]
    async fn api_get_request_returns_none_on_500() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/version"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        assert!(
            api_get_request(&server.uri(), "version", Duration::from_secs(2))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn get_peer_hash_content_base64_decodes() {
        let server = MockServer::start().await;
        let payload = base64::engine::general_purpose::STANDARD.encode(b"hello world");
        let body = format!("{{\"status\":\"success\",\"content\":\"{payload}\"}}");
        Mock::given(method("GET"))
            .and(path("/api/v0/storage/QmFoo"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .expect(1)
            .mount(&server)
            .await;
        let content = get_peer_hash_content(&server.uri(), "QmFoo", Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(content.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn request_hash_picks_first_successful() {
        let ok_server = MockServer::start().await;
        let payload = base64::engine::general_purpose::STANDARD.encode(b"data");
        Mock::given(method("GET"))
            .and(path("/api/v0/storage/QmFoo"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "{{\"status\":\"success\",\"content\":\"{payload}\"}}"
            )))
            .mount(&ok_server)
            .await;
        let servers = vec![ok_server.uri()];
        let res = request_hash(&servers, "QmFoo", Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(res.as_ref(), b"data");
    }

    #[tokio::test]
    async fn request_hash_returns_none_when_empty() {
        assert!(
            request_hash(&[], "QmFoo", Duration::from_secs(1))
                .await
                .is_none()
        );
    }
}
