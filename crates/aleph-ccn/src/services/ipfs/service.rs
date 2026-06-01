//! Async IPFS gateway client. Mirrors `aleph/services/ipfs/service.py`.
//!
//! Python uses `aioipfs.AsyncIPFS`. The Rust port talks directly to the kubo
//! HTTP API (`/api/v0/...`) with `reqwest::Client`. Method names mirror the
//! Python class verbatim, although Rust uses `snake_case` and `cat` returns
//! `Bytes` instead of `Optional[bytes]`.

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::BoxStream;
use serde::Deserialize;
use serde_json::Value;
use tokio_stream::StreamExt;

use super::common::{
    IpfsEndpoint, make_ipfs_client, make_ipfs_p2p_endpoint, make_ipfs_pinning_endpoint,
    should_use_separate_pinning_client,
};
use crate::config::IpfsSettings;
use crate::{AlephError, AlephResult};

/// Hard cap mirroring `MAX_LEN = 1024 * 1024 * 100` in Python.
pub const MAX_LEN: u64 = 1024 * 1024 * 100;

/// Result of [`IpfsService::stat`]. Mirrors Python's
/// `_get_file_stats_from_ipfs` return shape but exposed at the IPFS service
/// layer so handlers can reuse it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpfsFileStat {
    /// Total byte size: `Size` for files, `CumulativeSize` for directories.
    pub size: u64,
    /// `"file"`, `"directory"`, etc — passed straight from the kubo response.
    pub file_type: String,
    /// True when [`file_type`] is anything but `"file"`.
    pub is_directory: bool,
}

/// Async IPFS gateway client. Mirrors `aleph.services.ipfs.service.IpfsService`.
#[derive(Clone)]
pub struct IpfsService {
    pub client: reqwest::Client,
    pub pinning_client: reqwest::Client,
    pub p2p_endpoint: IpfsEndpoint,
    pub pinning_endpoint: IpfsEndpoint,
    /// True when `pinning_client` is shared with `client`.
    pub shared_clients: bool,
}

impl std::fmt::Debug for IpfsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpfsService")
            .field("p2p_endpoint", &self.p2p_endpoint)
            .field("pinning_endpoint", &self.pinning_endpoint)
            .field("shared_clients", &self.shared_clients)
            .finish()
    }
}

impl IpfsService {
    /// Build a service from `IpfsSettings`. Mirrors
    /// `IpfsService.new(config: Config)`.
    pub fn new(settings: &IpfsSettings) -> AlephResult<Self> {
        let p2p_endpoint = make_ipfs_p2p_endpoint(settings, Duration::from_secs(60));
        let client = make_ipfs_client(p2p_endpoint.timeout)?;

        if should_use_separate_pinning_client(settings) {
            tracing::info!("Using separate IPFS client for pinning operations");
            let pinning_endpoint = make_ipfs_pinning_endpoint(settings);
            let pinning_client = make_ipfs_client(pinning_endpoint.timeout)?;
            Ok(Self {
                client,
                pinning_client,
                p2p_endpoint,
                pinning_endpoint,
                shared_clients: false,
            })
        } else {
            let pinning_endpoint = make_ipfs_pinning_endpoint(settings);
            Ok(Self {
                pinning_client: client.clone(),
                client,
                p2p_endpoint,
                pinning_endpoint,
                shared_clients: true,
            })
        }
    }

    /// Construct a service from pre-built clients/endpoints. Used by tests.
    pub fn from_parts(
        client: reqwest::Client,
        pinning_client: Option<reqwest::Client>,
        p2p_endpoint: IpfsEndpoint,
        pinning_endpoint: IpfsEndpoint,
    ) -> Self {
        match pinning_client {
            Some(pc) => Self {
                client,
                pinning_client: pc,
                p2p_endpoint,
                pinning_endpoint,
                shared_clients: false,
            },
            None => Self {
                pinning_client: client.clone(),
                client,
                p2p_endpoint,
                pinning_endpoint,
                shared_clients: true,
            },
        }
    }

    fn p2p_url(&self, method: &str) -> String {
        self.p2p_endpoint.api_url(method)
    }

    fn pinning_url(&self, method: &str) -> String {
        self.pinning_endpoint.api_url(method)
    }

    /// `swarm.connect`. Mirrors `IpfsService.connect`.
    pub async fn connect(&self, peer: &str) -> AlephResult<Value> {
        let resp = self
            .client
            .post(self.p2p_url("swarm/connect"))
            .query(&[("arg", peer)])
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("swarm/connect failed: {e}")))?;
        ensure_ok(&resp).await?;
        resp.json::<Value>()
            .await
            .map_err(|e| AlephError::Ipfs(format!("swarm/connect json: {e}")))
    }

    /// Return a probable public multiaddress. Mirrors
    /// `IpfsService.get_public_address`.
    pub async fn get_public_address(&self) -> AlephResult<String> {
        let public_ip = crate::services::utils::get_ip()
            .await
            .unwrap_or_else(|_| "127.0.0.1".to_string());
        let resp = self
            .client
            .post(self.p2p_url("id"))
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("id call failed: {e}")))?;
        ensure_ok(&resp).await?;
        let id: Value = resp
            .json()
            .await
            .map_err(|e| AlephError::Ipfs(format!("id json: {e}")))?;

        let addresses = id
            .get("Addresses")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let as_strs: Vec<String> = addresses
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        for addr in &as_strs {
            if addr.contains(&public_ip) && addr.contains("/tcp") && addr.contains("/p2p") {
                return Ok(addr.clone());
            }
        }
        for addr in &as_strs {
            if !addr.contains("127.0.0.1") && addr.contains("/tcp") && addr.contains("/p2p") {
                return Ok(addr.clone());
            }
        }
        for addr in &as_strs {
            if addr.contains("127.0.0.1") && addr.contains("/tcp") && addr.contains("/p2p") {
                return Ok(addr.replace("127.0.0.1", &public_ip));
            }
        }
        Err(AlephError::Ipfs("no public address available".into()))
    }

    /// Return the size in bytes of an IPFS object. Mirrors
    /// `IpfsService.get_ipfs_size`.
    pub async fn get_ipfs_size(
        &self,
        hash: &str,
        timeout: Duration,
        tries: u32,
    ) -> AlephResult<Option<u64>> {
        let mut try_count = 0u32;
        let mut result: Option<u64> = None;
        while result.is_none() && try_count < tries {
            try_count += 1;
            let dag = match tokio::time::timeout(
                timeout,
                self.client
                    .post(self.p2p_url("dag/get"))
                    .query(&[("arg", hash)])
                    .send(),
            )
            .await
            {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    // Python: ClientConnectorError / CancelledError -> don't count as
                    // a try, sleep briefly. APIError (HTTP-level) sleeps 0.5s and
                    // counts. reqwest::Error::is_connect / is_timeout maps to the
                    // first category.
                    if e.is_connect() || e.is_timeout() {
                        try_count = try_count.saturating_sub(1);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    } else {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    continue;
                }
                Err(_) => {
                    // A timeout already consumed the full `timeout` budget, so it
                    // is its own backoff: retry immediately, or give up once the
                    // try budget is exhausted. Mirrors pyaleph #1169.
                    if try_count >= tries {
                        return Err(AlephError::Ipfs(format!(
                            "could not retrieve IPFS content at this time ({hash})"
                        )));
                    }
                    continue;
                }
            };
            if !dag.status().is_success() {
                let status = dag.status();
                let body = dag.text().await.unwrap_or_default();
                tracing::warn!(
                    "ipfs dag/get error for {hash}: status {status} body: {body}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            let text = match dag.text().await {
                Ok(t) => t,
                Err(e) => {
                    if e.is_connect() || e.is_timeout() {
                        try_count = try_count.saturating_sub(1);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    } else {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    continue;
                }
            };
            let dag_node: Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => {
                    // dag.get may return raw bytes for raw blocks. Fallback to block/stat.
                    return self.block_stat_size(hash, timeout).await.map(Some);
                }
            };
            let mut size: u64 = 0;
            if let Some(obj) = dag_node.as_object() {
                if let Some(data) = obj.get("Data").and_then(|d| d.as_object()) {
                    if let Some(filesize) = data.get("filesize").and_then(Value::as_u64) {
                        size = filesize;
                    } else if let Some(tsize) = data.get("Tsize").and_then(Value::as_u64) {
                        size = tsize;
                    } else if let Some(tsize) = obj.get("Tsize").and_then(Value::as_u64) {
                        size = tsize;
                    }
                }
                if size == 0 {
                    if let Some(links) = obj.get("Links").and_then(Value::as_array) {
                        let mut total: u64 = 0;
                        for link in links {
                            if let Some(t) = link.get("Tsize").and_then(Value::as_u64) {
                                total += t;
                            } else {
                                tracing::error!(
                                    "Error: CID {} did not return a list structure",
                                    hash
                                );
                            }
                        }
                        size = total;
                    } else if let Some(s) = obj.get("Size").and_then(Value::as_u64) {
                        size = s;
                    }
                }
            } else {
                tracing::info!(
                    "Warning: CID {} did not return a dictionary structure",
                    hash
                );
            }

            if size == 0 {
                tracing::info!(
                    "CID {} didn't return a Size field. Executing block stat",
                    hash
                );
                size = self.block_stat_size(hash, timeout).await?;
            }

            result = Some(size);
        }
        Ok(result)
    }

    /// Call the IPFS `/api/v0/files/stat` endpoint for `/ipfs/<cid>` and
    /// return its (size, type, is_directory) tuple. Mirrors Python's
    /// `_get_file_stats_from_ipfs` in `aleph.handlers.content.store`.
    ///
    /// The Python implementation uses the cumulative size for directories
    /// (since `Size` is 0 for them) and the regular size for files.
    pub async fn stat(&self, cid: &str, timeout: Duration) -> AlephResult<IpfsFileStat> {
        let resp = tokio::time::timeout(
            timeout,
            self.client
                .post(self.p2p_url("files/stat"))
                .query(&[("arg", &format!("/ipfs/{cid}"))])
                .send(),
        )
        .await
        .map_err(|_| {
            AlephError::Ipfs(format!(
                "Timeout ({}s) while retrieving stats of hash {cid}",
                timeout.as_secs()
            ))
        })?
        .map_err(|e| AlephError::Ipfs(format!("files/stat: {e}")))?;
        ensure_ok(&resp).await?;
        let stat: Value = resp
            .json()
            .await
            .map_err(|e| AlephError::Ipfs(format!("files/stat json: {e}")))?;
        let type_str = stat
            .get("Type")
            .and_then(Value::as_str)
            .unwrap_or("file")
            .to_string();
        let is_directory = type_str != "file";
        let size = if is_directory {
            // Folders report Size=0, fall back to CumulativeSize.
            stat.get("CumulativeSize")
                .and_then(Value::as_u64)
                .unwrap_or(0)
        } else {
            stat.get("Size").and_then(Value::as_u64).unwrap_or(0)
        };
        Ok(IpfsFileStat {
            size,
            file_type: type_str,
            is_directory,
        })
    }

    async fn block_stat_size(&self, hash: &str, timeout: Duration) -> AlephResult<u64> {
        let resp = tokio::time::timeout(
            timeout,
            self.client
                .post(self.p2p_url("block/stat"))
                .query(&[("arg", hash)])
                .send(),
        )
        .await
        .map_err(|_| AlephError::Ipfs("Could not retrieve IPFS content at this time".into()))?
        .map_err(|e| AlephError::Ipfs(format!("block/stat: {e}")))?;
        ensure_ok(&resp).await?;
        let stat: Value = resp
            .json()
            .await
            .map_err(|e| AlephError::Ipfs(format!("block/stat json: {e}")))?;
        Ok(stat.get("Size").and_then(Value::as_u64).unwrap_or(0))
    }

    /// Cat content. Mirrors `IpfsService.get_ipfs_content`.
    pub async fn get_ipfs_content(
        &self,
        hash: &str,
        timeout: Duration,
        tries: u32,
    ) -> AlephResult<Option<Bytes>> {
        let mut try_count = 0u32;
        while try_count < tries {
            try_count += 1;
            let req = self
                .client
                .post(self.p2p_url("cat"))
                .query(&[("arg", hash), ("length", &MAX_LEN.to_string())]);
            let resp = match tokio::time::timeout(timeout, req.send()).await {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    tracing::warn!("ipfs cat send error for {hash}: {e}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!("ipfs get error body: {body} (status {status} hash {hash})");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            let bytes = match tokio::time::timeout(timeout, resp.bytes()).await {
                Ok(Ok(b)) => b,
                Ok(Err(e)) => {
                    tracing::warn!("ipfs cat body error for {hash}: {e}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };
            if bytes.len() as u64 == MAX_LEN {
                // Truncated, Python returns None.
                return Ok(None);
            }
            return Ok(Some(bytes));
        }
        Ok(None)
    }

    /// Streaming `cat`. Mirrors `IpfsService.get_ipfs_content_iterator`.
    pub async fn get_ipfs_content_iterator(
        &self,
        cid: &str,
    ) -> AlephResult<BoxStream<'static, AlephResult<Bytes>>> {
        let resp = self
            .client
            .post(self.p2p_url("cat"))
            .query(&[("arg", cid)])
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("cat: {e}")))?;
        ensure_ok(&resp).await?;
        Ok(Box::pin(resp.bytes_stream().map(|chunk| {
            chunk.map_err(|e| AlephError::Ipfs(format!("cat stream: {e}")))
        })))
    }

    /// Streaming `get` (returns a tar archive). Mirrors
    /// `IpfsService.get_ipfs_directory_iterator`.
    pub async fn get_ipfs_directory_iterator(
        &self,
        cid: &str,
    ) -> AlephResult<BoxStream<'static, AlephResult<Bytes>>> {
        let resp = self
            .client
            .post(self.p2p_url("get"))
            .query(&[("arg", cid), ("archive", "true"), ("compress", "false")])
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("get: {e}")))?;
        ensure_ok(&resp).await?;
        Ok(Box::pin(resp.bytes_stream().map(|chunk| {
            chunk.map_err(|e| AlephError::Ipfs(format!("get stream: {e}")))
        })))
    }

    /// Fetch and JSON-decode an IPFS file. Mirrors `IpfsService.get_json`.
    pub async fn get_json(
        &self,
        hash: &str,
        timeout: Duration,
        tries: u32,
    ) -> AlephResult<Option<Value>> {
        let content = self.get_ipfs_content(hash, timeout, tries).await?;
        let Some(bytes) = content else {
            return Ok(None);
        };
        match serde_json::from_slice::<Value>(&bytes) {
            Ok(v) => Ok(Some(v)),
            Err(_) => {
                tracing::error!("Can't decode JSON for {hash}");
                // Python returns -1 in that case; we surface that as Ok(None).
                Ok(None)
            }
        }
    }

    /// Add a JSON value. Mirrors `IpfsService.add_json`.
    pub async fn add_json(&self, value: &Value) -> AlephResult<String> {
        let bytes = serde_json::to_vec(value)?;
        self.add_bytes_inner(bytes, 0, true).await
    }

    /// Add raw bytes. Mirrors `IpfsService.add_bytes`.
    ///
    /// `aioipfs.AsyncIPFS.add_bytes` defaults to `pin=True`, so we pin by default
    /// to match Python semantics.
    pub async fn add_bytes(&self, value: Bytes, cid_version: u8) -> AlephResult<String> {
        self.add_bytes_inner(value.to_vec(), cid_version, true)
            .await
    }

    /// Convenience: add an in-memory file as bytes and return the CID.
    pub async fn add_file(&self, data: Bytes) -> AlephResult<String> {
        self.add_bytes(data, 0).await
    }

    /// Add a file from disk using a streaming multipart part.
    pub async fn add_file_path(&self, path: &Path, cid_version: u8) -> AlephResult<String> {
        self.add_path_inner(path, cid_version, true, "application/octet-stream")
            .await
    }

    /// Stream a CAR file into kubo `/api/v0/dag/import` and return imported
    /// root CIDs. Mirrors Python `IpfsService.dag_import`.
    pub async fn dag_import(&self, car: Bytes, pin_roots: bool) -> AlephResult<Vec<String>> {
        let part = reqwest::multipart::Part::bytes(car.to_vec())
            .file_name("upload.car")
            .mime_str("application/vnd.ipld.car")
            .map_err(|e| AlephError::Ipfs(format!("dag/import multipart: {e}")))?;
        let form = reqwest::multipart::Form::new().part("file", part);
        let pin_roots_s = if pin_roots { "true" } else { "false" };
        let resp = self
            .pinning_client
            .post(self.pinning_url("dag/import"))
            .query(&[
                ("pin-roots", pin_roots_s),
                ("silent", "false"),
                ("stats", "false"),
            ])
            .multipart(form)
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("dag/import: {e}")))?;
        ensure_ok(&resp).await?;
        let body = resp
            .bytes()
            .await
            .map_err(|e| AlephError::Ipfs(format!("dag/import body: {e}")))?;
        parse_dag_import_response(&body)
    }

    /// Stream a CAR file path into kubo `/api/v0/dag/import`.
    pub async fn dag_import_path(
        &self,
        car_path: &Path,
        pin_roots: bool,
    ) -> AlephResult<Vec<String>> {
        let part = reqwest::multipart::Part::file(car_path)
            .await
            .map_err(|e| AlephError::Ipfs(format!("dag/import open file: {e}")))?
            .mime_str("application/vnd.ipld.car")
            .map_err(|e| AlephError::Ipfs(format!("dag/import multipart: {e}")))?;
        let form = reqwest::multipart::Form::new().part("file", part);
        let pin_roots_s = if pin_roots { "true" } else { "false" };
        let resp = self
            .pinning_client
            .post(self.pinning_url("dag/import"))
            .query(&[
                ("pin-roots", pin_roots_s),
                ("silent", "false"),
                ("stats", "false"),
            ])
            .multipart(form)
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("dag/import: {e}")))?;
        ensure_ok(&resp).await?;
        let body = resp
            .bytes()
            .await
            .map_err(|e| AlephError::Ipfs(format!("dag/import body: {e}")))?;
        parse_dag_import_response(&body)
    }

    async fn add_bytes_inner(
        &self,
        value: Vec<u8>,
        cid_version: u8,
        pin: bool,
    ) -> AlephResult<String> {
        let url = self.pinning_url("add");
        let part = reqwest::multipart::Part::bytes(value)
            .file_name("file")
            .mime_str("application/octet-stream")
            .map_err(|e| AlephError::Ipfs(format!("multipart: {e}")))?;
        let form = reqwest::multipart::Form::new().part("file", part);
        let cid_version_s = cid_version.to_string();
        let pin_s = if pin { "true" } else { "false" };
        let resp = self
            .pinning_client
            .post(&url)
            .query(&[("cid-version", cid_version_s.as_str()), ("pin", pin_s)])
            .multipart(form)
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("add: {e}")))?;
        ensure_ok(&resp).await?;
        // Kubo /add returns NDJSON; the last entry has the root hash.
        let body = resp
            .text()
            .await
            .map_err(|e| AlephError::Ipfs(format!("add text: {e}")))?;
        parse_add_response(&body)
    }

    async fn add_path_inner(
        &self,
        path: &Path,
        cid_version: u8,
        pin: bool,
        mime: &str,
    ) -> AlephResult<String> {
        let url = self.pinning_url("add");
        let part = reqwest::multipart::Part::file(path)
            .await
            .map_err(|e| AlephError::Ipfs(format!("add open file: {e}")))?
            .mime_str(mime)
            .map_err(|e| AlephError::Ipfs(format!("multipart: {e}")))?;
        let form = reqwest::multipart::Form::new().part("file", part);
        let cid_version_s = cid_version.to_string();
        let pin_s = if pin { "true" } else { "false" };
        let resp = self
            .pinning_client
            .post(&url)
            .query(&[("cid-version", cid_version_s.as_str()), ("pin", pin_s)])
            .multipart(form)
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("add: {e}")))?;
        ensure_ok(&resp).await?;
        let body = resp
            .text()
            .await
            .map_err(|e| AlephError::Ipfs(format!("add text: {e}")))?;
        parse_add_response(&body)
    }

    /// Pin a CID with progress polling. Mirrors `IpfsService.pin_add`.
    pub async fn pin_add(&self, cid: &str, timeout: Duration, tries: u32) -> AlephResult<()> {
        let mut remaining = tries.max(1);
        loop {
            match self._pin_add(cid, timeout).await {
                Ok(()) => return Ok(()),
                Err(AlephError::Ipfs(msg)) if msg.contains("could not pin IPFS content") => {
                    remaining -= 1;
                    if remaining == 0 {
                        return Err(AlephError::Ipfs(msg));
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn _pin_add(&self, cid: &str, timeout: Duration) -> AlephResult<()> {
        // pin/add streams JSON status objects. We need to check whether the
        // daemon stops making progress.
        let resp = self
            .pinning_client
            .post(self.pinning_url("pin/add"))
            .query(&[("arg", cid), ("progress", "true")])
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("pin/add: {e}")))?;
        ensure_ok(&resp).await?;

        let tick_seconds = timeout.as_secs().max(1) * 2;
        let mut tick_remaining: u64 = tick_seconds;
        let mut last_progress: Option<u64> = None;

        let mut stream = resp.bytes_stream();
        let mut buf = Vec::<u8>::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| AlephError::Ipfs(format!("pin/add stream: {e}")))?;
            buf.extend_from_slice(&chunk);

            while let Some(idx) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=idx).collect();
                let line_str = std::str::from_utf8(&line[..line.len().saturating_sub(1)])
                    .map_err(|e| AlephError::Ipfs(format!("pin/add utf8: {e}")))?;
                if line_str.trim().is_empty() {
                    continue;
                }
                let status: Value = serde_json::from_str(line_str)
                    .map_err(|e| AlephError::Ipfs(format!("pin/add bad json: {e}")))?;
                if status.get("Pins").is_some() {
                    return Ok(());
                }
                let progress = status.get("Progress").and_then(Value::as_u64);
                if progress == last_progress {
                    tick_remaining = tick_remaining.saturating_sub(1);
                    if tick_remaining == 0 {
                        let reason = if progress.is_none() {
                            "file not found"
                        } else {
                            "could not fetch some blocks"
                        };
                        return Err(AlephError::Ipfs(format!(
                            "could not pin IPFS content: {reason}"
                        )));
                    }
                } else {
                    last_progress = progress;
                    tick_remaining = tick_seconds;
                }
            }
        }
        Ok(())
    }

    /// Subscribe to a pubsub topic. Mirrors `IpfsService.sub`. Returns a
    /// boxed stream of decoded JSON status objects (as `Value`).
    pub async fn sub(&self, topic: &str) -> AlephResult<BoxStream<'static, AlephResult<Value>>> {
        let resp = self
            .client
            .post(self.p2p_url("pubsub/sub"))
            .query(&[("arg", topic)])
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("pubsub/sub: {e}")))?;
        ensure_ok(&resp).await?;
        let mut stream = resp.bytes_stream();
        let s = async_stream::try_stream! {
            let mut buf = Vec::<u8>::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| AlephError::Ipfs(format!("pubsub stream: {e}")))?;
                buf.extend_from_slice(&chunk);
                while let Some(idx) = buf.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=idx).collect();
                    let trimmed = line.strip_suffix(b"\n").unwrap_or(&line);
                    if trimmed.is_empty() {
                        continue;
                    }
                    let value: Value = serde_json::from_slice(trimmed)
                        .map_err(|e| AlephError::Ipfs(format!("pubsub bad json: {e}")))?;
                    yield decode_pubsub_payload(value);
                }
            }
        };
        Ok(Box::pin(s))
    }

    /// Publish to a pubsub topic. Mirrors `IpfsService.pub`.
    pub async fn pub_message(&self, topic: &str, message: impl AsRef<[u8]>) -> AlephResult<()> {
        let bytes = message.as_ref().to_vec();
        let part = reqwest::multipart::Part::bytes(bytes)
            .file_name("data")
            .mime_str("application/octet-stream")
            .map_err(|e| AlephError::Ipfs(format!("pub multipart: {e}")))?;
        let form = reqwest::multipart::Form::new().part("data", part);
        let resp = self
            .client
            .post(self.p2p_url("pubsub/pub"))
            .query(&[("arg", topic)])
            .multipart(form)
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("pubsub/pub: {e}")))?;
        ensure_ok(&resp).await?;
        Ok(())
    }

    /// Pubsub-publish wrapper used by the manager/publish routines.
    pub async fn pubsub_publish(&self, topic: &str, data: &str) -> AlephResult<()> {
        self.pub_message(topic, data).await
    }

    /// Pin an arbitrary CID with default settings. Convenience wrapper around
    /// [`pin_add`].
    pub async fn pin(&self, cid: &str) -> AlephResult<()> {
        self.pin_add(cid, Duration::from_secs(30), 1).await
    }

    /// Remove a pin. Mirrors a direct `/api/v0/pin/rm?arg=<cid>` call.
    pub async fn unpin(&self, cid: &str) -> AlephResult<()> {
        let resp = self
            .pinning_client
            .post(self.pinning_url("pin/rm"))
            .query(&[("arg", cid)])
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("pin/rm: {e}")))?;
        ensure_ok(&resp).await?;
        Ok(())
    }

    /// Fetch raw bytes via the streaming `cat` endpoint. Helper used by the
    /// peers/publish path.
    pub async fn cat(&self, cid: &str) -> AlephResult<Bytes> {
        let resp = self
            .client
            .post(self.p2p_url("cat"))
            .query(&[("arg", cid)])
            .send()
            .await
            .map_err(|e| AlephError::Ipfs(format!("cat: {e}")))?;
        ensure_ok(&resp).await?;
        resp.bytes()
            .await
            .map_err(|e| AlephError::Ipfs(format!("cat bytes: {e}")))
    }
}

/// Decode a single pubsub frame. Mirrors the implicit base64 decode performed
/// by `aioipfs`: the kubo API returns `{from, data, seqno, topicIDs}` with
/// `from` and `data` base64-encoded. Python's `aioipfs` returns them already
/// decoded; we replicate that here.
fn decode_pubsub_payload(mut value: Value) -> Value {
    use base64::Engine as _;
    let engine = base64::engine::general_purpose::STANDARD;
    if let Some(obj) = value.as_object_mut() {
        if let Some(Value::String(s)) = obj.get("data").cloned() {
            if let Ok(decoded) = engine.decode(&s) {
                obj.insert(
                    "data".into(),
                    Value::String(
                        String::from_utf8(decoded.clone())
                            .unwrap_or_else(|_| String::from_utf8_lossy(&decoded).into()),
                    ),
                );
            }
        }
        if let Some(Value::String(s)) = obj.get("from").cloned() {
            if let Ok(decoded) = engine.decode(&s) {
                if let Ok(text) = String::from_utf8(decoded) {
                    obj.insert("from".into(), Value::String(text));
                }
            }
        }
    }
    value
}

#[derive(Deserialize)]
struct AddEntry {
    #[serde(rename = "Hash")]
    hash: String,
}

fn parse_add_response(body: &str) -> AlephResult<String> {
    let last = body
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .last()
        .ok_or_else(|| AlephError::Ipfs("empty add response".into()))?;
    let entry: AddEntry = serde_json::from_str(last)
        .map_err(|e| AlephError::Ipfs(format!("add response bad json: {e}")))?;
    Ok(entry.hash)
}

fn parse_dag_import_response(body: &[u8]) -> AlephResult<Vec<String>> {
    let mut roots = Vec::new();
    for line in body.split(|b| *b == b'\n') {
        let line = trim_ascii(line);
        if line.is_empty() {
            continue;
        }
        let entry: Value = serde_json::from_slice(line)
            .map_err(|e| AlephError::Ipfs(format!("dag/import malformed NDJSON line: {e}")))?;
        let Some(root) = entry.get("Root") else {
            continue;
        };
        if let Some(pin_err) = root.get("PinErrorMsg").and_then(Value::as_str)
            && !pin_err.is_empty()
        {
            return Err(AlephError::Ipfs(format!("kubo pin error: {pin_err}")));
        }
        let cid = root
            .get("Cid")
            .and_then(|v| v.get("/"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AlephError::Ipfs(format!("dag/import malformed Root entry: {root}"))
            })?;
        roots.push(cid.to_string());
    }
    Ok(roots)
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map(|i| i + 1)
        .unwrap_or(start);
    &bytes[start..end]
}

async fn ensure_ok(resp: &reqwest::Response) -> AlephResult<()> {
    if !resp.status().is_success() {
        return Err(AlephError::Ipfs(format!(
            "ipfs http {}: {}",
            resp.status(),
            resp.url()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IpfsSettings;
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn service_for(server: &MockServer) -> IpfsService {
        let url = url::Url::parse(&server.uri()).unwrap();
        let host = url.host_str().unwrap().to_string();
        let port = url.port().unwrap();
        let mut s = IpfsSettings::default();
        s.host = host;
        s.port = port;
        s.scheme = "http".into();
        IpfsService::new(&s).unwrap()
    }

    #[test]
    fn parse_add_response_takes_last_line() {
        let body = "\
{\"Name\":\"a\",\"Hash\":\"QmFile1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"1\"}
{\"Name\":\"\",\"Hash\":\"QmRoot1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"100\"}
";
        let cid = parse_add_response(body).unwrap();
        assert_eq!(cid, "QmRoot1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    }

    #[test]
    fn parse_add_response_empty_is_error() {
        assert!(parse_add_response("").is_err());
    }

    #[test]
    fn parse_dag_import_response_extracts_roots() {
        let body = br#"
{"Root":{"Cid":{"/":"bafyroot"},"PinErrorMsg":""}}
{"Stats":{"BlockCount":1}}
"#;
        let roots = parse_dag_import_response(body).unwrap();
        assert_eq!(roots, vec!["bafyroot"]);
    }

    #[test]
    fn parse_dag_import_response_rejects_pin_error() {
        let body = br#"{"Root":{"Cid":{"/":"bafyroot"},"PinErrorMsg":"bad pin"}}"#;
        let err = parse_dag_import_response(body).unwrap_err();
        assert!(err.to_string().contains("bad pin"));
    }

    #[tokio::test]
    async fn add_bytes_posts_multipart_with_query() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .and(query_param("cid-version", "0"))
            .and(query_param("pin", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "{\"Name\":\"file\",\"Hash\":\"QmFile1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"5\"}\n",
            ))
            .expect(1)
            .mount(&server)
            .await;

        let s = service_for(&server);
        let cid = s.add_bytes(Bytes::from_static(b"hello"), 0).await.unwrap();
        assert_eq!(cid, "QmFile1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    }

    #[tokio::test]
    async fn add_file_path_posts_streamed_file_with_query() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .and(query_param("cid-version", "0"))
            .and(query_param("pin", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "{\"Name\":\"file\",\"Hash\":\"QmFile1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"5\"}\n",
            ))
            .expect(1)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upload.bin");
        tokio::fs::write(&path, b"hello").await.unwrap();
        let s = service_for(&server);
        let cid = s.add_file_path(&path, 0).await.unwrap();
        assert_eq!(cid, "QmFile1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    }

    #[tokio::test]
    async fn add_json_pins_and_returns_hash() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .and(query_param("cid-version", "0"))
            .and(query_param("pin", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "{\"Name\":\"file\",\"Hash\":\"QmJsonaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"5\"}\n",
            ))
            .expect(1)
            .mount(&server)
            .await;
        let s = service_for(&server);
        let cid = s.add_json(&json!({"hello": "world"})).await.unwrap();
        assert_eq!(cid, "QmJsonaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    }

    #[tokio::test]
    async fn dag_import_posts_multipart_with_query() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/import"))
            .and(query_param("pin-roots", "true"))
            .and(query_param("silent", "false"))
            .and(query_param("stats", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "{\"Root\":{\"Cid\":{\"/\":\"bafyroot\"},\"PinErrorMsg\":\"\"}}\n",
            ))
            .expect(1)
            .mount(&server)
            .await;
        let s = service_for(&server);
        let roots = s.dag_import(Bytes::from_static(b"car"), true).await.unwrap();
        assert_eq!(roots, vec!["bafyroot"]);
    }

    #[tokio::test]
    async fn dag_import_path_posts_streamed_file_with_query() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/import"))
            .and(query_param("pin-roots", "true"))
            .and(query_param("silent", "false"))
            .and(query_param("stats", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "{\"Root\":{\"Cid\":{\"/\":\"bafyroot\"},\"PinErrorMsg\":\"\"}}\n",
            ))
            .expect(1)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upload.car");
        tokio::fs::write(&path, b"car").await.unwrap();
        let s = service_for(&server);
        let roots = s.dag_import_path(&path, true).await.unwrap();
        assert_eq!(roots, vec!["bafyroot"]);
    }

    #[tokio::test]
    async fn cat_returns_bytes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("arg", "QmFoo"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello".to_vec()))
            .expect(1)
            .mount(&server)
            .await;
        let s = service_for(&server);
        let bytes = s.cat("QmFoo").await.unwrap();
        assert_eq!(bytes.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn get_ipfs_content_returns_bytes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("arg", "QmA"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"content".to_vec()))
            .expect(1)
            .mount(&server)
            .await;
        let s = service_for(&server);
        let res = s
            .get_ipfs_content("QmA", Duration::from_secs(2), 1)
            .await
            .unwrap();
        assert_eq!(res.as_deref(), Some(b"content".as_ref()));
    }

    #[tokio::test]
    async fn unpin_calls_pin_rm() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .and(query_param("arg", "QmA"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .expect(1)
            .mount(&server)
            .await;
        let s = service_for(&server);
        s.unpin("QmA").await.unwrap();
    }

    #[tokio::test]
    async fn pub_message_posts_to_pubsub_pub() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pubsub/pub"))
            .and(query_param("arg", "ALIVE"))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .expect(1)
            .mount(&server)
            .await;
        let s = service_for(&server);
        s.pub_message("ALIVE", "hi").await.unwrap();
    }

    #[tokio::test]
    async fn connect_returns_json() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/swarm/connect"))
            .and(query_param("arg", "/ip4/1.2.3.4/tcp/4001/p2p/Qm"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Strings\":[\"ok\"]}"))
            .expect(1)
            .mount(&server)
            .await;
        let s = service_for(&server);
        let v = s.connect("/ip4/1.2.3.4/tcp/4001/p2p/Qm").await.unwrap();
        assert!(v.get("Strings").is_some());
    }

    #[tokio::test]
    async fn cat_surfaces_http_error_as_ipfs() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;
        let s = service_for(&server);
        let err = s.cat("Qm").await.unwrap_err();
        assert!(matches!(err, AlephError::Ipfs(_)));
    }

    #[tokio::test]
    async fn stat_returns_size_and_directory_flag_for_directory() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .and(query_param("arg", "/ipfs/QmDir"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Type": "directory",
                "Size": 0,
                "CumulativeSize": 4096,
            })))
            .expect(1)
            .mount(&server)
            .await;
        let s = service_for(&server);
        let stat = s.stat("QmDir", Duration::from_secs(2)).await.unwrap();
        assert!(stat.is_directory);
        assert_eq!(stat.size, 4096);
        assert_eq!(stat.file_type, "directory");
    }

    #[tokio::test]
    async fn stat_returns_size_for_regular_file() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .and(query_param("arg", "/ipfs/QmFile"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Type": "file",
                "Size": 123,
                "CumulativeSize": 200,
            })))
            .expect(1)
            .mount(&server)
            .await;
        let s = service_for(&server);
        let stat = s.stat("QmFile", Duration::from_secs(2)).await.unwrap();
        assert!(!stat.is_directory);
        assert_eq!(stat.size, 123);
    }

    #[test]
    fn decode_pubsub_payload_base64s_data_and_from() {
        use base64::Engine as _;
        let engine = base64::engine::general_purpose::STANDARD;
        let raw = json!({
            "from": engine.encode(b"peer-id"),
            "data": engine.encode(b"hello"),
            "topicIDs": ["t"],
        });
        let decoded = decode_pubsub_payload(raw);
        assert_eq!(decoded.get("from").and_then(Value::as_str), Some("peer-id"));
        assert_eq!(decoded.get("data").and_then(Value::as_str), Some("hello"));
    }
}
