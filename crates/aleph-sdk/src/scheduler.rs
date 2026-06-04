//! Client for the Aleph VM scheduler API at <https://scheduler.api.aleph.cloud>.
//!
//! Used by `aleph instance list` to enrich CCN-sourced rows with the actual
//! VM placement and effective status. CCN remains authoritative for which
//! rows appear; this client only answers "where did each one end up".

use aleph_types::chain::Address;
use aleph_types::item_hash::ItemHash;
use serde::Deserialize;
use url::Url;

/// One VM entry as returned by `/api/v1/vms` and `/api/v1/vms/{vm_hash}`.
///
/// Typed fields cover the columns rendered by `aleph instance list`. Any
/// other fields the scheduler returns (requirements, payment, observations)
/// flow through `extra` so `--json` output can pass them on without losing
/// information when the scheduler schema evolves.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VmEntry {
    pub vm_hash: ItemHash,
    pub vm_type: String,
    pub allocated_node: Option<String>,
    /// Effective status: `scheduled`, `dispatched`, `duplicated`, `misplaced`,
    /// `missing`, `orphaned`, `migrating`, `unknown`, `unscheduled`,
    /// `unschedulable`, `removed`. Passed through verbatim.
    pub status: String,
    pub scheduling_status: String,
    pub migration_target: Option<String>,
    pub owner: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// One node entry as returned by `/api/v1/nodes/{node_hash}` and the list
/// endpoint. `address` is the CRN's HTTP base URL; it's `None` when the
/// scheduler hasn't (yet) discovered the node's reachable endpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeEntry {
    pub node_hash: String,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("scheduler returned {status}: {body}")]
    Status {
        status: reqwest::StatusCode,
        body: String,
    },
    #[error("invalid scheduler response: {0}")]
    InvalidResponse(String),
}

const PAGE_SIZE: u32 = 200;

#[derive(Deserialize)]
struct PageEnvelope<T> {
    items: Vec<T>,
    pagination: PaginationMeta,
}

#[derive(Deserialize)]
struct PaginationMeta {
    total_items: u32,
}

pub struct SchedulerClient {
    http: reqwest::Client,
    base_url: Url,
}

impl SchedulerClient {
    /// Construct a client pointing at the given scheduler base URL.
    pub fn new(base_url: Url) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
        }
    }

    /// Fetch every item of a paginated listing endpoint, following
    /// `pagination.total_items` across pages. `query` is appended to the
    /// `page_size`/`page` parameters on every request.
    async fn fetch_all_pages<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<Vec<T>, SchedulerError> {
        let url = self
            .base_url
            .join(path)
            .map_err(|e| SchedulerError::InvalidResponse(format!("URL join error: {e}")))?;

        let mut all = Vec::new();
        let mut page = 1u32;
        loop {
            let mut params: Vec<(&str, String)> =
                query.iter().map(|(k, v)| (*k, v.to_string())).collect();
            params.push(("page_size", PAGE_SIZE.to_string()));
            params.push(("page", page.to_string()));
            let response = self.http.get(url.clone()).query(&params).send().await?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(SchedulerError::Status { status, body });
            }
            let envelope: PageEnvelope<T> = response
                .json()
                .await
                .map_err(|e| SchedulerError::InvalidResponse(format!("decode failed: {e}")))?;
            let items_returned = envelope.items.len();
            let total = envelope.pagination.total_items;
            all.extend(envelope.items);
            if all.len() as u32 >= total || items_returned == 0 {
                break;
            }
            page += 1;
        }
        Ok(all)
    }

    /// Returns every VM the scheduler knows about for the given owner address.
    /// Paginates internally over `/api/v1/vms?owners=<owner>&page_size=200`.
    pub async fn list_vms_by_owner(&self, owner: &Address) -> Result<Vec<VmEntry>, SchedulerError> {
        let owner = owner.to_string();
        self.fetch_all_pages("/api/v1/vms", &[("owners", owner.as_str())])
            .await
    }

    /// Returns every VM the scheduler knows about for the given sender address
    /// (`message.sender`). This differs from the owner
    /// (`message.content.address`) for VMs created through Aleph's
    /// permission-delegation system, where sender != owner. Paginates
    /// internally over `/api/v1/vms?sender=<sender>&page_size=200`.
    ///
    /// Requires scheduler v0.1.1 or newer: older releases ignore the `sender`
    /// parameter and return the full VM set.
    pub async fn list_vms_by_sender(
        &self,
        sender: &Address,
    ) -> Result<Vec<VmEntry>, SchedulerError> {
        let sender = sender.to_string();
        self.fetch_all_pages("/api/v1/vms", &[("sender", sender.as_str())])
            .await
    }

    /// Find every VM whose hash starts with `prefix`. The scheduler's
    /// `/api/v1/vms?vm_hash=<prefix>` endpoint matches prefixes server-side,
    /// so this is O(matches) on the wire regardless of how many VMs exist
    /// (~9k as of writing). Paginates defensively for short prefixes.
    pub async fn find_vms_by_hash_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<VmEntry>, SchedulerError> {
        self.fetch_all_pages("/api/v1/vms", &[("vm_hash", prefix)])
            .await
    }

    /// Find every node whose hash matches `fragment`. The scheduler's
    /// `/api/v1/nodes?hash=<fragment>` endpoint matches anchored prefixes OR
    /// suffixes server-side (a middle fragment does not match); see
    /// aleph-vm-scheduler v0.1.1 feature #182. The parameter is named `hash`
    /// here, unlike `vm_hash` on `/api/v1/vms`. The shorthand node IDs printed
    /// by `aleph instance list` are the last 10 characters of the node hash,
    /// i.e. suffixes, so they resolve through this endpoint. Paginates
    /// defensively for short fragments, though the node population is small
    /// (~500 as of writing).
    pub async fn find_nodes_by_hash_fragment(
        &self,
        fragment: &str,
    ) -> Result<Vec<NodeEntry>, SchedulerError> {
        self.fetch_all_pages("/api/v1/nodes", &[("hash", fragment)])
            .await
    }

    /// Fetch one node by its hash. Returns `Ok(None)` on HTTP 404.
    pub async fn get_node(&self, node_hash: &str) -> Result<Option<NodeEntry>, SchedulerError> {
        let url = self
            .base_url
            .join(&format!("/api/v1/nodes/{node_hash}"))
            .map_err(|e| SchedulerError::InvalidResponse(format!("URL join error: {e}")))?;

        let response = self.http.get(url).send().await?;
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(SchedulerError::Status { status, body });
        }
        let entry = response
            .json::<NodeEntry>()
            .await
            .map_err(|e| SchedulerError::InvalidResponse(format!("decode failed: {e}")))?;
        Ok(Some(entry))
    }

    /// Fetch one VM by hash. Returns `Ok(None)` on HTTP 404.
    pub async fn get_vm(&self, vm_hash: &ItemHash) -> Result<Option<VmEntry>, SchedulerError> {
        let url = self
            .base_url
            .join(&format!("/api/v1/vms/{vm_hash}"))
            .map_err(|e| SchedulerError::InvalidResponse(format!("URL join error: {e}")))?;

        let response = self.http.get(url).send().await?;
        let status = response.status();

        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(SchedulerError::Status { status, body });
        }

        let entry = response
            .json::<VmEntry>()
            .await
            .map_err(|e| SchedulerError::InvalidResponse(format!("decode failed: {e}")))?;
        Ok(Some(entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_page(
        items_in_page: usize,
        page: u32,
        page_size: u32,
        total: u32,
    ) -> serde_json::Value {
        let items: Vec<serde_json::Value> = (0..items_in_page)
            .map(|i| {
                let raw_hash = format!("{:0>64x}", page as u64 * 1000 + i as u64);
                json!({
                    "vm_hash": raw_hash,
                    "vm_type": "instance",
                    "allocated_node": null,
                    "status": "scheduled",
                    "scheduling_status": "scheduled",
                    "migration_target": null,
                    "owner": "0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072"
                })
            })
            .collect();
        json!({
            "items": items,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": total,
                "total_pages": total.div_ceil(page_size),
            }
        })
    }

    #[tokio::test]
    async fn list_vms_by_owner_paginates_until_total_items_reached() {
        let server = MockServer::start().await;
        let owner = "0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072";
        // 250 total: page 1 = 100, page 2 = 100, page 3 = 50.
        for (page, items) in [(1u32, 100usize), (2, 100), (3, 50)] {
            Mock::given(method("GET"))
                .and(path("/api/v1/vms"))
                .and(query_param("owners", owner))
                .and(query_param("page_size", "200"))
                .and(query_param("page", page.to_string()))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(sample_page(items, page, 200, 250)),
                )
                .mount(&server)
                .await;
        }

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from(owner.to_string());
        let vms = client.list_vms_by_owner(&addr).await.unwrap();
        assert_eq!(vms.len(), 250);
    }

    #[tokio::test]
    async fn list_vms_by_owner_returns_status_error_on_5xx() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/vms"))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from("0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072".to_string());
        match client.list_vms_by_owner(&addr).await.unwrap_err() {
            SchedulerError::Status { status, .. } => assert_eq!(status.as_u16(), 503),
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_vms_by_owner_returns_empty_when_total_is_zero() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/vms"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_page(0, 1, 200, 0)))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from("0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072".to_string());
        let vms = client.list_vms_by_owner(&addr).await.unwrap();
        assert!(vms.is_empty());
    }

    #[tokio::test]
    async fn list_vms_by_sender_paginates_until_total_items_reached() {
        let server = MockServer::start().await;
        let sender = "0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072";
        // 250 total: page 1 = 100, page 2 = 100, page 3 = 50.
        for (page, items) in [(1u32, 100usize), (2, 100), (3, 50)] {
            Mock::given(method("GET"))
                .and(path("/api/v1/vms"))
                .and(query_param("sender", sender))
                .and(query_param("page_size", "200"))
                .and(query_param("page", page.to_string()))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(sample_page(items, page, 200, 250)),
                )
                .mount(&server)
                .await;
        }

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from(sender.to_string());
        let vms = client.list_vms_by_sender(&addr).await.unwrap();
        assert_eq!(vms.len(), 250);
    }

    #[tokio::test]
    async fn list_vms_by_sender_returns_empty_when_total_is_zero() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/vms"))
            .and(query_param(
                "sender",
                "0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_page(0, 1, 200, 0)))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from("0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072".to_string());
        let vms = client.list_vms_by_sender(&addr).await.unwrap();
        assert!(vms.is_empty());
    }

    #[tokio::test]
    async fn list_vms_by_sender_returns_status_error_on_5xx() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/vms"))
            .and(query_param(
                "sender",
                "0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072",
            ))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from("0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072".to_string());
        match client.list_vms_by_sender(&addr).await.unwrap_err() {
            SchedulerError::Status { status, .. } => assert_eq!(status.as_u16(), 503),
            other => panic!("expected Status, got {other:?}"),
        }
    }

    fn sample_vm_json() -> serde_json::Value {
        json!({
            "vm_hash": "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99",
            "vm_type": "instance",
            "allocated_node": "d704be0b15e2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d77",
            "status": "dispatched",
            "scheduling_status": "scheduled",
            "migration_target": null,
            "owner": "0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072",
            "requirements_vcpus": 4,
            "payment_type": "credits"
        })
    }

    #[tokio::test]
    async fn get_vm_returns_entry_on_200() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/v1/vms/5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_vm_json()))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let hash: ItemHash = "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99"
            .parse()
            .unwrap();
        let entry = client.get_vm(&hash).await.unwrap().expect("Some(entry)");
        assert_eq!(entry.status, "dispatched");
        assert_eq!(
            entry.allocated_node.as_deref(),
            Some("d704be0b15e2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d77")
        );
        // Extra fields flow through.
        assert_eq!(
            entry.extra.get("payment_type").and_then(|v| v.as_str()),
            Some("credits")
        );
    }

    #[tokio::test]
    async fn get_vm_returns_none_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/v1/vms/5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99",
            ))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let hash: ItemHash = "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99"
            .parse()
            .unwrap();
        assert!(client.get_vm(&hash).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_vms_by_owner_breaks_when_page_is_empty_despite_nonzero_total() {
        // Inconsistent scheduler response: total claims more items than the
        // server is willing to return. The loop must terminate, not hang.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/vms"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_page(0, 1, 200, 100)))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from("0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072".to_string());
        // The mock only matches one request; if the loop kept iterating, the
        // second call would 404 and the JSON decode would fail. Receiving an
        // empty Vec here proves we broke out cleanly.
        let vms = client.list_vms_by_owner(&addr).await.unwrap();
        assert!(vms.is_empty());
    }

    #[tokio::test]
    async fn find_vms_by_hash_prefix_returns_matches() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/vms"))
            .and(query_param("vm_hash", "4e7d"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_page(2, 1, 200, 2)))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let vms = client.find_vms_by_hash_prefix("4e7d").await.unwrap();
        assert_eq!(vms.len(), 2);
    }

    #[tokio::test]
    async fn find_vms_by_hash_prefix_empty_on_no_match() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/vms"))
            .and(query_param("vm_hash", "deadbeef"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_page(0, 1, 200, 0)))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let vms = client.find_vms_by_hash_prefix("deadbeef").await.unwrap();
        assert!(vms.is_empty());
    }

    fn sample_node_page(hashes: &[&str], total: u32) -> serde_json::Value {
        let items: Vec<serde_json::Value> = hashes
            .iter()
            .map(|hash| {
                json!({
                    "node_hash": hash,
                    "address": format!("https://{}.example.io/", &hash[..8]),
                    "status": "Healthy",
                })
            })
            .collect();
        json!({
            "items": items,
            "pagination": {
                "page": 1,
                "page_size": PAGE_SIZE,
                "total_items": total,
                "total_pages": 1,
            }
        })
    }

    #[tokio::test]
    async fn find_nodes_by_hash_fragment_matches_anchored_prefix() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/nodes"))
            .and(query_param("hash", "bb0a"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_node_page(
                &[
                    "bb0aa1a9fc7566286c0db32cd5c660066017430390ca779da4d3a241fa07c337",
                    "bb0ab44bf1252a8ce0a06b0526fac39da80e07b35c6dbb4d9e93264489ab6a05",
                ],
                2,
            )))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let nodes = client.find_nodes_by_hash_fragment("bb0a").await.unwrap();
        assert_eq!(nodes.len(), 2);
        // Matching is anchored at either end (prefix or suffix).
        assert!(
            nodes
                .iter()
                .all(|n| n.node_hash.starts_with("bb0a") || n.node_hash.ends_with("bb0a"))
        );
    }

    #[tokio::test]
    async fn find_nodes_by_hash_fragment_matches_anchored_suffix() {
        // The shorthand node IDs printed by `aleph instance list` are the last
        // 10 characters of the node hash, i.e. suffixes. The scheduler matches
        // those server-side (v0.1.1 #182); the client just forwards the value.
        let full = "bb0aa1a9fc7566286c0db32cd5c660066017430390ca779da4d3a241fa07c337";
        let suffix = &full[full.len() - 10..];
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/nodes"))
            .and(query_param("hash", suffix))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_node_page(&[full], 1)))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let nodes = client.find_nodes_by_hash_fragment(suffix).await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].node_hash.ends_with(suffix));
    }

    #[tokio::test]
    async fn find_nodes_by_hash_fragment_empty_on_no_match() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/nodes"))
            .and(query_param("hash", "deadbeef"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_node_page(&[], 0)))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let nodes = client
            .find_nodes_by_hash_fragment("deadbeef")
            .await
            .unwrap();
        assert!(nodes.is_empty());
    }

    #[tokio::test]
    async fn get_node_returns_entry_on_200() {
        let server = MockServer::start().await;
        let hash = "bb0aa1a9fc7566286c0db32cd5c660066017430390ca779da4d3a241fa07c337";
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/nodes/{hash}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "node_hash": hash,
                "name": "Confident VMs 4",
                "address": "https://computevm4.example.io/",
                "status": "Healthy",
            })))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let entry = client.get_node(hash).await.unwrap().expect("Some(entry)");
        assert_eq!(entry.node_hash, hash);
        assert_eq!(
            entry.address.as_deref(),
            Some("https://computevm4.example.io/")
        );
        assert_eq!(entry.status.as_deref(), Some("Healthy"));
    }

    #[tokio::test]
    async fn get_node_returns_none_on_404() {
        let server = MockServer::start().await;
        let hash = "0000000000000000000000000000000000000000000000000000000000000001";
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/nodes/{hash}")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        assert!(client.get_node(hash).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_vm_returns_status_error_on_500() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/v1/vms/5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99",
            ))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;

        let client = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let hash: ItemHash = "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99"
            .parse()
            .unwrap();
        let err = client.get_vm(&hash).await.unwrap_err();
        match err {
            SchedulerError::Status { status, body } => {
                assert_eq!(status.as_u16(), 500);
                assert_eq!(body, "boom");
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }
}
