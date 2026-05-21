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
struct PageEnvelope {
    items: Vec<VmEntry>,
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

    /// Returns every VM the scheduler knows about for the given owner address.
    /// Paginates internally over `/api/v1/vms?owners=<owner>&page_size=200`.
    pub async fn list_vms_by_owner(&self, owner: &Address) -> Result<Vec<VmEntry>, SchedulerError> {
        let url = self
            .base_url
            .join("/api/v1/vms")
            .map_err(|e| SchedulerError::InvalidResponse(format!("URL join error: {e}")))?;

        let mut all = Vec::new();
        let mut page = 1u32;
        loop {
            let response = self
                .http
                .get(url.clone())
                .query(&[
                    ("owners", owner.to_string()),
                    ("page_size", PAGE_SIZE.to_string()),
                    ("page", page.to_string()),
                ])
                .send()
                .await?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(SchedulerError::Status { status, body });
            }
            let envelope: PageEnvelope = response
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

    /// Find every VM whose hash starts with `prefix`. The scheduler's
    /// `/api/v1/vms?vm_hash=<prefix>` endpoint matches prefixes server-side,
    /// so this is O(matches) on the wire regardless of how many VMs exist
    /// (~9k as of writing). Paginates defensively for short prefixes.
    pub async fn find_vms_by_hash_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<VmEntry>, SchedulerError> {
        let url = self
            .base_url
            .join("/api/v1/vms")
            .map_err(|e| SchedulerError::InvalidResponse(format!("URL join error: {e}")))?;

        let mut all = Vec::new();
        let mut page = 1u32;
        loop {
            let response = self
                .http
                .get(url.clone())
                .query(&[
                    ("vm_hash", prefix.to_string()),
                    ("page_size", PAGE_SIZE.to_string()),
                    ("page", page.to_string()),
                ])
                .send()
                .await?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(SchedulerError::Status { status, body });
            }
            let envelope: PageEnvelope = response
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
