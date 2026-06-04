//! Resolves a user-supplied VM id (full item hash or prefix) and an optional
//! `--crn` override into the `(ItemHash, Url)` pair needed to address a
//! specific VM on a specific CRN.
//!
//! Used by `aleph instance ssh` and by the lifecycle subcommands (`start`,
//! `stop`, `reboot`, `erase`, `logs`). The CRN URL is normally discovered via
//! the scheduler's `/api/v1/nodes/<hash>` endpoint; the override is reserved
//! for emergency debugging (e.g. a duplicated allocation that the user wants
//! to address explicitly). The override accepts a node hash or hash prefix
//! (resolved through the scheduler like `--ccn` accepts an alias, matching
//! the shorthand IDs printed by `aleph instance list`) or a raw CRN URL
//! (anything containing `://`).

use aleph_sdk::scheduler::{NodeEntry, SchedulerClient, VmEntry};
use aleph_types::item_hash::ItemHash;
use anyhow::{Context, Result, anyhow, bail};
use url::Url;

/// Resolve `input` to a VM by exact hash or scheduler-side prefix match.
/// The returned `VmEntry` lets the caller skip a second scheduler round-trip
/// when looking up the CRN URL.
pub async fn resolve_vm(scheduler_url: &Url, input: &str) -> Result<(ItemHash, VmEntry)> {
    let scheduler = SchedulerClient::new(scheduler_url.clone());

    if let Ok(hash) = ItemHash::try_from(input) {
        let entry = scheduler
            .get_vm(&hash)
            .await
            .context("querying scheduler")?
            .ok_or_else(|| anyhow!("instance {hash} not found in the scheduler"))?;
        return Ok((hash, entry));
    }

    let matches = scheduler
        .find_vms_by_hash_prefix(input)
        .await
        .with_context(|| format!("looking up VMs matching prefix `{input}` in the scheduler"))?;
    pick_unique_match(input, matches)
}

pub fn pick_unique_match(input: &str, matches: Vec<VmEntry>) -> Result<(ItemHash, VmEntry)> {
    match matches.len() {
        0 => bail!(
            "no instance matches `{input}`. Run `aleph instance list` to see available hashes, \
             or pass a full hash."
        ),
        1 => {
            let entry = matches.into_iter().next().expect("len() == 1");
            let hash = entry.vm_hash.clone();
            Ok((hash, entry))
        }
        n => {
            let mut hashes: Vec<String> = matches.iter().map(|v| v.vm_hash.to_string()).collect();
            hashes.sort();
            bail!(
                "prefix `{input}` is ambiguous, matches {n} instances:\n  {}",
                hashes.join("\n  ")
            )
        }
    }
}

/// Resolve a CRN node hash to its endpoint URL via the scheduler's
/// `/api/v1/nodes/<hash>` endpoint (the same source used for placement
/// lookups, not the third-party crns-list aggregator).
async fn node_hash_to_url(scheduler_url: &Url, node_hash: &str) -> Result<Url> {
    let scheduler = SchedulerClient::new(scheduler_url.clone());
    let node = scheduler
        .get_node(node_hash)
        .await
        .with_context(|| format!("looking up node {node_hash} in the scheduler"))?
        .ok_or_else(|| anyhow!("the scheduler has no record of node {node_hash}"))?;
    node_entry_to_url(&node)
}

/// Extract and parse a node's reachable endpoint URL.
fn node_entry_to_url(node: &NodeEntry) -> Result<Url> {
    let address = node.address.as_deref().ok_or_else(|| {
        anyhow!(
            "scheduler knows node {} (status: {}) but has no reachable address for it",
            node.node_hash,
            node.status.as_deref().unwrap_or("unknown")
        )
    })?;
    Url::parse(address).with_context(|| format!("invalid CRN address `{address}`"))
}

/// Resolve a node-hash prefix to the node's endpoint URL. Errors when the
/// prefix matches no node or more than one.
async fn node_prefix_to_url(scheduler_url: &Url, prefix: &str) -> Result<Url> {
    let scheduler = SchedulerClient::new(scheduler_url.clone());
    let matches = scheduler
        .find_nodes_by_hash_prefix(prefix)
        .await
        .with_context(|| format!("looking up nodes matching prefix `{prefix}` in the scheduler"))?;
    match matches.len() {
        0 => bail!("no CRN node matches `{prefix}`. Pass a full node hash or the CRN's URL."),
        1 => node_entry_to_url(&matches[0]),
        n => {
            let mut hashes: Vec<&str> = matches.iter().map(|m| m.node_hash.as_str()).collect();
            hashes.sort_unstable();
            bail!(
                "prefix `{prefix}` is ambiguous, matches {n} nodes:\n  {}",
                hashes.join("\n  ")
            )
        }
    }
}

/// Translate a `VmEntry` to the URL of the CRN it's allocated to.
/// `dispatched` and `scheduled` proceed silently (the scheduler has already
/// placed the VM on a node, so its CRN is known); `duplicated` emits a stderr
/// warning before following the scheduler's canonical pick. Any other status
/// is an error pointing the user at `--crn`.
///
/// Looks the CRN up via the scheduler's `/api/v1/nodes/<hash>` endpoint
/// rather than the third-party crns-list aggregator.
pub async fn crn_url_from_entry(
    scheduler_url: &Url,
    vm_id: &ItemHash,
    entry: &VmEntry,
) -> Result<Url> {
    let status = entry.status.as_str();
    let allocated_node = match status {
        "dispatched" | "scheduled" => entry.allocated_node.as_deref().ok_or_else(|| {
            anyhow!("instance {vm_id} has status `{status}` but no allocated_node")
        })?,
        "duplicated" => {
            let node = entry.allocated_node.as_deref().ok_or_else(|| {
                anyhow!("instance {vm_id} has status `duplicated` but no allocated_node")
            })?;
            eprintln!(
                "warning: instance {vm_id} is reported as duplicated (allocated to multiple \
                 CRNs). Defaulting to scheduler's canonical pick {node}. Pass --crn to \
                 target a different one."
            );
            node
        }
        _ => bail!(
            "instance {vm_id} cannot be reached: scheduler reports status `{status}`. \
             Pass --crn to target a CRN directly."
        ),
    };

    node_hash_to_url(scheduler_url, allocated_node)
        .await
        .with_context(|| format!("resolving the CRN for instance {vm_id}"))
}

/// Resolve a user-supplied `--crn` override to a URL. Anything containing
/// `://` is treated as a raw URL and parsed directly; a full 64-char hash is
/// looked up via the scheduler's node endpoint; anything else is treated as
/// a node-hash prefix (matching the shorthand IDs printed by `aleph instance
/// list`) and must identify exactly one node, mirroring how a VM id accepts
/// either a full hash or a prefix.
async fn resolve_crn(scheduler_url: &Url, crn: &str) -> Result<Url> {
    if crn.contains("://") {
        return Url::parse(crn).context("invalid --crn URL");
    }
    if ItemHash::try_from(crn).is_ok() {
        return node_hash_to_url(scheduler_url, crn)
            .await
            .with_context(|| format!("resolving --crn node hash `{crn}`"));
    }
    node_prefix_to_url(scheduler_url, crn)
        .await
        .with_context(|| format!("resolving --crn node prefix `{crn}`"))
}

/// Resolve `(vm_id, crn_url)` for any lifecycle subcommand.
///
/// Three cases:
/// 1. Override + full hash: resolve the override (a URL is parsed directly; a
///    node hash or prefix is looked up in the scheduler) without fetching the
///    VM entry.
/// 2. Override + prefix: ask the scheduler to expand the prefix for the VM
///    hash, but take the CRN from the override.
/// 3. No override: ask the scheduler for the entry and derive the URL from
///    its `allocated_node`.
pub async fn resolve_target(
    scheduler_url: &Url,
    vm_id_input: &str,
    crn_override: Option<&str>,
) -> Result<(ItemHash, Url)> {
    match (crn_override, ItemHash::try_from(vm_id_input)) {
        (Some(crn), Ok(hash)) => Ok((hash, resolve_crn(scheduler_url, crn).await?)),
        (Some(crn), Err(_)) => {
            let (hash, _) = resolve_vm(scheduler_url, vm_id_input).await?;
            Ok((hash, resolve_crn(scheduler_url, crn).await?))
        }
        (None, _) => {
            let (hash, entry) = resolve_vm(scheduler_url, vm_id_input).await?;
            let url = crn_url_from_entry(scheduler_url, &hash, &entry).await?;
            Ok((hash, url))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn vm_entry(hash_hex: &str) -> VmEntry {
        VmEntry {
            vm_hash: hash_hex.parse().unwrap(),
            vm_type: "instance".to_string(),
            allocated_node: None,
            status: "dispatched".to_string(),
            scheduling_status: "scheduled".to_string(),
            migration_target: None,
            owner: None,
            extra: serde_json::Map::new(),
        }
    }

    #[test]
    fn pick_unique_match_single() {
        let entries = vec![vm_entry(
            "4e7df823423f0000000000000000000000000000000000000000000000000001",
        )];
        let (hash, _) = pick_unique_match("4e7df", entries).unwrap();
        assert!(hash.to_string().starts_with("4e7df"));
    }

    #[test]
    fn pick_unique_match_empty() {
        let err = pick_unique_match("dead", vec![]).unwrap_err();
        assert!(err.to_string().contains("no instance matches `dead`"));
    }

    #[test]
    fn pick_unique_match_ambiguous() {
        let entries = vec![
            vm_entry("4e7df823423f0000000000000000000000000000000000000000000000000001"),
            vm_entry("4e7df823423f0000000000000000000000000000000000000000000000000002"),
        ];
        let err = pick_unique_match("4e7df", entries).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ambiguous"));
        assert!(msg.contains("matches 2 instances"));
        assert!(msg.contains("0000000000000001"));
        assert!(msg.contains("0000000000000002"));
    }

    const FULL_HASH: &str = "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99";
    const NODE_HASH: &str = "d704be0b15e2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d77";

    fn dispatched_entry() -> VmEntry {
        let mut e = vm_entry(FULL_HASH);
        e.status = "dispatched".to_string();
        e.allocated_node = Some(NODE_HASH.to_string());
        e
    }

    #[tokio::test]
    async fn crn_url_from_entry_returns_node_address_when_dispatched() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/nodes/{NODE_HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "node_hash": NODE_HASH,
                "address": "https://crn.example.io/",
                "status": "ok",
            })))
            .mount(&server)
            .await;

        let vm_id: ItemHash = FULL_HASH.parse().unwrap();
        let entry = dispatched_entry();
        let url = crn_url_from_entry(&Url::parse(&server.uri()).unwrap(), &vm_id, &entry)
            .await
            .unwrap();
        assert_eq!(url.as_str(), "https://crn.example.io/");
    }

    #[tokio::test]
    async fn crn_url_from_entry_returns_node_address_when_scheduled() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/nodes/{NODE_HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "node_hash": NODE_HASH,
                "address": "https://crn.example.io/",
                "status": "ok",
            })))
            .mount(&server)
            .await;

        let vm_id: ItemHash = FULL_HASH.parse().unwrap();
        let mut entry = dispatched_entry();
        entry.status = "scheduled".to_string();
        let url = crn_url_from_entry(&Url::parse(&server.uri()).unwrap(), &vm_id, &entry)
            .await
            .unwrap();
        assert_eq!(url.as_str(), "https://crn.example.io/");
    }

    #[tokio::test]
    async fn crn_url_from_entry_refuses_unfit_status() {
        let server = MockServer::start().await;
        let vm_id: ItemHash = FULL_HASH.parse().unwrap();
        let mut entry = dispatched_entry();
        entry.status = "unscheduled".to_string();
        let err = crn_url_from_entry(&Url::parse(&server.uri()).unwrap(), &vm_id, &entry)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("status `unscheduled`"));
        assert!(msg.contains("--crn"));
    }

    #[tokio::test]
    async fn crn_url_from_entry_returns_address_when_duplicated() {
        // Stderr-capture in unit tests is awkward; we assert the URL is
        // returned rather than asserting on the warning text. The behavior
        // contract (warn-and-proceed) lives in the source.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/nodes/{NODE_HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "node_hash": NODE_HASH,
                "address": "https://crn.example.io/",
                "status": "ok",
            })))
            .mount(&server)
            .await;

        let vm_id: ItemHash = FULL_HASH.parse().unwrap();
        let mut entry = dispatched_entry();
        entry.status = "duplicated".to_string();
        let url = crn_url_from_entry(&Url::parse(&server.uri()).unwrap(), &vm_id, &entry)
            .await
            .unwrap();
        assert_eq!(url.as_str(), "https://crn.example.io/");
    }

    #[tokio::test]
    async fn resolve_target_override_with_full_hash_skips_scheduler() {
        // No mocks mounted: any scheduler call would 404 and bubble up.
        let server = MockServer::start().await;
        let scheduler = Url::parse(&server.uri()).unwrap();
        let (hash, url) = resolve_target(&scheduler, FULL_HASH, Some("https://crn.example.io/"))
            .await
            .unwrap();
        assert_eq!(hash.to_string(), FULL_HASH);
        assert_eq!(url.as_str(), "https://crn.example.io/");
    }

    #[tokio::test]
    async fn resolve_crn_parses_raw_url_without_scheduler() {
        // No mocks mounted: a URL override must not touch the scheduler.
        let server = MockServer::start().await;
        let url = resolve_crn(
            &Url::parse(&server.uri()).unwrap(),
            "https://crn.example.io/",
        )
        .await
        .unwrap();
        assert_eq!(url.as_str(), "https://crn.example.io/");
    }

    #[tokio::test]
    async fn resolve_target_override_with_node_hash_resolves_via_scheduler() {
        // A node-hash override (no `://`) is resolved through the scheduler's
        // node endpoint; the VM itself is not looked up because vm_id is a
        // full hash.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/nodes/{NODE_HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "node_hash": NODE_HASH,
                "address": "https://crn.example.io/",
                "status": "ok",
            })))
            .mount(&server)
            .await;

        let (hash, url) = resolve_target(
            &Url::parse(&server.uri()).unwrap(),
            FULL_HASH,
            Some(NODE_HASH),
        )
        .await
        .unwrap();
        assert_eq!(hash.to_string(), FULL_HASH);
        assert_eq!(url.as_str(), "https://crn.example.io/");
    }

    fn node_page(hashes: &[&str]) -> serde_json::Value {
        let items: Vec<serde_json::Value> = hashes
            .iter()
            .map(|hash| {
                json!({
                    "node_hash": hash,
                    "address": "https://crn.example.io/",
                    "status": "ok",
                })
            })
            .collect();
        json!({
            "items": items,
            "pagination": {
                "page": 1, "page_size": 200, "total_items": hashes.len(), "total_pages": 1,
            }
        })
    }

    #[tokio::test]
    async fn resolve_crn_with_node_prefix_resolves_via_scheduler() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/nodes"))
            .and(query_param("hash", "d704"))
            .respond_with(ResponseTemplate::new(200).set_body_json(node_page(&[NODE_HASH])))
            .mount(&server)
            .await;

        let url = resolve_crn(&Url::parse(&server.uri()).unwrap(), "d704")
            .await
            .unwrap();
        assert_eq!(url.as_str(), "https://crn.example.io/");
    }

    #[tokio::test]
    async fn resolve_crn_with_ambiguous_node_prefix_errors() {
        let other_hash = "d704c0ffee2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d88";
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/nodes"))
            .and(query_param("hash", "d704"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(node_page(&[NODE_HASH, other_hash])),
            )
            .mount(&server)
            .await;

        let err = resolve_crn(&Url::parse(&server.uri()).unwrap(), "d704")
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("ambiguous"));
        assert!(msg.contains("matches 2 nodes"));
        assert!(msg.contains(NODE_HASH));
        assert!(msg.contains(other_hash));
    }

    #[tokio::test]
    async fn resolve_crn_with_unknown_node_prefix_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/nodes"))
            .and(query_param("hash", "beef"))
            .respond_with(ResponseTemplate::new(200).set_body_json(node_page(&[])))
            .mount(&server)
            .await;

        let err = resolve_crn(&Url::parse(&server.uri()).unwrap(), "beef")
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("no CRN node matches `beef`"));
        assert!(msg.contains("full node hash"));
    }

    #[tokio::test]
    async fn resolve_target_override_with_prefix_consults_scheduler_for_hash_only() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/vms"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "items": [{
                    "vm_hash": FULL_HASH,
                    "vm_type": "instance",
                    "allocated_node": NODE_HASH,
                    "status": "dispatched",
                    "scheduling_status": "scheduled",
                    "migration_target": null,
                    "owner": null,
                }],
                "pagination": {
                    "page": 1, "page_size": 200, "total_items": 1, "total_pages": 1,
                }
            })))
            .mount(&server)
            .await;

        let (hash, url) = resolve_target(
            &Url::parse(&server.uri()).unwrap(),
            "5a586d", // prefix
            Some("https://override.example.io/"),
        )
        .await
        .unwrap();
        assert_eq!(hash.to_string(), FULL_HASH);
        // Override wins: the URL is what the user passed, not the dispatched node.
        assert_eq!(url.as_str(), "https://override.example.io/");
    }

    #[tokio::test]
    async fn resolve_target_no_override_uses_scheduler_placement() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/vms/{FULL_HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "vm_hash": FULL_HASH,
                "vm_type": "instance",
                "allocated_node": NODE_HASH,
                "status": "dispatched",
                "scheduling_status": "scheduled",
                "migration_target": null,
                "owner": null,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/nodes/{NODE_HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "node_hash": NODE_HASH,
                "address": "https://crn.example.io/",
                "status": "ok",
            })))
            .mount(&server)
            .await;

        let (hash, url) = resolve_target(&Url::parse(&server.uri()).unwrap(), FULL_HASH, None)
            .await
            .unwrap();
        assert_eq!(hash.to_string(), FULL_HASH);
        assert_eq!(url.as_str(), "https://crn.example.io/");
    }
}
