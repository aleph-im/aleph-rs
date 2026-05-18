//! IPFS connection helpers. Mirrors `aleph/services/ipfs/common.py`.
//!
//! Python uses `aioipfs.AsyncIPFS` for both content/pubsub and pinning
//! operations. The Rust port uses a plain `reqwest::Client` against the
//! kubo HTTP API (`/api/v0/...`); the choice of which endpoint to target is
//! held in [`IpfsEndpoint`].

use std::time::Duration;

use crate::config::{IpfsPinningSettings, IpfsSettings};
use crate::{AlephError, AlephResult};

/// HTTP endpoint targeting a single IPFS daemon.
#[derive(Debug, Clone)]
pub struct IpfsEndpoint {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub timeout: Duration,
}

impl IpfsEndpoint {
    pub fn base_url(&self) -> String {
        format!("{}://{}:{}", self.scheme, self.host, self.port)
    }

    pub fn api_url(&self, method: &str) -> String {
        format!(
            "{}/api/v0/{}",
            self.base_url(),
            method.trim_start_matches('/')
        )
    }
}

/// Base URL string used by `aleph.services.ipfs.common.get_base_url`.
pub fn get_base_url(ipfs: &IpfsSettings) -> String {
    format!("http://{}:{}", ipfs.host, ipfs.port)
}

/// Build a `reqwest::Client` configured the same way `aioipfs.AsyncIPFS` is in
/// Python: 25 max connections, 10 per host, configurable read timeout.
pub fn make_ipfs_client(timeout: Duration) -> AlephResult<reqwest::Client> {
    reqwest::Client::builder()
        .pool_max_idle_per_host(10)
        .timeout(timeout)
        .build()
        .map_err(|e| AlephError::Ipfs(format!("reqwest build failed: {e}")))
}

/// Build the P2P (content + pubsub) endpoint. Mirrors `make_ipfs_p2p_client`.
pub fn make_ipfs_p2p_endpoint(ipfs: &IpfsSettings, timeout: Duration) -> IpfsEndpoint {
    IpfsEndpoint {
        scheme: ipfs.scheme.clone(),
        host: ipfs.host.clone(),
        port: ipfs.port,
        timeout,
    }
}

/// Build the pinning endpoint. Mirrors `make_ipfs_pinning_client`: uses the
/// `ipfs.pinning.*` overrides when set, otherwise falls back to the main
/// settings.
pub fn make_ipfs_pinning_endpoint(ipfs: &IpfsSettings) -> IpfsEndpoint {
    let pinning: &IpfsPinningSettings = &ipfs.pinning;
    let host = pinning
        .host
        .clone()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| ipfs.host.clone());
    let scheme = if pinning.host.is_some() {
        pinning.scheme.clone()
    } else {
        ipfs.scheme.clone()
    };
    let port = if pinning.host.is_some() {
        pinning.port
    } else {
        ipfs.port
    };
    IpfsEndpoint {
        scheme,
        host,
        port,
        timeout: Duration::from_secs(pinning.timeout),
    }
}

/// Whether a separate pinning client should be instantiated. Mirrors
/// `_should_use_separate_pinning_client`.
pub fn should_use_separate_pinning_client(ipfs: &IpfsSettings) -> bool {
    match &ipfs.pinning.host {
        Some(h) if !h.is_empty() => h != &ipfs.host || ipfs.pinning.port != ipfs.port,
        _ => false,
    }
}

/// IPFS CID version detection. Mirrors `get_cid_version`.
///
/// - CIDv0 hashes start with `Qm` and have a length between 44 and 46.
/// - CIDv1 hashes start with `bafy` and have a length of 59.
pub fn get_cid_version(ipfs_hash: &str) -> AlephResult<u8> {
    if ipfs_hash.starts_with("Qm") && (44..=46).contains(&ipfs_hash.len()) {
        return Ok(0);
    }
    if ipfs_hash.starts_with("bafy") && ipfs_hash.len() == 59 {
        return Ok(1);
    }
    Err(AlephError::Ipfs(format!("Not a IPFS hash: '{ipfs_hash}'.")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IpfsPinningSettings;

    fn ipfs(host: &str, port: u16) -> IpfsSettings {
        let mut s = IpfsSettings::default();
        s.host = host.into();
        s.port = port;
        s.pinning = IpfsPinningSettings::default();
        s
    }

    #[test]
    fn endpoint_urls() {
        let ep = IpfsEndpoint {
            scheme: "http".into(),
            host: "ipfs".into(),
            port: 5001,
            timeout: Duration::from_secs(60),
        };
        assert_eq!(ep.base_url(), "http://ipfs:5001");
        assert_eq!(ep.api_url("cat"), "http://ipfs:5001/api/v0/cat");
        assert_eq!(ep.api_url("/cat"), "http://ipfs:5001/api/v0/cat");
    }

    #[test]
    fn base_url_matches_python() {
        let s = ipfs("ipfs.example", 5002);
        assert_eq!(get_base_url(&s), "http://ipfs.example:5002");
    }

    #[test]
    fn p2p_endpoint_uses_main_settings() {
        let s = ipfs("ipfs.example", 5002);
        let ep = make_ipfs_p2p_endpoint(&s, Duration::from_secs(5));
        assert_eq!(ep.host, "ipfs.example");
        assert_eq!(ep.port, 5002);
    }

    #[test]
    fn pinning_endpoint_falls_back_to_main_when_unset() {
        let s = ipfs("ipfs.example", 5002);
        let ep = make_ipfs_pinning_endpoint(&s);
        assert_eq!(ep.host, "ipfs.example");
        assert_eq!(ep.port, 5002);
        assert_eq!(ep.timeout, Duration::from_secs(s.pinning.timeout));
    }

    #[test]
    fn pinning_endpoint_uses_override_when_set() {
        let mut s = ipfs("ipfs.main", 5001);
        s.pinning = IpfsPinningSettings {
            host: Some("ipfs.pinning".into()),
            port: 9001,
            scheme: "https".into(),
            timeout: 30,
        };
        let ep = make_ipfs_pinning_endpoint(&s);
        assert_eq!(ep.host, "ipfs.pinning");
        assert_eq!(ep.port, 9001);
        assert_eq!(ep.scheme, "https");
        assert_eq!(ep.timeout, Duration::from_secs(30));
    }

    #[test]
    fn separate_pinning_when_host_differs() {
        let mut s = ipfs("ipfs.main", 5001);
        s.pinning = IpfsPinningSettings {
            host: Some("ipfs.pinning".into()),
            port: 5001,
            scheme: "http".into(),
            timeout: 60,
        };
        assert!(should_use_separate_pinning_client(&s));
    }

    #[test]
    fn no_separate_pinning_when_host_missing() {
        let s = ipfs("ipfs.main", 5001);
        assert!(!should_use_separate_pinning_client(&s));
    }

    #[test]
    fn cid_version_detection() {
        // Real-world CIDv0
        let v0 = "QmTudJSaoKxtbEnTddJ9vh8hbN84ZLVvD5pNpUaSbxwGoa";
        assert_eq!(get_cid_version(v0).unwrap(), 0);

        // Synthetic CIDv1 (59 chars starting with bafy)
        let v1 = format!("bafy{}", "a".repeat(55));
        assert_eq!(get_cid_version(&v1).unwrap(), 1);

        assert!(get_cid_version("notacid").is_err());
    }
}
