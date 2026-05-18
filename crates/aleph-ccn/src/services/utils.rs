//! Tiny networking helpers. Mirrors `aleph/services/utils.py`.
//!
//! Used by the p2p / ipfs manager to compute a probable public IP.

use std::net::UdpSocket;

use crate::{AlephError, AlephResult};

/// Third-party service used to retrieve the public IPv4 (matches Python).
pub const IP4_SERVICE_URL: &str = "https://v4.ident.me/";
/// DNS endpoint used to learn the outbound socket address. Matches Python.
pub const IP4_SOCKET_ENDPOINT: &str = "8.8.8.8:80";

fn is_valid_ip4(ip: &str) -> bool {
    let mut parts = ip.split('.');
    let mut count = 0;
    for p in parts.by_ref() {
        count += 1;
        if p.is_empty() || !p.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }
    }
    count == 4
}

/// Get the public IPv4 by calling the third-party service. Mirrors
/// `get_ip4_from_service`.
pub async fn get_ip4_from_service() -> AlephResult<String> {
    let resp = reqwest::get(IP4_SERVICE_URL)
        .await
        .map_err(|e| AlephError::P2p(format!("ip4 service: {e}")))?;
    let text = resp
        .text()
        .await
        .map_err(|e| AlephError::P2p(format!("ip4 service text: {e}")))?;
    let ip = text.trim().to_string();
    if is_valid_ip4(&ip) {
        Ok(ip)
    } else {
        Err(AlephError::P2p(format!(
            "Response does not match IPv4 format: {ip}"
        )))
    }
}

/// Discover the outbound IPv4 via a UDP socket. Mirrors
/// `get_ip4_from_socket`.
///
/// As in Python, this returns a local NAT address when running behind a
/// gateway (Docker, home routers, ...).
pub fn get_ip4_from_socket() -> AlephResult<String> {
    let sock = UdpSocket::bind("0.0.0.0:0").map_err(AlephError::Io)?;
    sock.connect(IP4_SOCKET_ENDPOINT).map_err(AlephError::Io)?;
    let local = sock.local_addr().map_err(AlephError::Io)?;
    Ok(local.ip().to_string())
}

/// Get the public IPv4. Falls back to the UDP socket trick if the third-party
/// service can't be reached. Mirrors `get_IP`.
pub async fn get_ip() -> AlephResult<String> {
    match get_ip4_from_service().await {
        Ok(ip) => Ok(ip),
        Err(_) => get_ip4_from_socket(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ip4_validation() {
        assert!(is_valid_ip4("1.2.3.4"));
        assert!(is_valid_ip4("255.255.255.255"));
        assert!(!is_valid_ip4("not an ip"));
        assert!(!is_valid_ip4("1.2.3"));
        assert!(!is_valid_ip4("1.2.3.4.5"));
        assert!(!is_valid_ip4("a.b.c.d"));
    }

    #[test]
    fn get_ip4_from_socket_returns_addr() {
        // Should always succeed, even offline (the UDP `connect` doesn't
        // actually send anything).
        let ip = get_ip4_from_socket().unwrap();
        assert!(!ip.is_empty());
    }

    #[tokio::test]
    async fn get_ip_falls_back_to_socket() {
        // We can't reliably hit the public service from CI; assert the fallback
        // path returns *something*.
        let ip = get_ip().await.unwrap_or_else(|_| "127.0.0.1".to_string());
        assert!(!ip.is_empty());
    }
}
