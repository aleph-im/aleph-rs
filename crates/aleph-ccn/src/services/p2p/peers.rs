//! Peer dialling utilities. Mirrors `aleph/services/p2p/peers.py`.

use crate::services::p2p::protocol::AlephP2PClient;
use crate::{AlephError, AlephResult};

/// Split a multiaddress like `/ip4/1.2.3.4/tcp/4025/p2p/Qm...` into its
/// transport prefix and trailing peer id.
///
/// Mirrors the parsing performed by `info_from_p2p_addr` in
/// `aleph.toolkit.libp2p_stubs.peer.peerinfo`.
pub fn split_peer_addr(maddr: &str) -> AlephResult<(String, String)> {
    let parts: Vec<&str> = maddr.split("/p2p/").collect();
    if parts.len() != 2 || parts[1].is_empty() {
        return Err(AlephError::P2p(format!(
            "Invalid peer multiaddress: {maddr}"
        )));
    }
    let transport = parts[0].to_string();
    let peer_id = parts[1].split('/').next().unwrap_or("").to_string();
    if peer_id.is_empty() {
        return Err(AlephError::P2p(format!(
            "Invalid peer multiaddress: {maddr}"
        )));
    }
    Ok((transport, peer_id))
}

/// Connect a P2P client to `peer_maddr`. Mirrors `connect_peer`.
///
/// As in Python, the caller-provided multiaddress is split into transport +
/// peer id. Attempts to dial self are skipped.
pub async fn connect_peer<C: AlephP2PClient>(p2p_client: &C, peer_maddr: &str) -> AlephResult<()> {
    let (transport, peer_id) = split_peer_addr(peer_maddr)?;
    let identify = p2p_client.identify().await?;
    if identify.peer_id == peer_id {
        return Ok(());
    }
    // The Python loop iterates over `peer_info.addrs` — we only kept the
    // single transport part, which matches what `Multiaddr` ends up holding
    // for normal `/ip4/.../tcp/.../p2p/...` strings.
    p2p_client.dial(&peer_id, &transport).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::p2p::protocol::{Identify, MockP2pClient};
    use std::sync::Arc;

    #[test]
    fn split_peer_addr_parses_full_address() {
        let (transport, peer_id) = split_peer_addr(
            "/ip4/1.2.3.4/tcp/4025/p2p/QmZkurbY2G2hWay59yiTgQNaQxHSNzKZFt2jbnwJhQcKgV",
        )
        .unwrap();
        assert_eq!(transport, "/ip4/1.2.3.4/tcp/4025");
        assert_eq!(peer_id, "QmZkurbY2G2hWay59yiTgQNaQxHSNzKZFt2jbnwJhQcKgV");
    }

    #[test]
    fn split_peer_addr_rejects_bad_input() {
        assert!(split_peer_addr("/ip4/1.2.3.4/tcp/4025").is_err());
        assert!(split_peer_addr("/p2p/").is_err());
    }

    #[tokio::test]
    async fn connect_peer_skips_self() {
        let client = MockP2pClient::new(Identify {
            peer_id: "QmSelfaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        });
        connect_peer(
            &client,
            "/ip4/1.2.3.4/tcp/4025/p2p/QmSelfaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .await
        .unwrap();
        assert!(client.dialed().is_empty());
    }

    #[tokio::test]
    async fn connect_peer_dials_other() {
        let client = MockP2pClient::new(Identify {
            peer_id: "QmSelf".into(),
        });
        connect_peer(
            &client,
            "/ip4/1.2.3.4/tcp/4025/p2p/QmOtheraaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .await
        .unwrap();
        let dialed = client.dialed();
        assert_eq!(dialed.len(), 1);
        assert_eq!(
            dialed[0],
            (
                "QmOtheraaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                "/ip4/1.2.3.4/tcp/4025".to_string()
            )
        );
        // Ensure dropping Arc is well-formed
        drop(Arc::new(client));
    }
}
