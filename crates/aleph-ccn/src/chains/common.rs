//! Shared chain helpers. Mirrors `aleph/chains/common.py`.

use super::abc::PendingMessageView;

/// Serialize the canonical bytes the sender is supposed to have signed.
///
/// Mirrors `get_verification_buffer` in `aleph/chains/common.py`:
/// `"{chain}\n{sender}\n{type}\n{item_hash}"` as UTF-8 bytes.
pub fn verification_buffer(message: &dyn PendingMessageView) -> Vec<u8> {
    let chain = message.chain().to_string();
    let sender = message.sender();
    let mtype = message.message_type().to_string();
    let item_hash = message.item_hash();
    format!("{chain}\n{sender}\n{mtype}\n{item_hash}").into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::abc::SimplePendingMessage;
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;

    #[test]
    fn buffer_matches_python_format() {
        let msg = SimplePendingMessage {
            chain: Chain::Ethereum,
            sender: "0xABC".into(),
            message_type: MessageType::Post,
            item_hash: "deadbeef".into(),
            signature: None,
            time_seconds: 0.0,
        };
        let buf = verification_buffer(&msg);
        assert_eq!(buf, b"ETH\n0xABC\nPOST\ndeadbeef");
    }
}
