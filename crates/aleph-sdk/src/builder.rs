use aleph_types::chain::Address;
use aleph_types::channel::Channel;
use aleph_types::item_hash::{AlephItemHash, ItemHash};
use aleph_types::message::item_type::ItemType;
use aleph_types::message::unsigned::UnsignedMessage;
use aleph_types::message::MessageType;
use aleph_types::timestamp::Timestamp;

use crate::verify::compute_cid;

const DEFAULT_INLINE_CUTOFF: usize = 50_000;
const DEFAULT_IPFS_CUTOFF: usize = 4 * 1024 * 1024; // 4 MiB

pub struct UnsignedMessageBuilder {
    message_type: MessageType,
    content: serde_json::Value,
    sender: Address,
    channel: Option<Channel>,
    time: Option<Timestamp>,
    allow_inlining: bool,
    inline_cutoff: usize,
    ipfs_cutoff: usize,
}

impl UnsignedMessageBuilder {
    pub fn new(message_type: MessageType, content: serde_json::Value, sender: Address) -> Self {
        Self {
            message_type,
            content,
            sender,
            channel: None,
            time: None,
            allow_inlining: true,
            inline_cutoff: DEFAULT_INLINE_CUTOFF,
            ipfs_cutoff: DEFAULT_IPFS_CUTOFF,
        }
    }

    pub fn channel(mut self, channel: Channel) -> Self {
        self.channel = Some(channel);
        self
    }

    pub fn time(mut self, time: Timestamp) -> Self {
        self.time = Some(time);
        self
    }

    pub fn allow_inlining(mut self, allow: bool) -> Self {
        self.allow_inlining = allow;
        self
    }

    pub fn inline_cutoff(mut self, cutoff: usize) -> Self {
        self.inline_cutoff = cutoff;
        self
    }

    pub fn ipfs_cutoff(mut self, cutoff: usize) -> Self {
        self.ipfs_cutoff = cutoff;
        self
    }

    pub fn build(self) -> UnsignedMessage {
        let time = self.time.unwrap_or_else(Timestamp::now);

        // Build the content envelope with address and time
        let mut envelope = serde_json::Map::new();
        envelope.insert(
            "address".to_string(),
            serde_json::Value::String(self.sender.as_str().to_string()),
        );
        envelope.insert("time".to_string(), serde_json::json!(time));

        // Merge in the content fields
        if let serde_json::Value::Object(map) = self.content {
            for (k, v) in map {
                envelope.insert(k, v);
            }
        }

        let item_content =
            serde_json::to_string(&serde_json::Value::Object(envelope)).unwrap();
        let len = item_content.len();

        let (item_type, item_hash) = if self.allow_inlining && len < self.inline_cutoff {
            let hash = AlephItemHash::from_bytes(item_content.as_bytes());
            (ItemType::Inline, ItemHash::Native(hash))
        } else if len < self.ipfs_cutoff {
            let hash = AlephItemHash::from_bytes(item_content.as_bytes());
            (ItemType::Storage, ItemHash::Native(hash))
        } else {
            let cid = compute_cid(item_content.as_bytes());
            (ItemType::Ipfs, ItemHash::Ipfs(cid))
        };

        UnsignedMessage {
            message_type: self.message_type,
            item_type,
            item_content,
            item_hash,
            time,
            channel: self.channel,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::chain::Address;
    use aleph_types::message::item_type::ItemType;
    use aleph_types::message::MessageType;

    fn test_address() -> Address {
        Address::from("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".to_string())
    }

    #[test]
    fn test_builder_inline_small_content() {
        let content = serde_json::json!({"type": "test", "content": {"body": "Hello"}});
        let msg = UnsignedMessageBuilder::new(MessageType::Post, content, test_address()).build();

        assert_eq!(msg.item_type, ItemType::Inline);
        assert_eq!(msg.message_type, MessageType::Post);
        assert!(!msg.item_content.is_empty());
        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["address"], "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef");
        assert!(parsed["time"].is_number());
    }

    #[test]
    fn test_builder_storage_large_content() {
        let big_body = "x".repeat(60_000);
        let content = serde_json::json!({"type": "test", "content": {"body": big_body}});
        let msg = UnsignedMessageBuilder::new(MessageType::Post, content, test_address()).build();

        assert_eq!(msg.item_type, ItemType::Storage);
    }

    #[test]
    fn test_builder_ipfs_very_large_content() {
        let big_body = "x".repeat(5 * 1024 * 1024);
        let content = serde_json::json!({"type": "test", "content": {"body": big_body}});
        let msg = UnsignedMessageBuilder::new(MessageType::Post, content, test_address()).build();

        assert_eq!(msg.item_type, ItemType::Ipfs);
    }

    #[test]
    fn test_builder_inlining_disabled() {
        let content = serde_json::json!({"type": "test", "content": {"body": "tiny"}});
        let msg = UnsignedMessageBuilder::new(MessageType::Post, content, test_address())
            .allow_inlining(false)
            .build();

        assert_eq!(msg.item_type, ItemType::Storage);
    }

    #[test]
    fn test_builder_custom_cutoffs() {
        let content = serde_json::json!({"type": "test", "content": {"body": "hello"}});
        let msg = UnsignedMessageBuilder::new(MessageType::Post, content, test_address())
            .inline_cutoff(1)
            .build();

        assert_eq!(msg.item_type, ItemType::Storage);
    }

    #[test]
    fn test_builder_channel() {
        let content = serde_json::json!({"type": "test"});
        let msg = UnsignedMessageBuilder::new(MessageType::Post, content, test_address())
            .channel(Channel::from("MY_CHANNEL".to_string()))
            .build();

        assert_eq!(msg.channel, Some(Channel::from("MY_CHANNEL".to_string())));
    }

    #[test]
    fn test_builder_item_hash_is_sha256_of_item_content() {
        let content = serde_json::json!({"type": "test", "content": {"body": "Hello"}});
        let msg = UnsignedMessageBuilder::new(MessageType::Post, content, test_address()).build();

        let expected = AlephItemHash::from_bytes(msg.item_content.as_bytes());
        assert_eq!(msg.item_hash, ItemHash::Native(expected));
    }
}
