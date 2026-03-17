use aleph_types::account::{Account, SignError, sign_message};
use aleph_types::channel::Channel;
use aleph_types::item_hash::{AlephItemHash, ItemHash};
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;
use aleph_types::message::pending::PendingMessage;
use aleph_types::message::unsigned::UnsignedMessage;
use aleph_types::timestamp::Timestamp;

use crate::verify::compute_cid;

const DEFAULT_INLINE_CUTOFF: usize = 50_000;
const DEFAULT_IPFS_CUTOFF: usize = 4 * 1024 * 1024; // 4 MiB

/// Builder for constructing and signing Aleph messages.
///
/// Handles content envelope construction (injecting `address` and `time`),
/// JSON serialization, storage routing based on size cutoffs, hash
/// computation (SHA-256 for inline/storage, IPFS CID for large content),
/// and signing via the provided `Account`.
pub struct MessageBuilder<'a, A: Account> {
    account: &'a A,
    message_type: MessageType,
    content: serde_json::Value,
    channel: Option<Channel>,
    time: Option<Timestamp>,
    allow_inlining: bool,
    inline_cutoff: usize,
    ipfs_cutoff: usize,
}

impl<'a, A: Account> MessageBuilder<'a, A> {
    pub fn new(account: &'a A, message_type: MessageType, content: serde_json::Value) -> Self {
        Self {
            account,
            message_type,
            content,
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

    /// Builds and signs the message, returning a `PendingMessage` ready for submission.
    ///
    /// 1. Injects `address` and `time` into the content envelope
    /// 2. Serializes to compact JSON
    /// 3. Routes to inline/storage/IPFS based on size
    /// 4. Computes the appropriate hash
    /// 5. Signs using the account
    pub fn build(self) -> Result<PendingMessage, SignError> {
        let time = self.time.unwrap_or_else(Timestamp::now);

        let mut envelope = serde_json::Map::new();
        envelope.insert(
            "address".to_string(),
            serde_json::Value::String(self.account.address().as_str().to_string()),
        );
        envelope.insert("time".to_string(), serde_json::json!(time));

        if let serde_json::Value::Object(map) = self.content {
            for (k, v) in map {
                envelope.insert(k, v);
            }
        }

        let item_content = serde_json::to_string(&serde_json::Value::Object(envelope)).unwrap();
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

        let unsigned = UnsignedMessage {
            message_type: self.message_type,
            item_type,
            item_content,
            item_hash,
            time,
            channel: self.channel,
        };

        sign_message(self.account, unsigned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::account::{Account, SignError};
    use aleph_types::chain::{Address, Chain, Signature};
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;

    /// Minimal test account that produces a dummy signature.
    struct TestAccount {
        address: Address,
    }

    impl TestAccount {
        fn new() -> Self {
            Self {
                address: Address::from("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".to_string()),
            }
        }
    }

    impl Account for TestAccount {
        fn chain(&self) -> Chain {
            Chain::Ethereum
        }
        fn address(&self) -> &Address {
            &self.address
        }
        fn sign_raw(&self, _buffer: &[u8]) -> Result<Signature, SignError> {
            Ok(Signature::from("0xDUMMY".to_string()))
        }
    }

    #[test]
    fn test_builder_inline_small_content() {
        let account = TestAccount::new();
        let content = serde_json::json!({"type": "test", "content": {"body": "Hello"}});
        let pending = MessageBuilder::new(&account, MessageType::Post, content)
            .build()
            .unwrap();

        assert_eq!(pending.item_type, ItemType::Inline);
        assert_eq!(pending.message_type, MessageType::Post);
        assert!(!pending.item_content.is_empty());
        let parsed: serde_json::Value = serde_json::from_str(&pending.item_content).unwrap();
        assert_eq!(
            parsed["address"],
            "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"
        );
        assert!(parsed["time"].is_number());
    }

    #[test]
    fn test_builder_storage_large_content() {
        let account = TestAccount::new();
        let big_body = "x".repeat(60_000);
        let content = serde_json::json!({"type": "test", "content": {"body": big_body}});
        let pending = MessageBuilder::new(&account, MessageType::Post, content)
            .build()
            .unwrap();

        assert_eq!(pending.item_type, ItemType::Storage);
    }

    #[test]
    fn test_builder_ipfs_very_large_content() {
        let account = TestAccount::new();
        let big_body = "x".repeat(5 * 1024 * 1024);
        let content = serde_json::json!({"type": "test", "content": {"body": big_body}});
        let pending = MessageBuilder::new(&account, MessageType::Post, content)
            .build()
            .unwrap();

        assert_eq!(pending.item_type, ItemType::Ipfs);
    }

    #[test]
    fn test_builder_inlining_disabled() {
        let account = TestAccount::new();
        let content = serde_json::json!({"type": "test", "content": {"body": "tiny"}});
        let pending = MessageBuilder::new(&account, MessageType::Post, content)
            .allow_inlining(false)
            .build()
            .unwrap();

        assert_eq!(pending.item_type, ItemType::Storage);
    }

    #[test]
    fn test_builder_custom_cutoffs() {
        let account = TestAccount::new();
        let content = serde_json::json!({"type": "test", "content": {"body": "hello"}});
        let pending = MessageBuilder::new(&account, MessageType::Post, content)
            .inline_cutoff(1)
            .build()
            .unwrap();

        assert_eq!(pending.item_type, ItemType::Storage);
    }

    #[test]
    fn test_builder_channel() {
        let account = TestAccount::new();
        let content = serde_json::json!({"type": "test"});
        let pending = MessageBuilder::new(&account, MessageType::Post, content)
            .channel(Channel::from("MY_CHANNEL".to_string()))
            .build()
            .unwrap();

        assert_eq!(
            pending.channel,
            Some(Channel::from("MY_CHANNEL".to_string()))
        );
    }

    #[test]
    fn test_builder_item_hash_is_sha256_of_item_content() {
        let account = TestAccount::new();
        let content = serde_json::json!({"type": "test", "content": {"body": "Hello"}});
        let pending = MessageBuilder::new(&account, MessageType::Post, content)
            .build()
            .unwrap();

        let expected = AlephItemHash::from_bytes(pending.item_content.as_bytes());
        assert_eq!(pending.item_hash, ItemHash::Native(expected));
    }
}
