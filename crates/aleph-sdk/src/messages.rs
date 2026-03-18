use aleph_types::account::{Account, SignError};
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::pending::PendingMessage;
use aleph_types::message::{
    AggregateContent, AggregateKey, ForgetContent, MessageType, PostContent, PostType,
};
use aleph_types::message::{RawFileRef, StorageBackend, StorageEngine, StoreContent};
use serde::Serialize;
use std::collections::HashMap;
use thiserror::Error;

use crate::builder::MessageBuilder;

#[derive(Debug, Error)]
pub enum MessageBuildError {
    #[error("content serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("message signing failed: {0}")]
    Signing(#[from] SignError),
    #[error("forget message must target at least one hash or aggregate")]
    EmptyForget,
    #[error("storage engine mismatch: engine {engine:?} does not match hash type '{hash}'")]
    StorageEngineMismatch { engine: StorageEngine, hash: String },
}

pub struct PostBuilder<'a, A: Account> {
    account: &'a A,
    post_type: PostType,
    content: serde_json::Value,
    reference: Option<String>,
    channel: Option<Channel>,
}

impl<'a, A: Account> PostBuilder<'a, A> {
    pub fn new(
        account: &'a A,
        post_type: impl Into<String>,
        content: impl Serialize,
    ) -> Result<Self, MessageBuildError> {
        Ok(Self {
            account,
            post_type: PostType::Other {
                post_type: post_type.into(),
            },
            content: serde_json::to_value(content)?,
            reference: None,
            channel: None,
        })
    }

    pub fn amend(
        account: &'a A,
        reference: ItemHash,
        content: impl Serialize,
    ) -> Result<Self, MessageBuildError> {
        Ok(Self {
            account,
            post_type: PostType::Amend {
                reference: reference.to_string(),
            },
            content: serde_json::to_value(content)?,
            reference: None,
            channel: None,
        })
    }

    /// Set a reference hash on the post. This is independent of amend semantics —
    /// it sets the `ref` field in the POST content, used e.g. by corechannel operations
    /// to point at a target node.
    pub fn reference(mut self, reference: impl Into<String>) -> Self {
        self.reference = Some(reference.into());
        self
    }

    pub fn channel(mut self, channel: Channel) -> Self {
        self.channel = Some(channel);
        self
    }

    pub fn build(self) -> Result<PendingMessage, MessageBuildError> {
        let post_content = PostContent {
            post_type: self.post_type,
            content: Some(self.content),
        };
        let mut value = serde_json::to_value(post_content)?;
        if let Some(reference) = self.reference {
            value
                .as_object_mut()
                .expect("PostContent serializes to an object")
                .insert("ref".to_string(), serde_json::Value::String(reference));
        }
        let mut builder = MessageBuilder::new(self.account, MessageType::Post, value);
        if let Some(channel) = self.channel {
            builder = builder.channel(channel);
        }
        Ok(builder.build()?)
    }
}

pub struct AggregateBuilder<'a, A: Account> {
    account: &'a A,
    key: String,
    content: serde_json::Map<String, serde_json::Value>,
    channel: Option<Channel>,
}

impl<'a, A: Account> AggregateBuilder<'a, A> {
    pub fn new(
        account: &'a A,
        key: impl Into<String>,
        content: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        Self {
            account,
            key: key.into(),
            content,
            channel: None,
        }
    }

    pub fn channel(mut self, channel: Channel) -> Self {
        self.channel = Some(channel);
        self
    }

    pub fn build(self) -> Result<PendingMessage, MessageBuildError> {
        let aggregate_content = AggregateContent {
            key: AggregateKey::String(self.key),
            content: self.content,
        };
        let value = serde_json::to_value(aggregate_content)?;
        let mut builder = MessageBuilder::new(self.account, MessageType::Aggregate, value);
        if let Some(channel) = self.channel {
            builder = builder.channel(channel);
        }
        Ok(builder.build()?)
    }
}

pub struct ForgetBuilder<'a, A: Account> {
    account: &'a A,
    hashes: Vec<ItemHash>,
    aggregates: Vec<ItemHash>,
    reason: Option<String>,
    channel: Option<Channel>,
}

impl<'a, A: Account> ForgetBuilder<'a, A> {
    pub fn new(account: &'a A, hashes: Vec<ItemHash>) -> Self {
        Self {
            account,
            hashes,
            aggregates: vec![],
            reason: None,
            channel: None,
        }
    }

    pub fn aggregates(mut self, aggregates: Vec<ItemHash>) -> Self {
        self.aggregates = aggregates;
        self
    }

    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn channel(mut self, channel: Channel) -> Self {
        self.channel = Some(channel);
        self
    }

    pub fn build(self) -> Result<PendingMessage, MessageBuildError> {
        if self.hashes.is_empty() && self.aggregates.is_empty() {
            return Err(MessageBuildError::EmptyForget);
        }
        let forget_content = ForgetContent::new(self.hashes, self.aggregates, self.reason);
        let value = serde_json::to_value(forget_content)?;
        let mut builder = MessageBuilder::new(self.account, MessageType::Forget, value);
        if let Some(channel) = self.channel {
            builder = builder.channel(channel);
        }
        Ok(builder.build()?)
    }
}

pub struct StoreBuilder<'a, A: Account> {
    account: &'a A,
    file_hash: ItemHash,
    storage_engine: StorageEngine,
    reference: Option<RawFileRef>,
    metadata: Option<HashMap<String, serde_json::Value>>,
    channel: Option<Channel>,
}

impl<'a, A: Account> StoreBuilder<'a, A> {
    /// Create a new StoreBuilder for a file that has already been uploaded.
    pub fn new(account: &'a A, file_hash: ItemHash, storage_engine: StorageEngine) -> Self {
        Self {
            account,
            file_hash,
            storage_engine,
            reference: None,
            metadata: None,
            channel: None,
        }
    }

    /// Set a user-defined file reference string.
    pub fn reference(mut self, reference: impl Into<String>) -> Self {
        self.reference = Some(RawFileRef::UserDefined(reference.into()));
        self
    }

    /// Set a file reference from an item hash.
    pub fn reference_hash(mut self, hash: ItemHash) -> Self {
        self.reference = Some(RawFileRef::ItemHash(hash));
        self
    }

    /// Set metadata key-value pairs.
    pub fn metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Set the message channel.
    pub fn channel(mut self, channel: Channel) -> Self {
        self.channel = Some(channel);
        self
    }

    /// Build and sign the STORE message.
    pub fn build(self) -> Result<PendingMessage, MessageBuildError> {
        let backend = match (self.storage_engine, self.file_hash) {
            (StorageEngine::Storage, ItemHash::Native(h)) => {
                StorageBackend::Storage { item_hash: h }
            }
            (StorageEngine::Ipfs, ItemHash::Ipfs(cid)) => StorageBackend::Ipfs { item_hash: cid },
            (engine, hash) => {
                return Err(MessageBuildError::StorageEngineMismatch {
                    engine,
                    hash: hash.to_string(),
                });
            }
        };

        let store_content = StoreContent::new(backend, self.reference, self.metadata);
        let value = serde_json::to_value(store_content)?;

        let mut builder = MessageBuilder::new(self.account, MessageType::Store, value);
        if let Some(channel) = self.channel {
            builder = builder.channel(channel);
        }
        Ok(builder.build()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::account::{Account, SignError};
    use aleph_types::chain::{Address, Chain, Signature};
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;

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
    fn test_post_builder_new() {
        let account = TestAccount::new();
        let msg = PostBuilder::new(&account, "chat", serde_json::json!({"body": "hello"}))
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(msg.message_type, MessageType::Post);
        assert_eq!(msg.item_type, ItemType::Inline);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(
            parsed["address"],
            "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"
        );
        assert_eq!(parsed["type"], "chat");
        assert_eq!(parsed["content"]["body"], "hello");
        assert!(parsed.get("ref").is_none());
    }

    #[test]
    fn test_post_builder_amend() {
        let account = TestAccount::new();
        let original_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let msg = PostBuilder::amend(
            &account,
            original_hash,
            serde_json::json!({"body": "edited"}),
        )
        .unwrap()
        .build()
        .unwrap();

        assert_eq!(msg.message_type, MessageType::Post);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(
            parsed["ref"],
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        assert_eq!(parsed["content"]["body"], "edited");
        assert!(parsed.get("type").is_none());
    }

    #[test]
    fn test_post_builder_with_reference() {
        let account = TestAccount::new();
        let msg = PostBuilder::new(
            &account,
            "corechan-operation",
            serde_json::json!({"action": "link", "tags": ["link", "mainnet"]}),
        )
        .unwrap()
        .reference("a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77")
        .build()
        .unwrap();

        assert_eq!(msg.message_type, MessageType::Post);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        // Has both type AND ref
        assert_eq!(parsed["type"], "corechan-operation");
        assert_eq!(
            parsed["ref"],
            "a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77"
        );
        assert_eq!(parsed["content"]["action"], "link");
    }

    #[test]
    fn test_post_builder_with_channel() {
        let account = TestAccount::new();
        let channel = aleph_types::channel::Channel::from("TEST".to_string());
        let msg = PostBuilder::new(&account, "chat", serde_json::json!({"body": "hello"}))
            .unwrap()
            .channel(channel.clone())
            .build()
            .unwrap();

        assert_eq!(msg.channel, Some(channel));
    }

    #[test]
    fn test_aggregate_builder() {
        let account = TestAccount::new();
        let mut content = serde_json::Map::new();
        content.insert("setting".into(), serde_json::json!("value"));

        let msg = AggregateBuilder::new(&account, "my_settings", content)
            .build()
            .unwrap();

        assert_eq!(msg.message_type, MessageType::Aggregate);
        assert_eq!(msg.item_type, ItemType::Inline);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["key"], "my_settings");
        assert_eq!(parsed["content"]["setting"], "value");
    }

    #[test]
    fn test_aggregate_builder_with_channel() {
        let account = TestAccount::new();
        let mut content = serde_json::Map::new();
        content.insert("setting".into(), serde_json::json!("value"));
        let channel = aleph_types::channel::Channel::from("TEST".to_string());

        let msg = AggregateBuilder::new(&account, "my_settings", content)
            .channel(channel.clone())
            .build()
            .unwrap();

        assert_eq!(msg.channel, Some(channel));
    }

    #[test]
    fn test_forget_builder() {
        let account = TestAccount::new();
        let hash = aleph_types::item_hash!(
            "ecd3bab3db7b449ad7875336c9a46dbbe6a010b023fc9525d81e8fdf56936ea1"
        );
        let msg = ForgetBuilder::new(&account, vec![hash])
            .reason("no longer needed")
            .build()
            .unwrap();

        assert_eq!(msg.message_type, MessageType::Forget);
        assert_eq!(msg.item_type, ItemType::Inline);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(
            parsed["hashes"][0],
            "ecd3bab3db7b449ad7875336c9a46dbbe6a010b023fc9525d81e8fdf56936ea1"
        );
        assert_eq!(parsed["reason"], "no longer needed");
    }

    #[test]
    fn test_forget_builder_with_aggregates() {
        let account = TestAccount::new();
        let hash = aleph_types::item_hash!(
            "ecd3bab3db7b449ad7875336c9a46dbbe6a010b023fc9525d81e8fdf56936ea1"
        );
        let agg_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let msg = ForgetBuilder::new(&account, vec![hash])
            .aggregates(vec![agg_hash])
            .reason("cleanup")
            .build()
            .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(
            parsed["aggregates"][0],
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
    }

    #[test]
    fn test_forget_builder_empty_rejects() {
        let account = TestAccount::new();
        let err = ForgetBuilder::new(&account, vec![]).build().unwrap_err();
        assert!(matches!(err, MessageBuildError::EmptyForget));
    }

    #[test]
    fn test_store_builder_storage() {
        use aleph_types::message::{StorageEngine, StoreContent};

        let account = TestAccount::new();
        let file_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let msg = StoreBuilder::new(&account, file_hash, StorageEngine::Storage)
            .build()
            .unwrap();

        assert_eq!(msg.message_type, MessageType::Store);
        assert_eq!(msg.item_type, ItemType::Inline);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["item_type"], "storage");
        assert_eq!(
            parsed["item_hash"],
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        assert_eq!(
            parsed["address"],
            "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"
        );
        assert!(parsed["time"].is_number());

        // Round-trip through StoreContent deserialization
        let store: StoreContent = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(
            store.file_hash().to_string(),
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
    }

    #[test]
    fn test_store_builder_ipfs() {
        use aleph_types::message::StorageEngine;

        let account = TestAccount::new();
        let file_hash = aleph_types::item_hash!("QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8");
        let msg = StoreBuilder::new(&account, file_hash, StorageEngine::Ipfs)
            .build()
            .unwrap();

        assert_eq!(msg.message_type, MessageType::Store);
        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["item_type"], "ipfs");
        assert_eq!(
            parsed["item_hash"],
            "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8"
        );
    }

    #[test]
    fn test_store_builder_with_reference() {
        use aleph_types::message::StorageEngine;

        let account = TestAccount::new();
        let file_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let msg = StoreBuilder::new(&account, file_hash, StorageEngine::Storage)
            .reference("my-custom-ref")
            .build()
            .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["ref"], "my-custom-ref");
    }

    #[test]
    fn test_store_builder_with_metadata() {
        use aleph_types::message::StorageEngine;
        use std::collections::HashMap;

        let account = TestAccount::new();
        let file_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let mut metadata = HashMap::new();
        metadata.insert("filename".to_string(), serde_json::json!("test.pdf"));
        let msg = StoreBuilder::new(&account, file_hash, StorageEngine::Storage)
            .metadata(metadata)
            .build()
            .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["metadata"]["filename"], "test.pdf");
    }

    #[test]
    fn test_store_builder_with_channel() {
        use aleph_types::message::StorageEngine;

        let account = TestAccount::new();
        let file_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let channel = aleph_types::channel::Channel::from("TEST".to_string());
        let msg = StoreBuilder::new(&account, file_hash, StorageEngine::Storage)
            .channel(channel.clone())
            .build()
            .unwrap();

        assert_eq!(msg.channel, Some(channel));
    }

    #[test]
    fn test_store_builder_engine_mismatch() {
        use aleph_types::message::StorageEngine;

        let account = TestAccount::new();
        // Native hash with Ipfs engine should error
        let file_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let err = StoreBuilder::new(&account, file_hash, StorageEngine::Ipfs)
            .build()
            .unwrap_err();
        assert!(matches!(
            err,
            MessageBuildError::StorageEngineMismatch { .. }
        ));
    }
}
