use aleph_types::account::{Account, SignError};
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::pending::PendingMessage;
use aleph_types::message::{
    AggregateContent, AggregateKey, ForgetContent, MessageType, PostContent, PostType,
};
use serde::Serialize;
use thiserror::Error;

use crate::builder::MessageBuilder;

#[derive(Debug, Error)]
pub enum MessageBuildError {
    #[error("content serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("message signing failed: {0}")]
    Signing(#[from] SignError),
}

pub struct PostBuilder<'a, A: Account> {
    account: &'a A,
    post_type: PostType,
    content: serde_json::Value,
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
            channel: None,
        })
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
        let value = serde_json::to_value(post_content)?;
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
        let forget_content = ForgetContent::new(self.hashes, self.aggregates, self.reason);
        let value = serde_json::to_value(forget_content)?;
        let mut builder = MessageBuilder::new(self.account, MessageType::Forget, value);
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
}
