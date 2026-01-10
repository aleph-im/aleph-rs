use crate::chain::{Address, Chain, Signature};
use crate::channel::Channel;
use crate::cid::{Cid, CidV0};
use crate::item_hash::{AlephItemHash, ItemHash};
use crate::message::aggregate::AggregateContent;
use crate::message::forget::ForgetContent;
use crate::message::instance::InstanceContent;
use crate::message::post::PostContent;
use crate::message::program::ProgramContent;
use crate::message::store::StoreContent;
use crate::timestamp::Timestamp;
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::Formatter;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MessageVerificationError {
    #[error("Item hash verification failed: expected {expected}, got {actual}")]
    ItemHashVerificationFailed {
        expected: ItemHash,
        actual: ItemHash,
    },
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MessageType {
    Aggregate,
    Forget,
    Instance,
    Post,
    Program,
    Store,
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MessageType::Aggregate => "AGGREGATE",
            MessageType::Forget => "FORGET",
            MessageType::Instance => "INSTANCE",
            MessageType::Post => "POST",
            MessageType::Program => "PROGRAM",
            MessageType::Store => "STORE",
        };

        f.write_str(s)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageStatus {
    Pending,
    Processed,
    Removing,
    Removed,
    Forgotten,
}

impl std::fmt::Display for MessageStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MessageStatus::Pending => "pending",
            MessageStatus::Processed => "processed",
            MessageStatus::Removing => "removing",
            MessageStatus::Removed => "removed",
            MessageStatus::Forgotten => "forgotten",
        };

        f.write_str(s)
    }
}

/// Content variants for different message types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContentEnum {
    Aggregate(AggregateContent),
    Forget(ForgetContent),
    Instance(InstanceContent),
    Post(PostContent),
    Program(ProgramContent),
    Store(StoreContent),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageContent {
    pub address: Address,
    pub time: Timestamp,
    #[serde(flatten)]
    pub content: MessageContentEnum,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageConfirmation {
    pub chain: Chain,
    pub height: u64,
    pub hash: String,
    pub time: Option<Timestamp>,
    pub publisher: Option<Address>,
}

/// Where to find the content of the message. Note that this is a mix of ItemType / ItemContent
/// if you are used to the Python SDK.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentSource {
    Inline { item_content: String },
    Storage,
    Ipfs,
}

impl<'de> Deserialize<'de> for ContentSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ContentSourceRaw {
            item_type: String,
            item_content: Option<String>,
        }

        let raw = ContentSourceRaw::deserialize(deserializer)?;

        match raw.item_type.as_str() {
            "inline" => {
                let item_content = raw
                    .item_content
                    .ok_or_else(|| de::Error::missing_field("item_content"))?;
                Ok(ContentSource::Inline { item_content })
            }
            "storage" => Ok(ContentSource::Storage),
            "ipfs" => Ok(ContentSource::Ipfs),
            other => Err(de::Error::unknown_variant(
                other,
                &["inline", "storage", "ipfs"],
            )),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Message {
    /// Blockchain used for this message.
    pub chain: Chain,
    /// Sender address.
    pub sender: Address,
    /// Cryptographic signature of the message by the sender.
    pub signature: Signature,
    /// Content of the message as created by the sender. Can either be inline or stored
    /// on Aleph Cloud.
    pub content_source: ContentSource,
    /// Hash of the content (SHA2-256).
    pub item_hash: ItemHash,
    /// List of confirmations for the message.
    pub confirmations: Vec<MessageConfirmation>,
    /// Unix timestamp or datetime when the message was published.
    pub time: Timestamp,
    /// Channel of the message, one application ideally has one channel.
    pub channel: Option<Channel>,
    /// Message type. (aggregate, forget, instance, post, program, store).
    pub message_type: MessageType,
    /// Message content.
    pub content: MessageContent,
}

impl Message {
    pub fn content(&self) -> &MessageContentEnum {
        &self.content.content
    }

    pub fn confirmed(&self) -> bool {
        !self.confirmations.is_empty()
    }

    /// Returns the address of the sender of the message. Note that the sender is not necessarily
    /// the owner of the resources, as the owner may have delegated their authority to create
    /// specific resources through the permission system.
    pub fn sender(&self) -> &Address {
        &self.sender
    }

    /// Returns the address of the owner of the resources.
    pub fn owner(&self) -> &Address {
        &self.content.address
    }

    /// Returns the time at which the message was sent.
    /// Notes:
    /// * This value is signed by the sender and should not be trusted accordingly.
    /// * We prefer `content.time` over `time` as `time` is not part of the signed payload.
    pub fn sent_at(&self) -> &Timestamp {
        &self.content.time
    }

    /// Returns the earliest confirmation time of the message.
    pub fn confirmed_at(&self) -> Option<&Timestamp> {
        self.confirmations.first().and_then(|c| c.time.as_ref())
    }

    /// Computes the item hash of the message from its content.
    fn compute_item_hash(&self) -> Result<ItemHash, serde_json::Error> {
        let serialized_message_content = match &self.content_source {
            ContentSource::Inline { item_content } => Cow::Borrowed(item_content),
            ContentSource::Storage | ContentSource::Ipfs => {
                let s = serde_json::to_string(&self.content)?;
                println!("SERIALIZED CONTENT: {:?}", s);
                println!("SERIALIZED CONTENT BYTES: {:?}", s.as_bytes());
                Cow::Owned(s)
            }
        };

        let computed_item_hash: ItemHash = match &self.content_source {
            ContentSource::Inline { .. } | ContentSource::Storage => {
                AlephItemHash::from_bytes(serialized_message_content.as_bytes()).into()
            }
            ContentSource::Ipfs => {
                Cid::from(CidV0::from_bytes(serialized_message_content.as_bytes())).into()
            }
        };

        Ok(computed_item_hash)
    }

    /// Verifies that the item hash of the message matches its content.
    pub fn verify_item_hash(&self) -> Result<(), MessageVerificationError> {
        let actual_hash = self.compute_item_hash()?;

        if actual_hash != self.item_hash {
            return Err(MessageVerificationError::ItemHashVerificationFailed {
                expected: self.item_hash.clone(),
                actual: actual_hash,
            });
        }

        Ok(())
    }
}

// Custom deserializer that uses message_type to efficiently deserialize content
impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct MessageRaw {
            chain: Chain,
            sender: Address,
            signature: Signature,
            #[serde(flatten)]
            content_source: ContentSource,
            item_hash: ItemHash,
            #[serde(default)]
            confirmations: Option<Vec<MessageConfirmation>>,
            time: Timestamp,
            #[serde(default)]
            channel: Option<Channel>,
            #[serde(rename = "type")]
            message_type: MessageType,
            content: serde_json::Value,
        }

        let raw = MessageRaw::deserialize(deserializer)?;

        let content_value = raw.content;

        let address = Address::deserialize(&content_value["address"]).map_err(de::Error::custom)?;
        let time = Timestamp::deserialize(&content_value["time"]).map_err(de::Error::custom)?;

        // Deserialize the specific variant based on message_type
        let variant = match raw.message_type {
            MessageType::Aggregate => MessageContentEnum::Aggregate(
                AggregateContent::deserialize(&content_value).map_err(de::Error::custom)?,
            ),
            MessageType::Forget => MessageContentEnum::Forget(
                ForgetContent::deserialize(&content_value).map_err(de::Error::custom)?,
            ),
            MessageType::Instance => MessageContentEnum::Instance(
                InstanceContent::deserialize(&content_value).map_err(de::Error::custom)?,
            ),
            MessageType::Post => MessageContentEnum::Post(
                PostContent::deserialize(&content_value).map_err(de::Error::custom)?,
            ),
            MessageType::Program => MessageContentEnum::Program(
                ProgramContent::deserialize(&content_value).map_err(de::Error::custom)?,
            ),
            MessageType::Store => MessageContentEnum::Store(
                StoreContent::deserialize(&content_value).map_err(de::Error::custom)?,
            ),
        };

        Ok(Message {
            chain: raw.chain,
            sender: raw.sender,
            signature: raw.signature,
            content_source: raw.content_source,
            item_hash: raw.item_hash,
            confirmations: raw.confirmations.unwrap_or_default(),
            time: raw.time,
            channel: raw.channel,
            message_type: raw.message_type,
            content: MessageContent {
                address,
                time,
                content: variant,
            },
        })
    }
}

// Manual Serialize for Message
impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("Message", 12)?;
        state.serialize_field("sender", &self.sender)?;
        state.serialize_field("chain", &self.chain)?;
        state.serialize_field("signature", &self.signature)?;
        state.serialize_field("type", &self.message_type)?;
        state.serialize_field("item_content", &match &self.content_source {
            ContentSource::Inline { item_content } => Some(item_content),
            ContentSource::Storage | ContentSource::Ipfs => None,
        })?;
        state.serialize_field("item_hash", &self.item_hash)?;
        state.serialize_field("item_type", match &self.content_source {
            ContentSource::Inline { .. } => "inline",
            ContentSource::Storage => "storage",
            ContentSource::Ipfs => "ipfs",
        })?;
        state.serialize_field("time", &self.time)?;
        state.serialize_field("channel", &self.channel)?;
        state.serialize_field("content", &self.content)?;
        state.serialize_field("confirmed", &self.confirmed())?;
        state.serialize_field("confirmations", &self.confirmations)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn test_deserialize_item_type_inline() {
        let item_content_str = "test".to_string();
        let content_source_str =
            format!("{{\"item_type\":\"inline\",\"item_content\":\"{item_content_str}\"}}");
        let content_source: ContentSource = serde_json::from_str(&content_source_str).unwrap();

        assert_matches!(
            content_source,
            ContentSource::Inline {
                item_content
            } if item_content == item_content_str
        );
    }

    #[test]
    fn test_deserialize_item_type_storage() {
        let content_source_str = r#"{"item_type":"storage"}"#;
        let content_source: ContentSource = serde_json::from_str(content_source_str).unwrap();
        assert_matches!(content_source, ContentSource::Storage);
    }

    #[test]
    fn test_deserialize_item_type_ipfs() {
        let content_source_str = r#"{"item_type":"ipfs"}"#;
        let content_source: ContentSource = serde_json::from_str(content_source_str).unwrap();
        assert_matches!(content_source, ContentSource::Ipfs);
    }

    #[test]
    fn test_message_verify_item_hash() {
        let json = include_str!("../../../../fixtures/messages/post/post.json");
        let message: Message = serde_json::from_str(json).unwrap();
        message.verify_item_hash().unwrap();

        let json = include_str!("../../../../fixtures/messages/store/store-ipfs.json");
        let message: Message = serde_json::from_str(json).unwrap();
        message.verify_item_hash().unwrap();
    }

    #[test]
    fn test_deserialize_item_type_invalid_type() {
        let content_source_str = r#"{"item_type":"invalid"}"#;
        let result = serde_json::from_str::<ContentSource>(content_source_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_item_type_invalid_format() {
        let content_source_str = r#"{"type":"inline"}"#;
        let result = serde_json::from_str::<ContentSource>(content_source_str);
        assert!(result.is_err());
    }
}
