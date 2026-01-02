use crate::chain::{Address, Chain, Signature};
use crate::channel::Channel;
use crate::item_hash::ItemHash;
use crate::message::aggregate::AggregateContent;
use crate::message::forget::ForgetContent;
use crate::message::instance::InstanceContent;
use crate::message::post::PostContent;
use crate::message::program::ProgramContent;
use crate::message::store::StoreContent;
use crate::timestamp::Timestamp;
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;

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
    content: MessageContentEnum,
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

        // Deserialize content based on message_type for efficiency
        let content_obj = raw
            .content
            .as_object()
            .ok_or_else(|| de::Error::custom("content must be an object"))?;

        let address = content_obj
            .get("address")
            .ok_or_else(|| de::Error::missing_field("content.address"))
            .and_then(|v| Address::deserialize(v).map_err(de::Error::custom))?;

        let time = content_obj
            .get("time")
            .ok_or_else(|| de::Error::missing_field("content.time"))
            .and_then(|v| Timestamp::deserialize(v).map_err(de::Error::custom))?;

        // Deserialize the specific variant based on message_type
        let variant = match raw.message_type {
            MessageType::Aggregate => MessageContentEnum::Aggregate(
                AggregateContent::deserialize(&raw.content).map_err(de::Error::custom)?,
            ),
            MessageType::Forget => MessageContentEnum::Forget(
                ForgetContent::deserialize(&raw.content).map_err(de::Error::custom)?,
            ),
            MessageType::Instance => MessageContentEnum::Instance(
                InstanceContent::deserialize(&raw.content).map_err(de::Error::custom)?,
            ),
            MessageType::Post => MessageContentEnum::Post(
                PostContent::deserialize(&raw.content).map_err(de::Error::custom)?,
            ),
            MessageType::Program => MessageContentEnum::Program(
                ProgramContent::deserialize(&raw.content).map_err(de::Error::custom)?,
            ),
            MessageType::Store => MessageContentEnum::Store(
                StoreContent::deserialize(&raw.content).map_err(de::Error::custom)?,
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

        let mut state = serializer.serialize_struct("Message", 9)?;
        state.serialize_field("chain", &self.chain)?;
        state.serialize_field("sender", &self.sender)?;
        match &self.content_source {
            ContentSource::Inline { item_content } => {
                state.serialize_field("item_type", "inline")?;
                state.serialize_field("item_content", item_content)?;
            }
            ContentSource::Storage => {
                state.serialize_field("item_type", "storage")?;
                state.serialize_field("item_content", &None::<String>)?;
            }
            ContentSource::Ipfs => {
                state.serialize_field("item_type", "ipfs")?;
                state.serialize_field("item_content", &None::<String>)?;
            }
        }
        state.serialize_field("signature", &self.signature)?;
        state.serialize_field("item_hash", &self.item_hash)?;
        if self.confirmed() {
            state.serialize_field("confirmed", &true)?;
            state.serialize_field("confirmations", &self.confirmations)?;
        }
        state.serialize_field("time", &self.time)?;
        if self.channel.is_some() {
            state.serialize_field("channel", &self.channel)?;
        }
        state.serialize_field("type", &self.message_type)?;
        state.serialize_field("content", &self.content)?;
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
