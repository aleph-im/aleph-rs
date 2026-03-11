use crate::chain::{Address, Chain, Signature};
use crate::channel::Channel;
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
use std::fmt::Formatter;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MessageVerificationError {
    #[error("Item hash verification failed: expected {expected}, got {actual}")]
    ItemHashVerificationFailed {
        expected: ItemHash,
        actual: ItemHash,
    },
    #[error("Cannot verify non-inline message locally; use the client to verify via /storage/raw/")]
    NonInlineMessage,
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
    Rejected,
}

impl std::fmt::Display for MessageStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MessageStatus::Pending => "pending",
            MessageStatus::Processed => "processed",
            MessageStatus::Removing => "removing",
            MessageStatus::Removed => "removed",
            MessageStatus::Forgotten => "forgotten",
            MessageStatus::Rejected => "rejected",
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

impl MessageContent {
    /// Deserializes message content from raw JSON bytes, using `message_type` to select
    /// the correct content variant. This is the type-directed deserialization used by
    /// the verified message path.
    pub fn deserialize_with_type(
        message_type: MessageType,
        raw: &[u8],
    ) -> Result<Self, serde_json::Error> {
        let value: serde_json::Value = serde_json::from_slice(raw)?;
        Self::from_json_value(message_type, &value)
    }

    fn from_json_value(
        message_type: MessageType,
        value: &serde_json::Value,
    ) -> Result<Self, serde_json::Error> {
        let address = Address::deserialize(&value["address"])?;
        let time = Timestamp::deserialize(&value["time"])?;

        let variant = match message_type {
            MessageType::Aggregate => {
                MessageContentEnum::Aggregate(AggregateContent::deserialize(value)?)
            }
            MessageType::Forget => MessageContentEnum::Forget(ForgetContent::deserialize(value)?),
            MessageType::Instance => {
                MessageContentEnum::Instance(InstanceContent::deserialize(value)?)
            }
            MessageType::Post => MessageContentEnum::Post(PostContent::deserialize(value)?),
            MessageType::Program => {
                MessageContentEnum::Program(ProgramContent::deserialize(value)?)
            }
            MessageType::Store => MessageContentEnum::Store(StoreContent::deserialize(value)?),
        };

        Ok(MessageContent {
            address,
            time,
            content: variant,
        })
    }
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

impl ContentSource {
    /// For inline messages, verifies that `item_content` hashes to `expected_hash`.
    ///
    /// Returns `Some(Ok(()))` if the hash matches, `Some(Err((expected, actual)))` on mismatch,
    /// or `None` for non-inline messages (which require network-based verification).
    pub fn verify_inline_hash(
        &self,
        expected_hash: &ItemHash,
    ) -> Option<Result<(), (ItemHash, ItemHash)>> {
        match self {
            ContentSource::Inline { item_content } => {
                let computed = AlephItemHash::from_bytes(item_content.as_bytes());
                if ItemHash::Native(computed) != *expected_hash {
                    Some(Err((expected_hash.clone(), computed.into())))
                } else {
                    Some(Ok(()))
                }
            }
            ContentSource::Storage | ContentSource::Ipfs => None,
        }
    }
}

/// A message without its deserialized content.
///
/// Used by the verified message path: the client fetches message headers, then downloads
/// and verifies raw content separately before deserializing it. This avoids trusting
/// the CCN's pre-deserialized `content` field.
#[derive(PartialEq, Debug, Clone)]
pub struct MessageHeader {
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
}

impl MessageHeader {
    /// Assembles a full [`Message`] by combining this header with deserialized content.
    pub fn with_content(self, content: MessageContent) -> Message {
        Message {
            chain: self.chain,
            sender: self.sender,
            signature: self.signature,
            content_source: self.content_source,
            item_hash: self.item_hash,
            confirmations: self.confirmations,
            time: self.time,
            channel: self.channel,
            message_type: self.message_type,
            content,
        }
    }
}

impl From<Message> for MessageHeader {
    fn from(message: Message) -> Self {
        MessageHeader {
            chain: message.chain,
            sender: message.sender,
            signature: message.signature,
            content_source: message.content_source,
            item_hash: message.item_hash,
            confirmations: message.confirmations,
            time: message.time,
            channel: message.channel,
            message_type: message.message_type,
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

    /// Verifies that the item hash of an inline message matches its content.
    ///
    /// For inline messages, the item hash is the SHA-256 hash of the `item_content` string.
    /// For non-inline messages (storage/ipfs), use the client's `verify_message()` method
    /// instead, which downloads the raw content from `/storage/raw/` for verification.
    pub fn verify_item_hash(&self) -> Result<(), MessageVerificationError> {
        match self.content_source.verify_inline_hash(&self.item_hash) {
            Some(Ok(())) => Ok(()),
            Some(Err((expected, actual))) => {
                Err(MessageVerificationError::ItemHashVerificationFailed { expected, actual })
            }
            None => Err(MessageVerificationError::NonInlineMessage),
        }
    }
}

/// Shared helper struct for deserializing message header fields.
/// Used by both `Message` and `MessageHeader` Deserialize impls.
#[derive(Deserialize)]
struct MessageHeaderRaw {
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
}

impl MessageHeaderRaw {
    fn into_header(self) -> MessageHeader {
        MessageHeader {
            chain: self.chain,
            sender: self.sender,
            signature: self.signature,
            content_source: self.content_source,
            item_hash: self.item_hash,
            confirmations: self.confirmations.unwrap_or_default(),
            time: self.time,
            channel: self.channel,
            message_type: self.message_type,
        }
    }
}

impl<'de> Deserialize<'de> for MessageHeader {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        MessageHeaderRaw::deserialize(deserializer).map(MessageHeaderRaw::into_header)
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
            #[serde(flatten)]
            header: MessageHeaderRaw,
            content: serde_json::Value,
        }

        let raw = MessageRaw::deserialize(deserializer)?;

        let content = MessageContent::from_json_value(raw.header.message_type, &raw.content)
            .map_err(de::Error::custom)?;

        Ok(raw.header.into_header().with_content(content))
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
    use crate::item_hash;
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
    fn test_verify_inline_message_item_hash() {
        let json = include_str!("../../../../fixtures/messages/post/post.json");
        let message: Message = serde_json::from_str(json).unwrap();
        message.verify_item_hash().unwrap();

        // STORE message envelope is inline; only the referenced file lives on IPFS.
        let json = include_str!("../../../../fixtures/messages/store/store-ipfs.json");
        let message: Message = serde_json::from_str(json).unwrap();
        message.verify_item_hash().unwrap();
    }

    #[test]
    fn test_verify_inline_message_detects_tampered_hash() {
        let json = include_str!("../../../../fixtures/messages/post/post.json");
        let mut message: Message = serde_json::from_str(json).unwrap();
        message.item_hash =
            item_hash!("0000000000000000000000000000000000000000000000000000000000000000");
        assert_matches!(
            message.verify_item_hash(),
            Err(MessageVerificationError::ItemHashVerificationFailed { .. })
        );
    }

    #[test]
    fn test_verify_inline_message_detects_tampered_content() {
        let json = include_str!("../../../../fixtures/messages/post/post.json");
        let mut message: Message = serde_json::from_str(json).unwrap();
        // Corrupt the item_content while keeping the original item_hash
        if let ContentSource::Inline {
            ref mut item_content,
        } = message.content_source
        {
            item_content.push('!');
        }
        assert_matches!(
            message.verify_item_hash(),
            Err(MessageVerificationError::ItemHashVerificationFailed { .. })
        );
    }

    #[test]
    fn test_verify_non_inline_message_returns_error() {
        let json = include_str!("../../../../fixtures/messages/aggregate/aggregate.json");
        let message: Message = serde_json::from_str(json).unwrap();
        assert_matches!(
            message.verify_item_hash(),
            Err(MessageVerificationError::NonInlineMessage)
        );
    }

    #[test]
    fn test_deserialize_message_header() {
        let json = include_str!("../../../../fixtures/messages/post/post.json");
        let header: MessageHeader = serde_json::from_str(json).unwrap();
        let message: Message = serde_json::from_str(json).unwrap();

        // Header fields should match Message fields
        assert_eq!(header.chain, message.chain);
        assert_eq!(header.sender, message.sender);
        assert_eq!(header.signature, message.signature);
        assert_eq!(header.content_source, message.content_source);
        assert_eq!(header.item_hash, message.item_hash);
        assert_eq!(header.time, message.time);
        assert_eq!(header.channel, message.channel);
        assert_eq!(header.message_type, message.message_type);
    }

    #[test]
    fn test_message_header_with_content_roundtrip() {
        let json = include_str!("../../../../fixtures/messages/post/post.json");
        let message: Message = serde_json::from_str(json).unwrap();

        // Convert to header, then reassemble with original content
        let content = message.content.clone();
        let header = MessageHeader::from(message.clone());
        let reassembled = header.with_content(content);
        assert_eq!(reassembled, message);
    }

    #[test]
    fn test_deserialize_content_with_type() {
        let json = include_str!("../../../../fixtures/messages/post/post.json");
        let message: Message = serde_json::from_str(json).unwrap();

        // For inline messages, deserialize_with_type from item_content should match
        if let ContentSource::Inline { ref item_content } = message.content_source {
            let content = MessageContent::deserialize_with_type(
                message.message_type,
                item_content.as_bytes(),
            )
            .unwrap();
            assert_eq!(content, message.content);
        } else {
            panic!("Expected inline message");
        }
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
