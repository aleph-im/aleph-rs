use crate::chain::{Address, Chain, Signature};
use crate::channel::Channel;
use crate::item_hash::ItemHash;
use crate::message::item_type::ItemType;
use crate::message::{ContentSource, Message, MessageType};
use crate::timestamp::Timestamp;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

/// A signed message ready for submission to the Aleph network.
///
/// The `item_content` field is always present in memory (needed for uploading
/// storage/IPFS content), but the custom `Serialize` implementation only emits
/// it when `item_type == Inline`.
#[derive(Debug, Clone)]
pub struct PendingMessage {
    pub chain: Chain,
    pub sender: Address,
    pub signature: Signature,
    pub message_type: MessageType,
    pub item_type: ItemType,
    pub item_content: String,
    pub item_hash: ItemHash,
    pub time: Timestamp,
    pub channel: Option<Channel>,
}

impl Serialize for PendingMessage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let has_content = self.item_type == ItemType::Inline;
        let has_channel = self.channel.is_some();
        let field_count = 7 + has_content as usize + has_channel as usize;

        let mut state = serializer.serialize_struct("PendingMessage", field_count)?;
        state.serialize_field("sender", &self.sender)?;
        state.serialize_field("chain", &self.chain)?;
        state.serialize_field("signature", &self.signature)?;
        state.serialize_field("type", &self.message_type)?;
        state.serialize_field("item_type", &self.item_type)?;
        if has_content {
            state.serialize_field("item_content", &self.item_content)?;
        }
        state.serialize_field("item_hash", &self.item_hash)?;
        state.serialize_field("time", &self.time)?;
        if let Some(channel) = &self.channel {
            state.serialize_field("channel", channel)?;
        }
        state.end()
    }
}

impl From<&Message> for PendingMessage {
    fn from(message: &Message) -> Self {
        let (item_type, item_content) = match &message.content_source {
            ContentSource::Inline { item_content } => {
                (ItemType::Inline, item_content.clone())
            }
            ContentSource::Storage => (ItemType::Storage, String::new()),
            ContentSource::Ipfs => (ItemType::Ipfs, String::new()),
        };
        PendingMessage {
            chain: message.chain.clone(),
            sender: message.sender.clone(),
            signature: message.signature.clone(),
            message_type: message.message_type,
            item_type,
            item_content,
            item_hash: message.item_hash.clone(),
            time: message.time.clone(),
            channel: message.channel.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{address, item_hash};

    fn make_pending(item_type: ItemType) -> PendingMessage {
        PendingMessage {
            chain: Chain::Ethereum,
            sender: address!("0xABCD"),
            signature: Signature::from("0xSIG".to_string()),
            message_type: MessageType::Post,
            item_type,
            item_content: r#"{"type":"test","address":"0xABCD","time":1234.0}"#.to_string(),
            item_hash: item_hash!("d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"),
            time: Timestamp::from(1234.0),
            channel: None,
        }
    }

    #[test]
    fn test_pending_message_inline_includes_item_content() {
        let msg = make_pending(ItemType::Inline);
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["item_type"], "inline");
        assert!(json.get("item_content").is_some());
        assert_eq!(json["item_content"], msg.item_content);
    }

    #[test]
    fn test_pending_message_storage_omits_item_content() {
        let msg = make_pending(ItemType::Storage);
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["item_type"], "storage");
        assert!(json.get("item_content").is_none());
    }

    #[test]
    fn test_pending_message_ipfs_omits_item_content() {
        let msg = make_pending(ItemType::Ipfs);
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["item_type"], "ipfs");
        assert!(json.get("item_content").is_none());
    }
}
