use crate::channel::Channel;
use crate::item_hash::ItemHash;
use crate::message::MessageType;
use crate::message::item_type::ItemType;
use crate::timestamp::Timestamp;

#[derive(Debug, Clone)]
pub struct UnsignedMessage {
    pub message_type: MessageType,
    pub item_type: ItemType,
    pub item_content: String,
    pub item_hash: ItemHash,
    pub time: Timestamp,
    pub channel: Option<Channel>,
}
