use crate::item_hash::ItemHash;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForgetContent {
    hashes: Vec<ItemHash>,
    #[serde(default)]
    aggregates: Vec<ItemHash>,
    #[serde(default)]
    reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::chain::Chain;
    use crate::message::base_message::MessageContentEnum;
    use crate::message::{ContentSource, Message, MessageType};
    use crate::timestamp::Timestamp;
    use crate::{address, channel, item_hash, signature};
    use assert_matches::assert_matches;

    const FORGET_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/forget/forget.json"
    ));

    #[test]
    fn test_deserialize_forget() {
        let message: Message = serde_json::from_str(FORGET_FIXTURE).unwrap();

        assert_eq!(
            message.sender,
            address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef")
        );
        assert_eq!(message.chain, Chain::Ethereum);
        assert_eq!(
            message.signature,
            signature!(
                "0x2e80006a8e60cb51b1aaa052069d7b86aeea6f4460f7f0fa824f3ed2b6989e4b6ec9cdf8522a257f1fa4e729e3bbec728f75b1cb538b359cbe7340937b336a771b"
            )
        );
        assert_matches!(message.message_type, MessageType::Forget);
        assert_matches!(
            message.content_source,
            ContentSource::Inline { item_content: _ }
        );
        assert_eq!(
            message.item_hash,
            item_hash!("35ea7a4bdd8c631b5ccec84ddf3b0ac65a0da1fbb2942d77eac27577326a8a0f")
        );
        assert_eq!(message.time, Timestamp::from(1762515432.413));
        assert_eq!(message.channel, Some(channel!("TEST")));

        // Check content fields
        assert_eq!(
            &message.content.address,
            &address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef")
        );
        assert_eq!(&message.content.time, &Timestamp::from(1762515432.413));

        // Check aggregate content fields
        let forget_content = match message.content() {
            MessageContentEnum::Forget(content) => content,
            other => {
                panic!("Expected MessageContentEnum::Forget, got {:?}", other);
            }
        };

        assert_eq!(
            forget_content.hashes,
            vec![item_hash!(
                "ecd3bab3db7b449ad7875336c9a46dbbe6a010b023fc9525d81e8fdf56936ea1"
            )]
        );
        assert_eq!(forget_content.aggregates, vec![]);
        assert_eq!(forget_content.reason, Some("None".to_string()));

        // No confirmation on this fixture
        assert!(!message.confirmed());
        assert!(message.confirmations.is_empty());
    }
}
