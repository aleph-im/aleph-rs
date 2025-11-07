use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateKeyDict {
    name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AggregateKey {
    String(String),
    Dict(AggregateKeyDict),
}

impl AggregateKey {
    pub fn key(&self) -> &str {
        match self {
            AggregateKey::String(key) => key,
            AggregateKey::Dict(dict) => dict.name.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateContent {
    /// The aggregate key can be either a string of a dict containing the key in field 'name'.
    key: AggregateKey,
    /// The content of the aggregate. The only restriction is that this must be a dictionary.
    content: HashMap<serde_json::Value, serde_json::Value>,
}

impl AggregateContent {
    pub fn key(&self) -> &str {
        self.key.key()
    }
}

#[cfg(test)]
mod tests {
    use crate::chain::Chain;
    use crate::message::base_message::MessageContentEnum;
    use crate::message::{ContentSource, Message, MessageType};
    use crate::timestamp::Timestamp;
    use crate::{address, channel, item_hash, signature};
    use assert_matches::assert_matches;

    const AGGREGATE_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/aggregate/aggregate.json"
    ));

    #[test]
    fn test_deserialize_aggregate() {
        let message: Message = serde_json::from_str(AGGREGATE_FIXTURE).unwrap();

        assert_eq!(
            message.sender,
            address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10")
        );
        assert_eq!(message.chain, Chain::Ethereum);
        assert_eq!(
            message.signature,
            signature!(
                "0x7d14b66772d97f5a7f9915875b34eae3df117f0a2cd6ffada3bfee09313441e853c1fdf813158b3109e1f85aacb8410894e7d76b552a7821d2280aac956528591c"
            )
        );
        assert_matches!(message.message_type, MessageType::Aggregate);
        assert_matches!(
            message.content_source,
            ContentSource::Storage,
            "Expected content_source to be ContentSource::Storage"
        );
        assert_eq!(
            message.item_hash,
            item_hash!("3ad7f29b5b451b3e49d6054a8966aa7e728ac0f07dd7ef25f3bd2455f1408190")
        );
        assert_eq!(
            message.time,
            Timestamp::try_from(1762518461.524221).unwrap()
        );
        assert_eq!(message.channel, Some(channel!("FOUNDATION")));

        // Check content fields
        assert_eq!(
            &message.content.address,
            &address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10")
        );
        assert_eq!(
            &message.content.time,
            &Timestamp::try_from(1762518461.4893668).unwrap()
        );

        // Check aggregate content fields
        let aggregate_content = match message.content() {
            MessageContentEnum::Aggregate(content) => content,
            other => {
                panic!("Expected MessageContentEnum::Aggregate, got {:?}", other);
            }
        };
        assert_eq!(aggregate_content.key(), "corechannel");
        assert!(
            aggregate_content
                .content
                .contains_key(&serde_json::Value::String("nodes".to_string()))
        );
        assert!(
            aggregate_content
                .content
                .contains_key(&serde_json::Value::String("resource_nodes".to_string()))
        );

        // No confirmation on this fixture
        assert!(!message.confirmed());
        assert!(message.confirmations.is_empty());
    }
}
