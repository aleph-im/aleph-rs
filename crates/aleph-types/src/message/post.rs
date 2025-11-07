use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PostType {
    Amend {
        #[serde(rename = "ref")]
        reference: String,
    },
    Other {
        #[serde(rename = "type")]
        post_type: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PostContent {
    #[serde(flatten)]
    pub post_type: PostType,
    pub content: Option<serde_json::Value>,
}

impl PostContent {
    pub fn is_amend(&self) -> bool {
        matches!(self.post_type, PostType::Amend { .. })
    }

    pub fn post_type_str(&self) -> &str {
        match &self.post_type {
            PostType::Amend { .. } => "amend",
            PostType::Other { post_type } => post_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::Chain;
    use crate::message::base_message::MessageContentEnum;
    use crate::message::{ContentSource, Message, MessageType};
    use crate::timestamp::Timestamp;
    use crate::{address, channel, item_hash, signature};
    use assert_matches::assert_matches;

    const POST_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/post/post.json"
    ));

    const AMEND_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/post/amend.json"
    ));

    #[test]
    fn test_deserialize_post() {
        let message: Message = serde_json::from_str(POST_FIXTURE).unwrap();

        assert_eq!(
            message.sender,
            address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef")
        );
        assert_eq!(message.chain, Chain::Ethereum);
        assert_eq!(
            message.signature,
            signature!(
                "0x636728dbea33fa6064f099045421b980dff3c71932cd89c2bf42387ebb6f53890a24e13f16678463876224772b90838c2b9557cd8fc2cdae45509ce8cb89e3fd1b"
            )
        );
        assert_matches!(message.message_type, MessageType::Post);
        assert_matches!(
            message.content_source,
            ContentSource::Inline { item_content: _ }
        );
        assert_eq!(
            message.item_hash,
            item_hash!("d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c")
        );
        assert_eq!(message.time, Timestamp::try_from(1762515431.653).unwrap());
        assert_eq!(message.channel, Some(channel!("TEST")));

        // Check content fields
        assert_eq!(
            &message.content.address,
            &address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef")
        );
        assert_eq!(
            &message.content.time,
            &Timestamp::try_from(1762515431.653).unwrap()
        );

        // Check aggregate content fields
        let post_content = match message.content() {
            MessageContentEnum::Post(content) => content,
            other => {
                panic!("Expected MessageContentEnum::Post, got {:?}", other);
            }
        };

        #[derive(Deserialize)]
        struct ContentStruct {
            body: String,
        }
        let deserialized_content =
            serde_json::from_value::<ContentStruct>(post_content.content.clone().unwrap()).unwrap();
        assert_eq!(deserialized_content.body, "Hello World");

        assert!(!post_content.is_amend());
        assert_eq!(
            post_content.post_type_str(),
            "05567c5b-0606-4a6e-a639-25734c06e2a0"
        );

        // No confirmation on this fixture
        assert!(!message.confirmed());
        assert!(message.confirmations.is_empty());
    }

    #[test]
    fn test_deserialize_amend() {
        let message: Message = serde_json::from_str(AMEND_FIXTURE).unwrap();

        assert_eq!(
            message.sender,
            address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef")
        );
        assert_eq!(message.chain, Chain::Ethereum);
        assert_eq!(
            message.signature,
            signature!(
                "0xf4a171d2715f2249c6f78160a83a64fb05c21962acdf3729e373fd4f527ed9f570274dedcc468195ba40a26002be0b72aedf260df74032d4ef5a12cb4ea838831c"
            )
        );
        assert_matches!(message.message_type, MessageType::Post);
        assert_matches!(
            message.content_source,
            ContentSource::Inline { item_content: _ }
        );
        assert_eq!(
            message.item_hash,
            item_hash!("203291b2581b379997b8a0fda43d3afe27573489ca695b711d67fd1a6311b3dd")
        );
        assert_eq!(message.time, Timestamp::try_from(1762515432.375).unwrap());
        assert_eq!(message.channel, Some(channel!("TEST")));

        // Check content fields
        assert_eq!(
            &message.content.address,
            &address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef")
        );
        assert_eq!(
            &message.content.time,
            &Timestamp::try_from(1762515432.375).unwrap()
        );

        // Check aggregate content fields
        let post_content = match message.content() {
            MessageContentEnum::Post(content) => content,
            other => {
                panic!("Expected MessageContentEnum::Post, got {:?}", other);
            }
        };

        #[derive(Deserialize)]
        struct ContentStruct {
            body: String,
        }
        let deserialized_content =
            serde_json::from_value::<ContentStruct>(post_content.content.clone().unwrap()).unwrap();
        assert_eq!(deserialized_content.body, "New content !");

        assert!(post_content.is_amend());
        assert_eq!(post_content.post_type_str(), "amend");

        // No confirmation on this fixture
        assert!(!message.confirmed());
        assert!(message.confirmations.is_empty());
    }
}
