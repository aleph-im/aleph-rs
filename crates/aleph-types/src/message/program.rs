use crate::item_hash::ItemHash;
use crate::message::execution::base::{Encoding, ExecutableContent, Interface};
use crate::message::execution::environment::{FunctionEnvironment, FunctionTriggers};
use crate::toolkit::serde::{default_some_false, default_true};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionRuntime {
    #[serde(rename = "ref")]
    reference: ItemHash,
    #[serde(default = "default_true")]
    use_latest: bool,
    comment: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CodeContent {
    encoding: Encoding,
    entrypoint: String,
    /// Reference to the STORE message containing the code.
    #[serde(rename = "ref")]
    reference: ItemHash,
    #[serde(default)]
    interface: Option<Interface>,
    #[serde(default)]
    args: Option<Vec<String>>,
    #[serde(default)]
    use_latest: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataContent {
    encoding: Encoding,
    mount: PathBuf,
    #[serde(default, rename = "ref")]
    reference: Option<ItemHash>,
    #[serde(default = "default_some_false")]
    use_latest: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Export {
    encoding: Encoding,
    mount: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgramContent {
    #[serde(flatten)]
    pub base: ExecutableContent,
    /// Code to execute.
    pub code: CodeContent,
    /// Execution runtime (rootfs with Python interpreter).
    pub runtime: FunctionRuntime,
    /// Data to use during computation.
    #[serde(default)]
    pub data: Option<DataContent>,
    /// Properties of the execution environment.
    pub environment: FunctionEnvironment,
    /// Data to export after computation.
    #[serde(default)]
    pub export: Option<Export>,
    /// Signals that trigger an execution.
    pub on: FunctionTriggers,
}

impl ProgramContent {
    pub fn executable_content(&self) -> &ExecutableContent {
        &self.base
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::{Address, Chain, Signature};
    use crate::message::base_message::MessageContentEnum;
    use crate::message::execution::environment::MachineResources;
    use crate::message::execution::volume::{BaseVolume, ImmutableVolume, MachineVolume};
    use crate::message::{ContentSource, Message, MessageType};
    use crate::storage_size::{MemorySize, MiB};
    use crate::timestamp::Timestamp;
    use crate::{channel, item_hash};
    use assert_matches::assert_matches;
    use std::collections::HashMap;

    const PROGRAM_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/program/program.json"
    ));

    const PROGRAM_WITH_EMPTY_ARRAY_AS_METADATA: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/program/program-with-array-as-metadata.json"
    ));

    #[test]
    fn test_deserialize_program_message() {
        let message: Message = serde_json::from_str(PROGRAM_FIXTURE).unwrap();

        assert_eq!(
            message.sender,
            Address::from("0x9C2FD74F9CA2B7C4941690316B0Ebc35ce55c885".to_string())
        );
        assert_eq!(message.chain, Chain::Ethereum);
        assert_eq!(
            message.signature,
            Signature::from(
                "0x421c656709851fba752f323a117bc7a07f175a4dd7faf1d8fc1cd9a99028081a6419f9e8b0a7cd454bfef1c52d1f0675a7a59a7d07eb4ebdb22e18bbaf415f881c".to_string()
            )
        );
        assert_matches!(message.message_type, MessageType::Program);
        assert_matches!(
            message.content_source,
            ContentSource::Inline { item_content: _ }
        );
        assert_eq!(
            &message.item_hash.to_string(),
            "acab01087137c68a5e84734e75145482651accf3bea80fb9b723b761639ecc1c"
        );
        assert_eq!(message.time, Timestamp::from(1757026128.773));
        assert_eq!(message.channel, Some(channel!("ALEPH-CLOUDSOLUTIONS")));

        // Check content fields
        assert_eq!(
            &message.content.address,
            &Address::from("0x9C2FD74F9CA2B7C4941690316B0Ebc35ce55c885".to_string())
        );
        assert_eq!(&message.content.time, &Timestamp::from(1757026128.773));

        // Check program content fields
        let program_content = match message.content() {
            MessageContentEnum::Program(content) => content,
            other => {
                panic!("Expected MessageContentEnum::Program, got {:?}", other);
            }
        };

        assert!(!program_content.base.allow_amend);
        assert_eq!(
            program_content.base.metadata,
            Some(HashMap::from([(
                "name".to_string(),
                serde_json::Value::String("Hoymiles".to_string())
            )]))
        );
        assert_eq!(program_content.base.variables, Some(HashMap::new()));
        assert_eq!(
            program_content.base.resources,
            MachineResources {
                vcpus: 2,
                memory: MiB::from_units(4096),
                seconds: 30,
                published_ports: None,
            }
        );
        assert_matches!(program_content.base.authorized_keys, None);
        assert_eq!(
            program_content.environment,
            FunctionEnvironment {
                reproducible: false,
                internet: true,
                aleph_api: true,
                shared_cache: false,
            }
        );
        assert_eq!(
            program_content.base.volumes,
            vec![MachineVolume::Immutable(ImmutableVolume {
                base: BaseVolume {
                    comment: None,
                    mount: Some(PathBuf::from("/opt/packages"))
                },
                reference: Some(item_hash!(
                    "8df728d560ed6e9103b040a6b5fc5417e0a52e890c12977464ebadf9becf1bf6"
                )),
                use_latest: true,
            })]
        );
        assert_eq!(program_content.base.replaces, None);
        assert_eq!(
            program_content.code,
            CodeContent {
                encoding: Encoding::Zip,
                entrypoint: "main:app".to_string(),
                reference: item_hash!(
                    "9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e"
                ),
                interface: None,
                args: None,
                use_latest: true,
            }
        );
        assert_eq!(
            program_content.runtime,
            FunctionRuntime {
                reference: item_hash!(
                    "63f07193e6ee9d207b7d1fcf8286f9aee34e6f12f101d2ec77c1229f92964696"
                ),
                use_latest: true,
                comment: "Aleph Alpine Linux with Python 3.12".to_string(),
            }
        );
        assert_eq!(program_content.data, None);
        assert_eq!(program_content.export, None);
        assert_eq!(
            program_content.on,
            FunctionTriggers {
                http: true,
                persistent: Some(false)
            }
        );

        // No confirmation on this fixture
        assert!(!message.confirmed());
        assert!(message.confirmations.is_empty());
    }

    #[test]
    /// Some nodes return old PROGRAM messages where the metadata field is an empty list instead of
    /// an object. While this should never happen, fixing this server-side is tricky so we support
    /// it in the SDK by treating it like an empty map.
    fn load_program_with_empty_array_as_metadata() {
        let message: Message = serde_json::from_str(PROGRAM_WITH_EMPTY_ARRAY_AS_METADATA).unwrap();

        // Check that the metadata field is empty
        let program_content = match message.content() {
            MessageContentEnum::Program(content) => content,
            other => {
                panic!("Expected MessageContentEnum::Program, got {:?}", other);
            }
        };

        assert_matches!(program_content.base.metadata, Some(ref map) if map.is_empty());
    }
}
