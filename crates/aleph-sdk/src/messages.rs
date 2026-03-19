use aleph_types::account::{Account, SignError};
use aleph_types::chain::{Address, Chain};
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::base::{Encoding, ExecutableContent, Interface, Payment};
use aleph_types::message::execution::environment::{
    FunctionEnvironment, FunctionTriggers, HostRequirements, Hypervisor, InstanceEnvironment,
    MachineResources, PublishedPort, TrustedExecutionEnvironment,
};
use aleph_types::message::execution::volume::{
    MachineVolume, ParentVolume, PersistentVolumeSize, RootfsVolume, VolumePersistence,
};
use aleph_types::message::pending::PendingMessage;
use aleph_types::message::{
    AggregateContent, AggregateKey, Authorization, CodeContent, DataContent, Export, ForgetContent,
    FunctionRuntime, InstanceContent, MessageType, PostContent, PostType, ProgramContent,
};
use aleph_types::message::{RawFileRef, StorageBackend, StorageEngine, StoreContent};
use memsizes::MiB;
use serde::Serialize;
use std::collections::HashMap;
use thiserror::Error;

use crate::builder::MessageBuilder;

#[derive(Debug, Error)]
pub enum MessageBuildError {
    #[error("content serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("message signing failed: {0}")]
    Signing(#[from] SignError),
    #[error("forget message must target at least one hash or aggregate")]
    EmptyForget,
    #[error("storage engine mismatch: engine {engine:?} does not match hash type '{hash}'")]
    StorageEngineMismatch { engine: StorageEngine, hash: String },
    #[error("invalid authorization: {0}")]
    InvalidAuthorization(String),
}

pub struct PostBuilder<'a, A: Account> {
    account: &'a A,
    post_type: PostType,
    content: serde_json::Value,
    reference: Option<String>,
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
            reference: None,
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
            reference: None,
            channel: None,
        })
    }

    /// Set a reference hash on the post. This is independent of amend semantics —
    /// it sets the `ref` field in the POST content, used e.g. by corechannel operations
    /// to point at a target node.
    pub fn reference(mut self, reference: impl Into<String>) -> Self {
        self.reference = Some(reference.into());
        self
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
        let mut value = serde_json::to_value(post_content)?;
        if let Some(reference) = self.reference {
            value
                .as_object_mut()
                .expect("PostContent serializes to an object")
                .insert("ref".to_string(), serde_json::Value::String(reference));
        }
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
        if self.hashes.is_empty() && self.aggregates.is_empty() {
            return Err(MessageBuildError::EmptyForget);
        }
        let forget_content = ForgetContent::new(self.hashes, self.aggregates, self.reason);
        let value = serde_json::to_value(forget_content)?;
        let mut builder = MessageBuilder::new(self.account, MessageType::Forget, value);
        if let Some(channel) = self.channel {
            builder = builder.channel(channel);
        }
        Ok(builder.build()?)
    }
}

pub struct StoreBuilder<'a, A: Account> {
    account: &'a A,
    file_hash: ItemHash,
    storage_engine: StorageEngine,
    reference: Option<RawFileRef>,
    metadata: Option<HashMap<String, serde_json::Value>>,
    channel: Option<Channel>,
}

impl<'a, A: Account> StoreBuilder<'a, A> {
    /// Create a new StoreBuilder for a file that has already been uploaded.
    pub fn new(account: &'a A, file_hash: ItemHash, storage_engine: StorageEngine) -> Self {
        Self {
            account,
            file_hash,
            storage_engine,
            reference: None,
            metadata: None,
            channel: None,
        }
    }

    /// Set a user-defined file reference string.
    pub fn reference(mut self, reference: impl Into<String>) -> Self {
        self.reference = Some(RawFileRef::UserDefined(reference.into()));
        self
    }

    /// Set a file reference from an item hash.
    pub fn reference_hash(mut self, hash: ItemHash) -> Self {
        self.reference = Some(RawFileRef::ItemHash(hash));
        self
    }

    /// Set metadata key-value pairs.
    pub fn metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Set the message channel.
    pub fn channel(mut self, channel: Channel) -> Self {
        self.channel = Some(channel);
        self
    }

    /// Build and sign the STORE message.
    pub fn build(self) -> Result<PendingMessage, MessageBuildError> {
        let backend = match (self.storage_engine, self.file_hash) {
            (StorageEngine::Storage, ItemHash::Native(h)) => {
                StorageBackend::Storage { item_hash: h }
            }
            (StorageEngine::Ipfs, ItemHash::Ipfs(cid)) => StorageBackend::Ipfs { item_hash: cid },
            (engine, hash) => {
                return Err(MessageBuildError::StorageEngineMismatch {
                    engine,
                    hash: hash.to_string(),
                });
            }
        };

        let store_content = StoreContent::new(backend, self.reference, self.metadata);
        let value = serde_json::to_value(store_content)?;

        let mut builder = MessageBuilder::new(self.account, MessageType::Store, value);
        if let Some(channel) = self.channel {
            builder = builder.channel(channel);
        }
        Ok(builder.build()?)
    }
}

pub struct ProgramBuilder<'a, A: Account> {
    account: &'a A,
    // Code
    program_ref: ItemHash,
    entrypoint: String,
    encoding: Encoding,
    interface: Option<Interface>,
    use_latest_code: bool,
    // Runtime
    runtime: ItemHash,
    runtime_comment: String,
    use_latest_runtime: bool,
    // Environment
    internet: bool,
    aleph_api: bool,
    reproducible: bool,
    shared_cache: bool,
    // Triggers
    http: bool,
    persistent: Option<bool>,
    // Resources
    vcpus: u32,
    memory: MiB,
    seconds: u32,
    // Optional fields
    data: Option<DataContent>,
    export: Option<Export>,
    variables: Option<HashMap<String, String>>,
    metadata: Option<HashMap<String, serde_json::Value>>,
    volumes: Vec<MachineVolume>,
    payment: Option<Payment>,
    requirements: Option<HostRequirements>,
    authorized_keys: Option<Vec<String>>,
    published_ports: Option<Vec<PublishedPort>>,
    args: Option<Vec<String>>,
    allow_amend: bool,
    replaces: Option<ItemHash>,
    channel: Option<Channel>,
}

impl<'a, A: Account> ProgramBuilder<'a, A> {
    pub fn new(
        account: &'a A,
        program_ref: ItemHash,
        entrypoint: impl Into<String>,
        runtime: ItemHash,
    ) -> Self {
        Self {
            account,
            program_ref,
            entrypoint: entrypoint.into(),
            encoding: Encoding::Zip,
            interface: None,
            use_latest_code: true,
            runtime,
            runtime_comment: String::new(),
            use_latest_runtime: true,
            internet: true,
            aleph_api: true,
            reproducible: false,
            shared_cache: false,
            http: true,
            persistent: None,
            vcpus: 1,
            memory: MiB::from(128),
            seconds: 1,
            data: None,
            export: None,
            variables: None,
            metadata: None,
            volumes: vec![],
            payment: None,
            requirements: None,
            authorized_keys: None,
            published_ports: None,
            args: None,
            allow_amend: false,
            replaces: None,
            channel: None,
        }
    }

    pub fn encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = encoding;
        self
    }

    pub fn interface(mut self, interface: Interface) -> Self {
        self.interface = Some(interface);
        self
    }

    pub fn use_latest_code(mut self, use_latest: bool) -> Self {
        self.use_latest_code = use_latest;
        self
    }

    pub fn runtime_comment(mut self, comment: impl Into<String>) -> Self {
        self.runtime_comment = comment.into();
        self
    }

    pub fn use_latest_runtime(mut self, use_latest: bool) -> Self {
        self.use_latest_runtime = use_latest;
        self
    }

    pub fn internet(mut self, internet: bool) -> Self {
        self.internet = internet;
        self
    }

    pub fn aleph_api(mut self, aleph_api: bool) -> Self {
        self.aleph_api = aleph_api;
        self
    }

    pub fn http(mut self, http: bool) -> Self {
        self.http = http;
        self
    }

    pub fn persistent(mut self, persistent: bool) -> Self {
        self.persistent = Some(persistent);
        self
    }

    pub fn vcpus(mut self, vcpus: u32) -> Self {
        self.vcpus = vcpus;
        self
    }

    pub fn memory(mut self, memory: MiB) -> Self {
        self.memory = memory;
        self
    }

    pub fn timeout_seconds(mut self, seconds: u32) -> Self {
        self.seconds = seconds;
        self
    }

    pub fn data(mut self, data: DataContent) -> Self {
        self.data = Some(data);
        self
    }

    pub fn export(mut self, export: Export) -> Self {
        self.export = Some(export);
        self
    }

    pub fn variables(mut self, variables: HashMap<String, String>) -> Self {
        self.variables = Some(variables);
        self
    }

    pub fn metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn volumes(mut self, volumes: Vec<MachineVolume>) -> Self {
        self.volumes = volumes;
        self
    }

    pub fn payment(mut self, payment: Payment) -> Self {
        self.payment = Some(payment);
        self
    }

    pub fn requirements(mut self, requirements: HostRequirements) -> Self {
        self.requirements = Some(requirements);
        self
    }

    pub fn authorized_keys(mut self, keys: Vec<String>) -> Self {
        self.authorized_keys = Some(keys);
        self
    }

    pub fn published_ports(mut self, ports: Vec<PublishedPort>) -> Self {
        self.published_ports = Some(ports);
        self
    }

    pub fn args(mut self, args: Vec<String>) -> Self {
        self.args = Some(args);
        self
    }

    pub fn reproducible(mut self, reproducible: bool) -> Self {
        self.reproducible = reproducible;
        self
    }

    pub fn shared_cache(mut self, shared_cache: bool) -> Self {
        self.shared_cache = shared_cache;
        self
    }

    pub fn allow_amend(mut self, allow: bool) -> Self {
        self.allow_amend = allow;
        self
    }

    pub fn replaces(mut self, replaces: ItemHash) -> Self {
        self.replaces = Some(replaces);
        self
    }

    pub fn channel(mut self, channel: Channel) -> Self {
        self.channel = Some(channel);
        self
    }

    pub fn build(self) -> Result<PendingMessage, MessageBuildError> {
        let content = ProgramContent {
            base: ExecutableContent {
                allow_amend: self.allow_amend,
                metadata: self.metadata,
                variables: self.variables,
                resources: MachineResources {
                    vcpus: self.vcpus,
                    memory: self.memory,
                    seconds: self.seconds,
                    published_ports: self.published_ports,
                },
                payment: self.payment,
                requirements: self.requirements,
                volumes: self.volumes,
                replaces: self.replaces,
                authorized_keys: self.authorized_keys,
            },
            code: CodeContent {
                encoding: self.encoding,
                entrypoint: self.entrypoint,
                reference: self.program_ref,
                interface: self.interface,
                args: self.args,
                use_latest: self.use_latest_code,
            },
            runtime: FunctionRuntime {
                reference: self.runtime,
                use_latest: self.use_latest_runtime,
                comment: self.runtime_comment,
            },
            data: self.data,
            environment: FunctionEnvironment {
                reproducible: self.reproducible,
                internet: self.internet,
                aleph_api: self.aleph_api,
                shared_cache: self.shared_cache,
            },
            export: self.export,
            on: FunctionTriggers {
                http: self.http,
                persistent: self.persistent,
            },
        };
        let value = serde_json::to_value(content)?;
        let mut builder = MessageBuilder::new(self.account, MessageType::Program, value);
        if let Some(channel) = self.channel {
            builder = builder.channel(channel);
        }
        Ok(builder.build()?)
    }
}

pub struct InstanceBuilder<'a, A: Account> {
    account: &'a A,
    // Root filesystem
    rootfs: ItemHash,
    rootfs_size: PersistentVolumeSize,
    rootfs_persistence: VolumePersistence,
    use_latest_rootfs: bool,
    // Environment
    internet: bool,
    aleph_api: bool,
    hypervisor: Option<Hypervisor>,
    trusted_execution: Option<TrustedExecutionEnvironment>,
    // Resources
    vcpus: u32,
    memory: MiB,
    seconds: u32,
    // Optional fields
    variables: Option<HashMap<String, String>>,
    metadata: Option<HashMap<String, serde_json::Value>>,
    volumes: Vec<MachineVolume>,
    payment: Option<Payment>,
    requirements: Option<HostRequirements>,
    authorized_keys: Option<Vec<String>>,
    published_ports: Option<Vec<PublishedPort>>,
    allow_amend: bool,
    replaces: Option<ItemHash>,
    channel: Option<Channel>,
}

impl<'a, A: Account> InstanceBuilder<'a, A> {
    pub fn new(account: &'a A, rootfs: ItemHash, rootfs_size: PersistentVolumeSize) -> Self {
        Self {
            account,
            rootfs,
            rootfs_size,
            rootfs_persistence: VolumePersistence::Host,
            use_latest_rootfs: true,
            internet: true,
            aleph_api: true,
            hypervisor: None,
            trusted_execution: None,
            vcpus: 1,
            memory: MiB::from(128),
            seconds: 1,
            variables: None,
            metadata: None,
            volumes: vec![],
            payment: None,
            requirements: None,
            authorized_keys: None,
            published_ports: None,
            allow_amend: false,
            replaces: None,
            channel: None,
        }
    }

    pub fn rootfs_persistence(mut self, persistence: VolumePersistence) -> Self {
        self.rootfs_persistence = persistence;
        self
    }

    pub fn use_latest_rootfs(mut self, use_latest: bool) -> Self {
        self.use_latest_rootfs = use_latest;
        self
    }

    pub fn internet(mut self, internet: bool) -> Self {
        self.internet = internet;
        self
    }

    pub fn aleph_api(mut self, aleph_api: bool) -> Self {
        self.aleph_api = aleph_api;
        self
    }

    pub fn hypervisor(mut self, hypervisor: Hypervisor) -> Self {
        self.hypervisor = Some(hypervisor);
        self
    }

    pub fn trusted_execution(mut self, tee: TrustedExecutionEnvironment) -> Self {
        self.trusted_execution = Some(tee);
        self
    }

    pub fn vcpus(mut self, vcpus: u32) -> Self {
        self.vcpus = vcpus;
        self
    }

    pub fn memory(mut self, memory: MiB) -> Self {
        self.memory = memory;
        self
    }

    pub fn timeout_seconds(mut self, seconds: u32) -> Self {
        self.seconds = seconds;
        self
    }

    pub fn variables(mut self, variables: HashMap<String, String>) -> Self {
        self.variables = Some(variables);
        self
    }

    pub fn metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn volumes(mut self, volumes: Vec<MachineVolume>) -> Self {
        self.volumes = volumes;
        self
    }

    pub fn payment(mut self, payment: Payment) -> Self {
        self.payment = Some(payment);
        self
    }

    pub fn requirements(mut self, requirements: HostRequirements) -> Self {
        self.requirements = Some(requirements);
        self
    }

    pub fn ssh_keys(mut self, keys: Vec<String>) -> Self {
        self.authorized_keys = Some(keys);
        self
    }

    pub fn published_ports(mut self, ports: Vec<PublishedPort>) -> Self {
        self.published_ports = Some(ports);
        self
    }

    pub fn allow_amend(mut self, allow: bool) -> Self {
        self.allow_amend = allow;
        self
    }

    pub fn replaces(mut self, replaces: ItemHash) -> Self {
        self.replaces = Some(replaces);
        self
    }

    pub fn channel(mut self, channel: Channel) -> Self {
        self.channel = Some(channel);
        self
    }

    pub fn build(self) -> Result<PendingMessage, MessageBuildError> {
        let content = InstanceContent {
            base: ExecutableContent {
                allow_amend: self.allow_amend,
                metadata: self.metadata,
                variables: self.variables,
                resources: MachineResources {
                    vcpus: self.vcpus,
                    memory: self.memory,
                    seconds: self.seconds,
                    published_ports: self.published_ports,
                },
                payment: self.payment,
                requirements: self.requirements,
                volumes: self.volumes,
                replaces: self.replaces,
                authorized_keys: self.authorized_keys,
            },
            environment: InstanceEnvironment {
                internet: self.internet,
                aleph_api: self.aleph_api,
                hypervisor: self.hypervisor,
                trusted_execution: self.trusted_execution,
                // Legacy fields kept for retro-compatibility, always false for instances.
                reproducible: false,
                shared_cache: false,
            },
            rootfs: RootfsVolume {
                parent: ParentVolume {
                    reference: self.rootfs,
                    use_latest: self.use_latest_rootfs,
                },
                persistence: self.rootfs_persistence,
                size_mib: self.rootfs_size,
                forgotten_by: None,
            },
        };
        let value = serde_json::to_value(content)?;
        let mut builder = MessageBuilder::new(self.account, MessageType::Instance, value);
        if let Some(channel) = self.channel {
            builder = builder.channel(channel);
        }
        Ok(builder.build()?)
    }
}

pub struct AuthorizationBuilder {
    address: Address,
    chain: Option<Chain>,
    channels: Vec<String>,
    message_types: Vec<MessageType>,
    post_types: Vec<String>,
    aggregate_keys: Vec<String>,
}

impl AuthorizationBuilder {
    pub fn new(address: Address) -> Self {
        Self {
            address,
            chain: None,
            channels: Vec::new(),
            message_types: Vec::new(),
            post_types: Vec::new(),
            aggregate_keys: Vec::new(),
        }
    }

    pub fn chain(mut self, chain: Chain) -> Self {
        self.chain = Some(chain);
        self
    }

    pub fn channel(mut self, channel: String) -> Self {
        self.channels.push(channel);
        self
    }

    pub fn message_type(mut self, message_type: MessageType) -> Self {
        self.message_types.push(message_type);
        self
    }

    pub fn post_type(mut self, post_type: String) -> Self {
        self.post_types.push(post_type);
        self
    }

    pub fn aggregate_key(mut self, aggregate_key: String) -> Self {
        self.aggregate_keys.push(aggregate_key);
        self
    }

    pub fn build(self) -> Result<Authorization, MessageBuildError> {
        if !self.post_types.is_empty() && !self.message_types.contains(&MessageType::Post) {
            return Err(MessageBuildError::InvalidAuthorization(
                "post_types requires POST in message types".to_string(),
            ));
        }
        if !self.aggregate_keys.is_empty() && !self.message_types.contains(&MessageType::Aggregate)
        {
            return Err(MessageBuildError::InvalidAuthorization(
                "aggregate_keys requires AGGREGATE in message types".to_string(),
            ));
        }
        Ok(Authorization {
            address: self.address,
            chain: self.chain,
            channels: self.channels,
            types: self.message_types,
            post_types: self.post_types,
            aggregate_keys: self.aggregate_keys,
        })
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
    fn test_post_builder_with_reference() {
        let account = TestAccount::new();
        let msg = PostBuilder::new(
            &account,
            "corechan-operation",
            serde_json::json!({"action": "link", "tags": ["link", "mainnet"]}),
        )
        .unwrap()
        .reference("a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77")
        .build()
        .unwrap();

        assert_eq!(msg.message_type, MessageType::Post);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        // Has both type AND ref
        assert_eq!(parsed["type"], "corechan-operation");
        assert_eq!(
            parsed["ref"],
            "a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77"
        );
        assert_eq!(parsed["content"]["action"], "link");
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
    fn test_aggregate_builder_with_channel() {
        let account = TestAccount::new();
        let mut content = serde_json::Map::new();
        content.insert("setting".into(), serde_json::json!("value"));
        let channel = aleph_types::channel::Channel::from("TEST".to_string());

        let msg = AggregateBuilder::new(&account, "my_settings", content)
            .channel(channel.clone())
            .build()
            .unwrap();

        assert_eq!(msg.channel, Some(channel));
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

    #[test]
    fn test_forget_builder_empty_rejects() {
        let account = TestAccount::new();
        let err = ForgetBuilder::new(&account, vec![]).build().unwrap_err();
        assert!(matches!(err, MessageBuildError::EmptyForget));
    }

    #[test]
    fn test_store_builder_storage() {
        use aleph_types::message::{StorageEngine, StoreContent};

        let account = TestAccount::new();
        let file_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let msg = StoreBuilder::new(&account, file_hash, StorageEngine::Storage)
            .build()
            .unwrap();

        assert_eq!(msg.message_type, MessageType::Store);
        assert_eq!(msg.item_type, ItemType::Inline);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["item_type"], "storage");
        assert_eq!(
            parsed["item_hash"],
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        assert_eq!(
            parsed["address"],
            "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"
        );
        assert!(parsed["time"].is_number());

        // Round-trip through StoreContent deserialization
        let store: StoreContent = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(
            store.file_hash().to_string(),
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
    }

    #[test]
    fn test_store_builder_ipfs() {
        use aleph_types::message::StorageEngine;

        let account = TestAccount::new();
        let file_hash = aleph_types::item_hash!("QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8");
        let msg = StoreBuilder::new(&account, file_hash, StorageEngine::Ipfs)
            .build()
            .unwrap();

        assert_eq!(msg.message_type, MessageType::Store);
        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["item_type"], "ipfs");
        assert_eq!(
            parsed["item_hash"],
            "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8"
        );
    }

    #[test]
    fn test_store_builder_with_reference() {
        use aleph_types::message::StorageEngine;

        let account = TestAccount::new();
        let file_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let msg = StoreBuilder::new(&account, file_hash, StorageEngine::Storage)
            .reference("my-custom-ref")
            .build()
            .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["ref"], "my-custom-ref");
    }

    #[test]
    fn test_store_builder_with_metadata() {
        use aleph_types::message::StorageEngine;
        use std::collections::HashMap;

        let account = TestAccount::new();
        let file_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let mut metadata = HashMap::new();
        metadata.insert("filename".to_string(), serde_json::json!("test.pdf"));
        let msg = StoreBuilder::new(&account, file_hash, StorageEngine::Storage)
            .metadata(metadata)
            .build()
            .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["metadata"]["filename"], "test.pdf");
    }

    #[test]
    fn test_store_builder_with_channel() {
        use aleph_types::message::StorageEngine;

        let account = TestAccount::new();
        let file_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let channel = aleph_types::channel::Channel::from("TEST".to_string());
        let msg = StoreBuilder::new(&account, file_hash, StorageEngine::Storage)
            .channel(channel.clone())
            .build()
            .unwrap();

        assert_eq!(msg.channel, Some(channel));
    }

    #[test]
    fn test_store_builder_engine_mismatch() {
        use aleph_types::message::StorageEngine;

        let account = TestAccount::new();
        // Native hash with Ipfs engine should error
        let file_hash = aleph_types::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let err = StoreBuilder::new(&account, file_hash, StorageEngine::Ipfs)
            .build()
            .unwrap_err();
        assert!(matches!(
            err,
            MessageBuildError::StorageEngineMismatch { .. }
        ));
    }

    #[test]
    fn test_program_builder_defaults() {
        let account = TestAccount::new();
        let code_ref = aleph_types::item_hash!(
            "9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e"
        );
        let runtime_ref = aleph_types::item_hash!(
            "63f07193e6ee9d207b7d1fcf8286f9aee34e6f12f101d2ec77c1229f92964696"
        );

        let msg = ProgramBuilder::new(&account, code_ref, "main:app", runtime_ref)
            .build()
            .unwrap();

        assert_eq!(msg.message_type, MessageType::Program);
        assert_eq!(msg.item_type, ItemType::Inline);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(
            parsed["address"],
            "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"
        );
        // Code
        assert_eq!(parsed["code"]["entrypoint"], "main:app");
        assert_eq!(parsed["code"]["encoding"], "zip");
        assert_eq!(parsed["code"]["use_latest"], true);
        // Runtime
        assert_eq!(parsed["runtime"]["use_latest"], true);
        // Environment defaults
        assert_eq!(parsed["environment"]["internet"], true);
        assert_eq!(parsed["environment"]["aleph_api"], true);
        assert_eq!(parsed["environment"]["reproducible"], false);
        // Triggers defaults
        assert_eq!(parsed["on"]["http"], true);
        // Resources defaults
        assert_eq!(parsed["resources"]["vcpus"], 1);
        assert_eq!(parsed["resources"]["memory"], 128);
        assert_eq!(parsed["resources"]["seconds"], 1);
        // Optional fields absent
        assert_eq!(parsed["allow_amend"], false);
    }

    #[test]
    fn test_program_builder_with_options() {
        let account = TestAccount::new();
        let code_ref = aleph_types::item_hash!(
            "9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e"
        );
        let runtime_ref = aleph_types::item_hash!(
            "63f07193e6ee9d207b7d1fcf8286f9aee34e6f12f101d2ec77c1229f92964696"
        );
        let channel = Channel::from("MY_CHANNEL".to_string());

        let msg = ProgramBuilder::new(&account, code_ref, "main:app", runtime_ref)
            .encoding(aleph_types::message::execution::base::Encoding::Squashfs)
            .persistent(true)
            .internet(false)
            .vcpus(4)
            .memory(memsizes::MiB::from(2048))
            .timeout_seconds(30)
            .allow_amend(true)
            .channel(channel.clone())
            .build()
            .unwrap();

        assert_eq!(msg.message_type, MessageType::Program);
        assert_eq!(msg.channel, Some(channel));

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["code"]["encoding"], "squashfs");
        assert_eq!(parsed["on"]["persistent"], true);
        assert_eq!(parsed["environment"]["internet"], false);
        assert_eq!(parsed["resources"]["vcpus"], 4);
        assert_eq!(parsed["resources"]["memory"], 2048);
        assert_eq!(parsed["resources"]["seconds"], 30);
        assert_eq!(parsed["allow_amend"], true);
    }

    #[test]
    fn test_instance_builder_defaults() {
        let account = TestAccount::new();
        let rootfs_ref = aleph_types::item_hash!(
            "b6ff5c3a8205d1ca4c7c3369300eeafff498b558f71b851aa2114afd0a532717"
        );
        let rootfs_size = aleph_types::message::execution::volume::PersistentVolumeSize::from(
            memsizes::MiB::from(20480),
        );

        let msg = InstanceBuilder::new(&account, rootfs_ref, rootfs_size)
            .build()
            .unwrap();

        assert_eq!(msg.message_type, MessageType::Instance);
        assert_eq!(msg.item_type, ItemType::Inline);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(
            parsed["address"],
            "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"
        );
        // Rootfs
        assert_eq!(parsed["rootfs"]["parent"]["use_latest"], true);
        assert_eq!(parsed["rootfs"]["persistence"], "host");
        assert_eq!(parsed["rootfs"]["size_mib"], 20480);
        // Environment defaults
        assert_eq!(parsed["environment"]["internet"], true);
        assert_eq!(parsed["environment"]["aleph_api"], true);
        assert!(parsed["environment"]["hypervisor"].is_null());
        // Resources defaults
        assert_eq!(parsed["resources"]["vcpus"], 1);
        assert_eq!(parsed["resources"]["memory"], 128);
        assert_eq!(parsed["allow_amend"], false);
    }

    #[test]
    fn test_instance_builder_with_options() {
        let account = TestAccount::new();
        let rootfs_ref = aleph_types::item_hash!(
            "b6ff5c3a8205d1ca4c7c3369300eeafff498b558f71b851aa2114afd0a532717"
        );
        let rootfs_size = aleph_types::message::execution::volume::PersistentVolumeSize::from(
            memsizes::MiB::from(20480),
        );
        let channel = Channel::from("ALEPH-CLOUDSOLUTIONS".to_string());

        let msg = InstanceBuilder::new(&account, rootfs_ref, rootfs_size)
            .hypervisor(aleph_types::message::execution::environment::Hypervisor::Qemu)
            .internet(true)
            .vcpus(12)
            .memory(memsizes::MiB::from(73728))
            .timeout_seconds(30)
            .ssh_keys(vec!["ssh-ed25519 AAAA... user@host".to_string()])
            .channel(channel.clone())
            .build()
            .unwrap();

        assert_eq!(msg.message_type, MessageType::Instance);
        assert_eq!(msg.channel, Some(channel));

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["environment"]["hypervisor"], "qemu");
        assert_eq!(parsed["resources"]["vcpus"], 12);
        assert_eq!(parsed["resources"]["memory"], 73728);
        assert_eq!(parsed["resources"]["seconds"], 30);
        assert_eq!(
            parsed["authorized_keys"][0],
            "ssh-ed25519 AAAA... user@host"
        );
    }

    #[test]
    fn test_authorization_builder_minimal() {
        let auth = AuthorizationBuilder::new(Address::from("0xabc".to_string()))
            .build()
            .unwrap();
        assert_eq!(auth.address, Address::from("0xabc".to_string()));
        assert_eq!(auth.chain, None);
        assert!(auth.channels.is_empty());
        assert!(auth.types.is_empty());
    }

    #[test]
    fn test_authorization_builder_full() {
        let auth = AuthorizationBuilder::new(Address::from("0xabc".to_string()))
            .chain(Chain::Ethereum)
            .channel("test-channel".to_string())
            .message_type(MessageType::Post)
            .message_type(MessageType::Aggregate)
            .post_type("blog".to_string())
            .aggregate_key("profile".to_string())
            .build()
            .unwrap();
        assert_eq!(auth.chain, Some(Chain::Ethereum));
        assert_eq!(auth.channels, vec!["test-channel"]);
        assert_eq!(auth.types, vec![MessageType::Post, MessageType::Aggregate]);
        assert_eq!(auth.post_types, vec!["blog"]);
        assert_eq!(auth.aggregate_keys, vec!["profile"]);
    }

    #[test]
    fn test_authorization_builder_post_type_without_post_fails() {
        let result = AuthorizationBuilder::new(Address::from("0xabc".to_string()))
            .post_type("blog".to_string())
            .build();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("POST"), "error should mention POST: {err}");
    }

    #[test]
    fn test_authorization_builder_aggregate_key_without_aggregate_fails() {
        let result = AuthorizationBuilder::new(Address::from("0xabc".to_string()))
            .aggregate_key("profile".to_string())
            .build();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("AGGREGATE"),
            "error should mention AGGREGATE: {err}"
        );
    }
}
