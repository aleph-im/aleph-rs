use crate::message::execution::base::ExecutableContent;
use crate::message::execution::environment::InstanceEnvironment;
use crate::message::execution::volume::RootfsVolume;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceContent {
    #[serde(flatten)]
    base: ExecutableContent,
    /// Properties of the instance execution environment.
    environment: InstanceEnvironment,
    /// Root filesystem for the instance.
    rootfs: RootfsVolume,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::chain::Chain;
    use crate::message::base_message::{MessageConfirmation, MessageContentEnum};
    use crate::message::execution::base::{Payment, PaymentType};
    use crate::message::execution::environment::{
        GpuDeviceClass, GpuProperties, HostRequirements, Hypervisor, MachineResources,
        NodeRequirements,
    };
    use crate::message::execution::volume::{ParentVolume, VolumePersistence};
    use crate::message::{ContentSource, Message, MessageType};
    use crate::storage_size::{MemorySize, MiB};
    use crate::timestamp::Timestamp;
    use crate::{address, channel, item_hash, signature};
    use assert_matches::assert_matches;
    use std::collections::HashMap;

    const INSTANCE_PAYG_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/instance/instance-gpu-payg.json"
    ));

    #[test]
    fn test_deserialize_program_message() {
        let message: Message = serde_json::from_str(INSTANCE_PAYG_FIXTURE).unwrap();

        assert_eq!(
            message.sender,
            address!("0x238224C744F4b90b4494516e074D2676ECfC6803")
        );
        assert_eq!(message.chain, Chain::Ethereum);
        assert_eq!(
            message.signature,
            signature!(
                "0x4f7250efd67d989ac3067358ee657e301cd437fd0b4acb38342402e60125a0a209818f555386cd17b82080aba0a7a5cd83e4e1d8875c58b38f2e64cbe5dd308f1c"
            )
        );
        assert_matches!(message.message_type, MessageType::Instance);
        assert_matches!(
            message.content_source,
            ContentSource::Inline { item_content: _ }
        );
        assert_eq!(
            message.item_hash,
            item_hash!("a41fb91c3e68370759b72338dd1947f18e2ed883837aec5dc731d5f427f90564")
        );
        assert_eq!(
            message.time,
            Timestamp::try_from(1762349117.833245).unwrap()
        );
        assert_eq!(message.channel, Some(channel!("ALEPH-CLOUDSOLUTIONS")));

        // Check content fields
        assert_eq!(
            &message.content.address,
            &address!("0x238224C744F4b90b4494516e074D2676ECfC6803")
        );
        assert_eq!(
            &message.content.time,
            &Timestamp::try_from(1762349117.833176).unwrap()
        );

        // Check program content fields
        let instance_content = match message.content() {
            MessageContentEnum::Instance(content) => content,
            other => {
                panic!("Expected MessageContentEnum::Instance, got {:?}", other);
            }
        };

        assert!(!instance_content.base.allow_amend);
        assert_eq!(
            instance_content.base.metadata,
            Some(HashMap::from([(
                "name".to_string(),
                serde_json::Value::String("gpu-l40s-2".to_string())
            )]))
        );
        assert_eq!(instance_content.base.variables, None);
        assert_eq!(
            instance_content.base.resources,
            MachineResources {
                vcpus: 12,
                memory: MiB::from_units(73728),
                seconds: 30,
                published_ports: None,
            }
        );
        assert_eq!(
            instance_content.base.authorized_keys,
            Some(vec![
                "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIC068SD08xlTtMxTdmCe3rPVM/uA7SvgDUiQwP0FrIGS Libertai".to_string()
            ])
        );
        assert_eq!(
            instance_content.environment,
            InstanceEnvironment {
                internet: true,
                aleph_api: true,
                hypervisor: Some(Hypervisor::Qemu),
                trusted_execution: None,
                reproducible: false,
                shared_cache: false,
            }
        );

        let expected_requirements = HostRequirements {
            cpu: None,
            node: Some(NodeRequirements {
                owner: None,
                address_regex: None,
                node_hash: Some(
                    "dc3d1d194a990b5c54380c3c0439562fefa42f5a46807cba1c500ec3affecf04".to_string(),
                ),
                terms_and_conditions: None,
            }),
            gpu: Some(vec![GpuProperties {
                vendor: "NVIDIA".to_string(),
                device_name: "AD102GL [L40S]".to_string(),
                device_class: GpuDeviceClass::_3DController,
                device_id: "10de:26b9".to_string(),
            }]),
        };

        assert_eq!(
            instance_content.base.payment,
            Some(Payment {
                chain: Some(Chain::Base),
                receiver: Some(address!("0xf0c0ddf11a0dCE6618B5DF8d9fAE3D95e72E04a9")),
                payment_type: PaymentType::Superfluid,
            })
        );
        assert_eq!(
            instance_content.base.requirements,
            Some(expected_requirements)
        );
        assert!(instance_content.base.volumes.is_empty());
        assert_eq!(instance_content.base.replaces, None);
        assert_eq!(
            instance_content.rootfs,
            RootfsVolume {
                parent: ParentVolume {
                    reference: item_hash!(
                        "b6ff5c3a8205d1ca4c7c3369300eeafff498b558f71b851aa2114afd0a532717"
                    ),
                    use_latest: true
                },
                persistence: VolumePersistence::Host,
                size_mib: MiB::from_units(737280).into(),
                forgotten_by: None,
            }
        );

        assert!(message.confirmed());
        assert_eq!(
            message.confirmations,
            vec![MessageConfirmation {
                chain: Chain::Ethereum,
                height: 23733404,
                hash: "0xda1dd1676b5f08cef019172a7b31de303c86aafe8cb209916cf5ffa2bc5871dc"
                    .to_string(),
                time: None,
                publisher: None,
            }]
        );
    }
}
