use crate::message::execution::base::{ExecutableContent, PaymentType};
use crate::message::execution::environment::InstanceEnvironment;
use crate::message::execution::volume::RootfsVolume;
use serde::{Deserialize, Serialize};

#[derive(thiserror::Error, Debug)]
pub enum InstanceContentError {
    #[error(
        "SEV-SNP confidential instances are credit-only: holder-tier and PAYG stream \
         payments are not supported"
    )]
    SnpCreditOnly,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawInstanceContent")]
pub struct InstanceContent {
    #[serde(flatten)]
    pub base: ExecutableContent,
    /// Properties of the instance execution environment.
    pub environment: InstanceEnvironment,
    /// Root filesystem for the instance.
    pub rootfs: RootfsVolume,
}

#[derive(Deserialize)]
struct RawInstanceContent {
    #[serde(flatten)]
    base: ExecutableContent,
    environment: InstanceEnvironment,
    rootfs: RootfsVolume,
}

impl TryFrom<RawInstanceContent> for InstanceContent {
    type Error = InstanceContentError;

    fn try_from(raw: RawInstanceContent) -> Result<Self, Self::Error> {
        // SEV-SNP confidential instances are credit-only.
        if let Some(tee) = &raw.environment.trusted_execution
            && tee.is_snp()
        {
            let is_credit = matches!(
                &raw.base.payment,
                Some(payment) if payment.payment_type == PaymentType::Credit
            );
            if !is_credit {
                return Err(InstanceContentError::SnpCreditOnly);
            }
        }
        Ok(Self {
            base: raw.base,
            environment: raw.environment,
            rootfs: raw.rootfs,
        })
    }
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
    use crate::timestamp::Timestamp;
    use crate::{address, channel, item_hash, signature};
    use assert_matches::assert_matches;
    use memsizes::MiB;
    use std::collections::HashMap;

    const INSTANCE_PAYG_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/instance/instance-gpu-payg.json"
    ));

    #[test]
    fn test_deserialize_instance_message() {
        let message: Message = serde_json::from_str(INSTANCE_PAYG_FIXTURE).unwrap();

        assert_eq!(
            message.sender,
            address!("0x238224C744F4b90b4494516e074D2676ECfC6803")
        );
        assert_eq!(message.chain, Chain::Ethereum);
        assert_eq!(
            message.signature,
            Some(signature!(
                "0x4f7250efd67d989ac3067358ee657e301cd437fd0b4acb38342402e60125a0a209818f555386cd17b82080aba0a7a5cd83e4e1d8875c58b38f2e64cbe5dd308f1c"
            ))
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
        assert_eq!(message.time, Timestamp::from(1762349117.833245));
        assert_eq!(message.channel, Some(channel!("ALEPH-CLOUDSOLUTIONS")));

        // Check content fields
        assert_eq!(
            &message.content.address,
            &address!("0x238224C744F4b90b4494516e074D2676ECfC6803")
        );
        assert_eq!(&message.content.time, &Timestamp::from(1762349117.833176));
        assert_eq!(message.sent_at(), &message.content.time);

        // Check instance content fields
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
                memory: MiB::from(73728),
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
                size_mib: MiB::from(737280).into(),
                forgotten_by: None,
            }
        );

        assert!(message.confirmed());
        assert_eq!(
            message.confirmed_at(),
            message.confirmations[0].time.as_ref()
        );
        assert_eq!(
            message.confirmations,
            vec![MessageConfirmation {
                chain: Chain::Ethereum,
                height: 23733404,
                hash: "0xda1dd1676b5f08cef019172a7b31de303c86aafe8cb209916cf5ffa2bc5871dc"
                    .to_string(),
                time: Some(Timestamp::from(1762349117.833245)),
                publisher: Some(address!("0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC")),
            }]
        );

        message.verify_item_hash().unwrap();
    }

    const SNP_DIGEST: &str = "abababababababababababababababababababababababababababababababababababababababababababababababab";

    fn snp_instance_json(payment: &str) -> String {
        format!(
            r#"{{
                "address": "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
                "time": 1719502000.0,
                "allow_amend": false,
                "payment": {payment},
                "environment": {{
                    "internet": true,
                    "aleph_api": false,
                    "hypervisor": "qemu",
                    "trusted_execution": {{
                        "mode": "sev_snp",
                        "policy": 196608,
                        "runtime": "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
                        "measurements": [{{"platform": "sev_snp", "digest": "{SNP_DIGEST}"}}]
                    }}
                }},
                "resources": {{"vcpus": 2, "memory": 2048, "seconds": 30}},
                "rootfs": {{
                    "parent": {{"ref": "b6ff5c3a8205d1ca4c7c3369300eeafff498b558f71b851aa2114afd0a532717", "use_latest": false}},
                    "persistence": "host",
                    "size_mib": 4096
                }},
                "volumes": []
            }}"#
        )
    }

    #[test]
    fn test_snp_instance_requires_credit_payment() {
        let content: InstanceContent =
            serde_json::from_str(&snp_instance_json(r#"{"type": "credit"}"#)).unwrap();
        assert!(
            content
                .environment
                .trusted_execution
                .as_ref()
                .unwrap()
                .is_snp()
        );

        for payment in [
            r#"{"type": "hold"}"#,
            r#"{"type": "superfluid", "chain": "AVAX"}"#,
        ] {
            assert!(
                serde_json::from_str::<InstanceContent>(&snp_instance_json(payment)).is_err(),
                "{payment} must be rejected for SNP instances"
            );
        }
    }

    #[test]
    fn test_legacy_sev_instance_payment_unrestricted() {
        let json = r#"{
            "address": "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
            "time": 1719502000.0,
            "allow_amend": false,
            "payment": {"type": "hold"},
            "environment": {
                "internet": true,
                "aleph_api": false,
                "hypervisor": "qemu",
                "trusted_execution": {
                    "policy": 1,
                    "firmware": "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe"
                }
            },
            "resources": {"vcpus": 2, "memory": 2048, "seconds": 30},
            "rootfs": {
                "parent": {"ref": "b6ff5c3a8205d1ca4c7c3369300eeafff498b558f71b851aa2114afd0a532717", "use_latest": false},
                "persistence": "host",
                "size_mib": 4096
            },
            "volumes": []
        }"#;
        let content: InstanceContent = serde_json::from_str(json).unwrap();
        assert!(
            !content
                .environment
                .trusted_execution
                .as_ref()
                .unwrap()
                .is_snp()
        );
    }
}
