//! SNP instance runtime cmdline template instantiation.
//!
//! Replaces {owner} and, when set, the same gpu_arch/gpu_count/gpu_models
//! slots `vprogram::cmdline` fills, sharing its offer check and canonicalization.

use crate::vprogram::cmdline::{GpuOfferError, canonical_models, check_gpu_is_offered};
use crate::vprogram::manifest::GpuArchSpec;
use aleph_types::message::{ConfidentialGpuRequirement, MAX_CONFIDENTIAL_GPUS};
use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error)]
pub enum InstanceCmdlineError {
    #[error(
        "instance runtime v1 requires an EVM owner address (0x followed by 40 hex digits), got {0:?}"
    )]
    NotEvmOwner(String),
    #[error("runtime cmdline template has no {{owner}} slot")]
    NoOwnerSlot,
    #[error(
        "cmdline template contains a placeholder not defined by aleph-instance-runtime/1: {{{0}}}"
    )]
    UnresolvedPlaceholder(String),
    #[error(
        "message declares confidential GPUs but the runtime cmdline template has no {{gpu_arch}}/{{gpu_count}} slots: this runtime does not run GPU workloads"
    )]
    NoGpuSlot,
    #[error(
        "message narrows the GPU requirement to specific models but the runtime cmdline template has no {{gpu_models}} slot"
    )]
    NoGpuModelsSlot,
    #[error(
        "runtime cmdline template has GPU slots but the message declares no GPUs: a GPU runtime only runs GPU workloads"
    )]
    GpuSlotsWithoutGpu,
    #[error(
        "runtime cmdline template puts a droppable slot in a shared token: {{owner}}, {{gpu_arch}}, {{gpu_count}} and {{gpu_models}} must each sit in a token of their own"
    )]
    SharedGpuToken,
    #[error("message asks for {0} GPUs; the supported range is 1..={MAX_CONFIDENTIAL_GPUS}")]
    BadGpuCount(u8),
    #[error("runtime does not offer GPU architecture {arch:?} (offered: {offered})")]
    GpuArchNotOffered { arch: String, offered: String },
    #[error("runtime architecture {arch} lists no board for GPU model {model}")]
    GpuModelNotOffered { arch: String, model: String },
}

/// Normalize and validate an EVM owner address.
///
/// Lowercases the address and validates that it is a valid EVM address
/// (0x prefix followed by exactly 40 hexadecimal digits).
pub fn normalize_evm_owner(owner: &str) -> Result<String, InstanceCmdlineError> {
    let lowercased = owner.to_lowercase();

    // Check for 0x prefix and total length of 42 chars (0x + 40 hex)
    if !lowercased.starts_with("0x") || lowercased.len() != 42 {
        return Err(InstanceCmdlineError::NotEvmOwner(owner.to_string()));
    }

    // Check that remaining 40 chars are all hex digits
    let hex_part = &lowercased[2..];
    if !hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(InstanceCmdlineError::NotEvmOwner(owner.to_string()));
    }

    Ok(lowercased)
}

/// Instantiate a SNP instance runtime cmdline template.
///
/// Replaces {owner}, plus gpu_arch/gpu_count/gpu_models when `gpu` is set,
/// using the same rules as `vprogram::cmdline::instantiate_cmdline`.
pub fn instantiate_instance_cmdline(
    template: &str,
    owner: &str,
    gpu: Option<&ConfidentialGpuRequirement>,
    runtime_archs: Option<&BTreeMap<String, GpuArchSpec>>,
) -> Result<String, InstanceCmdlineError> {
    // Validate that the template contains the {owner} slot
    if !template.contains("{owner}") {
        return Err(InstanceCmdlineError::NoOwnerSlot);
    }

    // Normalize and validate the owner address
    let normalized_owner = normalize_evm_owner(owner)?;

    let models = match gpu {
        Some(gpu) => {
            // The message type validates count on deserialize; a hand-built
            // value does not go through it, and a bad count must never reach
            // the measured token.
            if gpu.count == 0 || gpu.count > MAX_CONFIDENTIAL_GPUS {
                return Err(InstanceCmdlineError::BadGpuCount(gpu.count));
            }
            if !template.contains("{gpu_arch}") || !template.contains("{gpu_count}") {
                return Err(InstanceCmdlineError::NoGpuSlot);
            }
            let models = canonical_models(gpu);
            check_gpu_is_offered(gpu, &models, runtime_archs).map_err(|e| match e {
                GpuOfferError::ArchNotOffered { arch, offered } => {
                    InstanceCmdlineError::GpuArchNotOffered { arch, offered }
                }
                GpuOfferError::ModelNotOffered { arch, model } => {
                    InstanceCmdlineError::GpuModelNotOffered { arch, model }
                }
            })?;
            if !models.is_empty() && !template.contains("{gpu_models}") {
                return Err(InstanceCmdlineError::NoGpuModelsSlot);
            }
            models
        }
        None => {
            if template.contains("{gpu_arch}")
                || template.contains("{gpu_count}")
                || template.contains("{gpu_models}")
            {
                return Err(InstanceCmdlineError::GpuSlotsWithoutGpu);
            }
            Vec::new()
        }
    };

    let tokens: Vec<String> = template
        .split(' ')
        .filter(|token| !(models.is_empty() && token.contains("{gpu_models}")))
        .map(str::to_owned)
        .collect();
    // Dropping the {gpu_models} token must never take {owner} with it.
    if !tokens.iter().any(|t| t.contains("{owner}")) {
        return Err(InstanceCmdlineError::SharedGpuToken);
    }
    let kept = |slot: &str| tokens.iter().any(|t| t.contains(slot));
    if gpu.is_some() && !(kept("{gpu_arch}") && kept("{gpu_count}")) {
        return Err(InstanceCmdlineError::SharedGpuToken);
    }
    if !models.is_empty() && !kept("{gpu_models}") {
        return Err(InstanceCmdlineError::SharedGpuToken);
    }

    let mut out = tokens.join(" ");
    out = out.replace("{owner}", &normalized_owner);
    if let Some(gpu) = gpu {
        out = out.replace("{gpu_arch}", &gpu.arch);
        out = out.replace("{gpu_count}", &gpu.count.to_string());
        out = out.replace("{gpu_models}", &models.join(","));
    }

    // Check for any remaining placeholders: report the brace-to-brace span,
    // or everything after an unmatched `{`.
    if let Some(start) = out.find('{') {
        let end = out[start..]
            .find('}')
            .map_or(out.len(), |relative_end| start + relative_end);
        return Err(InstanceCmdlineError::UnresolvedPlaceholder(
            out[start + 1..end].to_string(),
        ));
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vprogram::manifest::GpuRuntimeSpec;
    use crate::vprogram::manifest::test::VALID_GPU_BLOCK;

    const OWNER: &str = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";

    /// The GPU-capable instance template: the three GPU slots appended to
    /// the plain owner-only template used elsewhere in this module.
    const GT: &str = "console=ttyS0 luks=1 swiotlb=262144 owner={owner} gpu_arch={gpu_arch} gpu_count={gpu_count} gpu_models={gpu_models}";

    fn runtime_archs() -> BTreeMap<String, GpuArchSpec> {
        let spec: GpuRuntimeSpec =
            serde_json::from_str(VALID_GPU_BLOCK).expect("the manifest fixture gpu block parses");
        spec.archs
    }

    fn gpu_req(arch: &str, count: u8, models: &[&str]) -> ConfidentialGpuRequirement {
        let models = (!models.is_empty()).then(|| models.to_vec());
        serde_json::from_value(serde_json::json!({
            "vendor": "nvidia",
            "arch": arch,
            "count": count,
            "models": models,
            "mode": "cc",
        }))
        .expect("a valid GPU requirement")
    }

    #[test]
    fn instantiates_and_lowercases_the_owner() {
        let out =
            instantiate_instance_cmdline("console=ttyS0 luks=1 owner={owner}", OWNER, None, None)
                .unwrap();
        assert_eq!(
            out,
            "console=ttyS0 luks=1 owner=0x9319ad3b7a8e0ee24f2e639c40d8ed124c5520ba"
        );
    }

    #[test]
    fn rejects_non_evm_owners() {
        for bad in [
            "",
            "0x123",
            "9319ad3b",
            "0xZZ19ad3b7a8e0ee24f2e639c40d8ed124c5520ba",
        ] {
            assert!(matches!(
                instantiate_instance_cmdline("owner={owner}", bad, None, None),
                Err(InstanceCmdlineError::NotEvmOwner(_))
            ));
        }
    }

    #[test]
    fn rejects_template_without_owner_slot() {
        assert!(matches!(
            instantiate_instance_cmdline("console=ttyS0 luks=1", OWNER, None, None),
            Err(InstanceCmdlineError::NoOwnerSlot)
        ));
    }

    #[test]
    fn rejects_leftover_placeholders() {
        assert!(matches!(
            instantiate_instance_cmdline("owner={owner} x={verified_volumes}", OWNER, None, None),
            Err(InstanceCmdlineError::UnresolvedPlaceholder(_))
        ));
    }

    #[test]
    fn fills_the_gpu_tokens_with_no_narrowing() {
        let out = instantiate_instance_cmdline(
            GT,
            OWNER,
            Some(&gpu_req("hopper", 1, &[])),
            Some(&runtime_archs()),
        )
        .unwrap();
        assert_eq!(
            out,
            "console=ttyS0 luks=1 swiotlb=262144 owner=0x9319ad3b7a8e0ee24f2e639c40d8ed124c5520ba gpu_arch=hopper gpu_count=1"
        );
        assert!(!out.contains("gpu_models"));
    }

    #[test]
    fn fills_the_gpu_tokens_with_sorted_deduplicated_models() {
        // Built directly: the message type's own deserialize path rejects a
        // duplicate model id, so serde_json can't reach the cmdline's dedup.
        let gpu = ConfidentialGpuRequirement {
            vendor: "nvidia".into(),
            arch: "hopper".into(),
            count: 1,
            models: Some(vec![
                "10de:233b".into(),
                "10de:2331".into(),
                "10de:233b".into(),
            ]),
            mode: "cc".into(),
        };
        let out =
            instantiate_instance_cmdline(GT, OWNER, Some(&gpu), Some(&runtime_archs())).unwrap();
        assert_eq!(
            out,
            "console=ttyS0 luks=1 swiotlb=262144 owner=0x9319ad3b7a8e0ee24f2e639c40d8ed124c5520ba gpu_arch=hopper gpu_count=1 gpu_models=10de:2331,10de:233b"
        );
    }

    #[test]
    fn gpu_on_a_template_without_gpu_slots_is_an_error() {
        let err = instantiate_instance_cmdline(
            "console=ttyS0 luks=1 owner={owner}",
            OWNER,
            Some(&gpu_req("hopper", 1, &[])),
            Some(&runtime_archs()),
        )
        .unwrap_err();
        assert!(matches!(err, InstanceCmdlineError::NoGpuSlot), "{err}");
    }

    #[test]
    fn no_gpu_on_the_gpu_template_is_an_error() {
        let err = instantiate_instance_cmdline(GT, OWNER, None, None).unwrap_err();
        assert!(
            matches!(err, InstanceCmdlineError::GpuSlotsWithoutGpu),
            "{err}"
        );
    }

    #[test]
    fn arch_the_runtime_does_not_offer_is_an_error() {
        let mut hopper_only = BTreeMap::new();
        hopper_only.insert(
            "hopper".to_string(),
            GpuArchSpec {
                accepted_models: vec!["GH100 A01 GSP BROM".to_string()],
                boards: BTreeMap::new(),
            },
        );
        let err = instantiate_instance_cmdline(
            GT,
            OWNER,
            Some(&gpu_req("blackwell", 1, &[])),
            Some(&hopper_only),
        )
        .unwrap_err();
        match err {
            InstanceCmdlineError::GpuArchNotOffered { arch, offered } => {
                assert_eq!(arch, "blackwell");
                assert_eq!(offered, "hopper");
            }
            other => panic!("unexpected: {other}"),
        }
        // A runtime with no gpu block at all cannot serve any GPU either.
        let err = instantiate_instance_cmdline(GT, OWNER, Some(&gpu_req("hopper", 1, &[])), None)
            .unwrap_err();
        assert!(
            matches!(err, InstanceCmdlineError::GpuArchNotOffered { .. }),
            "{err}"
        );
    }

    #[test]
    fn model_without_a_board_entry_is_an_error() {
        let err = instantiate_instance_cmdline(
            GT,
            OWNER,
            Some(&gpu_req("hopper", 1, &["10de:ffff"])),
            Some(&runtime_archs()),
        )
        .unwrap_err();
        match err {
            InstanceCmdlineError::GpuModelNotOffered { arch, model } => {
                assert_eq!(arch, "hopper");
                assert_eq!(model, "10de:ffff");
            }
            other => panic!("unexpected: {other}"),
        }
    }

    #[test]
    fn a_count_outside_the_supported_range_is_an_error() {
        for count in [0u8, 9] {
            let gpu = ConfidentialGpuRequirement {
                vendor: "nvidia".into(),
                arch: "hopper".into(),
                count,
                models: None,
                mode: "cc".into(),
            };
            let err = instantiate_instance_cmdline(GT, OWNER, Some(&gpu), Some(&runtime_archs()))
                .unwrap_err();
            assert!(
                matches!(err, InstanceCmdlineError::BadGpuCount(c) if c == count),
                "{err}"
            );
        }
    }
}
