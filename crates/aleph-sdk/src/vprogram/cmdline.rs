//! V-Program runtime cmdline template instantiation.
//!
//! Produces kernel boot cmdlines from templates by replacing placeholders defined
//! by aleph-vprogram-runtime/1.

use crate::vprogram::manifest::GpuRuntimeSpec;
use aleph_types::message::{ConfidentialGpuRequirement, MAX_CONFIDENTIAL_GPUS};

#[derive(Debug, thiserror::Error)]
pub enum CmdlineError {
    #[error(
        "runtime cmdline template has no {{workload_roothash}} slot: this runtime does not support workloads yet"
    )]
    NoWorkloadSlot,
    #[error(
        "message declares verified volumes but the runtime cmdline template has no {{verified_volumes}} slot"
    )]
    NoVerifiedVolumesSlot,
    #[error(
        "cmdline template contains a placeholder not defined by aleph-vprogram-runtime/1: {{{0}}}"
    )]
    UnknownPlaceholder(String),
    #[error(
        "runtime cmdline template puts {{workload_roothash}} and {{verified_volumes}} in the same token: the workload roothash would be dropped when no volumes are declared"
    )]
    SharedRoothashToken,
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
        "runtime cmdline template puts a droppable slot in a shared token: {{gpu_arch}}, {{gpu_count}}, {{gpu_models}} and {{verified_volumes}} must each sit in a token of their own"
    )]
    SharedGpuToken,
    #[error("message asks for {0} GPUs; the supported range is 1..={MAX_CONFIDENTIAL_GPUS}")]
    BadGpuCount(u8),
    #[error("runtime does not offer GPU architecture {arch:?} (offered: {offered})")]
    GpuArchNotOffered { arch: String, offered: String },
    #[error("runtime architecture {arch} lists no board for GPU model {model}")]
    GpuModelNotOffered { arch: String, model: String },
}

/// Canonical form of the message's model narrowing: sorted, de-duplicated,
/// as rendered into the measured `gpu_models=` token.
fn canonical_models(gpu: &ConfidentialGpuRequirement) -> Vec<String> {
    let mut models = gpu.models.clone().unwrap_or_default();
    models.sort();
    models.dedup();
    models
}

/// The requirement must be one the runtime can actually serve: an
/// architecture it was measured for, and a known board for every narrowed
/// model. Fails closed when the runtime declares no GPUs at all.
fn check_gpu_is_offered(
    gpu: &ConfidentialGpuRequirement,
    models: &[String],
    runtime_gpu: Option<&GpuRuntimeSpec>,
) -> Result<(), CmdlineError> {
    let arch = runtime_gpu
        .and_then(|runtime| runtime.archs.get(&gpu.arch))
        .ok_or_else(|| CmdlineError::GpuArchNotOffered {
            arch: gpu.arch.clone(),
            offered: match runtime_gpu {
                None => "none, the runtime manifest has no gpu block".to_string(),
                Some(runtime) => runtime
                    .archs
                    .keys()
                    .cloned()
                    .collect::<Vec<String>>()
                    .join(", "),
            },
        })?;
    for model in models {
        if !arch.boards.contains_key(model) {
            return Err(CmdlineError::GpuModelNotOffered {
                arch: gpu.arch.clone(),
                model: model.clone(),
            });
        }
    }
    Ok(())
}

/// Instantiate an aleph-vprogram-runtime/1 cmdline template.
///
/// Strict by design (the "no smuggled cmdline" rule): only the format-defined
/// placeholders are legal, and the whole space-delimited token carrying
/// {verified_volumes} or {gpu_models} is dropped when the message declares no
/// extra volumes, resp. no model narrowing.
///
/// `gpu` is the message's requirement and `runtime_gpu` the manifest's `gpu`
/// block; the two must agree, and a GPU runtime refuses a GPU-less workload
/// as much as the other way round.
pub fn instantiate_cmdline(
    template: &str,
    platform_roothash: &str,
    workload_roothash: &str,
    volume_roothashes: &[String],
    gpu: Option<&ConfidentialGpuRequirement>,
    runtime_gpu: Option<&GpuRuntimeSpec>,
) -> Result<String, CmdlineError> {
    if !template.contains("{workload_roothash}") {
        return Err(CmdlineError::NoWorkloadSlot);
    }
    let has_volumes_slot = template.contains("{verified_volumes}");
    if !volume_roothashes.is_empty() && !has_volumes_slot {
        return Err(CmdlineError::NoVerifiedVolumesSlot);
    }

    let models = match gpu {
        Some(gpu) => {
            // The type validates count on deserialize; a hand-built value
            // does not go through it, and a bad count must never reach the
            // measured token.
            if gpu.count == 0 || gpu.count > MAX_CONFIDENTIAL_GPUS {
                return Err(CmdlineError::BadGpuCount(gpu.count));
            }
            if !template.contains("{gpu_arch}") || !template.contains("{gpu_count}") {
                return Err(CmdlineError::NoGpuSlot);
            }
            let models = canonical_models(gpu);
            check_gpu_is_offered(gpu, &models, runtime_gpu)?;
            if !models.is_empty() && !template.contains("{gpu_models}") {
                return Err(CmdlineError::NoGpuModelsSlot);
            }
            models
        }
        None => {
            if template.contains("{gpu_arch}")
                || template.contains("{gpu_count}")
                || template.contains("{gpu_models}")
            {
                return Err(CmdlineError::GpuSlotsWithoutGpu);
            }
            Vec::new()
        }
    };

    let tokens: Vec<String> = template
        .split(' ')
        .filter(|token| !(volume_roothashes.is_empty() && token.contains("{verified_volumes}")))
        .filter(|token| !(models.is_empty() && token.contains("{gpu_models}")))
        .map(str::to_owned)
        .collect();
    // Dropping the {verified_volumes} token must never take the workload
    // roothash with it (the two slots sharing a space-delimited token).
    if !tokens.iter().any(|t| t.contains("{workload_roothash}")) {
        return Err(CmdlineError::SharedRoothashToken);
    }
    // Same rule between the two droppable slots and the GPU ones: a token
    // that goes away must carry nothing else.
    let kept = |slot: &str| tokens.iter().any(|t| t.contains(slot));
    if gpu.is_some() && !(kept("{gpu_arch}") && kept("{gpu_count}")) {
        return Err(CmdlineError::SharedGpuToken);
    }
    if !models.is_empty() && !kept("{gpu_models}") {
        return Err(CmdlineError::SharedGpuToken);
    }
    if !volume_roothashes.is_empty() && !kept("{verified_volumes}") {
        return Err(CmdlineError::SharedGpuToken);
    }

    let mut out = tokens.join(" ");
    out = out.replace("{platform_roothash}", platform_roothash);
    out = out.replace("{workload_roothash}", workload_roothash);
    out = out.replace("{verified_volumes}", &volume_roothashes.join(","));
    if let Some(gpu) = gpu {
        out = out.replace("{gpu_arch}", &gpu.arch);
        out = out.replace("{gpu_count}", &gpu.count.to_string());
        out = out.replace("{gpu_models}", &models.join(","));
    }

    if let Some(start) = out.find('{') {
        if let Some(relative_end) = out[start..].find('}') {
            let end = start + relative_end;
            return Err(CmdlineError::UnknownPlaceholder(
                out[start + 1..end].to_string(),
            ));
        } else {
            return Err(CmdlineError::UnknownPlaceholder(
                out[start + 1..].to_string(),
            ));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vprogram::manifest::test::VALID_GPU_BLOCK;

    const T: &str = "console=ttyS0 root=/dev/mapper/verity-root ro roothash={platform_roothash} workload_roothash={workload_roothash} verified_volumes={verified_volumes}";

    /// The GPU runtime template: the three GPU slots appended to `T`.
    const GT: &str = "console=ttyS0 root=/dev/mapper/verity-root ro roothash={platform_roothash} workload_roothash={workload_roothash} verified_volumes={verified_volumes} gpu_arch={gpu_arch} gpu_count={gpu_count} gpu_models={gpu_models}";

    fn runtime_gpu() -> GpuRuntimeSpec {
        serde_json::from_str(VALID_GPU_BLOCK).expect("the manifest fixture gpu block parses")
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
    fn rejects_roothash_and_volumes_sharing_a_token_when_volumes_are_empty() {
        let t =
            "console=ttyS0 roothash={platform_roothash} w={workload_roothash}{verified_volumes}";
        let err = instantiate_cmdline(t, "aa", "bb", &[], None, None).unwrap_err();
        assert!(matches!(err, CmdlineError::SharedRoothashToken), "{err}");
        // With volumes declared the token is kept and both slots are filled.
        let out = instantiate_cmdline(t, "aa", "bb", &["h1".into()], None, None).unwrap();
        assert_eq!(out, "console=ttyS0 roothash=aa w=bbh1");
    }

    #[test]
    fn fills_all_slots() {
        let out =
            instantiate_cmdline(T, "aa", "bb", &["h1".into(), "h2".into()], None, None).unwrap();
        assert_eq!(
            out,
            "console=ttyS0 root=/dev/mapper/verity-root ro roothash=aa workload_roothash=bb verified_volumes=h1,h2"
        );
    }

    #[test]
    fn drops_verified_volumes_token_when_empty() {
        let out = instantiate_cmdline(T, "aa", "bb", &[], None, None).unwrap();
        assert_eq!(
            out,
            "console=ttyS0 root=/dev/mapper/verity-root ro roothash=aa workload_roothash=bb"
        );
    }

    #[test]
    fn volumes_without_slot_is_an_error() {
        let t = "roothash={platform_roothash} workload_roothash={workload_roothash}";
        assert!(matches!(
            instantiate_cmdline(t, "aa", "bb", &["h1".into()], None, None).unwrap_err(),
            CmdlineError::NoVerifiedVolumesSlot
        ));
        // no volumes, no slot: fine
        assert!(instantiate_cmdline(t, "aa", "bb", &[], None, None).is_ok());
    }

    #[test]
    fn missing_workload_slot_is_an_error() {
        let t = "roothash={platform_roothash}";
        assert!(matches!(
            instantiate_cmdline(t, "aa", "bb", &[], None, None).unwrap_err(),
            CmdlineError::NoWorkloadSlot
        ));
    }

    #[test]
    fn unknown_placeholder_is_an_error() {
        let t = "roothash={platform_roothash} workload_roothash={workload_roothash} ip={guest_ip}";
        match instantiate_cmdline(t, "aa", "bb", &[], None, None).unwrap_err() {
            CmdlineError::UnknownPlaceholder(p) => assert_eq!(p, "guest_ip"),
            other => panic!("unexpected: {other}"),
        }
    }

    #[test]
    fn stray_brace_before_unknown_placeholder() {
        let t = "workload_roothash={workload_roothash} label}=x ip={guest_ip}";
        match instantiate_cmdline(t, "aa", "bb", &[], None, None).unwrap_err() {
            CmdlineError::UnknownPlaceholder(p) => assert_eq!(p, "guest_ip"),
            other => panic!("unexpected: {other}"),
        }
    }

    #[test]
    fn fills_the_gpu_tokens() {
        let runtime = runtime_gpu();
        let head = "console=ttyS0 root=/dev/mapper/verity-root ro roothash=aa workload_roothash=bb";

        // One Hopper card, no narrowing: the gpu_models token goes away.
        let out = instantiate_cmdline(
            GT,
            "aa",
            "bb",
            &[],
            Some(&gpu_req("hopper", 1, &[])),
            Some(&runtime),
        )
        .unwrap();
        assert_eq!(out, format!("{head} gpu_arch=hopper gpu_count=1"));
        assert!(!out.contains("gpu_models"));

        // Four Blackwell cards narrowed to one model.
        let out = instantiate_cmdline(
            GT,
            "aa",
            "bb",
            &[],
            Some(&gpu_req("blackwell", 4, &["10de:2bb5"])),
            Some(&runtime),
        )
        .unwrap();
        assert_eq!(
            out,
            format!("{head} gpu_arch=blackwell gpu_count=4 gpu_models=10de:2bb5")
        );

        // Model ids are sorted into the token, whatever order the message lists them in.
        let out = instantiate_cmdline(
            GT,
            "aa",
            "bb",
            &[],
            Some(&gpu_req(
                "hopper",
                2,
                &["10de:2335", "10de:233b", "10de:2331"],
            )),
            Some(&runtime),
        )
        .unwrap();
        assert_eq!(
            out,
            format!("{head} gpu_arch=hopper gpu_count=2 gpu_models=10de:2331,10de:2335,10de:233b")
        );
    }

    #[test]
    fn gpu_tokens_coexist_with_verified_volumes() {
        let out = instantiate_cmdline(
            GT,
            "aa",
            "bb",
            &["h1".into()],
            Some(&gpu_req("hopper", 1, &[])),
            Some(&runtime_gpu()),
        )
        .unwrap();
        assert_eq!(
            out,
            "console=ttyS0 root=/dev/mapper/verity-root ro roothash=aa workload_roothash=bb verified_volumes=h1 gpu_arch=hopper gpu_count=1"
        );
    }

    #[test]
    fn gpu_without_slots_is_an_error() {
        let err = instantiate_cmdline(
            T,
            "aa",
            "bb",
            &[],
            Some(&gpu_req("hopper", 1, &[])),
            Some(&runtime_gpu()),
        )
        .unwrap_err();
        assert!(matches!(err, CmdlineError::NoGpuSlot), "{err}");
        // A template with only one of the two slots is just as unusable.
        let half = "workload_roothash={workload_roothash} gpu_arch={gpu_arch}";
        let err = instantiate_cmdline(
            half,
            "aa",
            "bb",
            &[],
            Some(&gpu_req("hopper", 1, &[])),
            Some(&runtime_gpu()),
        )
        .unwrap_err();
        assert!(matches!(err, CmdlineError::NoGpuSlot), "{err}");
    }

    #[test]
    fn gpu_models_without_slot_is_an_error() {
        let t = "workload_roothash={workload_roothash} gpu_arch={gpu_arch} gpu_count={gpu_count}";
        let err = instantiate_cmdline(
            t,
            "aa",
            "bb",
            &[],
            Some(&gpu_req("hopper", 1, &["10de:2331"])),
            Some(&runtime_gpu()),
        )
        .unwrap_err();
        assert!(matches!(err, CmdlineError::NoGpuModelsSlot), "{err}");
        // Without narrowing the same template is fine.
        assert!(
            instantiate_cmdline(
                t,
                "aa",
                "bb",
                &[],
                Some(&gpu_req("hopper", 1, &[])),
                Some(&runtime_gpu()),
            )
            .is_ok()
        );
    }

    #[test]
    fn gpu_slots_without_a_gpu_requirement_are_an_error() {
        for t in [
            GT,
            "workload_roothash={workload_roothash} gpu_arch={gpu_arch}",
            "workload_roothash={workload_roothash} gpu_count={gpu_count}",
            "workload_roothash={workload_roothash} gpu_models={gpu_models}",
        ] {
            let err = instantiate_cmdline(t, "aa", "bb", &[], None, None).unwrap_err();
            assert!(
                matches!(err, CmdlineError::GpuSlotsWithoutGpu),
                "{t}: {err}"
            );
        }
    }

    #[test]
    fn rejects_gpu_slots_glued_into_a_droppable_token() {
        // gpu_arch riding along in the token dropped for want of volumes.
        let t = "workload_roothash={workload_roothash} v={verified_volumes}{gpu_arch} gpu_count={gpu_count}";
        let err = instantiate_cmdline(
            t,
            "aa",
            "bb",
            &[],
            Some(&gpu_req("hopper", 1, &[])),
            Some(&runtime_gpu()),
        )
        .unwrap_err();
        assert!(matches!(err, CmdlineError::SharedGpuToken), "{err}");

        // Mirror image: the volumes slot dropped with the empty gpu_models token.
        let t = "workload_roothash={workload_roothash} gpu_arch={gpu_arch} gpu_count={gpu_count} g={gpu_models}v={verified_volumes}";
        let err = instantiate_cmdline(
            t,
            "aa",
            "bb",
            &["h1".into()],
            Some(&gpu_req("hopper", 1, &[])),
            Some(&runtime_gpu()),
        )
        .unwrap_err();
        assert!(matches!(err, CmdlineError::SharedGpuToken), "{err}");

        // gpu_models glued into the token dropped for want of volumes.
        let t = "workload_roothash={workload_roothash} gpu_arch={gpu_arch} gpu_count={gpu_count} v={verified_volumes}{gpu_models}";
        let err = instantiate_cmdline(
            t,
            "aa",
            "bb",
            &[],
            Some(&gpu_req("hopper", 1, &["10de:2331"])),
            Some(&runtime_gpu()),
        )
        .unwrap_err();
        assert!(matches!(err, CmdlineError::SharedGpuToken), "{err}");
    }

    #[test]
    fn arch_the_runtime_does_not_offer_is_an_error() {
        let hopper_only: GpuRuntimeSpec = serde_json::from_value(serde_json::json!({
            "vendor": "nvidia",
            "driver_version": "595.71.05",
            "library_path": "/opt/nvidia/lib",
            "archs": {"hopper": {"accepted_models": ["GH100 A01 GSP BROM"]}},
        }))
        .unwrap();
        let err = instantiate_cmdline(
            GT,
            "aa",
            "bb",
            &[],
            Some(&gpu_req("blackwell", 1, &[])),
            Some(&hopper_only),
        )
        .unwrap_err();
        match err {
            CmdlineError::GpuArchNotOffered { arch, offered } => {
                assert_eq!(arch, "blackwell");
                assert_eq!(offered, "hopper");
            }
            other => panic!("unexpected: {other}"),
        }
        // A runtime with no gpu block at all cannot serve any GPU either.
        let err = instantiate_cmdline(GT, "aa", "bb", &[], Some(&gpu_req("hopper", 1, &[])), None)
            .unwrap_err();
        assert!(
            matches!(err, CmdlineError::GpuArchNotOffered { .. }),
            "{err}"
        );
    }

    #[test]
    fn model_without_a_board_entry_is_an_error() {
        let err = instantiate_cmdline(
            GT,
            "aa",
            "bb",
            &[],
            Some(&gpu_req("hopper", 1, &["10de:ffff"])),
            Some(&runtime_gpu()),
        )
        .unwrap_err();
        match err {
            CmdlineError::GpuModelNotOffered { arch, model } => {
                assert_eq!(arch, "hopper");
                assert_eq!(model, "10de:ffff");
            }
            other => panic!("unexpected: {other}"),
        }
        // Hopper boards are not Blackwell boards.
        let err = instantiate_cmdline(
            GT,
            "aa",
            "bb",
            &[],
            Some(&gpu_req("blackwell", 1, &["10de:2331"])),
            Some(&runtime_gpu()),
        )
        .unwrap_err();
        assert!(
            matches!(err, CmdlineError::GpuModelNotOffered { .. }),
            "{err}"
        );
    }

    #[test]
    fn a_count_outside_the_supported_range_is_an_error() {
        // Hand-built: the message type's deserialize path rejects these.
        for count in [0u8, 9] {
            let gpu = ConfidentialGpuRequirement {
                vendor: "nvidia".into(),
                arch: "hopper".into(),
                count,
                models: None,
                mode: "cc".into(),
            };
            let err = instantiate_cmdline(GT, "aa", "bb", &[], Some(&gpu), Some(&runtime_gpu()))
                .unwrap_err();
            assert!(
                matches!(err, CmdlineError::BadGpuCount(c) if c == count),
                "{err}"
            );
        }
    }
}
