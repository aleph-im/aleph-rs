//! SNP-specific `aleph instance create` assembly.
//!
//! Resolves the confidential instance runtime manifest, fetches and measures
//! its bundle, and assembles the `TrustedExecutionEnvironment` for
//! `mode: sev_snp`. Kept separate from `instance.rs` so the legacy SEV
//! create path (firmware resolution, `policy: 0x1`) stays untouched: the
//! create handler only branches on `args.tee`, it does not call into here
//! for `--tee sev`.

use std::path::Path;

use aleph_sdk::aggregate_models::vm_images::VmImagesData;
use aleph_sdk::client::{AlephClient, AlephStorageClient};
use aleph_sdk::instance_runtime::bundle::fetch_instance_bundle_artifacts;
use aleph_sdk::instance_runtime::cmdline::instantiate_instance_cmdline;
use aleph_sdk::instance_runtime::manifest::InstanceRuntimeManifest;
use aleph_sdk::vprogram::measure::compute_measurements;
use aleph_types::chain::Address;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::environment::{
    DEFAULT_SNP_POLICY, LaunchMeasurement, TeeMode, TrustedExecutionEnvironment,
};
use anyhow::{Context, Result, anyhow};

use crate::cli::ImageRef;

/// Default SEV-SNP 64-bit guest policy for confidential instances: no-debug,
/// SMT allowed, reserved bit 17 set. Distinct from the aleph-types schema's
/// wire default (`0x1`, legacy SEV NoDebug semantics), which must never be
/// sent in `sev_snp` mode.
pub(crate) const SNP_DEFAULT_POLICY: u64 = DEFAULT_SNP_POLICY;

/// Resolve `--runtime` (instance create, SNP flavor) against an in-memory
/// `VmImagesData`. Pure: does no network I/O. The flag wins when given;
/// otherwise falls back to `defaults.instance_runtime`, erroring with both
/// names when neither is available.
pub(crate) fn resolve_instance_runtime_ref(
    runtime: Option<ImageRef>,
    data: &VmImagesData,
) -> Result<ItemHash> {
    match runtime {
        Some(ImageRef::Hash(h)) => Ok(h),
        Some(ImageRef::Preset(name)) => Ok(data.instance_runtime(&name)?.hash.clone()),
        None => {
            let default_name = data.defaults.instance_runtime.as_deref().ok_or_else(|| {
                anyhow!(
                    "no instance runtime given: pass --runtime, or configure \
                     defaults.instance_runtime in the vm-images aggregate"
                )
            })?;
            Ok(data.instance_runtime(default_name)?.hash.clone())
        }
    }
}

/// The address whose lowercase form goes into the instance runtime cmdline:
/// the resolved `--on-behalf-of` owner when set, else the signer's own
/// account address. Returns the raw string; normalization (lowercasing and
/// EVM validation) happens in `instantiate_instance_cmdline`.
pub(crate) fn snp_owner(on_behalf_of: Option<&Address>, account_address: &Address) -> String {
    on_behalf_of.unwrap_or(account_address).to_string()
}

/// Assemble a `sev_snp` `TrustedExecutionEnvironment` from a launch
/// measurement list. Pure and unit-testable: no firmware (SNP runtimes carry
/// their own OVMF in the bundle), no attestation port override (the runtime
/// bundle's default applies).
pub(crate) fn tee_from_measurements(
    runtime_ref: &ItemHash,
    policy: u64,
    measurements: Vec<LaunchMeasurement>,
) -> TrustedExecutionEnvironment {
    TrustedExecutionEnvironment {
        firmware: None,
        policy,
        mode: Some(TeeMode::SevSnp),
        runtime: Some(runtime_ref.clone()),
        measurements: Some(measurements),
        attestation_port: None,
    }
}

/// Download the instance runtime manifest, fetch and cache its bundle,
/// instantiate the boot cmdline for `owner`, and compute the SEV-SNP launch
/// measurements for every CPU model the manifest declares.
///
/// Returns the assembled `TrustedExecutionEnvironment` alongside the parsed
/// manifest (the caller may want it, e.g. for its attestation descriptor).
pub(crate) async fn build_snp_trusted_execution(
    client: &AlephClient,
    runtime_ref: &ItemHash,
    owner: &str,
    vcpus: u32,
    policy: u64,
    cache_dir: &Path,
) -> Result<(TrustedExecutionEnvironment, InstanceRuntimeManifest)> {
    let manifest_bytes = client
        .download_file_by_message_hash(runtime_ref)
        .await
        .context("failed to download instance runtime manifest")?
        .with_verification()
        .bytes()
        .await
        .context("failed to download instance runtime manifest")?;
    let manifest = InstanceRuntimeManifest::parse(&manifest_bytes)
        .context("failed to parse instance runtime manifest")?;

    let artifacts = fetch_instance_bundle_artifacts(client, &manifest, cache_dir)
        .await
        .context("failed to fetch instance runtime bundle")?;

    let cmdline = instantiate_instance_cmdline(&manifest.boot.cmdline_template, owner)
        .context("failed to instantiate the instance runtime boot cmdline")?;

    let measurements = compute_measurements(&artifacts, &cmdline, vcpus, &manifest.boot.cpu_models)
        .context("failed to compute SEV-SNP launch measurements")?;

    let tee = tee_from_measurements(runtime_ref, policy, measurements);
    Ok((tee, manifest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_flag_beats_the_aggregate_default() {
        let data: VmImagesData = serde_json::from_str(&format!(
            r#"{{"instance_runtimes": {{"snp-1.0": {{"hash": "{default_hash}"}}}},
                "defaults": {{"instance_runtime": "snp-1.0"}}}}"#,
            default_hash = "aa".repeat(32),
        ))
        .unwrap();

        let flag_hash: ItemHash = "bb".repeat(32).parse().unwrap();
        let got =
            resolve_instance_runtime_ref(Some(ImageRef::Hash(flag_hash.clone())), &data).unwrap();
        assert_eq!(got, flag_hash);
    }

    #[test]
    fn missing_runtime_and_default_is_a_named_error() {
        let data: VmImagesData = serde_json::from_str("{}").unwrap();
        let err = resolve_instance_runtime_ref(None, &data)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--runtime"), "error must name the flag: {err}");
        assert!(
            err.contains("instance_runtime"),
            "error must name the aggregate default: {err}"
        );
    }

    #[test]
    fn on_behalf_of_is_the_owner() {
        let mine = Address::from("0x1111111111111111111111111111111111111a".to_string());
        let other = Address::from("0x2222222222222222222222222222222222222b".to_string());

        let got = snp_owner(Some(&other), &mine);
        assert_eq!(got, other.to_string());

        // No delegation: falls back to the signer's own address.
        let got = snp_owner(None, &mine);
        assert_eq!(got, mine.to_string());
    }

    #[test]
    fn tee_struct_shape() {
        let tee = tee_from_measurements(&"ab".repeat(32).parse().unwrap(), 0x30000, vec![]);
        assert_eq!(tee.mode, Some(TeeMode::SevSnp));
        assert_eq!(tee.policy, 0x30000);
        assert!(tee.firmware.is_none());
        assert!(tee.attestation_port.is_none());
    }
}
