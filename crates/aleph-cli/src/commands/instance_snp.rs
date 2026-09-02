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
use anyhow::{Context, Result, anyhow, bail};

use crate::cli::ImageRef;

/// Environment variable carrying the LUKS passphrase for `--encrypt-rootfs`,
/// the middle source in `read_passphrase`'s precedence (after
/// `--passphrase-file`, before the interactive prompt).
pub(crate) const LUKS_PASSPHRASE_ENV_VAR: &str = "ALEPH_LUKS_PASSPHRASE";

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

/// Source the LUKS passphrase for `--encrypt-rootfs`.
///
/// Precedence: `passphrase_file` when given (trimming exactly one trailing
/// `\n`, matching what `echo "$pass" > file` or a text editor produces),
/// then the `ALEPH_LUKS_PASSPHRASE` environment variable, then a hidden
/// interactive prompt on the controlling terminal (the same `rpassword`
/// helper `account/password.rs` uses for account passwords, a single prompt
/// rather than the double-entry new-password flow). Errors, naming all three
/// sources, when none is available (typically: no terminal attached, e.g. in
/// CI or a script).
pub(crate) fn read_passphrase(passphrase_file: Option<&Path>) -> Result<String> {
    if let Some(path) = passphrase_file {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read passphrase file {}", path.display()))?;
        let trimmed = contents.strip_suffix('\n').unwrap_or(&contents);
        return Ok(trimmed.to_string());
    }

    if let Ok(p) = std::env::var(LUKS_PASSPHRASE_ENV_VAR) {
        return Ok(p);
    }

    match rpassword::prompt_password("LUKS passphrase: ") {
        Ok(p) => Ok(p),
        Err(_) => bail!(
            "no LUKS passphrase source available: pass --passphrase-file, set the \
             {LUKS_PASSPHRASE_ENV_VAR} environment variable, or run interactively on a \
             terminal"
        ),
    }
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
    fn passphrase_file_wins_and_trims_one_newline() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("pass");
        std::fs::write(&p, "hunter2\n").unwrap();
        assert_eq!(read_passphrase(Some(&p)).unwrap(), "hunter2");
    }

    #[test]
    fn passphrase_file_trims_only_the_one_trailing_newline() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("pass");
        std::fs::write(&p, "hunter2\n\n").unwrap();
        assert_eq!(read_passphrase(Some(&p)).unwrap(), "hunter2\n");
    }

    /// Guards every test in this module that touches `LUKS_PASSPHRASE_ENV_VAR`.
    /// `cargo test` runs tests in parallel threads within one process, so
    /// without this lock one test's set_var -> read -> assert window could be
    /// clobbered by another's concurrent set_var on the same process-global
    /// variable (an intermittent, hard-to-reproduce CI flake). Held for the
    /// full body of each test below, not just around the set/remove calls, so
    /// the env var's value cannot change between one test's write and its
    /// `read_passphrase` call.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn passphrase_env_var_used_when_no_file_given() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: held under ENV_LOCK, so no other test in this module
        // observes or mutates ALEPH_LUKS_PASSPHRASE concurrently.
        unsafe {
            std::env::set_var(LUKS_PASSPHRASE_ENV_VAR, "s3cr3t-from-env");
        }
        let result = read_passphrase(None);
        unsafe {
            std::env::remove_var(LUKS_PASSPHRASE_ENV_VAR);
        }
        assert_eq!(result.unwrap(), "s3cr3t-from-env");
    }

    #[test]
    fn passphrase_file_wins_over_env_var() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: see `passphrase_env_var_used_when_no_file_given`.
        unsafe {
            std::env::set_var(LUKS_PASSPHRASE_ENV_VAR, "from-env-should-lose");
        }
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("pass");
        std::fs::write(&p, "from-file-wins\n").unwrap();
        let result = read_passphrase(Some(&p));
        unsafe {
            std::env::remove_var(LUKS_PASSPHRASE_ENV_VAR);
        }
        assert_eq!(result.unwrap(), "from-file-wins");
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
