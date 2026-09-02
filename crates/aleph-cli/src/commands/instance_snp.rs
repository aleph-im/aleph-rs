//! SNP-specific `aleph instance create` assembly.
//!
//! Resolves the confidential instance runtime manifest, fetches and measures
//! its bundle, and assembles the `TrustedExecutionEnvironment` for
//! `mode: sev_snp`. Kept separate from `instance.rs` so the legacy SEV
//! create path (firmware resolution, `policy: 0x1`) stays untouched: the
//! create handler only branches on `args.tee`, it does not call into here
//! for `--tee sev`.

use std::collections::BTreeMap;
use std::path::Path;

use aleph_sdk::aggregate_models::vm_images::VmImagesData;
use aleph_sdk::attest::owner_auth::{canonical_secrets_json, inject_secret_payload};
use aleph_sdk::attest::{
    AttestError, AttestedResponse, FreshAttestation, InjectSecretEnvelope, MeasurementPin,
    PolicyPin, fresh_attestation, post_secrets,
};
use aleph_sdk::client::{AlephClient, AlephMessageClient, AlephStorageClient, MessageWithStatus};
use aleph_sdk::crn::fetch_active_vms;
use aleph_sdk::instance_runtime::bundle::fetch_instance_bundle_artifacts;
use aleph_sdk::instance_runtime::cmdline::instantiate_instance_cmdline;
use aleph_sdk::instance_runtime::manifest::InstanceRuntimeManifest;
use aleph_sdk::vprogram::measure::compute_measurements;
use aleph_sdk::vprogram::status::resolve_attested_endpoint;
use aleph_types::account::Account;
use aleph_types::chain::Address;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::environment::{
    DEFAULT_SNP_POLICY, LaunchMeasurement, SevSnpRegisters, TeeMode, TrustedExecutionEnvironment,
};
use aleph_types::message::{InstanceContent, MessageContentEnum};
use anyhow::{Context, Result, anyhow, bail};
use url::Url;

use crate::cli::{ImageRef, InstanceAttestArgs, InstanceUnlockArgs};
use crate::commands::attest_common::{self, MeasurementExpectation};
use crate::commands::instance_target::{self, VmKind};
use crate::common::resolve_account;

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

// ---------------------------------------------------------------------
// `aleph instance attest <vm-id>`
// ---------------------------------------------------------------------

/// Fixed RA-TLS attestation transport port for SNP instance runtimes. Kept
/// as its own constant (rather than reusing `vprogram::ATTEST_PORT`) so
/// `instance attest`/`instance unlock` do not depend on the `vprogram`
/// module's private internals; both name the same runtime convention today.
pub(crate) const INSTANCE_ATTEST_PORT: u16 = 8443;

/// The result of a successful `instance attest`: the verified fresh
/// attestation evidence, the endpoint it was gathered from, the instance
/// message's content and owner (`content.address`, i.e. the message's
/// `Message::owner()`; `instance unlock` compares it against the signing
/// account before touching a secret), and which measurement(s) were pinned
/// for the call.
pub(crate) struct AttestOutcome {
    pub fresh: FreshAttestation,
    pub endpoint: Url,
    pub content: InstanceContent,
    /// The instance's owner address (`content.address`): the
    /// `--on-behalf-of` beneficiary when one was used at create, else the
    /// creator's own address. Captured from the message alongside `content`
    /// since `InstanceContent` itself carries no address field.
    pub owner: Address,
    pub expectation: MeasurementExpectation,
}

/// Reject anything that isn't a `sev_snp` confidential instance, naming the
/// actual mode (or its absence) in the error. Pure: no I/O, so directly
/// unit-testable.
pub(crate) fn check_snp_instance(
    content: &InstanceContent,
) -> Result<&TrustedExecutionEnvironment> {
    let Some(tee) = content.environment.trusted_execution.as_ref() else {
        bail!("not an SNP confidential instance (mode: none, no trusted_execution)");
    };
    match tee.mode {
        Some(TeeMode::SevSnp) => Ok(tee),
        Some(mode) => bail!("not an SNP confidential instance (mode: {})", mode.as_str()),
        None => bail!("not an SNP confidential instance (mode: sev, legacy)"),
    }
}

/// Whether `registers` satisfies `expectation`: exact equality for `Pin`,
/// set membership for `MemberOf`. Pure: no I/O.
pub(crate) fn measurement_is_expected(
    registers: &SevSnpRegisters,
    expectation: &MeasurementExpectation,
) -> bool {
    match expectation {
        MeasurementExpectation::Pin(pin) => registers == pin,
        MeasurementExpectation::MemberOf(set) => set.contains(registers),
    }
}

/// Resolve the instance's message, verify it declares `sev_snp`, discover
/// its attested endpoint, and run a fresh-nonce RA-TLS challenge against the
/// pinned measurement, guest policy, and TCB floor. Never sends a secret:
/// `instance unlock` (Task 12) builds the owner-authenticated injection on
/// top of the returned [`AttestOutcome`].
///
/// `--crn` is honored the same way `instance ssh` honors it (via
/// `instance_target::resolve_target_for`): the override picks which CRN to
/// address for both the message-placement check and the attested-endpoint
/// discovery, bypassing the scheduler's own placement record.
pub(crate) async fn run_instance_attest(
    aleph_client: &AlephClient,
    scheduler_url: &Url,
    args: &InstanceAttestArgs,
) -> Result<AttestOutcome> {
    let (item_hash, crn_url) = instance_target::resolve_target_for(
        scheduler_url,
        &args.vm_id,
        args.crn.as_deref(),
        VmKind::Instance,
    )
    .await?;

    let with_status = aleph_client
        .get_message(&item_hash)
        .await
        .with_context(|| format!("failed to fetch instance {item_hash}"))?;
    let message = match with_status {
        MessageWithStatus::Processed { message } => message,
        MessageWithStatus::Removing { message, .. } => message,
        MessageWithStatus::Removed { .. } => bail!("instance {item_hash} has been removed"),
        MessageWithStatus::Pending { .. } => {
            bail!("instance {item_hash} is still pending; try again in a few seconds")
        }
        MessageWithStatus::Forgotten { .. } => {
            bail!("instance {item_hash} has already been forgotten")
        }
        MessageWithStatus::Rejected { .. } => {
            bail!("instance {item_hash} was rejected by the network")
        }
    };
    let owner = message.owner().clone();
    let MessageContentEnum::Instance(content) = message.content().clone() else {
        bail!(
            "item {item_hash} is not an INSTANCE message (got {:?})",
            message.message_type
        );
    };

    let tee = check_snp_instance(&content)?;
    let policy = tee.policy;
    let measurements = tee.measurements.as_deref().unwrap_or(&[]);
    let expectation = attest_common::resolve_expected_measurement(
        measurements,
        args.expected_measurement.as_deref(),
    )?;

    let endpoint = match &args.url {
        Some(url) => url.clone(),
        None => {
            let http = reqwest::Client::new();
            let net = fetch_active_vms(&http, &crn_url)
                .await
                .with_context(|| format!("fetching executions from CRN {crn_url}"))?;
            net.0
                .get(&item_hash)
                .and_then(|vm| vm.networking.clone())
                .and_then(|n| resolve_attested_endpoint(&n, INSTANCE_ATTEST_PORT))
                .ok_or_else(|| {
                    anyhow!(
                        "instance {item_hash} is running on CRN {crn_url} but its attestation \
                         port ({INSTANCE_ATTEST_PORT}) is not yet mapped; try again shortly, or \
                         pass --url to bypass discovery"
                    )
                })?
        }
    };

    let measurement_pin = match &expectation {
        MeasurementExpectation::Pin(registers) => MeasurementPin::Exact(registers),
        // Fleet flow: the exact model is only known from the response, so
        // the handshake pin is explicitly deferred; the `measurement_is_expected`
        // check below is what discharges the CallerVerified obligation.
        MeasurementExpectation::MemberOf(_) => MeasurementPin::CallerVerified,
    };
    let policy_pin = PolicyPin::Exact(policy);
    let platform_policy = attest_common::platform_policy_from(&args.require_platform);

    let min_tcb = attest_common::resolve_tcb_floor(
        aleph_client,
        args.amd_product,
        args.min_tcb.as_ref(),
        args.accept_outdated_tcb,
    )
    .await?;

    let fresh = fresh_attestation(
        &endpoint,
        measurement_pin,
        policy_pin,
        args.amd_product,
        &min_tcb,
        &platform_policy,
    )
    .await
    .map_err(|e| anyhow!("attestation failed: {e}"))?;

    // Post-handshake re-check on the verified (SIGNED) measurement: for a
    // `Pin` this is belt-and-suspenders on top of the handshake pin; for a
    // `MemberOf` fleet it is the ONLY place the guest's measurement is
    // checked against the pinned set, since the handshake could not pin one
    // model ahead of time.
    if !measurement_is_expected(&fresh.registers, &expectation) {
        match &expectation {
            MeasurementExpectation::Pin(pin) => bail!(
                "measurement mismatch: guest presented {} which does not match the pinned \
                 launch measurement {}",
                fresh.registers.launch,
                pin.launch
            ),
            MeasurementExpectation::MemberOf(set) => {
                let pinned: Vec<&str> = set.iter().map(|r| r.launch.as_str()).collect();
                bail!(
                    "measurement mismatch: guest presented {} which matches none of the \
                     pinned measurements [{}]",
                    fresh.registers.launch,
                    pinned.join(", ")
                );
            }
        }
    }

    Ok(AttestOutcome {
        fresh,
        endpoint,
        content,
        owner,
        expectation,
    })
}

fn on(b: bool) -> &'static str {
    if b { "on" } else { "off" }
}

/// `component=value` rendering of a TCB, matching `vprogram call`'s style.
/// Takes the individual fields (rather than the `sev` crate's `TcbVersion`
/// directly) so this module does not need `sev` as a direct dependency.
fn tcb_summary(fmc: Option<u8>, bootloader: u8, tee: u8, snp: u8, microcode: u8) -> String {
    let mut parts = Vec::new();
    if let Some(fmc) = fmc {
        parts.push(format!("fmc={fmc}"));
    }
    parts.push(format!("bootloader={bootloader}"));
    parts.push(format!("tee={tee}"));
    parts.push(format!("snp={snp}"));
    parts.push(format!("microcode={microcode}"));
    parts.join(" ")
}

/// One-line platform posture, matching `vprogram call`'s style.
fn platform_summary(p: &aleph_sdk::attest::PlatformPosture) -> String {
    format!(
        "SMT={} TSME={} ECC={} RAPL={} ciphertext-hiding={} alias-check={} ({:#x})",
        on(p.smt_enabled),
        on(p.tsme_enabled),
        on(p.ecc_enabled),
        on(!p.rapl_disabled),
        on(p.ciphertext_hiding_enabled),
        if p.alias_check_complete { "yes" } else { "no" },
        p.raw,
    )
}

/// Print the `instance attest` result: measurement, policy, launch/reported
/// TCB, chip, platform posture, endpoint. Mirrors `vprogram call`'s
/// evidence rendering.
fn print_attest_summary(outcome: &AttestOutcome, json: bool) {
    let fresh = &outcome.fresh;
    if json {
        let out = serde_json::json!({
            "verified": true,
            "measurement": fresh.registers.launch,
            "policy": format!("{:#x}", fresh.policy),
            "launch_tcb": {
                "fmc": fresh.launch_tcb.fmc,
                "bootloader": fresh.launch_tcb.bootloader,
                "tee": fresh.launch_tcb.tee,
                "snp": fresh.launch_tcb.snp,
                "microcode": fresh.launch_tcb.microcode,
            },
            "reported_tcb": {
                "fmc": fresh.reported_tcb.fmc,
                "bootloader": fresh.reported_tcb.bootloader,
                "tee": fresh.reported_tcb.tee,
                "snp": fresh.reported_tcb.snp,
                "microcode": fresh.reported_tcb.microcode,
            },
            "cpuid": {
                "family": fresh.cpuid_family,
                "model": fresh.cpuid_model,
                "stepping": fresh.cpuid_stepping,
            },
            "platform_info": {
                "raw": format!("{:#x}", fresh.platform.raw),
                "smt_enabled": fresh.platform.smt_enabled,
                "tsme_enabled": fresh.platform.tsme_enabled,
                "ecc_enabled": fresh.platform.ecc_enabled,
                "rapl_disabled": fresh.platform.rapl_disabled,
                "ciphertext_hiding_enabled": fresh.platform.ciphertext_hiding_enabled,
                "alias_check_complete": fresh.platform.alias_check_complete,
            },
            "endpoint": outcome.endpoint.as_str(),
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&out).expect("attest summary always serializes")
        );
    } else {
        println!("Instance is genuine: verified AMD SEV-SNP attestation (fresh nonce)");
        println!("measurement: {}", fresh.registers.launch);
        println!("policy: {:#x}", fresh.policy);
        println!(
            "launch TCB: {}",
            tcb_summary(
                fresh.launch_tcb.fmc,
                fresh.launch_tcb.bootloader,
                fresh.launch_tcb.tee,
                fresh.launch_tcb.snp,
                fresh.launch_tcb.microcode,
            )
        );
        println!(
            "reported TCB: {}",
            tcb_summary(
                fresh.reported_tcb.fmc,
                fresh.reported_tcb.bootloader,
                fresh.reported_tcb.tee,
                fresh.reported_tcb.snp,
                fresh.reported_tcb.microcode,
            )
        );
        println!(
            "chip: family={} model={} stepping={}",
            fresh
                .cpuid_family
                .map_or("unknown".to_string(), |f| format!("{f:#x}")),
            fresh
                .cpuid_model
                .map_or("unknown".to_string(), |m| format!("{m:#x}")),
            fresh
                .cpuid_stepping
                .map_or("unknown".to_string(), |s| s.to_string()),
        );
        println!("platform: {}", platform_summary(&fresh.platform));
        println!("endpoint: {}", outcome.endpoint);
    }
}

/// `aleph instance attest <vm-id>`: verify the instance's RA-TLS
/// certificate chain and launch measurement, printing the evidence. Exits
/// nonzero (via the returned `Err` propagating out of `main`) on any
/// verification failure.
pub(crate) async fn handle_instance_attest(
    aleph_client: &AlephClient,
    scheduler_url: &Url,
    json: bool,
    args: &InstanceAttestArgs,
) -> Result<()> {
    let outcome = run_instance_attest(aleph_client, scheduler_url, args).await?;
    print_attest_summary(&outcome, json);
    Ok(())
}

// ---------------------------------------------------------------------
// `aleph instance unlock <vm-id>`
// ---------------------------------------------------------------------

/// Split `extra` entries on the first `=` and merge them with the LUKS
/// passphrase (keyed `luks_passphrase`) into one secrets map for injection.
///
/// Rejects: an entry with no `=` (nothing to split a key off of), an entry
/// with an empty key, and any duplicate key, including a `luks_passphrase`
/// extra colliding with `passphrase`. `instance unlock`'s handler always
/// sources the passphrase via `read_passphrase`, never via `--secret`; this
/// is what makes that the one source of truth for that key.
pub(crate) fn collect_secrets(
    passphrase: Option<String>,
    extra: &[String],
) -> Result<BTreeMap<String, String>> {
    let mut secrets = BTreeMap::new();
    if let Some(passphrase) = passphrase {
        secrets.insert("luks_passphrase".to_string(), passphrase);
    }
    for entry in extra {
        let Some((key, value)) = entry.split_once('=') else {
            bail!("invalid --secret {entry:?}: expected KEY=VALUE");
        };
        if key.is_empty() {
            bail!("invalid --secret {entry:?}: the key must not be empty");
        }
        if secrets.contains_key(key) {
            bail!("duplicate secret key \"{key}\": each key may be given only once");
        }
        secrets.insert(key.to_string(), value.to_string());
    }
    Ok(secrets)
}

/// Reject an unlock request whose signing account is not the instance
/// owner (the message's `content.address`). Case-insensitive, since EVM
/// addresses are conventionally compared without regard to checksum casing.
/// Named after the wire field in the error so a mismatch is traceable back
/// to the on-chain message.
pub(crate) fn check_owner_account(owner: &str, account_address: &Address) -> Result<()> {
    if owner.eq_ignore_ascii_case(account_address.as_str()) {
        return Ok(());
    }
    bail!(
        "signing account {account_address} does not match the instance owner {owner} \
         (content.address): only the owner may unlock this instance"
    );
}

/// Ensure `hex` carries a `0x` prefix before it goes into the signed
/// envelope. The EVM `sign_raw` path already produces one, but this is the
/// last line of defense: a missing prefix would silently mismatch the
/// agent's own EIP-191 recovery.
fn ensure_0x_prefixed(hex: &str) -> String {
    if hex.starts_with("0x") || hex.starts_with("0X") {
        hex.to_string()
    } else {
        format!("0x{hex}")
    }
}

/// Print the `instance unlock` result: the injected secret names, which
/// rootfs volume they were injected into, then the same attestation
/// evidence `instance attest` prints, sourced from the
/// `POST /confidential/inject-secret` exchange's own verified response
/// (not the earlier `fresh_attestation` call): this is the channel the
/// secret was actually sent over.
fn print_unlock_summary(
    injected: &[String],
    content: &InstanceContent,
    response: &AttestedResponse,
    endpoint: &Url,
    json: bool,
) {
    let rootfs_mib: u64 = content.rootfs.size_mib.into();
    let persistence = serde_json::to_value(&content.rootfs.persistence)
        .expect("VolumePersistence always serializes");
    if json {
        let out = serde_json::json!({
            "injected": injected,
            "rootfs": {
                "size_mib": rootfs_mib,
                "persistence": persistence,
            },
            "verified": true,
            "measurement": response.registers.launch,
            "policy": format!("{:#x}", response.policy),
            "launch_tcb": {
                "fmc": response.launch_tcb.fmc,
                "bootloader": response.launch_tcb.bootloader,
                "tee": response.launch_tcb.tee,
                "snp": response.launch_tcb.snp,
                "microcode": response.launch_tcb.microcode,
            },
            "reported_tcb": {
                "fmc": response.reported_tcb.fmc,
                "bootloader": response.reported_tcb.bootloader,
                "tee": response.reported_tcb.tee,
                "snp": response.reported_tcb.snp,
                "microcode": response.reported_tcb.microcode,
            },
            "cpuid": {
                "family": response.cpuid_family,
                "model": response.cpuid_model,
                "stepping": response.cpuid_stepping,
            },
            "platform_info": {
                "raw": format!("{:#x}", response.platform.raw),
                "smt_enabled": response.platform.smt_enabled,
                "tsme_enabled": response.platform.tsme_enabled,
                "ecc_enabled": response.platform.ecc_enabled,
                "rapl_disabled": response.platform.rapl_disabled,
                "ciphertext_hiding_enabled": response.platform.ciphertext_hiding_enabled,
                "alias_check_complete": response.platform.alias_check_complete,
            },
            "endpoint": endpoint.as_str(),
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&out).expect("unlock summary always serializes")
        );
    } else {
        println!("injected: {}", injected.join(", "));
        println!(
            "rootfs: {rootfs_mib} MiB ({} persistence)",
            persistence.as_str().unwrap_or("unknown")
        );
        println!("measurement: {}", response.registers.launch);
        println!("policy: {:#x}", response.policy);
        println!(
            "launch TCB: {}",
            tcb_summary(
                response.launch_tcb.fmc,
                response.launch_tcb.bootloader,
                response.launch_tcb.tee,
                response.launch_tcb.snp,
                response.launch_tcb.microcode,
            )
        );
        println!(
            "reported TCB: {}",
            tcb_summary(
                response.reported_tcb.fmc,
                response.reported_tcb.bootloader,
                response.reported_tcb.tee,
                response.reported_tcb.snp,
                response.reported_tcb.microcode,
            )
        );
        println!("platform: {}", platform_summary(&response.platform));
        println!("endpoint: {}", endpoint);
    }
}

/// `aleph instance unlock <vm-id>`: attest the instance, then inject its
/// LUKS passphrase (plus any `--secret` extras) over the same attested
/// RA-TLS channel, signed by the resolved account.
///
/// No secret byte is read, let alone sent, until two independent checks
/// pass: the attestation itself (measurement, policy, TCB floor, platform
/// posture, all enforced inside `run_instance_attest`), then the owner
/// check below. The injection request travels over a second, independently
/// attested exchange (`post_secrets`), pinned to the same measurement and
/// policy the first one verified, and its own verified measurement is
/// re-checked before the response is trusted.
pub(crate) async fn handle_instance_unlock(
    aleph_client: &AlephClient,
    scheduler_url: &Url,
    json: bool,
    args: &InstanceUnlockArgs,
) -> Result<()> {
    let account = resolve_account(&args.identity)?;

    let outcome = run_instance_attest(aleph_client, scheduler_url, &args.attest).await?;

    let owner = outcome.owner.as_str().to_lowercase();
    check_owner_account(&owner, account.address())?;

    let passphrase = read_passphrase(args.passphrase_file.as_deref())?;
    let secrets = collect_secrets(Some(passphrase), &args.secrets)?;

    let payload = inject_secret_payload(
        &outcome.fresh.served_public_key,
        &canonical_secrets_json(&secrets),
    );
    let signature = account
        .sign_raw(payload.as_bytes())
        .context("failed to sign the secret-injection payload")?;
    let envelope = InjectSecretEnvelope {
        secrets,
        signature: ensure_0x_prefixed(signature.as_str()),
    };

    // Always pin the POST handshake to the exact registers `fresh_attestation`
    // already verified, never `CallerVerified`, even for a `MemberOf` fleet
    // pin (where `run_instance_attest` already checked `outcome.fresh.registers`
    // against the pinned set). `CallerVerified` would let the handshake
    // succeed against any genuine, policy/TCB/platform-compliant SEV-SNP
    // guest regardless of its measurement, and the envelope (the plaintext
    // secret) is sent as soon as the handshake completes, before any
    // response-side check runs: a rogue-but-genuinely-attested guest could
    // receive the passphrase before its measurement was ever checked on this
    // exchange. With an Exact pin, a guest whose measurement changed between
    // attest and unlock (rebooted onto a different image) fails the
    // handshake itself, so no secret byte leaves; the recovery is the same
    // as any other stale-attestation case here: re-run unlock.
    let measurement_pin = MeasurementPin::Exact(&outcome.fresh.registers);
    let policy_pin = PolicyPin::Exact(outcome.fresh.policy);
    let platform_policy = attest_common::platform_policy_from(&args.attest.require_platform);
    let min_tcb = attest_common::resolve_tcb_floor(
        aleph_client,
        args.attest.amd_product,
        args.attest.min_tcb.as_ref(),
        args.attest.accept_outdated_tcb,
    )
    .await?;

    let (response, attested) = match post_secrets(
        &outcome.endpoint,
        &envelope,
        measurement_pin,
        policy_pin,
        args.attest.amd_product,
        &min_tcb,
        &platform_policy,
    )
    .await
    {
        Ok(pair) => pair,
        Err(AttestError::InjectRejected { status, body }) => bail!(
            "secret injection rejected: HTTP {status}: {body}\n\
             hint: if the VM rebooted since the attestation, its TLS key changed: re-run unlock"
        ),
        Err(e) => bail!(
            "secret injection failed: {e}\n\
             hint: if the VM rebooted or was rebuilt since the attestation, its TLS key or \
             measurement changed: re-run unlock"
        ),
    };

    // Sanity check, not a gate: `measurement_pin` above already pinned the
    // handshake to `outcome.fresh.registers` exactly, so a successful
    // `post_secrets` call can only have verified a report presenting those
    // same registers, which `run_instance_attest` already checked against
    // `outcome.expectation`. Nothing here can un-send an already-sent
    // secret; this only catches an internal inconsistency between this
    // handler and `attested_request`'s own pin enforcement.
    debug_assert!(
        measurement_is_expected(&attested.registers, &outcome.expectation),
        "post_secrets pinned MeasurementPin::Exact(&outcome.fresh.registers); the verified \
         response's registers must equal what run_instance_attest already checked"
    );

    print_unlock_summary(
        &response.injected,
        &outcome.content,
        &attested,
        &outcome.endpoint,
        json,
    );
    Ok(())
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

    use aleph_types::message::execution::base::{ExecutableContent, Payment};
    use aleph_types::message::execution::environment::{InstanceEnvironment, MachineResources};
    use aleph_types::message::execution::volume::{
        ParentVolume, PersistentVolumeSize, RootfsVolume, VolumePersistence,
    };
    use memsizes::MiB;

    /// A minimal, otherwise-valid `InstanceContent` with the given
    /// `trusted_execution`, for exercising `check_snp_instance` without a
    /// full message fixture. Constructed directly (bypassing
    /// `InstanceContent`'s `TryFrom` validation, e.g. the measured-mode
    /// credit-only check), since `check_snp_instance` itself doesn't care.
    fn minimal_instance_content(
        trusted_execution: Option<TrustedExecutionEnvironment>,
    ) -> InstanceContent {
        InstanceContent {
            base: ExecutableContent {
                allow_amend: false,
                metadata: None,
                variables: None,
                resources: MachineResources {
                    vcpus: 1,
                    memory: MiB::from(128),
                    seconds: 1,
                    published_ports: None,
                },
                payment: Some(Payment::credits()),
                requirements: None,
                volumes: vec![],
                replaces: None,
                authorized_keys: None,
            },
            environment: InstanceEnvironment {
                internet: false,
                aleph_api: false,
                hypervisor: None,
                trusted_execution,
                reproducible: false,
                shared_cache: false,
            },
            rootfs: RootfsVolume {
                parent: ParentVolume {
                    reference: "aa".repeat(32).parse().unwrap(),
                    use_latest: false,
                },
                persistence: VolumePersistence::Host,
                size_mib: PersistentVolumeSize::try_from(1u64).unwrap(),
                forgotten_by: None,
            },
        }
    }

    #[test]
    fn check_snp_instance_rejects_missing_trusted_execution() {
        let content = minimal_instance_content(None);
        let err = check_snp_instance(&content).unwrap_err().to_string();
        assert!(
            err.contains("not an SNP confidential instance"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn check_snp_instance_rejects_legacy_sev_mode() {
        let tee = TrustedExecutionEnvironment {
            firmware: Some("bb".repeat(32).parse().unwrap()),
            policy: 1,
            mode: None, // legacy SEV: mode is absent, not Some(TeeMode::Sev).
            runtime: None,
            measurements: None,
            attestation_port: None,
        };
        let content = minimal_instance_content(Some(tee));
        let err = check_snp_instance(&content).unwrap_err().to_string();
        assert!(
            err.contains("not an SNP confidential instance"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn check_snp_instance_accepts_sev_snp() {
        let tee = TrustedExecutionEnvironment {
            firmware: None,
            policy: 0x30000,
            mode: Some(TeeMode::SevSnp),
            runtime: Some("cc".repeat(32).parse().unwrap()),
            measurements: Some(vec![]),
            attestation_port: None,
        };
        let content = minimal_instance_content(Some(tee));
        let got = check_snp_instance(&content).unwrap();
        assert_eq!(got.mode, Some(TeeMode::SevSnp));
    }

    fn regs(launch: &str) -> SevSnpRegisters {
        SevSnpRegisters {
            launch: launch.to_string(),
        }
    }

    #[test]
    fn measurement_is_expected_pin_is_exact_equality() {
        let pin = MeasurementExpectation::Pin(regs(&"aa".repeat(48)));
        assert!(measurement_is_expected(&regs(&"aa".repeat(48)), &pin));
        assert!(!measurement_is_expected(&regs(&"bb".repeat(48)), &pin));
    }

    #[test]
    fn measurement_is_expected_member_of_is_set_membership() {
        let set =
            MeasurementExpectation::MemberOf(vec![regs(&"aa".repeat(48)), regs(&"bb".repeat(48))]);
        assert!(measurement_is_expected(&regs(&"aa".repeat(48)), &set));
        assert!(measurement_is_expected(&regs(&"bb".repeat(48)), &set));
        assert!(!measurement_is_expected(&regs(&"cc".repeat(48)), &set));
    }

    #[test]
    fn collect_secrets_parses_and_rejects_duplicates() {
        let m = collect_secrets(Some("p".into()), &["a=1".into(), "b=x=y".into()]).unwrap();
        assert_eq!(m.get("luks_passphrase").unwrap(), "p");
        assert_eq!(m.get("b").unwrap(), "x=y");
        assert!(collect_secrets(Some("p".into()), &["luks_passphrase=q".into()]).is_err());
        assert!(collect_secrets(None, &["a=1".into(), "a=2".into()]).is_err());
        assert!(collect_secrets(None, &["noequals".into()]).is_err());
    }

    #[test]
    fn collect_secrets_rejects_an_empty_key() {
        assert!(collect_secrets(None, &["=value".into()]).is_err());
    }

    #[test]
    fn owner_account_mismatch_is_named() {
        let owner = Address::from("0x1111111111111111111111111111111111111a".to_string());
        let signer = Address::from("0x2222222222222222222222222222222222222b".to_string());
        let err = check_owner_account(owner.as_str(), &signer)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(owner.as_str()),
            "error must name the owner: {err}"
        );
        assert!(
            err.contains(signer.as_str()),
            "error must name the signing account: {err}"
        );
        assert!(
            err.contains("content.address"),
            "error must name content.address: {err}"
        );
    }

    #[test]
    fn owner_account_match_is_case_insensitive() {
        let owner = Address::from("0xAAAA111111111111111111111111111111111a".to_string());
        let signer = Address::from("0xaaaa111111111111111111111111111111111A".to_string());
        assert!(check_owner_account(owner.as_str(), &signer).is_ok());
    }

    #[test]
    fn ensure_0x_prefixed_adds_missing_prefix_only() {
        assert_eq!(ensure_0x_prefixed("abcd"), "0xabcd");
        assert_eq!(ensure_0x_prefixed("0xabcd"), "0xabcd");
    }
}
