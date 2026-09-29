//! The Intel TDX arms of `aleph vprogram create`, `call` and `run`.
//!
//! A TDX runtime has a fixed cmdline (RTMR2 is constant per runtime) and
//! carries the per-deployment tokens on a descriptor drive bound by
//! MRCONFIGID, so the measured inputs differ from SEV-SNP's in shape, not in
//! content: the same workload and volume roothashes, rendered as the
//! descriptor suffix instead of cmdline slots.

use std::path::{Path, PathBuf};

use aleph_sdk::attest::{
    PcsClient, TcbStatus, TdxAttestedResponse, TdxFreshAttestation, TdxRegisterPin, TdxTcbPolicy,
    attested_request_tdx, fresh_attestation_tdx,
};
use aleph_sdk::vprogram::cmdline::{instantiate_tdx_cmdline, tdx_descriptor_suffix};
use aleph_sdk::vprogram::manifest::RuntimeManifest;
use aleph_sdk::vprogram::measure::compute_tdx_measurement;
use aleph_types::message::execution::environment::{
    DEFAULT_SNP_POLICY, LaunchMeasurement, TdxRegisters,
};
use aleph_types::message::{ConfidentialGpuRequirement, TeeVerification};
use anyhow::{Context, Result, anyhow, bail};
use url::Url;

use super::vprogram::{Freshness, LocalBuild};
use crate::cli::{TdxTcbStatusArg, VProgramCallArgs};

/// The fixed cmdline and the descriptor suffix of a TDX deployment.
pub(crate) fn tdx_boot_inputs(
    manifest: &RuntimeManifest,
    workload_roothash: &str,
    volume_roothashes: &[String],
    gpu: Option<&ConfidentialGpuRequirement>,
) -> Result<(String, String)> {
    if gpu.is_some() {
        bail!("confidential GPUs are not supported on tdx runtimes");
    }
    let cmdline = instantiate_tdx_cmdline(
        &manifest.boot.cmdline_template,
        &manifest.boot.platform_roothash,
    )?;
    Ok((
        cmdline,
        tdx_descriptor_suffix(workload_roothash, volume_roothashes),
    ))
}

/// The verification block of a TDX V-PROGRAM: one measurement, the runtime
/// triple cross-checked against the bundle and MRCONFIGID over this
/// deployment's descriptor. `--policy`/`--allow-debug` are SEV-SNP knobs a
/// TD has no meaning for, so a value is refused rather than dropped.
pub(crate) fn tdx_verification(
    build: &LocalBuild,
    policy: u64,
    allow_debug: bool,
    json: bool,
) -> Result<TeeVerification> {
    if policy != DEFAULT_SNP_POLICY || allow_debug {
        bail!("a tdx runtime has no host-chosen launch policy: drop --policy and --allow-debug");
    }
    let published = build
        .manifest
        .measurements
        .as_ref()
        .context("tdx runtime manifest publishes no measurements")?;
    let suffix = build
        .descriptor_suffix
        .as_deref()
        .context("tdx build has no descriptor suffix")?;
    if !json {
        eprintln!(
            "Computing TDX measurement (MRTD, RTMR1, RTMR2 from the bundle, MRCONFIGID from the descriptor)..."
        );
    }
    let measurement: LaunchMeasurement =
        compute_tdx_measurement(&build.artifacts, &build.cmdline, published, suffix)?;
    Ok(serde_json::from_value(serde_json::json!({
        "backend": "tdx",
        "measurements": [measurement],
    }))?)
}

/// First line of the descriptor drive; the guest init finds the drive by it.
const DESCRIPTOR_MAGIC: &[u8] = b"ALEPH-TDX-DESCRIPTOR-v1\n";
/// The drive is a raw 64 KiB image, zero-padded after the suffix line.
const DESCRIPTOR_SIZE: usize = 64 * 1024;

/// Write the descriptor drive a local (unattested) run attaches last, in
/// the exact layout the CRN writes: magic line, suffix line, zero padding.
pub(crate) fn write_descriptor_image(dir: &Path, suffix: &str) -> Result<PathBuf> {
    let mut image = Vec::with_capacity(DESCRIPTOR_SIZE);
    image.extend_from_slice(DESCRIPTOR_MAGIC);
    image.extend_from_slice(suffix.as_bytes());
    image.push(b'\n');
    if image.len() > DESCRIPTOR_SIZE {
        bail!(
            "TDX descriptor of {} bytes exceeds the {DESCRIPTOR_SIZE}-byte drive",
            image.len()
        );
    }
    image.resize(DESCRIPTOR_SIZE, 0);
    let path = dir.join("tdx-descriptor.img");
    std::fs::write(&path, &image)
        .with_context(|| format!("writing the TDX descriptor drive to {}", path.display()))?;
    Ok(path)
}

/// The TCB policy `call` verifies a TD's quote under: the builtin default
/// (UpToDate, SWHardeningNeeded) widened by `--tdx-accept-tcb` and narrowed
/// by `--tdx-deny-advisory`.
pub(crate) fn tdx_tcb_policy(accept: &[TdxTcbStatusArg], deny: &[String]) -> TdxTcbPolicy {
    let mut policy = TdxTcbPolicy::default();
    for status in accept {
        policy.accepted_statuses.insert(status.status());
    }
    for advisory in deny {
        policy.denied_advisories.insert(advisory.clone());
    }
    policy
}

impl TdxTcbStatusArg {
    fn status(self) -> TcbStatus {
        match self {
            TdxTcbStatusArg::UpToDate => TcbStatus::UpToDate,
            TdxTcbStatusArg::SwHardeningNeeded => TcbStatus::SwHardeningNeeded,
            TdxTcbStatusArg::ConfigurationNeeded => TcbStatus::ConfigurationNeeded,
            TdxTcbStatusArg::ConfigurationAndSwHardeningNeeded => {
                TcbStatus::ConfigurationAndSwHardeningNeeded
            }
            TdxTcbStatusArg::OutOfDate => TcbStatus::OutOfDate,
            TdxTcbStatusArg::OutOfDateConfigurationNeeded => {
                TcbStatus::OutOfDateConfigurationNeeded
            }
        }
    }
}

/// The register pin of a TDX message: exactly one measurement by schema, so
/// there is no fleet allow-list to defer. `--expected-measurement` is an
/// SNP launch digest and does not apply.
pub(crate) fn resolve_expected_registers(
    measurements: &[LaunchMeasurement],
    override_hex: Option<&str>,
) -> Result<TdxRegisters> {
    if override_hex.is_some() {
        bail!(
            "--expected-measurement is an SEV-SNP launch digest; a tdx V-Program pins its four \
             registers on the message"
        );
    }
    match measurements {
        [one] => one.registers.as_tdx().cloned().ok_or_else(|| {
            anyhow!(
                "the tdx message pins a {} measurement",
                one.platform.as_str()
            )
        }),
        other => bail!(
            "a tdx V-Program declares exactly one measurement, this message has {}",
            other.len()
        ),
    }
}

/// The attested call against a TD: fresh-nonce challenge first (unless
/// stale attestation is allowed), then the request, both pinned on the
/// message's registers and verified against Intel's collateral under the
/// caller's TCB policy.
pub(crate) async fn call_tdx(
    base_url: &Url,
    args: &VProgramCallArgs,
    headers: &[(String, String)],
    body: Option<bytes::Bytes>,
    expected: &TdxRegisters,
) -> Result<(TdxAttestedResponse, Freshness)> {
    let tcb_policy = tdx_tcb_policy(&args.tdx_accept_tcb, &args.tdx_deny_advisory);
    let pcs = PcsClient::intel();
    let pin = TdxRegisterPin::Exact(expected);

    let fresh = if args.allow_stale_attestation {
        None
    } else {
        Some(
            fresh_attestation_tdx(base_url, pin, &tcb_policy, &pcs)
                .await
                .map_err(|e| anyhow!("fresh attestation challenge failed: {e}"))?,
        )
    };

    let response = attested_request_tdx(
        base_url,
        args.method.clone(),
        &args.path,
        headers,
        body,
        pin,
        &tcb_policy,
        &pcs,
    )
    .await
    .map_err(|e| anyhow!("attestation failed: {e}"))?;

    // The handshake pinned the SIGNED registers; re-check on the verified
    // value so the trust decision never rests on a single site.
    if &response.registers != expected {
        bail!(
            "register mismatch: the guest presented {:?} which does not match the registers \
             pinned on the V-Program message",
            response.registers
        );
    }
    let freshness = match &fresh {
        Some(fresh) => {
            check_fresh_consistency(fresh, &response)?;
            Freshness::Verified
        }
        None => Freshness::Skipped,
    };
    Ok((response, freshness))
}

fn check_fresh_consistency(
    fresh: &TdxFreshAttestation,
    response: &TdxAttestedResponse,
) -> Result<()> {
    if fresh.served_public_key != response.served_public_key {
        bail!(
            "the fresh attestation challenge was answered by a different TLS identity than \
             the one that served the response; refusing to transfer liveness"
        );
    }
    if fresh.registers != response.registers {
        bail!("fresh quote registers do not match the response's verified registers");
    }
    Ok(())
}

/// Render a TDX call result as `(stdout, stderr_meta)`, the same contract
/// as the SNP renderer: raw body on stdout in text mode with the verdict on
/// stderr, one JSON document in `--json` mode.
pub(crate) fn render_tdx_call_result(
    response: &TdxAttestedResponse,
    freshness: Freshness,
    json: bool,
    verbose: bool,
) -> (String, Option<String>) {
    let tcb = format!("{:?}", response.tcb_status);
    if json {
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&response.body).into_owned())
        });
        let out = serde_json::json!({
            "backend": "tdx",
            "registers": response.registers,
            "tcb_status": tcb,
            "advisories": response.advisory_ids,
            "status": response.status,
            "body": body,
            "freshness": match freshness {
                Freshness::Verified => "verified",
                Freshness::Skipped => "skipped",
            },
        });
        (
            serde_json::to_string_pretty(&out).expect("call result always serializes"),
            None,
        )
    } else {
        let liveness = match freshness {
            Freshness::Verified => "fresh-nonce liveness",
            Freshness::Skipped => "liveness SKIPPED (--allow-stale-attestation)",
        };
        let mut meta = format!(
            "Attestation: Intel TDX quote verified (PCK chain, CRLs, QE identity, quote \
             signature, key binding, register pin, TCB {tcb}, {liveness})\nHTTP {}",
            response.status
        );
        if !response.advisory_ids.is_empty() {
            meta.push_str(&format!(
                "\nTCB advisories: {}",
                response.advisory_ids.join(", ")
            ));
        }
        if verbose {
            meta.push_str(&format!(
                "\nmrtd: {}\nrtmr1: {}\nrtmr2: {}\nmrconfigid: {}",
                response.registers.mrtd,
                response.registers.rtmr1,
                response.registers.rtmr2,
                response.registers.mrconfigid,
            ));
        }
        (
            String::from_utf8_lossy(&response.body).into_owned(),
            Some(meta),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::message::execution::environment::{MeasurementRegisters, TeePlatform};

    fn registers() -> TdxRegisters {
        TdxRegisters {
            mrtd: "11".repeat(48),
            rtmr1: "22".repeat(48),
            rtmr2: "33".repeat(48),
            mrconfigid: "44".repeat(48),
        }
    }

    fn measurement(registers: TdxRegisters) -> LaunchMeasurement {
        LaunchMeasurement {
            platform: TeePlatform::Tdx,
            registers: MeasurementRegisters::Tdx(registers),
            vcpu_type: None,
        }
    }

    #[test]
    fn descriptor_image_has_the_crn_layout() {
        let dir = tempfile::tempdir().unwrap();
        let suffix = format!("workload_roothash={}", "cd".repeat(32));
        let path = write_descriptor_image(dir.path(), &suffix).unwrap();
        let image = std::fs::read(path).unwrap();
        assert_eq!(image.len(), DESCRIPTOR_SIZE);
        let expected = format!("ALEPH-TDX-DESCRIPTOR-v1\n{suffix}\n");
        assert_eq!(&image[..expected.len()], expected.as_bytes());
        assert!(image[expected.len()..].iter().all(|b| *b == 0));
    }

    #[test]
    fn expected_registers_come_from_the_single_measurement() {
        let expected = resolve_expected_registers(&[measurement(registers())], None).unwrap();
        assert_eq!(expected, registers());
        assert!(resolve_expected_registers(&[], None).is_err());
        assert!(
            resolve_expected_registers(&[measurement(registers()), measurement(registers())], None)
                .is_err()
        );
        let err = resolve_expected_registers(&[measurement(registers())], Some("ab"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("--expected-measurement"), "{err}");
    }

    #[test]
    fn tcb_policy_flags_widen_and_narrow_the_default() {
        let policy = tdx_tcb_policy(&[], &[]);
        assert!(policy.accepted_statuses.contains(&TcbStatus::UpToDate));
        assert!(!policy.accepted_statuses.contains(&TcbStatus::OutOfDate));
        let policy = tdx_tcb_policy(
            &[TdxTcbStatusArg::OutOfDate],
            &["INTEL-SA-01245".to_string()],
        );
        assert!(policy.accepted_statuses.contains(&TcbStatus::OutOfDate));
        assert!(policy.denied_advisories.contains("INTEL-SA-01245"));
    }

    #[test]
    fn render_reports_the_verdict_and_advisories() {
        let response = TdxAttestedResponse {
            registers: registers(),
            tcb_status: TcbStatus::OutOfDate,
            advisory_ids: vec!["INTEL-SA-01268".into()],
            served_public_key: vec![4; 97],
            status: 200,
            headers: vec![],
            body: bytes::Bytes::from_static(b"{\"fib\":55}"),
        };
        let (out, meta) = render_tdx_call_result(&response, Freshness::Verified, false, true);
        assert_eq!(out, "{\"fib\":55}");
        let meta = meta.unwrap();
        assert!(meta.contains("TCB OutOfDate"), "{meta}");
        assert!(meta.contains("INTEL-SA-01268"), "{meta}");
        assert!(
            meta.contains(&format!("mrconfigid: {}", "44".repeat(48))),
            "{meta}"
        );
        let (out, meta) = render_tdx_call_result(&response, Freshness::Skipped, true, false);
        assert!(meta.is_none());
        let json: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(json["backend"], "tdx");
        assert_eq!(json["tcb_status"], "OutOfDate");
        assert_eq!(json["freshness"], "skipped");
        assert_eq!(json["body"]["fib"], 55);
    }
}
