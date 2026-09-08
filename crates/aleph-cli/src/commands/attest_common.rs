//! Attestation plumbing shared by `aleph vprogram call` and the SNP
//! instance verbs (`instance attest`, `instance unlock`): resolving which
//! launch measurement(s) an attested call must match, resolving the TCB
//! floor, building the `--require-platform` policy, and discovering a VM's
//! attested endpoint via the scheduler + CRN.
//!
//! Extracted from `vprogram.rs` (moved verbatim, `pub(crate)`) so
//! `instance_snp.rs`'s `run_instance_attest` can reuse the same pinning and
//! discovery logic rather than re-implementing it.

use aleph_sdk::attest::PlatformPolicy;
use aleph_sdk::client::{AlephAggregateClient, AlephClient};
use aleph_sdk::crn::{ActiveVmNetworking, fetch_active_vms};
use aleph_sdk::scheduler::SchedulerClient;
use aleph_sdk::vprogram::status::resolve_attested_endpoint;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::environment::{LaunchMeasurement, SevSnpRegisters};
use anyhow::{Context, Result, anyhow, bail};
use url::Url;

use crate::cli::PlatformRequirement;

/// A SEV-SNP launch measurement is a SHA-384 digest: 48 bytes, 96 hex chars.
const SEV_SNP_MEASUREMENT_BYTES: usize = 48;

/// The expected launch measurement(s) an attested call must match, resolved
/// by [`resolve_expected_measurement`].
///
/// - `Pin`: a single digest, known before the TLS handshake. Passed straight
///   into `attested_request`'s `expected_measurement`, so a mismatch fails
///   the handshake itself.
/// - `MemberOf`: more than one digest is pinned on the message (a
///   mixed-CPU-model fleet where different nodes measure differently).
///   Nothing can be pinned at handshake time since it isn't known in advance
///   which one the guest will present, so the handshake pins nothing and the
///   caller must check the verified measurement against this set
///   *after* `attested_request` returns - before trusting the response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MeasurementExpectation {
    Pin(SevSnpRegisters),
    MemberOf(Vec<SevSnpRegisters>),
}

/// Resolve which measurement(s) an attested call must match, per the
/// security invariant: the call is only trusted if the verified report's
/// measurement is among the ones pinned on-chain (or the explicit
/// `--expected-measurement` override).
///
/// Pure and I/O-free so it's directly unit-testable. Pass
/// `content.verification.measurements` as-is: an entry for a platform this
/// client cannot verify fails the resolution rather than being skipped.
pub(crate) fn resolve_expected_measurement(
    measurements: &[LaunchMeasurement],
    override_hex: Option<&str>,
) -> Result<MeasurementExpectation> {
    if let Some(hex_str) = override_hex {
        let bytes = hex::decode(hex_str)
            .with_context(|| format!("--expected-measurement is not valid hex: {hex_str:?}"))?;
        // A SEV-SNP launch measurement is a 48-byte SHA-384 digest; anything
        // else can never match a real report, so fail here with a clear
        // message instead of a cryptic measurement mismatch at call time.
        if bytes.len() != SEV_SNP_MEASUREMENT_BYTES {
            bail!(
                "--expected-measurement must be {} hex characters (a {}-byte SEV-SNP launch \
                 measurement), got {}",
                SEV_SNP_MEASUREMENT_BYTES * 2,
                SEV_SNP_MEASUREMENT_BYTES,
                hex_str.len()
            );
        }
        // Re-encode so the pin compares structurally against the lowercase
        // hex the SDK derives from the signed report.
        return Ok(MeasurementExpectation::Pin(SevSnpRegisters {
            launch: hex::encode(bytes),
        }));
    }

    // The message's registers are the pin, same type both sides. An entry
    // this client cannot check (another platform's register set) fails
    // closed rather than being skipped, since skipping would narrow the
    // accepted set silently. Unreachable through a valid message today: the
    // schema restricts the V-PROGRAM backend to sev_snp.
    let snp_registers: Vec<SevSnpRegisters> = measurements
        .iter()
        .map(|m| {
            m.registers.as_sev_snp().cloned().ok_or_else(|| {
                anyhow!(
                    "the message pins a {} measurement, which this client cannot verify",
                    m.platform.as_str()
                )
            })
        })
        .collect::<Result<_>>()?;

    match snp_registers.len() {
        0 => bail!(
            "V-Program message pins no launch measurements; cannot verify attestation without \
             --expected-measurement"
        ),
        1 => Ok(MeasurementExpectation::Pin(
            snp_registers.into_iter().next().expect("len checked"),
        )),
        _ => Ok(MeasurementExpectation::MemberOf(snp_registers)),
    }
}

/// Pure: fold an override patch onto every silicon line's floor of the
/// network policy, gating any lowering behind `accept_outdated`. The
/// lowering gate fires if the patch lowers ANY line's floor (a `--min-tcb`
/// between the Zen4c and classic floors still weakens the classic gate,
/// and the silicon line is only known once a report is in hand). I/O-free
/// so it is directly unit-testable.
pub(crate) fn resolve_effective_floor(
    network: &aleph_sdk::attest::TcbFloorPolicy,
    override_patch: Option<&aleph_sdk::attest::TcbFloorOverride>,
    accept_outdated: bool,
) -> Result<aleph_sdk::attest::TcbFloorPolicy> {
    let Some(patch) = override_patch else {
        return Ok(*network);
    };
    let (eff_default, mut lowered) = patch.apply_to(&network.default);
    let eff_x_variant = network.x_variant.as_ref().map(|floor| {
        let (eff, lowered_x) = patch.apply_to(floor);
        lowered.extend(lowered_x);
        eff
    });
    let eff_zen4c = network.zen4c.as_ref().map(|floor| {
        let (eff, lowered_zen4c) = patch.apply_to(floor);
        lowered.extend(lowered_zen4c);
        eff
    });
    lowered.sort();
    lowered.dedup();
    if !lowered.is_empty() {
        if !accept_outdated {
            bail!(
                "--min-tcb lowers {lowered:?} below the network floor; \
                 pass --accept-outdated-tcb to accept the risk"
            );
        }
        eprintln!(
            "warning: accepting a TCB below the network floor for {lowered:?}: the node \
             runs known-outdated firmware, so the guest may be exposed"
        );
    }
    Ok(aleph_sdk::attest::TcbFloorPolicy {
        default: eff_default,
        x_variant: eff_x_variant,
        zen4c: eff_zen4c,
    })
}

/// Resolve the network floor policy (per-family builtin baselines raised by
/// the settings aggregate), then apply the override. Aggregate failure falls
/// back to the baselines with a warning.
pub(crate) async fn resolve_tcb_floor(
    aleph_client: &AlephClient,
    product: aleph_sdk::attest::AmdProduct,
    override_patch: Option<&aleph_sdk::attest::TcbFloorOverride>,
    accept_outdated: bool,
) -> Result<aleph_sdk::attest::TcbFloorPolicy> {
    let baseline = aleph_sdk::attest::builtin_baseline_policy(product);
    let network = match aleph_client.get_settings_aggregate().await {
        Ok(agg) => match agg.settings.snp_min_tcb.floor_for(product) {
            Some(f) => baseline.raise_to(&f),
            None => baseline,
        },
        Err(e) => {
            eprintln!(
                "warning: could not fetch the network TCB floor ({e}); using the built-in baseline"
            );
            baseline
        }
    };
    resolve_effective_floor(&network, override_patch, accept_outdated)
}

/// Build the SDK's [`PlatformPolicy`] from the `--require-platform` values.
/// An empty list is [`PlatformPolicy::NONE`]: posture is surfaced, never
/// gated (the current fleet fails every bit, so requiring is opt-in).
pub(crate) fn platform_policy_from(requirements: &[PlatformRequirement]) -> PlatformPolicy {
    let mut policy = PlatformPolicy::NONE;
    for requirement in requirements {
        match requirement {
            PlatformRequirement::SmtOff => policy.require_smt_disabled = true,
            PlatformRequirement::Tsme => policy.require_tsme = true,
            PlatformRequirement::RaplOff => policy.require_rapl_disabled = true,
            PlatformRequirement::CiphertextHiding => policy.require_ciphertext_hiding = true,
            PlatformRequirement::AliasCheck => policy.require_alias_check = true,
        }
    }
    policy
}

/// True if `policy` has the SEV-SNP DEBUG bit (19) set: the host may then
/// decrypt guest memory via the firmware debug API, so the deployment is
/// not confidential in any meaningful sense.
pub(crate) fn policy_debug_allowed(policy: u64) -> bool {
    const SNP_POLICY_DEBUG_BIT: u64 = 1 << 19;
    policy & SNP_POLICY_DEBUG_BIT != 0
}

/// Reject a DEBUG-enabled policy unless the caller explicitly acknowledged
/// it with `--allow-debug`. When the DEBUG bit is set and acknowledged, emit
/// a loud warning so the operator knows the deployment is not confidential.
///
/// Shared by `vprogram create` and `instance create`'s SNP path so the guard
/// logic (and its unit tests) live in one place.
pub(crate) fn check_debug_policy(policy: u64, allow_debug: bool) -> Result<()> {
    if !policy_debug_allowed(policy) {
        return Ok(());
    }
    if !allow_debug {
        bail!(
            "--policy {:#x} has the SEV-SNP DEBUG bit (19) set: the host will be able to \
             decrypt guest memory, so this deployment will NOT be confidential. \
             Pass --allow-debug to acknowledge and publish anyway",
            policy
        );
    }
    eprintln!(
        "warning: --policy {:#x} has the SEV-SNP DEBUG bit (19) set: the host will be \
         able to decrypt guest memory, so this deployment will NOT be confidential",
        policy
    );
    Ok(())
}

/// Best-effort live-CRN lookup: resolves the scheduler placement for
/// `item_hash`, then the CRN's active-VM networking for it. Returns `None`
/// (never an error) whenever the VM isn't placed yet or any hop along the
/// way is unreachable - `render_show` treats that as "not running" and
/// shows only the message-side fields, mirroring how `instance show`
/// degrades when the scheduler/CRN is unreachable.
pub(crate) async fn fetch_live_networking(
    scheduler: &SchedulerClient,
    item_hash: &ItemHash,
) -> Option<ActiveVmNetworking> {
    let entry = match scheduler.get_vm(item_hash).await {
        Ok(Some(entry)) => entry,
        Ok(None) => return None,
        Err(e) => {
            eprintln!("warning: scheduler unreachable, live status unavailable: {e}");
            return None;
        }
    };
    let node_hash = entry.allocated_node?;
    let node = match scheduler.get_node(&node_hash).await {
        Ok(Some(node)) => node,
        Ok(None) => {
            eprintln!(
                "warning: scheduler has no record of node {node_hash}; live status unavailable"
            );
            return None;
        }
        Err(e) => {
            eprintln!("warning: scheduler unreachable for node {node_hash}: {e}");
            return None;
        }
    };
    let Some(addr) = node.address.as_deref() else {
        eprintln!(
            "warning: scheduler reports no address for node {node_hash}; live status unavailable"
        );
        return None;
    };
    let crn_url = match crate::common::parse_crn_address(addr, &node_hash.to_string()) {
        Some(url) => url,
        None => return None,
    };
    let http = reqwest::Client::new();
    match fetch_active_vms(&http, &crn_url).await {
        Ok(list) => list.0.get(item_hash).and_then(|vm| vm.networking.clone()),
        Err(e) => {
            eprintln!("warning: CRN {crn_url} unreachable, live status unavailable: {e}");
            None
        }
    }
}

/// One sample of the VM's attested endpoint via the scheduler + CRN.
pub(crate) async fn fetch_attested_endpoint(
    scheduler: &SchedulerClient,
    vm_id: &ItemHash,
    attest_port: u16,
) -> Option<Url> {
    let net = fetch_live_networking(scheduler, vm_id).await?;
    resolve_attested_endpoint(&net, attest_port)
}

/// Poll `fetch` until it yields an attested endpoint, or until `timeout`
/// elapses (always sampling at least once).
///
/// The CRN maps the attestation port to a host port only after the guest
/// finishes booting, which for a SEV-SNP V-Program (runtime bundle download
/// and measured boot) comes well after the scheduler/networking readiness
/// that `--wait` observes first; a single sample taken right at readiness
/// would nearly always miss the mapping. Generic over `fetch` and `sleep`,
/// mirroring `instance_wait::poll_until_ready`, so tests drive it without a
/// network or a clock.
pub(crate) async fn poll_attested_endpoint<F, Fut, S, SFut>(
    mut fetch: F,
    mut sleep: S,
    timeout: std::time::Duration,
    poll_interval: std::time::Duration,
) -> Option<Url>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<Url>>,
    S: FnMut(std::time::Duration) -> SFut,
    SFut: std::future::Future<Output = ()>,
{
    let start = std::time::Instant::now();
    loop {
        if let Some(endpoint) = fetch().await {
            return Some(endpoint);
        }
        if start.elapsed() >= timeout {
            return None;
        }
        sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_debug_allowed_detects_the_snp_debug_bit() {
        // 0x30000 is the recommended default: SMT allowed, no debug.
        assert!(!policy_debug_allowed(0x30000));
        // Bit 19 set: the host may decrypt guest memory.
        assert!(policy_debug_allowed(0x30000 | (1 << 19)));
    }

    #[test]
    fn check_debug_policy_accepts_non_debug_policy() {
        assert!(check_debug_policy(0x30000, false).is_ok());
        // allow_debug is irrelevant when the DEBUG bit is not set.
        assert!(check_debug_policy(0x30000, true).is_ok());
    }

    #[test]
    fn check_debug_policy_rejects_debug_without_allow_debug() {
        // 0xa0000 = bit 17 (reserved) | bit 19 (DEBUG): a valid policy that
        // passes validate_snp_policy but has the DEBUG bit set.
        let err = check_debug_policy(0xa0000, false).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("DEBUG bit (19) set") && msg.contains("--allow-debug"),
            "expected a DEBUG rejection mentioning --allow-debug, got: {msg}"
        );
    }

    #[test]
    fn check_debug_policy_accepts_debug_with_allow_debug() {
        // The --allow-debug ack clears the gate; the warning goes to stderr.
        assert!(check_debug_policy(0xa0000, true).is_ok());
    }

    fn measurement(digest: &str, vcpu_type: Option<&str>) -> LaunchMeasurement {
        let json = serde_json::json!({
            "platform": "sev_snp",
            "registers": {"launch": digest},
            "vcpu_type": vcpu_type,
        });
        serde_json::from_value(json).expect("valid measurement fixture")
    }

    #[test]
    fn resolve_expected_measurement_rejects_wrong_length_override() {
        let err = resolve_expected_measurement(&[], Some("ab"))
            .expect_err("a 1-byte override must be rejected");
        assert!(
            err.to_string().contains("96 hex characters"),
            "error should state the expected length, got: {err}"
        );
    }

    /// Registers pinning `launch` as given, the shape the resolver now
    /// yields and the SDK derives.
    fn regs(launch: &str) -> SevSnpRegisters {
        SevSnpRegisters {
            launch: launch.to_string(),
        }
    }

    #[test]
    fn resolve_expected_measurement_accepts_a_full_length_override() {
        let digest = "cd".repeat(48);
        let expected = resolve_expected_measurement(&[], Some(&digest)).unwrap();
        assert_eq!(expected, MeasurementExpectation::Pin(regs(&digest)));
    }

    #[test]
    fn resolve_expected_measurement_pins_the_single_measurement() {
        let digest = "ab".repeat(48);
        let measurements = vec![measurement(&digest, Some("EPYC-v4"))];

        let expected = resolve_expected_measurement(&measurements, None).unwrap();

        assert_eq!(expected, MeasurementExpectation::Pin(regs(&digest)));
    }

    #[test]
    fn resolve_expected_measurement_override_takes_precedence() {
        let digest = "ab".repeat(48);
        let override_digest = "cd".repeat(48);
        let measurements = vec![measurement(&digest, Some("EPYC-v4"))];

        let expected = resolve_expected_measurement(&measurements, Some(&override_digest)).unwrap();

        assert_eq!(
            expected,
            MeasurementExpectation::Pin(regs(&override_digest))
        );
    }

    #[test]
    fn resolve_expected_measurement_multiple_yields_member_of_set() {
        let digest_a = "ab".repeat(48);
        let digest_b = "cd".repeat(48);
        let measurements = vec![
            measurement(&digest_a, Some("EPYC-v4")),
            measurement(&digest_b, Some("EPYC-Genoa")),
        ];

        let expected = resolve_expected_measurement(&measurements, None).unwrap();

        assert_eq!(
            expected,
            MeasurementExpectation::MemberOf(vec![regs(&digest_a), regs(&digest_b)])
        );
    }

    #[test]
    fn resolve_expected_measurement_fails_closed_on_foreign_platform() {
        // A register set this client cannot verify must error, never be
        // skipped: skipping would silently narrow the accepted set.
        // Unreachable through a valid message today (the schema restricts
        // the V-PROGRAM backend to sev_snp), so pin the branch directly.
        let r = "11".repeat(48);
        let json = serde_json::json!({
            "platform": "tdx",
            "registers": {"mrtd": r, "rtmr1": r, "rtmr2": r, "mrconfigid": r},
        });
        let tdx: LaunchMeasurement = serde_json::from_value(json).expect("valid tdx measurement");
        let measurements = vec![measurement(&"ab".repeat(48), Some("EPYC-v4")), tdx];

        let err = resolve_expected_measurement(&measurements, None)
            .expect_err("a tdx measurement must fail the resolution");
        assert!(
            err.to_string().contains("tdx measurement"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_expected_measurement_zero_measurements_errors() {
        let result = resolve_expected_measurement(&[], None);

        assert!(
            result.is_err(),
            "a message pinning no measurements must fail closed without --expected-measurement"
        );
    }

    #[test]
    fn resolve_expected_measurement_bad_override_hex_errors() {
        let digest = "ab".repeat(48);
        let measurements = vec![measurement(&digest, Some("EPYC-v4"))];

        let result = resolve_expected_measurement(&measurements, Some("not-hex"));

        assert!(result.is_err());
    }

    #[test]
    fn platform_requirements_build_the_policy() {
        let policy = platform_policy_from(&[
            PlatformRequirement::RaplOff,
            PlatformRequirement::AliasCheck,
        ]);
        assert!(policy.require_rapl_disabled);
        assert!(policy.require_alias_check);
        assert!(!policy.require_smt_disabled);
        assert!(!policy.require_tsme);
        assert!(!policy.require_ciphertext_hiding);
    }

    use aleph_sdk::attest::{TcbFloor, TcbFloorOverride, TcbFloorPolicy};

    fn net() -> TcbFloorPolicy {
        TcbFloorPolicy {
            default: TcbFloor {
                fmc: None,
                bootloader: 4,
                tee: 0,
                snp: 21,
                microcode: 84,
            },
            // A Genoa-shaped policy: the Genoa-X and Zen4c lines each
            // follow their own, lower microcode sequence.
            x_variant: Some(TcbFloor {
                fmc: None,
                bootloader: 4,
                tee: 0,
                snp: 21,
                microcode: 79,
            }),
            zen4c: Some(TcbFloor {
                fmc: None,
                bootloader: 4,
                tee: 0,
                snp: 21,
                microcode: 28,
            }),
        }
    }

    #[test]
    fn no_override_yields_the_network_floor() {
        assert_eq!(resolve_effective_floor(&net(), None, false).unwrap(), net());
    }

    #[test]
    fn raising_override_needs_no_acknowledgement() {
        let o: TcbFloorOverride = "snp=30".parse().unwrap();
        let eff = resolve_effective_floor(&net(), Some(&o), false).unwrap();
        // The raise lands on every family floor.
        assert_eq!(eff.default.snp, 30);
        assert_eq!(eff.zen4c.unwrap().snp, 30);
    }

    #[test]
    fn lowering_override_without_ack_is_rejected() {
        let o: TcbFloorOverride = "snp=9".parse().unwrap();
        assert!(resolve_effective_floor(&net(), Some(&o), false).is_err());
    }

    #[test]
    fn lowering_override_with_ack_is_accepted() {
        let o: TcbFloorOverride = "snp=9".parse().unwrap();
        let eff = resolve_effective_floor(&net(), Some(&o), true).unwrap();
        assert_eq!(eff.default.snp, 9);
        assert_eq!(eff.zen4c.unwrap().snp, 9);
    }

    #[test]
    fn lowering_only_the_zen4c_floor_still_needs_the_ack() {
        // microcode=50 sits between the zen4c floor (28, raised) and the
        // classic floor (84, lowered): the gate fires because the silicon
        // family is unknown until a report is in hand, so ANY family's
        // weakened floor needs the explicit acknowledgement.
        let o: TcbFloorOverride = "microcode=50".parse().unwrap();
        assert!(resolve_effective_floor(&net(), Some(&o), false).is_err());
        let eff = resolve_effective_floor(&net(), Some(&o), true).unwrap();
        assert_eq!(eff.default.microcode, 50);
        assert_eq!(eff.x_variant.unwrap().microcode, 50);
        assert_eq!(eff.zen4c.unwrap().microcode, 50);
    }

    mod poll_endpoint {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        #[tokio::test]
        async fn resolves_once_the_mapping_appears() {
            let calls = AtomicUsize::new(0);
            let url = Url::parse("https://203.0.113.5:24101/").unwrap();

            let got = poll_attested_endpoint(
                || {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    let url = url.clone();
                    async move { (n >= 2).then_some(url) }
                },
                |_| async {},
                Duration::from_secs(60),
                Duration::from_secs(5),
            )
            .await;

            assert_eq!(got, Some(url));
            assert_eq!(calls.load(Ordering::SeqCst), 3);
        }

        #[tokio::test]
        async fn samples_at_least_once_then_gives_up_at_the_deadline() {
            let calls = AtomicUsize::new(0);
            let sleeps = AtomicUsize::new(0);

            let got = poll_attested_endpoint(
                || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async { None }
                },
                |_| {
                    sleeps.fetch_add(1, Ordering::SeqCst);
                    async {}
                },
                Duration::ZERO,
                Duration::from_secs(5),
            )
            .await;

            assert_eq!(got, None);
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "an exhausted budget still samples once"
            );
            assert_eq!(sleeps.load(Ordering::SeqCst), 0);
        }
    }
}
