//! Intel TDX quote verification for attested calls.
//!
//! The DCAP verifier lives in `aleph_tee::tdx` (shared with the CRN and the
//! guest agent); this module is the SDK's seam onto it: fetch the collateral
//! a quote's PCK chain names from Intel PCS, verify chain, signatures and
//! TCB against the caller's policy, and hand back the registers in the shape
//! the V-PROGRAM message pins.
//!
//! Where SEV-SNP has a `TcbFloorPolicy` (component version floors), TDX has
//! Intel's signed appraisal: a `TcbStatus` per platform plus the advisories
//! that status carries. [`TdxTcbPolicy`] is the set of statuses a caller
//! accepts and the advisories it refuses; the builtin default accepts
//! `UpToDate` and `SWHardeningNeeded` only.

use std::time::SystemTime;

use aleph_tee::tdx::collateral::TdxCollateral;
use aleph_tee::tdx::pcs::collateral_request;
use aleph_tee::tdx::quote::{TdxQuote, parse_tdx_quote};
use aleph_tee::tdx::verify::verify_tdx_quote;
use aleph_types::message::execution::environment::TdxRegisters;

pub use aleph_tee::tdx::pcs::PcsClient;
pub use aleph_tee::tdx::tcb::{TcbStatus, TdxTcbPolicy};

use super::x509::AttestError;
use super::{AttestationReport, TeeType};

/// The outcome of a verified TDX quote: genuine, at a TCB the policy
/// accepts, with the registers a caller pins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TdxVerificationResult {
    /// The pinned register set, hex, as the V-PROGRAM message declares it.
    pub registers: TdxRegisters,
    /// Intel's appraisal of the platform, already accepted by the policy.
    pub tcb_status: TcbStatus,
    /// The advisories the appraised TCB level carries (empty when UpToDate).
    pub advisory_ids: Vec<String>,
    pub summary: String,
}

/// Parse the quote out of a DTO, naming the TEE type on a mismatch.
pub(crate) fn parse_tdx_dto(dto: &AttestationReport) -> Result<TdxQuote, AttestError> {
    if dto.tee_type != TeeType::Tdx {
        return Err(AttestError::UnsupportedTeeType(dto.tee_type));
    }
    parse_tdx_quote(&dto.data).map_err(|e| AttestError::TdxParse(format!("{e:#}")))
}

/// The quote's registers in the message's hex shape.
pub(crate) fn registers_hex(registers: &aleph_tee::tdx::quote::TdxRegisters) -> TdxRegisters {
    TdxRegisters {
        mrtd: hex::encode(registers.mrtd),
        rtmr1: hex::encode(registers.rtmr1),
        rtmr2: hex::encode(registers.rtmr2),
        mrconfigid: hex::encode(registers.mrconfigid),
    }
}

/// Fully verify a TDX quote: collateral from `pcs`, then chain, signatures,
/// TCB appraisal and `policy`.
pub async fn verify_tdx_report(
    dto: &AttestationReport,
    policy: &TdxTcbPolicy,
    pcs: &PcsClient,
) -> Result<TdxVerificationResult, AttestError> {
    let quote = parse_tdx_dto(dto)?;
    let now = SystemTime::now();
    let request = collateral_request(&quote.signature.pck_chain_pem)
        .map_err(|e| AttestError::TdxCollateral(format!("{e:#}")))?;
    let collateral = pcs
        .fetch(&request, now)
        .await
        .map_err(|e| AttestError::TdxCollateral(format!("{e:#}")))?;
    verify_tdx_quote_with_collateral(&quote, &collateral, now, policy).map(|(result, _)| result)
}

/// The synchronous core of [`verify_tdx_report`], split out so tests run
/// on captured collateral with an injected clock. Also returns the signed
/// `report_data`, which the RA-TLS layer binds to the served key or nonce.
pub(crate) fn verify_tdx_quote_with_collateral(
    quote: &TdxQuote,
    collateral: &TdxCollateral,
    now: SystemTime,
    policy: &TdxTcbPolicy,
) -> Result<(TdxVerificationResult, [u8; 64]), AttestError> {
    let verified = verify_tdx_quote(quote, collateral, now, policy)
        .map_err(|e| AttestError::TdxVerification(format!("{e:#}")))?;
    let advisory_ids = verified.tcb.advisory_ids.clone();
    let summary = if advisory_ids.is_empty() {
        format!(
            "Intel TDX verified against PCS collateral (TCB {:?})",
            verified.tcb.status
        )
    } else {
        format!(
            "Intel TDX verified against PCS collateral (TCB {:?}, advisories {})",
            verified.tcb.status,
            advisory_ids.join(", ")
        )
    };
    Ok((
        TdxVerificationResult {
            registers: registers_hex(&verified.registers),
            tcb_status: verified.tcb.status,
            advisory_ids,
            summary,
        },
        verified.report_data,
    ))
}

#[cfg(test)]
pub(crate) mod test_support {
    //! The attested-TLS certificate aleph-vm's measured tdxImage runtime
    //! served on a Xeon 6731E (2026-09-29), the quote it embeds and the
    //! collateral PCCS served that day. Provenance in aleph-vm's
    //! `rust/crates/aleph-tee/tests/fixtures/tdx/README.md`.
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub(crate) const RATLS_CERT_PEM: &[u8] =
        include_bytes!("testdata/tdx/tdx_ratls_xeon6_cert.pem");
    pub(crate) const QUOTE: &[u8] = include_bytes!("testdata/tdx/tdx_quote_xeon6_ratls.bin");
    pub(crate) const COLLATERAL: &[u8] =
        include_bytes!("testdata/tdx/tdx_quote_xeon6_ratls_collateral.json");

    pub(crate) const MRTD: &str = "d4f5ee3d5fe9a5a3cbb1df8c40946714f55d5918b9b0e9ecd82a1d8adeea668495901baee134e3152dd5e0e2d1781262";
    pub(crate) const RTMR1: &str = "8d91abe1ea40a7dba9dbd110eea6fff8e3c79d983a7cae359a046ec8ae339cfbfbed4298c66ec2bdbbf08abb9c63e5c8";
    pub(crate) const RTMR2: &str = "c785503b238756732626c8162997f514084d10d699b602bba0691dcbb94a97901acc63c8aea322af4946141573d0766e";
    /// SHA-384 of the empty descriptor suffix.
    pub(crate) const MRCONFIGID: &str = "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b";

    /// Inside the collateral's validity windows: 2026-10-01T00:00:00Z.
    pub(crate) fn now() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_790_812_800)
    }

    /// The platform runs old firmware and appraises OutOfDate; the policy
    /// an operator would configure to talk to it.
    pub(crate) fn accepting_policy() -> super::TdxTcbPolicy {
        let mut policy = super::TdxTcbPolicy::default();
        policy.accepted_statuses.insert(super::TcbStatus::OutOfDate);
        policy
    }

    pub(crate) fn cert_der() -> Vec<u8> {
        x509_parser::pem::parse_x509_pem(RATLS_CERT_PEM)
            .expect("fixture PEM parses")
            .1
            .contents
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[test]
    fn verifies_the_captured_quote_once_out_of_date_is_admitted() {
        let dto = AttestationReport {
            tee_type: TeeType::Tdx,
            data: QUOTE.to_vec(),
        };
        let quote = parse_tdx_dto(&dto).unwrap();
        let collateral = TdxCollateral::from_json(COLLATERAL).unwrap();

        let err =
            verify_tdx_quote_with_collateral(&quote, &collateral, now(), &TdxTcbPolicy::default())
                .unwrap_err();
        assert!(matches!(err, AttestError::TdxVerification(_)), "{err}");
        assert!(err.to_string().contains("OutOfDate"), "{err}");

        let (result, report_data) =
            verify_tdx_quote_with_collateral(&quote, &collateral, now(), &accepting_policy())
                .unwrap();
        assert_eq!(result.tcb_status, TcbStatus::OutOfDate);
        assert_eq!(result.advisory_ids.len(), 8);
        assert_eq!(result.registers.mrtd, MRTD);
        assert_eq!(result.registers.rtmr1, RTMR1);
        assert_eq!(result.registers.rtmr2, RTMR2);
        assert_eq!(result.registers.mrconfigid, MRCONFIGID);
        assert!(
            result.summary.contains("INTEL-SA-01268"),
            "{}",
            result.summary
        );
        // The key-bound report_data the RA-TLS layer checks: 48 hash bytes
        // then zeros.
        assert_eq!(report_data[48..], [0u8; 16]);
        assert_ne!(report_data[..48], [0u8; 48]);
    }

    #[test]
    fn rejects_a_dto_declaring_another_tee_type() {
        let dto = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: QUOTE.to_vec(),
        };
        assert!(matches!(
            parse_tdx_dto(&dto),
            Err(AttestError::UnsupportedTeeType(TeeType::SevSnp))
        ));
        let dto = AttestationReport {
            tee_type: TeeType::Tdx,
            data: vec![0u8; 100],
        };
        assert!(matches!(parse_tdx_dto(&dto), Err(AttestError::TdxParse(_))));
    }

    #[test]
    fn expired_collateral_is_refused() {
        let quote = parse_tdx_quote(QUOTE).unwrap();
        let collateral = TdxCollateral::from_json(COLLATERAL).unwrap();
        let later = now() + std::time::Duration::from_secs(90 * 24 * 3600);
        let err = verify_tdx_quote_with_collateral(&quote, &collateral, later, &accepting_policy())
            .unwrap_err();
        assert!(matches!(err, AttestError::TdxVerification(_)), "{err}");
    }
}
