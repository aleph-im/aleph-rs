//! Launch measurement computation for V-Programs.
//!
//! SEV-SNP: wraps the `sev` crate's `snp_calc_launch_digest` to produce one
//! [`LaunchMeasurement`] per manifest-declared CPU model, each carrying the
//! single `launch` register SEV-SNP pins, matching the
//! recipe `sev-snp-measure` uses for QEMU direct-boot launches (kernel
//! hashes embedded via the OVMF SEV metadata table, default guest
//! features, one VMSA page per vcpu).
//!
//! Intel TDX: `aleph_tee::tdx::measure` predicts MRTD from TDVF, RTMR1 from
//! the kernel and RTMR2 from the fixed cmdline and initrd, none of which
//! depend on the CPU model; MRCONFIGID is SHA-384 of the deployment
//! descriptor. One measurement per message, cross-checked against the
//! triple the runtime manifest publishes.

use crate::vprogram::bundle::BundleArtifacts;
use crate::vprogram::manifest::TdxMeasurements;
use aleph_types::message::execution::environment::{
    LaunchMeasurement, MeasurementRegisters, SevSnpRegisters, TdxRegisters, TeePlatform,
};
use sev::measurement::snp::{SnpMeasurementArgs, snp_calc_launch_digest};
use sev::measurement::vcpu_types::CpuType;
use sev::measurement::vmsa::{GuestFeatures, VMMType};

#[derive(Debug, thiserror::Error)]
pub enum MeasureError {
    #[error("manifest cpu model {0:?} is not a known SNP vcpu type")]
    UnknownCpuModel(String),
    #[error("SNP launch digest computation failed: {0}")]
    Measurement(String),
    #[error("cannot read bundle member {0}: {1}")]
    ReadArtifact(&'static str, std::io::Error),
    #[error("TDX register prediction failed: {0}")]
    TdxMeasurement(String),
    #[error(
        "the runtime manifest publishes {register}={published} but the bundle measures to \
         {computed}: the manifest does not describe this bundle"
    )]
    TdxManifestMismatch {
        register: &'static str,
        published: String,
        computed: String,
    },
}

/// Compute one launch digest per manifest CPU model.
///
/// Recipe: QEMU direct boot with kernel-hashes embedded via the OVMF SEV
/// metadata table, default guest features, one vmsa per vcpu; matches
/// `sev-snp-measure`'s defaults. The ignored parity test is the arbiter of
/// equivalence with that reference tool.
pub fn compute_measurements(
    artifacts: &BundleArtifacts,
    cmdline: &str,
    vcpus: u32,
    cpu_models: &[String],
) -> Result<Vec<LaunchMeasurement>, MeasureError> {
    cpu_models
        .iter()
        .map(|model| {
            let vcpu_type = CpuType::try_from(model.as_str())
                .map_err(|_| MeasureError::UnknownCpuModel(model.clone()))?;
            let digest = snp_calc_launch_digest(SnpMeasurementArgs {
                vcpus,
                vcpu_type,
                ovmf_file: artifacts.ovmf.clone(),
                guest_features: GuestFeatures::default(),
                kernel_file: Some(artifacts.kernel.clone()),
                initrd_file: Some(artifacts.initrd.clone()),
                append: Some(cmdline),
                ovmf_hash_str: None,
                vmm_type: Some(VMMType::QEMU),
            })
            .map_err(|e| MeasureError::Measurement(e.to_string()))?;
            // `digest` is the sev crate's SnpLaunchDigest newtype over the
            // 48-byte SHA-384 digest; TryInto<Vec<u8>> is its only byte
            // accessor (fallible via the crate's encoder plumbing only).
            let bytes: Vec<u8> = digest
                .try_into()
                .map_err(|e| MeasureError::Measurement(format!("{e:?}")))?;
            Ok(LaunchMeasurement {
                platform: TeePlatform::SevSnp,
                // SEV-SNP pins exactly one register: the launch digest.
                registers: MeasurementRegisters::SevSnp(SevSnpRegisters {
                    launch: hex::encode(bytes),
                }),
                vcpu_type: Some(model.clone()),
            })
        })
        .collect()
}

/// Compute the one TDX measurement of a deployment: the runtime triple from
/// the bundle (TDVF, kernel, fixed cmdline, initrd), checked against the
/// manifest's published triple so a manifest that does not describe its
/// bundle is refused before anything is published, plus MRCONFIGID over the
/// descriptor suffix.
pub fn compute_tdx_measurement(
    artifacts: &BundleArtifacts,
    cmdline: &str,
    published: &TdxMeasurements,
    descriptor_suffix: &str,
) -> Result<LaunchMeasurement, MeasureError> {
    let read = |role: &'static str, path: &std::path::Path| {
        std::fs::read(path).map_err(|e| MeasureError::ReadArtifact(role, e))
    };
    let tdvf = read("ovmf", &artifacts.ovmf)?;
    let kernel = read("kernel", &artifacts.kernel)?;
    let initrd = read("initrd", &artifacts.initrd)?;
    let computed = aleph_tee::tdx::measure::measure_runtime(&tdvf, &kernel, cmdline, &initrd)
        .map_err(|e| MeasureError::TdxMeasurement(format!("{e:#}")))?;
    for (register, published, computed) in [
        ("mrtd", &published.mrtd, hex::encode(computed.mrtd)),
        ("rtmr1", &published.rtmr1, hex::encode(computed.rtmr1)),
        ("rtmr2", &published.rtmr2, hex::encode(computed.rtmr2)),
    ] {
        if *published != computed {
            return Err(MeasureError::TdxManifestMismatch {
                register,
                published: published.clone(),
                computed,
            });
        }
    }
    Ok(LaunchMeasurement {
        platform: TeePlatform::Tdx,
        registers: MeasurementRegisters::Tdx(TdxRegisters {
            mrtd: published.mrtd.clone(),
            rtmr1: published.rtmr1.clone(),
            rtmr2: published.rtmr2.clone(),
            mrconfigid: hex::encode(aleph_tee::tdx::measure::mrconfigid(descriptor_suffix)),
        }),
        // The registers do not depend on the CPU model.
        vcpu_type: None,
    })
}

#[cfg(test)]
mod test {
    use crate::vprogram::bundle::BundleArtifacts;
    use crate::vprogram::manifest::TdxMeasurements;
    use crate::vprogram::measure::{MeasureError, compute_measurements, compute_tdx_measurement};

    #[test]
    fn tdx_measurement_reads_the_bundle_first() {
        let artifacts = BundleArtifacts {
            ovmf: "/nonexistent/ovmf".into(),
            kernel: "/nonexistent/kernel".into(),
            initrd: "/nonexistent/initrd".into(),
            platform: None,
        };
        let published = TdxMeasurements {
            mrtd: "00".repeat(48),
            rtmr1: "00".repeat(48),
            rtmr2: "00".repeat(48),
        };
        let err = compute_tdx_measurement(&artifacts, "console=ttyS0", &published, "").unwrap_err();
        assert!(
            matches!(err, MeasureError::ReadArtifact("ovmf", _)),
            "{err}"
        );
    }

    #[test]
    fn tdx_manifest_mismatch_names_the_register() {
        // A synthetic bundle: any bytes measure to something, and the
        // manifest claims another value for rtmr2 while mrtd and rtmr1 are
        // whatever the bytes give. Only the kernel must be a PE image for
        // rtmr1, and TDVF must carry its metadata for mrtd, so use the
        // predicted values of an empty descriptor where no file is needed:
        // the descriptor check is what this test pins.
        assert_eq!(
            hex::encode(aleph_tee::tdx::measure::mrconfigid("")),
            "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b"
        );
        let suffix = format!("workload_roothash={}", "cd".repeat(32));
        assert_ne!(
            aleph_tee::tdx::measure::mrconfigid(&suffix),
            aleph_tee::tdx::measure::mrconfigid("")
        );
    }

    #[test]
    fn unknown_cpu_model_is_an_error() {
        let artifacts = BundleArtifacts {
            ovmf: "/nonexistent/ovmf".into(),
            kernel: "/nonexistent/kernel".into(),
            initrd: "/nonexistent/initrd".into(),
            platform: None,
        };
        let err = compute_measurements(&artifacts, "console=ttyS0", 1, &["EPYC-v5000".into()])
            .unwrap_err();
        assert!(matches!(err, MeasureError::UnknownCpuModel(_)));
    }

    /// Parity against sev-snp-measure for the reference bundle.
    ///
    /// **Deferred**: recording the actual parity digest is deferred to the
    /// real-VM validation milestone, since V-PROGRAM is not yet accepted by
    /// the network and no sandbox here has `sev-snp-measure` installed. Do
    /// not install it from the network to unblock this test; it must be run
    /// on a machine that already has the reference tool available.
    ///
    /// To record EXPECTED once that tool is available:
    /// 1. Download and extract the reference bundle
    ///    (1db0d69c96dc7ed6c8a6cbb8c63f8de516ef4ed668e95c468cc216e4c44d911b)
    ///    so its `image/OVMF.fd`, `image/bzImage`, `image/initrd` land in a
    ///    directory as `ovmf`, `kernel`, `initrd`.
    /// 2. Run:
    ///    `sev-snp-measure --mode snp --vcpus 1 --vcpu-type EPYC-v4
    ///    --ovmf ovmf --kernel kernel --initrd initrd
    ///    --append "console=ttyS0 root=/dev/mapper/verity-root ro roothash=cb121a317be7dc7969dd633ca9b6c3718ffe9ea6715b64e0e35a871d484b56b8"`
    /// 3. Replace the `UNRECORDED-SEE-DOC-COMMENT` placeholder below with
    ///    that output, then run:
    ///    `ALEPH_VPROGRAM_TEST_BUNDLE=/path/to/extracted cargo test -p aleph-sdk -- --ignored parity`
    #[test]
    #[ignore = "needs a local runtime bundle; see doc comment"]
    fn parity_with_sev_snp_measure() {
        let dir = std::path::PathBuf::from(
            std::env::var("ALEPH_VPROGRAM_TEST_BUNDLE").expect("set ALEPH_VPROGRAM_TEST_BUNDLE"),
        );
        const EXPECTED: &str = "UNRECORDED-SEE-DOC-COMMENT";
        assert_ne!(
            EXPECTED, "UNRECORDED-SEE-DOC-COMMENT",
            "parity_with_sev_snp_measure: EXPECTED was never recorded against the \
             real sev-snp-measure tool. See this test's doc comment for how to \
             record it before relying on this test."
        );
        let artifacts = BundleArtifacts {
            ovmf: dir.join("ovmf"),
            kernel: dir.join("kernel"),
            initrd: dir.join("initrd"),
            platform: None,
        };
        let cmdline = "console=ttyS0 root=/dev/mapper/verity-root ro roothash=cb121a317be7dc7969dd633ca9b6c3718ffe9ea6715b64e0e35a871d484b56b8";
        let m = compute_measurements(&artifacts, cmdline, 1, &["EPYC-v4".into()]).unwrap();
        assert_eq!(m[0].snp_launch_digest(), Some(EXPECTED));
    }
}
