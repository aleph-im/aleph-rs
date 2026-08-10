//! Run compute_measurements on explicit artifacts + cmdline, print the digest.
//! Usage: measure_parity <dir-with OVMF.fd/bzImage/initrd> <cmdline> <vcpus>
fn main() {
    let dir = std::path::PathBuf::from(std::env::args().nth(1).expect("dir"));
    let cmdline = std::env::args().nth(2).expect("cmdline");
    let vcpus: u32 = std::env::args()
        .nth(3)
        .expect("vcpus")
        .parse()
        .expect("vcpus u32");
    let artifacts = aleph_sdk::vprogram::bundle::BundleArtifacts {
        ovmf: dir.join("OVMF.fd"),
        kernel: dir.join("bzImage"),
        initrd: dir.join("initrd"),
    };
    let m = aleph_sdk::vprogram::measure::compute_measurements(
        &artifacts,
        &cmdline,
        vcpus,
        &["EPYC-v4".into()],
    )
    .expect("measurement");
    println!("{}", m[0].digest);
}
