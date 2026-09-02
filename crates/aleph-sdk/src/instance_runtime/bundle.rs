//! Runtime bundle fetching for SEV-SNP confidential instances.
//!
//! Thin adapter over `crate::vprogram::bundle`'s download/verify/extract
//! machinery: an [`InstanceRuntimeManifest`]'s bundle fields map onto a
//! `BundleSource`, the same generalized shape the vprogram flavor's manifest
//! maps onto, so both flavors share one fetch implementation.

use std::path::Path;

use crate::client::AlephClient;
use crate::instance_runtime::manifest::InstanceRuntimeManifest;
use crate::vprogram::bundle::{
    BundleArtifacts, BundleError, BundleSource, fetch_bundle_artifacts_from,
};

/// Map an [`InstanceRuntimeManifest`]'s bundle fields onto a `BundleSource`.
pub fn bundle_source(manifest: &InstanceRuntimeManifest) -> BundleSource {
    BundleSource {
        reference: manifest.bundle.reference.clone(),
        sha256: manifest.bundle.sha256.clone(),
        size: manifest.bundle.size,
        ovmf: manifest.bundle.members.ovmf.clone(),
        kernel: manifest.bundle.members.kernel.clone(),
        initrd: manifest.bundle.members.initrd.clone(),
    }
}

/// Fetch, verify, and extract the runtime bundle referenced by `manifest`.
///
/// Delegates to `fetch_bundle_artifacts_from` with a `BundleSource` built
/// from `manifest`'s bundle fields; see that function for caching and
/// verification behavior.
pub async fn fetch_instance_bundle_artifacts(
    client: &AlephClient,
    manifest: &InstanceRuntimeManifest,
    cache_dir: &Path,
) -> Result<BundleArtifacts, BundleError> {
    fetch_bundle_artifacts_from(client, &bundle_source(manifest), cache_dir).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance_runtime::manifest::InstanceRuntimeManifest;

    #[test]
    fn bundle_source_maps_the_manifest_fields() {
        let h = "ab".repeat(32);
        let json = format!(
            r#"{{"format": "aleph-instance-runtime", "format_version": 1,
                 "name": "n", "version": "v", "platform": "sev_snp",
                 "bundle": {{"ref": "{h}", "sha256": "{h}", "size": 7,
                             "members": {{"ovmf": "fw/ovmf.fd", "kernel": "bzImage", "initrd": "initrd.img"}}}},
                 "boot": {{"method": "qemu-direct-kernel", "kernel_hashes": true,
                           "cpu_models": ["EPYC-v4"],
                           "cmdline_template": "console=ttyS0 luks=1 owner={{owner}}"}},
                 "attestation": [{{"protocol": "aleph.ra-tls", "version": "1",
                                   "transport": {{"type": "tcp", "port": 8443}}}}],
                 "source": {{"repo": "r", "rev": "v", "build": "b"}}}}"#
        );
        let m = InstanceRuntimeManifest::parse(json.as_bytes()).unwrap();
        let s = bundle_source(&m);
        assert_eq!(s.reference, h);
        assert_eq!(s.sha256, h);
        assert_eq!(s.size, 7);
        assert_eq!(s.ovmf, "fw/ovmf.fd");
        assert_eq!(s.kernel, "bzImage");
        assert_eq!(s.initrd, "initrd.img");
    }
}
