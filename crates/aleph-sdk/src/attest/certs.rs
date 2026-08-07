//! AMD Key Distribution Service (KDS) client for fetching VCEK certificates.
//!
//! Ported from aleph-cvm `crates/aleph-tee/src/sev_snp/certs.rs::fetch_vcek`,
//! adapted to return [`AttestError`] instead of `anyhow::Error` and to key the
//! on-disk cache off [`AmdProduct`] instead of a raw product string.
//!
//! ARK/ASK are *not* fetched here: they come from the `sev` crate's builtin,
//! bundled copies (see [`AmdProduct::builtin_ca`](super::verify::AmdProduct)),
//! so only the per-chip VCEK ever needs a network round trip.

use std::path::{Path, PathBuf};

use sev::firmware::host::TcbVersion;

use super::AttestError;
use super::verify::AmdProduct;

/// Base URL for AMD's Key Distribution Service VCEK endpoint.
const KDS_BASE_URL: &str = "https://kdsintf.amd.com/vcek/v1";

/// Return the cache directory for AMD KDS certificates.
///
/// Uses `$XDG_CACHE_HOME/aleph-sdk/kds` if set, otherwise
/// `$HOME/.cache/aleph-sdk/kds`. Returns `None` if neither is set, in which
/// case the cache is simply skipped (every call hits the network).
fn cache_dir() -> Option<PathBuf> {
    let base = std::env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .ok()
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|h| PathBuf::from(h).join(".cache"))
        })?;
    Some(base.join("aleph-sdk").join("kds"))
}

/// Read a cached certificate file, if it exists and is non-empty.
fn read_cached(path: &Path) -> Option<Vec<u8>> {
    match std::fs::read(path) {
        Ok(data) if !data.is_empty() => Some(data),
        _ => None,
    }
}

/// Best-effort write of certificate data to the cache; failures are ignored
/// since the cache is purely a rate-limiting optimization, not a correctness
/// requirement.
fn write_cache(path: &Path, data: &[u8]) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(path, data);
}

/// Fetch the VCEK (Versioned Chip Endorsement Key) certificate from AMD KDS.
///
/// `chip_id` and `reported_tcb` come straight off the parsed attestation
/// report (`report.chip_id`, `report.reported_tcb`): the VCEK is versioned by
/// exactly that (chip, TCB) pair, so requesting the wrong one would fail
/// downstream chain verification anyway.
///
/// Returns the VCEK certificate in DER format (this is what AMD KDS actually
/// serves at this endpoint). Results are cached on disk to avoid hitting
/// AMD's rate limiter on repeated requests for the same (chip, TCB).
pub async fn fetch_vcek(
    product: AmdProduct,
    chip_id: &[u8; 64],
    reported_tcb: &TcbVersion,
) -> Result<Vec<u8>, AttestError> {
    let chip_id_hex = hex::encode(chip_id);
    let product_name = product.kds_name();

    let cache_path = cache_dir().map(|dir| {
        dir.join(product_name).join(format!(
            "vcek_{chip_id_hex}_{}_{}_{}_{}.der",
            reported_tcb.bootloader, reported_tcb.tee, reported_tcb.snp, reported_tcb.microcode
        ))
    });
    if let Some(path) = cache_path.as_deref()
        && let Some(data) = read_cached(path)
    {
        return Ok(data);
    }

    let url = format!(
        "{KDS_BASE_URL}/{product_name}/{chip_id_hex}?blSPL={}&teeSPL={}&snpSPL={}&ucodeSPL={}",
        reported_tcb.bootloader, reported_tcb.tee, reported_tcb.snp, reported_tcb.microcode
    );

    let response = reqwest::get(&url)
        .await
        .map_err(|e| AttestError::Kds(format!("failed to fetch VCEK from {url}: {e}")))?;

    let status = response.status();
    if !status.is_success() {
        return Err(AttestError::Kds(format!(
            "AMD KDS returned HTTP {status} for VCEK request: {url}"
        )));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| AttestError::Kds(format!("failed to read VCEK response body: {e}")))?;

    let data = bytes.to_vec();
    if let Some(path) = cache_path.as_deref() {
        write_cache(path, &data);
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_dir_prefers_xdg_cache_home() {
        // SAFETY: single-threaded test process env mutation; no other test
        // in this module reads these vars concurrently.
        unsafe {
            std::env::set_var("XDG_CACHE_HOME", "/tmp/xdg-cache-test");
        }
        let dir = cache_dir().expect("XDG_CACHE_HOME is set");
        assert_eq!(dir, PathBuf::from("/tmp/xdg-cache-test/aleph-sdk/kds"));
        unsafe {
            std::env::remove_var("XDG_CACHE_HOME");
        }
    }

    #[test]
    fn read_cached_ignores_missing_and_empty_files() {
        assert!(read_cached(Path::new("/nonexistent/path/to/nothing")).is_none());

        let dir = std::env::temp_dir().join("aleph-sdk-certs-test-empty");
        std::fs::create_dir_all(&dir).unwrap();
        let empty_path = dir.join("empty.der");
        std::fs::write(&empty_path, []).unwrap();
        assert!(read_cached(&empty_path).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_then_read_cache_round_trips() {
        let dir = std::env::temp_dir().join("aleph-sdk-certs-test-roundtrip");
        let path = dir.join("vcek.der");
        write_cache(&path, &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(read_cached(&path), Some(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
