use std::path::PathBuf;

use crate::error::{MicrovmError, Result};

/// Find an executable on PATH. Returns its full path if present and executable.
pub fn locate_binary(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// M1 preflight: firecracker present, /dev/kvm accessible. Returns the firecracker path.
pub fn check(firecracker_name: &str) -> Result<PathBuf> {
    let fc = locate_binary(firecracker_name).ok_or(MicrovmError::FirecrackerMissing)?;
    check_kvm()?;
    Ok(fc)
}

fn check_kvm() -> Result<()> {
    use std::fs::OpenOptions;
    match OpenOptions::new().read(true).write(true).open("/dev/kvm") {
        Ok(_) => Ok(()),
        Err(e) => Err(MicrovmError::KvmUnavailable(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_binary_is_reported() {
        // A name that will not be on PATH.
        assert!(locate_binary("definitely-not-a-real-binary-xyz").is_none());
    }
}
