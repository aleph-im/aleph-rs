//! Password sourcing for encrypted (keystore) accounts.
//!
//! Order: `ALEPH_PASSWORD` env var, then interactive prompt (up to 3
//! attempts), else a hard error when no terminal is available.

use super::keystore::{self, KeystoreError, KeystoreV3};
use anyhow::{Result, bail};
use zeroize::Zeroizing;

pub const PASSWORD_ENV_VAR: &str = "ALEPH_PASSWORD";
const MAX_ATTEMPTS: u32 = 3;

fn validate_new_password(password: &str) -> Result<()> {
    if password.is_empty() {
        bail!("password must not be empty");
    }
    Ok(())
}

/// Prompt for a password on the controlling terminal.
///
/// rpassword talks to /dev/tty (or the Windows console) directly, so this
/// works even when stdin/stdout are redirected. When no terminal is
/// available at all (headless environments), the prompt fails and we point
/// the user at the env var instead.
fn prompt_password(prompt: String) -> Result<Zeroizing<String>> {
    match rpassword::prompt_password(prompt) {
        Ok(p) => Ok(Zeroizing::new(p)),
        Err(e) => bail!(
            "failed to read password from the terminal ({e}); \
             set the {PASSWORD_ENV_VAR} environment variable for non-interactive use"
        ),
    }
}

/// Read a new password for encrypting a key: from `ALEPH_PASSWORD` if set,
/// otherwise prompt twice and require both entries to match.
pub fn read_new_password() -> Result<Zeroizing<String>> {
    // The String returned by env::var is moved (not copied) into Zeroizing,
    // so no unwiped heap copy is left behind.
    if let Ok(p) = std::env::var(PASSWORD_ENV_VAR) {
        let p = Zeroizing::new(p);
        validate_new_password(&p)?;
        return Ok(p);
    }
    let first = prompt_password("Enter password: ".to_string())?;
    validate_new_password(&first)?;
    let second = prompt_password("Confirm password: ".to_string())?;
    if *first != *second {
        bail!("passwords do not match");
    }
    Ok(first)
}

/// Decrypt a keystore, sourcing the password from `ALEPH_PASSWORD` or an
/// interactive prompt. `label` names the account in prompts and errors.
pub fn unlock_keystore(ks: &KeystoreV3, label: &str) -> Result<Zeroizing<[u8; 32]>> {
    if let Ok(p) = std::env::var(PASSWORD_ENV_VAR) {
        let p = Zeroizing::new(p);
        return match keystore::decrypt_key(ks, &p) {
            Ok(key) => Ok(key),
            Err(KeystoreError::IncorrectPassword) => {
                bail!("incorrect password for account '{label}' (from {PASSWORD_ENV_VAR})")
            }
            Err(e) => Err(e.into()),
        };
    }
    for attempt in 1..=MAX_ATTEMPTS {
        let p = prompt_password(format!("Password for account '{label}': "))?;
        match keystore::decrypt_key(ks, &p) {
            Ok(key) => return Ok(key),
            Err(KeystoreError::IncorrectPassword) if attempt < MAX_ATTEMPTS => {
                eprintln!("Incorrect password, try again.");
            }
            Err(KeystoreError::IncorrectPassword) => {
                bail!("incorrect password for account '{label}'")
            }
            Err(e) => return Err(e.into()),
        }
    }
    unreachable!("loop returns or bails on the last attempt")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_password_rejected() {
        assert!(validate_new_password("").is_err());
    }

    #[test]
    fn nonempty_password_accepted() {
        assert!(validate_new_password("hunter2").is_ok());
    }
}
