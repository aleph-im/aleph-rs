use anyhow::Result;
use aleph_sdk::client::AlephClient;
use url::Url;

use crate::cli::AdminCommand;

pub mod images;
pub mod vm_images_diff;
pub mod vm_images_mutate;

pub(crate) fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "y" | "yes"
    )
}

pub(crate) fn admin_env_enabled() -> bool {
    let value = std::env::var("ALEPH_ADMIN").unwrap_or_default();
    is_truthy(&value)
}

pub(crate) fn require_admin_env() -> Result<()> {
    if !admin_env_enabled() {
        anyhow::bail!(
            "the `admin` subcommand requires ALEPH_ADMIN=1 in the environment.\n\
             This is a runtime guard against accidental admin operations; \
             the binary was built with the `admin` cargo feature."
        );
    }
    Ok(())
}

pub async fn handle_admin_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: AdminCommand,
) -> Result<()> {
    require_admin_env()?;
    match command {
        AdminCommand::Images { command } => {
            images::handle_images_command(aleph_client, ccn_url, json, command).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_truthy;

    #[test]
    fn is_truthy_accepts_canonical_forms() {
        assert!(is_truthy("1"));
        assert!(is_truthy("true"));
        assert!(is_truthy("y"));
        assert!(is_truthy("yes"));
    }

    #[test]
    fn is_truthy_accepts_uppercase_variants() {
        assert!(is_truthy("TRUE"));
        assert!(is_truthy("Y"));
        assert!(is_truthy("YES"));
        assert!(is_truthy("True"));
    }

    #[test]
    fn is_truthy_rejects_other_values() {
        assert!(!is_truthy(""));
        assert!(!is_truthy("0"));
        assert!(!is_truthy("false"));
        assert!(!is_truthy("no"));
        assert!(!is_truthy("n"));
        assert!(!is_truthy("asdf"));
        assert!(!is_truthy("2"));
    }

    #[test]
    fn is_truthy_trims_whitespace() {
        assert!(is_truthy(" 1 "));
        assert!(is_truthy("\ttrue\n"));
        assert!(!is_truthy("  asdf  "));
    }
}
