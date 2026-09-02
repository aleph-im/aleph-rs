//! SNP instance runtime cmdline template instantiation.
//!
//! Produces LUKS boot cmdlines from templates by replacing the {owner} placeholder.

#[derive(Debug, thiserror::Error)]
pub enum InstanceCmdlineError {
    #[error(
        "instance runtime v1 requires an EVM owner address (0x followed by 40 hex digits), got {0:?}"
    )]
    NotEvmOwner(String),
    #[error("runtime cmdline template has no {{owner}} slot")]
    NoOwnerSlot,
    #[error(
        "cmdline template contains a placeholder not defined by aleph-instance-runtime/1: {{{0}}}"
    )]
    UnresolvedPlaceholder(String),
}

/// Normalize and validate an EVM owner address.
///
/// Lowercases the address and validates that it is a valid EVM address
/// (0x prefix followed by exactly 40 hexadecimal digits).
pub fn normalize_evm_owner(owner: &str) -> Result<String, InstanceCmdlineError> {
    let lowercased = owner.to_lowercase();

    // Check for 0x prefix and total length of 42 chars (0x + 40 hex)
    if !lowercased.starts_with("0x") || lowercased.len() != 42 {
        return Err(InstanceCmdlineError::NotEvmOwner(owner.to_string()));
    }

    // Check that remaining 40 chars are all hex digits
    let hex_part = &lowercased[2..];
    if !hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(InstanceCmdlineError::NotEvmOwner(owner.to_string()));
    }

    Ok(lowercased)
}

/// Instantiate a SNP instance runtime cmdline template.
///
/// Replaces the {owner} placeholder with the normalized owner address.
/// Rejects any other unresolved placeholders.
pub fn instantiate_instance_cmdline(
    template: &str,
    owner: &str,
) -> Result<String, InstanceCmdlineError> {
    // Validate that the template contains the {owner} slot
    if !template.contains("{owner}") {
        return Err(InstanceCmdlineError::NoOwnerSlot);
    }

    // Normalize and validate the owner address
    let normalized_owner = normalize_evm_owner(owner)?;

    // Replace the {owner} placeholder
    let out = template.replace("{owner}", &normalized_owner);

    // Check for any remaining placeholders
    if let Some(start) = out.find('{') {
        if let Some(relative_end) = out[start..].find('}') {
            let end = start + relative_end;
            return Err(InstanceCmdlineError::UnresolvedPlaceholder(
                out[start + 1..end].to_string(),
            ));
        } else {
            return Err(InstanceCmdlineError::UnresolvedPlaceholder(
                out[start + 1..].to_string(),
            ));
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: &str = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";

    #[test]
    fn instantiates_and_lowercases_the_owner() {
        let out =
            instantiate_instance_cmdline("console=ttyS0 luks=1 owner={owner}", OWNER).unwrap();
        assert_eq!(
            out,
            "console=ttyS0 luks=1 owner=0x9319ad3b7a8e0ee24f2e639c40d8ed124c5520ba"
        );
    }

    #[test]
    fn rejects_non_evm_owners() {
        for bad in [
            "",
            "0x123",
            "9319ad3b",
            "0xZZ19ad3b7a8e0ee24f2e639c40d8ed124c5520ba",
        ] {
            assert!(matches!(
                instantiate_instance_cmdline("owner={owner}", bad),
                Err(InstanceCmdlineError::NotEvmOwner(_))
            ));
        }
    }

    #[test]
    fn rejects_template_without_owner_slot() {
        assert!(matches!(
            instantiate_instance_cmdline("console=ttyS0 luks=1", OWNER),
            Err(InstanceCmdlineError::NoOwnerSlot)
        ));
    }

    #[test]
    fn rejects_leftover_placeholders() {
        assert!(matches!(
            instantiate_instance_cmdline("owner={owner} x={verified_volumes}", OWNER),
            Err(InstanceCmdlineError::UnresolvedPlaceholder(_))
        ));
    }
}
