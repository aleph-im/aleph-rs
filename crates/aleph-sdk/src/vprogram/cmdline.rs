//! V-Program runtime cmdline template instantiation.
//!
//! Produces kernel boot cmdlines from templates by replacing placeholders defined
//! by aleph-vprogram-runtime/1.

#[derive(Debug, thiserror::Error)]
pub enum CmdlineError {
    #[error(
        "runtime cmdline template has no {{workload_roothash}} slot: this runtime does not support workloads yet"
    )]
    NoWorkloadSlot,
    #[error(
        "message declares verified volumes but the runtime cmdline template has no {{verified_volumes}} slot"
    )]
    NoVerifiedVolumesSlot,
    #[error(
        "cmdline template contains a placeholder not defined by aleph-vprogram-runtime/1: {{{0}}}"
    )]
    UnknownPlaceholder(String),
}

/// Instantiate an aleph-vprogram-runtime/1 cmdline template.
///
/// Strict by design (the "no smuggled cmdline" rule): only the format-defined
/// placeholders are legal, and the whole space-delimited token carrying
/// {verified_volumes} is dropped when the message declares no extra volumes.
pub fn instantiate_cmdline(
    template: &str,
    platform_roothash: &str,
    workload_roothash: &str,
    volume_roothashes: &[String],
) -> Result<String, CmdlineError> {
    if !template.contains("{workload_roothash}") {
        return Err(CmdlineError::NoWorkloadSlot);
    }
    let has_volumes_slot = template.contains("{verified_volumes}");
    if !volume_roothashes.is_empty() && !has_volumes_slot {
        return Err(CmdlineError::NoVerifiedVolumesSlot);
    }

    let tokens: Vec<String> = template
        .split(' ')
        .filter(|token| !(volume_roothashes.is_empty() && token.contains("{verified_volumes}")))
        .map(str::to_owned)
        .collect();
    let mut out = tokens.join(" ");
    out = out.replace("{platform_roothash}", platform_roothash);
    out = out.replace("{workload_roothash}", workload_roothash);
    out = out.replace("{verified_volumes}", &volume_roothashes.join(","));

    if let Some(start) = out.find('{') {
        if let Some(relative_end) = out[start..].find('}') {
            let end = start + relative_end;
            return Err(CmdlineError::UnknownPlaceholder(
                out[start + 1..end].to_string(),
            ));
        } else {
            return Err(CmdlineError::UnknownPlaceholder(
                out[start + 1..].to_string(),
            ));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: &str = "console=ttyS0 root=/dev/mapper/verity-root ro roothash={platform_roothash} workload_roothash={workload_roothash} verified_volumes={verified_volumes}";

    #[test]
    fn fills_all_slots() {
        let out = instantiate_cmdline(T, "aa", "bb", &["h1".into(), "h2".into()]).unwrap();
        assert_eq!(
            out,
            "console=ttyS0 root=/dev/mapper/verity-root ro roothash=aa workload_roothash=bb verified_volumes=h1,h2"
        );
    }

    #[test]
    fn drops_verified_volumes_token_when_empty() {
        let out = instantiate_cmdline(T, "aa", "bb", &[]).unwrap();
        assert_eq!(
            out,
            "console=ttyS0 root=/dev/mapper/verity-root ro roothash=aa workload_roothash=bb"
        );
    }

    #[test]
    fn volumes_without_slot_is_an_error() {
        let t = "roothash={platform_roothash} workload_roothash={workload_roothash}";
        assert!(matches!(
            instantiate_cmdline(t, "aa", "bb", &["h1".into()]).unwrap_err(),
            CmdlineError::NoVerifiedVolumesSlot
        ));
        // no volumes, no slot: fine
        assert!(instantiate_cmdline(t, "aa", "bb", &[]).is_ok());
    }

    #[test]
    fn missing_workload_slot_is_an_error() {
        let t = "roothash={platform_roothash}";
        assert!(matches!(
            instantiate_cmdline(t, "aa", "bb", &[]).unwrap_err(),
            CmdlineError::NoWorkloadSlot
        ));
    }

    #[test]
    fn unknown_placeholder_is_an_error() {
        let t = "roothash={platform_roothash} workload_roothash={workload_roothash} ip={guest_ip}";
        match instantiate_cmdline(t, "aa", "bb", &[]).unwrap_err() {
            CmdlineError::UnknownPlaceholder(p) => assert_eq!(p, "guest_ip"),
            other => panic!("unexpected: {other}"),
        }
    }

    #[test]
    fn stray_brace_before_unknown_placeholder() {
        let t = "workload_roothash={workload_roothash} label}=x ip={guest_ip}";
        match instantiate_cmdline(t, "aa", "bb", &[]).unwrap_err() {
            CmdlineError::UnknownPlaceholder(p) => assert_eq!(p, "guest_ip"),
            other => panic!("unexpected: {other}"),
        }
    }
}
