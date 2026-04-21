//! Interactive `-i` resolver for `aleph instance create`.
//!
//! Runs before the normal build/submit path and fills in any `InstanceCreateArgs`
//! fields not already provided on the command line. Prompts, in order:
//! image → size → CRN → name → SSH public key path. CRN selection always runs
//! (the instance gets pinned to the chosen CRN via `node_hash`).

use crate::cli::{IMAGE_PRESETS, InstanceCreateArgs, parse_image};
use crate::commands::instance::validate_ssh_pubkey;
use aleph_sdk::client::{AlephAggregateClient, AlephClient};
use aleph_sdk::crns_list::{
    CrnFilter, CrnListEntry, CrnListResponse, DEFAULT_CRN_LIST_URL, fetch_crns_list,
};
use aleph_types::item_hash::ItemHash;
use dialoguer::{Confirm, FuzzySelect, Input, Select};
use std::cmp::Ordering;
use tokio::task::JoinHandle;

pub async fn resolve_interactive(
    args: &mut InstanceCreateArgs,
    aleph_client: &AlephClient,
) -> Result<(), Box<dyn std::error::Error>> {
    // Kick off the CRN list fetch in parallel with the early prompts.
    let crn_list_fut = spawn_crn_list_fetch();

    if args.image.is_none() {
        args.image = Some(prompt_image()?);
    }
    if args.size.is_none() && args.disk_size.is_none() {
        args.size = Some(prompt_size(aleph_client).await?);
    }

    let crn_list = crn_list_fut
        .await
        .map_err(|e| format!("background task error: {e}"))??;
    let (vcpus, memory_mib, disk_mib) = resolve_specs_for_filter(args, aleph_client).await?;
    let filter = CrnFilter {
        ipv6: true,
        min_vcpus: Some(vcpus),
        min_memory_mib: Some(memory_mib),
        min_disk_mib: Some(disk_mib),
        confidential: args.confidential,
        gpu: args.gpu.is_some(),
    };
    let filtered = crn_list.filter(&filter);
    if filtered.is_empty() {
        return Err(format!(
            "No CRN matches the requirements (vcpus={}, memory_mib={}, disk_mib={}, confidential={}, gpu={}). \
             Try a smaller size or wait for capacity.",
            vcpus, memory_mib, disk_mib, filter.confidential, filter.gpu
        ).into());
    }
    let chosen = prompt_crn(&filtered)?;
    accept_terms_and_conditions(aleph_client, chosen).await?;
    args.crn_hash = Some(chosen.hash.clone());

    if args.name.is_none() {
        args.name = prompt_name_optional()?;
    }
    if args.ssh_pubkey_file.is_empty() {
        args.ssh_pubkey_file = vec![prompt_ssh_pubkey_path()?];
    }

    Ok(())
}

async fn prompt_size(aleph_client: &AlephClient) -> Result<String, Box<dyn std::error::Error>> {
    let pricing = aleph_client
        .get_pricing_aggregate()
        .await
        .map_err(|e| format!("failed to fetch pricing tiers: {e}"))?;
    let instance_pricing = &pricing.pricing.instance;

    let mut tiers: Vec<_> = instance_pricing
        .tiers
        .iter()
        .filter(|t| t.model.is_none())
        .collect();
    tiers.sort_by_key(|t| t.compute_units);

    if tiers.is_empty() {
        return Err("no instance tiers available in the pricing aggregate".into());
    }

    let cu = &instance_pricing.compute_unit;
    let items: Vec<String> = tiers
        .iter()
        .map(|t| {
            let slug = instance_pricing.tier_slug(t);
            let vcpus = t.compute_units * cu.vcpus;
            let memory_mib = t.compute_units as u64 * cu.memory_mib;
            let disk_mib = t.compute_units as u64 * cu.disk_mib;
            format!(
                "{:<14}  {} vCPU · {} MiB RAM · {} MiB disk",
                slug, vcpus, memory_mib, disk_mib,
            )
        })
        .collect();

    let idx = Select::new()
        .with_prompt("Size")
        .items(&items)
        .default(0)
        .interact()?;

    Ok(instance_pricing.tier_slug(tiers[idx]))
}

fn crn_list_url() -> Result<url::Url, Box<dyn std::error::Error>> {
    let raw =
        std::env::var("ALEPH_CRN_LIST_URL").unwrap_or_else(|_| DEFAULT_CRN_LIST_URL.to_string());
    Ok(url::Url::parse(&raw)?)
}

fn spawn_crn_list_fetch() -> JoinHandle<Result<CrnListResponse, String>> {
    tokio::spawn(async move {
        let url = crn_list_url().map_err(|e| e.to_string())?;
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| e.to_string())?;
        fetch_crns_list(&http, &url, true)
            .await
            .map_err(|e| format!("failed to fetch CRN list from {url}: {e}"))
    })
}

/// Resolve (vcpus, memory_mib, disk_mib) for the CRN filter.
/// Uses the size tier if set, otherwise flag values with defaults matching `handle_instance_create`.
async fn resolve_specs_for_filter(
    args: &InstanceCreateArgs,
    aleph_client: &AlephClient,
) -> Result<(u32, u64, u64), Box<dyn std::error::Error>> {
    if let Some(slug) = &args.size {
        let pricing = aleph_client.get_pricing_aggregate().await?;
        let tier = pricing
            .pricing
            .instance
            .find_tier_by_slug(slug)
            .ok_or_else(|| format!("unknown size '{slug}'"))?;
        Ok((
            args.vcpus.unwrap_or(tier.vcpus),
            args.memory.unwrap_or(tier.memory_mib),
            args.disk_size.unwrap_or(tier.disk_mib),
        ))
    } else {
        crate::commands::instance::resolve_instance_specs_from_flags(
            args.vcpus,
            args.memory,
            args.disk_size,
        )
    }
}

fn prompt_image() -> Result<ItemHash, Box<dyn std::error::Error>> {
    let mut items: Vec<String> = IMAGE_PRESETS
        .iter()
        .map(|(name, _)| name.to_string())
        .collect();
    items.push("custom hash or IPFS CID…".into());

    let idx = Select::new()
        .with_prompt("Image")
        .items(&items)
        .default(0)
        .interact()?;

    if idx < IMAGE_PRESETS.len() {
        Ok(IMAGE_PRESETS[idx].1.parse()?)
    } else {
        let raw: String = Input::new()
            .with_prompt("Image (item hash or IPFS CID)")
            .validate_with(|s: &String| -> Result<(), String> { parse_image(s).map(|_| ()) })
            .interact_text()?;
        parse_image(&raw).map_err(Into::into)
    }
}

fn format_crn_table(entries: &[&CrnListEntry]) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{:>4}  {:>6}  {:<24}  {:<9}  {:>12}  {:>12}  {:<4}  {:<3}  {}\n",
        "#", "Score", "Name", "Version", "Free RAM", "Free Disk", "Conf", "GPU", "URL",
    ));
    for (i, e) in entries.iter().enumerate() {
        let score = e
            .score
            .map(|s| format!("{:.1}%", s * 100.0))
            .unwrap_or_else(|| "-".into());
        let version = e.version.clone().unwrap_or_else(|| "-".into());
        let (ram, disk) = match &e.system_usage {
            Some(u) => (
                format!("{} MiB", u.mem.available_kb / 1024),
                format!("{} MiB", u.disk.available_kb / 1024),
            ),
            None => ("-".into(), "-".into()),
        };
        let conf = if e.confidential_support { "✓" } else { " " };
        let gpu = if e.gpu_support { "✓" } else { " " };
        out.push_str(&format!(
            "{:>4}  {:>6}  {:<24}  {:<9}  {:>12}  {:>12}  {:<4}  {:<3}  {}\n",
            i + 1,
            score,
            truncate(&e.name, 24),
            truncate(&version, 9),
            ram,
            disk,
            conf,
            gpu,
            e.address,
        ));
    }
    out
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut t: String = s.chars().take(max.saturating_sub(1)).collect();
        t.push('…');
        t
    }
}

fn prompt_crn<'a>(
    entries: &'a [&CrnListEntry],
) -> Result<&'a CrnListEntry, Box<dyn std::error::Error>> {
    // Pre-sort by score desc; None scores sort last.
    let mut sorted: Vec<&CrnListEntry> = entries.to_vec();
    sorted.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));

    loop {
        eprintln!("{}", format_crn_table(&sorted));

        let labels: Vec<String> = sorted
            .iter()
            .map(|e| {
                let score = e
                    .score
                    .map(|s| format!("{:.1}%", s * 100.0))
                    .unwrap_or("-".into());
                format!("{:<6} {:<24} {}", score, truncate(&e.name, 24), e.address)
            })
            .collect();
        let idx = FuzzySelect::new()
            .with_prompt("Choose a CRN (type to search)")
            .items(&labels)
            .default(0)
            .interact()?;

        let chosen = sorted[idx];
        eprintln!(
            "\nSelected CRN:\n  name:    {}\n  hash:    {}\n  url:     {}\n  score:   {}\n  version: {}\n",
            chosen.name,
            chosen.hash,
            chosen.address,
            chosen
                .score
                .map(|s| format!("{:.1}%", s * 100.0))
                .unwrap_or("-".into()),
            chosen.version.as_deref().unwrap_or("-"),
        );

        if Confirm::new()
            .with_prompt("Deploy on this node?")
            .default(true)
            .interact()?
        {
            return Ok(chosen);
        }
        // User said no → back to FuzzySelect.
    }
}

async fn accept_terms_and_conditions(
    _aleph_client: &AlephClient,
    chosen: &CrnListEntry,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(tac_hash) = chosen.terms_and_conditions.as_deref() else {
        return Ok(());
    };
    eprintln!(
        "\nThis CRN requires accepting terms & conditions.\n\
         Document item hash: {tac_hash}\n\
         Review with: `aleph file download --message-hash {tac_hash}`\n",
    );
    if !Confirm::new()
        .with_prompt("Accept the CRN's terms & conditions?")
        .default(false)
        .interact()?
    {
        return Err("Terms & Conditions rejected: instance creation aborted.".into());
    }
    Ok(())
}

fn prompt_name_optional() -> Result<Option<String>, Box<dyn std::error::Error>> {
    let raw: String = Input::new()
        .with_prompt("Instance name (optional, press enter to skip)")
        .allow_empty(true)
        .interact_text()?;
    Ok(if raw.trim().is_empty() {
        None
    } else {
        Some(raw)
    })
}

fn prompt_ssh_pubkey_path() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let default = default_ssh_pubkey_path();
    loop {
        let raw: String = Input::new()
            .with_prompt("Path to SSH public key file")
            .default(default.display().to_string())
            .interact_text()?;
        let path = std::path::PathBuf::from(expand_tilde(&raw));
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                if let Err(e) = validate_ssh_pubkey(content.trim(), &path) {
                    eprintln!("  ✗ {e}");
                    continue;
                }
                return Ok(path);
            }
            Err(e) => {
                eprintln!("  ✗ failed to read '{}': {e}", path.display());
                continue;
            }
        }
    }
}

fn default_ssh_pubkey_path() -> std::path::PathBuf {
    directories::UserDirs::new()
        .map(|u| u.home_dir().to_path_buf())
        .unwrap_or_default()
        .join(".ssh/id_rsa.pub")
}

fn expand_tilde(s: &str) -> String {
    if let Some(rest) = s.strip_prefix("~/")
        && let Some(u) = directories::UserDirs::new()
    {
        return u.home_dir().join(rest).display().to_string();
    }
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_short_string_unchanged() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn truncate_long_string_appends_ellipsis() {
        assert_eq!(truncate("abcdefghij", 5), "abcd…");
    }

    #[test]
    fn sort_puts_none_score_last() {
        use aleph_sdk::crns_list::CrnListEntry;
        use std::collections::HashMap;
        let make = |name: &str, score: Option<f64>| CrnListEntry {
            hash: name.into(),
            name: name.into(),
            address: "https://x.y".into(),
            score,
            version: None,
            payment_receiver_address: None,
            gpu_support: false,
            confidential_support: false,
            qemu_support: false,
            ipv6_check: None,
            system_usage: None,
            compatible_available_gpus: None,
            terms_and_conditions: None,
            extra: HashMap::new(),
        };
        let a = make("a", None);
        let b = make("b", Some(0.5));
        let c = make("c", Some(0.9));
        let entries = vec![&a, &b, &c];

        let mut sorted: Vec<&CrnListEntry> = entries.clone();
        sorted.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let names: Vec<&str> = sorted.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["c", "b", "a"]);
    }
}
