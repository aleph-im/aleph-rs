//! Interactive `-i` resolver for `aleph instance create`.
//!
//! Runs before the normal build/submit path and fills in any `InstanceCreateArgs`
//! fields not already provided on the command line. Prompts, in order:
//! image → size → node placement → SSH public key path. For node placement the
//! user can let the scheduler pick a node automatically (leaving `crn_hash`
//! unset, like the non-interactive path) or pin to a specific CRN via
//! `node_hash`. The instance name is a required positional argument and is
//! never prompted for.

use crate::cli::{ImageRef, InstanceCreateArgs, parse_image_ref};
use crate::commands::instance::{resolve_gpu, validate_ssh_pubkey};
use aleph_sdk::aggregate_models::pricing::{ComputeUnitSpec, GpuModel, PricingPerEntity};
use aleph_sdk::aggregate_models::vm_images::VmImagesData;
use aleph_sdk::client::{AlephAggregateClient, AlephClient};
use aleph_sdk::crns_list::{
    CrnFilter, CrnListEntry, CrnListResponse, DEFAULT_CRN_LIST_URL, fetch_crns_list,
};
use aleph_sdk::ssh::AlephSshClient;
use aleph_types::chain::Address;
use anyhow::{Result, anyhow, bail};
use dialoguer::{Confirm, FuzzySelect, Input, Select};
use std::cmp::Ordering;
use tokio::task::JoinHandle;

pub async fn resolve_interactive(
    args: &mut InstanceCreateArgs,
    aleph_client: &AlephClient,
    owner_address: &Address,
) -> Result<()> {
    // Kick off the CRN list fetch in parallel with the early prompts, but only
    // if a node isn't already pinned via `--crn-hash` (in which case the list is
    // never needed). When the user later picks "Automatic" placement we drop the
    // result; that's the cost of overlapping the fetch with the image/size
    // prompts, since the placement choice isn't known until after them.
    let crn_list_fut = args.crn_hash.is_none().then(spawn_crn_list_fetch);

    if args.image.is_none() {
        let vm_images = aleph_client
            .get_vm_images_aggregate()
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to fetch vm-images aggregate: {e}. \
                     As a fallback, run without -i and pass --image with a raw item hash or IPFS CID."
                )
            })?
            .vm_images;
        args.image = Some(prompt_image(&vm_images)?);
    }

    // GPU selection runs before sizing. When a GPU is chosen we size from the GPU
    // minimum CU and set explicit vcpus/memory/disk, so we must not also run the
    // regular (non-GPU) size prompt.
    let mut gpu_selected = false;
    if args.gpu.is_none() {
        gpu_selected = prompt_gpu(args, aleph_client).await?;
    }

    if !gpu_selected && args.size.is_none() && args.disk_size.is_none() {
        args.size = Some(prompt_size(aleph_client).await?);
    }

    // Node placement: let the scheduler pick automatically (leaving `crn_hash`
    // unset, like the non-interactive path), or pin to a specific CRN. If
    // `--crn-hash` was already passed on the command line, honor it and skip.
    if args.crn_hash.is_none() && prompt_pick_specific_crn()? {
        // `crn_list_fut` is `Some` exactly when `crn_hash` was None, which is
        // the branch we're in.
        let crn_list = crn_list_fut
            .expect("CRN list fetch is spawned whenever crn_hash is None")
            .await
            .map_err(|e| anyhow!("background task error: {e}"))?
            .map_err(anyhow::Error::msg)?;
        let (vcpus, memory_mib, disk_mib) = resolve_specs_for_filter(args, aleph_client).await?;
        // Filter by the requested GPU model's PCI id, not merely "has a GPU", so
        // the picker matches the GPU requirement put in the instance message.
        let gpu_filter = match &args.gpu {
            Some(names) => {
                let mut device_ids = Vec::with_capacity(names.len());
                for name in names {
                    device_ids.push(resolve_gpu(name)?.device_id);
                }
                Some(device_ids)
            }
            None => None,
        };
        // `ipv6: true` filters the CRN's own infrastructure. CRNs without working IPv6
        // can't route traffic to their VMs, so they can't host usable instances. This is
        // unrelated to the user's local IPv6 connectivity. Matches the Python CLI.
        let filter = CrnFilter {
            ipv6: true,
            min_vcpus: Some(vcpus),
            min_memory_mib: Some(memory_mib),
            min_disk_mib: Some(disk_mib),
            confidential: args.confidential,
            gpu: gpu_filter,
        };
        let filtered = crn_list.filter(&filter);
        if filtered.is_empty() {
            let gpu_desc = match &args.gpu {
                Some(names) => names.join(","),
                None => "none".into(),
            };
            bail!(
                "No CRN matches the requirements (vcpus={}, memory_mib={}, disk_mib={}, confidential={}, gpu={}). \
                 Try a smaller size or wait for capacity.",
                vcpus,
                memory_mib,
                disk_mib,
                filter.confidential,
                gpu_desc
            );
        }
        let chosen = prompt_crn(&filtered)?;
        accept_terms_and_conditions(chosen).await?;
        args.crn_hash = Some(chosen.hash.parse().map_err(|e| {
            anyhow!(
                "CRN list returned an invalid node hash '{}': {}",
                chosen.hash,
                e
            )
        })?);
    }

    // Only prompt for a key file when the user has no other key source. If
    // --ssh-key labels were given, or the owner already has registered keys,
    // the resolver in handle_instance_create will attach those instead.
    if args.ssh_pubkey_file.is_empty() && args.ssh_key.is_empty() {
        let registered = aleph_client
            .list_ssh_keys(owner_address)
            .await
            .unwrap_or_default();
        if registered.is_empty() {
            args.ssh_pubkey_file = vec![prompt_ssh_pubkey_path()?];
        } else {
            eprintln!(
                "Using {} registered SSH key(s). Pass --ssh-pubkey-file to override.",
                registered.len()
            );
        }
    }

    Ok(())
}

async fn prompt_size(aleph_client: &AlephClient) -> Result<String> {
    let pricing = aleph_client
        .get_pricing_aggregate()
        .await
        .map_err(|e| anyhow!("failed to fetch pricing tiers: {e}"))?;
    let instance_pricing = &pricing.pricing.instance;

    let mut tiers: Vec<_> = instance_pricing
        .tiers
        .iter()
        .filter(|t| t.model.is_none())
        .collect();
    tiers.sort_by_key(|t| t.compute_units);

    if tiers.is_empty() {
        bail!("no instance tiers available in the pricing aggregate");
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

/// Eligible compute-unit counts for a GPU: unique `compute_units` from the GPU
/// entity's tiers that are >= the chosen GPU model's minimum. Sorted ascending,
/// so the first entry is always the minimum.
///
/// The GPU entity (`instance_gpu_standard` / `instance_gpu_premium`) lists one tier
/// per GPU model; the model field is informational for sizing because
/// `find_tier_by_slug` resolves a slug from `compute_units` alone. So any CU value
/// >= the chosen GPU's minimum that exists in the same entity is a valid size.
fn gpu_eligible_compute_units(entity: &PricingPerEntity, min_cu: u32) -> Vec<u32> {
    let mut cus: Vec<u32> = entity
        .tiers
        .iter()
        .map(|t| t.compute_units)
        .filter(|&cu| cu >= min_cu)
        .collect();
    cus.sort_unstable();
    cus.dedup();
    cus
}

/// Resolve (vcpus, memory_mib, disk_mib) for a given CU count using a `ComputeUnitSpec`.
fn specs_for_cu(cu: u32, spec: &ComputeUnitSpec) -> (u32, u64, u64) {
    (
        cu * spec.vcpus,
        cu as u64 * spec.memory_mib,
        cu as u64 * spec.disk_mib,
    )
}

/// Prompt for the size (CU count) to allocate alongside the chosen GPU. The
/// GPU's own `compute_units` is the minimum (and the default); larger CU values
/// that also exist in the same GPU entity may be selected to scale up the VM
/// resources beyond the GPU minimum.
fn prompt_gpu_size(model: &GpuModel, entity: &PricingPerEntity) -> Result<u32> {
    let cus = gpu_eligible_compute_units(entity, model.compute_units);
    if cus.is_empty() {
        // Should not happen in practice: the GPU's own tier is always present.
        bail!(
            "no compute-unit sizes >= GPU minimum ({}) found for '{}'",
            model.compute_units,
            model.name
        );
    }

    let items: Vec<String> = cus
        .iter()
        .map(|&cu| {
            let slug = entity.slug_for_compute_units(cu);
            let (vcpus, memory_mib, disk_mib) = specs_for_cu(cu, &entity.compute_unit);
            let suffix = if cu == model.compute_units {
                "  (minimum)"
            } else {
                ""
            };
            format!(
                "{:<14}  {} vCPU · {} MiB RAM · {} MiB disk{}",
                slug, vcpus, memory_mib, disk_mib, suffix,
            )
        })
        .collect();

    let idx = Select::new()
        .with_prompt(format!(
            "Size for {} (min {} CU)",
            model.name, model.compute_units
        ))
        .items(&items)
        .default(0)
        .interact()?;

    Ok(cus[idx])
}

/// Prompt for an optional GPU. Returns `true` if a GPU was chosen (in which case
/// `args.gpu` and explicit `vcpus`/`memory`/`disk_size` are set and `args.size` is
/// left `None`), or `false` for "No GPU" (the caller then runs the regular size prompt).
async fn prompt_gpu(args: &mut InstanceCreateArgs, aleph_client: &AlephClient) -> Result<bool> {
    let pricing = aleph_client
        .get_pricing_aggregate()
        .await
        .map_err(|e| anyhow!("failed to fetch pricing tiers: {e}"))?;
    let models = pricing.pricing.available_gpu_models();
    if models.is_empty() {
        // No GPU models on the network: silently fall back to the regular flow.
        return Ok(false);
    }

    let mut items: Vec<String> = vec!["No GPU".to_string()];
    items.extend(models.iter().map(|m| {
        let vram = match m.vram_mib {
            Some(v) => format!("{} MiB VRAM", v),
            None => "VRAM n/a".to_string(),
        };
        format!("{}  ({}, {} tier)", m.name, vram, m.tier)
    }));

    let idx = Select::new()
        .with_prompt("GPU")
        .items(&items)
        .default(0)
        .interact()?;

    if idx == 0 {
        return Ok(false);
    }

    let model = &models[idx - 1];
    let entity = pricing.pricing.for_instance(false, Some(&model.name));
    let cu = prompt_gpu_size(model, entity)?;
    let (vcpus, memory_mib, disk_mib) = specs_for_cu(cu, &entity.compute_unit);

    args.gpu = Some(vec![model.slug()]);
    args.vcpus = Some(vcpus);
    args.memory = Some(memory_mib);
    args.disk_size = Some(disk_mib);
    args.size = None;

    eprintln!(
        "Selected GPU: {} ({} tier) -> {} vCPU, {} MiB RAM, {} MiB disk",
        model.name, model.tier, vcpus, memory_mib, disk_mib,
    );

    Ok(true)
}

fn crn_list_url() -> Result<url::Url> {
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
) -> Result<(u32, u64, u64)> {
    if let Some(slug) = &args.size {
        let pricing = aleph_client.get_pricing_aggregate().await?;
        let tier = pricing
            .pricing
            .instance
            .find_tier_by_slug(slug)
            .ok_or_else(|| anyhow!("unknown size '{slug}'"))?;
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

fn prompt_image(vm_images: &VmImagesData) -> Result<ImageRef> {
    let active = vm_images.active_rootfs();
    if active.is_empty() {
        eprintln!("No rootfs presets available; enter a raw item hash or IPFS CID.");
        return prompt_custom_image();
    }

    let mut items: Vec<String> = active
        .iter()
        .map(|(slug, entry)| match &entry.display_name {
            Some(d) => format!("{slug}  {d}"),
            None => slug.to_string(),
        })
        .collect();
    items.push("custom hash or IPFS CID...".into());

    let default_idx = vm_images
        .defaults
        .rootfs
        .as_deref()
        .and_then(|d| active.iter().position(|(slug, _)| *slug == d))
        .unwrap_or(0);

    let idx = Select::new()
        .with_prompt("Image")
        .items(&items)
        .default(default_idx)
        .interact()?;

    if idx < active.len() {
        Ok(ImageRef::Hash(active[idx].1.hash.clone()))
    } else {
        prompt_custom_image()
    }
}

fn prompt_custom_image() -> Result<ImageRef> {
    let raw: String = Input::new()
        .with_prompt("Image (item hash or IPFS CID)")
        .validate_with(|s: &String| -> std::result::Result<(), String> {
            parse_image_ref(s).map(|_| ())
        })
        .interact_text()?;
    parse_image_ref(&raw).map_err(anyhow::Error::msg)
}

/// Score suitable for sorting: `None` (or NaN) becomes `None` so those entries
/// sort after every finite score.
fn score_key(e: &CrnListEntry) -> Option<f64> {
    e.score.filter(|s| !s.is_nan())
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

/// Ask how the instance should be placed on the network.
///
/// Returns `true` if the user wants to pin the instance to a specific CRN
/// (triggering the CRN list/filter/select flow), or `false` to let the
/// scheduler choose a node automatically (leaving `crn_hash` unset).
fn prompt_pick_specific_crn() -> Result<bool> {
    let idx = Select::new()
        .with_prompt("Node placement")
        .items(&["Automatic", "Choose a specific node"])
        .default(0)
        .interact()?;
    Ok(idx == 1)
}

fn prompt_crn<'a>(entries: &'a [&CrnListEntry]) -> Result<&'a CrnListEntry> {
    // Pre-sort by score desc; None (and NaN) sort last.
    //
    // JSON doesn't encode NaN, so in practice `score` is always finite or None.
    // We still normalize defensively: a stray NaN `unwrap_or(Equal)` would make
    // the NaN entry's position non-deterministic relative to finite scores.
    let mut sorted: Vec<&CrnListEntry> = entries.to_vec();
    sorted.sort_by(|a, b| {
        score_key(b)
            .partial_cmp(&score_key(a))
            .unwrap_or(Ordering::Equal)
    });

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

/// The CRN's T&C hash, or `None` if unset/empty.
/// The aggregator serves `""` as well as absent; both mean "no T&C".
fn effective_tac_hash(chosen: &CrnListEntry) -> Option<&str> {
    chosen
        .terms_and_conditions
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

async fn accept_terms_and_conditions(chosen: &CrnListEntry) -> Result<()> {
    let Some(tac_hash) = effective_tac_hash(chosen) else {
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
        bail!("Terms & Conditions rejected: instance creation aborted.");
    }
    Ok(())
}

fn prompt_ssh_pubkey_path() -> Result<std::path::PathBuf> {
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

    fn entry_with_score(name: &str, score: Option<f64>) -> CrnListEntry {
        use std::collections::HashMap;
        CrnListEntry {
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
        }
    }

    fn sort_by_score(entries: Vec<&CrnListEntry>) -> Vec<&str> {
        let mut sorted = entries;
        sorted.sort_by(|a, b| {
            score_key(b)
                .partial_cmp(&score_key(a))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted.iter().map(|e| e.name.as_str()).collect()
    }

    #[test]
    fn sort_puts_none_score_last() {
        let a = entry_with_score("a", None);
        let b = entry_with_score("b", Some(0.5));
        let c = entry_with_score("c", Some(0.9));
        assert_eq!(sort_by_score(vec![&a, &b, &c]), ["c", "b", "a"]);
    }

    #[test]
    fn sort_puts_nan_score_last() {
        let a = entry_with_score("a", Some(f64::NAN));
        let b = entry_with_score("b", Some(0.5));
        let c = entry_with_score("c", Some(0.9));
        assert_eq!(sort_by_score(vec![&a, &b, &c]), ["c", "b", "a"]);
    }

    fn entry_with_tac(tac: Option<&str>) -> CrnListEntry {
        use std::collections::HashMap;
        CrnListEntry {
            hash: "h".into(),
            name: "n".into(),
            address: "https://x.y".into(),
            score: None,
            version: None,
            payment_receiver_address: None,
            gpu_support: false,
            confidential_support: false,
            qemu_support: false,
            ipv6_check: None,
            system_usage: None,
            compatible_available_gpus: None,
            terms_and_conditions: tac.map(str::to_string),
            extra: HashMap::new(),
        }
    }

    #[test]
    fn effective_tac_hash_none_when_absent() {
        assert_eq!(effective_tac_hash(&entry_with_tac(None)), None);
    }

    #[test]
    fn effective_tac_hash_none_when_empty_or_whitespace() {
        assert_eq!(effective_tac_hash(&entry_with_tac(Some(""))), None);
        assert_eq!(effective_tac_hash(&entry_with_tac(Some("   "))), None);
    }

    #[test]
    fn effective_tac_hash_returns_trimmed() {
        assert_eq!(
            effective_tac_hash(&entry_with_tac(Some("  abc123  "))),
            Some("abc123")
        );
    }

    use aleph_sdk::aggregate_models::pricing::{ComputeUnitSpec, PricingPerEntity, Tier};
    use std::collections::HashMap;

    fn gpu_tier(model_name: &str, compute_units: u32) -> Tier {
        Tier {
            id: format!("tier-{model_name}"),
            compute_units,
            model: Some(model_name.into()),
            vram: Some(20480),
        }
    }

    fn standard_gpu_entity(tiers: Vec<Tier>) -> PricingPerEntity {
        PricingPerEntity {
            compute_unit: ComputeUnitSpec {
                vcpus: 1,
                memory_mib: 6144,
                disk_mib: 61440,
            },
            tiers,
            price: HashMap::new(),
        }
    }

    #[test]
    fn specs_for_cu_multiplies_cu_by_per_cu_resources() {
        let spec = ComputeUnitSpec {
            vcpus: 1,
            memory_mib: 6144,
            disk_mib: 61440,
        };
        // 3 CU * (1 vcpu, 6144 MiB, 61440 MiB) per CU.
        assert_eq!(specs_for_cu(3, &spec), (3, 18432, 184320));
        // 16 CU at the same per-CU spec.
        assert_eq!(specs_for_cu(16, &spec), (16, 98304, 983040));
    }

    #[test]
    fn gpu_eligible_cus_includes_min_and_larger_only() {
        // Mirror the standard GPU entity in production: several GPU model tiers
        // with various compute_units. Pick "RTX 4000 ADA" (3 CU) as the min.
        let entity = standard_gpu_entity(vec![
            gpu_tier("RTX 4000 ADA", 3),
            gpu_tier("RTX 3090", 4),
            gpu_tier("RTX 4090", 6),
            gpu_tier("RTX 5090", 8),
            gpu_tier("L40S", 12),
            gpu_tier("RTX A5000", 3),
            gpu_tier("RTX A6000", 4),
            gpu_tier("RTX 6000 ADA", 11),
        ]);
        // Min 3 → 3 (dedup with RTX A5000), 4 (dedup with RTX A6000), 6, 8, 11, 12.
        assert_eq!(
            gpu_eligible_compute_units(&entity, 3),
            vec![3, 4, 6, 8, 11, 12]
        );
    }

    #[test]
    fn gpu_eligible_cus_filters_out_smaller_models() {
        let entity = standard_gpu_entity(vec![
            gpu_tier("RTX 4000 ADA", 3),
            gpu_tier("RTX 3090", 4),
            gpu_tier("RTX 4090", 6),
            gpu_tier("L40S", 12),
        ]);
        // Min 6 (e.g. RTX 4090) drops the smaller-CU tiers.
        assert_eq!(gpu_eligible_compute_units(&entity, 6), vec![6, 12]);
    }

    #[test]
    fn gpu_eligible_cus_minimum_alone_when_no_larger_tiers() {
        let entity = standard_gpu_entity(vec![gpu_tier("RTX 4000 ADA", 3)]);
        assert_eq!(gpu_eligible_compute_units(&entity, 3), vec![3]);
    }
}
