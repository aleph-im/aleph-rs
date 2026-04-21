//! Interactive `-i` resolver for `aleph instance create`.
//!
//! Runs before the normal build/submit path and fills in any `InstanceCreateArgs`
//! fields not already provided on the command line. Prompts, in order:
//! image → size → CRN → name → SSH public key path. CRN selection always runs
//! (the instance gets pinned to the chosen CRN via `node_hash`).

use crate::cli::{IMAGE_PRESETS, InstanceCreateArgs, parse_image};
use aleph_sdk::client::AlephClient;
use aleph_sdk::crns_list::{CrnFilter, CrnListResponse, DEFAULT_CRN_LIST_URL, fetch_crns_list};
use aleph_types::item_hash::ItemHash;
use dialoguer::{Input, Select};
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
    let _filtered = crn_list.filter(&filter);
    // (CRN picker implemented in Task 10.)

    Ok(())
}

async fn prompt_size(aleph_client: &AlephClient) -> Result<String, Box<dyn std::error::Error>> {
    use aleph_sdk::client::AlephAggregateClient;

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
    let raw = std::env::var("ALEPH_CRN_LIST_URL").unwrap_or_else(|_| DEFAULT_CRN_LIST_URL.to_string());
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
    use aleph_sdk::client::AlephAggregateClient;

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
            args.vcpus, args.memory, args.disk_size,
        )
    }
}

fn prompt_image() -> Result<ItemHash, Box<dyn std::error::Error>> {
    let mut items: Vec<String> = IMAGE_PRESETS.iter().map(|(name, _)| name.to_string()).collect();
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
            .validate_with(|s: &String| -> Result<(), String> {
                parse_image(s).map(|_| ())
            })
            .interact_text()?;
        parse_image(&raw).map_err(Into::into)
    }
}
