//! Interactive `-i` resolver for `aleph instance create`.
//!
//! Runs before the normal build/submit path and fills in any `InstanceCreateArgs`
//! fields not already provided on the command line. Prompts, in order:
//! image → size → CRN → name → SSH public key path. CRN selection always runs
//! (the instance gets pinned to the chosen CRN via `node_hash`).

use crate::cli::{IMAGE_PRESETS, InstanceCreateArgs, parse_image};
use aleph_sdk::client::AlephClient;
use aleph_types::item_hash::ItemHash;
use dialoguer::{Input, Select};

pub async fn resolve_interactive(
    args: &mut InstanceCreateArgs,
    aleph_client: &AlephClient,
) -> Result<(), Box<dyn std::error::Error>> {
    if args.image.is_none() {
        args.image = Some(prompt_image()?);
    }
    if args.size.is_none() && args.disk_size.is_none() {
        args.size = Some(prompt_size(aleph_client).await?);
    }
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
