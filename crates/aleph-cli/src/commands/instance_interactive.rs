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
    _aleph_client: &AlephClient,
) -> Result<(), Box<dyn std::error::Error>> {
    if args.image.is_none() {
        args.image = Some(prompt_image()?);
    }
    Ok(())
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
