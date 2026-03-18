use crate::cli::NodeCommand;
use crate::common::submit_or_preview;
use aleph_sdk::client::AlephClient;
use aleph_sdk::corechannel;
use url::Url;

use crate::account::load_account;

pub async fn handle_node_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: NodeCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        NodeCommand::CreateCcn(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::create_ccn(&account, &args.name, &args.multiaddress)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::CreateCrn(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::create_crn(&account, &args.name, &args.address)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Link(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::link_crn(&account, args.crn)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Unlink(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::unlink_crn(&account, args.crn)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Stake(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::stake(&account, args.node)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Unstake(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::unstake(&account, args.node)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Drop(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::drop_node(&account, args.node)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
    }
}
