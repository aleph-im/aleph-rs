use crate::cli::NodeCommand;
use crate::common::submit_or_preview;
use aleph_sdk::client::AlephClient;
use aleph_sdk::corechannel::{self, AmendDetails};
use url::Url;

use crate::common::resolve_account;

pub async fn handle_node_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: NodeCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        NodeCommand::CreateCcn(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::create_ccn(&account, &args.name, &args.multiaddress)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::CreateCrn(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::create_crn(&account, &args.name, &args.address)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Link(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::link_crn(&account, args.crn)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Unlink(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::unlink_crn(&account, args.crn)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Stake(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::stake(&account, args.node)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Unstake(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::unstake(&account, args.node)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Drop(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::drop_node(&account, args.node)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Amend(args) => {
            let details = AmendDetails {
                name: args.name,
                multiaddress: args.multiaddress,
                address: args.address,
                picture: args.picture,
                banner: args.banner,
                description: args.description,
                reward: args.reward,
                stream_reward: args.stream_reward,
                manager: args.manager,
                authorized: args.authorized,
                locked: args.locked,
                registration_url: args.registration_url,
                terms_and_conditions: args.terms_and_conditions,
            };
            if details == AmendDetails::default() {
                return Err("at least one field must be provided".into());
            }
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::amend_node(&account, args.node, details)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
    }
}
