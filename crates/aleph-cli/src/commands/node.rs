use crate::cli::{NodeCommand, NodeListArgs, NodeTypeCli};
use crate::common::{resolve_account, resolve_address, submit_or_preview};
use aleph_sdk::aggregate_models::corechannel::{CORECHANNEL_ADDRESS, CcnInfo, CrnInfo, CrnStatus};
use aleph_sdk::client::{AlephAggregateClient, AlephClient};
use aleph_sdk::corechannel::{self, AmendDetails};
use aleph_types::account::Account;
use aleph_types::chain::Address;
use serde::Serialize;
use url::Url;

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum NodeInfo {
    Ccn(CcnInfo),
    Crn(CrnInfo),
}

pub async fn handle_node_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: NodeCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        NodeCommand::List(args) => list_nodes(aleph_client, json, args).await,
        NodeCommand::CreateCcn(args) => {
            let account = resolve_account(&args.signing)?;
            let pending =
                corechannel::create_ccn(&account, &args.name, &args.multiaddress, &args.network)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::CreateCrn(args) => {
            let account = resolve_account(&args.signing)?;
            let pending =
                corechannel::create_crn(&account, &args.name, &args.address, &args.network)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Link(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::link_crn(&account, args.crn, &args.network)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Unlink(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::unlink_crn(&account, args.crn, &args.network)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Stake(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::stake(&account, args.node, &args.network)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Unstake(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::unstake(&account, args.node, &args.network)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Drop(args) => {
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::drop_node(&account, args.node, &args.network)?;
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
            let pending = corechannel::amend_node(&account, args.node, details, &args.network)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
    }
}

async fn list_nodes(
    aleph_client: &AlephClient,
    json: bool,
    args: NodeListArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let filter_address = if args.all {
        None
    } else if let Some(addr) = &args.address {
        Some(resolve_address(addr)?)
    } else {
        match resolve_account(&args.signing) {
            Ok(account) => Some(account.address().clone()),
            Err(_) => {
                return Err("No address provided. Use --address <ADDRESS> or --all, \
                     or configure a signing account."
                    .into());
            }
        }
    };

    let cc_address = match &args.corechannel_address {
        Some(addr) => Address::from(addr.clone()),
        None => CORECHANNEL_ADDRESS.clone(),
    };
    let aggregate = aleph_client
        .get_corechannel_aggregate(&cc_address)
        .await?;

    let mut nodes: Vec<NodeInfo> = Vec::new();

    let include_ccn = !matches!(args.r#type, Some(NodeTypeCli::Crn));
    let include_crn = !matches!(args.r#type, Some(NodeTypeCli::Ccn));

    if include_ccn {
        for ccn in aggregate.corechannel.nodes {
            if filter_address.as_ref().is_none_or(|a| *a == ccn.owner) {
                nodes.push(NodeInfo::Ccn(ccn));
            }
        }
    }

    if include_crn {
        for crn in aggregate.corechannel.resource_nodes {
            if filter_address.as_ref().is_none_or(|a| *a == crn.owner) {
                nodes.push(NodeInfo::Crn(crn));
            }
        }
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&nodes)?);
    } else if nodes.is_empty() {
        match &filter_address {
            Some(addr) => eprintln!("No nodes found for {addr}"),
            None => eprintln!("No nodes found"),
        }
    } else {
        for node in &nodes {
            match node {
                NodeInfo::Ccn(ccn) => {
                    eprintln!("CCN  {}  {}  score: {:.2}", ccn.hash, ccn.name, ccn.score,);
                }
                NodeInfo::Crn(crn) => {
                    let status = match &crn.status {
                        CrnStatus::Linked { parent } => format!("linked (parent: {parent})"),
                        CrnStatus::Waiting => "waiting".to_string(),
                    };
                    eprintln!(
                        "CRN  {}  {}  score: {:.2}  status: {}",
                        crn.hash, crn.name, crn.score, status,
                    );
                }
            }
        }
    }
    Ok(())
}
