use crate::cli::AuthorizationCommand;
use crate::common::{resolve_account, submit_or_preview};
use aleph_sdk::authorization::AlephAuthorizationClient;
use aleph_sdk::client::AlephClient;
use aleph_sdk::messages::AuthorizationBuilder;
use aleph_types::account::Account;
use aleph_types::chain::Address;
use url::Url;

pub async fn handle_authorization_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: AuthorizationCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        AuthorizationCommand::List(args) => {
            let address = match &args.address {
                Some(addr) => Address::from(addr.clone()),
                None => {
                    let account = resolve_account(&args.signing)?;
                    account.address().clone()
                }
            };

            let authorizations = aleph_client.get_authorizations(&address).await?;

            let filtered: Vec<_> = match &args.delegate {
                Some(delegate) => {
                    let delegate_addr = Address::from(delegate.clone());
                    authorizations
                        .into_iter()
                        .filter(|a| a.address == delegate_addr)
                        .collect()
                }
                None => authorizations,
            };

            if json {
                println!("{}", serde_json::to_string_pretty(&filtered)?);
            } else if filtered.is_empty() {
                eprintln!("No authorizations found for {address}");
            } else {
                eprintln!("Authorizations for {address}:\n");
                for auth in &filtered {
                    eprintln!("  Delegate:       {}", auth.address);
                    if let Some(chain) = &auth.chain {
                        eprintln!("  Chain:          {chain}");
                    }
                    if !auth.channels.is_empty() {
                        eprintln!("  Channels:       {}", auth.channels.join(", "));
                    }
                    if !auth.types.is_empty() {
                        let types: Vec<String> = auth.types.iter().map(|t| t.to_string()).collect();
                        eprintln!("  Message Types:  {}", types.join(", "));
                    }
                    if !auth.post_types.is_empty() {
                        eprintln!("  Post Types:     {}", auth.post_types.join(", "));
                    }
                    if !auth.aggregate_keys.is_empty() {
                        eprintln!("  Aggregate Keys: {}", auth.aggregate_keys.join(", "));
                    }
                    eprintln!();
                }
            }
        }
        AuthorizationCommand::Add(args) => {
            let dry_run = args.signing.dry_run;
            let account = resolve_account(&args.signing)?;

            let mut builder =
                AuthorizationBuilder::new(Address::from(args.delegate_address.clone()));

            if let Some(chain_cli) = args.chain {
                builder = builder.chain(chain_cli.into());
            }

            for channel in args.channels {
                builder = builder.channel(channel);
            }

            for mt in args.message_types {
                builder = builder.message_type(mt.into());
            }

            for pt in args.post_types {
                builder = builder.post_type(pt);
            }

            for ak in args.aggregate_keys {
                builder = builder.aggregate_key(ak);
            }

            let authorization = builder.build()?;

            // Fetch existing authorizations, append, and submit as aggregate
            let mut existing = aleph_client.get_authorizations(account.address()).await?;
            existing.push(authorization);
            let content = aleph_types::message::SecurityAggregateContent {
                authorizations: existing,
            };
            let content_map = match serde_json::to_value(&content)? {
                serde_json::Value::Object(map) => map,
                _ => unreachable!(),
            };
            let pending_msg =
                aleph_sdk::messages::AggregateBuilder::new(&account, "security", content_map)
                    .build()?;

            submit_or_preview(aleph_client, ccn_url, &pending_msg, dry_run, json).await?;

            if !json && !dry_run {
                eprintln!("Added authorization for {}", args.delegate_address);
            }
        }
        AuthorizationCommand::Revoke(args) => {
            // Validation (exactly one of delegate_address or --all) is enforced
            // by clap's ArgGroup on AuthorizationRevokeArgs.

            let dry_run = args.signing.dry_run;
            let account = resolve_account(&args.signing)?;

            let authorizations = if args.all {
                vec![]
            } else {
                let delegate_addr = Address::from(args.delegate_address.as_ref().unwrap().clone());
                aleph_client
                    .get_authorizations(account.address())
                    .await?
                    .into_iter()
                    .filter(|a| a.address != delegate_addr)
                    .collect()
            };

            let content = aleph_types::message::SecurityAggregateContent { authorizations };
            let content_map = match serde_json::to_value(&content)? {
                serde_json::Value::Object(map) => map,
                _ => unreachable!(),
            };
            let pending_msg =
                aleph_sdk::messages::AggregateBuilder::new(&account, "security", content_map)
                    .build()?;

            submit_or_preview(aleph_client, ccn_url, &pending_msg, dry_run, json).await?;

            if !json && !dry_run {
                if args.all {
                    eprintln!("Revoked all authorizations");
                } else {
                    eprintln!(
                        "Revoked authorizations for {}",
                        args.delegate_address.unwrap()
                    );
                }
            }
        }
    }
    Ok(())
}
