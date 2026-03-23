use crate::cli::AuthorizationCommand;
use crate::common::{format_address, resolve_account, resolve_address, submit_or_preview};
use aleph_sdk::authorization::AlephAuthorizationClient;
use aleph_sdk::client::AlephClient;
use aleph_sdk::messages::AuthorizationBuilder;
use aleph_types::account::Account;
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
                Some(addr) => resolve_address(addr)?,
                None => {
                    let account = resolve_account(&args.signing)?;
                    account.address().clone()
                }
            };

            let authorizations = aleph_client.get_authorizations(&address).await?;

            let filtered: Vec<_> = match &args.delegate {
                Some(delegate) => {
                    let delegate_addr = resolve_address(delegate)?;
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
        AuthorizationCommand::Received(args) => {
            let address = match &args.address {
                Some(addr) => resolve_address(addr)?,
                None => {
                    let account = resolve_account(&args.signing)?;
                    account.address().clone()
                }
            };

            let received = aleph_client.get_received_authorizations(&address).await?;

            let filtered: Vec<_> = match &args.granter {
                Some(granter) => {
                    let granter_addr = resolve_address(granter)?;
                    received
                        .into_iter()
                        .filter(|r| r.granter == granter_addr)
                        .collect()
                }
                None => received,
            };

            if json {
                println!("{}", serde_json::to_string_pretty(&filtered)?);
            } else if filtered.is_empty() {
                eprintln!("No received authorizations found for {address}");
            } else {
                eprintln!("Received authorizations for {address}:\n");
                for entry in &filtered {
                    eprintln!("  From: {}", entry.granter);
                    if entry.authorizations.is_empty() {
                        eprintln!("    (no permission entries)");
                    }
                    for (i, auth) in entry.authorizations.iter().enumerate() {
                        if entry.authorizations.len() > 1 {
                            eprintln!("    Permission #{}:", i + 1);
                        }
                        let indent = if entry.authorizations.len() > 1 {
                            "      "
                        } else {
                            "    "
                        };
                        let mut has_restriction = false;

                        if let Some(chain) = auth.get("chain").and_then(|v| v.as_str()) {
                            eprintln!("{indent}Chain:          {chain}");
                            has_restriction = true;
                        }

                        if let Some(channels) = auth.get("channels").and_then(|v| v.as_array()) {
                            let vals: Vec<&str> =
                                channels.iter().filter_map(|v| v.as_str()).collect();
                            if !vals.is_empty() {
                                eprintln!("{indent}Channels:       {}", vals.join(", "));
                                has_restriction = true;
                            }
                        }

                        if let Some(types) = auth.get("types").and_then(|v| v.as_array()) {
                            let vals: Vec<&str> = types.iter().filter_map(|v| v.as_str()).collect();
                            if !vals.is_empty() {
                                eprintln!("{indent}Message Types:  {}", vals.join(", "));
                                has_restriction = true;
                            }
                        }

                        if let Some(post_types) = auth.get("post_types").and_then(|v| v.as_array())
                        {
                            let vals: Vec<&str> =
                                post_types.iter().filter_map(|v| v.as_str()).collect();
                            if !vals.is_empty() {
                                eprintln!("{indent}Post Types:     {}", vals.join(", "));
                                has_restriction = true;
                            }
                        }

                        if let Some(agg_keys) =
                            auth.get("aggregate_keys").and_then(|v| v.as_array())
                        {
                            let vals: Vec<&str> =
                                agg_keys.iter().filter_map(|v| v.as_str()).collect();
                            if !vals.is_empty() {
                                eprintln!("{indent}Aggregate Keys: {}", vals.join(", "));
                                has_restriction = true;
                            }
                        }

                        if !has_restriction {
                            eprintln!("{indent}All permissions (unrestricted)");
                        }
                    }
                    eprintln!();
                }
            }
        }
        AuthorizationCommand::Add(args) => {
            let dry_run = args.signing.dry_run;
            let account = resolve_account(&args.signing)?;

            let delegate_addr = resolve_address(&args.delegate_address)?;
            let delegate_display = format_address(&args.delegate_address, &delegate_addr);
            let mut builder = AuthorizationBuilder::new(delegate_addr);

            if let Some(chain_cli) = args.delegate_chain {
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
                eprintln!("Added authorization for {delegate_display}");
            }
        }
        AuthorizationCommand::Revoke(args) => {
            // Validation (exactly one of delegate_address or --all) is enforced
            // by clap's ArgGroup on AuthorizationRevokeArgs.

            let dry_run = args.signing.dry_run;
            let account = resolve_account(&args.signing)?;

            let delegate_input = args.delegate_address.as_deref();
            let authorizations = if args.all {
                vec![]
            } else {
                let delegate_addr = resolve_address(delegate_input.unwrap())?;
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
                    let input = delegate_input.unwrap();
                    let addr = resolve_address(input)?;
                    eprintln!(
                        "Revoked authorizations for {}",
                        format_address(input, &addr)
                    );
                }
            }
        }
    }
    Ok(())
}
