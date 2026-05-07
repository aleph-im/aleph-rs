use crate::account::store::AccountStore;
use crate::cli::{AggregateCommand, AggregateCreateArgs, AggregateGetArgs, AggregateListArgs};
use crate::common::{read_content, resolve_account, resolve_address, submit_or_preview};
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::{AlephAggregateClient, AlephClient};
use aleph_types::chain::Address;
use aleph_types::channel::Channel;
use aleph_types::message::MessageType;
use anyhow::{Result, anyhow, bail};
use url::Url;

pub async fn handle_aggregate_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: AggregateCommand,
) -> Result<()> {
    match command {
        AggregateCommand::Create(args) => {
            handle_aggregate_create(aleph_client, ccn_url, json, args).await?;
        }
        AggregateCommand::Get(args) => {
            handle_aggregate_get(aleph_client, json, args).await?;
        }
        AggregateCommand::List(args) => {
            handle_aggregate_list(aleph_client, json, args).await?;
        }
    }
    Ok(())
}

async fn handle_aggregate_create(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: AggregateCreateArgs,
) -> Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;
    let content = read_content(args.content)?;
    let map = match content {
        serde_json::Value::Object(map) => map,
        _ => bail!("aggregate content must be a JSON object"),
    };
    let envelope = serde_json::json!({
        "key": args.key,
        "content": serde_json::Value::Object(map),
    });
    let mut builder = MessageBuilder::new(&account, MessageType::Aggregate, envelope);
    if let Some(owner) = args.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

async fn handle_aggregate_get(
    aleph_client: &AlephClient,
    json: bool,
    args: AggregateGetArgs,
) -> Result<()> {
    let address = resolve_owner_address(args.address.as_deref())?;
    let value: serde_json::Value = match aleph_client.get_aggregate(&address, &args.key).await {
        Ok(v) => v,
        Err(e) if e.is_not_found() => {
            eprintln!("No aggregate at {}/{}", address, args.key);
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    if json {
        println!("{}", serde_json::to_string(&value)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&value)?);
    }
    Ok(())
}

async fn handle_aggregate_list(
    aleph_client: &AlephClient,
    json: bool,
    args: AggregateListArgs,
) -> Result<()> {
    let address = resolve_owner_address(args.address.as_deref())?;
    let aggregates = aleph_client.get_all_aggregates(&address).await?;

    if json {
        println!("{}", serde_json::to_string(&aggregates)?);
        return Ok(());
    }

    if aggregates.is_empty() {
        eprintln!("No aggregates for {address}");
        return Ok(());
    }

    let mut keys: Vec<&String> = aggregates.keys().collect();
    keys.sort();
    for key in keys {
        println!("=== {key} ===");
        println!("{}", serde_json::to_string_pretty(&aggregates[key])?);
    }
    Ok(())
}

/// Resolve the owner address for read-only aggregate queries.
///
/// Mirrors the precedence used by `aleph account balance`: explicit
/// `--address` (raw or local-name) wins, otherwise we use the default
/// account from the local store.
fn resolve_owner_address(args_address: Option<&str>) -> Result<Address> {
    if let Some(value) = args_address {
        return resolve_address(value);
    }
    let store = AccountStore::open().map_err(|e| anyhow!("failed to open account store: {e}"))?;
    let name = store.default_account_name()?.ok_or_else(|| {
        anyhow!(
            "no --address provided and no default account set; \
             pass --address or set a default with: aleph account use <NAME>"
        )
    })?;
    let entry = store.get_account(&name)?;
    Ok(Address::from(entry.address))
}

#[cfg(test)]
mod tests {
    #[test]
    fn aggregate_content_must_be_object() {
        assert!(!serde_json::json!("not an object").is_object());
        assert!(serde_json::json!({"setting": "value"}).is_object());
    }
}
