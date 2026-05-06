use crate::account::store::AccountStore;
use crate::cli::{
    AggregateCommand, AggregateCreateArgs, AggregateForgetArgs, AggregateGetArgs, AggregateListArgs,
};
use crate::common::{
    confirm_action, read_content, resolve_account, resolve_address, submit_or_preview,
};
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::{AlephAggregateClient, AlephClient, AlephMessageClient, MessageWithStatus};
use aleph_types::chain::Address;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
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
        AggregateCommand::Forget(args) => {
            handle_aggregate_forget(aleph_client, ccn_url, json, args).await?;
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

async fn handle_aggregate_forget(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: AggregateForgetArgs,
) -> Result<()> {
    if args.hashes.is_empty() {
        bail!("at least one hash is required");
    }

    let dry_run = args.signing.dry_run;
    validate_aggregate_hashes(aleph_client, &args.hashes).await?;

    if !dry_run {
        let n = args.hashes.len();
        let prompt = format!(
            "Forget {n} aggregate(s) in their entirety? This is irreversible: every \
             AGGREGATE element under each (sender, key) pair will be tombstoned."
        );
        if !confirm_action(&prompt, args.yes)? {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    let account = resolve_account(&args.signing.identity)?;
    let agg_strs: Vec<String> = args.hashes.iter().map(|h| h.to_string()).collect();
    let mut envelope = serde_json::json!({
        "hashes": Vec::<String>::new(),
        "aggregates": agg_strs,
    });
    if let Some(reason) = args.reason {
        envelope["reason"] = serde_json::json!(reason);
    }
    let mut builder = MessageBuilder::new(&account, MessageType::Forget, envelope);
    if let Some(owner) = args.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

/// Verify each hash points at a processed AGGREGATE message before submitting
/// the forget. Bails with a targeted error otherwise so the user does not
/// produce a forget that the network will reject (or worse, that wipes the
/// wrong message type).
async fn validate_aggregate_hashes(aleph_client: &AlephClient, hashes: &[ItemHash]) -> Result<()> {
    for hash in hashes {
        let status = aleph_client.get_message(hash).await?;
        let message_type = match &status {
            MessageWithStatus::Processed { message }
            | MessageWithStatus::Removing { message, .. }
            | MessageWithStatus::Removed { message, .. } => message.message_type,
            MessageWithStatus::Pending { .. } => {
                bail!(
                    "{hash}: message is still pending and its type cannot be \
                     verified. Wait for it to be processed, or use `aleph \
                     message forget --aggregates {hash}` to skip validation."
                );
            }
            MessageWithStatus::Forgotten { .. } => {
                bail!("{hash}: message is already forgotten");
            }
            MessageWithStatus::Rejected { .. } => {
                bail!("{hash}: message was rejected by the network and cannot be forgotten");
            }
        };
        if message_type != MessageType::Aggregate {
            bail!(
                "{hash} is a {message_type:?} message, not an AGGREGATE. Use \
                 `aleph message forget {hash}` for non-aggregate messages."
            );
        }
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

    #[test]
    fn forget_envelope_uses_aggregates_field() {
        let hashes = vec!["abc123".to_string()];
        let mut envelope = serde_json::json!({
            "hashes": Vec::<String>::new(),
            "aggregates": hashes,
        });
        envelope["reason"] = serde_json::json!("cleanup");
        assert!(envelope["hashes"].as_array().unwrap().is_empty());
        assert_eq!(envelope["aggregates"][0], "abc123");
        assert_eq!(envelope["reason"], "cleanup");
    }
}
