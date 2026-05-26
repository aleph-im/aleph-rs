use crate::cli::{ForgetArgs, GetMessageArgs, MessageCommand, RetryArgs, SigningArgs};
use crate::common::{confirm_action, resolve_account, resolve_address, submit_or_preview};
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::{AlephClient, AlephMessageClient};
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::MessageType;
use anyhow::{Result, bail};
use futures_util::{StreamExt, TryStreamExt};
use url::Url;

pub async fn handle_message_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: MessageCommand,
) -> Result<()> {
    match command {
        MessageCommand::Get(GetMessageArgs { item_hash }) => {
            let message = aleph_client.get_message(&item_hash).await?;
            println!("{}", serde_json::to_string_pretty(&message)?);
        }
        MessageCommand::List(args) => {
            let messages: Vec<_> = aleph_client
                .get_messages_iterator(args.filter.into(), None)
                .take(args.count as usize)
                .try_collect()
                .await?;
            println!("{}", serde_json::to_string_pretty(&messages)?);
        }
        MessageCommand::Sync(sync_args) => {
            super::sync::handle_sync(*sync_args).await?;
        }
        MessageCommand::Forget(args) => {
            handle_forget(aleph_client, ccn_url, json, args).await?;
        }
        MessageCommand::Retry(args) => {
            handle_retry(aleph_client, ccn_url, json, args).await?;
        }
    }

    Ok(())
}

async fn handle_forget(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: ForgetArgs,
) -> Result<()> {
    forget_targets(
        aleph_client,
        ccn_url,
        json,
        ForgetTargets {
            hashes: args.hashes,
            aggregates: args.aggregates.unwrap_or_default(),
            reason: args.reason,
            channel: args.channel,
            on_behalf_of: args.on_behalf_of,
            yes: args.yes,
            confirm_label: "target",
            signing: args.signing,
        },
    )
    .await
}

pub struct ForgetTargets {
    pub hashes: Vec<ItemHash>,
    pub aggregates: Vec<ItemHash>,
    pub reason: Option<String>,
    pub channel: Option<String>,
    pub on_behalf_of: Option<String>,
    pub yes: bool,
    /// Singular noun used in the confirmation prompt (pluralized with `(s)`).
    pub confirm_label: &'static str,
    pub signing: SigningArgs,
}

pub async fn forget_targets(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    targets: ForgetTargets,
) -> Result<()> {
    let dry_run = targets.signing.dry_run;
    let total = targets.hashes.len() + targets.aggregates.len();
    if total == 0 {
        bail!("at least one hash is required");
    }
    if !dry_run {
        let prompt = format!(
            "Forget {total} {label}(s)? This is irreversible - content will be tombstoned on the network.",
            label = targets.confirm_label,
        );
        if !confirm_action(&prompt, targets.yes)? {
            eprintln!("Aborted.");
            return Ok(());
        }
    }
    let account = resolve_account(&targets.signing.identity)?;
    let hashes: Vec<String> = targets.hashes.iter().map(|h| h.to_string()).collect();
    let mut envelope = serde_json::json!({
        "hashes": hashes,
    });
    if !targets.aggregates.is_empty() {
        let agg_strs: Vec<String> = targets.aggregates.iter().map(|h| h.to_string()).collect();
        envelope["aggregates"] = serde_json::json!(agg_strs);
    }
    if let Some(reason) = targets.reason {
        envelope["reason"] = serde_json::json!(reason);
    }
    let mut builder = MessageBuilder::new(&account, MessageType::Forget, envelope);
    if let Some(owner) = targets.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }
    if let Some(ch) = targets.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

async fn handle_retry(
    _aleph_client: &AlephClient,
    _ccn_url: &Url,
    _json: bool,
    _args: RetryArgs,
) -> Result<()> {
    bail!("not yet implemented")
}

#[cfg(test)]
mod tests {
    #[test]
    fn forget_envelope_shape() {
        let hashes = vec!["abc123".to_string()];
        let mut envelope = serde_json::json!({ "hashes": hashes });
        envelope["aggregates"] = serde_json::json!(["def456"]);
        envelope["reason"] = serde_json::json!("cleanup");

        assert_eq!(envelope["hashes"][0], "abc123");
        assert_eq!(envelope["aggregates"][0], "def456");
        assert_eq!(envelope["reason"], "cleanup");
    }
}
