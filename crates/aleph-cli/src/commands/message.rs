use crate::cli::{ForgetArgs, GetMessageArgs, MessageCommand};
use crate::common::submit_or_preview;
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::{AlephClient, AlephMessageClient};
use aleph_types::channel::Channel;
use aleph_types::message::MessageType;
use url::Url;

use crate::account::load_account;

pub async fn handle_message_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: MessageCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        MessageCommand::Get(GetMessageArgs { item_hash }) => {
            let message = aleph_client.get_message(&item_hash).await?;
            println!("{}", serde_json::to_string_pretty(&message)?);
        }
        MessageCommand::List(message_filter) => {
            let messages = aleph_client.get_messages(&(*message_filter).into()).await?;
            println!("{}", serde_json::to_string_pretty(&messages)?);
        }
        MessageCommand::Sync(sync_args) => {
            super::sync::handle_sync(*sync_args).await?;
        }
        MessageCommand::Forget(args) => {
            handle_forget(aleph_client, ccn_url, json, args).await?;
        }
    }

    Ok(())
}

async fn handle_forget(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: ForgetArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let dry_run = args.signing.dry_run;
    let account = load_account(
        args.signing.private_key.as_deref(),
        args.signing.chain.into(),
    )?;
    let hashes: Vec<String> = args.hashes.iter().map(|h| h.to_string()).collect();
    let mut envelope = serde_json::json!({
        "hashes": hashes,
    });
    if let Some(aggs) = args.aggregates {
        let agg_strs: Vec<String> = aggs.iter().map(|h| h.to_string()).collect();
        envelope["aggregates"] = serde_json::json!(agg_strs);
    }
    if let Some(reason) = args.reason {
        envelope["reason"] = serde_json::json!(reason);
    }
    let mut builder = MessageBuilder::new(&account, MessageType::Forget, envelope);
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
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
