use crate::cli::{AggregateCommand, AggregateCreateArgs};
use crate::common::{read_content, submit_or_preview};
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::AlephClient;
use aleph_types::channel::Channel;
use aleph_types::message::MessageType;
use url::Url;

use crate::account::load_account;

pub async fn handle_aggregate_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: AggregateCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        AggregateCommand::Create(args) => {
            handle_aggregate_create(aleph_client, ccn_url, json, args).await?;
        }
    }
    Ok(())
}

async fn handle_aggregate_create(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: AggregateCreateArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let dry_run = args.signing.dry_run;
    let account = load_account(
        args.signing.private_key.as_deref(),
        args.signing.chain.into(),
    )?;
    let content = read_content(args.content)?;
    let map = match content {
        serde_json::Value::Object(map) => map,
        _ => return Err("aggregate content must be a JSON object".into()),
    };
    let envelope = serde_json::json!({
        "key": args.key,
        "content": serde_json::Value::Object(map),
    });
    let mut builder = MessageBuilder::new(&account, MessageType::Aggregate, envelope);
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn aggregate_content_must_be_object() {
        assert!(!serde_json::json!("not an object").is_object());
        assert!(serde_json::json!({"setting": "value"}).is_object());
    }
}
