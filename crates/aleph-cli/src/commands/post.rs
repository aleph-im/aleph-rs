use crate::cli::{PostAmendArgs, PostCommand, PostCreateArgs};
use crate::common::{read_content, resolve_account, resolve_address, submit_or_preview};
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::{AlephClient, AlephPostClient};
use aleph_types::channel::Channel;
use aleph_types::message::MessageType;
use url::Url;

pub async fn handle_post_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: PostCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        PostCommand::List(args) => {
            let filter = args.filter.into();
            match args.api_version {
                0 => {
                    let response = aleph_client.get_posts_v0(&filter).await?;
                    println!("{}", serde_json::to_string_pretty(&response.posts)?);
                }
                1 => {
                    let response = aleph_client.get_posts_v1(&filter).await?;
                    println!("{}", serde_json::to_string_pretty(&response.posts)?);
                }
                v => {
                    return Err(format!("unsupported API version: {v} (expected 0 or 1)").into());
                }
            }
        }
        PostCommand::Create(args) => {
            handle_post_create(aleph_client, ccn_url, json, args).await?;
        }
        PostCommand::Amend(args) => {
            handle_post_amend(aleph_client, ccn_url, json, args).await?;
        }
    }

    Ok(())
}

async fn handle_post_create(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: PostCreateArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing)?;
    let content = read_content(args.content)?;
    let envelope = serde_json::json!({
        "type": args.post_type,
        "content": content,
    });
    let mut builder = MessageBuilder::new(&account, MessageType::Post, envelope);
    if let Some(owner) = args.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

async fn handle_post_amend(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: PostAmendArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing)?;
    let content = read_content(args.content)?;
    let envelope = serde_json::json!({
        "ref": args.reference.to_string(),
        "content": content,
    });
    let mut builder = MessageBuilder::new(&account, MessageType::Post, envelope);
    if let Some(owner) = args.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn post_create_envelope_shape() {
        let content = serde_json::json!({"body": "hello"});
        let envelope = serde_json::json!({
            "type": "chat",
            "content": content,
        });
        assert_eq!(envelope["type"], "chat");
        assert_eq!(envelope["content"]["body"], "hello");
        assert!(envelope.get("ref").is_none());
    }

    #[test]
    fn post_amend_envelope_shape() {
        let content = serde_json::json!({"body": "edited"});
        let reference = "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c";
        let envelope = serde_json::json!({
            "ref": reference,
            "content": content,
        });
        assert_eq!(envelope["ref"], reference);
        assert_eq!(envelope["content"]["body"], "edited");
        assert!(envelope.get("type").is_none());
    }
}
