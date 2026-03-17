use std::collections::HashSet;
use std::io::Read;
use std::time::Duration;

use crate::account::load_account;
use crate::cli::{
    AggregateCommand, AggregateCreateArgs, Cli, ForgetArgs, GetMessageArgs, MessageCommand,
    NodeCommand, PostAmendArgs, PostCommand, PostCreateArgs,
};
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::{
    AlephClient, AlephMessageClient, AlephPostClient, MessageError, PostMessageResponse,
};
use aleph_sdk::corechannel;
use aleph_types::channel::Channel;
use aleph_types::message::pending::PendingMessage;
use aleph_types::message::{Message, MessageStatus, MessageType};
use clap::Parser;
use url::Url;

/// Returns true if the error is an HTTP 429 Too Many Requests.
fn is_rate_limited(err: &MessageError) -> bool {
    matches!(err, MessageError::ApiError { status: 429, .. })
        || matches!(err, MessageError::HttpError(e) if e.status().is_some_and(|s| s == 429))
}

const MAX_RETRIES: u32 = 5;
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Retry a fallible async operation with exponential backoff on 429s.
async fn with_retry<F, Fut, T>(mut f: F) -> Result<T, MessageError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, MessageError>>,
{
    let mut backoff = INITIAL_BACKOFF;
    for attempt in 0..MAX_RETRIES {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if is_rate_limited(&e) => {
                if attempt + 1 == MAX_RETRIES {
                    return Err(e);
                }
                eprintln!("  rate limited, retrying in {}s...", backoff.as_secs());
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

/// Read JSON content from --content flag or stdin.
fn read_content(
    content_flag: Option<String>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let raw = match content_flag {
        Some(c) => c,
        None => {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            buf
        }
    };
    let value: serde_json::Value = serde_json::from_str(&raw)?;
    Ok(value)
}

/// Submit a signed message, or print it if --dry-run.
/// Handles --json vs human-readable output.
async fn submit_or_preview(
    client: &AlephClient,
    ccn_url: &Url,
    pending: &PendingMessage,
    dry_run: bool,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if dry_run {
        if json {
            println!("{}", serde_json::to_string_pretty(pending)?);
        } else {
            eprintln!("Dry run — message not submitted.\n");
            println!("{}", serde_json::to_string_pretty(pending)?);
        }
        return Ok(());
    }

    let response = match client.submit_message(pending, true).await {
        Ok(r) => r,
        Err(MessageError::ApiError { status, body }) => {
            return Err(format_api_error(status, &body, json).into());
        }
        Err(e) => return Err(e.into()),
    };

    if json {
        print_json_result(ccn_url, pending, &response)?;
    } else {
        print_human_result(ccn_url, pending, &response);
    }
    Ok(())
}

fn print_json_result(
    ccn_url: &Url,
    pending: &PendingMessage,
    response: &PostMessageResponse,
) -> Result<(), Box<dyn std::error::Error>> {
    let explorer_url = format!("{}api/v0/messages/{}", ccn_url.as_str(), pending.item_hash);
    let output = serde_json::json!({
        "item_hash": pending.item_hash.to_string(),
        "type": pending.message_type.to_string(),
        "chain": pending.chain.to_string(),
        "sender": pending.sender.to_string(),
        "channel": pending.channel.as_ref().map(|c| serde_json::to_value(c).unwrap()),
        "time": pending.time,
        "explorer_url": explorer_url,
        "publication_status": response.publication_status.status,
        "message_status": response.message_status,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn print_human_result(ccn_url: &Url, pending: &PendingMessage, response: &PostMessageResponse) {
    let explorer_url = format!("{}api/v0/messages/{}", ccn_url.as_str(), pending.item_hash);
    eprintln!(
        "Message {} ({})",
        response.message_status, pending.message_type
    );
    eprintln!("  Item hash: {}", pending.item_hash);
    eprintln!("  Sender:    {}", pending.sender);
    if let Some(ch) = &pending.channel {
        // Channel has no Display impl, serialize to get the string
        if let Ok(serde_json::Value::String(s)) = serde_json::to_value(ch) {
            eprintln!("  Channel:   {}", s);
        }
    }
    eprintln!("  Explorer:  {}", explorer_url);
}

/// Format an API error for display. Tries to extract a human-readable message
/// from the JSON body; falls back to the raw body if parsing fails.
fn format_api_error(status: u16, body: &str, json: bool) -> String {
    if json {
        // In JSON mode, output structured error to stdout and return a short message
        let error_json = if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) {
            serde_json::json!({ "error": parsed, "http_status": status })
        } else {
            serde_json::json!({ "error": body, "http_status": status })
        };
        // Print the structured error to stdout for tooling to parse
        println!(
            "{}",
            serde_json::to_string_pretty(&error_json).unwrap_or_default()
        );
        return format!("API request failed (HTTP {status})");
    }

    // Human-readable: try to extract the error message from JSON
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) {
        let message = parsed["error"]["message"]
            .as_str()
            .or_else(|| parsed["error"].as_str())
            .or_else(|| parsed["message"].as_str());
        let status_str = parsed["message_status"].as_str().unwrap_or("error");

        if let Some(msg) = message {
            return format!("Message {status_str} (HTTP {status}): {msg}");
        }
    }

    format!("API error (HTTP {status}): {body}")
}

mod account;
mod cli;

async fn handle_message_command(
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
            handle_sync(*sync_args).await?;
        }
        MessageCommand::Forget(args) => {
            handle_forget(aleph_client, ccn_url, json, args).await?;
        }
    }

    Ok(())
}

async fn handle_post_command(
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
    let account = load_account(
        args.signing.private_key.as_deref(),
        args.signing.chain.into(),
    )?;
    let content = read_content(args.content)?;
    let envelope = serde_json::json!({
        "type": args.post_type,
        "content": content,
    });
    let mut builder = MessageBuilder::new(&account, MessageType::Post, envelope);
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
    let account = load_account(
        args.signing.private_key.as_deref(),
        args.signing.chain.into(),
    )?;
    let content = read_content(args.content)?;
    let envelope = serde_json::json!({
        "ref": args.reference.to_string(),
        "content": content,
    });
    let mut builder = MessageBuilder::new(&account, MessageType::Post, envelope);
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
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

async fn handle_aggregate_command(
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

async fn handle_node_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: NodeCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        NodeCommand::CreateCcn(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::create_ccn(&account, &args.name, &args.multiaddress)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::CreateCrn(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::create_crn(&account, &args.name, &args.address)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Link(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::link_crn(&account, args.crn)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Unlink(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::unlink_crn(&account, args.crn)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Stake(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::stake(&account, args.node)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Unstake(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::unstake(&account, args.node)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
        NodeCommand::Drop(args) => {
            let account = load_account(
                args.signing.private_key.as_deref(),
                args.signing.chain.into(),
            )?;
            let pending = corechannel::drop_node(&account, args.node)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
    }
}

async fn handle_sync(args: cli::SyncArgs) -> Result<(), Box<dyn std::error::Error>> {
    let source_url = Url::parse(&args.source)?;
    let target_url = Url::parse(&args.target)?;

    let source_client = AlephClient::new(source_url);
    let target_client = AlephClient::new(target_url);

    // Build filter with count as pagination, page 1
    let mut filter: aleph_sdk::client::MessageFilter = args.filter.into();
    filter.pagination = Some(args.count);
    filter.page = Some(1);

    // Fetch from both nodes concurrently
    eprintln!(
        "Fetching up to {} messages from source and target...",
        args.count
    );
    let (source_messages, target_messages) = tokio::try_join!(
        source_client.get_messages(&filter),
        target_client.get_messages(&filter),
    )?;
    eprintln!(
        "  Found {} messages on source, {} on target.",
        source_messages.len(),
        target_messages.len()
    );

    // Diff by item_hash
    let target_hashes: HashSet<_> = target_messages.iter().map(|m| &m.item_hash).collect();
    let candidates: Vec<_> = source_messages
        .iter()
        .filter(|m| !target_hashes.contains(&m.item_hash))
        .collect();

    eprintln!(
        "{} candidate messages not in target listing, verifying...",
        candidates.len()
    );

    if candidates.is_empty() {
        eprintln!("Nothing to sync.");
        return Ok(());
    }

    // Verify each candidate by calling get_message on the target.
    // The list diff can have false positives due to ordering differences.
    let mut truly_missing: Vec<&Message> = Vec::new();
    let mut skipped = 0u32;
    for msg in &candidates {
        match with_retry(|| target_client.get_message(&msg.item_hash)).await {
            Ok(status) => {
                let s = status.status();
                match s {
                    // Already exists on target — false positive from ordering diff
                    MessageStatus::Processed
                    | MessageStatus::Removing
                    | MessageStatus::Removed
                    | MessageStatus::Pending
                    | MessageStatus::Forgotten => {
                        skipped += 1;
                    }
                    // Abnormal status — keep for sync and show the status
                    _ => {
                        eprintln!("  {} status on target: {s}", msg.item_hash);
                        truly_missing.push(msg);
                    }
                }
            }
            Err(MessageError::NotFound(_)) => {
                truly_missing.push(msg);
            }
            Err(e) => {
                eprintln!(
                    "  {} failed to verify on target: {e}, including in sync",
                    msg.item_hash
                );
                truly_missing.push(msg);
            }
        }
    }

    if skipped > 0 {
        eprintln!("  {skipped} already on target (ordering difference), skipped.");
    }
    eprintln!(
        "{} messages truly missing from target.",
        truly_missing.len()
    );

    if truly_missing.is_empty() {
        eprintln!("Nothing to sync.");
        return Ok(());
    }

    if args.dry_run {
        eprintln!("Dry run — would sync these messages:");
        for msg in &truly_missing {
            eprintln!(
                "  {} (type={}, sender={})",
                msg.item_hash, msg.message_type, msg.sender
            );
        }
        return Ok(());
    }

    // POST missing messages
    let mut success = 0u32;
    let mut errors = 0u32;
    for (i, msg) in truly_missing.iter().enumerate() {
        eprint!(
            "[{}/{}] Posting {} ... ",
            i + 1,
            truly_missing.len(),
            msg.item_hash
        );
        let pending = PendingMessage::from(*msg);
        match with_retry(|| target_client.post_message(&pending, false)).await {
            Ok(resp) => {
                eprintln!("{}", resp.message_status);
                success += 1;
            }
            Err(e) => {
                eprintln!("ERROR: {e}");
                errors += 1;
            }
        }
    }

    eprintln!("Done. {} synced, {} errors.", success, errors);
    if errors > 0 {
        return Err(format!("{errors} messages failed to sync").into());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let ccn_url = Url::parse(&cli.ccn_url).unwrap_or_else(|e| panic!("invalid CCN url: {e}"));
    let aleph_client = AlephClient::new(ccn_url.clone());
    let json = cli.json;

    match cli.command {
        cli::Commands::Message {
            command: message_command,
        } => handle_message_command(&aleph_client, &ccn_url, json, message_command).await?,
        cli::Commands::Post {
            command: post_command,
        } => handle_post_command(&aleph_client, &ccn_url, json, post_command).await?,
        cli::Commands::Aggregate {
            command: aggregate_command,
        } => handle_aggregate_command(&aleph_client, &ccn_url, json, aggregate_command).await?,
        cli::Commands::Node {
            command: node_command,
        } => handle_node_command(&aleph_client, &ccn_url, json, node_command).await?,
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_content_from_flag() {
        let value = read_content(Some(r#"{"key": "value"}"#.to_string())).unwrap();
        assert_eq!(value["key"], "value");
    }

    #[test]
    fn read_content_invalid_json() {
        assert!(read_content(Some("not json".to_string())).is_err());
    }

    #[test]
    fn read_content_nested_json() {
        let value = read_content(Some(r#"{"a": {"b": [1, 2, 3]}}"#.to_string())).unwrap();
        assert_eq!(value["a"]["b"][1], 2);
    }

    #[test]
    fn format_api_error_extracts_nested_message() {
        let body = r#"{"error":{"code":503,"message":"forget address does not match"},"message_status":"rejected"}"#;
        let formatted = format_api_error(422, body, false);
        assert_eq!(
            formatted,
            "Message rejected (HTTP 422): forget address does not match"
        );
    }

    #[test]
    fn format_api_error_extracts_top_level_message() {
        let body = r#"{"message":"bad request"}"#;
        let formatted = format_api_error(400, body, false);
        assert_eq!(formatted, "Message error (HTTP 400): bad request");
    }

    #[test]
    fn format_api_error_falls_back_to_raw_body() {
        let formatted = format_api_error(500, "internal server error", false);
        assert_eq!(formatted, "API error (HTTP 500): internal server error");
    }

    #[test]
    fn format_api_error_json_mode() {
        let body = r#"{"error":"something broke"}"#;
        let formatted = format_api_error(422, body, true);
        assert_eq!(formatted, "API request failed (HTTP 422)");
    }

    /// Verify that the post create envelope has the correct shape.
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

    /// Verify that the post amend envelope has ref and no type.
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

    /// Verify aggregate content must be a JSON object.
    #[test]
    fn aggregate_content_must_be_object() {
        assert!(!serde_json::json!("not an object").is_object());
        assert!(serde_json::json!({"setting": "value"}).is_object());
    }

    /// Verify forget envelope shape with all optional fields.
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
