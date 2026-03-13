use std::collections::HashSet;
use std::time::Duration;

use crate::cli::{Cli, GetMessageArgs, MessageCommand, PostCommand};
use aleph_sdk::client::{AlephClient, AlephMessageClient, AlephPostClient, MessageError};
use aleph_types::message::{Message, MessageStatus};
use clap::Parser;
use url::Url;

/// Returns true if the error is an HTTP 429 Too Many Requests.
fn is_rate_limited(err: &MessageError) -> bool {
    matches!(err, MessageError::HttpError(e) if e.status().is_some_and(|s| s == 429))
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

mod cli;

async fn handle_message_command(
    aleph_client: &AlephClient,
    command: MessageCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        MessageCommand::Get(GetMessageArgs { item_hash }) => {
            let message = aleph_client.get_message(&item_hash).await?;
            let serialized_message = serde_json::to_string_pretty(&message)?;
            println!("{}", serialized_message);
        }
        MessageCommand::List(message_filter) => {
            let messages = aleph_client.get_messages(&(*message_filter).into()).await?;
            let serialized_messages = serde_json::to_string_pretty(&messages)?;
            println!("{}", serialized_messages);
        }
        MessageCommand::Sync(sync_args) => {
            handle_sync(*sync_args).await?;
        }
    }

    Ok(())
}

async fn handle_post_command(
    aleph_client: &AlephClient,
    command: PostCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        PostCommand::List(args) => {
            let filter = args.filter.into();
            match args.api_version {
                0 => {
                    let response = aleph_client.get_posts_v0(&filter).await?;
                    let serialized = serde_json::to_string_pretty(&response.posts)?;
                    println!("{}", serialized);
                }
                1 => {
                    let response = aleph_client.get_posts_v1(&filter).await?;
                    let serialized = serde_json::to_string_pretty(&response.posts)?;
                    println!("{}", serialized);
                }
                v => {
                    return Err(format!("unsupported API version: {v} (expected 0 or 1)").into());
                }
            }
        }
    }

    Ok(())
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
        match with_retry(|| target_client.post_message(msg, false)).await {
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
    let ccn_url =
        Url::parse("https://api3.aleph.im").unwrap_or_else(|e| panic!("invalid CCN url: {e}"));
    let aleph_client = AlephClient::new(ccn_url);

    match cli.command {
        cli::Commands::Message {
            command: message_command,
        } => handle_message_command(&aleph_client, message_command).await?,
        cli::Commands::Post {
            command: post_command,
        } => handle_post_command(&aleph_client, post_command).await?,
    }

    Ok(())
}
