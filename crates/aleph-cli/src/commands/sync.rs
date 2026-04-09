use std::collections::HashSet;

use crate::cli::SyncArgs;
use crate::common::with_retry;
use aleph_sdk::client::{AlephClient, AlephMessageClient, MessageError};
use aleph_types::message::MessageStatus;
use aleph_types::message::pending::PendingMessage;
use url::Url;

pub async fn handle_sync(args: SyncArgs) -> Result<(), Box<dyn std::error::Error>> {
    let source_url = Url::parse(&args.source)?;
    let target_url = Url::parse(&args.target)?;

    let source_client = AlephClient::new(source_url);
    let target_client = AlephClient::new(target_url);

    let filter: aleph_sdk::client::MessageFilter = args.filter.into();
    let pagination = Some(args.count);

    // Fetch from both nodes concurrently
    eprintln!(
        "Fetching up to {} messages from source and target...",
        args.count
    );
    let (source_messages, target_messages) = tokio::try_join!(
        source_client.get_messages(&filter, pagination, Some(1)),
        target_client.get_messages(&filter, pagination, Some(1)),
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
    let mut truly_missing: Vec<&aleph_types::message::Message> = Vec::new();
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
