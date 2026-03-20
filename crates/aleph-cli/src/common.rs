use std::io::Read;
use std::time::Duration;

use aleph_sdk::client::{AlephMessageClient, MessageError, PostMessageResponse};
use aleph_types::message::pending::PendingMessage;
use url::Url;

/// Returns true if the error is an HTTP 429 Too Many Requests.
pub fn is_rate_limited(err: &MessageError) -> bool {
    matches!(err, MessageError::ApiError { status: 429, .. })
        || matches!(err, MessageError::HttpError(e) if e.status().is_some_and(|s| s == 429))
}

pub const MAX_RETRIES: u32 = 5;
pub const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Retry a fallible async operation with exponential backoff on 429s.
pub async fn with_retry<F, Fut, T>(mut f: F) -> Result<T, MessageError>
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
pub fn read_content(
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
pub async fn submit_or_preview(
    client: &aleph_sdk::client::AlephClient,
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
pub fn format_api_error(status: u16, body: &str, json: bool) -> String {
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

use crate::account::store::AccountStore;
use crate::account::{CliAccount, load_account, load_account_by_name};
use crate::cli::SigningArgs;
use crate::config::store::ConfigStore;

/// Resolve the CCN URL from CLI flags or config.
///
/// Resolution order:
/// 1. --ccn-url flag (explicit URL)
/// 2. --ccn flag (named CCN from config)
/// 3. default_ccn from config.toml
pub fn resolve_ccn_url(
    ccn_url: Option<&str>,
    ccn: Option<&str>,
) -> Result<Url, Box<dyn std::error::Error>> {
    // 1. Explicit URL
    if let Some(raw) = ccn_url {
        return Ok(Url::parse(raw).map_err(|e| format!("invalid --ccn-url: {e}"))?);
    }

    // open() seeds the built-in "official" entry, so there is always a default.
    let store =
        ConfigStore::open().map_err(|e| anyhow::anyhow!("failed to open config store: {e}"))?;

    // 2. Named CCN from --ccn flag
    if let Some(name) = ccn {
        let entry = store.get_ccn(name).map_err(|e| anyhow::anyhow!("{e}"))?;
        return Ok(
            Url::parse(&entry.url).map_err(|e| format!("invalid URL for CCN '{name}': {e}"))?
        );
    }

    // 3. Default from config (always set — open() ensures the built-in exists)
    let default_name = store
        .default_ccn_name()
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .expect("open() ensures a default CCN exists");

    let entry = store
        .get_ccn(&default_name)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(Url::parse(&entry.url).map_err(|e| format!("invalid URL for CCN '{default_name}': {e}"))?)
}

/// Resolve a signing account from CLI args.
///
/// Resolution order:
/// 1. --private-key flag or ALEPH_PRIVATE_KEY env var
/// 2. --account flag (named account from store)
/// 3. Default account from store
pub fn resolve_account(signing: &SigningArgs) -> Result<CliAccount, Box<dyn std::error::Error>> {
    // 1. Explicit private key takes precedence
    if signing.private_key.is_some() || std::env::var("ALEPH_PRIVATE_KEY").is_ok() {
        return Ok(load_account(
            signing.private_key.as_deref(),
            signing.chain.into(),
        )?);
    }

    // 2-3. Named account or default from store
    let store =
        AccountStore::open().map_err(|e| anyhow::anyhow!("failed to open account store: {e}"))?;

    let name = match &signing.account {
        Some(name) => name.clone(),
        None => store
            .default_account_name()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!(
                "no account specified and no default account set.\n\
                 Use --private-key, --account, or create an account with: aleph account create --name <NAME>"
            ))?
            .to_string(),
    };

    Ok(load_account_by_name(&store, &name)?)
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
}
