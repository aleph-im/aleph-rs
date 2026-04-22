use std::io::Read;
use std::time::Duration;

use aleph_sdk::client::{AlephMessageClient, MessageError, MessageWithStatus};
use aleph_types::item_hash::ItemHash;
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
            let rejection_code = if status == 422 && is_rejection_body(&body) {
                fetch_rejection_error_code(client, &pending.item_hash).await
            } else {
                None
            };
            return Err(format_api_error(status, &body, rejection_code, json).into());
        }
        Err(e) => return Err(e.into()),
    };

    print_submission_result(
        ccn_url,
        pending,
        &response.publication_status.status,
        &response.message_status,
        json,
    )
}

/// Emit the CLI's standard "message submitted" envelope.
///
/// Used by both the generic `submit_or_preview` path and the authenticated
/// file-upload path, which gets a server-side guarantee that the message has
/// been processed but no structured status response body to draw from.
pub fn print_submission_result(
    ccn_url: &Url,
    pending: &PendingMessage,
    publication_status: &str,
    message_status: &str,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if json {
        print_json_result(ccn_url, pending, publication_status, message_status)
    } else {
        print_human_result(ccn_url, pending, message_status);
        Ok(())
    }
}

fn print_json_result(
    ccn_url: &Url,
    pending: &PendingMessage,
    publication_status: &str,
    message_status: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let explorer_url = format!("{}api/v0/messages/{}", ccn_url.as_str(), pending.item_hash);
    let output = serde_json::json!({
        "item_hash": pending.item_hash.to_string(),
        "type": pending.message_type.to_string(),
        "chain": pending.chain.to_string(),
        "sender": pending.sender.to_string(),
        "channel": &pending.channel,
        "time": pending.time,
        "explorer_url": explorer_url,
        "publication_status": publication_status,
        "message_status": message_status,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn print_human_result(ccn_url: &Url, pending: &PendingMessage, message_status: &str) {
    let explorer_url = format!("{}api/v0/messages/{}", ccn_url.as_str(), pending.item_hash);
    eprintln!("Message {} ({})", message_status, pending.message_type);
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
///
/// `rejection_code` is the `error_code` from the rejected-message record when
/// the caller has already fetched it (see `fetch_rejection_error_code`). Pass
/// `None` when unknown.
pub fn format_api_error(
    status: u16,
    body: &str,
    rejection_code: Option<i64>,
    json: bool,
) -> String {
    if json {
        // In JSON mode, output structured error to stdout and return a short message
        let error_json = if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) {
            serde_json::json!({
                "error": parsed,
                "http_status": status,
                "rejection_code": rejection_code,
            })
        } else {
            serde_json::json!({
                "error": body,
                "http_status": status,
                "rejection_code": rejection_code,
            })
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

        if status_str == "rejected" {
            if let Some(code) = rejection_code {
                return format!(
                    "Message rejected by the CCN (HTTP {status}): {reason} (error code {code}).",
                    reason = describe_rejection_error_code(code),
                );
            }
            return format!(
                "Message rejected by the CCN (HTTP {status}) — no reason provided. \
                 Common causes: insufficient credit balance, invalid signature, \
                 or an unknown/forgotten reference (image, volume, T&C). \
                 Check your credits and inputs, then retry."
            );
        }
    }

    format!("API error (HTTP {status}): {body}")
}

/// Returns true if `body` is a CCN rejection envelope (`message_status: rejected`).
fn is_rejection_body(body: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v["message_status"].as_str().map(str::to_string))
        .is_some_and(|s| s == "rejected")
}

/// Best-effort lookup: fetch the rejected message from the CCN and return its
/// `error_code`. Returns `None` on any failure (message not yet queryable, network,
/// etc.) — the caller falls back to a generic hint.
async fn fetch_rejection_error_code(
    client: &aleph_sdk::client::AlephClient,
    hash: &ItemHash,
) -> Option<i64> {
    match client.get_message(hash).await.ok()? {
        MessageWithStatus::Rejected { error_code, .. } => Some(error_code),
        _ => None,
    }
}

/// Human-readable label for a pyaleph `ErrorCode`. Mirrors
/// `aleph.types.message_status.ErrorCode`.
fn describe_rejection_error_code(code: i64) -> &'static str {
    match code {
        -1 => "internal server error",
        0 => "invalid message format",
        1 => "invalid signature",
        2 => "permission denied",
        3 => "referenced content unavailable",
        4 => "referenced file unavailable",
        5 => "insufficient $ALEPH balance",
        6 => "insufficient credit balance",
        100 => "post amend: no target specified",
        101 => "post amend: target not found",
        102 => "post amend: cannot amend an amend",
        200 => "store: reference not found",
        201 => "store update: cannot update an update",
        202 => "invalid payment method",
        300 => "VM: reference not found",
        301 => "VM: volume not found",
        302 => "VM: amend not allowed",
        303 => "VM: cannot update an update",
        304 => "VM: volume too small",
        500 => "forget: no target specified",
        501 => "forget: target not found",
        502 => "forget: cannot forget a forget",
        503 => "forget: not allowed",
        504 => "message already forgotten",
        _ => "unknown rejection reason",
    }
}

use aleph_types::chain::Address;

use crate::account::store::AccountStore;
use crate::account::{CliAccount, load_account, load_account_by_name};
use crate::cli::SigningArgs;
use crate::config::store::ConfigStore;

/// Resolve the CCN URL using a provided `ConfigStore` (testable form).
///
/// Resolution order:
/// 1. `ccn_url` (explicit URL) — returned as-is, no network membership check.
///    This is an escape hatch for advanced use / quick tests.
/// 2. Select the network:
///    - `network` if provided — error if unknown.
///    - else `default_network` from config — error if unset.
/// 3. Within the selected network:
///    - `ccn` (named) → lookup within that network; `CcnNotFound` if absent.
///    - else → the network's `default_ccn`; error if unset.
pub fn resolve_ccn_url_with_store(
    store: &ConfigStore,
    ccn_url: Option<&str>,
    ccn: Option<&str>,
    network: Option<&str>,
) -> Result<Url, Box<dyn std::error::Error>> {
    if let Some(raw) = ccn_url {
        return Ok(Url::parse(raw).map_err(|e| format!("invalid --ccn-url: {e}"))?);
    }

    let network_name: String = match network {
        Some(n) => {
            store.get_network(n).map_err(|e| anyhow::anyhow!("{e}"))?;
            n.to_string()
        }
        None => store
            .default_network_name()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!(
                "no default network set; use: aleph config network use <NAME>"
            ))?,
    };

    let ccn_name = match ccn {
        Some(name) => name.to_string(),
        None => store
            .get_network(&network_name)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .default_ccn
            .ok_or_else(|| anyhow::anyhow!(
                "network '{network_name}' has no default CCN; use: aleph config ccn use <NAME>"
            ))?,
    };

    let entry = store
        .get_ccn(&network_name, &ccn_name)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(Url::parse(&entry.url)
        .map_err(|e| format!("invalid URL for CCN '{ccn_name}' in network '{network_name}': {e}"))?)
}

/// Resolve the CCN URL using the user-global config store. Call site: `main.rs`.
pub fn resolve_ccn_url(
    ccn_url: Option<&str>,
    ccn: Option<&str>,
    network: Option<&str>,
) -> Result<Url, Box<dyn std::error::Error>> {
    let store =
        ConfigStore::open().map_err(|e| anyhow::anyhow!("failed to open config store: {e}"))?;
    resolve_ccn_url_with_store(&store, ccn_url, ccn, network)
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

/// Resolve a user-supplied value to an address.
///
/// Accepts either a raw address (hex string starting with "0x"), an account
/// name, or an alias name from the local account store.
pub fn resolve_address(value: &str) -> Result<Address, Box<dyn std::error::Error>> {
    if value.starts_with("0x") || value.starts_with("0X") {
        return Ok(Address::from(value.to_string()));
    }

    let store =
        AccountStore::open().map_err(|e| anyhow::anyhow!("failed to open account store: {e}"))?;

    // Try account first, then alias.
    if let Ok(entry) = store.get_account(value) {
        return Ok(Address::from(entry.address));
    }
    if let Ok(alias) = store.get_alias(value) {
        return Ok(Address::from(alias.address));
    }

    Err(anyhow::anyhow!("'{value}' is not a valid address or known account/alias name").into())
}

/// Format a user-supplied address value for display.
///
/// If the input was an account name (resolved via the store), returns
/// `"name (0xABC...)"`. If it was already a raw address, returns it as-is.
pub fn format_address(input: &str, resolved: &Address) -> String {
    if input.starts_with("0x") || input.starts_with("0X") {
        resolved.to_string()
    } else {
        format!("{input} ({resolved})")
    }
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
        let formatted = format_api_error(422, body, None, false);
        assert_eq!(
            formatted,
            "Message rejected (HTTP 422): forget address does not match"
        );
    }

    #[test]
    fn format_api_error_extracts_top_level_message() {
        let body = r#"{"message":"bad request"}"#;
        let formatted = format_api_error(400, body, None, false);
        assert_eq!(formatted, "Message error (HTTP 400): bad request");
    }

    #[test]
    fn format_api_error_falls_back_to_raw_body() {
        let formatted = format_api_error(500, "internal server error", None, false);
        assert_eq!(formatted, "API error (HTTP 500): internal server error");
    }

    #[test]
    fn format_api_error_rejected_without_code_gives_generic_hint() {
        // No rejection_code → fall back to the generic hint, not the raw envelope.
        let body = r#"{"publication_status":{"status":"success","failed":[]},"message_status":"rejected"}"#;
        let formatted = format_api_error(422, body, None, false);
        assert!(formatted.contains("no reason provided"), "got: {formatted}");
        assert!(
            !formatted.contains("publication_status"),
            "got: {formatted}"
        );
    }

    #[test]
    fn format_api_error_rejected_with_code_surfaces_reason() {
        let body = r#"{"publication_status":{"status":"success","failed":[]},"message_status":"rejected"}"#;
        // Error code 6 = CREDIT_INSUFFICIENT.
        let formatted = format_api_error(422, body, Some(6), false);
        assert!(
            formatted.contains("insufficient credit balance"),
            "got: {formatted}",
        );
        assert!(formatted.contains("error code 6"), "got: {formatted}");
    }

    #[test]
    fn describe_rejection_error_code_covers_known_codes() {
        assert_eq!(
            describe_rejection_error_code(5),
            "insufficient $ALEPH balance"
        );
        assert_eq!(
            describe_rejection_error_code(6),
            "insufficient credit balance"
        );
        assert_eq!(describe_rejection_error_code(1), "invalid signature");
        assert_eq!(describe_rejection_error_code(301), "VM: volume not found");
        assert_eq!(
            describe_rejection_error_code(9999),
            "unknown rejection reason"
        );
    }

    #[test]
    fn is_rejection_body_detects_envelope() {
        assert!(is_rejection_body(r#"{"message_status":"rejected"}"#));
        assert!(!is_rejection_body(r#"{"message_status":"processed"}"#));
        assert!(!is_rejection_body("not json"));
        assert!(!is_rejection_body("{}"));
    }

    #[test]
    fn format_api_error_json_mode() {
        let body = r#"{"error":"something broke"}"#;
        let formatted = format_api_error(422, body, None, true);
        assert_eq!(formatted, "API request failed (HTTP 422)");
    }

    use crate::config::store::ConfigStore;
    use tempfile::TempDir;

    fn store_with_fixture() -> (TempDir, ConfigStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = ConfigStore::with_manifest_path(dir.path().join("config.toml"));
        // Seed mainnet + official manually (ensure_builtin is a private helper).
        store.add_network("mainnet").unwrap();
        store.add_ccn("mainnet", "official", "https://api.aleph.im").unwrap();
        store.add_network("testnet").unwrap();
        store.add_ccn("testnet", "local", "http://localhost:4024").unwrap();
        (dir, store)
    }

    #[test]
    fn ccn_url_escape_hatch_wins() {
        let (_dir, store) = store_with_fixture();
        let url = resolve_ccn_url_with_store(
            &store,
            Some("http://escape.example"),
            Some("official"),
            Some("mainnet"),
        )
        .unwrap();
        assert_eq!(url.as_str(), "http://escape.example/");
    }

    #[test]
    fn ccn_url_escape_hatch_skips_network_check() {
        let (_dir, store) = store_with_fixture();
        // Neither the ccn name nor a network is needed when --ccn-url is set.
        let url = resolve_ccn_url_with_store(&store, Some("http://escape.example"), None, None)
            .unwrap();
        assert_eq!(url.as_str(), "http://escape.example/");
    }

    #[test]
    fn network_plus_ccn_resolves_within_network() {
        let (_dir, store) = store_with_fixture();
        let url =
            resolve_ccn_url_with_store(&store, None, Some("local"), Some("testnet")).unwrap();
        assert_eq!(url.as_str(), "http://localhost:4024/");
    }

    #[test]
    fn network_only_uses_network_default_ccn() {
        let (_dir, store) = store_with_fixture();
        let url = resolve_ccn_url_with_store(&store, None, None, Some("mainnet")).unwrap();
        assert_eq!(url.as_str(), "https://api.aleph.im/");
    }

    #[test]
    fn no_flags_uses_default_network_default_ccn() {
        let (_dir, store) = store_with_fixture();
        // mainnet is the default (first network added).
        let url = resolve_ccn_url_with_store(&store, None, None, None).unwrap();
        assert_eq!(url.as_str(), "https://api.aleph.im/");
    }

    #[test]
    fn unknown_network_errors() {
        let (_dir, store) = store_with_fixture();
        let err =
            resolve_ccn_url_with_store(&store, None, None, Some("nope")).unwrap_err();
        assert!(err.to_string().contains("network 'nope' not found"));
    }

    #[test]
    fn unknown_ccn_in_selected_network_errors() {
        let (_dir, store) = store_with_fixture();
        let err =
            resolve_ccn_url_with_store(&store, None, Some("nope"), Some("mainnet")).unwrap_err();
        assert!(err.to_string().contains("ccn 'nope' not found in network 'mainnet'"));
    }

    #[test]
    fn ccn_lookup_does_not_fall_back_across_networks() {
        let (_dir, store) = store_with_fixture();
        // 'local' exists in testnet but not in mainnet.
        let err = resolve_ccn_url_with_store(&store, None, Some("local"), Some("mainnet"))
            .unwrap_err();
        assert!(err.to_string().contains("ccn 'local' not found in network 'mainnet'"));
    }
}
