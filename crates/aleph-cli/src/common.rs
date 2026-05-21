use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aleph_sdk::client::{AlephMessageClient, MessageError, MessageWithStatus};
use aleph_types::item_hash::ItemHash;
use aleph_types::message::pending::PendingMessage;
use anyhow::{Result, anyhow, bail};
use url::Url;

/// Current Unix time as fractional seconds, matching the float `updated_at`
/// field used across the dashboard's aggregates (`websites`, `domains`, ...).
pub fn now_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Format a fractional Unix timestamp (as stored in dashboard aggregates) as
/// `YYYY-MM-DD HH:MM:SS UTC` for TTY display. Falls back to the raw float
/// formatting if the value is out of the chrono range.
pub fn format_epoch_for_tty(secs: f64) -> String {
    chrono::DateTime::from_timestamp(secs as i64, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| secs.to_string())
}

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
pub fn read_content(content_flag: Option<String>) -> Result<serde_json::Value> {
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

/// Interactive yes/no confirmation for destructive commands.
///
/// Prints `prompt` to stderr and reads a line from stdin. Returns `Ok(true)`
/// only on `y` or `yes` (case-insensitive). If `assume_yes` is true (i.e. the
/// caller passed `--yes`), skips the prompt and returns `Ok(true)`.
///
/// Errors only on stdin read failure — not on a "no" answer.
pub fn confirm_action(prompt: &str, assume_yes: bool) -> Result<bool, std::io::Error> {
    if assume_yes {
        return Ok(true);
    }
    eprint!("{prompt} [y/N]: ");
    use std::io::Write;
    let _ = std::io::stderr().flush();
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    let trimmed = answer.trim().to_ascii_lowercase();
    Ok(trimmed == "y" || trimmed == "yes")
}

/// Stricter confirmation: the user must type `expected` verbatim. Used when
/// the action is irreversible enough that a reflexive `y` would be a problem
/// (deleting a key, exposing one to the terminal). `warning` is printed
/// before the read so the user sees *why* the prompt is asking.
///
/// Returns `Ok(true)` on a verbatim match or when `assume_yes` is true
/// (i.e. the caller passed `--yes`); `Ok(false)` on anything else.
pub fn confirm_typed_match(
    warning: &str,
    expected: &str,
    assume_yes: bool,
) -> Result<bool, std::io::Error> {
    if assume_yes {
        return Ok(true);
    }
    eprintln!("{warning}");
    eprint!("Type '{expected}' to confirm: ");
    use std::io::Write;
    let _ = std::io::stderr().flush();
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    Ok(answer.trim() == expected)
}

/// Submit a signed message, or print it if --dry-run.
/// Handles --json vs human-readable output.
pub async fn submit_or_preview(
    client: &aleph_sdk::client::AlephClient,
    ccn_url: &Url,
    pending: &PendingMessage,
    dry_run: bool,
    json: bool,
) -> Result<()> {
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
            bail!("{}", format_api_error(status, &body, rejection_code, json));
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

/// Report the outcome of an authenticated file upload (`/api/v0/{storage,ipfs}/add_file`).
///
/// The upload endpoint returns 2xx once the file is pinned and the STORE
/// message has been queued for ingest; it does NOT carry final processing
/// status in the response body. To avoid lying to the user when the message
/// is subsequently rejected (e.g. insufficient credits), we follow up with a
/// status fetch via `get_message` and surface the real outcome.
///
/// On `Rejected`, bails with a formatted error mirroring `submit_or_preview`.
/// On any other variant, prints the standard submission envelope.
pub async fn report_authenticated_upload_status(
    client: &aleph_sdk::client::AlephClient,
    ccn_url: &Url,
    pending: &PendingMessage,
    json: bool,
) -> Result<()> {
    let status = client.get_message(&pending.item_hash).await?;
    match status {
        MessageWithStatus::Rejected { error_code, .. } => {
            let explorer = format!("{}api/v0/messages/{}", ccn_url.as_str(), pending.item_hash);
            if json {
                let envelope = serde_json::json!({
                    "error": "message rejected",
                    "rejection_code": error_code,
                    "rejection_reason": describe_rejection_error_code(error_code),
                    "item_hash": pending.item_hash.to_string(),
                    "explorer_url": explorer,
                });
                println!("{}", serde_json::to_string_pretty(&envelope)?);
                bail!("Message rejected by the CCN (error code {error_code})");
            }
            bail!(
                "Message rejected by the CCN: {reason} (error code {error_code}).\nSee: {explorer}",
                reason = describe_rejection_error_code(error_code),
            );
        }
        MessageWithStatus::Processed { .. } => {
            print_submission_result(ccn_url, pending, "success", "processed", json)
        }
        MessageWithStatus::Pending { .. } => {
            print_submission_result(ccn_url, pending, "success", "pending", json)
        }
        MessageWithStatus::Removing { .. } => {
            print_submission_result(ccn_url, pending, "success", "removing", json)
        }
        MessageWithStatus::Removed { .. } => {
            print_submission_result(ccn_url, pending, "success", "removed", json)
        }
        MessageWithStatus::Forgotten { .. } => {
            print_submission_result(ccn_url, pending, "success", "forgotten", json)
        }
    }
}

/// Emit the CLI's standard "message submitted" envelope.
///
/// Used by `submit_or_preview` and by `report_authenticated_upload_status`
/// for the post-upload status fetch.
pub fn print_submission_result(
    ccn_url: &Url,
    pending: &PendingMessage,
    publication_status: &str,
    message_status: &str,
    json: bool,
) -> Result<()> {
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
) -> Result<()> {
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
use crate::cli::IdentityArgs;
use crate::config::store::ConfigStore;

/// Resolve the CCN URL using a provided `ConfigStore` (testable form).
///
/// The single `--ccn` flag accepts either a raw URL (anything containing
/// `://`) or a config alias name (looked up within the selected network).
///
/// Resolution order:
/// 1. If `ccn` is `Some(s)` and `s` contains `://`: parse `s` as a URL and
///    return — no network membership check. This is the escape hatch for
///    advanced use / quick tests.
/// 2. Select the network:
///    - `network` if provided — error if unknown.
///    - else `default_network` from config — error if unset.
/// 3. Within the selected network:
///    - `ccn` (named alias) → lookup within that network; `CcnNotFound` if absent.
///    - else → the network's `default_ccn`; error if unset.
pub fn resolve_ccn_url_with_store(
    store: &ConfigStore,
    ccn: Option<&str>,
    network: Option<&str>,
) -> Result<Url> {
    // URL form: anything containing "://" is treated as a raw URL.
    if let Some(raw) = ccn
        && raw.contains("://")
    {
        return Url::parse(raw).map_err(|e| anyhow!("invalid --ccn URL '{raw}': {e}"));
    }

    let (network_name, network_entry) = match network {
        Some(n) => {
            let entry = store.get_network(n).map_err(|e| anyhow!("{e}"))?;
            (n.to_string(), entry)
        }
        None => {
            let name = store
                .default_network_name()
                .map_err(|e| anyhow!("{e}"))?
                .ok_or_else(|| {
                    anyhow!("no default network set; use: aleph config network use <NAME>")
                })?;
            let entry = store.get_network(&name).map_err(|e| anyhow!("{e}"))?;
            (name, entry)
        }
    };

    let ccn_name = match ccn {
        Some(name) => name.to_string(),
        None => network_entry.default_ccn.ok_or_else(|| {
            anyhow!("network '{network_name}' has no default CCN; use: aleph config ccn use <NAME>")
        })?,
    };

    let entry = store.get_ccn(&network_name, &ccn_name).map_err(|e| {
        anyhow!("{e} (and '{ccn_name}' doesn't look like a URL — missing scheme like https://)")
    })?;
    Url::parse(&entry.url)
        .map_err(|e| anyhow!("invalid URL for CCN '{ccn_name}' in network '{network_name}': {e}"))
}

/// Resolve the CCN URL using the user-global config store. Call site: `main.rs`.
pub fn resolve_ccn_url(ccn: Option<&str>, network: Option<&str>) -> Result<Url> {
    let store = ConfigStore::open().map_err(|e| anyhow!("failed to open config store: {e}"))?;
    resolve_ccn_url_with_store(&store, ccn, network)
}

/// Resolve a network entry from an explicit name or the config's current default.
///
/// Resolution order: `network_override` (e.g. top-level `--network`) >
/// current `default_network` from config. Errors if neither is set or the
/// named network is unknown.
pub fn resolve_network_with_store(
    store: &ConfigStore,
    network_override: Option<&str>,
) -> Result<crate::config::store::NetworkEntry> {
    let name = match network_override {
        Some(n) => n.to_string(),
        None => store
            .default_network_name()
            .map_err(|e| anyhow!("{e}"))?
            .ok_or_else(|| {
                anyhow!("no default network set; use: aleph config network use <NAME>")
            })?,
    };
    store.get_network(&name).map_err(|e| anyhow!("{e}"))
}

/// Resolve a network entry using the user-global config store.
pub fn resolve_network(
    network_override: Option<&str>,
) -> Result<crate::config::store::NetworkEntry> {
    let store = ConfigStore::open().map_err(|e| anyhow!("failed to open config store: {e}"))?;
    resolve_network_with_store(&store, network_override)
}

/// Resolve the Aleph VM scheduler base URL for a network.
pub fn resolve_scheduler_url_with_store(
    store: &ConfigStore,
    network_override: Option<&str>,
) -> Result<Url> {
    let net = resolve_network_with_store(store, network_override)?;
    Url::parse(&net.scheduler_url)
        .map_err(|e| anyhow!("invalid scheduler_url for network '{}': {e}", net.name))
}

/// Resolve the scheduler URL using the user-global config store.
pub fn resolve_scheduler_url(network_override: Option<&str>) -> Result<Url> {
    let store = ConfigStore::open().map_err(|e| anyhow!("failed to open config store: {e}"))?;
    resolve_scheduler_url_with_store(&store, network_override)
}

/// Resolve a signing account from CLI args.
///
/// Resolution order:
/// 1. --private-key flag or ALEPH_PRIVATE_KEY env var (requires --chain)
/// 2. --account flag (named account from store)
/// 3. Default account from store
pub fn resolve_account(identity: &IdentityArgs) -> Result<CliAccount> {
    // 1. Explicit private key takes precedence
    if identity.private_key.is_some() || std::env::var("ALEPH_PRIVATE_KEY").is_ok() {
        let chain = identity.chain.ok_or_else(|| {
            anyhow!("--chain is required when signing with --private-key (or ALEPH_PRIVATE_KEY)")
        })?;
        return load_account(identity.private_key.as_deref(), chain.into());
    }

    // 2-3. Named account or default from store
    let store = AccountStore::open().map_err(|e| anyhow!("failed to open account store: {e}"))?;

    let name = match &identity.account {
        Some(name) => name.clone(),
        None => store
            .default_account_name()
            .map_err(|e| anyhow!("{e}"))?
            .ok_or_else(|| anyhow!(
                "no account specified and no default account set.\n\
                 Use --private-key, --account, or create an account with: aleph account create <NAME>"
            ))?
            .to_string(),
    };

    load_account_by_name(&store, &name)
}

/// Resolve an address from an explicit `--address` value, falling back to the
/// active (default) account when no value is supplied.
///
/// Used by read-only listing commands that operate on the signed-in user's
/// data by default but accept `--address <NAME-OR-HEX>` to inspect another
/// account. Returns a clear error when no override is given and no default
/// account is configured, telling the caller exactly what to do.
pub fn resolve_address_or_active(maybe_addr: Option<&str>) -> Result<Address> {
    if let Some(value) = maybe_addr {
        return resolve_address(value);
    }
    let store =
        AccountStore::open().map_err(|e| anyhow::anyhow!("failed to open account store: {e}"))?;
    let name = store
        .default_account_name()
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no --address given and no default account set; \
                 use --address <ADDRESS> or: aleph account use <NAME>"
            )
        })?;
    let entry = store
        .get_account(&name)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(Address::from(entry.address))
}

/// Resolve a user-supplied value to an address using a provided `AccountStore`
/// (testable form).
///
/// Accepts either a raw address (hex string starting with "0x"), an account
/// name, or an alias name from the store.
pub fn resolve_address_with_store(store: &AccountStore, value: &str) -> Result<Address> {
    if value.starts_with("0x") || value.starts_with("0X") {
        return Ok(Address::from(value.to_string()));
    }

    // Try account first, then alias.
    if let Ok(entry) = store.get_account(value) {
        return Ok(Address::from(entry.address));
    }
    if let Ok(alias) = store.get_alias(value) {
        return Ok(Address::from(alias.address));
    }

    Err(anyhow!(
        "'{value}' is not a valid address or known account/alias name"
    ))
}

/// Resolve a user-supplied value to an address using the user-global account
/// store.
///
/// Accepts either a raw address (hex string starting with "0x"), an account
/// name, or an alias name from the local account store.
pub fn resolve_address(value: &str) -> Result<Address> {
    // Hex addresses don't need the store; skip the open() to avoid touching
    // disk for the common case.
    if value.starts_with("0x") || value.starts_with("0X") {
        return Ok(Address::from(value.to_string()));
    }
    let store = AccountStore::open().map_err(|e| anyhow!("failed to open account store: {e}"))?;
    resolve_address_with_store(&store, value)
}

/// Prompt the user for `y/N` confirmation on a TTY.
///
/// Returns `Ok(true)` if the user types `y` / `yes` (case-insensitive),
/// `Ok(false)` for anything else (including an empty line - the default is
/// "no"). Errors when stdin is not a terminal: callers should pass `--yes`
/// to confirm non-interactively rather than silently proceed or skip.
///
/// The prompt is written to stderr so it doesn't pollute stdout (which may
/// be captured by tooling expecting JSON output).
pub fn confirm_tty(prompt: &str) -> Result<bool> {
    use std::io::{self, IsTerminal, Write};
    if !io::stdin().is_terminal() {
        return Err(anyhow!(
            "{} refusing to prompt without a TTY; pass --yes to confirm non-interactively",
            prompt
        ));
    }
    eprint!("{} [y/N] ", prompt);
    io::stderr().flush()?;
    let mut s = String::new();
    io::stdin().read_line(&mut s)?;
    Ok(matches!(
        s.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
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
    use crate::config::store::ConfigStore;
    use tempfile::TempDir;

    #[test]
    fn confirm_action_short_circuits_when_assume_yes() {
        // No stdin read happens — verifies the --yes path is purely synchronous.
        assert!(confirm_action("Delete everything?", true).unwrap());
    }

    #[test]
    fn confirm_typed_match_short_circuits_when_assume_yes() {
        assert!(confirm_typed_match("WARNING", "expected", true).unwrap());
    }

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

    fn store_with_fixture() -> (TempDir, ConfigStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = ConfigStore::with_manifest_path(dir.path().join("config.toml"));
        // Seed mainnet + official manually (ensure_builtin is a private helper).
        store.add_network("mainnet").unwrap();
        store
            .add_ccn("mainnet", "official", "https://api.aleph.im")
            .unwrap();
        store.add_network("testnet").unwrap();
        store
            .add_ccn("testnet", "local", "http://localhost:4024")
            .unwrap();
        (dir, store)
    }

    #[test]
    fn scheduler_url_resolves_to_seeded_default() {
        // Mirrors the fresh-config path: `ConfigStore::open()` calls
        // `ensure_builtin()`, which seeds mainnet via `add_network`. That
        // helper populates `scheduler_url` with `BUILTIN_SCHEDULER_URL`, so
        // `aleph instance list` against a brand-new config must succeed
        // without the user ever running `aleph config network` commands.
        let (_dir, store) = store_with_fixture();
        let url = resolve_scheduler_url_with_store(&store, None).unwrap();
        assert_eq!(
            url.as_str(),
            crate::config::store::BUILTIN_SCHEDULER_URL
                .trim_end_matches('/')
                .to_string()
                + "/"
        );
    }

    #[test]
    fn scheduler_url_honors_explicit_override_value() {
        let (_dir, store) = store_with_fixture();
        store
            .set_network_scheduler_url("testnet", "https://scheduler.test/")
            .unwrap();
        let url = resolve_scheduler_url_with_store(&store, Some("testnet")).unwrap();
        assert_eq!(url.as_str(), "https://scheduler.test/");
    }

    #[test]
    fn ccn_url_form_skips_network_check() {
        let (_dir, store) = store_with_fixture();
        // A value containing "://" is treated as a raw URL; no network or
        // alias lookup is performed.
        let url = resolve_ccn_url_with_store(&store, Some("http://escape.example"), None).unwrap();
        assert_eq!(url.as_str(), "http://escape.example/");
    }

    #[test]
    fn ccn_url_form_works_even_with_network_set() {
        let (_dir, store) = store_with_fixture();
        // Passing --network alongside a URL value still uses the URL.
        let url =
            resolve_ccn_url_with_store(&store, Some("http://escape.example"), Some("mainnet"))
                .unwrap();
        assert_eq!(url.as_str(), "http://escape.example/");
    }

    #[test]
    fn ccn_url_form_rejects_unparseable_url() {
        let (_dir, store) = store_with_fixture();
        // Contains "://" → treated as URL → must parse cleanly.
        let err = resolve_ccn_url_with_store(&store, Some("://broken"), None).unwrap_err();
        assert!(
            err.to_string().contains("invalid --ccn URL"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn network_plus_ccn_resolves_within_network() {
        let (_dir, store) = store_with_fixture();
        let url = resolve_ccn_url_with_store(&store, Some("local"), Some("testnet")).unwrap();
        assert_eq!(url.as_str(), "http://localhost:4024/");
    }

    #[test]
    fn network_only_uses_network_default_ccn() {
        let (_dir, store) = store_with_fixture();
        let url = resolve_ccn_url_with_store(&store, None, Some("mainnet")).unwrap();
        assert_eq!(url.as_str(), "https://api.aleph.im/");
    }

    #[test]
    fn no_flags_uses_default_network_default_ccn() {
        let (_dir, store) = store_with_fixture();
        // mainnet is the default (first network added).
        let url = resolve_ccn_url_with_store(&store, None, None).unwrap();
        assert_eq!(url.as_str(), "https://api.aleph.im/");
    }

    #[test]
    fn unknown_network_errors() {
        let (_dir, store) = store_with_fixture();
        let err = resolve_ccn_url_with_store(&store, None, Some("nope")).unwrap_err();
        assert!(err.to_string().contains("network 'nope' not found"));
    }

    #[test]
    fn unknown_ccn_in_selected_network_errors() {
        let (_dir, store) = store_with_fixture();
        let err = resolve_ccn_url_with_store(&store, Some("nope"), Some("mainnet")).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("ccn 'nope' not found in network 'mainnet'"),
            "unexpected error: {msg}"
        );
        // The hint about a missing URL scheme should also be present so a user
        // who mistyped a URL like "api.example.com" gets a useful nudge.
        assert!(
            msg.contains("missing scheme like https://"),
            "expected URL-scheme hint in error: {msg}"
        );
    }

    #[test]
    fn ccn_lookup_does_not_fall_back_across_networks() {
        let (_dir, store) = store_with_fixture();
        // 'local' exists in testnet but not in mainnet.
        let err = resolve_ccn_url_with_store(&store, Some("local"), Some("mainnet")).unwrap_err();
        assert!(
            err.to_string()
                .contains("ccn 'local' not found in network 'mainnet'")
        );
    }

    #[test]
    fn network_without_default_ccn_errors() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConfigStore::with_manifest_path(dir.path().join("config.toml"));
        // Add a network but no CCN — so it has no default_ccn.
        store.add_network("barenet").unwrap();
        let err = resolve_ccn_url_with_store(&store, None, Some("barenet")).unwrap_err();
        assert!(
            err.to_string()
                .contains("network 'barenet' has no default CCN"),
            "unexpected error: {err}"
        );
    }

    fn temp_account_store() -> (TempDir, AccountStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = AccountStore::with_manifest_path(dir.path().join("accounts.toml"));
        (dir, store)
    }

    #[test]
    fn resolve_address_accepts_hex() {
        let (_dir, store) = temp_account_store();
        let addr = resolve_address_with_store(&store, "0xABCD1234").unwrap();
        assert_eq!(addr.to_string(), "0xABCD1234");
    }

    #[test]
    fn resolve_address_accepts_uppercase_prefix() {
        let (_dir, store) = temp_account_store();
        let addr = resolve_address_with_store(&store, "0XdeadBEEF").unwrap();
        assert_eq!(addr.to_string(), "0XdeadBEEF");
    }

    #[test]
    fn resolve_address_finds_account_by_name() {
        let (_dir, store) = temp_account_store();
        // add_ledger_account doesn't touch the OS keyring.
        store
            .add_ledger_account(
                "alice",
                aleph_types::chain::Chain::Ethereum,
                "0xAAAA1111".to_string(),
                "m/44'/60'/0'/0/0".to_string(),
            )
            .unwrap();

        let addr = resolve_address_with_store(&store, "alice").unwrap();
        assert_eq!(addr.to_string(), "0xAAAA1111");
    }

    #[test]
    fn resolve_address_finds_alias_by_name() {
        let (_dir, store) = temp_account_store();
        store
            .add_alias("treasurer", "0xBBBB2222".to_string())
            .unwrap();

        let addr = resolve_address_with_store(&store, "treasurer").unwrap();
        assert_eq!(addr.to_string(), "0xBBBB2222");
    }

    #[test]
    fn resolve_address_rejects_unknown_name() {
        let (_dir, store) = temp_account_store();
        let err = resolve_address_with_store(&store, "nobody").unwrap_err();
        assert!(
            err.to_string()
                .contains("'nobody' is not a valid address or known account/alias name"),
            "unexpected error: {err}"
        );
    }
}
