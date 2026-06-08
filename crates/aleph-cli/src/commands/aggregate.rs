use crate::account::store::AccountStore;
use crate::cli::{
    AggregateCommand, AggregateCreateArgs, AggregateEditArgs, AggregateForgetArgs, AggregateGetArgs,
    AggregateListArgs,
};
use crate::common::{
    confirm_action, read_content, resolve_account, resolve_address, submit_or_preview,
};
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::{AlephAggregateClient, AlephClient, AlephMessageClient, MessageWithStatus};
use aleph_types::account::Account;
use aleph_types::chain::Address;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::MessageType;
use anyhow::{Result, anyhow, bail};
use serde_json::{Map, Value};
use url::Url;

/// Parse `--content` (or edited buffer) and validate it is well-formed JSON.
/// Any JSON value is accepted; a key's content is typically an object, but a
/// subkey value may be a scalar, array, or `null`.
fn parse_content_json(raw: &str) -> Result<Value> {
    serde_json::from_str(raw).map_err(|e| anyhow!("invalid JSON content: {e}"))
}

/// Compute the minimal merge patch turning `old` into `new`:
/// added/changed subkeys carry their new value; subkeys present in `old` but
/// absent from `new` are set to `null` (the network's delete-by-merge). Keys
/// unchanged between the two are omitted. An empty result means "no changes".
fn diff_to_patch(old: &Map<String, Value>, new: &Map<String, Value>) -> Map<String, Value> {
    let mut patch = Map::new();
    for (key, value) in new {
        match old.get(key) {
            Some(existing) if existing == value => {}
            _ => {
                patch.insert(key.clone(), value.clone());
            }
        }
    }
    for key in old.keys() {
        if !new.contains_key(key) {
            patch.insert(key.clone(), Value::Null);
        }
    }
    patch
}

/// Refuse to touch the `security` aggregate, which holds account
/// authorizations; hand-editing it can lock an account out.
fn reject_security_key(key: &str) -> Result<()> {
    if key == "security" {
        bail!(
            "the `security` aggregate holds account authorizations and cannot be \
             edited directly. Use `aleph authorization` to manage it."
        );
    }
    Ok(())
}

/// Fetch the current content stored at `(owner, key)`.
///
/// Returns `Some(content)` when the key exists, `None` when it does not
/// (`get_aggregate` returns the `{key: content}` data map; a missing key, a
/// `null` value, or a 404 all mean "does not exist").
async fn fetch_aggregate_content(
    aleph_client: &AlephClient,
    owner: &Address,
    key: &str,
) -> Result<Option<Value>> {
    match aleph_client.get_aggregate::<Value>(owner, key).await {
        Ok(Value::Object(mut data)) => match data.remove(key) {
            None | Some(Value::Null) => Ok(None),
            Some(content) => Ok(Some(content)),
        },
        Ok(_) => Ok(None),
        Err(e) if e.is_not_found() => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Resolve the user's editor: $VISUAL, then $EDITOR, then `vi`.
fn resolve_editor() -> String {
    std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| "vi".to_string())
}

/// Write `initial` to a temp file, open it in the resolved editor, and parse the
/// saved result as JSON. On a parse failure, the temp file is kept and its path
/// reported so the user's edits are not lost.
fn edit_via_editor(initial: &str) -> Result<Value> {
    use std::io::Write;
    let mut file = tempfile::Builder::new().suffix(".json").tempfile()?;
    file.write_all(initial.as_bytes())?;
    file.flush()?;
    let editor = resolve_editor();
    // `sh -c '<editor> "$1"' sh <path>` so compound editors (e.g. `code --wait`)
    // work and the path is passed safely as a positional argument.
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("{editor} \"$1\""))
        .arg("sh")
        .arg(file.path())
        .status()?;
    if !status.success() {
        bail!("editor `{editor}` exited with a non-zero status; aborting");
    }
    let edited = std::fs::read_to_string(file.path())?;
    match serde_json::from_str(&edited) {
        Ok(value) => Ok(value),
        Err(e) => {
            let kept = file.into_temp_path().keep()?;
            bail!(
                "edited content is not valid JSON: {e}. Your edits were saved to {}",
                kept.display()
            );
        }
    }
}

/// Diff `current` vs `desired` into the content to POST.
///
/// When both are JSON objects we emit a minimal merge patch (added/changed
/// subkeys, plus `null` for removed ones). When `desired` is not an object
/// (a key holding a scalar/array), we post it verbatim; there are no subkeys
/// to diff.
fn content_to_post(current: &Value, desired: &Value) -> Value {
    match (current.as_object(), desired.as_object()) {
        (Some(old), Some(new)) => Value::Object(diff_to_patch(old, new)),
        _ => desired.clone(),
    }
}

async fn handle_aggregate_edit(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: AggregateEditArgs,
) -> Result<()> {
    reject_security_key(&args.key)?;
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;
    let on_behalf_of = args
        .on_behalf_of
        .as_deref()
        .map(resolve_address)
        .transpose()?;
    let owner = on_behalf_of
        .clone()
        .unwrap_or_else(|| account.address().clone());

    let current = fetch_aggregate_content(aleph_client, &owner, &args.key)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "aggregate `{}` does not exist for {owner}. Use `aleph aggregate \
                 create --key {}` to create it.",
                args.key,
                args.key
            )
        })?;

    // Build the content to post (a merge patch).
    let patch: Value = match (args.subkey.as_deref(), args.content.as_deref()) {
        // Targeted subkey: post {subkey: content} verbatim (content may be null).
        (Some(subkey), Some(raw)) => {
            let value = parse_content_json(raw)?;
            let mut map = Map::new();
            map.insert(subkey.to_string(), value);
            Value::Object(map)
        }
        (Some(_), None) => bail!("--subkey requires --content (use `--content null` to delete it)"),
        // Whole-content replace: diff against current, nulling removed subkeys.
        (None, Some(raw)) => {
            let desired = parse_content_json(raw)?;
            content_to_post(&current, &desired)
        }
        // Interactive: open the current content in $EDITOR, then diff.
        (None, None) => {
            let initial = serde_json::to_string_pretty(&current)?;
            let edited = edit_via_editor(&initial)?;
            content_to_post(&current, &edited)
        }
    };

    // Only an empty *object* patch means "no changes"; a non-object replace
    // (a key holding a scalar/array) is always a real post.
    if matches!(&patch, Value::Object(m) if m.is_empty()) {
        eprintln!("No changes; nothing to submit.");
        return Ok(());
    }

    if !dry_run {
        let preview = serde_json::to_string_pretty(&patch)?;
        let prompt = format!("Apply this patch to `{}` for {owner}?\n{preview}", args.key);
        if !confirm_action(&prompt, args.yes)? {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    let envelope = serde_json::json!({ "key": args.key, "content": patch });
    let mut builder = MessageBuilder::new(&account, MessageType::Aggregate, envelope);
    if let Some(addr) = on_behalf_of {
        builder = builder.on_behalf_of(addr);
    }
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

pub async fn handle_aggregate_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: AggregateCommand,
) -> Result<()> {
    match command {
        AggregateCommand::Create(args) => {
            handle_aggregate_create(aleph_client, ccn_url, json, args).await?;
        }
        AggregateCommand::Edit(args) => {
            handle_aggregate_edit(aleph_client, ccn_url, json, args).await?;
        }
        AggregateCommand::Get(args) => {
            handle_aggregate_get(aleph_client, json, args).await?;
        }
        AggregateCommand::List(args) => {
            handle_aggregate_list(aleph_client, json, args).await?;
        }
        AggregateCommand::Forget(args) => {
            handle_aggregate_forget(aleph_client, ccn_url, json, args).await?;
        }
    }
    Ok(())
}

async fn handle_aggregate_create(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: AggregateCreateArgs,
) -> Result<()> {
    reject_security_key(&args.key)?;
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;
    let on_behalf_of = args
        .on_behalf_of
        .as_deref()
        .map(resolve_address)
        .transpose()?;
    let owner = on_behalf_of
        .clone()
        .unwrap_or_else(|| account.address().clone());

    // The existence check runs even for --dry-run: we want to catch "already
    // exists" before the user commits to the operation, not after.
    if fetch_aggregate_content(aleph_client, &owner, &args.key)
        .await?
        .is_some()
    {
        bail!(
            "aggregate `{}` already exists for {owner}. Use `aleph aggregate edit \
             --key {}` to change it.",
            args.key,
            args.key
        );
    }

    let content = read_content(args.content)?;
    let envelope = serde_json::json!({
        "key": args.key,
        "content": content,
    });
    let mut builder = MessageBuilder::new(&account, MessageType::Aggregate, envelope);
    if let Some(addr) = on_behalf_of {
        builder = builder.on_behalf_of(addr);
    }
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

async fn handle_aggregate_get(
    aleph_client: &AlephClient,
    json: bool,
    args: AggregateGetArgs,
) -> Result<()> {
    let address = resolve_owner_address(args.address.as_deref())?;
    let value: serde_json::Value = match aleph_client.get_aggregate(&address, &args.key).await {
        Ok(v) => v,
        Err(e) if e.is_not_found() => {
            eprintln!("No aggregate at {}/{}", address, args.key);
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    if json {
        println!("{}", serde_json::to_string(&value)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&value)?);
    }
    Ok(())
}

async fn handle_aggregate_list(
    aleph_client: &AlephClient,
    json: bool,
    args: AggregateListArgs,
) -> Result<()> {
    let address = resolve_owner_address(args.address.as_deref())?;
    let aggregates = aleph_client.get_all_aggregates(&address).await?;

    if json {
        println!("{}", serde_json::to_string(&aggregates)?);
        return Ok(());
    }

    if aggregates.is_empty() {
        eprintln!("No aggregates for {address}");
        return Ok(());
    }

    let mut keys: Vec<&String> = aggregates.keys().collect();
    keys.sort();
    for key in keys {
        println!("=== {key} ===");
        println!("{}", serde_json::to_string_pretty(&aggregates[key])?);
    }
    Ok(())
}

async fn handle_aggregate_forget(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: AggregateForgetArgs,
) -> Result<()> {
    if args.hashes.is_empty() {
        bail!("at least one hash is required");
    }

    let dry_run = args.signing.dry_run;
    validate_aggregate_hashes(aleph_client, &args.hashes).await?;

    if !dry_run {
        let n = args.hashes.len();
        let prompt = format!(
            "Forget {n} aggregate(s) in their entirety? This is irreversible: every \
             AGGREGATE element under each (sender, key) pair will be permanently deleted."
        );
        if !confirm_action(&prompt, args.yes)? {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    let account = resolve_account(&args.signing.identity)?;
    let agg_strs: Vec<String> = args.hashes.iter().map(|h| h.to_string()).collect();
    let mut envelope = serde_json::json!({
        "hashes": Vec::<String>::new(),
        "aggregates": agg_strs,
    });
    if let Some(reason) = args.reason {
        envelope["reason"] = serde_json::json!(reason);
    }
    let mut builder = MessageBuilder::new(&account, MessageType::Forget, envelope);
    if let Some(owner) = args.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

/// Verify each hash points at a processed AGGREGATE message before submitting
/// the forget. Bails with a targeted error otherwise so the user does not
/// produce a forget that the network will reject (or worse, that wipes the
/// wrong message type).
async fn validate_aggregate_hashes(aleph_client: &AlephClient, hashes: &[ItemHash]) -> Result<()> {
    for hash in hashes {
        let status = aleph_client.get_message(hash).await?;
        let message_type = match &status {
            MessageWithStatus::Processed { message }
            | MessageWithStatus::Removing { message, .. }
            | MessageWithStatus::Removed { message, .. } => message.message_type,
            MessageWithStatus::Pending { .. } => {
                bail!(
                    "{hash}: message is still pending and its type cannot be \
                     verified. Wait for it to be processed, or use `aleph \
                     message forget --aggregates {hash}` to skip validation."
                );
            }
            MessageWithStatus::Forgotten { .. } => {
                bail!("{hash}: message is already forgotten");
            }
            MessageWithStatus::Rejected { .. } => {
                bail!("{hash}: message was rejected by the network and cannot be forgotten");
            }
        };
        if message_type != MessageType::Aggregate {
            bail!(
                "{hash} is a {message_type:?} message, not an AGGREGATE. Use \
                 `aleph message forget {hash}` for non-aggregate messages."
            );
        }
    }
    Ok(())
}

/// Resolve the owner address for read-only aggregate queries.
///
/// Mirrors the precedence used by `aleph account balance`: explicit
/// `--address` (raw or local-name) wins, otherwise we use the default
/// account from the local store.
fn resolve_owner_address(args_address: Option<&str>) -> Result<Address> {
    if let Some(value) = args_address {
        return resolve_address(value);
    }
    let store = AccountStore::open().map_err(|e| anyhow!("failed to open account store: {e}"))?;
    let name = store.default_account_name()?.ok_or_else(|| {
        anyhow!(
            "no --address provided and no default account set; \
             pass --address or set a default with: aleph account use <NAME>"
        )
    })?;
    let entry = store.get_account(&name)?;
    Ok(Address::from(entry.address))
}

#[cfg(test)]
mod tests {
    use super::{diff_to_patch, parse_content_json, reject_security_key};
    use serde_json::{Map, Value, json};

    fn obj(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => panic!("not an object"),
        }
    }

    #[test]
    fn diff_adds_changes_and_nulls_removed() {
        let old = obj(json!({"a": 1, "keep": "x", "gone": true}));
        let new = obj(json!({"a": 2, "keep": "x", "added": 9}));
        let patch = diff_to_patch(&old, &new);
        // changed
        assert_eq!(patch.get("a"), Some(&json!(2)));
        // added
        assert_eq!(patch.get("added"), Some(&json!(9)));
        // removed -> explicit null
        assert_eq!(patch.get("gone"), Some(&Value::Null));
        // unchanged omitted
        assert!(!patch.contains_key("keep"));
    }

    #[test]
    fn diff_of_identical_is_empty() {
        let old = obj(json!({"a": 1}));
        let new = obj(json!({"a": 1}));
        assert!(diff_to_patch(&old, &new).is_empty());
    }

    #[test]
    fn parse_content_accepts_non_object_json() {
        // valid JSON, not a dict, must be accepted (e.g. a subkey value)
        assert_eq!(parse_content_json("42").unwrap(), json!(42));
        assert_eq!(parse_content_json("\"hi\"").unwrap(), json!("hi"));
        assert_eq!(parse_content_json("null").unwrap(), Value::Null);
        assert!(parse_content_json("not json").is_err());
    }

    #[test]
    fn security_key_is_rejected() {
        assert!(reject_security_key("security").is_err());
        assert!(reject_security_key("vm_images").is_ok());
    }

    #[test]
    fn forget_envelope_uses_aggregates_field() {
        let hashes = vec!["abc123".to_string()];
        let mut envelope = serde_json::json!({
            "hashes": Vec::<String>::new(),
            "aggregates": hashes,
        });
        envelope["reason"] = serde_json::json!("cleanup");
        assert!(envelope["hashes"].as_array().unwrap().is_empty());
        assert_eq!(envelope["aggregates"][0], "abc123");
        assert_eq!(envelope["reason"], "cleanup");
    }
}
