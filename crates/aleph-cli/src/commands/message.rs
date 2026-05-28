use crate::cli::{ForgetArgs, GetMessageArgs, MessageCommand, RetryArgs, SigningArgs};
use crate::common::{
    confirm_action, repost_or_preview, resolve_account, resolve_address, submit_or_preview,
};
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::{AlephClient, AlephMessageClient, MessageWithStatus};
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;
use aleph_types::message::pending::PendingMessage;
use anyhow::{Result, anyhow, bail};
use futures_util::{StreamExt, TryStreamExt};
use url::Url;

pub async fn handle_message_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: MessageCommand,
) -> Result<()> {
    match command {
        MessageCommand::Get(GetMessageArgs { item_hash }) => {
            let message = aleph_client.get_message(&item_hash).await?;
            println!("{}", serde_json::to_string_pretty(&message)?);
        }
        MessageCommand::List(args) => {
            let messages: Vec<_> = aleph_client
                .get_messages_iterator(args.filter.into(), None)
                .take(args.count as usize)
                .try_collect()
                .await?;
            println!("{}", serde_json::to_string_pretty(&messages)?);
        }
        MessageCommand::Sync(sync_args) => {
            super::sync::handle_sync(*sync_args).await?;
        }
        MessageCommand::Forget(args) => {
            handle_forget(aleph_client, ccn_url, json, args).await?;
        }
        MessageCommand::Retry(args) => {
            handle_retry(aleph_client, ccn_url, json, args).await?;
        }
    }

    Ok(())
}

async fn handle_forget(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: ForgetArgs,
) -> Result<()> {
    forget_targets(
        aleph_client,
        ccn_url,
        json,
        ForgetTargets {
            hashes: args.hashes,
            aggregates: args.aggregates.unwrap_or_default(),
            reason: args.reason,
            channel: args.channel,
            on_behalf_of: args.on_behalf_of,
            yes: args.yes,
            confirm_label: "target",
            signing: args.signing,
        },
    )
    .await
}

pub struct ForgetTargets {
    pub hashes: Vec<ItemHash>,
    pub aggregates: Vec<ItemHash>,
    pub reason: Option<String>,
    pub channel: Option<String>,
    pub on_behalf_of: Option<String>,
    pub yes: bool,
    /// Singular noun used in the confirmation prompt (pluralized with `(s)`).
    pub confirm_label: &'static str,
    pub signing: SigningArgs,
}

pub async fn forget_targets(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    targets: ForgetTargets,
) -> Result<()> {
    let dry_run = targets.signing.dry_run;
    let total = targets.hashes.len() + targets.aggregates.len();
    if total == 0 {
        bail!("at least one hash is required");
    }
    if !dry_run {
        let prompt = format!(
            "Forget {total} {label}(s)? This is irreversible: content will be deleted permanently from the network.",
            label = targets.confirm_label,
        );
        if !confirm_action(&prompt, targets.yes)? {
            eprintln!("Aborted.");
            return Ok(());
        }
    }
    let account = resolve_account(&targets.signing.identity)?;
    let hashes: Vec<String> = targets.hashes.iter().map(|h| h.to_string()).collect();
    let mut envelope = serde_json::json!({
        "hashes": hashes,
    });
    if !targets.aggregates.is_empty() {
        let agg_strs: Vec<String> = targets.aggregates.iter().map(|h| h.to_string()).collect();
        envelope["aggregates"] = serde_json::json!(agg_strs);
    }
    if let Some(reason) = targets.reason {
        envelope["reason"] = serde_json::json!(reason);
    }
    let mut builder = MessageBuilder::new(&account, MessageType::Forget, envelope);
    if let Some(owner) = targets.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }
    if let Some(ch) = targets.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

async fn handle_retry(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: RetryArgs,
) -> Result<()> {
    let status = aleph_client.get_message(&args.item_hash).await?;
    match status {
        MessageWithStatus::Rejected {
            message,
            error_code: _,
        } => {
            let signature = message
                .signature
                .ok_or_else(|| anyhow!("rejected envelope has no signature; cannot retry"))?;
            let item_content = match message.item_type {
                ItemType::Inline => message.item_content.ok_or_else(|| {
                    anyhow!("rejected inline message envelope has no item_content; cannot retry")
                })?,
                ItemType::Storage | ItemType::Ipfs => String::new(),
            };
            let pending = PendingMessage {
                chain: message.chain,
                sender: message.sender,
                signature,
                message_type: message.message_type,
                item_type: message.item_type,
                item_content,
                item_hash: message.item_hash,
                time: message.time,
                channel: message.channel,
            };
            repost_or_preview(aleph_client, ccn_url, &pending, args.dry_run, json).await
        }
        other => bail!(
            "message {hash} is {status}, nothing to retry",
            hash = args.item_hash,
            status = other.status(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_sdk::client::AlephClient;
    use aleph_types::item_hash::ItemHash;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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

    const HASH: &str = "0000000000000000000000000000000000000000000000000000000000000001";

    fn item_hash() -> ItemHash {
        HASH.parse().unwrap()
    }

    /// Build the `{status, message: ...}` envelope returned by GET /api/v0/messages/{hash}.
    fn processed_envelope() -> serde_json::Value {
        serde_json::json!({
            "status": "processed",
            "message": {
                "sender": "0xABCD",
                "chain": "ETH",
                "signature": "0xSIG",
                "type": "POST",
                "item_type": "inline",
                "item_content": "{\"type\":\"test\",\"address\":\"0xABCD\",\"time\":1234.0}",
                "item_hash": HASH,
                "time": 1234.0,
                "channel": null,
                "content": {"type": "test", "address": "0xABCD", "time": 1234.0},
                "confirmed": true,
                "confirmations": [],
            }
        })
    }

    /// Build a `{status: rejected}` envelope for an inline message.
    fn rejected_inline_envelope(item_content: &str) -> serde_json::Value {
        serde_json::json!({
            "status": "rejected",
            "error_code": 6,
            "message": {
                "sender": "0xABCD",
                "chain": "ETH",
                "signature": "0xSIG",
                "type": "POST",
                "item_type": "inline",
                "item_content": item_content,
                "item_hash": HASH,
                "time": 1234.0,
                "channel": null,
                "content": null,
            }
        })
    }

    fn post_message_success() -> serde_json::Value {
        serde_json::json!({
            "publication_status": { "status": "success", "failed": [] },
            "message_status": "pending"
        })
    }

    #[tokio::test]
    async fn retry_inline_rejected_reposts_envelope() {
        let server = MockServer::start().await;
        let item_content = r#"{"type":"test","address":"0xABCD","time":1234.0}"#;

        // 1. GET status -> rejected
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/messages/{HASH}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(rejected_inline_envelope(item_content)),
            )
            .mount(&server)
            .await;

        // 2. POST /api/v0/messages with the reconstructed envelope.
        //    We assert the request body carries the same item_content (inline messages
        //    serialize it on the wire). The outer JSON wraps item_content as a string
        //    value, so we use body_partial_json to assert it by value.
        let post_mock = Mock::given(method("POST"))
            .and(path("/api/v0/messages"))
            .and(wiremock::matchers::body_partial_json(serde_json::json!({
                "message": { "item_content": item_content }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(post_message_success()))
            .expect(1);
        post_mock.mount(&server).await;

        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let ccn_url = Url::parse(&server.uri()).unwrap();
        let args = RetryArgs {
            item_hash: item_hash(),
            dry_run: false,
        };

        handle_retry(&client, &ccn_url, false, args)
            .await
            .expect("retry succeeds");
        // MockServer's `expect(1)` is verified on drop.
    }

    fn rejected_storage_envelope() -> serde_json::Value {
        serde_json::json!({
            "status": "rejected",
            "error_code": 6,
            "message": {
                "sender": "0xABCD",
                "chain": "ETH",
                "signature": "0xSIG",
                "type": "STORE",
                "item_type": "storage",
                "item_content": null,
                "item_hash": HASH,
                "time": 1234.0,
                "channel": null,
                "content": null,
            }
        })
    }

    /// wiremock matcher: rejects requests whose body contains the substring "item_content".
    /// We can't easily express absence via body_partial_json, so use a plain custom Match.
    struct NoItemContentInBody;
    impl wiremock::Match for NoItemContentInBody {
        fn matches(&self, req: &wiremock::Request) -> bool {
            let body = std::str::from_utf8(&req.body).unwrap_or("");
            !body.contains("\"item_content\"")
        }
    }

    #[tokio::test]
    async fn retry_storage_rejected_reposts_without_item_content() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(format!("/api/v0/messages/{HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(rejected_storage_envelope()))
            .mount(&server)
            .await;

        let post_mock = Mock::given(method("POST"))
            .and(path("/api/v0/messages"))
            .and(NoItemContentInBody)
            .respond_with(ResponseTemplate::new(200).set_body_json(post_message_success()))
            .expect(1);
        post_mock.mount(&server).await;

        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let ccn_url = Url::parse(&server.uri()).unwrap();
        let args = RetryArgs {
            item_hash: item_hash(),
            dry_run: false,
        };

        handle_retry(&client, &ccn_url, false, args)
            .await
            .expect("retry succeeds");
    }

    #[tokio::test]
    async fn retry_rejected_missing_signature_bails() {
        let server = MockServer::start().await;
        let body = serde_json::json!({
            "status": "rejected",
            "error_code": 1,
            "message": {
                "sender": "0xABCD",
                "chain": "ETH",
                "signature": null,
                "type": "POST",
                "item_type": "inline",
                "item_content": "{}",
                "item_hash": HASH,
                "time": 1234.0,
                "channel": null,
                "content": null,
            }
        });
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/messages/{HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let ccn_url = Url::parse(&server.uri()).unwrap();
        let args = RetryArgs {
            item_hash: item_hash(),
            dry_run: false,
        };

        let err = handle_retry(&client, &ccn_url, false, args)
            .await
            .expect_err("expected missing-signature bail");
        assert!(err.to_string().contains("signature"), "got: {err}");
    }

    #[tokio::test]
    async fn retry_inline_rejected_missing_item_content_bails() {
        let server = MockServer::start().await;
        let body = serde_json::json!({
            "status": "rejected",
            "error_code": 0,
            "message": {
                "sender": "0xABCD",
                "chain": "ETH",
                "signature": "0xSIG",
                "type": "POST",
                "item_type": "inline",
                "item_content": null,
                "item_hash": HASH,
                "time": 1234.0,
                "channel": null,
                "content": null,
            }
        });
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/messages/{HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let ccn_url = Url::parse(&server.uri()).unwrap();
        let args = RetryArgs {
            item_hash: item_hash(),
            dry_run: false,
        };

        let err = handle_retry(&client, &ccn_url, false, args)
            .await
            .expect_err("expected missing-item_content bail");
        assert!(err.to_string().contains("item_content"), "got: {err}");
    }

    #[tokio::test]
    async fn retry_dry_run_does_not_post() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/messages/{HASH}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(rejected_inline_envelope(r#"{"x":1}"#)),
            )
            .mount(&server)
            .await;

        // If a POST were issued, wiremock returns 404 for unmocked paths and
        // repost_or_preview would surface that as an error. We assert success,
        // proving the dry-run path skipped the POST entirely.
        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let ccn_url = Url::parse(&server.uri()).unwrap();
        let args = RetryArgs {
            item_hash: item_hash(),
            dry_run: true,
        };

        handle_retry(&client, &ccn_url, false, args)
            .await
            .expect("dry-run succeeds without POSTing");
    }

    #[tokio::test]
    async fn retry_non_rejected_bails_with_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/messages/{HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(processed_envelope()))
            .mount(&server)
            .await;

        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let ccn_url = Url::parse(&server.uri()).unwrap();
        let args = RetryArgs {
            item_hash: item_hash(),
            dry_run: false,
        };

        let err = handle_retry(&client, &ccn_url, false, args)
            .await
            .expect_err("expected non-rejected status to bail");
        let msg = err.to_string();
        assert!(msg.contains("processed"), "got: {msg}");
        assert!(msg.contains("nothing to retry"), "got: {msg}");
    }
}
