//! Abstract content handler trait. Mirrors
//! `aleph/handlers/content/content_handler.py`.
//!
//! Each per-content-type handler (aggregate, forget, post, store, vm)
//! implements this trait. The orchestrator in
//! [`crate::handlers::message_handler`] uses these methods to drive the
//! processing pipeline.

use async_trait::async_trait;
use std::collections::HashSet;

use crate::AlephResult;
use crate::db::models::account_costs::AccountCostsDb;
use crate::db::models::messages::MessageDb;
use crate::permissions::{AuthorityLookup, MessageForAuth};
use crate::types::message_status::MessageProcessingException;

/// Minimal view that exposes the fields used during message permission checks.
///
/// `MessageDb` does not carry parsed content; the orchestrator builds this
/// view by inspecting the JSON content of the message.
pub struct MessageAuthView<'a> {
    pub sender: &'a str,
    pub chain: String,
    pub channel: Option<String>,
    pub message_type: aleph_types::message::MessageType,
    pub content_address: String,
    pub content_type: Option<String>,
    pub content_key: Option<String>,
    pub content_ref: Option<String>,
}

impl<'a> MessageAuthView<'a> {
    /// Build a view from a `MessageDb`. The `content_address` defaults to
    /// `message.sender` if the JSON content is missing the `address` field
    /// (mirrors `MessageDb._coerce_content`).
    pub fn from_message(message: &'a MessageDb) -> Self {
        let chain = serde_json::to_value(&message.chain)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default();
        let channel = message
            .channel
            .as_ref()
            .and_then(|c| serde_json::to_value(c).ok())
            .and_then(|v| v.as_str().map(|s| s.to_string()));
        let content_address = message
            .content
            .get("address")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| message.sender.clone());
        let content_type = message
            .content
            .get("type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let content_key = message
            .content
            .get("key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        // `content.ref` in Python accepts both a bare string hash and a
        // `ChainRef` object `{chain, type, address, item_hash}`. The auth
        // layer (and the rest of the code) only cares about the item hash,
        // so we extract it from either shape.
        let content_ref = match message.content.get("ref") {
            Some(serde_json::Value::String(s)) => Some(s.clone()),
            Some(serde_json::Value::Object(obj)) => obj
                .get("item_hash")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            _ => None,
        };
        Self {
            sender: &message.sender,
            chain,
            channel,
            message_type: message.r#type,
            content_address,
            content_type,
            content_key,
            content_ref,
        }
    }
}

impl<'a> MessageForAuth for MessageAuthView<'a> {
    fn sender(&self) -> &str {
        self.sender
    }
    fn chain(&self) -> &str {
        &self.chain
    }
    fn channel(&self) -> Option<&str> {
        self.channel.as_deref()
    }
    fn message_type(&self) -> aleph_types::message::MessageType {
        self.message_type
    }
    fn content_address(&self) -> &str {
        &self.content_address
    }
    fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }
    fn content_key(&self) -> Option<&str> {
        self.content_key.as_deref()
    }
    fn content_ref(&self) -> Option<&str> {
        self.content_ref.as_deref()
    }
}

/// Send-friendly re-implementation of
/// `crate::permissions::check_sender_authorization` that uses the concrete
/// [`MessageAuthView`] type rather than going through `&dyn MessageForAuth`.
///
/// The Python implementation lives in `aleph/permissions.py` and the Rust
/// port in `crate::permissions::check_sender_authorization`. The shared
/// implementation there takes a `&dyn MessageForAuth` argument which is
/// `!Send`, preventing it from being used across an `await` inside an
/// `async_trait` method that must be `Send`. This helper performs the
/// equivalent logic on a `MessageAuthView` value (which is `Send + Sync`)
/// and keeps full parity with `check_sender_authorization`.
pub async fn check_authorization_local(
    lookup: &dyn AuthorityLookup,
    view: &MessageAuthView<'_>,
) -> bool {
    let sender = view.sender;
    let address = view.content_address.as_str();
    if sender.eq_ignore_ascii_case(address) {
        return true;
    }

    // Amend path: defer to the original sender.
    if view.message_type == aleph_types::message::MessageType::Post
        && view.content_type.as_deref() == Some("amend")
    {
        if let Some(ref_hash) = view.content_ref.as_deref() {
            if let Some(original) = lookup.get_message_by_item_hash(ref_hash).await {
                // `original` is Box<dyn MessageForAuth + Send + Sync>.
                let original_addr = original.content_address().to_string();
                if !address.eq_ignore_ascii_case(&original_addr) {
                    return false;
                }
                return check_delegated_authorization_local(lookup, sender, &original_addr, view)
                    .await;
            }
        }
    }

    check_delegated_authorization_local(lookup, sender, address, view).await
}

/// Send-friendly local copy of `permissions::is_sender_authorized_for_owner`.
///
/// Mirrors the public `is_sender_authorized_for_owner` helper in
/// `aleph/permissions.py`: it answers whether `sender` is authorized to act
/// for `owner_address` per the owner's `security` aggregate, using `view`'s
/// attributes (type, channel, chain, post/aggregate scoping) as the filter
/// match. Callers pass either an inbound message (authorize a submission) or
/// an existing target message (authorize an action against it, e.g. FORGET).
pub(crate) async fn check_delegated_authorization_local(
    lookup: &dyn AuthorityLookup,
    sender: &str,
    owner_address: &str,
    view: &MessageAuthView<'_>,
) -> bool {
    use aleph_types::message::MessageType;
    if sender.eq_ignore_ascii_case(owner_address) {
        return true;
    }
    let Some(agg) = lookup.get_security_aggregate(owner_address).await else {
        return false;
    };
    let authorizations = match agg.get("authorizations").and_then(|v| v.as_array()) {
        Some(a) => a.clone(),
        None => return false,
    };
    for auth in &authorizations {
        let auth_addr = auth.get("address").and_then(|v| v.as_str()).unwrap_or("");
        if !auth_addr.eq_ignore_ascii_case(sender) {
            continue;
        }
        if let Some(c) = auth.get("chain").and_then(|v| v.as_str()) {
            if c != view.chain {
                continue;
            }
        }
        let channels = string_array(auth.get("channels"));
        let mtypes = string_array(auth.get("types"));
        let ptypes = string_array(auth.get("post_types"));
        let akeys = string_array(auth.get("aggregate_keys"));

        if !channels.is_empty() {
            match view.channel.as_deref() {
                Some(ch) if channels.iter().any(|c| *c == ch) => {}
                _ => continue,
            }
        }
        if !mtypes.is_empty()
            && !mtypes
                .iter()
                .any(|t| t.eq_ignore_ascii_case(&view.message_type.to_string()))
        {
            continue;
        }
        if view.message_type == MessageType::Post && !ptypes.is_empty() {
            match view.content_type.as_deref() {
                Some(t) if ptypes.iter().any(|s| *s == t) => {}
                _ => continue,
            }
        }
        if view.message_type == MessageType::Aggregate && !akeys.is_empty() {
            match view.content_key.as_deref() {
                Some(k) if akeys.iter().any(|s| *s == k) => {}
                _ => continue,
            }
        }
        return true;
    }
    false
}

fn string_array(v: Option<&serde_json::Value>) -> Vec<String> {
    v.and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Per–content-type handler. Mirrors Python `ContentHandler`.
///
/// All `client` parameters take an in-flight `tokio_postgres::Transaction`,
/// mirroring pyaleph's `with session.begin(): ...` boundary. The processor
/// commits or rolls back the transaction depending on the handler's
/// outcome.
///
/// The returned futures are required to be `Send` so the trait can be driven
/// from a multi-threaded axum runtime; trait objects therefore implement
/// `Send + Sync`.
#[async_trait]
pub trait ContentHandler: Send + Sync {
    /// Fetch additional content from the network. Default: no-op.
    async fn fetch_related_content(
        &self,
        _client: &tokio_postgres::Transaction<'_>,
        _message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        Ok(())
    }

    /// True when the network content referenced by `message` is already on
    /// the node.
    async fn is_related_content_fetched(
        &self,
        _client: &tokio_postgres::Transaction<'_>,
        _message: &MessageDb,
    ) -> AlephResult<bool> {
        Ok(true)
    }

    /// Apply the changes carried by a batch of messages.
    async fn process(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        messages: &[MessageDb],
    ) -> Result<(), MessageProcessingException>;

    /// Pre-flight balance check (cheap, run before fetching content).
    async fn pre_check_balance(
        &self,
        _client: &tokio_postgres::Transaction<'_>,
        _message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        Ok(())
    }

    /// Full balance check, returning the cost rows to be persisted.
    async fn check_balance(
        &self,
        _client: &tokio_postgres::Transaction<'_>,
        _message: &MessageDb,
    ) -> Result<Option<Vec<AccountCostsDb>>, MessageProcessingException> {
        Ok(None)
    }

    /// Check message-level dependencies (target messages exist, etc.).
    async fn check_dependencies(
        &self,
        _client: &tokio_postgres::Transaction<'_>,
        _message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        Ok(())
    }

    /// Check user permissions via the injected [`AuthorityLookup`].
    async fn check_permissions(
        &self,
        _client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
        lookup: &dyn AuthorityLookup,
    ) -> Result<(), MessageProcessingException> {
        let view = MessageAuthView::from_message(message);
        let authorized = check_authorization_local(lookup, &view).await;
        if authorized {
            Ok(())
        } else {
            Err(MessageProcessingException::PermissionDenied {
                errors: vec![format!(
                    "Sender {} is not authorized to post on behalf of address {}",
                    message.sender, view.content_address
                )],
            })
        }
    }

    /// Clean up state related to a forgotten message. Returns the additional
    /// item hashes that must also be marked forgotten.
    async fn forget_message(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<HashSet<String>, MessageProcessingException>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::message_status::MessageStatus;
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use chrono::Utc;
    use serde_json::json;

    fn make_message(sender: &str, address: Option<&str>) -> MessageDb {
        MessageDb {
            item_hash: "h".into(),
            r#type: MessageType::Post,
            chain: Chain::Ethereum,
            sender: sender.into(),
            signature: None,
            item_type: ItemType::Inline,
            item_content: None,
            content: match address {
                Some(a) => json!({"address": a, "type": "amend", "ref": "r"}),
                None => json!({"type": "store"}),
            },
            time: Utc::now(),
            channel: None,
            size: 0,
            status_value: MessageStatus::Processed,
            reception_time: Utc::now(),
            owner: address.map(|s| s.to_string()),
            content_type: Some("amend".into()),
            content_ref: Some("r".into()),
            content_key: None,
            first_confirmed_at: None,
            first_confirmed_height: None,
            payment_type: None,
            content_item_hash: None,
            tags: None,
        }
    }

    #[test]
    fn auth_view_falls_back_to_sender() {
        let msg = make_message("0xabc", None);
        let view = MessageAuthView::from_message(&msg);
        assert_eq!(view.content_address, "0xabc");
        assert_eq!(view.content_type, Some("store".to_string()));
    }

    /// Compile-time check: a `ContentHandler` trait object can be driven
    /// from a `Send` context. axum 0.8 requires this for the orchestrator's
    /// `process_pending_message` future to be `Send`. The check below is
    /// purely a type-system assertion — it never executes.
    #[test]
    fn content_handler_trait_object_is_send_sync() {
        use std::sync::Arc;
        fn assert_send_sync<T: Send + Sync + ?Sized>() {}
        assert_send_sync::<dyn ContentHandler>();
        // `Arc<dyn ContentHandler>` must also be Send so it can be moved
        // into a tokio task driven by the multi-threaded scheduler.
        fn assert_send<T: Send>() {}
        assert_send::<Arc<dyn ContentHandler>>();
    }

    #[test]
    fn auth_view_reads_content_address() {
        let msg = make_message("0xabc", Some("0xowner"));
        let view = MessageAuthView::from_message(&msg);
        assert_eq!(view.content_address, "0xowner");
        assert_eq!(view.content_type, Some("amend".to_string()));
        assert_eq!(view.content_ref, Some("r".to_string()));
    }
}
