//! Access control. Mirrors `aleph/permissions.py`.
//!
//! Pure logic — every DB lookup is injected via the [`AuthorityLookup`] trait
//! so this module can be exercised without standing up Postgres and so it
//! survives accessor refactors.

use async_trait::async_trait;

use aleph_types::message::MessageType;
use serde_json::Value;

use crate::db::DbPool;

const SECURITY_AGGREGATE_KEY: &str = "security";

/// Minimal view over a message required for permission checks.
pub trait MessageForAuth {
    fn sender(&self) -> &str;
    fn chain(&self) -> &str;
    fn channel(&self) -> Option<&str>;
    fn message_type(&self) -> MessageType;
    /// `content.address` for the *parsed* content.
    fn content_address(&self) -> &str;
    /// `content.type` for POST messages, otherwise `None`.
    fn content_type(&self) -> Option<&str>;
    /// `content.key` for AGGREGATE messages, otherwise `None`.
    fn content_key(&self) -> Option<&str>;
    /// `content.ref` for POST `amend` messages, otherwise `None`.
    fn content_ref(&self) -> Option<&str>;
}

/// Aggregate-lookup contract used by permission checks.
#[async_trait]
pub trait AuthorityLookup: Send + Sync {
    /// Returns the `security` aggregate for `owner_address`, if any.
    /// The shape is the raw JSONB blob (Pydantic-side `aggregate.content`).
    async fn get_security_aggregate(&self, owner_address: &str) -> Option<Value>;

    /// Returns the original message by `item_hash` so that amend permissions
    /// can defer to the original sender.
    async fn get_message_by_item_hash(
        &self,
        item_hash: &str,
    ) -> Option<Box<dyn MessageForAuth + Send + Sync>>;
}

/// Database-backed authority lookup used by the production message pipeline.
///
/// Pyaleph resolves delegated permissions from the owner's `security`
/// aggregate and loads referenced messages for POST `amend` authorization.
#[derive(Clone)]
pub struct DbAuthorityLookup {
    pool: DbPool,
}

impl DbAuthorityLookup {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

struct OwnedMessageForAuth {
    sender: String,
    chain: String,
    channel: Option<String>,
    message_type: MessageType,
    content_address: String,
    content_type: Option<String>,
    content_key: Option<String>,
    content_ref: Option<String>,
}

impl OwnedMessageForAuth {
    fn from_message(message: crate::db::models::messages::MessageDb) -> Self {
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
            .map(str::to_string)
            .unwrap_or_else(|| message.sender.clone());
        let content_type = message
            .content
            .get("type")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let content_key = message
            .content
            .get("key")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let content_ref = match message.content.get("ref") {
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Object(obj)) => obj
                .get("item_hash")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            _ => None,
        };
        Self {
            sender: message.sender,
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

impl MessageForAuth for OwnedMessageForAuth {
    fn sender(&self) -> &str {
        &self.sender
    }

    fn chain(&self) -> &str {
        &self.chain
    }

    fn channel(&self) -> Option<&str> {
        self.channel.as_deref()
    }

    fn message_type(&self) -> MessageType {
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

#[async_trait]
impl AuthorityLookup for DbAuthorityLookup {
    async fn get_security_aggregate(&self, owner_address: &str) -> Option<Value> {
        let client = match self.pool.get().await {
            Ok(client) => client,
            Err(e) => {
                tracing::warn!("authority lookup pool acquire failed: {e}");
                return None;
            }
        };
        match crate::db::accessors::aggregates::get_aggregate_by_key(
            &**client,
            owner_address,
            SECURITY_AGGREGATE_KEY,
            true,
        )
        .await
        {
            Ok(Some(aggregate)) => Some(aggregate.content),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("authority lookup security aggregate failed: {e}");
                None
            }
        }
    }

    async fn get_message_by_item_hash(
        &self,
        item_hash: &str,
    ) -> Option<Box<dyn MessageForAuth + Send + Sync>> {
        let client = match self.pool.get().await {
            Ok(client) => client,
            Err(e) => {
                tracing::warn!("authority lookup pool acquire failed: {e}");
                return None;
            }
        };
        match crate::db::accessors::messages::get_message_by_item_hash(&**client, item_hash).await {
            Ok(Some(message)) => Some(Box::new(OwnedMessageForAuth::from_message(message))),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("authority lookup message fetch failed: {e}");
                None
            }
        }
    }
}

/// Check whether `sender` is authorized to act for `owner_address` per the
/// `security` aggregate (Python `is_sender_authorized_for_owner`).
///
/// `message`'s attributes scope the authorization filters (`types`,
/// `channels`, `chain`, `post_types`, `aggregate_keys`). Callers pass either
/// an inbound message (authorize a submission on behalf of `owner_address`)
/// or an existing target message (authorize an action against it, e.g. a
/// FORGET, expressing that the right to forget content follows the right to
/// create it). If `sender == owner_address` the sender acts for themselves
/// and no aggregate lookup is needed.
pub async fn is_sender_authorized_for_owner<L: AuthorityLookup + ?Sized>(
    lookup: &L,
    sender: &str,
    owner_address: &str,
    message: &dyn MessageForAuth,
) -> bool {
    if sender.eq_ignore_ascii_case(owner_address) {
        return true;
    }

    let Some(agg) = lookup.get_security_aggregate(owner_address).await else {
        return false;
    };

    let authorizations = match agg.get("authorizations").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return false,
    };

    for auth in authorizations {
        let auth_addr = auth.get("address").and_then(|v| v.as_str()).unwrap_or("");
        if !auth_addr.eq_ignore_ascii_case(sender) {
            continue;
        }

        if let Some(c) = auth.get("chain").and_then(|v| v.as_str()) {
            if c != message.chain() {
                continue;
            }
        }

        let channels = string_array(auth.get("channels"));
        let mtypes = string_array(auth.get("types"));
        let ptypes = string_array(auth.get("post_types"));
        let akeys = string_array(auth.get("aggregate_keys"));

        if !channels.is_empty() {
            match message.channel() {
                Some(ch) if channels.iter().any(|c| *c == ch) => {}
                _ => continue,
            }
        }

        if !mtypes.is_empty()
            && !mtypes
                .iter()
                .any(|t| message_type_matches(t, message.message_type()))
        {
            continue;
        }

        if message.message_type() == MessageType::Post {
            if !ptypes.is_empty() {
                match message.content_type() {
                    Some(t) if ptypes.iter().any(|s| *s == t) => {}
                    _ => continue,
                }
            }
        }

        if message.message_type() == MessageType::Aggregate {
            if !akeys.is_empty() {
                match message.content_key() {
                    Some(k) if akeys.iter().any(|s| *s == k) => {}
                    _ => continue,
                }
            }
        }

        return true;
    }

    false
}

/// Top-level permission gate (Python `check_sender_authorization`).
pub async fn check_sender_authorization<L: AuthorityLookup + ?Sized>(
    lookup: &L,
    message: &dyn MessageForAuth,
) -> bool {
    let sender = message.sender();
    let address = message.content_address();
    if sender.eq_ignore_ascii_case(address) {
        return true;
    }

    if message.message_type() == MessageType::Post && message.content_type() == Some("amend") {
        if let Some(ref_hash) = message.content_ref() {
            if let Some(original) = lookup.get_message_by_item_hash(ref_hash).await {
                let original_addr = original.content_address();
                if !address.eq_ignore_ascii_case(original_addr) {
                    return false;
                }
                return is_sender_authorized_for_owner(
                    lookup,
                    sender,
                    original_addr,
                    original.as_ref(),
                )
                .await;
            }
        }
    }

    is_sender_authorized_for_owner(lookup, sender, address, message).await
}

fn string_array(v: Option<&Value>) -> Vec<&str> {
    v.and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|x| x.as_str()).collect())
        .unwrap_or_default()
}

fn message_type_matches(serialized: &str, mt: MessageType) -> bool {
    mt.to_string().eq_ignore_ascii_case(serialized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct FakeMessage {
        sender: String,
        chain: String,
        channel: Option<String>,
        mtype: MessageType,
        content_address: String,
        content_type: Option<String>,
        content_key: Option<String>,
        content_ref: Option<String>,
    }
    impl MessageForAuth for FakeMessage {
        fn sender(&self) -> &str {
            &self.sender
        }
        fn chain(&self) -> &str {
            &self.chain
        }
        fn channel(&self) -> Option<&str> {
            self.channel.as_deref()
        }
        fn message_type(&self) -> MessageType {
            self.mtype
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

    struct FakeLookup {
        agg: Option<Value>,
    }
    #[async_trait]
    impl AuthorityLookup for FakeLookup {
        async fn get_security_aggregate(&self, _owner: &str) -> Option<Value> {
            self.agg.clone()
        }
        async fn get_message_by_item_hash(
            &self,
            _item_hash: &str,
        ) -> Option<Box<dyn MessageForAuth + Send + Sync>> {
            None
        }
    }

    fn msg(mtype: MessageType, sender: &str, owner: &str) -> FakeMessage {
        FakeMessage {
            sender: sender.into(),
            chain: "ETH".into(),
            channel: None,
            mtype,
            content_address: owner.into(),
            content_type: None,
            content_key: None,
            content_ref: None,
        }
    }

    #[tokio::test]
    async fn sender_equals_owner_is_authorized() {
        let m = msg(MessageType::Post, "0xabc", "0xABC");
        let lookup = FakeLookup { agg: None };
        assert!(check_sender_authorization(&lookup, &m).await);
    }

    #[tokio::test]
    async fn unrelated_sender_without_aggregate_denied() {
        let m = msg(MessageType::Post, "0xsender", "0xowner");
        let lookup = FakeLookup { agg: None };
        assert!(!check_sender_authorization(&lookup, &m).await);
    }

    #[tokio::test]
    async fn delegated_sender_authorized() {
        let m = msg(MessageType::Post, "0xsender", "0xowner");
        let lookup = FakeLookup {
            agg: Some(json!({
                "authorizations": [{"address": "0xSENDER"}]
            })),
        };
        assert!(check_sender_authorization(&lookup, &m).await);
    }

    #[tokio::test]
    async fn delegation_with_chain_filter_mismatch() {
        let m = msg(MessageType::Post, "0xsender", "0xowner");
        let lookup = FakeLookup {
            agg: Some(json!({
                "authorizations": [{"address": "0xsender", "chain": "SOL"}]
            })),
        };
        assert!(!check_sender_authorization(&lookup, &m).await);
    }

    #[tokio::test]
    async fn delegation_with_channel_filter_match() {
        let mut m = msg(MessageType::Post, "0xsender", "0xowner");
        m.channel = Some("MYCHAN".into());
        let lookup = FakeLookup {
            agg: Some(json!({
                "authorizations": [{"address": "0xsender", "channels": ["MYCHAN"]}]
            })),
        };
        assert!(check_sender_authorization(&lookup, &m).await);
    }

    #[tokio::test]
    async fn delegation_with_post_type_filter() {
        let mut m = msg(MessageType::Post, "0xsender", "0xowner");
        m.content_type = Some("note".into());
        let lookup = FakeLookup {
            agg: Some(json!({
                "authorizations": [
                    {"address": "0xsender", "post_types": ["chart"]}
                ]
            })),
        };
        assert!(!check_sender_authorization(&lookup, &m).await);

        m.content_type = Some("chart".into());
        assert!(check_sender_authorization(&lookup, &m).await);
    }

    #[tokio::test]
    async fn delegation_with_aggregate_key_filter() {
        let mut m = msg(MessageType::Aggregate, "0xsender", "0xowner");
        m.content_key = Some("prefs".into());
        let lookup = FakeLookup {
            agg: Some(json!({
                "authorizations": [
                    {"address": "0xsender", "aggregate_keys": ["acl"]}
                ]
            })),
        };
        assert!(!check_sender_authorization(&lookup, &m).await);

        m.content_key = Some("acl".into());
        assert!(check_sender_authorization(&lookup, &m).await);
    }
}
