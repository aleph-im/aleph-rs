//! Access control. Mirrors `aleph/permissions.py`.
//!
//! Pure logic — every DB lookup is injected via the [`AuthorityLookup`] trait
//! so this module can be exercised without standing up Postgres and so it
//! survives accessor refactors.

use async_trait::async_trait;

use aleph_types::message::MessageType;
use serde_json::Value;

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

/// Direct delegation check (Python `_check_delegated_authorization`).
async fn check_delegated_authorization<L: AuthorityLookup + ?Sized>(
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
                return check_delegated_authorization(
                    lookup,
                    sender,
                    original_addr,
                    original.as_ref(),
                )
                .await;
            }
        }
    }

    check_delegated_authorization(lookup, sender, address, message).await
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
