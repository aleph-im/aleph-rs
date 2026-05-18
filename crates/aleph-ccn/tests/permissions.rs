//! Ports `tests/permissions/test_check_sender_authorization.py`.
//!
//! Mirrors each scenario from the Python test using the `StubAuthorityLookup`
//! defined in `tests/common/mod.rs`. The actual permission logic lives in
//! `aleph_ccn::permissions::check_sender_authorization`, so these tests
//! exercise real behaviour — only the *aggregate lookup* is stubbed (the same
//! way the Python tests mock `get_aggregate_by_key`).

mod common;

use aleph_types::message::MessageType;
use serde_json::json;

use aleph_ccn::permissions::check_sender_authorization;

use common::{FakeAuthMessage, StubAuthorityLookup};

#[tokio::test]
async fn test_owner_is_sender() {
    // Mirrors `test_owner_is_sender`: when content.address == sender, the
    // message is authorized without touching the aggregate store.
    let m = FakeAuthMessage {
        sender: "0xdeF61fAadE93a8aaE303D083Ead5BF7a25E55a23".into(),
        chain: "ETH".into(),
        channel: Some("TEST".into()),
        mtype: MessageType::Store,
        content_address: "0xdeF61fAadE93a8aaE303D083Ead5BF7a25E55a23".into(),
        content_type: None,
        content_key: None,
        content_ref: None,
    };
    let lookup = StubAuthorityLookup::new();
    assert!(check_sender_authorization(&lookup, &m).await);
}

#[tokio::test]
async fn test_owner_is_sender_case_insensitive() {
    let checksummed = "0xDeF61fAadE93a8aaE303D083Ead5BF7a25E55a23";
    let lowercase = checksummed.to_lowercase();
    let m = FakeAuthMessage {
        sender: lowercase,
        chain: "ETH".into(),
        channel: Some("TEST".into()),
        mtype: MessageType::Store,
        content_address: checksummed.into(),
        content_type: None,
        content_key: None,
        content_ref: None,
    };
    let lookup = StubAuthorityLookup::new();
    assert!(check_sender_authorization(&lookup, &m).await);
}

#[tokio::test]
async fn test_store_unauthorized() {
    // Mirrors `test_store_unauthorized`: sender != address, no aggregate
    // available => denied.
    let m = FakeAuthMessage {
        sender: "0x8b5C865d6ff6Dd5C5c402C8D918F7edd189C74D4".into(),
        chain: "ETH".into(),
        channel: Some("TEST".into()),
        mtype: MessageType::Post,
        content_address: "VM on executor".into(),
        content_type: Some("test".into()),
        content_key: None,
        content_ref: None,
    };
    let lookup = StubAuthorityLookup::new();
    assert!(!check_sender_authorization(&lookup, &m).await);
}

#[tokio::test]
async fn test_authorized_via_aggregate() {
    // Mirrors `test_authorized`: delegation entry in the security aggregate.
    let owner = "0xA3c613b12e862EB6e0C9897E03F1deEb207b5B58";
    let sender = "0x86F39e17910E3E6d9F38412EB7F24Bf0Ba31eb2E";
    let m = FakeAuthMessage {
        sender: sender.into(),
        chain: "ETH".into(),
        channel: Some("TEST".into()),
        mtype: MessageType::Post,
        content_address: owner.into(),
        content_type: Some("test".into()),
        content_key: None,
        content_ref: None,
    };
    let lookup = StubAuthorityLookup::new().with_security_aggregate(
        owner,
        json!({
            "authorizations": [{"address": sender}],
        }),
    );
    assert!(check_sender_authorization(&lookup, &m).await);
}

#[tokio::test]
async fn test_delegated_account_amend_permission() {
    // Mirrors `test_delegated_account_amend_permission`. The amend message
    // looks up its `ref`'d original; if the original's owner matches and the
    // delegated address has POST/post permission, the amend is allowed.
    let original_owner = "0xContentOwner12345678901234567890123456789012";
    let original_sender = "0xOriginalSender12345678901234567890123456789012";
    let delegate = "0xDelegatedAccount12345678901234567890123456789012";
    let original_hash = "1".repeat(64);
    let amend_hash = "2".repeat(64);

    let original = FakeAuthMessage {
        sender: original_sender.into(),
        chain: "ETH".into(),
        channel: Some("TEST".into()),
        mtype: MessageType::Post,
        content_address: original_owner.into(),
        content_type: Some("post".into()),
        content_key: None,
        content_ref: None,
    };
    let amend = FakeAuthMessage {
        sender: delegate.into(),
        chain: "ETH".into(),
        channel: Some("TEST".into()),
        mtype: MessageType::Post,
        content_address: original_owner.into(),
        content_type: Some("amend".into()),
        content_key: None,
        content_ref: Some(original_hash.clone()),
    };
    let _ = amend_hash;

    let lookup = StubAuthorityLookup::new()
        .with_original_message(&original_hash, original)
        .with_security_aggregate(
            original_owner,
            json!({
                "authorizations": [
                    {"address": delegate, "types": ["POST"], "post_types": ["post"]}
                ]
            }),
        );
    assert!(check_sender_authorization(&lookup, &amend).await);
}

#[tokio::test]
async fn test_delegated_account_amend_permission_denied() {
    // Mirrors `test_delegated_account_amend_permission_denied`: the
    // amend-sender is *not* in the original's authorizations -> denied.
    let original_owner = "0xContentOwner12345678901234567890123456789012";
    let delegate = "0xDelegatedAccount12345678901234567890123456789012";
    let unauthorized = "0xUnauthorizedAccount1234567890123456789012345678";
    let original_hash = "1".repeat(64);

    let original = FakeAuthMessage {
        sender: "0xOriginalSender12345678901234567890123456789012".into(),
        chain: "ETH".into(),
        channel: Some("TEST".into()),
        mtype: MessageType::Post,
        content_address: original_owner.into(),
        content_type: Some("post".into()),
        content_key: None,
        content_ref: None,
    };
    let amend = FakeAuthMessage {
        sender: unauthorized.into(),
        chain: "ETH".into(),
        channel: Some("TEST".into()),
        mtype: MessageType::Post,
        content_address: original_owner.into(),
        content_type: Some("amend".into()),
        content_key: None,
        content_ref: Some(original_hash.clone()),
    };

    let lookup = StubAuthorityLookup::new()
        .with_original_message(&original_hash, original)
        .with_security_aggregate(
            original_owner,
            json!({
                "authorizations": [
                    {"address": delegate, "types": ["POST"], "post_types": ["post"]}
                ]
            }),
        );
    assert!(!check_sender_authorization(&lookup, &amend).await);
}

#[tokio::test]
async fn test_amend_with_missing_original_falls_back_to_authorisation_check() {
    // Mirrors `test_amend_with_missing_original_post`: when the original
    // doesn't exist, authorization falls back to a normal delegation check
    // against `content.address`.
    let owner = "0xContentOwner12345678901234567890123456789012";
    let delegate = "0xDelegatedAccount12345678901234567890123456789012";
    let amend = FakeAuthMessage {
        sender: delegate.into(),
        chain: "ETH".into(),
        channel: Some("TEST".into()),
        mtype: MessageType::Post,
        content_address: owner.into(),
        content_type: Some("amend".into()),
        content_key: None,
        content_ref: Some("0".repeat(64)),
    };
    let lookup = StubAuthorityLookup::new().with_security_aggregate(
        owner,
        json!({
            "authorizations": [
                {"address": delegate, "types": ["POST"], "post_types": ["amend"]}
            ]
        }),
    );
    assert!(check_sender_authorization(&lookup, &amend).await);
}

#[tokio::test]
async fn test_delegation_with_chain_filter() {
    // Aggregate restricts delegation to chain="SOL" so an ETH message is denied.
    let m = FakeAuthMessage {
        sender: "0xsender".into(),
        chain: "ETH".into(),
        channel: None,
        mtype: MessageType::Post,
        content_address: "0xowner".into(),
        content_type: Some("post".into()),
        content_key: None,
        content_ref: None,
    };
    let lookup = StubAuthorityLookup::new().with_security_aggregate(
        "0xowner",
        json!({
            "authorizations": [{"address": "0xsender", "chain": "SOL"}]
        }),
    );
    assert!(!check_sender_authorization(&lookup, &m).await);
}

#[tokio::test]
async fn test_delegation_with_aggregate_key_filter() {
    // Aggregate keys filter — delegate can only write to a specific key.
    let m = FakeAuthMessage {
        sender: "0xsender".into(),
        chain: "ETH".into(),
        channel: None,
        mtype: MessageType::Aggregate,
        content_address: "0xowner".into(),
        content_type: None,
        content_key: Some("prefs".into()),
        content_ref: None,
    };
    let lookup = StubAuthorityLookup::new().with_security_aggregate(
        "0xowner",
        json!({
            "authorizations": [
                {"address": "0xsender", "aggregate_keys": ["acl"]}
            ]
        }),
    );
    assert!(!check_sender_authorization(&lookup, &m).await);

    let mut allowed = m.clone();
    allowed.content_key = Some("acl".into());
    assert!(check_sender_authorization(&lookup, &allowed).await);
}

#[tokio::test]
async fn test_delegation_with_post_types_filter() {
    let mut m = FakeAuthMessage {
        sender: "0xsender".into(),
        chain: "ETH".into(),
        channel: None,
        mtype: MessageType::Post,
        content_address: "0xowner".into(),
        content_type: Some("note".into()),
        content_key: None,
        content_ref: None,
    };
    let lookup = StubAuthorityLookup::new().with_security_aggregate(
        "0xowner",
        json!({
            "authorizations": [
                {"address": "0xsender", "post_types": ["chart"]}
            ]
        }),
    );
    assert!(!check_sender_authorization(&lookup, &m).await);
    m.content_type = Some("chart".into());
    assert!(check_sender_authorization(&lookup, &m).await);
}

#[tokio::test]
async fn test_delegation_with_channel_filter_mismatch() {
    let m = FakeAuthMessage {
        sender: "0xsender".into(),
        chain: "ETH".into(),
        channel: Some("MYCHAN".into()),
        mtype: MessageType::Post,
        content_address: "0xowner".into(),
        content_type: Some("post".into()),
        content_key: None,
        content_ref: None,
    };
    let lookup = StubAuthorityLookup::new().with_security_aggregate(
        "0xowner",
        json!({
            "authorizations": [{"address": "0xsender", "channels": ["OTHER"]}]
        }),
    );
    assert!(!check_sender_authorization(&lookup, &m).await);
}
