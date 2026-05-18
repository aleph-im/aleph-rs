//! Pre-built `MessageDb` factories. Ports the JSON fixtures from
//! `tests/api/fixtures/fixture_messages.json` and the inline
//! `MessageDb(...)` literals in `tests/api/test_get_message.py`.
//!
//! Two distinct shapes:
//!
//! * [`fixture_messages`] — a representative subset of the Python
//!   `fixture_messages.json` array; each entry is the raw JSON shape the API
//!   returns. Tests insert these via [`crate::common::insert_processed_message`].
//! * [`fixture_messages_with_status`] — the four canonical statuses used in
//!   `test_get_message.py`.

#![allow(dead_code)]

use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};

use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use aleph_ccn::AlephResult;
use aleph_ccn::db::DbPool;
use aleph_ccn::db::accessors::aggregates::{
    insert_aggregate, insert_aggregate_element, refresh_aggregate,
};
use aleph_ccn::db::accessors::messages::{upsert_message, upsert_message_status};
use aleph_ccn::db::models::messages::{ForgottenMessageDb, MessageDb, RejectedMessageDb};
use aleph_ccn::db::models::pending_messages::PendingMessageDb;
use aleph_ccn::db::models::posts::PostDb;
use aleph_ccn::toolkit::timestamp::utc_now;
use aleph_ccn::types::channel::Channel;
use aleph_ccn::types::message_status::{ErrorCode, MessageStatus};

use super::ts;

/// A canonical POST-with-content sample used by both processed-message and
/// list-messages tests.
pub fn processed_post(item_hash: &str, sender: &str, channel: &str, time_sec: i64) -> MessageDb {
    let content = json!({
        "address": sender,
        "time": time_sec as f64,
        "type": "test",
        "content": {"title": "My first blog post", "body": "Body"},
    });
    let item_content = serde_json::to_string(&content).unwrap();
    let payload = json!({
        "item_hash": item_hash,
        "type": "POST",
        "chain": "ETH",
        "sender": sender,
        "signature": "0xdeadbeef",
        "item_type": "inline",
        "item_content": item_content,
        "content": content,
        "time": time_sec as f64,
        "channel": channel,
        "size": 256,
    });
    let mut m = MessageDb::from_message_dict(&payload);
    m.status_value = MessageStatus::Processed;
    m.reception_time = ts(time_sec);
    m
}

/// A subset of `fixture_messages.json` covering the three most-used senders +
/// types. Returns `MessageDb` rows ready to insert.
pub fn fixture_messages() -> Vec<MessageDb> {
    vec![
        processed_post(
            "4c33dd1ebf61bbb4342d8258b591fcd52cca73fd7c425542f78311d8f45ba274",
            "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            "unit-tests",
            1_652_126_646,
        ),
        // STORE message
        {
            let item_hash = "2953f0b52beb79fc0ed1bc455346fdcb530611605e16c636778a0d673d7184af";
            let content_hash = "5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21";
            let content = json!({
                "address": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
                "time": 1_652_126_721.4974446_f64,
                "item_type": "storage",
                "item_hash": content_hash,
                "mime_type": "text/plain",
            });
            let payload = json!({
                "item_hash": item_hash,
                "type": "STORE",
                "chain": "ETH",
                "sender": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
                "signature": "0xa10129dd",
                "item_type": "inline",
                "item_content": serde_json::to_string(&content).unwrap(),
                "content": content,
                "time": 1_652_126_721_i64,
                "channel": "unit-tests",
                "size": 200,
            });
            let mut m = MessageDb::from_message_dict(&payload);
            m.reception_time = ts(1_652_126_721);
            m
        },
        // AGGREGATE-tests channel POST
        {
            let item_hash = "bc411ae2ba89289458d0168714457e7c9394a29ca83159240585591f4f46444a";
            let content = json!({
                "address": "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4",
                "time": 1_648_215_810.245091_f64,
                "type": "POST",
            });
            let payload = json!({
                "item_hash": item_hash,
                "type": "POST",
                "chain": "ETH",
                "sender": "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4",
                "signature": "0xfeedface",
                "item_type": "inline",
                "item_content": serde_json::to_string(&content).unwrap(),
                "content": content,
                "time": 1_648_215_810_i64,
                "channel": "aggregates-tests",
                "size": 120,
            });
            let mut m = MessageDb::from_message_dict(&payload);
            m.reception_time = ts(1_648_215_810);
            m
        },
        // FORGET / not-quite — really a POST that targets a hash
        {
            let item_hash = "f3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c6";
            let content = json!({
                "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
                "time": 1_645_794_065.439_f64,
                "type": "content-test",
                "content": {"test": "value"},
                "ref": "None",
            });
            let payload = json!({
                "item_hash": item_hash,
                "type": "POST",
                "chain": "ETH",
                "sender": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
                "signature": "0xabfa661a",
                "item_type": "storage",
                "item_content": Value::Null,
                "content": content,
                "time": 1_645_794_065_i64,
                "channel": "TEST",
                "size": 154,
            });
            let mut m = MessageDb::from_message_dict(&payload);
            m.reception_time = ts(1_645_794_065);
            m
        },
    ]
}

/// The four canonical statuses used by `test_get_message.py`.
pub struct StatusFixture {
    pub processed: Vec<MessageDb>,
    pub pending: Vec<PendingMessageDb>,
    pub forgotten: Vec<ForgottenMessageDb>,
    pub rejected: Vec<RejectedMessageDb>,
}

pub fn fixture_messages_with_status() -> StatusFixture {
    let reception = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

    // ---- processed ----
    let processed_hash = "e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5";
    let processed_content = json!({
        "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
        "time": 1_645_794_065.439_f64,
        "aggregates": [],
        "hashes": ["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],
        "reason": "None",
    });
    let processed = MessageDb {
        item_hash: processed_hash.into(),
        r#type: MessageType::Forget,
        chain: Chain::Ethereum,
        sender: "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".into(),
        signature: Some("0xabfa661a".into()),
        item_type: ItemType::Storage,
        item_content: None,
        content: processed_content,
        time: ts(1_645_794_065),
        channel: Some(Channel::from("TEST".to_string())),
        size: 154,
        status_value: MessageStatus::Processed,
        reception_time: reception,
        owner: Some("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".into()),
        content_type: None,
        content_ref: None,
        content_key: None,
        content_item_hash: None,
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: None,
        tags: None,
    };

    // ---- pending ----
    let pending_hash = "9ee49b5457baf686aa9b8d9941009b99c921b01873a611f3b972c2103bf4ef55";
    let pending_content = json!({
        "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
        "time": 1_645_794_065.439_f64,
        "aggregates": [],
        "hashes": ["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],
        "reason": "None",
    });
    let pending = PendingMessageDb {
        id: 0,
        item_hash: pending_hash.into(),
        r#type: MessageType::Aggregate,
        chain: Chain::Ethereum,
        sender: "0x59f1f0464540073Bc70edAab069496366c128115".into(),
        signature: Some("0x359de717".into()),
        item_type: ItemType::Storage,
        item_content: None,
        content: Some(pending_content),
        time: ts(1_645_794_080),
        channel: Some(Channel::from("TEST".to_string())),
        reception_time: reception,
        check_message: true,
        next_attempt: ts(1_672_531_200),
        retries: 1,
        tx_hash: None,
        fetched: false,
        origin: Some("p2p".into()),
    };

    // ---- forgotten ----
    let forgotten_hash = "QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF";
    let forgotten = ForgottenMessageDb {
        item_hash: forgotten_hash.into(),
        r#type: MessageType::Store,
        chain: Chain::Ethereum,
        sender: "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".into(),
        signature: Some("some-signature".into()),
        item_type: ItemType::Inline,
        time: ts(1_645_794_000),
        channel: Some(Channel::from("TEST".to_string())),
        forgotten_by: vec![processed_hash.into()],
    };

    // ---- rejected ----
    let rejected_hash = "3946eb27511391a04a599d56f0f44c0a0787797b8b2274be8b8cf2c38244a93a";
    let rejected_msg = json!({
        "time": 1_672_671_290.836_f64,
        "type": "FORGET",
        "chain": "ETH",
        "sender": "0xD498D9267b68Da05dd986B00f6fEF42f46e134Da",
        "channel": "TEST",
        "content": {
            "time": 1_672_671_290.836_f64,
            "hashes": [],
            "reason": "None",
            "address": "0xD498D9267b68Da05dd986B00f6fEF42f46e134Da",
        },
        "item_hash": rejected_hash,
        "item_type": "inline",
        "signature": "0xe36ff18a",
        "item_content": "{}",
    });
    let rejected = RejectedMessageDb {
        item_hash: rejected_hash.into(),
        message: rejected_msg,
        error_code: ErrorCode::ForgetNoTarget,
        details: None,
        traceback: None,
        tx_hash: None,
    };

    StatusFixture {
        processed: vec![processed],
        pending: vec![pending],
        forgotten: vec![forgotten],
        rejected: vec![rejected],
    }
}

// ---------------------------------------------------------------------------
// Builders for porting Python test fixtures verbatim.
// ---------------------------------------------------------------------------

/// Generic `MessageDb` builder with reasonable defaults. Mirrors the inline
/// `MessageDb(...)` literals in `tests/api/*.py`.
pub fn make_message(
    item_hash: &str,
    sender: &str,
    chain: Chain,
    mtype: MessageType,
    item_type: ItemType,
    content: Value,
    channel: Option<&str>,
    time_sec: f64,
) -> MessageDb {
    let item_content = match item_type {
        ItemType::Inline => Some(serde_json::to_string(&content).unwrap()),
        _ => None,
    };
    let secs = time_sec.trunc() as i64;
    MessageDb {
        item_hash: item_hash.into(),
        r#type: mtype,
        chain,
        sender: sender.into(),
        signature: Some("0xdeadbeef".into()),
        item_type,
        item_content,
        content: content.clone(),
        time: ts(secs),
        channel: channel.map(|c| Channel::from(c.to_string())),
        size: 100,
        status_value: MessageStatus::Processed,
        reception_time: ts(secs),
        owner: content
            .get("address")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| Some(sender.to_string())),
        content_type: content
            .get("type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        content_ref: content
            .get("ref")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        content_key: content
            .get("key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        content_item_hash: content
            .get("item_hash")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: None,
        tags: None,
    }
}

/// Convenience builder used by tests/db_address_stats and friends.
pub fn build_message(
    item_hash: &str,
    sender: &str,
    mtype: MessageType,
    channel: Option<&str>,
    content: Value,
    item_content: Option<String>,
    time: DateTime<Utc>,
) -> MessageDb {
    MessageDb {
        item_hash: item_hash.into(),
        r#type: mtype,
        chain: Chain::Ethereum,
        sender: sender.into(),
        signature: Some("0xdeadbeef".into()),
        item_type: ItemType::Inline,
        item_content,
        content: content.clone(),
        time,
        channel: channel.map(|c| Channel::from(c.to_string())),
        size: 100,
        status_value: MessageStatus::Processed,
        reception_time: time,
        owner: content
            .get("address")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| Some(sender.to_string())),
        content_type: content
            .get("type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        content_ref: content
            .get("ref")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        content_key: content
            .get("key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        content_item_hash: content
            .get("item_hash")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: None,
        tags: None,
    }
}

/// Insert one processed message (status row included).
pub async fn insert_processed(pool: &DbPool, message: &MessageDb) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    upsert_message(&**client, message).await?;
    upsert_message_status(
        &**client,
        &message.item_hash,
        MessageStatus::Processed,
        message.reception_time,
        None,
    )
    .await?;
    Ok(())
}

/// Owner/key/content + creation_datetime, used to build an aggregate element.
pub struct AggSeed {
    pub item_hash: String,
    pub key: String,
    pub owner: String,
    pub content: Value,
    pub creation: DateTime<Utc>,
}

/// Insert one aggregate element + refresh the merged `aggregates` row.
pub async fn insert_aggregate_seed(pool: &DbPool, seed: &AggSeed) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    insert_aggregate_element(
        &**client,
        &seed.item_hash,
        &seed.key,
        &seed.owner,
        &seed.content,
        seed.creation,
    )
    .await?;
    refresh_aggregate(&**client, &seed.owner, &seed.key).await?;
    Ok(())
}

/// Insert an aggregate revision directly with no element. Mirrors the
/// `AggregateDb(...)` literal in some Python tests that bypass refresh.
pub async fn insert_aggregate_row(
    pool: &DbPool,
    key: &str,
    owner: &str,
    content: &Value,
    creation: DateTime<Utc>,
    last_revision_hash: &str,
    dirty: bool,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    if dirty {
        client
            .execute(
                "INSERT INTO aggregates(key, owner, content, creation_datetime, \
                                         last_revision_hash, dirty) \
                 VALUES ($1, $2, $3, $4, $5, TRUE)",
                &[&key, &owner, content, &creation, &last_revision_hash],
            )
            .await
            .map_err(aleph_ccn::AlephError::Db)?;
        return Ok(());
    }
    insert_aggregate(
        &**client,
        key,
        owner,
        content,
        creation,
        last_revision_hash,
    )
    .await?;
    Ok(())
}

/// Insert one aggregate_element row directly.
pub async fn insert_aggregate_element_row(
    pool: &DbPool,
    item_hash: &str,
    key: &str,
    owner: &str,
    content: &Value,
    creation: DateTime<Utc>,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    insert_aggregate_element(&**client, item_hash, key, owner, content, creation).await
}

/// Insert one post row.
pub async fn insert_post(pool: &DbPool, post: &PostDb) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    let channel: Option<String> = post.channel.as_ref().and_then(|c| {
        serde_json::to_value(c)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
    });
    client
        .execute(
            "INSERT INTO posts(item_hash, owner, type, ref, amends, channel, content, \
                                creation_datetime, latest_amend, tags) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            &[
                &post.item_hash,
                &post.owner,
                &post.r#type,
                &post.r#ref,
                &post.amends,
                &channel,
                &post.content,
                &post.creation_datetime,
                &post.latest_amend,
                &post.tags,
            ],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    Ok(())
}

/// Build a `PostDb` from a `MessageDb` (mirrors Python `make_post_db`).
pub fn make_post_db(message: &MessageDb) -> PostDb {
    let content = &message.content;
    let inner = content
        .get("content")
        .cloned()
        .unwrap_or_else(|| content.clone());
    let ctype = content
        .get("type")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let cref = content
        .get("ref")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let amends = if ctype.as_deref() == Some("amend") {
        cref.clone()
    } else {
        None
    };
    let tags = PostDb::extract_post_tags(&inner);
    PostDb {
        item_hash: message.item_hash.clone(),
        owner: message.sender.clone(),
        r#type: ctype,
        r#ref: cref,
        amends,
        channel: message.channel.clone(),
        content: inner,
        creation_datetime: message.time,
        latest_amend: None,
        tags,
    }
}

/// Quick AppState updater that adds an IPFS service — currently no production
/// constructor is publicly exposed, so tests fall back to leaving it unset.
pub fn _ignore_unused_utc_now() -> DateTime<Utc> {
    utc_now()
}

// ---------------------------------------------------------------------------
// Extra helpers used by the per-table DB tests.
// ---------------------------------------------------------------------------

/// Row builder for `credit_history`.
#[derive(Debug, Clone)]
pub struct CreditHistoryRow {
    pub address: String,
    pub amount: i64,
    pub credit_ref: String,
    pub credit_index: i32,
    pub message_timestamp: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
    pub price: Option<rust_decimal::Decimal>,
    pub bonus_amount: Option<i64>,
    pub tx_hash: Option<String>,
    pub expiration_date: Option<DateTime<Utc>>,
    pub token: Option<String>,
    pub chain: Option<String>,
    pub origin: Option<String>,
    pub provider: Option<String>,
    pub origin_ref: Option<String>,
    pub payment_method: Option<String>,
}

impl CreditHistoryRow {
    pub fn new(address: &str, amount: i64, credit_ref: &str, message_ts: DateTime<Utc>) -> Self {
        Self {
            address: address.into(),
            amount,
            credit_ref: credit_ref.into(),
            credit_index: 0,
            message_timestamp: message_ts,
            last_update: message_ts,
            price: None,
            bonus_amount: None,
            tx_hash: None,
            expiration_date: None,
            token: None,
            chain: None,
            origin: None,
            provider: None,
            origin_ref: None,
            payment_method: None,
        }
    }

    pub fn with_expiration(mut self, exp: Option<DateTime<Utc>>) -> Self {
        self.expiration_date = exp;
        self
    }

    pub fn with_payment_method(mut self, pm: &str) -> Self {
        self.payment_method = Some(pm.into());
        self
    }

    pub fn with_origin(mut self, origin: &str) -> Self {
        self.origin = Some(origin.into());
        self
    }
}

/// Insert a credit_history row plus the matching credit_balances lot row for
/// positive non-expense entries. Mirrors `_insert_credit_history_entries` +
/// `_rebuild_credit_lots_for_address` in the Python tests.
pub async fn insert_credit_history_with_lot(
    pool: &DbPool,
    row: &CreditHistoryRow,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    client
        .execute(
            "INSERT INTO credit_history(address, amount, credit_ref, credit_index, \
                                         message_timestamp, last_update, price, bonus_amount, \
                                         tx_hash, expiration_date, token, chain, origin, \
                                         provider, origin_ref, payment_method) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
            &[
                &row.address,
                &row.amount,
                &row.credit_ref,
                &row.credit_index,
                &row.message_timestamp,
                &row.last_update,
                &row.price,
                &row.bonus_amount,
                &row.tx_hash,
                &row.expiration_date,
                &row.token,
                &row.chain,
                &row.origin,
                &row.provider,
                &row.origin_ref,
                &row.payment_method,
            ],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    if row.amount > 0 && row.payment_method.as_deref() != Some("credit_expense") {
        client
            .execute(
                "INSERT INTO credit_balances(address, credit_ref, credit_index, \
                                              amount_remaining, expiration_date, \
                                              message_timestamp) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT(address, credit_ref, credit_index) DO NOTHING",
                &[
                    &row.address,
                    &row.credit_ref,
                    &row.credit_index,
                    &row.amount,
                    &row.expiration_date,
                    &row.message_timestamp,
                ],
            )
            .await
            .map_err(aleph_ccn::AlephError::Db)?;
    }
    Ok(())
}

/// Apply an expense by decrementing the oldest matching lots (FIFO).
pub async fn apply_expense_to_lots(
    pool: &DbPool,
    address: &str,
    expense_amount: i64,
    now: DateTime<Utc>,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    let rows = client
        .query(
            "SELECT credit_ref, credit_index, amount_remaining FROM credit_balances \
             WHERE address = $1 AND amount_remaining > 0 \
               AND (expiration_date IS NULL OR expiration_date > $2) \
             ORDER BY message_timestamp ASC, credit_ref ASC, credit_index ASC",
            &[&address, &now],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    let mut remaining = expense_amount;
    for row in rows {
        if remaining <= 0 {
            break;
        }
        let credit_ref: String = row.get("credit_ref");
        let credit_index: i32 = row.get("credit_index");
        let amount_remaining: i64 = row.get("amount_remaining");
        let take = amount_remaining.min(remaining);
        client
            .execute(
                "UPDATE credit_balances SET amount_remaining = amount_remaining - $1 \
                 WHERE address = $2 AND credit_ref = $3 AND credit_index = $4",
                &[&take, &address, &credit_ref, &credit_index],
            )
            .await
            .map_err(aleph_ccn::AlephError::Db)?;
        remaining -= take;
    }
    Ok(())
}

/// Insert a chain_txs row directly. Used by tests that need to seed a
/// confirmation chain.
#[allow(clippy::too_many_arguments)]
pub async fn insert_chain_tx_row(
    pool: &DbPool,
    hash: &str,
    chain: &str,
    height: i32,
    datetime: DateTime<Utc>,
    publisher: &str,
    protocol: &str,
    protocol_version: i32,
    content: &Value,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    client
        .execute(
            "INSERT INTO chain_txs(hash, chain, height, datetime, publisher, protocol, \
                                    protocol_version, content) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT DO NOTHING",
            &[
                &hash,
                &chain,
                &height,
                &datetime,
                &publisher,
                &protocol,
                &protocol_version,
                content,
            ],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    Ok(())
}

/// Insert a `pending_txs` row.
pub async fn insert_pending_tx_row(pool: &DbPool, tx_hash: &str) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    client
        .execute(
            "INSERT INTO pending_txs(tx_hash) VALUES ($1) ON CONFLICT DO NOTHING",
            &[&tx_hash],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    Ok(())
}

/// Insert a `message_confirmations` row.
pub async fn insert_confirmation_row(
    pool: &DbPool,
    item_hash: &str,
    tx_hash: &str,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    client
        .execute(
            "INSERT INTO message_confirmations(item_hash, tx_hash) VALUES ($1, $2) \
             ON CONFLICT DO NOTHING",
            &[&item_hash, &tx_hash],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    Ok(())
}

/// Insert a `files` row.
pub async fn insert_file_row(
    pool: &DbPool,
    hash: &str,
    size: i64,
    file_type: &str,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    client
        .execute(
            "INSERT INTO files(hash, size, type) VALUES ($1, $2, $3) \
             ON CONFLICT DO NOTHING",
            &[&hash, &size, &file_type],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    Ok(())
}

/// Insert a `file_pins` row of any kind.
#[allow(clippy::too_many_arguments)]
pub async fn insert_file_pin_row(
    pool: &DbPool,
    file_hash: &str,
    pin_type: &str,
    owner: Option<&str>,
    item_hash: Option<&str>,
    tx_hash: Option<&str>,
    r#ref: Option<&str>,
    created: DateTime<Utc>,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    client
        .execute(
            "INSERT INTO file_pins(file_hash, type, owner, item_hash, tx_hash, ref, created) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &file_hash,
                &pin_type,
                &owner,
                &item_hash,
                &tx_hash,
                &r#ref,
                &created,
            ],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    Ok(())
}

/// Insert a `peers` row directly with arbitrary type+source as strings. The
/// `peers` table allows multiple rows per `peer_id` (different `peer_type`s).
pub async fn insert_peer_row(
    pool: &DbPool,
    peer_id: &str,
    peer_type: &str,
    address: &str,
    source: &str,
    last_seen: DateTime<Utc>,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    client
        .execute(
            "INSERT INTO peers(peer_id, peer_type, address, source, last_seen) \
             VALUES ($1, $2, $3, $4, $5)",
            &[&peer_id, &peer_type, &address, &source, &last_seen],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    Ok(())
}

/// Update `posts.latest_amend` for `item_hash`.
pub async fn set_post_latest_amend(
    pool: &DbPool,
    item_hash: &str,
    latest_amend: Option<&str>,
) -> AlephResult<()> {
    let client = pool.get().await.unwrap();
    client
        .execute(
            "UPDATE posts SET latest_amend = $2 WHERE item_hash = $1",
            &[&item_hash, &latest_amend],
        )
        .await
        .map_err(aleph_ccn::AlephError::Db)?;
    Ok(())
}
