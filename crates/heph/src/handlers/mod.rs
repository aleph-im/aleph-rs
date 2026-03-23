use crate::db::Db;
use crate::db::messages::{DenormalizedFields, InsertMessage, get_message_status, insert_message};
use aleph_types::chain::{Address, Chain, Signature};
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::MessageContent;
use aleph_types::message::MessageContentEnum;
use aleph_types::message::item_type::ItemType;
use aleph_types::message::{MessageStatus, MessageType};
use aleph_types::timestamp::Timestamp;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Error codes for message processing failures (spec section 5.5).
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessingError {
    InternalError(String),
    InvalidFormat(String),
    InvalidSignature(String),
    PermissionDenied(String),
    ContentUnavailable(String),
    FileUnavailable(String),
    BalanceInsufficient(String),
    CreditInsufficient(String),
    PostAmendNoTarget(String),
    PostAmendTargetNotFound(String),
    PostAmendAmend(String),
    StoreRefNotFound(String),
    StoreUpdateUpdate(String),
    InvalidPaymentMethod(String),
    VmRefNotFound(String),
    VmVolumeNotFound(String),
    VmAmendNotAllowed(String),
    VmUpdateUpdate(String),
    VmVolumeTooSmall(String),
    ForgetNoTarget(String),
    ForgetTargetNotFound(String),
    ForgetForget(String),
    ForgetNotAllowed(String),
    ForgottenDuplicate(String),
}

impl ProcessingError {
    pub fn error_code(&self) -> i32 {
        match self {
            ProcessingError::InternalError(_) => -1,
            ProcessingError::InvalidFormat(_) => 0,
            ProcessingError::InvalidSignature(_) => 1,
            ProcessingError::PermissionDenied(_) => 2,
            ProcessingError::ContentUnavailable(_) => 3,
            ProcessingError::FileUnavailable(_) => 4,
            ProcessingError::BalanceInsufficient(_) => 5,
            ProcessingError::CreditInsufficient(_) => 6,
            ProcessingError::PostAmendNoTarget(_) => 100,
            ProcessingError::PostAmendTargetNotFound(_) => 101,
            ProcessingError::PostAmendAmend(_) => 102,
            ProcessingError::StoreRefNotFound(_) => 200,
            ProcessingError::StoreUpdateUpdate(_) => 201,
            ProcessingError::InvalidPaymentMethod(_) => 202,
            ProcessingError::VmRefNotFound(_) => 300,
            ProcessingError::VmVolumeNotFound(_) => 301,
            ProcessingError::VmAmendNotAllowed(_) => 302,
            ProcessingError::VmUpdateUpdate(_) => 303,
            ProcessingError::VmVolumeTooSmall(_) => 304,
            ProcessingError::ForgetNoTarget(_) => 500,
            ProcessingError::ForgetTargetNotFound(_) => 501,
            ProcessingError::ForgetForget(_) => 502,
            ProcessingError::ForgetNotAllowed(_) => 503,
            ProcessingError::ForgottenDuplicate(_) => 504,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            ProcessingError::InternalError(m) => m,
            ProcessingError::InvalidFormat(m) => m,
            ProcessingError::InvalidSignature(m) => m,
            ProcessingError::PermissionDenied(m) => m,
            ProcessingError::ContentUnavailable(m) => m,
            ProcessingError::FileUnavailable(m) => m,
            ProcessingError::BalanceInsufficient(m) => m,
            ProcessingError::CreditInsufficient(m) => m,
            ProcessingError::PostAmendNoTarget(m) => m,
            ProcessingError::PostAmendTargetNotFound(m) => m,
            ProcessingError::PostAmendAmend(m) => m,
            ProcessingError::StoreRefNotFound(m) => m,
            ProcessingError::StoreUpdateUpdate(m) => m,
            ProcessingError::InvalidPaymentMethod(m) => m,
            ProcessingError::VmRefNotFound(m) => m,
            ProcessingError::VmVolumeNotFound(m) => m,
            ProcessingError::VmAmendNotAllowed(m) => m,
            ProcessingError::VmUpdateUpdate(m) => m,
            ProcessingError::VmVolumeTooSmall(m) => m,
            ProcessingError::ForgetNoTarget(m) => m,
            ProcessingError::ForgetTargetNotFound(m) => m,
            ProcessingError::ForgetForget(m) => m,
            ProcessingError::ForgetNotAllowed(m) => m,
            ProcessingError::ForgottenDuplicate(m) => m,
        }
    }
}

impl std::fmt::Display for ProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProcessingError(code={}): {}",
            self.error_code(),
            self.message()
        )
    }
}

impl std::error::Error for ProcessingError {}

/// Result type for message processing.
pub type ProcessingResult<T> = Result<T, ProcessingError>;

/// Incoming message struct for deserialization — mirrors PendingMessage fields but implements
/// Deserialize (PendingMessage only has Serialize).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IncomingMessage {
    pub chain: Chain,
    pub sender: Address,
    pub signature: Signature,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    pub item_type: ItemType,
    /// Present only for inline messages.
    pub item_content: Option<String>,
    pub item_hash: ItemHash,
    pub time: Timestamp,
    #[serde(default)]
    pub channel: Option<Channel>,
}

/// Verify the cryptographic signature of an incoming message.
///
/// Delegates to `aleph_types::verify_signature::verify()`.
pub fn verify_signature(msg: &IncomingMessage) -> ProcessingResult<()> {
    aleph_types::verify_signature::verify(
        &msg.chain,
        &msg.sender,
        &msg.signature,
        msg.message_type,
        &msg.item_hash,
    )
    .map_err(|e| ProcessingError::InvalidSignature(e.to_string()))
}

/// Permission check — delegates to the permissions module.
fn check_permissions(
    db: &Db,
    msg: &IncomingMessage,
    content: &MessageContent,
) -> ProcessingResult<()> {
    crate::permissions::check_sender_authorization(db, msg, content)
}

/// Credit balance check — dispatches to cost module for paid message types.
fn check_balance(db: &Db, msg: &IncomingMessage, content: &MessageContent) -> ProcessingResult<()> {
    match msg.message_type {
        MessageType::Post | MessageType::Aggregate | MessageType::Forget => Ok(()),
        MessageType::Store => {
            let store_content = match &content.content {
                MessageContentEnum::Store(s) => s,
                _ => {
                    return Err(ProcessingError::InternalError(
                        "check_balance: non-STORE content for STORE message".into(),
                    ));
                }
            };
            let size_bytes = store_content.size.map(|s| s.count()).unwrap_or(0);
            let per_second = crate::cost::calculate_store_cost(size_bytes);
            crate::cost::check_credit_balance(db, content.address.as_str(), per_second)
        }
        MessageType::Program => {
            let program = match &content.content {
                MessageContentEnum::Program(p) => p,
                _ => {
                    return Err(ProcessingError::InternalError(
                        "check_balance: non-PROGRAM content for PROGRAM message".into(),
                    ));
                }
            };
            let vcpus = program.base.resources.vcpus;
            let memory_mib: u64 = program.base.resources.memory.count();
            let memory_mib_u32 = memory_mib.min(u32::MAX as u64) as u32;
            let total_volume_mib = collect_vm_volume_mib_program(program);
            let per_second =
                crate::cost::calculate_vm_cost(vcpus, memory_mib_u32, total_volume_mib);
            crate::cost::check_credit_balance(db, content.address.as_str(), per_second)
        }
        MessageType::Instance => {
            let instance = match &content.content {
                MessageContentEnum::Instance(i) => i,
                _ => {
                    return Err(ProcessingError::InternalError(
                        "check_balance: non-INSTANCE content for INSTANCE message".into(),
                    ));
                }
            };
            let vcpus = instance.base.resources.vcpus;
            let memory_mib: u64 = instance.base.resources.memory.count();
            let memory_mib_u32 = memory_mib.min(u32::MAX as u64) as u32;
            let total_volume_mib = collect_vm_volume_mib_instance(instance);
            let per_second =
                crate::cost::calculate_vm_cost(vcpus, memory_mib_u32, total_volume_mib);
            crate::cost::check_credit_balance(db, content.address.as_str(), per_second)
        }
    }
}

/// Sum up all known-size volumes for a PROGRAM message (in MiB).
fn collect_vm_volume_mib_program(program: &aleph_types::message::ProgramContent) -> u64 {
    use aleph_types::message::execution::volume::MachineVolume;
    let mut total: u64 = 0;
    for vol in &program.base.volumes {
        match vol {
            MachineVolume::Ephemeral(e) => {
                total += u64::from(e.size_mib);
            }
            MachineVolume::Persistent(p) => {
                total += u64::from(p.size_mib);
            }
            MachineVolume::Immutable(_) => {}
        }
    }
    total
}

/// Sum up all known-size volumes for an INSTANCE message (in MiB).
fn collect_vm_volume_mib_instance(instance: &aleph_types::message::InstanceContent) -> u64 {
    use aleph_types::message::execution::volume::MachineVolume;
    // Rootfs size.
    let rootfs_mib: u64 = instance.rootfs.size_mib.into();
    let mut total: u64 = rootfs_mib;
    for vol in &instance.base.volumes {
        match vol {
            MachineVolume::Persistent(p) => {
                let size: u64 = p.size_mib.into();
                total += size;
            }
            MachineVolume::Ephemeral(_) | MachineVolume::Immutable(_) => {}
        }
    }
    total
}

/// Dispatch to the type-specific handler for this message.
fn dispatch_type_specific(
    db: &Db,
    msg: &IncomingMessage,
    content: &MessageContent,
) -> ProcessingResult<()> {
    match msg.message_type {
        MessageType::Post => post::process_post(db, msg, content),
        MessageType::Aggregate => aggregate::process_aggregate(db, msg, content),
        MessageType::Store => store::process_store(db, msg, content),
        MessageType::Forget => forget::process_forget(db, msg, content),
        MessageType::Program => program::process_program(db, msg, content),
        MessageType::Instance => instance::process_instance(db, msg, content),
    }
}

/// Current UNIX timestamp as f64 seconds.
fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Full message processing pipeline (spec section 5.3).
///
/// Steps:
/// 1. Duplicate check
/// 2. Format validation
/// 3. Signature verification
/// 4. Permission check (stub)
/// 5. Balance check (stub)
/// 6. Insert into DB as PROCESSED
/// 7. Type-specific processing (stub)
pub fn process_message(db: &Db, msg: &IncomingMessage) -> ProcessingResult<()> {
    process_message_with_store(db, msg, None)
}

/// Full message processing pipeline (spec section 5.3), with optional local
/// file store for resolving non-inline (storage/IPFS) message content.
pub fn process_message_with_store(
    db: &Db,
    msg: &IncomingMessage,
    file_store: Option<&crate::files::FileStore>,
) -> ProcessingResult<()> {
    let item_hash_str = msg.item_hash.to_string();

    // Step 1 — duplicate check.
    let existing_status: Option<String> = db
        .with_conn(|conn| get_message_status(conn, &item_hash_str))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    if let Some(ref status) = existing_status {
        match status.as_str() {
            "processed" => return Ok(()), // already processed — skip silently
            "forgotten" => {
                return Err(ProcessingError::ForgottenDuplicate(format!(
                    "message {item_hash_str} was previously forgotten"
                )));
            }
            "pending" => return Ok(()), // already queued — skip silently
            "rejected" => {}            // allow retry — fall through to reprocess
            _ => {}                     // any other status: continue
        }
    }

    // Step 2 — format validation.
    // For non-inline messages we also capture the raw content bytes so they
    // can be stored in item_content (non-inline messages arrive without it).
    let (content, item_content_resolved) = match validate::validate_format(msg) {
        Ok(c) => (c, msg.item_content.clone()),
        Err(ProcessingError::ContentUnavailable(_)) => {
            let store = file_store.ok_or_else(|| {
                ProcessingError::ContentUnavailable(
                    "non-inline message and no file store available".into(),
                )
            })?;
            let raw = store.read(&item_hash_str).map_err(|_| {
                ProcessingError::ContentUnavailable(format!(
                    "content for {item_hash_str} not found in local storage"
                ))
            })?;
            let c = validate::validate_fetched_content(msg, &raw)?;
            let raw_str = String::from_utf8(raw).map_err(|e| {
                ProcessingError::InternalError(format!("content is not valid UTF-8: {e}"))
            })?;
            (c, Some(raw_str))
        }
        Err(e) => return Err(e),
    };

    // Step 3 — signature verification.
    verify_signature(msg)?;

    // Step 4 — permission check.
    check_permissions(db, msg, &content)?;

    // Step 5 — balance check.
    check_balance(db, msg, &content)?;

    // Step 6 — insert into DB with status PROCESSED.
    let denorm = DenormalizedFields::from_content(&content.content, msg.sender.as_str());
    let item_hash_str2 = item_hash_str.clone();
    let chain_str = msg.chain.to_string();
    let sender_str = msg.sender.as_str().to_string();
    let sig_str = msg.signature.as_str().to_string();
    let item_type_str = match msg.item_type {
        ItemType::Inline => "inline",
        ItemType::Storage => "storage",
        ItemType::Ipfs => "ipfs",
    };
    // Channel serializes as a JSON string (newtype wrapper), so serialize+strip quotes.
    let channel_str: Option<String> = msg.channel.as_ref().and_then(|c| {
        serde_json::to_value(c)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
    });
    let time_val = msg.time.as_f64();

    let reception_time = now_secs();

    let insert = InsertMessage {
        item_hash: &item_hash_str2,
        message_type: msg.message_type,
        chain: &chain_str,
        sender: &sender_str,
        signature: &sig_str,
        item_type: item_type_str,
        item_content: item_content_resolved.as_deref(),
        channel: channel_str.as_deref(),
        time: time_val,
        size: item_content_resolved
            .as_deref()
            .map(|s| s.len() as i64)
            .unwrap_or(0),
        status: MessageStatus::Processed,
        reception_time,
        owner: denorm.owner.as_deref(),
        content_type: denorm.content_type.as_deref(),
        content_ref: denorm.content_ref.as_deref(),
        content_key: denorm.content_key.as_deref(),
        content_item_hash: denorm.content_item_hash.as_deref(),
        payment_type: denorm.payment_type.as_deref(),
    };

    // If a prior rejected record exists, we need to update it; otherwise insert.
    if existing_status.as_deref() == Some("rejected") {
        // For simplicity: delete the old record and re-insert.
        db.with_conn(|conn| {
            conn.execute(
                "DELETE FROM messages WHERE item_hash = ?1",
                [&item_hash_str],
            )
        })
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
    }

    db.with_conn(|conn| insert_message(conn, &insert))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // Step 7 — type-specific processing.
    dispatch_type_specific(db, msg, &content)?;

    // Step 8 — insert cost records for paid message types.
    insert_cost_records(db, msg, &content, &item_hash_str)?;

    Ok(())
}

/// Insert cost records into `account_costs` for paid message types (STORE/PROGRAM/INSTANCE).
fn insert_cost_records(
    db: &Db,
    msg: &IncomingMessage,
    content: &MessageContent,
    item_hash: &str,
) -> ProcessingResult<()> {
    use crate::db::costs::{AccountCostRecord, insert_account_costs};

    let owner = content.address.as_str().to_string();
    let payment_type = "credit".to_string();

    let costs: Vec<AccountCostRecord> = match msg.message_type {
        MessageType::Post | MessageType::Aggregate | MessageType::Forget => return Ok(()),
        MessageType::Store => {
            let store_content = match &content.content {
                MessageContentEnum::Store(s) => s,
                _ => return Ok(()),
            };
            let size_bytes = store_content.size.map(|s| s.count()).unwrap_or(0);
            let per_second = crate::cost::calculate_store_cost(size_bytes);
            vec![AccountCostRecord {
                owner,
                item_hash: item_hash.to_string(),
                cost_type: "STORAGE".to_string(),
                name: item_hash.to_string(),
                ref_hash: None,
                payment_type,
                cost_hold: "0".to_string(),
                cost_stream: "0".to_string(),
                cost_credit: per_second.to_string(),
            }]
        }
        MessageType::Program => {
            let program = match &content.content {
                MessageContentEnum::Program(p) => p,
                _ => return Ok(()),
            };
            let vcpus = program.base.resources.vcpus;
            let memory_mib: u64 = program.base.resources.memory.count();
            let memory_mib_u32 = memory_mib.min(u32::MAX as u64) as u32;
            let total_volume_mib = collect_vm_volume_mib_program(program);
            let per_second =
                crate::cost::calculate_vm_cost(vcpus, memory_mib_u32, total_volume_mib);
            vec![AccountCostRecord {
                owner,
                item_hash: item_hash.to_string(),
                cost_type: "EXECUTION".to_string(),
                name: item_hash.to_string(),
                ref_hash: None,
                payment_type,
                cost_hold: "0".to_string(),
                cost_stream: "0".to_string(),
                cost_credit: per_second.to_string(),
            }]
        }
        MessageType::Instance => {
            let instance = match &content.content {
                MessageContentEnum::Instance(i) => i,
                _ => return Ok(()),
            };
            let vcpus = instance.base.resources.vcpus;
            let memory_mib: u64 = instance.base.resources.memory.count();
            let memory_mib_u32 = memory_mib.min(u32::MAX as u64) as u32;
            let total_volume_mib = collect_vm_volume_mib_instance(instance);
            let per_second =
                crate::cost::calculate_vm_cost(vcpus, memory_mib_u32, total_volume_mib);
            vec![AccountCostRecord {
                owner,
                item_hash: item_hash.to_string(),
                cost_type: "EXECUTION".to_string(),
                name: item_hash.to_string(),
                ref_hash: None,
                payment_type,
                cost_hold: "0".to_string(),
                cost_stream: "0".to_string(),
                cost_credit: per_second.to_string(),
            }]
        }
    };

    if !costs.is_empty() {
        db.with_conn(|conn| insert_account_costs(conn, &costs))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
    }

    Ok(())
}

pub mod aggregate;
pub mod forget;
pub mod instance;
pub mod post;
pub mod program;
pub mod store;
pub mod validate;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        assert_eq!(ProcessingError::InternalError("".into()).error_code(), -1);
        assert_eq!(ProcessingError::InvalidFormat("".into()).error_code(), 0);
        assert_eq!(ProcessingError::InvalidSignature("".into()).error_code(), 1);
        assert_eq!(ProcessingError::PermissionDenied("".into()).error_code(), 2);
        assert_eq!(
            ProcessingError::ContentUnavailable("".into()).error_code(),
            3
        );
        assert_eq!(ProcessingError::FileUnavailable("".into()).error_code(), 4);
        assert_eq!(
            ProcessingError::BalanceInsufficient("".into()).error_code(),
            5
        );
        assert_eq!(
            ProcessingError::CreditInsufficient("".into()).error_code(),
            6
        );
        assert_eq!(
            ProcessingError::PostAmendNoTarget("".into()).error_code(),
            100
        );
        assert_eq!(
            ProcessingError::PostAmendTargetNotFound("".into()).error_code(),
            101
        );
        assert_eq!(ProcessingError::PostAmendAmend("".into()).error_code(), 102);
        assert_eq!(
            ProcessingError::StoreRefNotFound("".into()).error_code(),
            200
        );
        assert_eq!(
            ProcessingError::StoreUpdateUpdate("".into()).error_code(),
            201
        );
        assert_eq!(
            ProcessingError::InvalidPaymentMethod("".into()).error_code(),
            202
        );
        assert_eq!(ProcessingError::VmRefNotFound("".into()).error_code(), 300);
        assert_eq!(
            ProcessingError::VmVolumeNotFound("".into()).error_code(),
            301
        );
        assert_eq!(
            ProcessingError::VmAmendNotAllowed("".into()).error_code(),
            302
        );
        assert_eq!(ProcessingError::VmUpdateUpdate("".into()).error_code(), 303);
        assert_eq!(
            ProcessingError::VmVolumeTooSmall("".into()).error_code(),
            304
        );
        assert_eq!(ProcessingError::ForgetNoTarget("".into()).error_code(), 500);
        assert_eq!(
            ProcessingError::ForgetTargetNotFound("".into()).error_code(),
            501
        );
        assert_eq!(ProcessingError::ForgetForget("".into()).error_code(), 502);
        assert_eq!(
            ProcessingError::ForgetNotAllowed("".into()).error_code(),
            503
        );
        assert_eq!(
            ProcessingError::ForgottenDuplicate("".into()).error_code(),
            504
        );
    }

    #[test]
    fn test_deserialize_inline_incoming_message() {
        let json = r#"{
            "chain": "ETH",
            "sender": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            "signature": "0xdeadbeef",
            "type": "POST",
            "item_type": "inline",
            "item_content": "{\"type\":\"test\",\"address\":\"0xB68B9D4f3771c246233823ed1D3Add451055F9Ef\",\"time\":1000.0}",
            "item_hash": "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c",
            "time": 1000.0
        }"#;
        let msg: IncomingMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.item_type, ItemType::Inline);
        assert!(msg.item_content.is_some());
    }

    #[test]
    fn test_deserialize_storage_incoming_message() {
        let json = r#"{
            "chain": "ETH",
            "sender": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            "signature": "0xdeadbeef",
            "type": "STORE",
            "item_type": "storage",
            "item_hash": "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c",
            "time": 1000.0
        }"#;
        let msg: IncomingMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.item_type, ItemType::Storage);
        assert!(msg.item_content.is_none());
    }

    // -----------------------------------------------------------------------
    // Pipeline tests
    // -----------------------------------------------------------------------

    use crate::db::Db;
    use crate::db::messages::get_message_status;
    use aleph_types::account::{Account, EvmAccount, sign_message};
    use aleph_types::item_hash::{AlephItemHash, ItemHash};
    use aleph_types::message::MessageType;
    use aleph_types::message::unsigned::UnsignedMessage;
    use aleph_types::timestamp::Timestamp;

    /// Build a valid POST item_content string.
    fn make_post_content(addr: &str, time: f64) -> String {
        format!(
            r#"{{"type":"test","address":"{}","time":{},"content":{{"body":"Hello"}}}}"#,
            addr, time
        )
    }

    /// Sign a POST inline message and return it as an `IncomingMessage`.
    fn sign_post_message(key: &[u8; 32], time: f64) -> IncomingMessage {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let addr_str = account.address().as_str().to_string();
        let item_content = make_post_content(&addr_str, time);
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));

        let unsigned = UnsignedMessage {
            message_type: MessageType::Post,
            item_type: ItemType::Inline,
            item_content: item_content.clone(),
            item_hash: item_hash.clone(),
            time: Timestamp::from(time),
            channel: None,
        };

        let pending = sign_message(&account, unsigned).unwrap();

        IncomingMessage {
            chain: pending.chain,
            sender: pending.sender,
            signature: pending.signature,
            message_type: pending.message_type,
            item_type: pending.item_type,
            item_content: Some(pending.item_content),
            item_hash: pending.item_hash,
            time: pending.time,
            channel: pending.channel,
        }
    }

    #[test]
    fn test_valid_signed_message_accepted() {
        let key = [1u8; 32];
        let msg = sign_post_message(&key, 1_700_000_000.0);
        let db = Db::open_in_memory().unwrap();

        let result = process_message(&db, &msg);
        assert!(result.is_ok(), "expected Ok but got {:?}", result);

        // Verify it was stored as PROCESSED.
        let hash_str = msg.item_hash.to_string();
        let status = db
            .with_conn(|conn| get_message_status(conn, &hash_str))
            .unwrap();
        assert_eq!(status, Some("processed".to_string()));
    }

    #[test]
    fn test_tampered_signature_rejected() {
        let key = [1u8; 32];
        let mut msg = sign_post_message(&key, 1_700_000_001.0);

        // Replace signature with garbage.
        msg.signature = Signature::from("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef00".to_string());

        let db = Db::open_in_memory().unwrap();
        let result = process_message(&db, &msg);

        assert!(result.is_err(), "expected Err but got Ok");
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code(),
            1,
            "expected INVALID_SIGNATURE (1), got code {}",
            err.error_code()
        );
    }

    #[test]
    fn test_duplicate_processed_skipped_silently() {
        let key = [2u8; 32];
        let msg = sign_post_message(&key, 1_700_000_002.0);
        let db = Db::open_in_memory().unwrap();

        // First submission.
        process_message(&db, &msg).expect("first submission should succeed");

        // Second submission of the same message — should succeed silently (duplicate skip).
        let result = process_message(&db, &msg);
        assert!(
            result.is_ok(),
            "second (duplicate) submission should return Ok, got {:?}",
            result
        );
    }

    #[test]
    fn test_wrong_sender_rejected() {
        let key = [3u8; 32];
        let mut msg = sign_post_message(&key, 1_700_000_003.0);

        // Replace the sender with a different address.
        msg.sender = aleph_types::address!("0x0000000000000000000000000000000000000001");

        let db = Db::open_in_memory().unwrap();
        let result = process_message(&db, &msg);

        assert!(result.is_err(), "expected Err for wrong sender");
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code(),
            1,
            "expected INVALID_SIGNATURE (1), got code {}",
            err.error_code()
        );
    }

    #[test]
    fn test_storage_message_resolved_from_file_store() {
        let key = [4u8; 32];
        let account = EvmAccount::new(Chain::Ethereum, &key).unwrap();
        let addr_str = account.address().as_str().to_string();

        // Build a POST content payload and pre-write it to the file store.
        let item_content = make_post_content(&addr_str, 1_700_000_004.0);
        let tmp = tempfile::tempdir().unwrap();
        let store = crate::files::FileStore::new(tmp.path()).unwrap();
        let file_hash = store.write(item_content.as_bytes()).unwrap();

        // Build a storage-type message (no item_content, hash points to stored file).
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));
        assert_eq!(item_hash.to_string(), file_hash);

        let unsigned = UnsignedMessage {
            message_type: MessageType::Post,
            item_type: ItemType::Storage,
            item_content: item_content.clone(),
            item_hash: item_hash.clone(),
            time: Timestamp::from(1_700_000_004.0),
            channel: None,
        };
        let pending = sign_message(&account, unsigned).unwrap();

        let msg = IncomingMessage {
            chain: pending.chain,
            sender: pending.sender,
            signature: pending.signature,
            message_type: pending.message_type,
            item_type: ItemType::Storage,
            item_content: None, // not inline — content must be fetched
            item_hash: pending.item_hash,
            time: pending.time,
            channel: pending.channel,
        };

        let db = Db::open_in_memory().unwrap();

        // Without file store: should fail with ContentUnavailable.
        let err = process_message(&db, &msg).unwrap_err();
        assert_eq!(err.error_code(), 3, "expected CONTENT_UNAVAILABLE");

        // With file store: should succeed.
        let result = process_message_with_store(&db, &msg, Some(&store));
        assert!(result.is_ok(), "expected Ok but got {:?}", result);

        let status = db
            .with_conn(|conn| get_message_status(conn, &file_hash))
            .unwrap();
        assert_eq!(status, Some("processed".to_string()));
    }
}
