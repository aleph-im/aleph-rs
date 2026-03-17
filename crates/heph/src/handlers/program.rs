//! PROGRAM message handler — spec section 3.4.

use crate::db::Db;
use crate::db::messages::get_message_by_hash;
use crate::db::vms::{VmRecord, VmVolumeRecord, insert_vm, insert_vm_volumes, is_vm_amend_allowed};
use crate::handlers::{IncomingMessage, ProcessingError, ProcessingResult};
use aleph_types::message::MessageContent;
use aleph_types::message::MessageContentEnum;
use aleph_types::message::execution::volume::MachineVolume;

/// Process a PROGRAM message (spec section 3.4).
pub fn process_program(
    db: &Db,
    msg: &IncomingMessage,
    content: &MessageContent,
) -> ProcessingResult<()> {
    // Extract ProgramContent.
    let program = match &content.content {
        MessageContentEnum::Program(p) => p,
        _ => {
            return Err(ProcessingError::InternalError(
                "process_program called with non-PROGRAM content".into(),
            ));
        }
    };

    let item_hash = msg.item_hash.to_string();
    let owner = content.address.as_str().to_string();
    let time = content.time.as_f64();

    // Step 2 — validate code, runtime, and data volume refs.
    validate_store_ref(db, program.code.reference.to_string().as_str())?;
    validate_store_ref(db, program.runtime.reference.to_string().as_str())?;
    if let Some(data) = &program.data {
        validate_store_ref(db, data.reference.to_string().as_str())?;
    }

    // Validate immutable volumes in the base volumes list.
    for vol in &program.base.volumes {
        if let MachineVolume::Immutable(imm) = vol {
            validate_store_ref(db, imm.reference.to_string().as_str())?;
        }
    }

    // Step 3 — check replaces/amend.
    if let Some(replaces) = &program.base.replaces {
        let replaces_str = replaces.to_string();
        match is_vm_amend_allowed_checked(db, &replaces_str)? {
            Some(true) => {} // allowed
            Some(false) => {
                return Err(ProcessingError::VmAmendNotAllowed(format!(
                    "VM {replaces_str} does not allow amendments"
                )));
            }
            None => {
                return Err(ProcessingError::VmRefNotFound(format!(
                    "VM {replaces_str} not found"
                )));
            }
        }
    }

    // Step 4 — extract payment type.
    let payment_type = program.base.payment.as_ref().map(|p| {
        serde_json::to_value(&p.payment_type)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| format!("{:?}", p.payment_type).to_lowercase())
    });

    // Step 5 — insert into vms table.
    let content_json = serde_json::to_string(&content.content)
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    let record = VmRecord {
        item_hash: item_hash.clone(),
        vm_type: "program".to_string(),
        owner,
        allow_amend: program.base.allow_amend,
        replaces: program.base.replaces.as_ref().map(|r| r.to_string()),
        time,
        content: content_json,
        payment_type,
    };

    db.with_conn(|conn| insert_vm(conn, &record))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // Step 6 — collect and insert volume records.
    let mut volumes: Vec<VmVolumeRecord> = Vec::new();

    // Code volume.
    volumes.push(VmVolumeRecord {
        volume_type: "code".to_string(),
        ref_hash: Some(program.code.reference.to_string()),
        use_latest: program.code.use_latest,
        size_mib: None,
        mount: None,
    });

    // Runtime volume.
    volumes.push(VmVolumeRecord {
        volume_type: "runtime".to_string(),
        ref_hash: Some(program.runtime.reference.to_string()),
        use_latest: program.runtime.use_latest,
        size_mib: None,
        mount: None,
    });

    // Data volume (optional).
    if let Some(data) = &program.data {
        volumes.push(VmVolumeRecord {
            volume_type: "data".to_string(),
            ref_hash: Some(data.reference.to_string()),
            use_latest: data.use_latest.unwrap_or(false),
            size_mib: None,
            mount: Some(data.mount.to_string_lossy().to_string()),
        });
    }

    // Machine volumes (immutable, ephemeral, persistent).
    for vol in &program.base.volumes {
        match vol {
            MachineVolume::Immutable(imm) => {
                volumes.push(VmVolumeRecord {
                    volume_type: "immutable".to_string(),
                    ref_hash: Some(imm.reference.to_string()),
                    use_latest: imm.use_latest,
                    size_mib: None,
                    mount: imm
                        .base
                        .mount
                        .as_ref()
                        .map(|m| m.to_string_lossy().to_string()),
                });
            }
            MachineVolume::Ephemeral(eph) => {
                let size: u64 = eph.size_mib.into();
                volumes.push(VmVolumeRecord {
                    volume_type: "ephemeral".to_string(),
                    ref_hash: None,
                    use_latest: false,
                    size_mib: Some(size as i64),
                    mount: eph
                        .base
                        .mount
                        .as_ref()
                        .map(|m| m.to_string_lossy().to_string()),
                });
            }
            MachineVolume::Persistent(per) => {
                let size: u64 = per.size_mib.into();
                volumes.push(VmVolumeRecord {
                    volume_type: "persistent".to_string(),
                    ref_hash: per.parent.as_ref().map(|p| p.reference.to_string()),
                    use_latest: per.parent.as_ref().map(|p| p.use_latest).unwrap_or(false),
                    size_mib: Some(size as i64),
                    mount: per
                        .base
                        .mount
                        .as_ref()
                        .map(|m| m.to_string_lossy().to_string()),
                });
            }
        }
    }

    db.with_conn(|conn| insert_vm_volumes(conn, &item_hash, &volumes))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    Ok(())
}

/// Check that a hash points to an existing STORE message.
pub(crate) fn validate_store_ref(db: &Db, ref_hash: &str) -> ProcessingResult<()> {
    let msg = db
        .with_conn(|conn| get_message_by_hash(conn, ref_hash))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
    match msg {
        Some(m) if m.message_type.to_uppercase() == "STORE" => Ok(()),
        Some(m) => Err(ProcessingError::VmRefNotFound(format!(
            "{ref_hash} is not a STORE message (got {})",
            m.message_type
        ))),
        None => Err(ProcessingError::VmRefNotFound(format!(
            "{ref_hash} not found"
        ))),
    }
}

/// Wrapper around `is_vm_amend_allowed` that maps DB errors.
fn is_vm_amend_allowed_checked(db: &Db, item_hash: &str) -> ProcessingResult<Option<bool>> {
    db.with_conn(|conn| is_vm_amend_allowed(conn, item_hash))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::db::vms::get_vm;
    use crate::handlers::process_message;
    use aleph_types::account::{Account, EvmAccount, sign_message};
    use aleph_types::chain::Chain;
    use aleph_types::item_hash::{AlephItemHash, ItemHash};
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use aleph_types::message::unsigned::UnsignedMessage;
    use aleph_types::timestamp::Timestamp;

    fn addr_for_key(key: &[u8; 32]) -> String {
        EvmAccount::new(Chain::Ethereum, key)
            .unwrap()
            .address()
            .as_str()
            .to_string()
    }

    fn sign_message_inline(
        key: &[u8; 32],
        msg_type: MessageType,
        time: f64,
        item_content: String,
    ) -> IncomingMessage {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));
        let unsigned = UnsignedMessage {
            message_type: msg_type,
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

    /// Build a minimal valid PROGRAM item_content with no volume refs to validate
    /// (code.ref, runtime.ref will point to fake hashes that we pre-insert as STORE messages).
    fn make_program_content(addr: &str, time: f64, code_ref: &str, runtime_ref: &str) -> String {
        format!(
            r#"{{
                "address": "{addr}",
                "time": {time},
                "allow_amend": false,
                "resources": {{"vcpus": 1, "memory": 128, "seconds": 10}},
                "environment": {{"reproducible": false, "internet": true, "aleph_api": true, "shared_cache": false}},
                "on": {{"http": true}},
                "code": {{"encoding": "zip", "entrypoint": "main:app", "ref": "{code_ref}", "use_latest": false}},
                "runtime": {{"ref": "{runtime_ref}", "use_latest": false, "comment": "test runtime"}}
            }}"#
        )
    }

    fn make_store_content(addr: &str, time: f64, file_hash: &str) -> String {
        format!(
            r#"{{"address":"{}","time":{},"item_type":"storage","item_hash":"{}"}}"#,
            addr, time, file_hash
        )
    }

    fn fake_hash(n: u8) -> String {
        format!("{:0>64}", format!("{:x}", n))
    }

    /// Insert a fake STORE message into the DB so volume ref validation passes.
    fn insert_fake_store(db: &Db, key: &[u8; 32], time: f64, file_hash: &str) -> String {
        let addr = addr_for_key(key);
        let ic = make_store_content(&addr, time, file_hash);
        let msg = sign_message_inline(key, MessageType::Store, time, ic);
        let hash = msg.item_hash.to_string();
        process_message(db, &msg).expect("fake store should be processed");
        hash
    }

    // -----------------------------------------------------------------------
    // Test 1: Program with no extra volume refs stores successfully
    // -----------------------------------------------------------------------

    #[test]
    fn test_program_stores_successfully() {
        let key = [40u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        // Pre-insert STORE messages for code and runtime refs.
        let code_fh = fake_hash(1);
        let runtime_fh = fake_hash(2);
        insert_fake_store(&db, &key, 999.0, &code_fh);
        insert_fake_store(&db, &key, 998.0, &runtime_fh);

        // Get actual item hashes from the DB (the STORE item_hash is what matters).
        // We use the STORE message hashes as the code/runtime refs.
        // But actually, we need the item_hash of the STORE, not the file_hash.
        // Let me re-read: validate_store_ref checks that the ref points to an existing STORE message.
        // So ref_hash must be the item_hash of the STORE message.
        let code_store_hash = {
            let ic = make_store_content(&addr, 999.0, &code_fh);
            let hash = AlephItemHash::from_bytes(ic.as_bytes());
            ItemHash::Native(hash).to_string()
        };
        let runtime_store_hash = {
            let ic = make_store_content(&addr, 998.0, &runtime_fh);
            let hash = AlephItemHash::from_bytes(ic.as_bytes());
            ItemHash::Native(hash).to_string()
        };

        let ic = make_program_content(&addr, 1_000.0, &code_store_hash, &runtime_store_hash);
        let msg = sign_message_inline(&key, MessageType::Program, 1_000.0, ic);

        let result = process_message(&db, &msg);
        assert!(result.is_ok(), "program should store: {:?}", result);
    }

    // -----------------------------------------------------------------------
    // Test 2: Program stored in vms table with type='program'
    // -----------------------------------------------------------------------

    #[test]
    fn test_program_stored_in_vms_table() {
        let key = [41u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        let code_fh = fake_hash(3);
        let runtime_fh = fake_hash(4);
        insert_fake_store(&db, &key, 999.0, &code_fh);
        insert_fake_store(&db, &key, 998.0, &runtime_fh);

        let code_store_hash = {
            let ic = make_store_content(&addr, 999.0, &code_fh);
            ItemHash::Native(AlephItemHash::from_bytes(ic.as_bytes())).to_string()
        };
        let runtime_store_hash = {
            let ic = make_store_content(&addr, 998.0, &runtime_fh);
            ItemHash::Native(AlephItemHash::from_bytes(ic.as_bytes())).to_string()
        };

        let ic = make_program_content(&addr, 1_001.0, &code_store_hash, &runtime_store_hash);
        let msg = sign_message_inline(&key, MessageType::Program, 1_001.0, ic);
        let item_hash = msg.item_hash.to_string();

        process_message(&db, &msg).expect("should process");

        db.with_conn(|conn| {
            let record = get_vm(conn, &item_hash).unwrap().expect("VM should exist");
            assert_eq!(record.vm_type, "program");
            assert_eq!(record.owner.to_lowercase(), addr.to_lowercase());
            assert!(!record.allow_amend);
        });
    }
}
