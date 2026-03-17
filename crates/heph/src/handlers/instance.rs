//! INSTANCE message handler — spec section 3.5.

use crate::db::Db;
use crate::db::vms::{VmRecord, VmVolumeRecord, insert_vm, insert_vm_volumes, is_vm_amend_allowed};
use crate::handlers::program::validate_store_ref;
use crate::handlers::{IncomingMessage, ProcessingError, ProcessingResult};
use aleph_types::message::MessageContent;
use aleph_types::message::MessageContentEnum;
use aleph_types::message::execution::volume::MachineVolume;

/// Process an INSTANCE message (spec section 3.5).
pub fn process_instance(
    db: &Db,
    msg: &IncomingMessage,
    content: &MessageContent,
) -> ProcessingResult<()> {
    // Extract InstanceContent.
    let instance = match &content.content {
        MessageContentEnum::Instance(i) => i,
        _ => {
            return Err(ProcessingError::InternalError(
                "process_instance called with non-INSTANCE content".into(),
            ));
        }
    };

    let item_hash = msg.item_hash.to_string();
    let owner = content.address.as_str().to_string();
    let time = content.time.as_f64();

    // Step 2 — validate rootfs parent ref.
    validate_store_ref(db, instance.rootfs.parent.reference.to_string().as_str())?;

    // Validate immutable volumes in the base volumes list.
    for vol in &instance.base.volumes {
        if let MachineVolume::Immutable(imm) = vol {
            validate_store_ref(db, imm.reference.to_string().as_str())?;
        }
    }

    // Step 4 — check replaces/amend.
    if let Some(replaces) = &instance.base.replaces {
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

    // Extract payment type.
    let payment_type = instance.base.payment.as_ref().map(|p| {
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
        vm_type: "instance".to_string(),
        owner,
        allow_amend: instance.base.allow_amend,
        replaces: instance.base.replaces.as_ref().map(|r| r.to_string()),
        time,
        content: content_json,
        payment_type,
    };

    db.with_conn(|conn| insert_vm(conn, &record))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // Step 6 — collect and insert volume records.
    let mut volumes: Vec<VmVolumeRecord> = Vec::new();

    // Rootfs volume.
    let rootfs_size: u64 = instance.rootfs.size_mib.into();
    volumes.push(VmVolumeRecord {
        volume_type: "rootfs".to_string(),
        ref_hash: Some(instance.rootfs.parent.reference.to_string()),
        use_latest: instance.rootfs.parent.use_latest,
        size_mib: Some(rootfs_size as i64),
        mount: None,
    });

    // Machine volumes (immutable, ephemeral, persistent).
    for vol in &instance.base.volumes {
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

    fn make_store_content(addr: &str, time: f64, file_hash: &str) -> String {
        format!(
            r#"{{"address":"{}","time":{},"item_type":"storage","item_hash":"{}"}}"#,
            addr, time, file_hash
        )
    }

    fn make_instance_content(addr: &str, time: f64, rootfs_ref: &str) -> String {
        format!(
            r#"{{
                "address": "{addr}",
                "time": {time},
                "allow_amend": false,
                "resources": {{"vcpus": 2, "memory": 2048, "seconds": 30}},
                "environment": {{"internet": true, "aleph_api": true, "reproducible": false, "shared_cache": false}},
                "rootfs": {{
                    "parent": {{"ref": "{rootfs_ref}", "use_latest": false}},
                    "persistence": "host",
                    "size_mib": 20480
                }},
                "volumes": []
            }}"#
        )
    }

    fn fake_hash(n: u8) -> String {
        format!("{:0>64}", format!("{:x}", n))
    }

    /// Insert a fake STORE message into the DB so volume ref validation passes.
    fn insert_fake_store(db: &Db, key: &[u8; 32], time: f64, file_hash: &str) -> String {
        let addr = addr_for_key(key);
        let ic = make_store_content(&addr, time, file_hash);
        let msg = sign_message_inline(key, MessageType::Store, time, ic.clone());
        let hash = msg.item_hash.to_string();
        process_message(db, &msg).expect("fake store should be processed");
        hash
    }

    // -----------------------------------------------------------------------
    // Test 1: Instance stores successfully
    // -----------------------------------------------------------------------

    #[test]
    fn test_instance_stores_successfully() {
        let key = [50u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        let rootfs_fh = fake_hash(10);
        let rootfs_store_hash = insert_fake_store(&db, &key, 999.0, &rootfs_fh);

        let ic = make_instance_content(&addr, 1_000.0, &rootfs_store_hash);
        let msg = sign_message_inline(&key, MessageType::Instance, 1_000.0, ic);

        let result = process_message(&db, &msg);
        assert!(result.is_ok(), "instance should store: {:?}", result);
    }

    // -----------------------------------------------------------------------
    // Test 2: Instance stored in vms table with type='instance'
    // -----------------------------------------------------------------------

    #[test]
    fn test_instance_stored_in_vms_table() {
        let key = [51u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        let rootfs_fh = fake_hash(11);
        let rootfs_store_hash = insert_fake_store(&db, &key, 999.0, &rootfs_fh);

        let ic = make_instance_content(&addr, 1_001.0, &rootfs_store_hash);
        let msg = sign_message_inline(&key, MessageType::Instance, 1_001.0, ic);
        let item_hash = msg.item_hash.to_string();

        process_message(&db, &msg).expect("should process");

        db.with_conn(|conn| {
            let record = get_vm(conn, &item_hash).unwrap().expect("VM should exist");
            assert_eq!(record.vm_type, "instance");
            assert_eq!(record.owner.to_lowercase(), addr.to_lowercase());
            assert!(!record.allow_amend);
        });
    }
}
