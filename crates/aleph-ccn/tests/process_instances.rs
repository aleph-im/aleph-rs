//! Ports `tests/message_processing/test_process_instances.py` and
//! `test_process_confidential.py`. Both exercise the INSTANCE handler.
//!
//! Many of the Python tests rely on real `MessageHandler` plumbing (balance
//! checks + volume-ref lookups). The tests here drive [`VmMessageHandler`]
//! directly and verify the database side-effects: a `vms` row + a
//! `vm_versions` pointer.

mod common;

use chrono::{TimeZone, Utc};
use serde_json::{Value, json};

use aleph_ccn::db::accessors::vms::{get_instance, get_vm_version};
use aleph_ccn::db::models::messages::MessageDb;
use aleph_ccn::handlers::content::content_handler::ContentHandler;
use aleph_ccn::handlers::content::vm::VmMessageHandler;
use aleph_ccn::types::channel::Channel;
use aleph_ccn::types::message_status::MessageStatus;
use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use common::{start_postgres};

fn instance_content(sender: &str, time: f64) -> Value {
    // V0059 made `vm_machine_volumes.id` an IDENTITY column; this test keeps
    // the `volumes` list empty and stays focused on the `vms` row + the
    // `vm_versions` pointer.
    json!({
        "address": sender,
        "time": time,
        "allow_amend": false,
        "variables": {},
        "environment": {
            "reproducible": true,
            "internet": false,
            "aleph_api": false,
            "shared_cache": false,
            "hypervisor": "qemu",
        },
        "resources": {"vcpus": 1, "memory": 128, "seconds": 30},
        "rootfs": {
            "parent": {
                "ref": "549ec451d9b099cad112d4aaa2c00ac40fb6729a92ff252ff22eef0b5c3cb613",
                "use_latest": true,
            },
            "persistence": "host",
            "size_mib": 20000,
        },
        "authorized_keys": ["ssh-ed25519 AAAA..."],
        "volumes": [],
    })
}

fn confidential_instance_content(sender: &str, time: f64) -> Value {
    let mut c = instance_content(sender, time);
    let env = c.get_mut("environment").unwrap().as_object_mut().unwrap();
    env.insert(
        "trusted_execution".into(),
        json!({
            "policy": 1,
            "firmware": "e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317",
        }),
    );
    let req = json!({
        "cpu": {"architecture": "x86_64"},
        "node": {"node_hash": "149ec451d9b099cad112d4aaa2c00ac40fb6729a92ff252ff22eef0b5c3cb6db"},
    });
    c.as_object_mut().unwrap().insert("requirements".into(), req);
    c
}

fn instance_message(item_hash: &str, sender: &str, time: f64, content: Value) -> MessageDb {
    let dt = Utc.timestamp_opt(time as i64, 0).unwrap();
    MessageDb {
        item_hash: item_hash.into(),
        r#type: MessageType::Instance,
        chain: Chain::Ethereum,
        sender: sender.into(),
        signature: Some("0xsig".into()),
        item_type: ItemType::Inline,
        item_content: Some(content.to_string()),
        content,
        time: dt,
        channel: Some(Channel::from("TEST".to_string())),
        size: 2048,
        status_value: MessageStatus::Processed,
        reception_time: dt,
        owner: Some(sender.into()),
        content_type: None,
        content_ref: None,
        content_key: None,
        content_item_hash: None,
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: Some("hold".into()),
        tags: None,
    }
}

async fn process_instance(pool: &aleph_ccn::db::DbPool, msg: MessageDb) {
    let h = VmMessageHandler::new();
    let mut client = pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    ContentHandler::process(&h, &*tx, &[msg]).await.unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn process_instance_inserts_vm_row_and_version() {
    let pg = start_postgres().await;
    let sender = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";
    let item_hash = "734a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26";
    let msg = instance_message(item_hash, sender, 1_619_017_773.0, instance_content(sender, 1_619_017_773.0));
    process_instance(&pg.pool, msg).await;

    let client = pg.pool.get().await.unwrap();
    let inst = get_instance(&**client, item_hash).await.unwrap().unwrap();
    assert_eq!(inst.owner, sender);
    assert!(!inst.allow_amend);
    assert_eq!(inst.resources_vcpus, 1);
    assert_eq!(inst.resources_memory, 128);

    let version = get_vm_version(&**client, item_hash).await.unwrap().unwrap();
    assert_eq!(version.owner, sender);
}

#[tokio::test]
async fn process_confidential_instance_stores_trusted_execution_fields() {
    let pg = start_postgres().await;
    let sender = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";
    let item_hash = "844a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26";
    let msg = instance_message(
        item_hash,
        sender,
        1_619_017_773.0,
        confidential_instance_content(sender, 1_619_017_773.0),
    );
    process_instance(&pg.pool, msg).await;
    let client = pg.pool.get().await.unwrap();
    let inst = get_instance(&**client, item_hash).await.unwrap().unwrap();
    assert_eq!(inst.environment_trusted_execution_policy, Some(1));
    assert_eq!(
        inst.environment_trusted_execution_firmware.as_deref(),
        Some("e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317"),
    );
    assert_eq!(
        inst.node_hash.as_deref(),
        Some("149ec451d9b099cad112d4aaa2c00ac40fb6729a92ff252ff22eef0b5c3cb6db"),
    );
}

#[tokio::test]
async fn forget_instance_deletes_vm_row() {
    let pg = start_postgres().await;
    let h = VmMessageHandler::new();
    let sender = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";
    let item_hash = "944a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26";
    let msg = instance_message(item_hash, sender, 1_619_018_000.0, instance_content(sender, 1_619_018_000.0));
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::forget_message(&h, &*tx, &msg).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let inst = get_instance(&**client, item_hash).await.unwrap();
    assert!(inst.is_none());
}

#[tokio::test]
async fn process_two_instances_inserts_two_vm_rows() {
    let pg = start_postgres().await;
    let h = VmMessageHandler::new();
    let sender = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";
    let ihs = [
        "aa4a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26",
        "bb4a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26",
    ];
    for (i, ih) in ihs.iter().enumerate() {
        let msg = instance_message(
            ih,
            sender,
            1_619_018_000.0 + (i as f64) * 60.0,
            instance_content(sender, 1_619_018_000.0 + (i as f64) * 60.0),
        );
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg]).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    for ih in ihs.iter() {
        let inst = get_instance(&**client, ih).await.unwrap();
        assert!(inst.is_some());
    }
}

#[tokio::test]
async fn instance_resources_round_trip_through_db() {
    let pg = start_postgres().await;
    let h = VmMessageHandler::new();
    let sender = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";
    let item_hash = "cc4a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26";
    let mut content = instance_content(sender, 1_619_018_000.0);
    content.as_object_mut().unwrap().get_mut("resources").unwrap().as_object_mut().unwrap()
        .insert("vcpus".into(), json!(4));
    content.as_object_mut().unwrap().get_mut("resources").unwrap().as_object_mut().unwrap()
        .insert("memory".into(), json!(8192));
    let msg = instance_message(item_hash, sender, 1_619_018_000.0, content);
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg]).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let inst = get_instance(&**client, item_hash).await.unwrap().unwrap();
    assert_eq!(inst.resources_vcpus, 4);
    assert_eq!(inst.resources_memory, 8192);
}

#[tokio::test]
async fn instance_environment_fields_persisted() {
    let pg = start_postgres().await;
    let h = VmMessageHandler::new();
    let sender = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";
    let item_hash = "dd4a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26";
    let msg = instance_message(item_hash, sender, 1_619_019_000.0, instance_content(sender, 1_619_019_000.0));
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg]).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let inst = get_instance(&**client, item_hash).await.unwrap().unwrap();
    assert!(inst.environment_reproducible);
    assert!(!inst.environment_internet);
    assert!(!inst.environment_aleph_api);
    assert!(!inst.environment_shared_cache);
}

#[tokio::test]
async fn instance_message_with_no_resources_still_inserts_row() {
    let pg = start_postgres().await;
    let h = VmMessageHandler::new();
    let sender = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";
    let item_hash = "ee4a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26";
    let mut content = instance_content(sender, 1_619_020_000.0);
    content.as_object_mut().unwrap().remove("resources");
    let msg = instance_message(item_hash, sender, 1_619_020_000.0, content);
    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    // The handler should not panic even when `resources` is absent; the row
    // is inserted with NULL columns.
    ContentHandler::process(&h, &*tx, &[msg]).await.unwrap();
    let inst = get_instance(&*tx, item_hash).await.unwrap();
    tx.commit().await.unwrap();
    assert!(inst.is_some());
}

#[tokio::test]
async fn instance_message_with_metadata_persists_metadata_field() {
    let pg = start_postgres().await;
    let h = VmMessageHandler::new();
    let sender = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";
    let item_hash = "ff4a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26";
    let mut content = instance_content(sender, 1_619_021_000.0);
    content
        .as_object_mut()
        .unwrap()
        .insert("metadata".into(), json!({"name": "My VM", "description": "hi"}));
    let msg = instance_message(item_hash, sender, 1_619_021_000.0, content);
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg]).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let inst = get_instance(&**client, item_hash).await.unwrap().unwrap();
    assert!(inst.metadata.is_some());
}
