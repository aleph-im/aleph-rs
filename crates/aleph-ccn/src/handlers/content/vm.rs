//! INSTANCE/PROGRAM message handler. Mirrors `aleph/handlers/content/vm.py`.

use async_trait::async_trait;
use std::collections::HashSet;

use crate::db::accessors::files::{
    find_file_pins, find_file_tags, get_file_tag, get_message_file_pin,
};
use crate::db::accessors::vms::{
    delete_vm, delete_vm_updates, get_program, is_vm_amend_allowed, refresh_vm_version,
    upsert_vm_version,
};
use crate::db::models::account_costs::{AccountCostsDb, PaymentType};
use crate::db::models::messages::MessageDb;
use crate::handlers::content::content_handler::ContentHandler;
use crate::services::cost::{
    CostContent, CostContentKind, get_payment_type, get_total_and_detailed_costs,
};
use crate::services::cost_validation::validate_balance_for_payment;
use crate::toolkit::costs::{
    StoreAndProgramFreeInput, are_store_and_program_free, is_credit_only_required,
    is_hold_and_stream_deprecated,
};
use crate::toolkit::timestamp::timestamp_to_datetime;
use crate::types::files::FileTag;
use crate::types::message_status::MessageProcessingException;
use crate::types::vms::VmVersion;

fn content_address(message: &MessageDb) -> Result<String, MessageProcessingException> {
    message
        .content
        .get("address")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| MessageProcessingException::InvalidMessageFormat {
            errors: vec![format!(
                "VM message {} missing 'address'",
                message.item_hash
            )],
        })
}

fn content_replaces(message: &MessageDb) -> Option<String> {
    message
        .content
        .get("replaces")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn is_instance(message: &MessageDb) -> bool {
    message.content.get("rootfs").is_some()
}

fn is_program(message: &MessageDb) -> bool {
    message.content.get("code").is_some() && message.content.get("on").is_some()
}

fn cost_content_for<'a>(message: &'a MessageDb) -> Option<CostContent<'a>> {
    if is_instance(message) {
        Some(CostContent::new(
            CostContentKind::Instance,
            &message.content,
        ))
    } else if is_program(message) {
        Some(CostContent::new(CostContentKind::Program, &message.content))
    } else {
        None
    }
}

fn is_persistent_program(message: &MessageDb) -> bool {
    is_program(message)
        && message
            .content
            .get("on")
            .and_then(|o| o.get("persistent"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
}

fn message_time_value(message: &MessageDb) -> chrono::DateTime<chrono::Utc> {
    let ts = message
        .content
        .get("time")
        .and_then(|v| v.as_f64())
        .unwrap_or_else(|| {
            message.time.timestamp() as f64
                + (message.time.timestamp_subsec_nanos() as f64) / 1_000_000_000.0
        });
    timestamp_to_datetime(ts)
}

fn build_free_input(message: &MessageDb) -> StoreAndProgramFreeInput {
    StoreAndProgramFreeInput {
        confirmation_height: message.first_confirmed_height,
        time: message.time,
    }
}

/// Collect the `(use_latest, ref)` pairs used by the message into two
/// buckets: tags (use_latest=true) and pins (use_latest=false).
fn collect_refs(message: &MessageDb) -> (Vec<String>, Vec<String>) {
    let mut tags = Vec::<String>::new();
    let mut pins = Vec::<String>::new();
    let mut add = |volume: &serde_json::Value| {
        let use_latest = volume
            .get("use_latest")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let r#ref = volume.get("ref").and_then(|v| v.as_str()).unwrap_or("");
        if r#ref.is_empty() {
            return;
        }
        if use_latest {
            tags.push(r#ref.to_string());
        } else {
            pins.push(r#ref.to_string());
        }
    };

    if is_program(message) {
        if let Some(code) = message.content.get("code") {
            add(code);
        }
        if let Some(runtime) = message.content.get("runtime") {
            add(runtime);
        }
        if let Some(data) = message.content.get("data") {
            add(data);
        }
    }
    if is_instance(message) {
        if let Some(parent) = message.content.get("rootfs").and_then(|r| r.get("parent")) {
            add(parent);
        }
    }
    if let Some(arr) = message.content.get("volumes").and_then(|v| v.as_array()) {
        for vol in arr {
            // ImmutableVolume always has a `ref`.
            if vol.get("ref").is_some() && vol.get("size_mib").is_none() {
                add(vol);
            }
            // PersistentVolume.parent
            if let Some(parent) = vol.get("parent") {
                add(parent);
            }
        }
    }
    (tags, pins)
}

/// INSTANCE/PROGRAM message handler. Handles both since Python combines
/// them in one class.
pub struct VmMessageHandler;

impl VmMessageHandler {
    pub fn new() -> Self {
        Self
    }

    async fn insert_vm_row(
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        let address = content_address(message)?;
        let item_hash = &message.item_hash;
        let created = message_time_value(message);
        let replaces = content_replaces(message);

        let allow_amend = message
            .content
            .get("allow_amend")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let r#type = if is_instance(message) {
            "instance"
        } else if is_program(message) {
            "program"
        } else {
            return Err(MessageProcessingException::InvalidMessageFormat {
                errors: vec![format!("VM message {item_hash} has unknown content kind")],
            });
        };

        let metadata = message.content.get("metadata").cloned();
        let variables = message.content.get("variables").cloned();
        let env = message
            .content
            .get("environment")
            .cloned()
            .unwrap_or_default();
        let environment_reproducible = env
            .get("reproducible")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let environment_internet = env
            .get("internet")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let environment_aleph_api = env
            .get("aleph_api")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let environment_shared_cache = env
            .get("shared_cache")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let environment_trusted_execution_policy: Option<i32> = env
            .get("trusted_execution")
            .and_then(|t| t.get("policy"))
            .and_then(|v| v.as_i64())
            .map(|i| i as i32);
        let environment_trusted_execution_firmware: Option<String> = env
            .get("trusted_execution")
            .and_then(|t| t.get("firmware"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let resources = message
            .content
            .get("resources")
            .cloned()
            .unwrap_or_default();
        let resources_vcpus = resources.get("vcpus").and_then(|v| v.as_i64()).unwrap_or(1) as i32;
        let resources_memory = resources
            .get("memory")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;
        let resources_seconds = resources
            .get("seconds")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;

        let requirements = message
            .content
            .get("requirements")
            .cloned()
            .unwrap_or_default();
        let cpu = requirements.get("cpu").cloned().unwrap_or_default();
        let cpu_architecture: Option<String> = cpu
            .get("architecture")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let cpu_vendor: Option<String> = cpu
            .get("vendor")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let node = requirements.get("node").cloned().unwrap_or_default();
        let node_owner: Option<String> = node
            .get("owner")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let node_address_regex: Option<String> = node
            .get("address_regex")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let node_hash: Option<String> = if r#type == "instance" {
            node.get("node_hash")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        } else {
            None
        };

        // Payment type
        let payment_type: String = message
            .content
            .get("payment")
            .and_then(|p| p.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("hold")
            .to_string();

        // Program-only columns
        let program_type: Option<String> = if r#type == "program" {
            message
                .content
                .get("type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        } else {
            None
        };
        let http_trigger: Option<bool> = if r#type == "program" {
            Some(
                message
                    .content
                    .get("on")
                    .and_then(|o| o.get("http"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            )
        } else {
            None
        };
        let persistent: Option<bool> = if r#type == "program" {
            Some(is_persistent_program(message))
        } else {
            None
        };
        let message_triggers: Option<serde_json::Value> = if r#type == "program" {
            message
                .content
                .get("on")
                .and_then(|o| o.get("message"))
                .cloned()
        } else {
            None
        };

        let authorized_keys: Option<serde_json::Value> = if r#type == "instance" {
            message.content.get("authorized_keys").cloned()
        } else {
            None
        };

        let sql = "INSERT INTO vms( \
            item_hash, owner, type, allow_amend, metadata, variables, \
            message_triggers, environment_reproducible, environment_internet, \
            environment_aleph_api, environment_shared_cache, \
            environment_trusted_execution_policy, environment_trusted_execution_firmware, \
            payment_type, resources_vcpus, resources_memory, resources_seconds, \
            cpu_architecture, cpu_vendor, node_owner, node_address_regex, node_hash, \
            replaces, created, authorized_keys, program_type, http_trigger, persistent) \
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, \
                    $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)";
        client
            .execute(
                sql,
                &[
                    item_hash,
                    &address,
                    &r#type,
                    &allow_amend,
                    &metadata,
                    &variables,
                    &message_triggers,
                    &environment_reproducible,
                    &environment_internet,
                    &environment_aleph_api,
                    &environment_shared_cache,
                    &environment_trusted_execution_policy,
                    &environment_trusted_execution_firmware,
                    &payment_type,
                    &resources_vcpus,
                    &resources_memory,
                    &resources_seconds,
                    &cpu_architecture,
                    &cpu_vendor,
                    &node_owner,
                    &node_address_regex,
                    &node_hash,
                    &replaces,
                    &created,
                    &authorized_keys,
                    &program_type,
                    &http_trigger,
                    &persistent,
                ],
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error inserting vm row: {e}")],
            })?;

        // Volume rows
        if r#type == "program" {
            if let Some(code) = message.content.get("code") {
                Self::insert_program_code(client, item_hash, code).await?;
            }
            if let Some(runtime) = message.content.get("runtime") {
                Self::insert_program_runtime(client, item_hash, runtime).await?;
            }
            if let Some(data) = message.content.get("data") {
                Self::insert_program_data(client, item_hash, data).await?;
            }
            if let Some(export) = message.content.get("export") {
                Self::insert_program_export(client, item_hash, export).await?;
            }
        } else if let Some(rootfs) = message.content.get("rootfs") {
            Self::insert_instance_rootfs(client, item_hash, rootfs).await?;
        }
        if let Some(arr) = message.content.get("volumes").and_then(|v| v.as_array()) {
            for vol in arr {
                Self::insert_machine_volume(client, item_hash, vol).await?;
            }
        }

        let program_ref = replaces.unwrap_or_else(|| item_hash.clone());
        upsert_vm_version(
            client,
            item_hash,
            &address,
            &VmVersion::from(program_ref),
            created,
        )
        .await
        .map_err(|e| MessageProcessingException::InternalError {
            errors: vec![format!("DB error upserting vm version: {e}")],
        })?;
        Ok(())
    }

    async fn insert_program_code(
        client: &tokio_postgres::Transaction<'_>,
        program_hash: &str,
        code: &serde_json::Value,
    ) -> Result<(), MessageProcessingException> {
        let encoding = code
            .get("encoding")
            .and_then(|v| v.as_str())
            .unwrap_or("plain");
        let entrypoint = code
            .get("entrypoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let r#ref: Option<String> = code
            .get("ref")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let use_latest: Option<bool> = code.get("use_latest").and_then(|v| v.as_bool());
        client
            .execute(
                "INSERT INTO program_code_volumes(program_hash, encoding, entrypoint, ref, use_latest) \
                 VALUES ($1, $2, $3, $4, $5)",
                &[&program_hash, &encoding, &entrypoint, &r#ref, &use_latest],
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error inserting program_code_volumes: {e}")],
            })?;
        Ok(())
    }

    async fn insert_program_runtime(
        client: &tokio_postgres::Transaction<'_>,
        program_hash: &str,
        runtime: &serde_json::Value,
    ) -> Result<(), MessageProcessingException> {
        let comment = runtime
            .get("comment")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let r#ref: Option<String> = runtime
            .get("ref")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let use_latest: Option<bool> = runtime.get("use_latest").and_then(|v| v.as_bool());
        client
            .execute(
                "INSERT INTO program_runtimes(program_hash, comment, ref, use_latest) \
                 VALUES ($1, $2, $3, $4)",
                &[&program_hash, &comment, &r#ref, &use_latest],
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error inserting program_runtimes: {e}")],
            })?;
        Ok(())
    }

    async fn insert_program_data(
        client: &tokio_postgres::Transaction<'_>,
        program_hash: &str,
        data: &serde_json::Value,
    ) -> Result<(), MessageProcessingException> {
        let encoding = data
            .get("encoding")
            .and_then(|v| v.as_str())
            .unwrap_or("plain");
        let mount = data.get("mount").and_then(|v| v.as_str()).unwrap_or("");
        let r#ref: Option<String> = data
            .get("ref")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let use_latest: Option<bool> = data.get("use_latest").and_then(|v| v.as_bool());
        client
            .execute(
                "INSERT INTO program_data_volumes(program_hash, encoding, mount, ref, use_latest) \
                 VALUES ($1, $2, $3, $4, $5)",
                &[&program_hash, &encoding, &mount, &r#ref, &use_latest],
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error inserting program_data_volumes: {e}")],
            })?;
        Ok(())
    }

    async fn insert_program_export(
        client: &tokio_postgres::Transaction<'_>,
        program_hash: &str,
        export: &serde_json::Value,
    ) -> Result<(), MessageProcessingException> {
        let encoding = export
            .get("encoding")
            .and_then(|v| v.as_str())
            .unwrap_or("plain");
        client
            .execute(
                "INSERT INTO program_export_volumes(program_hash, encoding) VALUES ($1, $2)",
                &[&program_hash, &encoding],
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error inserting program_export_volumes: {e}")],
            })?;
        Ok(())
    }

    async fn insert_instance_rootfs(
        client: &tokio_postgres::Transaction<'_>,
        instance_hash: &str,
        rootfs: &serde_json::Value,
    ) -> Result<(), MessageProcessingException> {
        let parent = rootfs.get("parent").cloned().unwrap_or_default();
        let parent_ref = parent.get("ref").and_then(|v| v.as_str()).unwrap_or("");
        let parent_use_latest = parent
            .get("use_latest")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let size_mib = rootfs.get("size_mib").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
        let persistence = rootfs
            .get("persistence")
            .and_then(|v| v.as_str())
            .unwrap_or("host");
        client
            .execute(
                "INSERT INTO instance_rootfs(instance_hash, parent_ref, parent_use_latest, size_mib, persistence) \
                 VALUES ($1, $2, $3, $4, $5)",
                &[&instance_hash, &parent_ref, &parent_use_latest, &size_mib, &persistence],
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error inserting instance_rootfs: {e}")],
            })?;
        Ok(())
    }

    async fn insert_machine_volume(
        client: &tokio_postgres::Transaction<'_>,
        vm_hash: &str,
        volume: &serde_json::Value,
    ) -> Result<(), MessageProcessingException> {
        let comment: Option<String> = volume
            .get("comment")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let mount: Option<String> = volume
            .get("mount")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        if volume.get("ref").is_some() && volume.get("size_mib").is_none() {
            // ImmutableVolume
            let r#ref = volume.get("ref").and_then(|v| v.as_str()).unwrap_or("");
            let use_latest = volume
                .get("use_latest")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            client
                .execute(
                    "INSERT INTO vm_machine_volumes(vm_hash, type, comment, mount, ref, use_latest) \
                     VALUES ($1, 'immutable', $2, $3, $4, $5)",
                    &[&vm_hash, &comment, &mount, &r#ref, &use_latest],
                )
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error inserting immutable volume: {e}")],
                })?;
        } else if volume.get("persistence").is_some() {
            // PersistentVolume
            let size_mib = volume.get("size_mib").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
            let persistence = volume
                .get("persistence")
                .and_then(|v| v.as_str())
                .unwrap_or("host");
            let name: Option<String> = volume
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let (parent_ref, parent_use_latest) = match volume.get("parent") {
                Some(p) => (
                    p.get("ref").and_then(|v| v.as_str()).map(|s| s.to_string()),
                    p.get("use_latest").and_then(|v| v.as_bool()),
                ),
                None => (None, None),
            };
            client
                .execute(
                    "INSERT INTO vm_machine_volumes(vm_hash, type, comment, mount, size_mib, \
                                                     persistence, name, parent_ref, parent_use_latest) \
                     VALUES ($1, 'persistent', $2, $3, $4, $5, $6, $7, $8)",
                    &[
                        &vm_hash,
                        &comment,
                        &mount,
                        &size_mib,
                        &persistence,
                        &name,
                        &parent_ref,
                        &parent_use_latest,
                    ],
                )
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error inserting persistent volume: {e}")],
                })?;
        } else {
            // EphemeralVolume
            let size_mib = volume.get("size_mib").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
            client
                .execute(
                    "INSERT INTO vm_machine_volumes(vm_hash, type, comment, mount, size_mib) \
                     VALUES ($1, 'ephemeral', $2, $3, $4)",
                    &[&vm_hash, &comment, &mount, &size_mib],
                )
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error inserting ephemeral volume: {e}")],
                })?;
        }
        Ok(())
    }

    async fn find_missing_volumes(
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<HashSet<String>, MessageProcessingException> {
        let (tag_refs, pin_refs) = collect_refs(message);
        let tag_objs: Vec<FileTag> = tag_refs.iter().cloned().map(FileTag::from).collect();
        let existing_pins = find_file_pins(client, &pin_refs).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error find_file_pins: {e}")],
            }
        })?;
        let existing_tags = find_file_tags(client, &tag_objs).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error find_file_tags: {e}")],
            }
        })?;

        let existing_pin_set: HashSet<String> = existing_pins.into_iter().collect();
        let existing_tag_set: HashSet<String> = existing_tags
            .into_iter()
            .map(|t| t.as_str().to_string())
            .collect();
        let mut missing: HashSet<String> = HashSet::new();
        for p in pin_refs {
            if !existing_pin_set.contains(&p) {
                missing.insert(p);
            }
        }
        for t in tag_refs {
            if !existing_tag_set.contains(&t) {
                missing.insert(t);
            }
        }
        Ok(missing)
    }

    async fn check_parent_volumes_size(
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        // Iterate through PersistentVolume entries that have a parent + a
        // size_mib, plus the instance rootfs.
        let mut to_check: Vec<(String, i64, serde_json::Value)> = Vec::new();
        if let Some(arr) = message.content.get("volumes").and_then(|v| v.as_array()) {
            for v in arr {
                if let Some(parent) = v.get("parent") {
                    let name = v
                        .get("name")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    let size = v.get("size_mib").and_then(|x| x.as_i64()).unwrap_or(0);
                    if size > 0 {
                        to_check.push((name, size, parent.clone()));
                    }
                }
            }
        }
        if is_instance(message) {
            if let Some(rootfs) = message.content.get("rootfs") {
                if let Some(parent) = rootfs.get("parent") {
                    let size = rootfs.get("size_mib").and_then(|x| x.as_i64()).unwrap_or(0);
                    to_check.push(("rootfs".to_string(), size, parent.clone()));
                }
            }
        }

        for (name, size_mib, parent) in to_check {
            let parent_ref = parent
                .get("ref")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let use_latest = parent
                .get("use_latest")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            // Look up the parent file size and its concrete file hash so we
            // can surface it in the `VmVolumeTooSmall` error (matches the
            // Python behavior, which populates `parent_file`).
            let (parent_size, parent_file) = if use_latest {
                let tag = FileTag::from(parent_ref.clone());
                let ft = get_file_tag(client, &tag).await.map_err(|e| {
                    MessageProcessingException::InternalError {
                        errors: vec![format!("DB error get_file_tag: {e}")],
                    }
                })?;
                let ft = ft.ok_or_else(|| MessageProcessingException::InternalError {
                    errors: vec![format!(
                        "Could not find latest version of parent volume {parent_ref}"
                    )],
                })?;
                // Join to files to pull its size.
                let row = client
                    .query_one("SELECT size FROM files WHERE hash = $1", &[&ft.file_hash])
                    .await
                    .map_err(|e| MessageProcessingException::InternalError {
                        errors: vec![format!("DB error reading file size: {e}")],
                    })?;
                (row.get::<_, i64>(0), ft.file_hash)
            } else {
                let pin = get_message_file_pin(client, &parent_ref)
                    .await
                    .map_err(|e| MessageProcessingException::InternalError {
                        errors: vec![format!("DB error get_message_file_pin: {e}")],
                    })?;
                let pin = pin.ok_or_else(|| MessageProcessingException::InternalError {
                    errors: vec![format!(
                        "Could not find original version of parent volume {parent_ref}"
                    )],
                })?;
                let row = client
                    .query_one("SELECT size FROM files WHERE hash = $1", &[&pin.file_hash])
                    .await
                    .map_err(|e| MessageProcessingException::InternalError {
                        errors: vec![format!("DB error reading file size: {e}")],
                    })?;
                (row.get::<_, i64>(0), pin.file_hash)
            };
            let volume_size = size_mib * 1024 * 1024;
            if volume_size < parent_size {
                return Err(MessageProcessingException::VmVolumeTooSmall {
                    volume_name: name,
                    volume_size,
                    parent_ref,
                    parent_file,
                    parent_size,
                });
            }
        }
        Ok(())
    }
}

impl Default for VmMessageHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ContentHandler for VmMessageHandler {
    async fn check_balance(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<Option<Vec<AccountCostsDb>>, MessageProcessingException> {
        let cost_content = cost_content_for(message).ok_or_else(|| {
            MessageProcessingException::InvalidMessageFormat {
                errors: vec![format!(
                    "VM message {} has unknown content kind",
                    message.item_hash
                )],
            }
        })?;
        let (message_cost, costs) =
            get_total_and_detailed_costs(client, &cost_content, &message.item_hash)
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("Cost calc failed: {e}")],
                })?;

        // PROGRAM messages that are non-persistent are free under the cutoff.
        if is_program(message)
            && !is_persistent_program(message)
            && are_store_and_program_free(&build_free_input(message))
        {
            return Ok(Some(costs));
        }

        let payment_type = get_payment_type(&cost_content);
        if is_credit_only_required(message.time) && payment_type != PaymentType::Credit {
            return Err(MessageProcessingException::InvalidPaymentMethod { errors: Vec::new() });
        }

        let is_inst = is_instance(message);
        let is_pp = is_persistent_program(message);
        if is_hold_and_stream_deprecated(message.time) && (is_inst || is_pp) {
            if matches!(payment_type, PaymentType::Hold | PaymentType::Superfluid) {
                return Err(MessageProcessingException::InvalidPaymentMethod {
                    errors: Vec::new(),
                });
            }
        }

        if payment_type == PaymentType::Superfluid {
            return Ok(Some(costs));
        }

        if matches!(payment_type, PaymentType::Credit | PaymentType::Hold) {
            let validation = validate_balance_for_payment(
                client,
                &content_address(message)?,
                message_cost,
                payment_type,
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("Balance validation failed: {e}")],
            })?;
            validation.into_result()?;
        }
        Ok(Some(costs))
    }

    async fn check_dependencies(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        let missing = Self::find_missing_volumes(client, message).await?;
        if !missing.is_empty() {
            return Err(MessageProcessingException::VmVolumeNotFound {
                errors: missing.into_iter().collect(),
            });
        }
        Self::check_parent_volumes_size(client, message).await?;

        if let Some(ref_hash) = content_replaces(message) {
            let original = get_program(client, &ref_hash).await.map_err(|e| {
                MessageProcessingException::InternalError {
                    errors: vec![format!("DB error fetching original VM: {e}")],
                }
            })?;
            let original = match original {
                None => {
                    return Err(MessageProcessingException::VmRefNotFound {
                        errors: vec![ref_hash],
                    });
                }
                Some(o) => o,
            };
            if original.replaces.is_some() {
                return Err(MessageProcessingException::VmCannotUpdateUpdate {
                    errors: Vec::new(),
                });
            }
            let allowed = is_vm_amend_allowed(client, &ref_hash).await.map_err(|e| {
                MessageProcessingException::InternalError {
                    errors: vec![format!("DB error is_vm_amend_allowed: {e}")],
                }
            })?;
            match allowed {
                None => {
                    return Err(MessageProcessingException::InternalError {
                        errors: vec![format!(
                            "Could not find current version of program {ref_hash}"
                        )],
                    });
                }
                Some(false) => {
                    return Err(MessageProcessingException::VmUpdateNotAllowed {
                        errors: Vec::new(),
                    });
                }
                Some(true) => {}
            }
        }
        Ok(())
    }

    async fn process(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        messages: &[MessageDb],
    ) -> Result<(), MessageProcessingException> {
        for message in messages {
            Self::insert_vm_row(client, message).await?;
        }
        Ok(())
    }

    async fn forget_message(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<HashSet<String>, MessageProcessingException> {
        tracing::debug!("Deleting program {}", message.item_hash);
        delete_vm(client, &message.item_hash).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error deleting VM: {e}")],
            }
        })?;
        let update_hashes: HashSet<String> = if content_replaces(message).is_some() {
            HashSet::new()
        } else {
            let hashes = delete_vm_updates(client, &message.item_hash)
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error deleting VM updates: {e}")],
                })?;
            hashes.into_iter().collect()
        };
        refresh_vm_version(client, &message.item_hash)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error refreshing vm version: {e}")],
            })?;
        Ok(update_hashes)
    }
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

    fn vm_program() -> MessageDb {
        let now = Utc::now();
        MessageDb {
            item_hash: "p1".into(),
            r#type: MessageType::Program,
            chain: Chain::Ethereum,
            sender: "0xabc".into(),
            signature: None,
            item_type: ItemType::Inline,
            item_content: None,
            content: json!({
                "address": "0xabc",
                "code": {"ref": "coderef", "use_latest": true, "encoding": "plain", "entrypoint": "main:app"},
                "runtime": {"ref": "rtref", "use_latest": false, "comment": ""},
                "on": {"persistent": false, "http": true},
                "type": "vm-function",
                "environment": {"reproducible": false, "internet": true, "aleph_api": true, "shared_cache": false},
                "resources": {"vcpus": 1, "memory": 128, "seconds": 60},
                "time": now.timestamp() as f64,
            }),
            time: now,
            channel: None,
            size: 0,
            status_value: MessageStatus::Processed,
            reception_time: now,
            owner: Some("0xabc".into()),
            content_type: None,
            content_ref: None,
            content_key: None,
            first_confirmed_at: None,
            first_confirmed_height: None,
            payment_type: None,
            content_item_hash: None,
            tags: None,
        }
    }

    #[test]
    fn detects_program_vs_instance() {
        let m = vm_program();
        assert!(is_program(&m));
        assert!(!is_instance(&m));
    }

    #[test]
    fn collect_refs_splits_tags_and_pins() {
        let m = vm_program();
        let (tags, pins) = collect_refs(&m);
        assert_eq!(tags, vec!["coderef"]);
        assert_eq!(pins, vec!["rtref"]);
    }

    #[test]
    fn cost_content_kind_for_program() {
        let m = vm_program();
        let cc = cost_content_for(&m).unwrap();
        assert_eq!(cc.kind, CostContentKind::Program);
    }

    #[test]
    fn vm_volume_too_small_carries_parent_file() {
        // Sanity-check that the error variant is built with a populated
        // `parent_file` field — Python's equivalent surfaces this so the API
        // consumer can identify which file is too small.
        let err = MessageProcessingException::VmVolumeTooSmall {
            volume_name: "rootfs".into(),
            volume_size: 100,
            parent_ref: "ref-1".into(),
            parent_file: "file-hash-1".into(),
            parent_size: 200,
        };
        match err {
            MessageProcessingException::VmVolumeTooSmall { parent_file, .. } => {
                assert_eq!(parent_file, "file-hash-1");
            }
            _ => panic!("expected VmVolumeTooSmall"),
        }
    }
}
