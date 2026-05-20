//! VM-related accessors. Mirrors `aleph/db/accessors/vms.py`.

use chrono::{DateTime, Utc};
use tokio_postgres::GenericClient;

use crate::AlephResult;
use crate::db::models::vms::{VmBaseDb, VmVersionDb};
use crate::types::vms::{VmType, VmVersion};

const VM_BASE_COLS: &str = "item_hash, owner, type, allow_amend, metadata, variables, \
    message_triggers, environment_reproducible, environment_internet, \
    environment_aleph_api, environment_shared_cache, \
    environment_trusted_execution_policy, environment_trusted_execution_firmware, \
    payment_type, resources_vcpus, resources_memory, resources_seconds, \
    cpu_architecture, cpu_vendor, node_owner, node_address_regex, node_hash, \
    replaces, created, authorized_keys, program_type, http_trigger, persistent";

fn vm_type_value(t: VmType) -> &'static str {
    match t {
        VmType::Instance => "instance",
        VmType::Program => "program",
    }
}

/// Fetch a VM-instance row, if it exists. Mirrors `get_instance`.
pub async fn get_instance(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Option<VmBaseDb>> {
    let sql = format!(
        "SELECT {cols} FROM vms WHERE item_hash = $1 AND type = $2",
        cols = VM_BASE_COLS
    );
    let row = client
        .query_opt(&sql, &[&item_hash, &vm_type_value(VmType::Instance)])
        .await?;
    row.as_ref().map(VmBaseDb::try_from_row).transpose()
}

/// Fetch a VM-program row, if it exists. Mirrors `get_program`.
pub async fn get_program(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Option<VmBaseDb>> {
    let sql = format!(
        "SELECT {cols} FROM vms WHERE item_hash = $1 AND type = $2",
        cols = VM_BASE_COLS
    );
    let row = client
        .query_opt(&sql, &[&item_hash, &vm_type_value(VmType::Program)])
        .await?;
    row.as_ref().map(VmBaseDb::try_from_row).transpose()
}

/// Whether amending the VM identified by `vm_hash` is allowed. Mirrors
/// `is_vm_amend_allowed`.
pub async fn is_vm_amend_allowed(
    client: &impl GenericClient,
    vm_hash: &str,
) -> AlephResult<Option<bool>> {
    let sql = "SELECT v.allow_amend \
               FROM vm_versions vv \
               JOIN vms v ON vv.current_version = v.item_hash \
               WHERE vv.vm_hash = $1";
    let row = client.query_opt(sql, &[&vm_hash]).await?;
    Ok(row.as_ref().map(|r| r.get::<_, bool>(0)))
}

/// Delete VM rows matching `where_clause` (raw SQL fragment) with `params`.
/// Returns the list of deleted `item_hash`es.
async fn _delete_vm(
    client: &impl GenericClient,
    where_clause: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> AlephResult<Vec<String>> {
    let sql = format!("DELETE FROM vms WHERE {where_clause} RETURNING item_hash");
    let rows = client.query(&sql, params).await?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Delete one VM by item hash.
pub async fn delete_vm(client: &impl GenericClient, vm_hash: &str) -> AlephResult<()> {
    let _ = _delete_vm(client, "item_hash = $1", &[&vm_hash]).await?;
    Ok(())
}

/// Delete every VM that replaces `vm_hash`. Returns the deleted hashes.
pub async fn delete_vm_updates(
    client: &impl GenericClient,
    vm_hash: &str,
) -> AlephResult<Vec<String>> {
    _delete_vm(client, "replaces = $1", &[&vm_hash]).await
}

/// Fetch a vm_versions row.
pub async fn get_vm_version(
    client: &impl GenericClient,
    vm_hash: &str,
) -> AlephResult<Option<VmVersionDb>> {
    let row = client
        .query_opt(
            "SELECT vm_hash, owner, current_version, last_updated FROM vm_versions \
             WHERE vm_hash = $1",
            &[&vm_hash],
        )
        .await?;
    Ok(row.as_ref().map(VmVersionDb::from_row))
}

/// Whether any VM still references a given volume hash. Returns one of the
/// dependent VMs, or `None`.
pub async fn get_vms_dependent_volumes(
    client: &impl GenericClient,
    volume_hash: &str,
) -> AlephResult<Option<VmBaseDb>> {
    let sql = format!(
        "SELECT DISTINCT {cols} FROM vms v \
         LEFT JOIN vm_machine_volumes mv ON mv.vm_hash = v.item_hash AND mv.type = 'immutable' \
         LEFT JOIN program_code_volumes cv ON cv.program_hash = v.item_hash \
         LEFT JOIN program_data_volumes dv ON dv.program_hash = v.item_hash \
         LEFT JOIN program_runtimes rt ON rt.program_hash = v.item_hash \
         LEFT JOIN instance_rootfs rfv ON rfv.instance_hash = v.item_hash \
         WHERE mv.ref = $1 OR cv.ref = $1 OR dv.ref = $1 OR rt.ref = $1 \
            OR rfv.parent_ref = $1 \
         LIMIT 1",
        cols = VM_BASE_COLS
            .split(", ")
            .map(|c| format!("v.{c}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let row = client.query_opt(&sql, &[&volume_hash]).await?;
    row.as_ref().map(VmBaseDb::try_from_row).transpose()
}

/// Upsert one row in `vm_versions`. Mirrors `upsert_vm_version`.
pub async fn upsert_vm_version(
    client: &impl GenericClient,
    vm_hash: &str,
    owner: &str,
    current_version: &VmVersion,
    last_updated: DateTime<Utc>,
) -> AlephResult<()> {
    let sql = "INSERT INTO vm_versions(vm_hash, owner, current_version, last_updated) \
               VALUES ($1, $2, $3, $4) \
               ON CONFLICT ON CONSTRAINT program_versions_pkey \
               DO UPDATE SET current_version = EXCLUDED.current_version, \
                             last_updated = EXCLUDED.last_updated \
               WHERE vm_versions.last_updated < EXCLUDED.last_updated";
    client
        .execute(
            sql,
            &[&vm_hash, &owner, &current_version.as_str(), &last_updated],
        )
        .await?;
    Ok(())
}

/// Recompute the current_version for `vm_hash` from `vms` (latest by `created`).
///
/// Mirrors `refresh_vm_version`.
pub async fn refresh_vm_version(client: &impl GenericClient, vm_hash: &str) -> AlephResult<()> {
    client
        .execute("DELETE FROM vm_versions WHERE vm_hash = $1", &[&vm_hash])
        .await?;
    let sql = "WITH latest AS ( \
                   SELECT COALESCE(replaces, item_hash) AS replaces, \
                          MAX(created) AS created \
                   FROM vms \
                   GROUP BY COALESCE(replaces, item_hash) \
               ) \
               INSERT INTO vm_versions(vm_hash, owner, current_version, last_updated) \
               SELECT COALESCE(v.replaces, v.item_hash), v.owner, v.item_hash, v.created \
               FROM vms v \
               JOIN latest l \
                   ON COALESCE(v.replaces, v.item_hash) = l.replaces \
                  AND v.created = l.created \
               WHERE COALESCE(v.replaces, v.item_hash) = $1 \
               ON CONFLICT ON CONSTRAINT program_versions_pkey \
               DO UPDATE SET current_version = EXCLUDED.current_version, \
                             last_updated = EXCLUDED.last_updated";
    client.execute(sql, &[&vm_hash]).await?;
    Ok(())
}
