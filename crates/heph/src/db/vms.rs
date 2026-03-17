use rusqlite::{Connection, OptionalExtension, Result as SqlResult, params};

/// A VM record as stored in (and read back from) the `vms` table.
#[derive(Debug, Clone)]
pub struct VmRecord {
    pub item_hash: String,
    pub vm_type: String,
    pub owner: String,
    pub allow_amend: bool,
    pub replaces: Option<String>,
    pub time: f64,
    pub content: String, // Full JSON
    pub payment_type: Option<String>,
}

/// A volume entry as stored in the `vm_volumes` table.
#[derive(Debug, Clone)]
pub struct VmVolumeRecord {
    pub volume_type: String,
    pub ref_hash: Option<String>,
    pub use_latest: bool,
    pub size_mib: Option<i64>,
    pub mount: Option<String>,
}

/// Insert a VM record into the `vms` table.
pub fn insert_vm(conn: &Connection, record: &VmRecord) -> SqlResult<()> {
    conn.execute(
        "INSERT INTO vms (item_hash, vm_type, owner, allow_amend, replaces, time, content, payment_type)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            record.item_hash,
            record.vm_type,
            record.owner,
            record.allow_amend as i64,
            record.replaces,
            record.time,
            record.content,
            record.payment_type,
        ],
    )?;
    Ok(())
}

/// Retrieve a VM record by its item_hash.
pub fn get_vm(conn: &Connection, item_hash: &str) -> SqlResult<Option<VmRecord>> {
    conn.query_row(
        "SELECT item_hash, vm_type, owner, allow_amend, replaces, time, content, payment_type
         FROM vms WHERE item_hash = ?1",
        params![item_hash],
        |row| {
            Ok(VmRecord {
                item_hash: row.get(0)?,
                vm_type: row.get(1)?,
                owner: row.get(2)?,
                allow_amend: row.get::<_, i64>(3)? != 0,
                replaces: row.get(4)?,
                time: row.get(5)?,
                content: row.get(6)?,
                payment_type: row.get(7)?,
            })
        },
    )
    .optional()
}

/// Insert volume records for a VM into the `vm_volumes` table.
pub fn insert_vm_volumes(
    conn: &Connection,
    vm_hash: &str,
    volumes: &[VmVolumeRecord],
) -> SqlResult<()> {
    for vol in volumes {
        conn.execute(
            "INSERT INTO vm_volumes (vm_hash, volume_type, ref_hash, use_latest, size_mib, mount)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                vm_hash,
                vol.volume_type,
                vol.ref_hash,
                vol.use_latest as i64,
                vol.size_mib,
                vol.mount,
            ],
        )?;
    }
    Ok(())
}

/// Check whether a VM allows amendments.
/// Returns `Some(true/false)` if the VM exists, `None` if not found.
pub fn is_vm_amend_allowed(conn: &Connection, item_hash: &str) -> SqlResult<Option<bool>> {
    conn.query_row(
        "SELECT allow_amend FROM vms WHERE item_hash = ?1",
        params![item_hash],
        |row| row.get::<_, i64>(0).map(|v| v != 0),
    )
    .optional()
}

/// Delete a VM record (program or instance) by item_hash.
pub fn delete_vm(conn: &Connection, item_hash: &str) -> SqlResult<usize> {
    conn.execute("DELETE FROM vms WHERE item_hash = ?1", params![item_hash])
}

/// Delete all vm_volumes entries for a given vm_hash.
pub fn delete_vm_volumes(conn: &Connection, vm_hash: &str) -> SqlResult<usize> {
    conn.execute(
        "DELETE FROM vm_volumes WHERE vm_hash = ?1",
        params![vm_hash],
    )
}

/// Delete account_costs entries for a given item_hash.
pub fn delete_account_costs(conn: &Connection, item_hash: &str) -> SqlResult<usize> {
    conn.execute(
        "DELETE FROM account_costs WHERE item_hash = ?1",
        params![item_hash],
    )
}

/// Count active (non-forgotten) VMs that reference the given store hash as a volume.
pub fn count_vm_dependencies(conn: &Connection, store_hash: &str) -> SqlResult<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM vm_volumes v
         JOIN vms m ON v.vm_hash = m.item_hash
         WHERE v.ref_hash = ?1",
        params![store_hash],
        |row| row.get(0),
    )
}
