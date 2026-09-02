//! `aleph vprogram run`: boot a V-PROGRAM locally in plain QEMU.

use aleph_sdk::client::AlephClient;
use anyhow::{Result, bail};

use crate::cli::VProgramRunArgs;

pub async fn handle_run(
    _aleph_client: &AlephClient,
    _json: bool,
    _args: VProgramRunArgs,
) -> Result<()> {
    bail!("vprogram run is not implemented yet")
}
