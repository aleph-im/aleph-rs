//! `aleph instance show` - detail view for a single VM.
//!
//! Default view aggregates the CCN INSTANCE message and scheduler placement.
//! Passing `--verbose` additionally fetches live CRN networking and the
//! owner's port-forwarding aggregate.

use crate::cli::InstanceShowArgs;
use aleph_sdk::client::AlephClient;
use anyhow::{bail, Result};
use url::Url;

pub async fn handle_instance_show(
    _aleph_client: &AlephClient,
    _scheduler_url: Url,
    _json: bool,
    _args: InstanceShowArgs,
) -> Result<()> {
    bail!("not yet implemented")
}
