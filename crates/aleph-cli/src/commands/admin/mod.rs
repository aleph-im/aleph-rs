use anyhow::Result;
use aleph_sdk::client::AlephClient;
use url::Url;

use crate::cli::AdminCommand;

pub async fn handle_admin_command(
    _aleph_client: &AlephClient,
    _ccn_url: &Url,
    _json: bool,
    _command: AdminCommand,
) -> Result<()> {
    anyhow::bail!("admin: handler not yet implemented (Task 11 wires this up)")
}
