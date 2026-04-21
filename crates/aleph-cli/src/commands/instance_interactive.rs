//! Interactive `-i` resolver for `aleph instance create`.
//!
//! Runs before the normal build/submit path and fills in any `InstanceCreateArgs`
//! fields not already provided on the command line. Prompts, in order:
//! image → size → CRN → name → SSH public key path. CRN selection always runs
//! (the instance gets pinned to the chosen CRN via `node_hash`).

use crate::cli::InstanceCreateArgs;
use aleph_sdk::client::AlephClient;

pub async fn resolve_interactive(
    _args: &mut InstanceCreateArgs,
    _aleph_client: &AlephClient,
) -> Result<(), Box<dyn std::error::Error>> {
    // Prompts are implemented in subsequent tasks.
    Err("interactive mode is not yet implemented".into())
}
