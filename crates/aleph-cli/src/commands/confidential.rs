//! `aleph instance confidential` command tree. Handlers land in Tasks 14-17;
//! this file lays down the dispatch scaffolding so the parser tests can land.

use crate::cli::ConfidentialCommand;
use anyhow::{Result, anyhow};
use url::Url;

pub async fn dispatch(_scheduler_url: Url, _json: bool, cmd: ConfidentialCommand) -> Result<()> {
    match cmd {
        ConfidentialCommand::InitSession(_) => Err(anyhow!(
            "`aleph instance confidential init-session` is not yet implemented"
        )),
        ConfidentialCommand::Start(_) => Err(anyhow!(
            "`aleph instance confidential start` is not yet implemented"
        )),
        ConfidentialCommand::Create(_) => Err(anyhow!(
            "`aleph instance confidential create` is not yet implemented"
        )),
    }
}
