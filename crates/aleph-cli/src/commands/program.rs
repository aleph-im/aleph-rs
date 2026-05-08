use crate::cli::{ProgramCommand, ProgramCreateArgs, ProgramDeleteArgs, ProgramListArgs};
use aleph_sdk::client::AlephClient;
use anyhow::{Result, bail};
use url::Url;

pub async fn handle_program_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: ProgramCommand,
) -> Result<()> {
    match command {
        ProgramCommand::Create(args) => handle_create(aleph_client, ccn_url, json, args).await,
        ProgramCommand::List(args) => handle_list(aleph_client, json, args).await,
        ProgramCommand::Delete(args) => handle_delete(aleph_client, ccn_url, json, args).await,
        ProgramCommand::Update(_) => {
            bail!("`aleph program update` lands in PR 2 of the program CLI work")
        }
        ProgramCommand::Persist(_) => {
            bail!("`aleph program persist` lands in PR 2 of the program CLI work")
        }
        ProgramCommand::Unpersist(_) => {
            bail!("`aleph program unpersist` lands in PR 2 of the program CLI work")
        }
        ProgramCommand::Logs(_) => {
            bail!("`aleph program logs` lands in PR 2 of the program CLI work")
        }
    }
}

async fn handle_create(
    _aleph_client: &AlephClient,
    _ccn_url: &Url,
    _json: bool,
    _args: ProgramCreateArgs,
) -> Result<()> {
    bail!("`aleph program create` not yet implemented (PR1 T9)")
}

async fn handle_list(
    _aleph_client: &AlephClient,
    _json: bool,
    _args: ProgramListArgs,
) -> Result<()> {
    bail!("`aleph program list` not yet implemented (PR1 T10)")
}

async fn handle_delete(
    _aleph_client: &AlephClient,
    _ccn_url: &Url,
    _json: bool,
    _args: ProgramDeleteArgs,
) -> Result<()> {
    bail!("`aleph program delete` not yet implemented (PR1 T11)")
}
