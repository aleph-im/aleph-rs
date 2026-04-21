use crate::cli::Cli;
use aleph_sdk::client::AlephClient;
use clap::Parser;

mod account;
mod cli;
mod commands;
mod common;
mod config;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        // Walk the source chain to find the root cause — avoids
        // redundant "Storage error: File not found: ..." nesting.
        let mut cause: &dyn std::error::Error = e.as_ref();
        while let Some(src) = cause.source() {
            cause = src;
        }
        eprintln!("Error: {cause}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let json = cli.json;

    // Config subcommand doesn't need a CCN URL
    if let cli::Commands::Config {
        command: config_command,
    } = cli.command
    {
        commands::config::handle_config_command(config_command, json).await?;
        return Ok(());
    }

    let ccn_url = common::resolve_ccn_url(cli.ccn_url.as_deref(), cli.ccn.as_deref())?;
    let aleph_client = AlephClient::new(ccn_url.clone());

    match cli.command {
        cli::Commands::Message {
            command: message_command,
        } => {
            commands::message::handle_message_command(
                &aleph_client,
                &ccn_url,
                json,
                message_command,
            )
            .await?
        }
        cli::Commands::Post {
            command: post_command,
        } => {
            commands::post::handle_post_command(&aleph_client, &ccn_url, json, post_command).await?
        }
        cli::Commands::Aggregate {
            command: aggregate_command,
        } => {
            commands::aggregate::handle_aggregate_command(
                &aleph_client,
                &ccn_url,
                json,
                aggregate_command,
            )
            .await?
        }
        cli::Commands::Node {
            command: node_command,
        } => {
            commands::node::handle_node_command(&aleph_client, &ccn_url, json, node_command).await?
        }
        cli::Commands::File {
            command: file_command,
        } => {
            commands::file::handle_file_command(&aleph_client, &ccn_url, json, file_command).await?
        }
        cli::Commands::Instance {
            command: instance_command,
        } => {
            commands::instance::handle_instance_command(
                &aleph_client,
                &ccn_url,
                json,
                instance_command,
            )
            .await?
        }
        cli::Commands::Account {
            command: account_command,
        } => {
            commands::account::handle_account_command(&aleph_client, account_command, json).await?
        }
        cli::Commands::Authorization {
            command: authorization_command,
        } => {
            commands::authorization::handle_authorization_command(
                &aleph_client,
                &ccn_url,
                json,
                authorization_command,
            )
            .await?
        }
        cli::Commands::Config { .. } => unreachable!(),
    }

    Ok(())
}
