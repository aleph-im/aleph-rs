use crate::cli::Cli;
use aleph_sdk::client::AlephClient;
use clap::Parser;
use std::sync::OnceLock;

mod account;
mod cli;
mod commands;
mod common;
mod config;

#[cfg(unix)]
static ORIGINAL_TERMIOS: OnceLock<libc::termios> = OnceLock::new();

// dialoguer's Select puts the tty into raw mode and hides the cursor,
// restoring both via Drop. Ctrl+C skips Drop, so without this handler an
// interrupt during e.g. `aleph account import --ledger` leaves the user's
// shell with raw mode on and the cursor hidden. We snapshot the tty state
// at startup (before any prompt mutates it) and restore that exact mode on
// SIGINT, rather than hardcoding ICANON|ECHO and clobbering custom stty.
fn install_terminal_restore_handler() {
    #[cfg(unix)]
    unsafe {
        let mut t: libc::termios = std::mem::zeroed();
        if libc::tcgetattr(libc::STDIN_FILENO, &mut t) == 0 {
            let _ = ORIGINAL_TERMIOS.set(t);
        }
    }

    let _ = ctrlc::set_handler(|| {
        use std::io::Write;
        let _ = std::io::stderr().write_all(b"\x1b[?25h");
        #[cfg(unix)]
        if let Some(t) = ORIGINAL_TERMIOS.get() {
            unsafe {
                libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, t);
            }
        }
        std::process::exit(130);
    });
}

#[tokio::main]
async fn main() {
    install_terminal_restore_handler();
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
        commands::config::handle_config_command(config_command, json, cli.network.as_deref())
            .await?;
        return Ok(());
    }

    let ccn_url = common::resolve_ccn_url(
        cli.ccn_url.as_deref(),
        cli.ccn.as_deref(),
        cli.network.as_deref(),
    )?;
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
            commands::node::handle_node_command(
                &aleph_client,
                &ccn_url,
                json,
                node_command,
                cli.network.as_deref(),
            )
            .await?
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
        cli::Commands::Credit {
            command: credit_command,
        } => {
            commands::credit::handle_credit_command(
                &aleph_client,
                &ccn_url,
                json,
                credit_command,
                cli.network.as_deref(),
            )
            .await?
        }
    }

    Ok(())
}
