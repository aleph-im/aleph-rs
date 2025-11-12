use crate::cli::{Cli, GetMessageArgs, MessageCommand};
use aleph_sdk::client::AlephClient;
use clap::Parser;
use url::Url;

mod cli;

async fn handle_message_command(
    aleph_client: &AlephClient,
    command: MessageCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        MessageCommand::Get(GetMessageArgs { item_hash }) => {
            let message = aleph_client.get_message(item_hash).await?;
            let serialized_message = serde_json::to_string_pretty(&message)?;
            println!("{}", serialized_message);
        }
        MessageCommand::List(message_filter) => {
            let messages = aleph_client.get_messages(&(*message_filter).into()).await?;
            let serialized_messages = serde_json::to_string_pretty(&messages)?;
            println!("{}", serialized_messages);
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let ccn_url =
        Url::parse("https://api3.aleph.im").unwrap_or_else(|e| panic!("invalid CCN url: {e}"));
    let aleph_client = AlephClient::new(ccn_url);

    match cli.command {
        cli::Commands::Message {
            command: message_command,
        } => handle_message_command(&aleph_client, message_command).await?,
    }

    Ok(())
}
