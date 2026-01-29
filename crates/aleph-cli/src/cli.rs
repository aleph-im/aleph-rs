use aleph_types::item_hash::ItemHash;
use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "aleph", version, about = "Aleph CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Work with messages
    Message {
        #[clap(subcommand)]
        command: MessageCommand,
    },
}

#[derive(Subcommand)]
pub enum MessageCommand {
    /// Get a message by its item hash
    Get(GetMessageArgs),
    // Boxing because of a large enum variant.
    /// List messages (with filters).
    List(Box<MessageFilterCli>),
}

#[derive(Args)]
pub struct GetMessageArgs {
    /// The item hash of the message to fetch.
    pub item_hash: ItemHash,
}

use aleph_sdk::client::{MessageFilter, SortBy, SortOrder};
use aleph_types::message::{MessageStatus, MessageType};
use aleph_types::timestamp::Timestamp;
use chrono::{DateTime, FixedOffset};
use std::str::FromStr;

fn parse_timestamp(s: &str) -> Result<Timestamp, String> {
    println!("Parsing datetime: {}", s);
    // Try unix seconds (int/float)
    if let Ok(timestamp) = s.parse::<f64>() {
        return Ok(Timestamp::from(timestamp));
    }
    // Fallback: deserialize as RFC3339
    let timestamp = DateTime::<FixedOffset>::from_str(s)
        .map_err(|e| e.to_string())?
        .timestamp();
    Ok(Timestamp::from(timestamp as f64))
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum MessageTypeCli {
    Aggregate,
    Forget,
    Instance,
    Program,
    Post,
    Store,
}

impl From<MessageTypeCli> for MessageType {
    fn from(v: MessageTypeCli) -> Self {
        match v {
            MessageTypeCli::Aggregate => MessageType::Aggregate,
            MessageTypeCli::Forget => MessageType::Forget,
            MessageTypeCli::Instance => MessageType::Instance,
            MessageTypeCli::Post => MessageType::Post,
            MessageTypeCli::Program => MessageType::Program,
            MessageTypeCli::Store => MessageType::Store,
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum MessageStatusCli {
    Pending,
    Processed,
    Removing,
    Removed,
    Forgotten,
}

impl From<MessageStatusCli> for MessageStatus {
    fn from(v: MessageStatusCli) -> Self {
        match v {
            MessageStatusCli::Pending => MessageStatus::Pending,
            MessageStatusCli::Processed => MessageStatus::Processed,
            MessageStatusCli::Removing => MessageStatus::Removing,
            MessageStatusCli::Removed => MessageStatus::Removed,
            MessageStatusCli::Forgotten => MessageStatus::Forgotten,
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SortByCli {
    Time,
    TxTime,
}
impl From<SortByCli> for SortBy {
    fn from(v: SortByCli) -> Self {
        match v {
            SortByCli::Time => SortBy::Time,
            SortByCli::TxTime => SortBy::TxTime,
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SortOrderCli {
    Asc,
    Desc,
}
impl From<SortOrderCli> for SortOrder {
    fn from(v: SortOrderCli) -> Self {
        match v {
            SortOrderCli::Asc => SortOrder::Asc,
            SortOrderCli::Desc => SortOrder::Desc,
        }
    }
}

// ---------- CLI filter (mirror of MessageFilter) ----------
#[derive(Debug, Clone, Args)]
pub struct MessageFilterCli {
    /// Filter by message type
    #[arg(long, value_delimiter = ',', value_enum)]
    pub message_type: Option<MessageTypeCli>,

    /// Filter by message type(s). CSV or repeat the flag.
    #[arg(long, value_delimiter = ',', value_enum)]
    pub message_types: Option<Vec<MessageTypeCli>>,

    /// Filter by content types. CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub content_types: Option<Vec<String>>,

    /// Filter by content keys. CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub content_keys: Option<Vec<String>>,

    /// Filter by content hashes (content.item_hash). CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub content_hashes: Option<Vec<String>>,

    /// Only posts that reference these hashes. CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub refs: Option<Vec<String>>,

    /// Addresses. CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub addresses: Option<Vec<String>>,

    /// Tags. CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub tags: Option<Vec<String>>,

    /// Specific item hashes. CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub hashes: Option<Vec<String>>,

    /// Channels. CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub channels: Option<Vec<String>>,

    /// Sender chains. CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub chains: Option<Vec<String>>,

    /// Earliest date (RFC3339 or unix seconds).
    #[arg(long, value_parser = parse_timestamp)]
    pub start_date: Option<Timestamp>,

    /// Latest date (RFC3339 or unix seconds).
    #[arg(long, value_parser = parse_timestamp)]
    pub end_date: Option<Timestamp>,

    /// Sort key.
    #[arg(long, value_enum)]
    pub sort_by: Option<SortByCli>,

    /// Sort order.
    #[arg(long, value_enum)]
    pub sort_order: Option<SortOrderCli>,

    /// Message statuses. CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub message_statuses: Option<Vec<MessageStatusCli>>,

    #[arg(long, default_value = "200")]
    pub pagination: u32,
    #[arg(long, default_value = "1")]
    pub page: u32,
}

impl From<MessageFilterCli> for MessageFilter {
    fn from(c: MessageFilterCli) -> Self {
        MessageFilter {
            message_type: c.message_type.map(Into::into),
            message_types: c
                .message_types
                .map(|v| v.into_iter().map(Into::into).collect()),
            content_types: c.content_types,
            content_keys: c.content_keys,
            content_hashes: c
                .content_hashes
                .map(|v| v.into_iter().map(|s| s.parse().unwrap()).collect()),
            refs: c.refs,
            addresses: c.addresses.map(|v| v.into_iter().map(Into::into).collect()),
            owners: None,
            tags: c.tags,
            hashes: c
                .hashes
                .map(|v| v.into_iter().map(|s| s.parse().unwrap()).collect()),
            channels: c.channels,
            chains: c.chains,
            start_date: c.start_date,
            end_date: c.end_date,
            sort_by: c.sort_by.map(Into::into),
            sort_order: c.sort_order.map(Into::into),
            message_statuses: c
                .message_statuses
                .map(|v| v.into_iter().map(Into::into).collect()),
            pagination: Some(c.pagination),
            page: Some(c.page),
        }
    }
}
