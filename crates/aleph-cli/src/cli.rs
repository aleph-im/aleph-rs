use aleph_sdk::aggregate_models::corechannel::NodeHash;
use aleph_types::item_hash::ItemHash;
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// Parse a human-readable size string (e.g. "20GB", "1024MB", "1TiB") into MiB.
/// Bare numbers without units are rejected.
pub fn parse_size_to_mib(s: &str) -> Result<u64, String> {
    let s = s.trim();
    // Find where the numeric part ends and the unit begins
    let unit_start = s
        .find(|c: char| c.is_alphabetic())
        .ok_or_else(|| format!("missing unit in size '{s}' (use e.g. 20GB, 1024MB, 1TiB)"))?;
    let (num_str, unit) = s.split_at(unit_start);
    let value: f64 = num_str
        .trim()
        .parse()
        .map_err(|_| format!("invalid number in size '{s}'"))?;
    if value < 0.0 {
        return Err(format!("size cannot be negative: '{s}'"));
    }

    // Convert to MiB
    let mib = match unit.to_lowercase().as_str() {
        // Binary units (1024-based)
        "mib" => value,
        "gib" => value * 1024.0,
        "tib" => value * 1024.0 * 1024.0,
        // Decimal units (1000-based), converted to MiB
        "mb" => value * 1_000_000.0 / 1_048_576.0,
        "gb" => value * 1_000_000_000.0 / 1_048_576.0,
        "tb" => value * 1_000_000_000_000.0 / 1_048_576.0,
        _ => return Err(format!("unknown size unit '{unit}' (use MB, GB, TB, MiB, GiB, TiB)")),
    };

    let mib_rounded = mib.round() as u64;
    if mib_rounded == 0 {
        return Err(format!("size too small: '{s}' rounds to 0 MiB"));
    }
    Ok(mib_rounded)
}

#[derive(Parser)]
#[command(name = "aleph", version, about = "Aleph CLI")]
pub struct Cli {
    /// CCN endpoint URL.
    #[arg(long, default_value = "https://api3.aleph.im")]
    pub ccn_url: String,

    /// Output results as JSON (for scripting/tooling).
    #[arg(long, global = true)]
    pub json: bool,

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
    /// Work with posts (merged view of POST messages)
    Post {
        #[clap(subcommand)]
        command: PostCommand,
    },
    /// Work with aggregates
    Aggregate {
        #[clap(subcommand)]
        command: AggregateCommand,
    },
    /// Work with network nodes (CCN/CRN operations)
    Node {
        #[clap(subcommand)]
        command: NodeCommand,
    },
    /// Work with files (upload, download)
    File {
        #[clap(subcommand)]
        command: FileCommand,
    },
    /// Work with instances (VM deployments)
    Instance {
        #[clap(subcommand)]
        command: InstanceCommand,
    },
}

#[derive(Subcommand)]
pub enum MessageCommand {
    /// Get a message by its item hash
    Get(GetMessageArgs),
    // Boxing because of a large enum variant.
    /// List messages (with filters).
    List(Box<MessageFilterCli>),
    /// Sync messages from one node to another.
    Sync(Box<SyncArgs>),
    /// Forget messages by their item hashes.
    Forget(ForgetArgs),
}

#[derive(Args)]
pub struct SyncArgs {
    /// URL of the source node (messages are fetched from here).
    #[arg(long)]
    pub source: String,

    /// URL of the target node (missing messages are POSTed here).
    #[arg(long)]
    pub target: String,

    /// Maximum number of messages to fetch from each node.
    #[arg(long, default_value = "200")]
    pub count: u32,

    /// Show what would be synced without actually POSTing.
    #[arg(long)]
    pub dry_run: bool,

    /// Message filters (same as `message list`).
    #[command(flatten)]
    pub filter: MessageFilterCli,
}

#[derive(Args)]
pub struct GetMessageArgs {
    /// The item hash of the message to fetch.
    pub item_hash: ItemHash,
}

#[derive(Subcommand)]
pub enum PostCommand {
    /// List posts (with filters).
    List(Box<PostListArgs>),
    /// Create a new post message.
    Create(PostCreateArgs),
    /// Amend an existing post message.
    Amend(PostAmendArgs),
}

#[derive(Args)]
pub struct PostListArgs {
    /// API version to use (0 for legacy format with full message envelope, 1 for lean format).
    #[arg(long, default_value = "1")]
    pub api_version: u8,

    #[command(flatten)]
    pub filter: PostFilterCli,
}

use aleph_sdk::client::{MessageFilter, PostFilter, SortBy, SortOrder};
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

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum ChainCli {
    // EVM chains
    Arb,
    Aurora,
    Avax,
    Base,
    Blast,
    Bob,
    Bsc,
    Cyber,
    Eth,
    Etherlink,
    Frax,
    Hype,
    Ink,
    Lens,
    Linea,
    Lisk,
    Metis,
    Mode,
    Op,
    Pol,
    Stt,
    Sonic,
    Unichain,
    Wld,
    Zora,
    // SVM chains
    Sol,
    Es,
}

impl From<ChainCli> for aleph_types::chain::Chain {
    fn from(v: ChainCli) -> Self {
        use aleph_types::chain::Chain;
        match v {
            ChainCli::Arb => Chain::Arbitrum,
            ChainCli::Aurora => Chain::Aurora,
            ChainCli::Avax => Chain::Avax,
            ChainCli::Base => Chain::Base,
            ChainCli::Blast => Chain::Blast,
            ChainCli::Bob => Chain::Bob,
            ChainCli::Bsc => Chain::Bsc,
            ChainCli::Cyber => Chain::Cyber,
            ChainCli::Eth => Chain::Ethereum,
            ChainCli::Etherlink => Chain::Etherlink,
            ChainCli::Frax => Chain::Fraxtal,
            ChainCli::Hype => Chain::Hype,
            ChainCli::Ink => Chain::Ink,
            ChainCli::Lens => Chain::Lens,
            ChainCli::Linea => Chain::Linea,
            ChainCli::Lisk => Chain::Lisk,
            ChainCli::Metis => Chain::Metis,
            ChainCli::Mode => Chain::Mode,
            ChainCli::Op => Chain::Optimism,
            ChainCli::Pol => Chain::Pol,
            ChainCli::Stt => Chain::Somnia,
            ChainCli::Sonic => Chain::Sonic,
            ChainCli::Unichain => Chain::Unichain,
            ChainCli::Wld => Chain::Worldchain,
            ChainCli::Zora => Chain::Zora,
            ChainCli::Sol => Chain::Sol,
            ChainCli::Es => Chain::Eclipse,
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct SigningArgs {
    /// Hex-encoded private key. Falls back to ALEPH_PRIVATE_KEY env var.
    #[arg(long)]
    pub private_key: Option<String>,

    /// Signing chain.
    #[arg(long, value_enum, default_value = "eth")]
    pub chain: ChainCli,

    /// Build and sign the message but don't submit it.
    #[arg(long)]
    pub dry_run: bool,
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

// ---------- CLI filter for posts ----------
#[derive(Debug, Clone, Args)]
pub struct PostFilterCli {
    /// Filter by sender address(es). CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub addresses: Option<Vec<String>>,

    /// Filter by item hash(es). CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub hashes: Option<Vec<String>>,

    /// Filter by reference hash(es). CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub refs: Option<Vec<String>>,

    /// Filter by post type(s) (e.g., "corechan-operation"). CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub post_types: Option<Vec<String>>,

    /// Filter by tag(s). CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub tags: Option<Vec<String>>,

    /// Filter by channel(s). CSV or repeat the flag.
    #[arg(long, value_delimiter = ',')]
    pub channels: Option<Vec<String>>,

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

    #[arg(long, default_value = "200")]
    pub pagination: u32,
    #[arg(long, default_value = "1")]
    pub page: u32,
}

impl From<PostFilterCli> for PostFilter {
    fn from(c: PostFilterCli) -> Self {
        PostFilter {
            addresses: c.addresses.map(|v| v.into_iter().map(Into::into).collect()),
            hashes: c
                .hashes
                .map(|v| v.into_iter().map(|s| s.parse().unwrap()).collect()),
            refs: c.refs,
            post_types: c.post_types,
            tags: c.tags,
            channels: c.channels,
            start_date: c.start_date,
            end_date: c.end_date,
            sort_by: c.sort_by.map(Into::into),
            sort_order: c.sort_order.map(Into::into),
            pagination: Some(c.pagination),
            page: Some(c.page),
        }
    }
}

#[derive(Args)]
pub struct PostCreateArgs {
    /// Post type (e.g. "chat", "note").
    #[arg(long = "type")]
    pub post_type: String,

    /// JSON content. If absent, reads from stdin.
    #[arg(long)]
    pub content: Option<String>,

    /// Channel name.
    #[arg(long)]
    pub channel: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct PostAmendArgs {
    /// Item hash of the post to amend.
    #[arg(long = "ref")]
    pub reference: ItemHash,

    /// JSON content. If absent, reads from stdin.
    #[arg(long)]
    pub content: Option<String>,

    /// Channel name.
    #[arg(long)]
    pub channel: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Subcommand)]
pub enum AggregateCommand {
    /// Create a new aggregate message.
    Create(AggregateCreateArgs),
}

#[derive(Args)]
pub struct AggregateCreateArgs {
    /// Aggregate key.
    #[arg(long)]
    pub key: String,

    /// JSON object content. If absent, reads from stdin.
    #[arg(long)]
    pub content: Option<String>,

    /// Channel name.
    #[arg(long)]
    pub channel: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct ForgetArgs {
    /// Item hashes to forget.
    pub hashes: Vec<ItemHash>,

    /// Aggregate hashes to forget.
    #[arg(long, value_delimiter = ',')]
    pub aggregates: Option<Vec<ItemHash>>,

    /// Reason for forgetting.
    #[arg(long)]
    pub reason: Option<String>,

    /// Channel name.
    #[arg(long)]
    pub channel: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Subcommand)]
pub enum NodeCommand {
    /// Register a new Core Channel Node (CCN).
    CreateCcn(CreateCcnArgs),
    /// Register a new Compute Resource Node (CRN).
    CreateCrn(CreateCrnArgs),
    /// Link a CRN to one of your CCNs.
    Link(LinkCrnArgs),
    /// Unlink a CRN from your CCN.
    Unlink(UnlinkCrnArgs),
    /// Stake ALEPH tokens on a node.
    Stake(StakeArgs),
    /// Remove your stake from a node.
    Unstake(UnstakeArgs),
    /// Remove a node from the network.
    Drop(DropNodeArgs),
}

#[derive(Args)]
pub struct CreateCcnArgs {
    /// Human-readable node name.
    #[arg(long)]
    pub name: String,

    /// libp2p multiaddress (e.g. /ip4/1.2.3.4/tcp/4025/p2p/Qm...).
    #[arg(long)]
    pub multiaddress: String,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct CreateCrnArgs {
    /// Human-readable node name.
    #[arg(long)]
    pub name: String,

    /// HTTPS URL of the CRN API endpoint.
    #[arg(long)]
    pub address: String,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct LinkCrnArgs {
    /// Hash of the CRN to link.
    #[arg(long)]
    pub crn: NodeHash,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct UnlinkCrnArgs {
    /// Hash of the CRN to unlink.
    #[arg(long)]
    pub crn: NodeHash,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct StakeArgs {
    /// Hash of the node to stake on.
    #[arg(long)]
    pub node: NodeHash,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct UnstakeArgs {
    /// Hash of the node to unstake from.
    #[arg(long)]
    pub node: NodeHash,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct DropNodeArgs {
    /// Hash of the node to remove.
    #[arg(long)]
    pub node: NodeHash,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Subcommand)]
pub enum FileCommand {
    /// Upload a file to Aleph Cloud and create a STORE message.
    Upload(FileUploadArgs),
    /// Download a file from Aleph Cloud by file hash, message hash, or ref.
    Download(FileDownloadArgs),
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum StorageEngineCli {
    /// Aleph native storage (default, recommended for files up to 100 MB).
    Storage,
    /// IPFS storage.
    Ipfs,
}

#[derive(Args)]
pub struct FileUploadArgs {
    /// Path of the file to upload.
    pub path: std::path::PathBuf,

    /// Storage engine to use.
    #[arg(long, value_enum, default_value = "storage")]
    pub storage_engine: StorageEngineCli,

    /// Channel name.
    #[arg(long)]
    pub channel: Option<String>,

    /// User-defined file reference (for updates/versioning).
    #[arg(long = "ref")]
    pub reference: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct FileDownloadArgs {
    /// File hash to download (direct access).
    #[arg(conflicts_with_all = ["message_hash", "reference"])]
    pub hash: Option<ItemHash>,

    /// Download by STORE message hash (resolves file hash from message metadata).
    #[arg(long, conflicts_with_all = ["hash", "reference"])]
    pub message_hash: Option<ItemHash>,

    /// Download by user-defined file reference. Requires --owner.
    #[arg(long = "ref", conflicts_with_all = ["hash", "message_hash"])]
    pub reference: Option<String>,

    /// Owner address, required when downloading by --ref.
    #[arg(long, requires = "reference")]
    pub owner: Option<String>,

    /// Output file path. Defaults to `./<file_hash>` in the current directory.
    #[arg(short, long)]
    pub output: Option<std::path::PathBuf>,

    /// Write file contents to stdout instead of saving to a file.
    #[arg(long)]
    pub stdout: bool,
}

#[derive(Subcommand)]
pub enum InstanceCommand {
    /// Create a new instance (VM).
    Create(InstanceCreateArgs),
}

#[derive(Args)]
pub struct InstanceCreateArgs {
    /// Root filesystem image hash (STORE message hash).
    #[arg(long)]
    pub rootfs: ItemHash,

    /// Root filesystem size (e.g. 20GB, 1024MB, 1TiB).
    #[arg(long, value_parser = parse_size_to_mib)]
    pub rootfs_size: u64,

    /// Number of virtual CPUs.
    #[arg(long, default_value = "1")]
    pub vcpus: u32,

    /// Memory size (e.g. 2GB, 2048MB, 2GiB).
    #[arg(long, value_parser = parse_size_to_mib, default_value = "2GiB")]
    pub memory: u64,

    /// Path to an SSH public key file.
    #[arg(long)]
    pub ssh_pubkey_file: PathBuf,

    /// Instance name.
    #[arg(long)]
    pub name: Option<String>,

    /// Channel name.
    #[arg(long)]
    pub channel: Option<String>,

    /// Persistent volume: `name=N,mount=PATH,size=SIZE[,persistence=host|store]`.
    /// SIZE uses human-readable format (e.g. 10GB, 500MiB). Can be repeated.
    #[arg(long)]
    pub persistent_volume: Option<Vec<String>>,

    /// Ephemeral volume: `mount=PATH,size=SIZE`.
    /// SIZE uses human-readable format (e.g. 100MB, 500MiB). Can be repeated.
    #[arg(long)]
    pub ephemeral_volume: Option<Vec<String>>,

    /// Immutable volume: `ref=HASH,mount=PATH[,use_latest=BOOL]`.
    /// Can be repeated for multiple volumes.
    #[arg(long)]
    pub immutable_volume: Option<Vec<String>>,

    #[command(flatten)]
    pub signing: SigningArgs,
}
