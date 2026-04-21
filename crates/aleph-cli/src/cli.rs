use aleph_sdk::aggregate_models::corechannel::NodeHash;
use aleph_types::item_hash::ItemHash;
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// Parse a human-readable size string into MiB.
///
/// Supports binary units (MiB, GiB, TiB) and decimal units (MB, GB, TB).
/// Bare numbers without units are rejected.
///
/// ```text
/// parse_size_to_mib("2GiB")  -> Ok(2048)
/// parse_size_to_mib("100MiB") -> Ok(100)
/// parse_size_to_mib("20GB")  -> Ok(19073)
/// parse_size_to_mib("1024")  -> Err (no unit)
/// ```
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
        _ => {
            return Err(format!(
                "unknown size unit '{unit}' (use MB, GB, TB, MiB, GiB, TiB)"
            ));
        }
    };

    let mib_rounded = mib.round() as u64;
    if mib_rounded == 0 {
        return Err(format!("size too small: '{s}' rounds to 0 MiB"));
    }
    Ok(mib_rounded)
}

/// Well-known rootfs image presets.
const IMAGE_PRESETS: &[(&str, &str)] = &[
    (
        "ubuntu22",
        "4a0f62da42f4478544616519e6f5d58adb1096e069b392b151d47c3609492d0c",
    ),
    (
        "ubuntu24",
        "5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e",
    ),
    (
        "debian12",
        "b6ff5c3a8205d1ca4c7c3369300eeafff498b558f71b851aa2114afd0a532717",
    ),
];

/// Parse an image argument: either a preset name or an item hash (native hex or IPFS CID).
pub fn parse_image(s: &str) -> Result<ItemHash, String> {
    for (name, hash) in IMAGE_PRESETS {
        if s.eq_ignore_ascii_case(name) {
            return hash.parse().map_err(|e| format!("{e}"));
        }
    }
    ItemHash::try_from(s).map_err(|_| {
        let preset_names: Vec<&str> = IMAGE_PRESETS.iter().map(|(n, _)| *n).collect();
        format!(
            "'{s}' is not a valid image. Use a preset ({}) or an item hash.",
            preset_names.join(", ")
        )
    })
}

#[derive(Parser)]
#[command(name = "aleph", version, about = "Aleph CLI")]
pub struct Cli {
    /// CCN endpoint URL (overrides --ccn and config default).
    #[arg(long, conflicts_with = "ccn")]
    pub ccn_url: Option<String>,

    /// Named CCN from config (see: aleph config ccn list).
    #[arg(long, conflicts_with = "ccn_url")]
    pub ccn: Option<String>,

    /// Output results as JSON (for scripting/tooling).
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Manage local accounts and signing keys
    Account {
        #[clap(subcommand)]
        command: AccountCommand,
    },
    /// Create aggregate key-value entries
    Aggregate {
        #[clap(subcommand)]
        command: AggregateCommand,
    },
    /// Manage delegated authorizations
    Authorization {
        #[clap(subcommand)]
        command: AuthorizationCommand,
    },
    /// Manage CLI configuration (CCN endpoints, etc.)
    Config {
        #[clap(subcommand)]
        command: ConfigCommand,
    },
    /// Upload and download files
    File {
        #[clap(subcommand)]
        command: FileCommand,
    },
    /// Create and manage VM instances
    Instance {
        #[clap(subcommand)]
        command: InstanceCommand,
    },
    /// Query, sync, and forget raw protocol messages
    Message {
        #[clap(subcommand)]
        command: MessageCommand,
    },
    /// Register, stake, and manage network nodes
    Node {
        #[clap(subcommand)]
        command: NodeCommand,
    },
    /// List, create, and amend posts
    Post {
        #[clap(subcommand)]
        command: PostCommand,
    },
}

#[derive(Subcommand)]
pub enum MessageCommand {
    /// Forget messages by their item hashes
    Forget(ForgetArgs),
    /// Get a message by its item hash
    Get(GetMessageArgs),
    // Boxing because of a large enum variant.
    /// List messages (with filters)
    List(Box<MessageFilterCli>),
    /// Sync messages from one node to another
    Sync(Box<SyncArgs>),
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
    /// Amend an existing post
    Amend(PostAmendArgs),
    /// Create a new post
    Create(PostCreateArgs),
    /// List posts (with filters)
    List(Box<PostListArgs>),
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
    /// Named account from `aleph account list`.
    /// Defaults to the active account set by `aleph account use`.
    #[arg(long)]
    pub account: Option<String>,

    /// Hex-encoded private key. Falls back to ALEPH_PRIVATE_KEY env var.
    /// Overrides --account if both are provided.
    #[arg(long)]
    pub private_key: Option<String>,

    /// Signing chain. Only required with --private-key.
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

    /// Sign on behalf of another address (requires an authorization from that address).
    #[arg(long)]
    pub on_behalf_of: Option<String>,

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

    /// Sign on behalf of another address (requires an authorization from that address).
    #[arg(long)]
    pub on_behalf_of: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Subcommand)]
pub enum AggregateCommand {
    /// Create a new aggregate message
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

    /// Sign on behalf of another address (requires an authorization from that address).
    #[arg(long)]
    pub on_behalf_of: Option<String>,

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

    /// Sign on behalf of another address (requires an authorization from that address).
    #[arg(long)]
    pub on_behalf_of: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum NodeCommand {
    /// Amend metadata fields on an existing node
    Amend(AmendNodeArgs),
    /// Register a new Core Channel Node (CCN)
    CreateCcn(CreateCcnArgs),
    /// Register a new Compute Resource Node (CRN)
    CreateCrn(CreateCrnArgs),
    /// Remove a node from the network
    Drop(DropNodeArgs),
    /// Link a CRN to one of your CCNs
    Link(LinkCrnArgs),
    /// List nodes on the network
    List(NodeListArgs),
    /// Stake ALEPH tokens on a node
    Stake(StakeArgs),
    /// Unlink a CRN from your CCN
    Unlink(UnlinkCrnArgs),
    /// Remove your stake from a node
    Unstake(UnstakeArgs),
}

#[derive(Debug, Clone, Args)]
#[command(group = clap::ArgGroup::new("scope").args(["address", "all"]))]
pub struct NodeListArgs {
    /// Filter by owner address. Defaults to own address from signing config.
    #[arg(long)]
    pub address: Option<String>,

    /// List all nodes on the network.
    #[arg(long)]
    pub all: bool,

    /// Filter by node type.
    #[arg(long, value_enum, rename_all = "lowercase")]
    pub r#type: Option<NodeTypeCli>,

    /// Address of the corechannel aggregate owner. Defaults to the mainnet address.
    #[arg(long)]
    pub corechannel_address: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum NodeTypeCli {
    Ccn,
    Crn,
}

#[derive(Args)]
pub struct CreateCcnArgs {
    /// Human-readable node name.
    #[arg(long)]
    pub name: String,

    /// libp2p multiaddress (e.g. /ip4/1.2.3.4/tcp/4025/p2p/Qm...).
    #[arg(long)]
    pub multiaddress: String,

    /// Network tag (e.g. mainnet, testnet).
    #[arg(long, default_value = "mainnet")]
    pub network: String,

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

    /// Network tag (e.g. mainnet, testnet).
    #[arg(long, default_value = "mainnet")]
    pub network: String,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct LinkCrnArgs {
    /// Hash of the CRN to link.
    #[arg(long)]
    pub crn: NodeHash,

    /// Network tag (e.g. mainnet, testnet).
    #[arg(long, default_value = "mainnet")]
    pub network: String,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct UnlinkCrnArgs {
    /// Hash of the CRN to unlink.
    #[arg(long)]
    pub crn: NodeHash,

    /// Network tag (e.g. mainnet, testnet).
    #[arg(long, default_value = "mainnet")]
    pub network: String,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct StakeArgs {
    /// Hash of the node to stake on.
    #[arg(long)]
    pub node: NodeHash,

    /// Network tag (e.g. mainnet, testnet).
    #[arg(long, default_value = "mainnet")]
    pub network: String,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct UnstakeArgs {
    /// Hash of the node to unstake from.
    #[arg(long)]
    pub node: NodeHash,

    /// Network tag (e.g. mainnet, testnet).
    #[arg(long, default_value = "mainnet")]
    pub network: String,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct DropNodeArgs {
    /// Hash of the node to remove.
    #[arg(long)]
    pub node: NodeHash,

    /// Network tag (e.g. mainnet, testnet).
    #[arg(long, default_value = "mainnet")]
    pub network: String,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct AmendNodeArgs {
    /// Hash of the node to amend.
    #[arg(long)]
    pub node: NodeHash,

    /// Human-readable node name.
    #[arg(long)]
    pub name: Option<String>,

    /// libp2p multiaddress (CCN only).
    #[arg(long)]
    pub multiaddress: Option<String>,

    /// HTTPS endpoint address (CRN only).
    #[arg(long)]
    pub address: Option<String>,

    /// Profile picture URL.
    #[arg(long)]
    pub picture: Option<String>,

    /// Banner image URL.
    #[arg(long)]
    pub banner: Option<String>,

    /// Node description.
    #[arg(long)]
    pub description: Option<String>,

    /// Reward address.
    #[arg(long)]
    pub reward: Option<String>,

    /// PAYG stream reward address.
    #[arg(long)]
    pub stream_reward: Option<String>,

    /// Manager address.
    #[arg(long)]
    pub manager: Option<String>,

    /// Authorized staker addresses (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub authorized: Option<Vec<String>>,

    /// Restrict staking to authorized addresses.
    #[arg(long)]
    pub locked: Option<bool>,

    /// Registration URL.
    #[arg(long)]
    pub registration_url: Option<String>,

    /// Terms and conditions hash or URL.
    #[arg(long)]
    pub terms_and_conditions: Option<String>,

    /// Network tag (e.g. mainnet, testnet).
    #[arg(long, default_value = "mainnet")]
    pub network: String,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Subcommand)]
pub enum AccountCommand {
    /// Manage address aliases (bookmarks for external addresses)
    Alias {
        #[clap(subcommand)]
        command: AliasCommand,
    },
    /// Show the balance of any address
    Balance(AccountBalanceArgs),
    /// Generate a new private key and store it in the OS keychain
    Create(AccountCreateArgs),
    /// Delete an account from the keychain
    Delete(AccountDeleteArgs),
    /// Export the private key of a local account
    Export(AccountExportArgs),
    /// Import an existing private key
    Import(AccountImportArgs),
    /// List all stored accounts
    List,
    /// Migrate accounts from the Python CLI (~/.aleph-im/)
    Migrate(AccountMigrateArgs),
    /// Show details of an account (defaults to the active account)
    Show(AccountShowArgs),
    /// Set the default account used for signing
    Use(AccountUseArgs),
}

#[derive(Args)]
pub struct AccountCreateArgs {
    /// Name for the new account.
    #[arg(long)]
    pub name: String,

    /// Chain for the new account.
    #[arg(long, value_enum, default_value = "eth")]
    pub chain: ChainCli,
}

#[derive(Args)]
pub struct AccountImportArgs {
    /// Name for the imported account.
    #[arg(long)]
    pub name: String,

    /// Chain for the imported account.
    #[arg(long, value_enum, default_value = "eth")]
    pub chain: ChainCli,

    /// Hex-encoded private key. If not provided, reads from stdin.
    #[arg(long, conflicts_with = "ledger")]
    pub private_key: Option<String>,

    /// Import from a key file (raw 32-byte binary or hex text).
    #[arg(long, conflicts_with_all = ["private_key", "ledger"])]
    pub from_file: Option<PathBuf>,

    /// Import from a Ledger hardware wallet instead of a private key.
    #[arg(long)]
    pub ledger: bool,

    /// BIP44 derivation path override (only with --ledger).
    #[arg(long, requires = "ledger")]
    pub derivation_path: Option<String>,

    /// Number of addresses to fetch from the Ledger (only with --ledger).
    #[arg(long, requires = "ledger", default_value = "5")]
    pub ledger_count: usize,
}

#[derive(Args)]
pub struct AccountMigrateArgs {
    /// Path to the Python CLI config directory (defaults to ~/.aleph-im).
    #[arg(long)]
    pub python_home: Option<PathBuf>,

    /// Show what would be imported without actually importing.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Args)]
pub struct AccountShowArgs {
    /// Account name (defaults to the active account).
    pub name: Option<String>,
}

#[derive(Args)]
pub struct AccountBalanceArgs {
    /// Address to query (e.g. 0x...). If omitted, uses the default account.
    pub address: Option<String>,
}

#[derive(Args)]
pub struct AccountDeleteArgs {
    /// Name of the account to delete.
    pub name: String,
}

#[derive(Args)]
pub struct AccountUseArgs {
    /// Name of the account to set as default.
    pub name: String,
}

#[derive(Args)]
pub struct AccountExportArgs {
    /// Name of the account whose key to export.
    pub name: String,

    /// Skip confirmation prompt.
    #[arg(long)]
    pub yes: bool,
}

#[derive(Subcommand)]
pub enum AliasCommand {
    /// Add a named alias for an address
    Add(AliasAddArgs),
    /// List all address aliases
    List,
    /// Remove an address alias
    Remove(AliasRemoveArgs),
}

#[derive(Args)]
pub struct AliasAddArgs {
    /// Name for the alias.
    pub name: String,
    /// Address to associate with the alias (e.g. 0x...).
    pub address: String,
}

#[derive(Args)]
pub struct AliasRemoveArgs {
    /// Name of the alias to remove.
    pub name: String,
}

#[derive(Subcommand)]
pub enum FileCommand {
    /// Download a file by hash, message hash, or ref
    Download(FileDownloadArgs),
    /// Upload a file and create a STORE message
    Upload(FileUploadArgs),
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

    /// Sign on behalf of another address (requires an authorization from that address).
    #[arg(long)]
    pub on_behalf_of: Option<String>,

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
#[allow(clippy::large_enum_variant)]
pub enum InstanceCommand {
    /// Create a new instance (VM)
    Create(InstanceCreateArgs),
    /// Erase a VM instance's data on the CRN
    Erase(CrnArgs),
    /// List instances belonging to an account.
    ///
    /// Lists instances where the address is either the sender (signer) or the
    /// owner (resource address). When --address is omitted, the current default
    /// account's address is used.
    List(InstanceListArgs),
    /// Stream logs from a running VM instance
    Logs(CrnArgs),
    /// Show pricing for an instance configuration
    #[command(long_about = "\
Show pricing for an instance configuration.

There are three ways to specify the instance:

  1. By size slug:    --size 1vcpu-2gb
  2. By resources:    --vcpus 4 --memory 8GB --disk-size 100GB
  3. By GPU model:    --gpu h100

GPU instances have a minimum size determined by the model. Use --size \
or --vcpus/--memory to request more resources (must be >= the GPU minimum). \
GPU and confidential instances use separate pricing tiers and cannot \
be combined.

Examples:
  aleph instance price --size 4vcpu-8gb
  aleph instance price --vcpus 2 --memory 4GB --disk-size 50GB
  aleph instance price --gpu h100
  aleph instance price --gpu h100 --size 32vcpu-192gb
  aleph instance price --gpu            # list available GPU models
  aleph instance price --size 1vcpu-2gb --confidential")]
    Price(InstancePriceArgs),
    /// Reboot a VM instance
    Reboot(CrnArgs),
    /// Start (allocate) a VM instance on the CRN
    Start(CrnStartArgs),
    /// Stop a running VM instance
    Stop(CrnArgs),
}

#[derive(Args)]
pub struct InstanceListArgs {
    /// Address to query, as a hex address (`0x…`) or a local account/alias name.
    /// Defaults to the address of the current default account.
    #[arg(long)]
    pub address: Option<String>,
}

#[derive(Args)]
pub struct InstancePriceArgs {
    /// Instance size slug (e.g. 1vcpu-2gb, 4vcpu-8gb).
    #[arg(long)]
    pub size: Option<String>,

    /// Number of virtual CPUs. Used with --memory and --disk-size for custom sizing.
    #[arg(long)]
    pub vcpus: Option<u32>,

    /// Memory size (e.g. 2GB, 2048MB, 2GiB). Used with --vcpus and --disk-size.
    #[arg(long, value_parser = parse_size_to_mib)]
    pub memory: Option<u64>,

    /// Disk size (e.g. 20GB, 1024MB, 1TiB).
    #[arg(long, value_parser = parse_size_to_mib)]
    pub disk_size: Option<u64>,

    /// GPU model name (e.g. h100, a100, rtx-4090). Pass --gpu without a value to list models.
    #[arg(long, num_args = 0..=1, default_missing_value = "")]
    pub gpu: Option<String>,

    /// Use confidential VM pricing (AMD SEV).
    #[arg(long)]
    pub confidential: bool,
}

#[derive(Args)]
pub struct InstanceCreateArgs {
    /// Root filesystem image: a preset name (ubuntu22, ubuntu24, debian12) or an item hash (hex or IPFS CID).
    #[arg(
        long,
        value_parser = parse_image,
        required_unless_present = "interactive"
    )]
    pub image: Option<ItemHash>,

    /// Disk size (e.g. 20GB, 1024MB, 1TiB). Required unless --size is used.
    #[arg(long, value_parser = parse_size_to_mib)]
    pub disk_size: Option<u64>,

    /// Instance size slug (e.g. 1vcpu-2gb, 4vcpu-8gb).
    /// Fetches pricing tiers from the network and derives vcpus, memory, and disk-size.
    #[arg(long)]
    pub size: Option<String>,

    /// Number of virtual CPUs. Overrides the value from --size.
    #[arg(long)]
    pub vcpus: Option<u32>,

    /// Memory size (e.g. 2GB, 2048MB, 2GiB). Overrides the value from --size.
    #[arg(long, value_parser = parse_size_to_mib)]
    pub memory: Option<u64>,

    /// Path to an SSH public key file. Can be repeated for multiple keys.
    #[arg(long, required_unless_present = "interactive")]
    pub ssh_pubkey_file: Vec<PathBuf>,

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

    /// Launch a confidential VM (AMD SEV).
    #[arg(long)]
    pub confidential: bool,

    /// UEFI firmware hash for confidential VMs.
    #[arg(
        long,
        default_value = "ba5bb13f3abca960b101a759be162b229e2b7e93ecad9d1307e54de887f177ff"
    )]
    pub confidential_firmware: String,

    /// GPU model name (e.g. rtx4090, a100, l40s). Can be repeated for multiple GPUs.
    /// Use `aleph instance price --gpu` to list available models.
    #[arg(long)]
    pub gpu: Option<Vec<String>>,

    /// CRN node hash. Pins the instance to a specific compute node.
    #[arg(long)]
    pub crn_hash: Option<String>,

    /// Sign on behalf of another address (requires an authorization from that address).
    #[arg(long)]
    pub on_behalf_of: Option<String>,

    /// Prompt interactively for any values not provided on the command line.
    /// Always runs the CRN picker.
    #[arg(short = 'i', long)]
    pub interactive: bool,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Subcommand)]
pub enum AuthorizationCommand {
    /// Add an authorization for a delegate
    Add(AuthorizationAddArgs),
    /// List authorizations granted by an address
    List(AuthorizationListArgs),
    /// List authorizations received from other addresses
    Received(AuthorizationReceivedArgs),
    /// Revoke authorizations for a delegate
    Revoke(AuthorizationRevokeArgs),
}

#[derive(Debug, Clone, Args)]
pub struct AuthorizationListArgs {
    /// Address to list authorizations for (defaults to own address from --private-key)
    #[arg(long)]
    pub address: Option<String>,

    /// Filter by delegate address
    #[arg(long)]
    pub delegate: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Debug, Clone, Args)]
pub struct AuthorizationReceivedArgs {
    /// Address to check received authorizations for (defaults to own address from --private-key)
    #[arg(long)]
    pub address: Option<String>,

    /// Filter by granter address
    #[arg(long)]
    pub granter: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Debug, Clone, Args)]
pub struct AuthorizationAddArgs {
    /// Address to delegate authorization to
    pub delegate_address: String,

    /// Restrict the authorization to a specific chain
    #[arg(long = "delegate-chain", value_enum)]
    pub delegate_chain: Option<ChainCli>,

    /// Comma-separated list of allowed channels
    #[arg(long, value_delimiter = ',')]
    pub channels: Vec<String>,

    /// Comma-separated list of allowed message types (e.g. post,aggregate)
    #[arg(long, value_delimiter = ',', value_enum)]
    pub message_types: Vec<MessageTypeCli>,

    /// Comma-separated list of allowed post types
    #[arg(long, value_delimiter = ',')]
    pub post_types: Vec<String>,

    /// Comma-separated list of allowed aggregate keys
    #[arg(long, value_delimiter = ',')]
    pub aggregate_keys: Vec<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Debug, Clone, Args)]
#[command(group(clap::ArgGroup::new("target").required(true)))]
pub struct AuthorizationRevokeArgs {
    /// Address to revoke authorization from
    #[arg(group = "target")]
    pub delegate_address: Option<String>,

    /// Revoke all authorizations
    #[arg(long, group = "target")]
    pub all: bool,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct CrnArgs {
    /// CRN endpoint URL.
    #[arg(long)]
    pub crn_url: String,

    /// VM instance item hash.
    pub vm_id: ItemHash,

    #[command(flatten)]
    pub signing: SigningArgs,
}

/// Start is separate because it's unauthenticated — signing args are still
/// required to construct the CrnClient but no auth headers are sent.
#[derive(Args)]
pub struct CrnStartArgs {
    /// CRN endpoint URL.
    #[arg(long)]
    pub crn_url: String,

    /// VM instance item hash.
    pub vm_id: ItemHash,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Subcommand)]
pub enum ConfigCommand {
    /// Manage named CCN endpoints
    Ccn {
        #[clap(subcommand)]
        command: CcnCommand,
    },
}

#[derive(Subcommand)]
pub enum CcnCommand {
    /// Register a new CCN endpoint
    Add(CcnAddArgs),
    /// List all registered CCN endpoints
    List,
    /// Remove a registered CCN endpoint
    Remove(CcnRemoveArgs),
    /// Show details of a CCN endpoint (defaults to the active one)
    Show(CcnShowArgs),
    /// Set the default CCN endpoint
    Use(CcnUseArgs),
}

#[derive(Args)]
pub struct CcnAddArgs {
    /// Name for this CCN endpoint.
    pub name: String,
    /// URL of the CCN endpoint.
    pub url: String,
}

#[derive(Args)]
pub struct CcnUseArgs {
    /// Name of the CCN to set as default.
    pub name: String,
}

#[derive(Args)]
pub struct CcnShowArgs {
    /// CCN name (defaults to the active CCN).
    pub name: Option<String>,
}

#[derive(Args)]
pub struct CcnRemoveArgs {
    /// Name of the CCN to remove.
    pub name: String,
}
