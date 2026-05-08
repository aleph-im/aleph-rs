use aleph_sdk::aggregate_models::corechannel::NodeHash;
use aleph_sdk::credit::PriceSource;
use aleph_types::item_hash::ItemHash;
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use url::Url;

/// Clap adapter for [`PriceSource::from_str`]. Exposed as a named function so
/// it can be referenced from `value_parser = parse_price_source` attributes.
fn parse_price_source(s: &str) -> Result<PriceSource, String> {
    s.parse::<PriceSource>().map_err(|e| e.to_string())
}

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

/// A user-supplied image reference: either a preset name to be resolved against
/// the `vm-images` aggregate, or a raw item hash / IPFS CID.
#[derive(Clone, Debug)]
pub enum ImageRef {
    Preset(String),
    Hash(ItemHash),
}

/// Parse an image-or-preset argument. Accepts a raw item hash or IPFS CID
/// (returns `Hash`), or any non-empty non-hash string (returns `Preset`).
/// Empty / whitespace-only input is rejected. Preset name validity is checked
/// later, against the network-published `vm-images` aggregate.
pub fn parse_image_ref(s: &str) -> Result<ImageRef, String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err("image cannot be empty".to_string());
    }
    match ItemHash::try_from(trimmed) {
        Ok(h) => Ok(ImageRef::Hash(h)),
        Err(_) => Ok(ImageRef::Preset(trimmed.to_string())),
    }
}

/// Well-known rootfs image presets.
pub(crate) const IMAGE_PRESETS: &[(&str, &str)] = &[
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

/// Long-form version string shown by `aleph --version`. Includes the git
/// commit SHA and date captured at build time by `build.rs`.
const LONG_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("ALEPH_GIT_COMMIT"),
    " ",
    env!("ALEPH_COMMIT_DATE"),
    ")"
);

#[derive(Parser)]
#[command(name = "aleph", version, long_version = LONG_VERSION, about = "Aleph CLI")]
pub struct Cli {
    /// CCN to talk to: either a config alias name (see `aleph config ccn list`) or a raw URL (anything containing `://`).
    #[arg(long)]
    pub ccn: Option<String>,

    /// Output results as JSON (for scripting/tooling).
    #[arg(long, global = true)]
    pub json: bool,

    /// Named network from config (see: aleph config network list).
    #[arg(long)]
    pub network: Option<String>,

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
    /// Manage CLI configuration (networks, CCN endpoints, etc.)
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
    /// Buy and manage Aleph credits
    Credit {
        #[clap(subcommand)]
        command: CreditCommand,
    },
    /// Generate shell completion script
    Completions {
        /// Target shell.
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

#[derive(Subcommand)]
pub enum MessageCommand {
    /// Forget messages or entire aggregates
    #[command(long_about = "\
Forget messages on the network. Two scopes are supported:

  <HASHES>...        Forget specific messages, one per item hash. Each hash \
identifies a single message (POST, AGGREGATE element, STORE, PROGRAM, or \
INSTANCE). The network cascades the appropriate cleanup based on the \
target's type: forgetting a STORE releases the file pin, forgetting a POST \
also forgets its amends, forgetting a PROGRAM/INSTANCE tears down its VM, \
and forgetting an AGGREGATE element rebuilds the aggregate from the \
elements that remain.

  --aggregates <HASH>...  Forget an entire aggregate, identified by the \
item hash of any of its element messages. This forgets *every* AGGREGATE \
message sent by the same address under the same key — not just the one \
hash you passed. Use this when you want the whole aggregate gone, not just \
one update to it.

Examples:
  # Forget two specific messages
  aleph message forget abc123... def456...

  # Forget the entire aggregate keyed at the (sender, key) of this element
  aleph message forget --aggregates abc123...

  # Combine: forget some specific messages AND wipe an aggregate
  aleph message forget abc123... --aggregates def456... --reason \"superseded\"

Forget is irreversible. You can only forget messages your own address owns \
(or that you have an authorization to forget on behalf of).")]
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
use chrono::{DateTime, FixedOffset, Utc};
use std::str::FromStr;

fn parse_timestamp(s: &str) -> Result<Timestamp, String> {
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

/// Identity args: the bits needed to *find* a signing key. Used on its own
/// for read-only commands that just need to know whose address to query.
#[derive(Debug, Clone, Args)]
pub struct IdentityArgs {
    /// Named account from `aleph account list`.
    /// Defaults to the active account set by `aleph account use`.
    #[arg(long)]
    pub account: Option<String>,

    /// Hex-encoded private key. Falls back to ALEPH_PRIVATE_KEY env var.
    /// Overrides --account if both are provided.
    #[arg(long)]
    pub private_key: Option<String>,

    /// Signing chain. Required with --private-key (or ALEPH_PRIVATE_KEY env);
    /// ignored when --account or the default account is used (the chain comes
    /// from the stored account).
    #[arg(long, value_enum)]
    pub chain: Option<ChainCli>,
}

/// Signing args: identity plus a `--dry-run` switch. Used on commands that
/// actually submit a message to the network.
#[derive(Debug, Clone, Args)]
pub struct SigningArgs {
    #[command(flatten)]
    pub identity: IdentityArgs,

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
    pub content_hashes: Option<Vec<ItemHash>>,

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
    pub hashes: Option<Vec<ItemHash>>,

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
            content_hashes: c.content_hashes,
            refs: c.refs,
            addresses: c.addresses.map(|v| v.into_iter().map(Into::into).collect()),
            owners: None,
            tags: c.tags,
            hashes: c.hashes,
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
    pub hashes: Option<Vec<ItemHash>>,

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
            hashes: c.hashes,
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
    /// Fetch a single aggregate by key
    Get(AggregateGetArgs),
    /// List every aggregate owned by an address
    List(AggregateListArgs),
    /// Forget entire aggregates by element hash, with type validation
    Forget(AggregateForgetArgs),
}

#[derive(Args)]
pub struct AggregateGetArgs {
    /// Aggregate key to fetch.
    pub key: String,

    /// Owner address. Accepts a raw address (`0x...`) or a local account /
    /// alias name. Defaults to the current default account.
    #[arg(long)]
    pub address: Option<String>,
}

#[derive(Args)]
pub struct AggregateListArgs {
    /// Owner address. Accepts a raw address (`0x...`) or a local account /
    /// alias name. Defaults to the current default account.
    #[arg(long)]
    pub address: Option<String>,
}

#[derive(Args)]
pub struct AggregateForgetArgs {
    /// Item hashes of any AGGREGATE element message belonging to the
    /// aggregates to forget. The network resolves the (sender, key) pair
    /// from each hash and tombstones every AGGREGATE message under that
    /// key from that sender.
    pub hashes: Vec<ItemHash>,

    /// Reason for forgetting.
    #[arg(long)]
    pub reason: Option<String>,

    /// Channel name.
    #[arg(long)]
    pub channel: Option<String>,

    /// Sign on behalf of another address (requires an authorization from that address).
    #[arg(long)]
    pub on_behalf_of: Option<String>,

    /// Skip the confirmation prompt and submit immediately.
    #[arg(short = 'y', long)]
    pub yes: bool,

    #[command(flatten)]
    pub signing: SigningArgs,
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
    /// Item hashes of the messages to forget (one-by-one).
    ///
    /// Each hash identifies a single message to tombstone. To forget an
    /// entire aggregate (all elements with the same sender + key), use
    /// `--aggregates` instead.
    pub hashes: Vec<ItemHash>,

    /// Item hashes identifying aggregates to forget *in their entirety*.
    ///
    /// Pass the item hash of any element message belonging to the
    /// aggregate; the network resolves the (sender, key) pair from it and
    /// forgets every AGGREGATE message under that key from that sender.
    /// Use this when you want the whole aggregate gone — not just one
    /// update to it. Compare with positional `<HASHES>` which forgets one
    /// message per hash.
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

    /// Skip the confirmation prompt and submit immediately.
    #[arg(short = 'y', long)]
    pub yes: bool,

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
    pub identity: IdentityArgs,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum NodeTypeCli {
    Ccn,
    Crn,
}

#[derive(Args)]
pub struct CreateCcnArgs {
    /// Human-readable node name.
    #[arg(value_name = "NAME")]
    pub name: String,

    /// libp2p multiaddress (e.g. /ip4/1.2.3.4/tcp/4025/p2p/Qm...).
    #[arg(long)]
    pub multiaddress: String,

    /// Override the corechannel network tag embedded in the node's
    /// aggregate. Defaults to the current network's name (from --network
    /// or the configured default network).
    #[arg(long)]
    pub network_tag: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct CreateCrnArgs {
    /// Human-readable node name.
    #[arg(value_name = "NAME")]
    pub name: String,

    /// HTTPS URL of the CRN.
    #[arg(long)]
    pub url: String,

    /// Override the corechannel network tag embedded in the node's
    /// aggregate. Defaults to the current network's name (from --network
    /// or the configured default network).
    #[arg(long)]
    pub network_tag: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct LinkCrnArgs {
    /// Hash of the CRN to link.
    #[arg(long)]
    pub crn: NodeHash,

    /// Override the corechannel network tag embedded in the node's
    /// aggregate. Defaults to the current network's name (from --network
    /// or the configured default network).
    #[arg(long)]
    pub network_tag: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct UnlinkCrnArgs {
    /// Hash of the CRN to unlink.
    #[arg(long)]
    pub crn: NodeHash,

    /// Override the corechannel network tag embedded in the node's
    /// aggregate. Defaults to the current network's name (from --network
    /// or the configured default network).
    #[arg(long)]
    pub network_tag: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct StakeArgs {
    /// Hash of the node to stake on.
    #[arg(long)]
    pub node: NodeHash,

    /// Override the corechannel network tag embedded in the node's
    /// aggregate. Defaults to the current network's name (from --network
    /// or the configured default network).
    #[arg(long)]
    pub network_tag: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct UnstakeArgs {
    /// Hash of the node to unstake from.
    #[arg(long)]
    pub node: NodeHash,

    /// Override the corechannel network tag embedded in the node's
    /// aggregate. Defaults to the current network's name (from --network
    /// or the configured default network).
    #[arg(long)]
    pub network_tag: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct DropNodeArgs {
    /// Hash of the node to remove.
    #[arg(long)]
    pub node: NodeHash,

    /// Override the corechannel network tag embedded in the node's
    /// aggregate. Defaults to the current network's name (from --network
    /// or the configured default network).
    #[arg(long)]
    pub network_tag: Option<String>,

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

    /// HTTPS URL of the CRN (CRN only).
    #[arg(long)]
    pub url: Option<String>,

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

    /// Override the corechannel network tag embedded in the node's
    /// aggregate. Defaults to the current network's name (from --network
    /// or the configured default network).
    #[arg(long)]
    pub network_tag: Option<String>,

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
    #[command(long_about = "\
Generate a fresh private key and store it in the OS keychain under the \
given name. The key never touches disk. Use the resulting account by \
passing `--account <NAME>` to signing commands, or set it as the default \
with `aleph account use <NAME>`.

To import an existing key (private-key, keystore file, or Ledger), use \
`aleph account import` instead.

Examples:
  aleph account create alice                    # EVM (default chain: eth)
  aleph account create alice --chain sol        # Solana
  aleph account use alice                       # set as default for signing")]
    Create(AccountCreateArgs),
    /// Remove an account from the keychain
    Remove(AccountRemoveArgs),
    /// Export the private key of a local account
    Export(AccountExportArgs),
    /// Import an existing private key
    #[command(long_about = "\
Import an existing key into the OS keychain under the given name. Three \
sources are supported, mutually exclusive:

  --private-key <HEX>   Hex-encoded private key on the command line. If \
omitted (and no other source given), the key is read from stdin so it does \
not appear in shell history.

  --from-file <PATH>    Read from a file containing a raw 32-byte binary \
key or a hex-encoded text key.

  --ledger              Use a Ledger hardware wallet. Combine with \
`--derivation-path` to override the default BIP44 path, and \
`--ledger-count` to fetch more than the default 5 candidate addresses.

Examples:
  aleph account import alice --private-key 0xabcd1234...
  aleph account import alice --from-file ~/keys/alice.key
  aleph account import alice --ledger
  aleph account import alice --chain sol --ledger
  echo \"0xabcd1234...\" | aleph account import alice    # via stdin")]
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
    #[arg(value_name = "NAME")]
    pub name: String,

    /// Chain for the new account.
    #[arg(long, value_enum, default_value = "eth")]
    pub chain: ChainCli,
}

#[derive(Args)]
pub struct AccountImportArgs {
    /// Name for the imported account.
    #[arg(value_name = "NAME")]
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
    /// Address to query, as a hex address (`0x…`) or a local account/alias
    /// name. If omitted, uses the default account.
    pub address: Option<String>,
}

#[derive(Args)]
pub struct AccountRemoveArgs {
    /// Name of the account to remove.
    pub name: String,

    /// Skip the type-the-name confirmation prompt.
    #[arg(short = 'y', long)]
    pub yes: bool,
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
    /// Pin an existing file by creating a STORE message for a known item hash
    Pin(FilePinArgs),
    /// Upload a file and create a STORE message
    #[command(long_about = "\
Upload a file (or directory) and create a STORE message announcing it on \
the network. The signed STORE message anchors a content-addressed pin: \
its item hash is the file's hash, and the network keeps the content as \
long as the pin is paid for.

Storage engine defaults to `storage` (Aleph native, ≤ 100 MB) for files \
and `ipfs` for directories. Pass `--storage-engine ipfs` to put a single \
file on IPFS instead. Payment defaults to credits; pass \
`--payment-type hold` to fall back to locked-stake.

Use `--ref <NAME>` to give the file a stable user-defined identifier \
(e.g. `report/latest`) — useful for in-place updates and for downloading \
later via `aleph file download --ref ...`.

Examples:
  aleph file upload ./report.pdf
  aleph file upload ./website/                          # directory → IPFS
  aleph file upload ./big.bin --storage-engine ipfs
  aleph file upload ./report.pdf --ref reports/q4
  aleph file upload ./data.bin --channel my-channel")]
    Upload(FileUploadArgs),
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum StorageEngineCli {
    /// Aleph native storage (default, recommended for files up to 100 MB).
    Storage,
    /// IPFS storage.
    Ipfs,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum PaymentTypeCli {
    /// Locked-stake payment, no credit consumption (deprecated upstream).
    Hold,
    /// Credit-based payment.
    Credit,
}

#[derive(Args)]
pub struct FileUploadArgs {
    /// Path of the file to upload.
    pub path: std::path::PathBuf,

    /// Storage engine to use. Defaults to `storage` for files and `ipfs` for
    /// directories (native storage does not support directory uploads).
    #[arg(long, value_enum)]
    pub storage_engine: Option<StorageEngineCli>,

    /// Payment type for the STORE message. `credit` (default) consumes
    /// credits; `hold` requires locked stake on the account.
    #[arg(long, value_enum)]
    pub payment_type: Option<PaymentTypeCli>,

    /// Channel name.
    #[arg(long)]
    pub channel: Option<String>,

    /// User-defined file reference (for updates/versioning).
    #[arg(long = "ref")]
    pub reference: Option<String>,

    /// Sign on behalf of another address (requires an authorization from that address).
    #[arg(long)]
    pub on_behalf_of: Option<String>,

    /// IPFS gateway URL (kubo daemon host) to use for directory uploads. The
    /// SDK appends `/api/v0/...` paths internally, so pass scheme + host (and
    /// optionally port), e.g. `http://localhost:5001`. Defaults to
    /// `https://ipfs.aleph.cloud`. Can also be set via the
    /// `ALEPH_IPFS_GATEWAY` environment variable.
    #[arg(long, env = "ALEPH_IPFS_GATEWAY")]
    pub ipfs_gateway: Option<Url>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct FilePinArgs {
    /// Item hash of the file to pin. A native hex hash selects the `storage`
    /// engine; an IPFS CID selects the `ipfs` engine.
    pub item_hash: ItemHash,

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
    #[command(long_about = "\
Create a new VM instance. Sizing is specified one of three ways:

  --size <SLUG>                       e.g. `1vcpu-2gb`, `2vcpu-4gb`, \
`4vcpu-8gb`, `8vcpu-16gb`.
  --vcpus N --memory <SIZE> --disk-size <SIZE>
                                      Custom sizing. Sizes accept \
human-readable forms like `4GB`, `512MiB`, `1TiB`.
  --gpu <MODEL> [...]                 GPU instance. Use \
`aleph instance price --list-gpus` to list models. Combine with `--size` \
to request resources above the GPU's minimum.

Required: NAME (positional), `--image`, and at least one \
`--ssh-pubkey-file`. Image accepts a preset name (`ubuntu22`, `ubuntu24`, \
`debian12`) or an item hash (hex or IPFS CID).

Pin to a specific compute node with `--crn-hash <HASH>`. For an \
interactive walkthrough that prompts for any missing fields and lets you \
pick a CRN from a list, pass `-i` / `--interactive`.

Volumes can be added with `--persistent-volume`, `--ephemeral-volume`, or \
`--immutable-volume` (each can be repeated).

Examples:
  aleph instance create web --image ubuntu24 --size 1vcpu-2gb \\
                            --ssh-pubkey-file ~/.ssh/id_ed25519.pub

  aleph instance create gpu-job --image ubuntu24 --gpu h100 \\
                                --ssh-pubkey-file ~/.ssh/id_ed25519.pub

  aleph instance create db --image ubuntu24 --size 4vcpu-8gb \\
      --persistent-volume name=data,mount=/data,size=100GB \\
      --ssh-pubkey-file ~/.ssh/id_ed25519.pub

  aleph instance create -i web   # interactive prompts for everything else")]
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
  aleph instance price --list-gpus      # list available GPU models (recommended)
  aleph instance price --gpu            # same, kept as a shortcut
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

    /// GPU model name (e.g. h100, a100, rtx-4090). Pass --gpu without a value to list models
    /// (or use --list-gpus).
    #[arg(long, num_args = 0..=1, default_missing_value = "")]
    pub gpu: Option<String>,

    /// List available GPU models and exit.
    #[arg(long, conflicts_with = "gpu")]
    pub list_gpus: bool,

    /// Use confidential VM pricing (AMD SEV).
    #[arg(long)]
    pub confidential: bool,
}

#[derive(Args)]
pub struct InstanceCreateArgs {
    /// Instance name.
    #[arg(value_name = "NAME")]
    pub name: String,

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
    pub crn_hash: Option<NodeHash>,

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
    pub identity: IdentityArgs,
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
    pub identity: IdentityArgs,
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
    /// Manage named CCN endpoints inside the current network
    Ccn {
        #[clap(subcommand)]
        command: CcnCommand,
    },
    /// Manage named networks (mainnet, testnet, etc.)
    Network {
        #[clap(subcommand)]
        command: NetworkCommand,
    },
}

#[derive(Subcommand)]
pub enum CcnCommand {
    /// Register a new CCN endpoint in the current network
    Add(CcnAddArgs),
    /// List CCN endpoints in the current network (or across all networks with --all)
    List(CcnListArgs),
    /// Remove a registered CCN endpoint
    Remove(CcnRemoveArgs),
    /// Show details of a CCN endpoint (defaults to the current network's default)
    Show(CcnShowArgs),
    /// Set the default CCN for a network
    Use(CcnUseArgs),
}

#[derive(Args)]
pub struct CcnAddArgs {
    /// Name for this CCN endpoint.
    pub name: String,
    /// URL of the CCN endpoint.
    pub url: String,
    /// Target network (defaults to the current network).
    #[arg(long)]
    pub network: Option<String>,
}

#[derive(Args)]
pub struct CcnListArgs {
    /// Target network (defaults to the current network). Mutually exclusive with --all.
    #[arg(long, conflicts_with = "all")]
    pub network: Option<String>,
    /// List CCNs across every network. Mutually exclusive with --network.
    #[arg(long)]
    pub all: bool,
}

#[derive(Args)]
pub struct CcnUseArgs {
    /// Name of the CCN to set as the network's default.
    pub name: String,
    /// Target network (defaults to the current network).
    #[arg(long)]
    pub network: Option<String>,
}

#[derive(Args)]
pub struct CcnShowArgs {
    /// CCN name (defaults to the network's default CCN).
    pub name: Option<String>,
    /// Target network (defaults to the current network).
    #[arg(long)]
    pub network: Option<String>,
}

#[derive(Args)]
pub struct CcnRemoveArgs {
    /// Name of the CCN to remove.
    pub name: String,
    /// Target network (defaults to the current network).
    #[arg(long)]
    pub network: Option<String>,
    /// Skip the confirmation prompt.
    #[arg(short = 'y', long)]
    pub yes: bool,
}

#[derive(Subcommand)]
pub enum NetworkCommand {
    /// Register a new (empty) network, optionally with Ethereum settlement config
    Add(NetworkAddArgs),
    /// List all registered networks
    List,
    /// Remove a network and all its CCNs (refuses if it's the default)
    Remove(NetworkRemoveArgs),
    /// Show details of a network (defaults to the current one)
    Show(NetworkShowArgs),
    /// Set the default (current) network
    Use(NetworkUseArgs),
    /// Update a network's Ethereum settlement config in place
    Set(NetworkSetArgs),
}

/// Ethereum settlement flags shared by `network add` and `network set`.
/// All fields are optional; `network add` applies them on top of the
/// mainnet defaults, `network set` patches the existing config.
#[derive(Args)]
pub struct NetworkEthereumArgs {
    /// Ethereum JSON-RPC endpoint for this network.
    #[arg(long)]
    pub rpc_url: Option<String>,
    /// Credit smart-contract address (receives ERC20 transfers).
    #[arg(long)]
    pub credit_contract: Option<alloy_primitives::Address>,
    /// ALEPH ERC20 token address on this network.
    #[arg(long)]
    pub aleph_token: Option<alloy_primitives::Address>,
    /// USDC ERC20 token address on this network.
    #[arg(long)]
    pub usdc_token: Option<alloy_primitives::Address>,
    /// ALEPH/USD price source: `coingecko`, `fixed:<usd>`, or `none`.
    #[arg(long, value_parser = parse_price_source)]
    pub price_source: Option<aleph_sdk::credit::PriceSource>,
    /// Explorer URL prefix for transaction links (e.g. `https://etherscan.io/tx/`).
    #[arg(long)]
    pub explorer_tx_base: Option<String>,
}

#[derive(Args)]
pub struct NetworkAddArgs {
    /// Name for this network.
    pub name: String,

    #[command(flatten)]
    pub ethereum: NetworkEthereumArgs,
}

#[derive(Args)]
pub struct NetworkUseArgs {
    /// Name of the network to set as default.
    pub name: String,
}

#[derive(Args)]
pub struct NetworkShowArgs {
    /// Network name (defaults to the current network).
    pub name: Option<String>,
}

#[derive(Args)]
pub struct NetworkRemoveArgs {
    /// Name of the network to remove.
    pub name: String,

    /// Skip the confirmation prompt.
    #[arg(short = 'y', long)]
    pub yes: bool,
}

#[derive(Args)]
pub struct NetworkSetArgs {
    /// Name of the network to update (defaults to the current network).
    #[arg(long)]
    pub network: Option<String>,

    #[command(flatten)]
    pub ethereum: NetworkEthereumArgs,
}

#[derive(Subcommand)]
pub enum CreditCommand {
    /// Buy Aleph credits by transferring ALEPH or USDC tokens
    #[command(long_about = "\
Buy Aleph credits by transferring ALEPH or USDC tokens from your EVM \
account. The transfer goes to the network's credit purchase address; the \
protocol mints credits to your address once the transfer is confirmed.

`--amount` is in human-readable token units (decimals OK), not credits. \
1 USD purchases 1,000,000 credits. Use `aleph account balance` afterwards \
to confirm the credits arrived.

Examples:
  aleph credit buy --token aleph --amount 100
  aleph credit buy --token usdc  --amount 50.5
  aleph credit buy --token usdc  --amount 25 --yes      # skip confirmation
  aleph credit buy --token aleph --amount 10 --rpc-url https://my-node.example")]
    Buy(BuyCreditArgs),
    /// Transfer Aleph credits to another account
    #[command(long_about = "\
Transfer credits to another address. `--amount` is the number of credits \
(integer, not tokens) — 1 USD ≈ 1,000,000 credits. The recipient can be a \
hex address, a local account name, or an alias from `aleph account alias \
list`.

Optionally pass `--expiration <RFC3339>` to claw back any unspent portion \
after that time.

Examples:
  aleph credit transfer --to bob --amount 1000000
  aleph credit transfer --to 0xab12... --amount 500000
  aleph credit transfer --to bob --amount 250000 \\
                        --expiration 2026-12-31T23:59:59Z")]
    Transfer(TransferCreditArgs),
    /// Display the paginated credit history of an address
    History(CreditHistoryArgs),
}

#[derive(Args)]
pub struct CreditHistoryArgs {
    /// Owner address. Accepts a raw address (`0x...`) or a local account /
    /// alias name. Defaults to the current default account.
    #[arg(long)]
    pub address: Option<String>,

    /// Page number (1-indexed).
    #[arg(long, default_value_t = 1)]
    pub page: u32,

    /// Items per page (server-clamped).
    #[arg(long, default_value_t = 100)]
    pub page_size: u32,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CreditTokenCli {
    Aleph,
    Usdc,
}

#[derive(Args)]
pub struct BuyCreditArgs {
    /// Token to pay with
    #[arg(long, value_enum)]
    pub token: CreditTokenCli,

    /// Amount in human-readable units (e.g. 100 for 100 ALEPH)
    #[arg(long)]
    pub amount: String,

    /// Ethereum JSON-RPC endpoint — overrides the network's configured `rpc_url`.
    #[arg(long)]
    pub rpc_url: Option<String>,

    /// Skip the confirmation prompt and submit the transaction immediately.
    #[arg(short = 'y', long)]
    pub yes: bool,

    #[command(flatten)]
    pub signing: SigningArgs,
}

pub fn parse_rfc3339_utc(s: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| format!("invalid RFC3339 timestamp '{s}': {e}"))
}

#[derive(Args)]
pub struct TransferCreditArgs {
    /// Recipient address, account name, or alias from the local store.
    #[arg(long)]
    pub to: String,

    /// Number of credits to transfer (strictly positive integer).
    #[arg(long)]
    pub amount: u64,

    /// Optional expiration as an RFC3339 timestamp
    /// (e.g. `2026-12-31T23:59:59Z`). The recipient loses any unspent
    /// portion of this transfer after that time.
    #[arg(long, value_parser = parse_rfc3339_utc)]
    pub expiration: Option<DateTime<Utc>>,

    /// Optional channel for the underlying POST message.
    #[arg(long)]
    pub channel: Option<String>,

    /// Skip the confirmation prompt and submit immediately.
    #[arg(short = 'y', long)]
    pub yes: bool,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[cfg(test)]
mod credit_transfer_args_tests {
    use super::*;

    #[test]
    fn parse_rfc3339_utc_accepts_z_suffix() {
        let dt = parse_rfc3339_utc("2026-12-31T23:59:59Z").unwrap();
        assert_eq!(dt.to_rfc3339(), "2026-12-31T23:59:59+00:00");
    }

    #[test]
    fn parse_rfc3339_utc_accepts_offset_and_normalizes_to_utc() {
        let dt = parse_rfc3339_utc("2026-12-31T23:59:59+01:00").unwrap();
        assert_eq!(dt.to_rfc3339(), "2026-12-31T22:59:59+00:00");
    }

    #[test]
    fn parse_rfc3339_utc_rejects_garbage() {
        assert!(parse_rfc3339_utc("not a date").is_err());
    }

    fn assert_value_validation_err(args: &[&str]) {
        match Cli::try_parse_from(args) {
            Ok(_) => panic!("expected parse error for {args:?}"),
            Err(e) => assert_eq!(e.kind(), clap::error::ErrorKind::ValueValidation),
        }
    }

    #[test]
    fn message_list_rejects_malformed_hashes_cleanly() {
        // Previously these arguments would panic via .unwrap() during the
        // From<MessageFilterCli> conversion. They must now fail at parse
        // time with a clap error.
        assert_value_validation_err(&["aleph", "message", "list", "--hashes", "not-a-hash"]);
        assert_value_validation_err(&[
            "aleph",
            "message",
            "list",
            "--content-hashes",
            "definitely-not-hex",
        ]);
    }

    #[test]
    fn post_list_rejects_malformed_hashes_cleanly() {
        assert_value_validation_err(&["aleph", "post", "list", "--hashes", "not-a-hash"]);
    }
}
