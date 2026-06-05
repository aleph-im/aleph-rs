use aleph_sdk::aggregate_models::corechannel::NodeHash;
use aleph_sdk::credit::PriceSource;
use aleph_types::chain::Address;
use aleph_types::item_hash::ItemHash;
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use url::Url;

use crate::common::resolve_address;

/// Clap adapter for `resolve_address`. Accepts a hex address (`0x...`), a
/// local account name, or an alias from the account store.
fn parse_address(s: &str) -> Result<Address, String> {
    resolve_address(s).map_err(|e| e.to_string())
}

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
    /// Manage custom domains attached to websites, programs, or instances
    Domain {
        #[clap(subcommand)]
        command: DomainCommand,
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
    /// Manage Aleph Cloud programs (serverless functions / micro-VMs)
    Program {
        #[clap(subcommand)]
        command: ProgramCommand,
    },
    /// Buy and manage Aleph credits
    Credit {
        #[clap(subcommand)]
        command: CreditCommand,
    },
    /// Acquire ALEPH tokens by swapping ETH or USDC via CoW Swap or Uniswap
    Token {
        #[clap(subcommand)]
        command: TokenCommand,
    },
    /// Generate shell completion script
    Completions {
        /// Target shell.
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
    /// Deploy and manage static websites
    Website {
        #[clap(subcommand)]
        command: WebsiteCommand,
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
    List(Box<MessageListArgs>),
    /// Re-submit a previously rejected message
    Retry(RetryArgs),
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

#[derive(Args)]
pub struct RetryArgs {
    /// The item hash of the rejected message to re-submit.
    pub item_hash: ItemHash,

    /// Print the reconstructed envelope without submitting.
    #[arg(long)]
    pub dry_run: bool,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum FrameworkCli {
    None,
    Nextjs,
    React,
    Vue,
    Gatsby,
    Svelte,
    Nuxt,
    Angular,
    Other,
}

impl std::fmt::Display for FrameworkCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            FrameworkCli::None => "none",
            FrameworkCli::Nextjs => "nextjs",
            FrameworkCli::React => "react",
            FrameworkCli::Vue => "vue",
            FrameworkCli::Gatsby => "gatsby",
            FrameworkCli::Svelte => "svelte",
            FrameworkCli::Nuxt => "nuxt",
            FrameworkCli::Angular => "angular",
            FrameworkCli::Other => "other",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framework_display_round_trip() {
        assert_eq!(FrameworkCli::Nextjs.to_string(), "nextjs");
        assert_eq!(FrameworkCli::Other.to_string(), "other");
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

    /// Sender addresses. Hex (`0x...`), local account name, or alias.
    /// CSV or repeat the flag.
    #[arg(long, value_delimiter = ',', value_parser = parse_address)]
    pub addresses: Option<Vec<Address>>,

    /// Content owners. Hex (`0x...`), local account name, or alias.
    /// CSV or repeat the flag.
    #[arg(long, value_delimiter = ',', value_parser = parse_address)]
    pub owners: Option<Vec<Address>>,

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
}

#[derive(Debug, Clone, Args)]
pub struct MessageListArgs {
    /// Maximum number of messages to return. Walks cursor pagination
    /// server-side; safe for large values (no offset cost).
    #[arg(long, default_value = "200")]
    pub count: u32,

    #[command(flatten)]
    pub filter: MessageFilterCli,
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
            addresses: c.addresses,
            owners: c.owners,
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
    /// Filter by sender address(es). Hex (`0x...`), local account name, or
    /// alias. CSV or repeat the flag.
    #[arg(long, value_delimiter = ',', value_parser = parse_address)]
    pub addresses: Option<Vec<Address>>,

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
            addresses: c.addresses,
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
    /// from each hash and permanently deletes every AGGREGATE message
    /// under that key from that sender.
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
    /// Each hash identifies a single message to delete permanently. To
    /// forget an entire aggregate (all elements with the same sender +
    /// key), use `--aggregates` instead.
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
    /// Delete files by hash, releasing the matching STORE pins
    #[command(long_about = "\
Delete files by their content hash (IPFS CID or native hex). Each hash is \
resolved to the STORE message that pins the file for the owner, and that \
message is forgotten - releasing the pin.

Use `aleph message forget` instead when you need to forget a specific \
STORE *message* by its item hash (e.g. to remove a duplicate pin while \
keeping the file alive via the others).

Forget is irreversible. You can only delete files owned by your own \
address (or that you have an authorization to forget on behalf of).

Examples:
  aleph file delete Qmabc...                          # IPFS CID
  aleph file delete 9675a23e...                       # native hex
  aleph file delete Qmabc... QmDef... --reason \"superseded\"
  aleph file delete Qmabc... -y --on-behalf-of 0x...")]
    Delete(FileDeleteArgs),
    /// Download a file by hash, message hash, or ref
    Download(FileDownloadArgs),
    /// List the files stored by an address
    List(FileListArgs),
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

    /// Use the legacy unauthenticated kubo-gateway path for directory
    /// uploads. Default is the authenticated `/api/v0/ipfs/add_car` endpoint
    /// on the CCN. Pass this flag to force the older path; only meaningful
    /// for directory uploads.
    #[arg(long, default_value_t = false)]
    pub use_gateway_relay: bool,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct FilePinArgs {
    /// Item hash of the file to pin. A native hex hash selects the `storage`
    /// engine; an IPFS CID selects the `ipfs` engine.
    pub item_hash: ItemHash,

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

#[derive(Args)]
pub struct FileListArgs {
    /// Address to query. Accepts a hex address (`0x…`), a local account
    /// name, or an alias. Defaults to the current default account.
    #[arg(long)]
    pub address: Option<String>,

    /// Maximum number of files to display. Walks the server's cursor
    /// pagination behind the scenes; safe for large values.
    #[arg(long, default_value = "25")]
    pub count: u32,

    /// Sort order by creation time.
    #[arg(long, value_enum, default_value = "desc")]
    pub sort_order: SortOrderCli,
}

#[derive(Args)]
pub struct FileDeleteArgs {
    /// File hashes to delete (IPFS CID or native hex). Each hash is resolved
    /// to the owner's STORE message and forgotten, releasing the file pin.
    /// To forget a specific STORE *message* by hash, use `aleph message forget`.
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
`aleph instance price --list-gpus` to list models. `--gpu <MODEL>` alone \
sizes the VM at the GPU's minimum. GPU sizes scale in compute-unit steps \
(1 vCPU + 6 GiB each), so `--size` accepts the minimum or any larger multiple \
(e.g. `4vcpu-24gb`, `5vcpu-30gb`); `--vcpus`/`--memory` work too. `--disk-size` \
is optional for GPU instances.

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
    /// Forget an INSTANCE message (send a FORGET).
    #[command(long_about = "\
Delete an instance. Forgets the corresponding INSTANCE message.

This command does ONLY the FORGET. It does NOT:
  - erase the VM on the CRN  (run `aleph instance erase` first)
  - remove port forwards     (run `aleph instance port-forward delete`)
  - stop a streaming flow    (Superfluid flow stays open on PAYG)

Run those subcommands separately if you need that cleanup.

Examples:
  aleph instance delete a41fb91c3e68
  aleph instance delete a41fb91c3e68 --reason \"decommission\"
  aleph instance delete a41fb91c3e68 --dry-run --json
")]
    Delete(InstanceDeleteArgs),
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
    /// Manage TCP/UDP port forwards for VMs, programs, or IPFS websites.
    #[command(visible_alias = "pfw")]
    PortForward {
        #[clap(subcommand)]
        command: PortForwardCommand,
    },
    /// Show pricing for an instance configuration
    #[command(long_about = "\
Show pricing for an instance configuration.

There are three ways to specify the instance:

  1. By size slug:    --size 1vcpu-2gb
  2. By resources:    --vcpus 4 --memory 8GB --disk-size 100GB
  3. By GPU model:    --gpu h100

GPU instances have a minimum size determined by the model, then scale in \
compute-unit steps (1 vCPU + 6 GiB each). --size accepts the minimum or any \
larger multiple (e.g. 4vcpu-24gb, 5vcpu-30gb); --vcpus/--memory work too. \
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
    /// Show details of an instance.
    #[command(long_about = "\
Show details of a single VM instance.

By default, gathers data without authenticated calls: the INSTANCE message
from the CCN, plus scheduler placement and status.

Pass --verbose to additionally reach the allocated CRN for live networking
(IPv4/IPv6, mapped host ports) and pull the owner's port-forwarding
aggregate. These extras add up to three HTTP calls; skip them for a fast
lookup.

For the raw INSTANCE message, use `aleph message get <hash>`.

VM_ID accepts a unique hash prefix (e.g. the 12-char hash shown by `aleph
instance list`); the scheduler matches it server-side.

Examples:
  aleph instance show a41fb91c3e68
  aleph instance show a41fb91c3e68 --verbose
  aleph instance show a41fb91c3e68 --json")]
    Show(InstanceShowArgs),
    /// SSH into a dispatched VM instance
    #[command(long_about = "\
Open an SSH session to a dispatched VM instance.

The instance must already be dispatched: the scheduler is queried to find
which CRN owns it, then the CRN's `/about/executions/list` endpoint is
consulted to discover the VM's IPv6 address. SSH is then exec'd with that
target.

Pass `--crn` (a node hash, unique hash prefix or suffix, or URL) to skip scheduler discovery. Extra arguments after `--`
are forwarded verbatim to `ssh` (e.g. to run a remote command).

Examples:
  aleph instance ssh <vm-hash>
  aleph instance ssh <vm-hash> --user ubuntu --identity ~/.ssh/id_ed25519
  aleph instance ssh <vm-hash> -- uptime")]
    Ssh(InstanceSshArgs),
    /// Start (allocate) a VM instance on the CRN
    Start(CrnStartArgs),
    /// Stop a running VM instance
    Stop(CrnArgs),
    /// Manage VM backups (create / info / download / delete / restore).
    #[command(subcommand)]
    Backup(InstanceBackupCommand),
    /// Confidential VM workflow (init session, validate measurement, inject secret).
    #[command(subcommand)]
    Confidential(ConfidentialCommand),
}

#[derive(Args)]
pub struct InstanceListArgs {
    /// Address to query, as a hex address (`0x…`) or a local account/alias name.
    /// Defaults to the address of the current default account.
    #[arg(long)]
    pub address: Option<String>,
}

#[derive(Args)]
pub struct InstanceShowArgs {
    /// VM instance item hash. Accepts a unique prefix (e.g. the 12-char hash
    /// shown by `aleph instance list`); the scheduler matches it server-side.
    pub vm_id: String,

    /// Also reach the allocated CRN for live networking (IPv4/IPv6, mapped
    /// host ports) and fetch the owner's port-forwarding aggregate.
    #[arg(short = 'v', long)]
    pub verbose: bool,
}

#[derive(Args)]
pub struct InstanceDeleteArgs {
    /// VM instance item hash to forget. Accepts a unique prefix (e.g. the
    /// 12-char hash shown by `aleph instance list`); the scheduler matches it
    /// server-side.
    pub vm_id: String,

    /// Reason recorded on the FORGET message.
    #[arg(long, default_value = "User deletion")]
    pub reason: String,

    /// Skip the confirmation prompt.
    #[arg(short = 'y', long)]
    pub yes: bool,

    #[command(flatten)]
    pub signing: SigningArgs,
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

    /// Root filesystem image: a preset name (resolved via the network's
    /// `vm-images` aggregate), or an item hash (hex or IPFS CID).
    #[arg(
        long,
        value_parser = parse_image_ref,
        required_unless_present = "interactive"
    )]
    pub image: Option<ImageRef>,

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

    /// Persistent volume: `name=N,mount=PATH,size=SIZE[,persistence=host|store][,comment=TEXT]`.
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

    /// UEFI firmware preset or hash for confidential VMs. Defaults to
    /// `defaults.firmware` from the `vm-images` aggregate when not set.
    #[arg(long, value_parser = parse_image_ref)]
    pub confidential_firmware: Option<ImageRef>,

    /// GPU model name (e.g. rtx4090, a100, l40s). Can be repeated for multiple GPUs.
    /// Use `aleph instance price --list-gpus` to list available models. The VM is
    /// sized at the GPU's minimum; pass `--size`, `--vcpus`, or `--memory` to request
    /// more. `--disk-size` is optional for GPU instances (defaults to the tier disk).
    #[arg(long)]
    pub gpu: Option<Vec<String>>,

    /// CRN node hash. Pins the instance to a specific compute node.
    #[arg(long)]
    pub crn_hash: Option<NodeHash>,

    /// Sign on behalf of another address (requires an authorization from that address).
    #[arg(long)]
    pub on_behalf_of: Option<String>,

    /// Prompt interactively for any values not provided on the command line.
    /// Prompts for node placement: let the scheduler choose automatically, or
    /// pick a specific CRN. Skipped if `--crn-hash` is already set.
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
    /// CRN to target: either a node hash or unique hash prefix or suffix
    /// (resolved to its URL via the scheduler; a suffix matches the shorthand
    /// IDs shown by `aleph instance list`) or a raw endpoint URL (anything
    /// containing `://`).
    ///
    /// Optional override: if omitted, the CRN is discovered via the scheduler.
    /// Pass this to bypass the scheduler's choice (e.g. when an instance is
    /// reported as `duplicated` and you want to target a specific node).
    #[arg(long, alias = "crn-url")]
    pub crn: Option<String>,

    /// VM instance item hash. Accepts a unique prefix (e.g. the 12-char hash
    /// shown by `aleph instance list`); the scheduler matches it server-side.
    pub vm_id: String,

    #[command(flatten)]
    pub signing: SigningArgs,
}

/// Start is separate because it's unauthenticated: signing args are still
/// required to construct the CrnClient but no auth headers are sent.
#[derive(Args)]
pub struct CrnStartArgs {
    /// CRN to target: either a node hash or unique hash prefix or suffix
    /// (resolved to its URL via the scheduler; a suffix matches the shorthand
    /// IDs shown by `aleph instance list`) or a raw endpoint URL (anything
    /// containing `://`).
    ///
    /// Optional override: if omitted, the CRN is discovered via the scheduler.
    /// Pass this to bypass the scheduler's choice (e.g. when an instance is
    /// reported as `duplicated` and you want to target a specific node).
    #[arg(long, alias = "crn-url")]
    pub crn: Option<String>,

    /// VM instance item hash. Accepts a unique prefix (e.g. the 12-char hash
    /// shown by `aleph instance list`); the scheduler matches it server-side.
    pub vm_id: String,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Subcommand)]
pub enum InstanceBackupCommand {
    /// Create a backup of a running VM. POSTs to the CRN and either returns
    /// immediately (`--follow` polls until completion).
    Create(InstanceBackupCreateArgs),
    /// Show the latest backup status for a VM.
    Info(InstanceBackupInfoArgs),
    /// Download a backup archive to disk.
    Download(InstanceBackupDownloadArgs),
    /// Delete a backup.
    Delete(InstanceBackupDeleteArgs),
    /// Restore a VM from a local QCOW2 file or an Aleph volume reference.
    Restore(InstanceBackupRestoreArgs),
}

#[derive(Args)]
pub struct InstanceBackupCreateArgs {
    /// VM instance item hash (accepts a unique prefix).
    pub vm_id: String,
    /// Include persistent volumes in the backup archive.
    #[arg(long)]
    pub include_volumes: bool,
    /// Skip the QEMU guest agent filesystem freeze. Faster, less consistent.
    #[arg(long)]
    pub skip_fsfreeze: bool,
    /// Poll the CRN until the backup completes (or times out after 30 min).
    #[arg(long)]
    pub follow: bool,
    /// CRN to target: a node hash or unique hash prefix or suffix (resolved via
    /// the scheduler) or a raw URL.
    /// Optional override; the CRN is normally discovered via the scheduler.
    #[arg(long, alias = "crn-url")]
    pub crn: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct InstanceBackupInfoArgs {
    pub vm_id: String,
    #[arg(long, alias = "crn-url")]
    pub crn: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct InstanceBackupDownloadArgs {
    /// Either a VM item hash (or prefix), or a presigned backup URL.
    pub vm_id_or_url: String,
    /// Output path. Defaults to ./backup-<vm_id_short>.tar.
    #[arg(short, long)]
    pub output: Option<std::path::PathBuf>,
    /// CRN to target: a node hash or unique hash prefix or suffix (resolved via
    /// the scheduler) or a raw URL (ignored when the positional arg is already a
    /// presigned URL).
    #[arg(long, alias = "crn-url")]
    pub crn: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct InstanceBackupDeleteArgs {
    pub vm_id: String,
    pub backup_id: String,
    #[arg(long, alias = "crn-url")]
    pub crn: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
#[command(group(
    clap::ArgGroup::new("restore_source")
        .required(true)
        .args(["file", "volume_ref"]),
))]
pub struct InstanceBackupRestoreArgs {
    pub vm_id: String,
    /// Local QCOW2 file to upload as the new rootfs.
    #[arg(short, long, group = "restore_source")]
    pub file: Option<std::path::PathBuf>,
    /// Item hash of an Aleph volume to restore from (server-side download).
    #[arg(long, group = "restore_source")]
    pub volume_ref: Option<String>,
    #[arg(long, alias = "crn-url")]
    pub crn: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct InstanceSshArgs {
    /// VM instance item hash. Accepts a unique prefix (e.g. the 12-char hash
    /// shown by `aleph instance list`); the scheduler matches it server-side.
    pub vm_id: String,

    /// CRN to target: a node hash (resolved via the scheduler) or a raw URL.
    /// If omitted, the dispatched CRN is discovered via the scheduler.
    #[arg(long, alias = "crn-url")]
    pub crn: Option<String>,

    /// SSH user to connect as.
    #[arg(long, default_value = "root")]
    pub user: String,

    /// SSH port.
    #[arg(long, default_value_t = 22)]
    pub port: u16,

    /// Path to an SSH private key (forwarded to `ssh -i`).
    #[arg(short = 'i', long)]
    pub identity: Option<std::path::PathBuf>,

    /// Extra arguments forwarded verbatim to `ssh` (after the host).
    /// Use a leading `--` to separate them from aleph-cli flags.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub ssh_args: Vec<String>,
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
    /// Register a new (empty) network, optionally with a scheduler URL and
    /// Ethereum settlement config
    Add(NetworkAddArgs),
    /// List all registered networks
    List,
    /// Remove a network and all its CCNs (refuses if it's the default)
    Remove(NetworkRemoveArgs),
    /// Show details of a network (defaults to the current one)
    Show(NetworkShowArgs),
    /// Set the default (current) network
    Use(NetworkUseArgs),
    /// Update a network's scheduler URL or Ethereum settlement config in place
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

    /// Aleph VM scheduler base URL. Defaults to the mainnet scheduler when
    /// not set explicitly; override later with `aleph config network set`.
    #[arg(long)]
    pub scheduler_url: Option<String>,

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

    /// Aleph VM scheduler base URL.
    #[arg(long)]
    pub scheduler_url: Option<String>,

    #[command(flatten)]
    pub ethereum: NetworkEthereumArgs,
}

#[derive(Subcommand)]
pub enum CreditCommand {
    /// Buy Aleph credits by transferring ALEPH, USDC, or ETH
    #[command(long_about = "\
Buy Aleph credits by transferring ALEPH, USDC, or native ETH from your EVM \
account. ALEPH and USDC are ERC20 transfers; ETH is a plain value transfer to \
the network's credit purchase address. Either way the protocol mints credits \
to your address once the transfer is confirmed.

`--amount` is in human-readable token units (decimals OK), not credits. \
1 USD purchases 1,000,000 credits. Use `aleph account balance` afterwards \
to confirm the credits arrived.

Examples:
  aleph credit buy --token aleph --amount 100
  aleph credit buy --token usdc  --amount 50.5
  aleph credit buy --token eth   --amount 0.05
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
    /// Native mainnet ETH (paid by a value transfer to the credit contract).
    Eth,
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

#[derive(Subcommand)]
pub enum TokenCommand {
    /// Swap ETH or USDC for ALEPH via CoW Swap or Uniswap
    #[command(long_about = "\
Swap native ETH or USDC for ALEPH, leaving the ALEPH in your wallet.
--amount is in human-readable units of the sell token.

Two venues (--venue):
  cow (default): gasless off-chain order filled by solvers; may expire
                 unfilled on a thin pair. Prints the order UID and exits;
                 check fill status on the explorer.
  uniswap:       immediate on-chain swap against Uniswap v3 pools; you pay
                 gas and the pool fee is embedded in the price. Useful when
                 a CoW order expired unfilled.

Examples:
  aleph token swap --sell-token usdc --amount 100
  aleph token swap --sell-token eth  --amount 0.5 --slippage 1.0
  aleph token swap --sell-token eth  --amount 0.5 --venue uniswap
  aleph token swap --sell-token usdc --amount 50 --yes")]
    Swap(TokenSwapArgs),
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum SwapTokenCli {
    Eth,
    Usdc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SwapVenueCli {
    Cow,
    Uniswap,
}

#[derive(Args)]
pub struct TokenSwapArgs {
    /// Token to sell (native ETH or USDC)
    #[arg(long, value_enum)]
    pub sell_token: SwapTokenCli,

    /// Swap venue. CoW submits a gasless off-chain order that solvers fill
    /// (may expire unfilled); Uniswap executes immediately on-chain and you
    /// pay gas.
    #[arg(long, value_enum, default_value = "cow")]
    pub venue: SwapVenueCli,

    /// Amount of the sell token to spend, in human-readable units
    #[arg(long)]
    pub amount: String,

    /// Max slippage percent for the minimum-received floor
    #[arg(long, default_value_t = 0.5)]
    pub slippage: f64,

    /// Where the bought ALEPH lands (defaults to the signer's address).
    /// Hex (0x...), local account name, or alias.
    #[arg(long)]
    pub receiver: Option<String>,

    /// Order validity window in seconds (CoW) or transaction deadline
    /// (Uniswap)
    #[arg(long, default_value_t = 1200)]
    pub valid_for: u32,

    /// Ethereum JSON-RPC endpoint - overrides the network's configured rpc_url
    #[arg(long)]
    pub rpc_url: Option<String>,

    /// Skip the confirmation prompt and submit immediately
    #[arg(short = 'y', long)]
    pub yes: bool,

    // NOTE: the inherited --dry-run help text says "Build and sign the message
    // but don't submit it." For swap that means quote-only (no on-chain
    // submission). Clap does not allow overriding help text of flattened
    // fields, so the mismatch is accepted as a known limitation.
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Subcommand)]
pub enum ProgramCommand {
    /// Deploy a new serverless program. Auto-uploads PATH as a STORE message,
    /// then publishes the PROGRAM message referencing it.
    Create(ProgramCreateArgs),
    /// Update the code of an existing program. Item hash is unchanged.
    Update(ProgramUpdateArgs),
    /// Forget a program (and its code STORE unless --keep-code).
    Delete(ProgramDeleteArgs),
    /// List programs owned by an address.
    List(ProgramListArgs),
    /// Recreate a program with persistent=true. New item hash.
    Persist(ProgramPersistArgs),
    /// Recreate a program with persistent=false. New item hash.
    Unpersist(ProgramPersistArgs),
    /// Fetch logs from one CRN running this program.
    Logs(ProgramLogsArgs),
    /// Show full information about a single program (creation time,
    /// ownership, per-ref freshness for code / runtime / data / volumes).
    Show(ProgramShowArgs),
}

#[derive(Args)]
pub struct ProgramCreateArgs {
    /// Source code path: directory, .zip, or .squashfs.
    pub path: PathBuf,
    /// Program entrypoint (e.g. `main:app` or `run.sh`).
    pub entrypoint: String,

    /// Friendly name (stored in metadata.name).
    #[arg(long)]
    pub name: Option<String>,

    /// Runtime: preset slug from the vm-images aggregate (e.g. `python312`) or a
    /// 64-char item hash. When omitted, resolves against the aggregate's
    /// `defaults.runtime`.
    #[arg(long, value_parser = parse_image_ref)]
    pub runtime: Option<ImageRef>,

    /// Resource preset slug (e.g. `1vcpu-2gb`). Mutually exclusive with --vcpus / --memory.
    #[arg(long, conflicts_with_all = ["vcpus", "memory"])]
    pub size: Option<String>,

    /// Number of virtual CPUs.
    #[arg(long)]
    pub vcpus: Option<u32>,

    /// Memory size (e.g. 2GB, 512MiB).
    #[arg(long, value_parser = parse_size_to_mib)]
    pub memory: Option<u64>,

    /// Idle timeout before shutdown (seconds). Default: 30.
    #[arg(long, default_value_t = 30)]
    pub timeout_seconds: u32,

    /// Allow internet access from the program.
    #[arg(long)]
    pub internet: bool,

    /// Make the program persistent (always running) instead of ephemeral.
    #[arg(long)]
    pub persistent: bool,

    /// Allow future updates with `aleph program update`. Sets allow_amend.
    #[arg(long)]
    pub updatable: bool,

    /// Environment variables (comma-separated KEY=value pairs).
    #[arg(long)]
    pub env_vars: Option<String>,

    /// Persistent volume: `name=N,mount=PATH,size=SIZE[,persistence=host|store][,comment=TEXT]`.
    /// Can be repeated.
    #[arg(long)]
    pub persistent_volume: Option<Vec<String>>,

    /// Ephemeral volume: `mount=PATH,size=SIZE`. Can be repeated.
    #[arg(long)]
    pub ephemeral_volume: Option<Vec<String>>,

    /// Immutable volume: `ref=HASH,mount=PATH[,use_latest=BOOL]`. Can be repeated.
    #[arg(long)]
    pub immutable_volume: Option<Vec<String>>,

    /// Storage engine for the code STORE message.
    #[arg(long, value_enum, default_value_t = StorageEngineCli::Storage)]
    pub storage_engine: StorageEngineCli,

    /// Payment type for both the STORE and the PROGRAM messages.
    /// PROGRAM does not yet support credit payments, so defaults to `hold`.
    #[arg(long, value_enum, default_value_t = PaymentTypeCli::Hold)]
    pub payment_type: PaymentTypeCli,

    /// Channel name (default: ALEPH-CLOUDSOLUTIONS).
    #[arg(long)]
    pub channel: Option<String>,

    /// Sign on behalf of another address (requires authorization from that address).
    #[arg(long)]
    pub on_behalf_of: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct ProgramUpdateArgs {
    /// Item hash of the program to update.
    pub item_hash: ItemHash,
    /// New source code path: directory, .zip, or .squashfs.
    pub path: PathBuf,

    /// Channel for the new code STORE (default: original code's channel).
    #[arg(long)]
    pub channel: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct ProgramDeleteArgs {
    /// Item hash of the program to forget.
    pub item_hash: ItemHash,

    /// Reason for deletion (recorded on the FORGET message).
    #[arg(long, default_value = "User deletion")]
    pub reason: String,

    /// Keep the code STORE intact (do not also forget it).
    #[arg(long)]
    pub keep_code: bool,

    /// Skip the confirmation prompt.
    #[arg(short = 'y', long)]
    pub yes: bool,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct ProgramListArgs {
    /// Address whose programs to list. Hex, local account name, or alias.
    /// Defaults to the current default account.
    #[arg(long)]
    pub address: Option<String>,
}

#[derive(Args)]
pub struct ProgramShowArgs {
    /// Item hash of the program to inspect.
    pub item_hash: ItemHash,
}

#[derive(Args)]
pub struct ProgramPersistArgs {
    /// Item hash of the program to convert.
    pub item_hash: ItemHash,

    /// Keep the previous program message instead of forgetting it.
    #[arg(long)]
    pub keep_prev: bool,

    /// Skip the confirmation prompt for the forget step.
    #[arg(short = 'y', long)]
    pub yes: bool,

    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct ProgramLogsArgs {
    /// Item hash of the program.
    pub item_hash: ItemHash,

    /// CRN URL (e.g. `https://crn.example.com`).
    #[arg(long)]
    pub crn: Url,

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

    /// Hex addresses bypass the account store, so this test does not need any
    /// store fixture. It pins the contract: `--addresses` / `--owners` accept
    /// hex strings without touching disk.
    #[test]
    fn message_list_accepts_hex_addresses() {
        let cli = Cli::try_parse_from([
            "aleph",
            "message",
            "list",
            "--addresses",
            "0xABCD1234,0xEF560000",
            "--owners",
            "0xDEADBEEF",
        ])
        .expect("clap parse");
        match cli.command {
            Commands::Message {
                command: MessageCommand::List(args),
            } => {
                let addresses = args.filter.addresses.unwrap();
                assert_eq!(addresses.len(), 2);
                assert_eq!(addresses[0].to_string(), "0xABCD1234");
                let owners = args.filter.owners.unwrap();
                assert_eq!(owners.len(), 1);
                assert_eq!(owners[0].to_string(), "0xDEADBEEF");
            }
            _ => panic!("expected message list"),
        }
    }

    #[test]
    fn post_list_accepts_hex_addresses() {
        let cli =
            Cli::try_parse_from(["aleph", "post", "list", "--addresses", "0xABCD1234"]).unwrap();
        match cli.command {
            Commands::Post {
                command: PostCommand::List(args),
            } => {
                let addresses = args.filter.addresses.unwrap();
                assert_eq!(addresses.len(), 1);
                assert_eq!(addresses[0].to_string(), "0xABCD1234");
            }
            _ => panic!("expected post list"),
        }
    }
}

#[cfg(test)]
mod port_forward_args_tests {
    use super::*;

    #[test]
    fn list_accepts_address_and_vm_id() {
        let cli = Cli::try_parse_from([
            "aleph",
            "instance",
            "port-forward",
            "list",
            "--address",
            "0xABCD1234",
            "--vm-id",
            "1111111111111111111111111111111111111111111111111111111111111111",
        ])
        .expect("clap parse");
        match cli.command {
            Commands::Instance {
                command:
                    InstanceCommand::PortForward {
                        command: PortForwardCommand::List(args),
                    },
            } => {
                assert_eq!(args.address.as_deref(), Some("0xABCD1234"));
                assert!(args.vm_id.is_some());
            }
            _ => panic!("expected port-forward list"),
        }
    }

    #[test]
    fn create_rejects_port_zero() {
        match Cli::try_parse_from([
            "aleph",
            "instance",
            "port-forward",
            "create",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "0",
        ]) {
            Ok(_) => panic!("expected parse error for port 0"),
            Err(e) => assert_eq!(e.kind(), clap::error::ErrorKind::ValueValidation),
        }
    }

    #[test]
    fn create_rejects_port_above_max() {
        let result = Cli::try_parse_from([
            "aleph",
            "instance",
            "port-forward",
            "create",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "65536",
        ]);
        match result {
            Ok(_) => panic!("expected parse error for port 65536"),
            Err(e) => assert_eq!(e.kind(), clap::error::ErrorKind::ValueValidation),
        }
    }

    #[test]
    fn create_defaults_tcp_true_udp_false() {
        let cli = Cli::try_parse_from([
            "aleph",
            "instance",
            "port-forward",
            "create",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "443",
        ])
        .expect("clap parse");
        match cli.command {
            Commands::Instance {
                command:
                    InstanceCommand::PortForward {
                        command: PortForwardCommand::Create(args),
                    },
            } => {
                assert_eq!(args.port, 443);
                assert!(args.tcp);
                assert!(!args.udp);
            }
            _ => panic!("expected port-forward create"),
        }
    }

    #[test]
    fn create_accepts_explicit_udp_only() {
        let cli = Cli::try_parse_from([
            "aleph",
            "instance",
            "port-forward",
            "create",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "5353",
            "--tcp",
            "false",
            "--udp",
            "true",
        ])
        .expect("clap parse");
        match cli.command {
            Commands::Instance {
                command:
                    InstanceCommand::PortForward {
                        command: PortForwardCommand::Create(args),
                    },
            } => {
                assert!(!args.tcp);
                assert!(args.udp);
            }
            _ => panic!("expected port-forward create"),
        }
    }

    #[test]
    fn delete_accepts_optional_port() {
        let cli = Cli::try_parse_from([
            "aleph",
            "instance",
            "port-forward",
            "delete",
            "1111111111111111111111111111111111111111111111111111111111111111",
        ])
        .expect("clap parse");
        match cli.command {
            Commands::Instance {
                command:
                    InstanceCommand::PortForward {
                        command: PortForwardCommand::Delete(args),
                    },
            } => {
                assert!(args.port.is_none());
            }
            _ => panic!("expected port-forward delete"),
        }
    }

    #[test]
    fn create_accepts_hash_prefix() {
        let cli = Cli::try_parse_from([
            "aleph",
            "instance",
            "port-forward",
            "create",
            "a41fb91c3e68",
            "443",
        ])
        .expect("clap parse");
        match cli.command {
            Commands::Instance {
                command:
                    InstanceCommand::PortForward {
                        command: PortForwardCommand::Create(args),
                    },
            } => {
                assert_eq!(args.vm_id, "a41fb91c3e68");
                assert_eq!(args.port, 443);
            }
            _ => panic!("expected port-forward create"),
        }
    }

    #[test]
    fn pfw_alias_resolves_to_port_forward() {
        let cli = Cli::try_parse_from(["aleph", "instance", "pfw", "list"]).expect("clap parse");
        assert!(matches!(
            cli.command,
            Commands::Instance {
                command: InstanceCommand::PortForward {
                    command: PortForwardCommand::List(_),
                },
            }
        ));
    }
}

#[derive(Subcommand)]
pub enum DomainCommand {
    /// List all domains for an account
    List(DomainListArgs),
    /// Add (or update) a domain entry pointing at a target
    Add(DomainAddArgs),
    /// Re-point an existing domain to a different target
    Attach(DomainAttachArgs),
    /// Clear a domain's target (entry kept, message_id emptied)
    Detach(DomainDetachArgs),
    /// Remove a domain entry (soft-delete: sets to null)
    Remove(DomainRemoveArgs),
}

#[derive(Args)]
pub struct DomainListArgs {
    /// Inspect another address's domains.
    #[arg(long)]
    pub address: Option<String>,
}

#[derive(Args)]
pub struct DomainAddArgs {
    /// Domain name (e.g. site.example.com).
    pub domain: String,
    /// Target: a website name (resolved against your `websites` aggregate)
    /// or a raw message hash.
    #[arg(long)]
    pub target: String,
    /// Target type.
    #[arg(long, value_enum, default_value = "ipfs")]
    pub kind: DomainKindCli,
    /// Catch-all path for IPFS sites (default: /404.html).
    #[arg(long)]
    pub catch_all_path: Option<String>,
    /// Overwrite existing entry without erroring.
    #[arg(long)]
    pub force: bool,
    /// Channel name.
    #[arg(long)]
    pub channel: Option<String>,
    /// Sign on behalf of another address (requires an authorization from that
    /// address). The aggregate write and any website-name lookup target the
    /// owner.
    #[arg(long)]
    pub on_behalf_of: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct DomainAttachArgs {
    pub domain: String,
    /// Website name or message hash.
    #[arg(long = "to")]
    pub target: String,
    #[arg(long)]
    pub channel: Option<String>,
    /// Sign on behalf of another address (requires an authorization from that
    /// address). The aggregate write and any website-name lookup target the
    /// owner.
    #[arg(long)]
    pub on_behalf_of: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct DomainDetachArgs {
    pub domain: String,
    /// Skip TTY confirmation.
    #[arg(long)]
    pub yes: bool,
    #[arg(long)]
    pub channel: Option<String>,
    /// Sign on behalf of another address (requires an authorization from that
    /// address). The aggregate write targets the owner.
    #[arg(long)]
    pub on_behalf_of: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct DomainRemoveArgs {
    pub domain: String,
    #[arg(long)]
    pub yes: bool,
    #[arg(long)]
    pub channel: Option<String>,
    /// Sign on behalf of another address (requires an authorization from that
    /// address). The soft-delete aggregate write targets the owner.
    #[arg(long)]
    pub on_behalf_of: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum DomainKindCli {
    Ipfs,
    Program,
    Instance,
}

#[derive(Subcommand)]
pub enum WebsiteCommand {
    /// List websites for an account
    List(WebsiteListArgs),
    /// Show details about one website
    Show(WebsiteShowArgs),
    /// Deploy a new website from a folder
    Deploy(WebsiteDeployArgs),
    /// Update an existing website with a new folder
    Update(WebsiteUpdateArgs),
    /// Soft-delete a website (sets aggregate entry to null)
    Delete(WebsiteDeleteArgs),
}

#[derive(Args)]
pub struct WebsiteListArgs {
    #[arg(long)]
    pub address: Option<String>,
}

#[derive(Args)]
pub struct WebsiteShowArgs {
    pub name: String,
    #[arg(long)]
    pub address: Option<String>,
}

#[derive(Args)]
pub struct WebsiteDeployArgs {
    /// Website name (used as the aggregate key; lowercase-with-dashes recommended).
    pub name: String,
    /// Path to the folder containing the static site.
    pub path: std::path::PathBuf,
    #[arg(long, value_enum, default_value = "none")]
    pub framework: FrameworkCli,
    #[arg(long)]
    pub tag: Vec<String>,
    /// Override payment chain (defaults to signing account's chain).
    #[arg(long)]
    pub payment_chain: Option<String>,
    /// Payment type for the STORE message and the websites aggregate entry.
    /// `credit` (default) consumes credits; `hold` requires locked stake.
    #[arg(long, value_enum)]
    pub payment_type: Option<PaymentTypeCli>,
    /// Attach a domain (repeatable). Each domain creates/updates a `domains` entry.
    #[arg(long)]
    pub domain: Vec<String>,
    /// Skip upload+STORE; reuse an existing volume by item_hash.
    #[arg(long)]
    pub volume_id: Option<String>,
    #[arg(long)]
    pub channel: Option<String>,
    /// Sign on behalf of another address (requires an authorization from that
    /// address). The STORE message and the `websites` / `domains` aggregate
    /// entries are written under the owner, not the signer.
    #[arg(long)]
    pub on_behalf_of: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct WebsiteUpdateArgs {
    pub name: String,
    pub path: std::path::PathBuf,
    #[arg(long, value_enum)]
    pub framework: Option<FrameworkCli>,
    #[arg(long)]
    pub tag: Option<Vec<String>>,
    #[arg(long)]
    pub domain: Vec<String>,
    /// Leave all attached domains pointing at the previous volume_id.
    #[arg(long)]
    pub skip_domain_update: bool,
    #[arg(long)]
    pub volume_id: Option<String>,
    /// Skip aggregate write if folder content + version already match.
    #[arg(long)]
    pub idempotent: bool,
    #[arg(long)]
    pub channel: Option<String>,
    /// Payment type for the new STORE message. `credit` (default) consumes
    /// credits; `hold` requires locked stake. The existing `websites`
    /// aggregate payment metadata is preserved verbatim.
    #[arg(long, value_enum)]
    pub payment_type: Option<PaymentTypeCli>,
    /// Sign on behalf of another address (requires an authorization from that
    /// address). All aggregate writes and the STORE upload target the owner.
    #[arg(long)]
    pub on_behalf_of: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct WebsiteDeleteArgs {
    pub name: String,
    #[arg(long)]
    pub yes: bool,
    #[arg(long)]
    pub channel: Option<String>,
    /// Sign on behalf of another address (requires an authorization from that
    /// address). The soft-delete aggregate write targets the owner.
    #[arg(long)]
    pub on_behalf_of: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[cfg(test)]
mod instance_delete_args_tests {
    use super::*;

    const HASH: &str = "a41fb91c3e68370759b72338dd1947f18e2ed883837aec5dc731d5f427f90564";

    #[test]
    fn parses_minimal_invocation() {
        let cli = Cli::try_parse_from(["aleph", "instance", "delete", HASH]).expect("clap parse");
        match cli.command {
            Commands::Instance {
                command: InstanceCommand::Delete(args),
            } => {
                assert_eq!(args.vm_id.to_string(), HASH);
                assert_eq!(args.reason, "User deletion");
                assert!(!args.yes);
                assert!(!args.signing.dry_run);
            }
            _ => panic!("expected instance delete"),
        }
    }

    #[test]
    fn accepts_reason_and_yes() {
        let cli = Cli::try_parse_from([
            "aleph",
            "instance",
            "delete",
            HASH,
            "--reason",
            "decommission",
            "-y",
        ])
        .expect("clap parse");
        match cli.command {
            Commands::Instance {
                command: InstanceCommand::Delete(args),
            } => {
                assert_eq!(args.reason, "decommission");
                assert!(args.yes);
            }
            _ => panic!("expected instance delete"),
        }
    }

    #[test]
    fn rejects_missing_vm_id() {
        let result = Cli::try_parse_from(["aleph", "instance", "delete"]);
        assert!(result.is_err());
    }

    #[test]
    fn accepts_hash_prefix() {
        let cli = Cli::try_parse_from(["aleph", "instance", "delete", "a41fb91c3e68"])
            .expect("clap parse");
        match cli.command {
            Commands::Instance {
                command: InstanceCommand::Delete(args),
            } => {
                assert_eq!(args.vm_id, "a41fb91c3e68");
            }
            _ => panic!("expected instance delete"),
        }
    }

    #[test]
    fn accepts_dry_run_flag() {
        let cli = Cli::try_parse_from(["aleph", "instance", "delete", HASH, "--dry-run"])
            .expect("clap parse");
        match cli.command {
            Commands::Instance {
                command: InstanceCommand::Delete(args),
            } => {
                assert!(args.signing.dry_run);
            }
            _ => panic!("expected instance delete"),
        }
    }
}

#[derive(Subcommand)]
pub enum PortForwardCommand {
    /// List configured port forwards for an address (optionally for one VM).
    List(PortForwardListArgs),
    /// Create a port forward for `<VM_ID>` on `<PORT>`.
    Create(PortForwardCreateArgs),
    /// Update an existing port forward's TCP/UDP flags.
    Update(PortForwardUpdateArgs),
    /// Delete a port forward (a single port, or all ports if `--port` is omitted).
    Delete(PortForwardDeleteArgs),
    /// Ask the CRN running this VM to re-read the aggregate immediately.
    Refresh(PortForwardRefreshArgs),
}

#[derive(Args)]
pub struct PortForwardListArgs {
    /// Address to inspect (hex, account name, or alias).
    /// Defaults to the current default account's address.
    #[arg(long)]
    pub address: Option<String>,

    /// Restrict the list to a single VM. Accepts a unique hash prefix
    /// (e.g. the 12-char hash shown by `aleph instance list`).
    #[arg(long)]
    pub vm_id: Option<String>,
}

#[derive(Args)]
pub struct PortForwardCreateArgs {
    /// Item hash of the target VM / program / IPFS website. Accepts a unique
    /// prefix (e.g. the 12-char hash shown by `aleph instance list`).
    pub vm_id: String,
    /// Port number to forward (1..=65535).
    #[arg(value_parser = clap::value_parser!(u16).range(1..=65535))]
    pub port: u16,
    /// Allow TCP for this port. (Use `--tcp false` to disable.)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub tcp: bool,
    /// Allow UDP for this port. (Use `--udp true` to enable.)
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    pub udp: bool,
    /// Channel for the AGGREGATE message.
    #[arg(long)]
    pub channel: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct PortForwardUpdateArgs {
    /// Item hash of the target VM / program / IPFS website. Accepts a unique
    /// prefix (e.g. the 12-char hash shown by `aleph instance list`).
    pub vm_id: String,
    /// Port number to forward (1..=65535).
    #[arg(value_parser = clap::value_parser!(u16).range(1..=65535))]
    pub port: u16,
    /// Allow TCP for this port. (Use `--tcp false` to disable.)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub tcp: bool,
    /// Allow UDP for this port. (Use `--udp true` to enable.)
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    pub udp: bool,
    /// Channel for the AGGREGATE message.
    #[arg(long)]
    pub channel: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct PortForwardDeleteArgs {
    /// Item hash of the target VM / program / IPFS website. Accepts a unique
    /// prefix (e.g. the 12-char hash shown by `aleph instance list`).
    pub vm_id: String,
    /// If set, only delete this port. Otherwise delete the whole entry for `vm_id`.
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..=65535))]
    pub port: Option<u16>,
    /// Skip the confirmation prompt.
    #[arg(short = 'y', long)]
    pub yes: bool,
    /// Channel for the AGGREGATE message.
    #[arg(long)]
    pub channel: Option<String>,
    #[command(flatten)]
    pub signing: SigningArgs,
}

#[derive(Args)]
pub struct PortForwardRefreshArgs {
    /// Item hash of the target VM. Accepts a unique prefix.
    pub vm_id: String,
    #[command(flatten)]
    pub identity: IdentityArgs,
}

#[derive(Subcommand)]
pub enum ConfidentialCommand {
    /// Initialize a confidential session (fetch cert, verify chain, derive session keys, post to CRN).
    InitSession(ConfidentialInitSessionArgs),
    /// Validate the VM launch measurement and inject the disk-decryption secret.
    Start(ConfidentialStartArgs),
    /// All-in-one: create (optional), allocate, init session, then start.
    Create(ConfidentialCreateArgs),
}

#[derive(Args)]
pub struct ConfidentialInitSessionArgs {
    /// VM item-hash. Accepts a unique prefix (e.g. the 12-char hash shown by
    /// `aleph instance list`); the scheduler matches it server-side.
    pub vm_id: String,
    /// CRN to target: a node hash (resolved via the scheduler) or a raw URL.
    /// Overrides the CRN otherwise discovered via the scheduler.
    #[arg(long, alias = "crn-url")]
    pub crn: Option<String>,
    #[command(flatten)]
    pub identity: IdentityArgs,
    /// SEV policy mask.
    #[arg(long, default_value_t = 0x1)]
    pub policy: u32,
    /// Reuse existing session files if present (skip the overwrite prompt).
    #[arg(long)]
    pub keep_session: bool,
    /// Enable debug logging.
    #[arg(long)]
    pub debug: bool,
}

#[derive(Args)]
pub struct ConfidentialStartArgs {
    /// VM item-hash. Accepts a unique prefix.
    pub vm_id: String,
    /// CRN to target: a node hash (resolved via the scheduler) or a raw URL.
    /// Overrides the CRN otherwise discovered via the scheduler.
    #[arg(long, alias = "crn-url")]
    pub crn: Option<String>,
    #[command(flatten)]
    pub identity: IdentityArgs,
    /// Expected OVMF firmware hash (hex). Defaults to the active value from the
    /// vm-images aggregate when omitted.
    #[arg(long)]
    pub firmware_hash: Option<String>,
    /// Path to a local OVMF firmware blob; computes its SHA-256 and overrides
    /// `--firmware-hash`.
    #[arg(long)]
    pub firmware_file: Option<std::path::PathBuf>,
    /// VM disk-decryption secret. Prompts interactively if absent.
    ///
    /// Prefer the interactive prompt: a value passed here is visible to other
    /// users via the process table (`ps aux`) and is recorded in shell history.
    /// Only pass `--secret` in trusted, non-interactive contexts.
    #[arg(long)]
    pub secret: Option<String>,
    /// Emit JSON-formatted success/failure.
    #[arg(long)]
    pub json: bool,
    /// Enable debug logging.
    #[arg(long)]
    pub debug: bool,
}

#[derive(Args)]
pub struct ConfidentialCreateArgs {
    /// Existing VM hash. If omitted, runs `instance create --confidential ...`
    /// first to allocate a fresh VM.
    pub vm_id: Option<String>,
    /// CRN to target: a node hash (resolved via the scheduler) or a raw URL.
    /// Overrides the CRN otherwise discovered via the scheduler.
    #[arg(long, alias = "crn-url")]
    pub crn: Option<String>,
    #[command(flatten)]
    pub identity: IdentityArgs,
    /// SEV policy mask.
    #[arg(long, default_value_t = 0x1)]
    pub policy: u32,
    /// Reuse existing session files if present.
    #[arg(long)]
    pub keep_session: bool,
    /// Expected OVMF firmware hash (hex).
    #[arg(long)]
    pub firmware_hash: Option<String>,
    /// Path to a local OVMF blob.
    #[arg(long)]
    pub firmware_file: Option<std::path::PathBuf>,
    /// VM disk-decryption secret.
    #[arg(long)]
    pub secret: Option<String>,
    // NB: forwarded `instance create` flags get flattened in Task 17.
    /// Enable debug logging.
    #[arg(long)]
    pub debug: bool,
}

#[cfg(test)]
mod confidential_parser_tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("parse")
    }

    #[test]
    fn init_session_accepts_hash_prefix() {
        let cli = parse(&[
            "aleph",
            "instance",
            "confidential",
            "init-session",
            "236328f6",
        ]);
        let Commands::Instance {
            command: InstanceCommand::Confidential(ConfidentialCommand::InitSession(a)),
        } = cli.command
        else {
            panic!("wrong subcommand");
        };
        assert_eq!(a.vm_id, "236328f6");
        assert_eq!(a.policy, 0x1);
        assert!(!a.keep_session);
    }

    #[test]
    fn start_accepts_secret_and_json() {
        let cli = parse(&[
            "aleph",
            "instance",
            "confidential",
            "start",
            "236328f6",
            "--secret",
            "hunter2",
            "--json",
        ]);
        let Commands::Instance {
            command: InstanceCommand::Confidential(ConfidentialCommand::Start(a)),
        } = cli.command
        else {
            panic!("wrong subcommand");
        };
        assert_eq!(a.secret.as_deref(), Some("hunter2"));
        assert!(a.json);
    }

    #[test]
    fn create_accepts_no_positional() {
        let cli = parse(&["aleph", "instance", "confidential", "create"]);
        let Commands::Instance {
            command: InstanceCommand::Confidential(ConfidentialCommand::Create(a)),
        } = cli.command
        else {
            panic!("wrong subcommand");
        };
        assert_eq!(a.vm_id, None);
    }
}
