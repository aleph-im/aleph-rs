use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "heph", about = "Lightweight local Aleph CCN for testing")]
pub struct HephConfig {
    /// Port to listen on
    #[arg(short, long, default_value = "4024")]
    pub port: u16,

    /// Host to bind to
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Data directory (default: temp dir)
    #[arg(short, long)]
    pub data_dir: Option<PathBuf>,

    /// Pre-seed these addresses with credits (comma-separated)
    #[arg(long, value_delimiter = ',')]
    pub accounts: Vec<String>,

    /// Initial credit balance for pre-seeded accounts
    #[arg(long, default_value = "1000000000")]
    pub balance: i64,

    /// Log level
    #[arg(long, default_value = "info")]
    pub log_level: String,
}
