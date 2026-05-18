//! aleph-ccn entry point. Mirrors `aleph/commands.py` and `aleph/cli/args.py`.

use std::path::PathBuf;

use aleph_ccn::AlephResult;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "aleph-ccn",
    version,
    about = "Aleph.im Core Channel Node (Rust)"
)]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(short = 'c', long = "config", env = "ALEPH_CONFIG")]
    config: Option<PathBuf>,

    /// HTTP API bind port (overrides config).
    #[arg(short = 'p', long = "port")]
    port: Option<u16>,

    /// HTTP API bind host (overrides config).
    #[arg(short = 'b', long = "bind")]
    host: Option<String>,

    /// Verbose logging (INFO).
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,

    /// Very verbose logging (DEBUG).
    #[arg(long = "very-verbose")]
    very_verbose: bool,

    /// Skip writing newly received messages to disk.
    #[arg(long = "no-commit")]
    no_commit: bool,

    /// Don't start background jobs (API-only mode).
    #[arg(long = "no-jobs")]
    no_jobs: bool,

    /// Disable Sentry error tracking even if a DSN is configured.
    #[arg(long = "disable-sentry")]
    disable_sentry: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the full Core Channel Node (default).
    Run,
    /// Apply database migrations and exit.
    Migrate,
    /// Generate a node key, save it under `--key-dir`, and exit.
    GenKeys {
        /// Path to the key directory.
        #[arg(short = 'k', long = "key-dir", default_value = "keys")]
        key_dir: PathBuf,
        /// Also print the generated private key to stdout.
        #[arg(long = "print-key")]
        print_key: bool,
    },
    /// Print effective configuration as YAML and exit.
    PrintConfig,
}

#[tokio::main]
async fn main() -> AlephResult<()> {
    let cli = Cli::parse();

    let log_level = if cli.very_verbose {
        "debug"
    } else if cli.verbose {
        "info"
    } else {
        "warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level)),
        )
        .init();

    let mut cfg = aleph_ccn::config::load(cli.config.as_deref())?;
    if let Some(p) = cli.port {
        cfg.p2p.http_port = p;
    }
    if let Some(h) = cli.host {
        // host is wired through the runtime call below
        // SAFETY: edition 2024 marks set_var unsafe because env mutation is
        // unsound when other threads read env concurrently; we are single-
        // threaded at this point in startup.
        unsafe {
            std::env::set_var("ALEPH_BIND_HOST", h);
        }
    }

    match cli.command.unwrap_or(Command::Run) {
        Command::Run => {
            tracing::info!("starting aleph-ccn v{}", aleph_ccn::VERSION);
            aleph_ccn::run(cfg).await?;
        }
        Command::Migrate => {
            let pool = aleph_ccn::db::connect(&cfg.postgres).await?;
            aleph_ccn::db::migrate(&pool).await?;
            tracing::info!("migrations applied");
        }
        Command::GenKeys { key_dir, print_key } => {
            let key_pair = aleph_ccn::services::keys::generate_keypair(print_key)?;
            aleph_ccn::services::keys::save_keys(&key_pair, &key_dir)?;
            tracing::info!("wrote node key material to {}", key_dir.display());
        }
        Command::PrintConfig => {
            println!("{}", serde_yaml::to_string(&cfg)?);
        }
    }

    Ok(())
}
