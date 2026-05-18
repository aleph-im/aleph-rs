//! Configuration loading. Mirrors `aleph/config.py`.
//!
//! Defaults are wired into a `Settings` struct; YAML overlays load through `config-rs`.

mod defaults;
mod settings;

pub use settings::{
    AlephSettings, AuthSettings, BalancesSettings, BscSettings, CacheSettings, CorechannelSettings,
    CreditBalancesSettings, CronSettings, EthereumSettings, IpfsPinningSettings, IpfsSettings,
    JobsSettings, LoggingSettings, Nuls2Settings, P2pSettings, PendingMessagesJobSettings,
    PendingTxsJobSettings, PerfSettings, PostgresSettings, RabbitmqSettings, RedisSettings,
    SentrySettings, Settings, StorageSettings, TezosSettings, TtlSettings, WebsocketSettings,
};

use std::path::Path;

use crate::{AlephError, AlephResult};

/// Load a `Settings` from an optional YAML file, layered over the defaults.
pub fn load(path: Option<&Path>) -> AlephResult<Settings> {
    let mut builder = ::config::Config::builder()
        // Source defaults from the typed Settings::default() snapshot.
        .add_source(::config::Config::try_from(&Settings::default()).map_err(cfg_err)?);

    if let Some(p) = path {
        builder = builder.add_source(::config::File::from(p).required(true));
    }
    builder = builder.add_source(::config::Environment::with_prefix("ALEPH").separator("__"));

    let cfg = builder.build().map_err(cfg_err)?;
    cfg.try_deserialize::<Settings>().map_err(cfg_err)
}

fn cfg_err(e: ::config::ConfigError) -> AlephError {
    AlephError::Config(e.to_string())
}
