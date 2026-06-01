//! Typed mirror of `aleph/config.py` defaults.

use serde::{Deserialize, Serialize};

use super::defaults::{
    DEFAULT_MAX_FILE_SIZE, DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    DEFAULT_MAX_UPLOAD_CAR_SIZE, DEFAULT_MAX_UPLOAD_FILE_SIZE,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    #[serde(default)]
    pub logging: LoggingSettings,
    #[serde(default)]
    pub aleph: AlephSettings,
    #[serde(default)]
    pub p2p: P2pSettings,
    #[serde(default)]
    pub storage: StorageSettings,
    #[serde(default)]
    pub nuls2: Nuls2Settings,
    #[serde(default)]
    pub bsc: BscSettings,
    #[serde(default)]
    pub ethereum: EthereumSettings,
    #[serde(default)]
    pub tezos: TezosSettings,
    #[serde(default)]
    pub postgres: PostgresSettings,
    #[serde(default)]
    pub ipfs: IpfsSettings,
    #[serde(default)]
    pub rabbitmq: RabbitmqSettings,
    #[serde(default)]
    pub redis: RedisSettings,
    #[serde(default)]
    pub sentry: SentrySettings,
    #[serde(default)]
    pub perf: PerfSettings,
    #[serde(default)]
    pub websocket: WebsocketSettings,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            logging: LoggingSettings::default(),
            aleph: AlephSettings::default(),
            p2p: P2pSettings::default(),
            storage: StorageSettings::default(),
            nuls2: Nuls2Settings::default(),
            bsc: BscSettings::default(),
            ethereum: EthereumSettings::default(),
            tezos: TezosSettings::default(),
            postgres: PostgresSettings::default(),
            ipfs: IpfsSettings::default(),
            rabbitmq: RabbitmqSettings::default(),
            redis: RedisSettings::default(),
            sentry: SentrySettings::default(),
            perf: PerfSettings::default(),
            websocket: WebsocketSettings::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingSettings {
    pub level: u32,
    pub max_log_file_size: u64,
}
impl Default for LoggingSettings {
    fn default() -> Self {
        Self {
            level: 30, // logging.WARNING
            max_log_file_size: 50_000_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlephSettings {
    pub queue_topic: String,
    pub reference_node_url: Option<String>,
    pub indexer_url: String,
    pub auth: AuthSettings,
    pub corechannel: CorechannelSettings,
    pub balances: BalancesSettings,
    pub credit_balances: CreditBalancesSettings,
    #[serde(default)]
    pub scoring: ScoringSettings,
    pub jobs: JobsSettings,
    pub cache: CacheSettings,
}
impl Default for AlephSettings {
    fn default() -> Self {
        Self {
            queue_topic: "ALEPH-TEST".into(),
            reference_node_url: None,
            indexer_url: "https://multichain.api.aleph.cloud".into(),
            auth: AuthSettings::default(),
            corechannel: CorechannelSettings::default(),
            balances: BalancesSettings::default(),
            credit_balances: CreditBalancesSettings::default(),
            scoring: ScoringSettings::default(),
            jobs: JobsSettings::default(),
            cache: CacheSettings::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSettings {
    pub public_key: String,
    pub max_token_age: u64,
}
impl Default for AuthSettings {
    fn default() -> Self {
        Self {
            public_key: "0209fe82e08ec3c5c3ee4904fa147a11d49c7130579066c8a452d279d539959389".into(),
            max_token_age: 300,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorechannelSettings {
    pub address: String,
    pub cache_ttl: u64,
}
impl Default for CorechannelSettings {
    fn default() -> Self {
        Self {
            address: "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10".into(),
            cache_ttl: 300,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalancesSettings {
    pub addresses: Vec<String>,
    pub post_type: String,
}
impl Default for BalancesSettings {
    fn default() -> Self {
        Self {
            addresses: vec![
                "0xB34f25f2c935bCA437C061547eA12851d719dEFb".into(),
                "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10".into(),
            ],
            post_type: "balances-update".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditBalancesSettings {
    pub addresses: Vec<String>,
    pub post_types: Vec<String>,
    pub channels: Vec<String>,
}
impl Default for CreditBalancesSettings {
    fn default() -> Self {
        Self {
            addresses: vec!["0x2E4454fAD1906c0Ce6e45cBFA05cE898Ac3AC1dC".into()],
            post_types: vec![
                "aleph_credit_distribution".into(),
                "aleph_credit_transfer".into(),
                "aleph_credit_expense".into(),
            ],
            channels: vec!["ALEPH_CREDIT".into()],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoringSettings {
    /// Addresses allowed to publish node scoring metrics.
    pub addresses: Vec<String>,
    /// Channel scoring messages are published on.
    pub channel: String,
    /// POST message type that carries the node metrics payload.
    pub metrics_post_type: String,
    /// Retention horizon for `crn_metrics` / `ccn_metrics`. Partitions whose
    /// upper bound is older than this are detached and dropped by the
    /// `metrics_partition` cron job.
    pub retention_months: i32,
    /// How many months ahead of "now" to keep partitions pre-created. Guards
    /// against incoming scoring posts falling into the DEFAULT catch-all
    /// partition.
    pub partition_lookahead_months: i32,
}
impl Default for ScoringSettings {
    fn default() -> Self {
        Self {
            addresses: vec!["0x4D52380D3191274a04846c89c069E6C3F2Ed94e4".into()],
            channel: "aleph-scoring".into(),
            metrics_post_type: "aleph-network-metrics".into(),
            retention_months: 12,
            partition_lookahead_months: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsSettings {
    pub pending_messages: PendingMessagesJobSettings,
    pub pending_txs: PendingTxsJobSettings,
    pub max_unconfirmed_messages: u64,
    pub cron: CronSettings,
}
impl Default for JobsSettings {
    fn default() -> Self {
        Self {
            pending_messages: PendingMessagesJobSettings::default(),
            pending_txs: PendingTxsJobSettings::default(),
            max_unconfirmed_messages: 10_000,
            cron: CronSettings::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingMessagesJobSettings {
    pub max_retries: u32,
    pub max_concurrency: u32,
    pub idle_timeout: u64,
}
impl Default for PendingMessagesJobSettings {
    fn default() -> Self {
        Self {
            max_retries: 10,
            max_concurrency: 10,
            idle_timeout: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingTxsJobSettings {
    pub max_concurrency: u32,
}
impl Default for PendingTxsJobSettings {
    fn default() -> Self {
        Self {
            max_concurrency: 20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronSettings {
    pub period: f64,
}
impl Default for CronSettings {
    fn default() -> Self {
        Self { period: 0.5 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSettings {
    pub ttl: TtlSettings,
}
impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            ttl: TtlSettings::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlSettings {
    pub total_aleph_messages: u64,
    pub eth_height: u64,
    pub metrics: u64,
}
impl Default for TtlSettings {
    fn default() -> Self {
        Self {
            total_aleph_messages: 120,
            eth_height: 600,
            metrics: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2pSettings {
    pub http_port: u16,
    pub port: u16,
    pub control_port: u16,
    pub daemon_host: String,
    pub mq_host: String,
    pub reconnect_delay: u64,
    pub max_peer_age: u64,
    pub alive_topic: String,
    pub clients: Vec<String>,
    pub peers: Vec<String>,
    pub topics: Vec<String>,
}
impl Default for P2pSettings {
    fn default() -> Self {
        Self {
            http_port: 4024,
            port: 4025,
            control_port: 4030,
            daemon_host: "p2p-service".into(),
            mq_host: "rabbitmq".into(),
            reconnect_delay: 60,
            max_peer_age: 24 * 60 * 60,
            alive_topic: "ALIVE".into(),
            clients: vec!["http".into()],
            peers: vec![
                "/dns/api2.aleph.im/tcp/4025/p2p/QmZkurbY2G2hWay59yiTgQNaQxHSNzKZFt2jbnwJhQcKgV"
                    .into(),
                "/dns/api3.aleph.im/tcp/4025/p2p/Qmb5b2ZwJm9pVWrppf3D3iMF1bXbjZhbJTwGvKEBMZNxa2"
                    .into(),
            ],
            topics: vec!["ALIVE".into(), "ALEPH-TEST".into()],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSettings {
    pub folder: String,
    pub store_files: bool,
    pub garbage_collector_period: u64,
    pub grace_period: u64,
    pub max_file_size: u64,
    pub max_unauthenticated_upload_file_size: u64,
}
impl Default for StorageSettings {
    fn default() -> Self {
        Self {
            folder: "/var/lib/pyaleph".into(),
            store_files: true,
            garbage_collector_period: 24,
            grace_period: 24,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            max_unauthenticated_upload_file_size: DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Nuls2Settings {
    pub chain_id: u64,
    pub enabled: bool,
    pub packing_node: bool,
    pub api_url: String,
    pub explorer_url: String,
    pub private_key: Option<String>,
    pub sync_address: Option<String>,
    pub commit_delay: u64,
    pub remark: String,
}
impl Default for Nuls2Settings {
    fn default() -> Self {
        Self {
            chain_id: 1,
            enabled: false,
            packing_node: false,
            api_url: "https://apiserver.nuls.io/".into(),
            explorer_url: "https://nuls.world".into(),
            private_key: None,
            sync_address: None,
            commit_delay: 14,
            remark: "ALEPH-SYNC".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BscSettings {
    pub enabled: bool,
    pub sync_contract: String,
}
impl Default for BscSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            sync_contract: "0xdF270752C8C71D08acbae4372687DA65AECe2D5D".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthereumSettings {
    pub enabled: bool,
    pub api_url: String,
    pub packing_node: bool,
    pub chain_id: u64,
    pub private_key: Option<String>,
    pub sync_contract: Option<String>,
    pub start_height: u64,
    pub max_block_range: u64,
    pub commit_delay: u64,
    pub max_gas_price: u64,
    pub authorized_emitters: Vec<String>,
    pub message_delay: u64,
    pub client_timeout: u64,
}
impl Default for EthereumSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            api_url: "http://127.0.0.1:8545".into(),
            packing_node: false,
            chain_id: 1,
            private_key: None,
            sync_contract: None,
            start_height: 11_400_000,
            max_block_range: 100_000,
            commit_delay: 35,
            max_gas_price: 150_000_000_000,
            authorized_emitters: vec!["0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC".into()],
            message_delay: 30,
            client_timeout: 60,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TezosSettings {
    pub enabled: bool,
    pub indexer_url: String,
    pub sync_contract: String,
}
impl Default for TezosSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            indexer_url: "https://tezos-mainnet.api.aleph.cloud".into(),
            sync_contract: "KT1FfEoaNvooDfYrP61Ykct6L8z7w7e2pgnT".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSettings {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub pool_size: u32,
    pub pool_pre_ping: bool,
    pub pool_recycle: u64,
}
impl Default for PostgresSettings {
    fn default() -> Self {
        Self {
            host: "postgres".into(),
            port: 5432,
            database: "aleph".into(),
            user: "aleph".into(),
            password: "decentralize-everything".into(),
            pool_size: 50,
            pool_pre_ping: true,
            pool_recycle: 3600,
        }
    }
}

impl PostgresSettings {
    /// Build the connection URL for sqlx.
    pub fn url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpfsSettings {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub scheme: String,
    pub alive_topic: String,
    pub reconnect_delay: u64,
    pub peers: Vec<String>,
    pub max_upload_file_size: u64,
    pub max_unauthenticated_upload_file_size: u64,
    pub max_upload_car_size: u64,
    pub pinning: IpfsPinningSettings,
    pub stat_timeout: u64,
    /// Randomized delay (in seconds, drawn uniformly from `[0, value]`) before
    /// an IPFS file fetch starts, to spread the thundering herd of CCNs pulling
    /// a newly-announced CID from the origin. Set to 0 to disable.
    pub fetch_jitter_seconds: f64,
}
impl Default for IpfsSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "ipfs".into(),
            port: 5001,
            scheme: "http".into(),
            alive_topic: "ALEPH_ALIVE".into(),
            reconnect_delay: 60,
            peers: vec![
                "/ip4/51.159.57.71/tcp/4001/p2p/12D3KooWBH3JVSBwHLNzxv7EzniBP3tDmjJaoa3EJBF9wyhZtHt2".into(),
                "/ip4/62.210.93.220/tcp/4001/p2p/12D3KooWLcmvqojHzUnR7rr8YhFKGDD8z7fmsPyBfAm2rT3sFGAF".into(),
            ],
            max_upload_file_size: DEFAULT_MAX_UPLOAD_FILE_SIZE,
            max_unauthenticated_upload_file_size: DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
            max_upload_car_size: DEFAULT_MAX_UPLOAD_CAR_SIZE,
            pinning: IpfsPinningSettings::default(),
            stat_timeout: 30,
            fetch_jitter_seconds: 5.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpfsPinningSettings {
    pub host: Option<String>,
    pub port: u16,
    pub scheme: String,
    pub timeout: u64,
}
impl Default for IpfsPinningSettings {
    fn default() -> Self {
        Self {
            host: None,
            port: 5001,
            scheme: "http".into(),
            timeout: 60,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RabbitmqSettings {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub pub_exchange: String,
    pub sub_exchange: String,
    pub message_exchange: String,
    pub pending_message_exchange: String,
    pub pending_tx_exchange: String,
    pub heartbeat: u64,
}
impl Default for RabbitmqSettings {
    fn default() -> Self {
        Self {
            host: "rabbitmq".into(),
            port: 5672,
            username: "aleph-p2p".into(),
            password: "change-me!".into(),
            pub_exchange: "p2p-publish".into(),
            sub_exchange: "p2p-subscribe".into(),
            message_exchange: "aleph-messages".into(),
            pending_message_exchange: "aleph-pending-messages".into(),
            pending_tx_exchange: "aleph-pending-txs".into(),
            heartbeat: 600,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisSettings {
    pub host: String,
    pub port: u16,
}
impl Default for RedisSettings {
    fn default() -> Self {
        Self {
            host: "redis".into(),
            port: 6379,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SentrySettings {
    pub dsn: Option<String>,
    pub traces_sample_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfSettings {
    pub message_count_cache_ttl: u64,
}
impl Default for PerfSettings {
    fn default() -> Self {
        Self {
            message_count_cache_ttl: 300,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketSettings {
    pub max_message_connections: u32,
    pub max_status_connections: u32,
    pub heartbeat: u64,
}
impl Default for WebsocketSettings {
    fn default() -> Self {
        Self {
            max_message_connections: 10_000,
            max_status_connections: 1_000,
            heartbeat: 30,
        }
    }
}
