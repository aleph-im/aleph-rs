//! aleph-ccn — Rust port of pyaleph (Aleph.im Core Channel Node).
//!
//! Modules mirror `aleph/<name>` in the Python tree. The goal is iso-functional
//! parity with `pyaleph`. We deliberately avoid stubs — if a module is declared
//! here, it ships with a real implementation.

#![allow(
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_if,
    clippy::collapsible_match,
    clippy::default_constructed_unit_structs,
    clippy::derivable_impls,
    clippy::double_ended_iterator_last,
    clippy::field_reassign_with_default,
    clippy::if_same_then_else,
    clippy::items_after_test_module,
    clippy::large_enum_variant,
    clippy::let_and_return,
    clippy::manual_contains,
    clippy::manual_unwrap_or_default,
    clippy::map_identity,
    clippy::needless_bool,
    clippy::needless_borrow,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_match,
    clippy::explicit_auto_deref,
    clippy::redundant_closure,
    clippy::redundant_guards,
    clippy::single_match,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::unnecessary_cast,
    clippy::useless_format,
    clippy::useless_conversion,
    clippy::useless_vec,
    clippy::wrong_self_convention
)]

pub mod chains;
pub mod config;
pub mod db;
pub mod error;
pub mod handlers;
pub mod jobs;
pub mod network;
pub mod permissions;
pub mod repair;
pub mod schemas;
pub mod services;
pub mod storage;
pub mod toolkit;
pub mod types;
pub mod web;

use std::sync::Arc;
use std::time::Duration;

pub use error::{AlephError, AlephResult};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimeOptions {
    pub no_commit: bool,
    pub no_jobs: bool,
}

fn handlers_config(cfg: &config::Settings) -> handlers::message_handler::HandlersConfig {
    handlers::message_handler::HandlersConfig {
        balances_addresses: cfg.aleph.balances.addresses.clone(),
        balances_post_type: cfg.aleph.balances.post_type.clone(),
        credit_balances_addresses: cfg.aleph.credit_balances.addresses.clone(),
        credit_balances_post_types: cfg.aleph.credit_balances.post_types.clone(),
        credit_balances_channels: cfg.aleph.credit_balances.channels.clone(),
        storage_grace_period_hours: cfg.storage.grace_period as i64,
        max_unauthenticated_upload_file_size: cfg
            .storage
            .max_unauthenticated_upload_file_size as i64,
        ipfs_enabled: cfg.ipfs.enabled,
        store_files: cfg.storage.store_files,
        ipfs_stat_timeout: cfg.ipfs.stat_timeout,
        api_servers: Vec::new(),
    }
}

async fn supervise_void_handles(
    handles: Vec<tokio::task::JoinHandle<()>>,
    cancel: jobs::job_utils::CancelToken,
    name: &'static str,
) -> AlephResult<()> {
    if handles.is_empty() {
        cancel.cancelled().await;
        return Ok(());
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel(handles.len());
    let mut aborts = Vec::with_capacity(handles.len());
    for handle in handles {
        aborts.push(handle.abort_handle());
        let tx = tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(handle.await).await;
        });
    }
    drop(tx);

    tokio::select! {
        _ = cancel.cancelled() => {
            for abort in aborts {
                abort.abort();
            }
            Ok(())
        }
        joined = rx.recv() => {
            for abort in aborts {
                abort.abort();
            }
            match joined {
                Some(Ok(())) => Err(AlephError::P2p(format!("{name} task exited unexpectedly"))),
                Some(Err(e)) => Err(AlephError::P2p(format!("{name} task join error: {e}"))),
                None => Ok(()),
            }
        }
    }
}

pub async fn run(cfg: config::Settings) -> AlephResult<()> {
    run_with_options(cfg, RuntimeOptions::default()).await
}

pub async fn run_with_options(
    mut cfg: config::Settings,
    options: RuntimeOptions,
) -> AlephResult<()> {
    if options.no_commit {
        cfg.storage.store_files = false;
    }

    let pool = db::connect(&cfg.postgres).await?;
    db::migrate(&pool).await?;
    repair::repair_credit_balances(&pool).await?;
    tracing::info!("database ready");

    let ipfs_service = if cfg.ipfs.enabled {
        Some(Arc::new(services::ipfs::IpfsService::new(&cfg.ipfs)?))
    } else {
        None
    };
    let storage_engine = if cfg.storage.store_files {
        Some(Arc::new(services::storage::filesystem_engine::FileSystemStorageEngine::new(
            &cfg.storage.folder,
        )?) as Arc<dyn services::storage::engine::StorageEngine>)
    } else {
        None
    };
    let job_storage_engine: Arc<dyn services::storage::engine::StorageEngine> = storage_engine
        .clone()
        .unwrap_or_else(|| Arc::new(services::storage::in_memory::InMemoryStorageEngine::new()));

    let host = std::env::var("ALEPH_BIND_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = cfg.p2p.http_port;
    let cfg = Arc::new(cfg);
    let api_p2p_client = Arc::new(services::p2p::protocol::HttpP2pClient::new(
        &cfg.p2p,
        Some(cfg.rabbitmq.clone()),
        "api",
    ));
    let mut state = web::AppState::new(pool.clone(), cfg.clone());
    state.ipfs_service = ipfs_service.clone();
    state.p2p_client = Some(api_p2p_client.clone());
    state.storage_engine = storage_engine.clone();
    let message_broadcast = state.message_broadcast.clone();

    if options.no_jobs {
        tokio::select! {
            result = web::serve(state, &host, port) => result?,
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
                tracing::info!("shutdown signal received");
            }
        }
        return Ok(());
    }

    let mq_conn = toolkit::rabbitmq::make_mq_conn_direct(&cfg.rabbitmq).await?;
    let pending_message_channel = mq_conn
        .create_channel()
        .await
        .map_err(|e| AlephError::P2p(format!("rabbitmq create channel failed: {e}")))?;
    let pending_tx_channel = mq_conn
        .create_channel()
        .await
        .map_err(|e| AlephError::P2p(format!("rabbitmq create channel failed: {e}")))?;
    let message_channel = mq_conn
        .create_channel()
        .await
        .map_err(|e| AlephError::P2p(format!("rabbitmq create channel failed: {e}")))?;
    toolkit::rabbitmq::declare_pending_message_exchange(&pending_message_channel, &cfg.rabbitmq)
        .await?;
    toolkit::rabbitmq::declare_pending_tx_exchange(&pending_tx_channel, &cfg.rabbitmq).await?;
    toolkit::rabbitmq::declare_message_exchange(&message_channel, &cfg.rabbitmq).await?;

    let pending_tx_queue = jobs::job_utils::make_pending_tx_queue(
        &pending_tx_channel,
        &cfg.rabbitmq.pending_tx_exchange,
    )
    .await?;
    let fetch_queue = jobs::job_utils::make_pending_message_queue(
        &pending_message_channel,
        &cfg.rabbitmq.pending_message_exchange,
        "fetch.#",
    )
    .await?;
    let process_queue = jobs::job_utils::make_pending_message_queue(
        &pending_message_channel,
        &cfg.rabbitmq.pending_message_exchange,
        "process.#",
    )
    .await?;

    let pending_tx_watcher = Arc::new(jobs::job_utils::MqWatcher::spawn(
        pending_tx_channel.clone(),
        pending_tx_queue,
        "#".to_string(),
    )?);
    let fetch_watcher = Arc::new(jobs::job_utils::MqWatcher::spawn(
        pending_message_channel.clone(),
        fetch_queue,
        "fetch.#".to_string(),
    )?);
    let process_watcher = Arc::new(jobs::job_utils::MqWatcher::spawn(
        pending_message_channel.clone(),
        process_queue,
        "process.#".to_string(),
    )?);

    let message_publisher = Arc::new(handlers::message_handler::MessagePublisher::new(
        pending_message_channel.clone(),
        cfg.rabbitmq.pending_message_exchange.clone(),
    ));
    state.message_publisher = message_publisher.clone();
    let message_handler = Arc::new(handlers::message_handler::MessageHandler::new(
        Arc::new(chains::signature_verifier::SignatureVerifier::new()),
        job_storage_engine.clone(),
        ipfs_service.clone(),
        Arc::new(permissions::DbAuthorityLookup::new(pool.clone())),
        &handlers_config(&cfg),
    ));

    let cancel = jobs::job_utils::CancelToken::new();
    let node_cache = Arc::new(
        services::cache::node_cache::NodeCache::new(
            &cfg.redis.host,
            cfg.redis.port,
            cfg.perf.message_count_cache_ttl,
        )
        .await?,
    );
    let chain_ipfs_service = match ipfs_service.clone() {
        Some(ipfs) => ipfs,
        None => Arc::new(services::ipfs::IpfsService::new(&cfg.ipfs)?),
    };
    let chain_storage_service = Arc::new(
        storage::StorageService::new(
            job_storage_engine.clone(),
            chain_ipfs_service,
            node_cache.clone(),
        )
        .with_ipfs_enabled(cfg.ipfs.enabled)
        .with_http_p2p_enabled(cfg.p2p.clients.iter().any(|client| client == "http")),
    );
    let chain_data_service = Arc::new(chains::chain_data_service::ChainDataService::with_storage(
        chain_storage_service,
    ));
    let pending_tx_publisher = Arc::new(chains::chain_data_service::PendingTxPublisher::new(
        Box::new(chains::chain_data_service::MqPendingTxSink::new(
            pool.clone(),
            pending_tx_channel.clone(),
            cfg.rabbitmq.pending_tx_exchange.clone(),
        )),
    ));
    let chain_connector = Arc::new(
        chains::connector::ChainConnector::from_settings_with_services(
            &cfg,
            Some(pool.clone()),
            chain_data_service.clone(),
            pending_tx_publisher,
        )
        .await?,
    );
    let chain_connector_job =
        chain_connector.start_all_until_cancel(cfg.clone(), cancel.clone());
    let pending_tx_job = jobs::process_pending_txs::run(
        pool.clone(),
        Arc::new(jobs::process_pending_txs::DbTxMessageProvider::new(
            pool.clone(),
            chain_data_service,
        )),
        Arc::new(jobs::process_pending_txs::DbPendingMessagePublisher::new(
            pool.clone(),
            message_publisher.clone(),
        )),
        pending_tx_watcher,
        jobs::process_pending_txs::PendingTxConfig {
            max_concurrency: cfg.aleph.jobs.pending_txs.max_concurrency as usize,
            idle_timeout: Duration::from_secs(cfg.aleph.jobs.pending_messages.idle_timeout),
            one_shot: false,
        },
        cancel.clone(),
    );
    let fetch_job = jobs::fetch_pending_messages::run(
        pool.clone(),
        Arc::new(jobs::fetch_pending_messages::HandlerFetchRunner {
            handler: message_handler.clone(),
            max_retries: cfg.aleph.jobs.pending_messages.max_retries as i32,
        }),
        Some(Arc::new(jobs::fetch_pending_messages::FetchNotifier {
            channel: pending_message_channel.clone(),
            exchange: cfg.rabbitmq.pending_message_exchange.clone(),
        })),
        fetch_watcher,
        jobs::fetch_pending_messages::FetchConfig {
            max_concurrency: cfg.aleph.jobs.pending_messages.max_concurrency as usize,
            idle_timeout: Duration::from_secs(cfg.aleph.jobs.pending_messages.idle_timeout),
            one_shot: false,
        },
        cancel.clone(),
    );
    let process_job = jobs::process_pending_messages::run(
        pool.clone(),
        Arc::new(jobs::process_pending_messages::HandlerRunner {
            handler: message_handler,
            max_retries: cfg.aleph.jobs.pending_messages.max_retries as i32,
        }),
        Some(Arc::new(jobs::process_pending_messages::OutcomePublisher {
            channel: message_channel,
            exchange: cfg.rabbitmq.message_exchange.clone(),
            broadcast: Some(message_broadcast),
        })),
        process_watcher,
        jobs::process_pending_messages::PendingMessageProcessorConfig {
            idle_timeout: Duration::from_secs(cfg.aleph.jobs.pending_messages.idle_timeout),
            one_shot: false,
        },
        cancel.clone(),
    );
    let cron_period =
        Duration::from_secs_f64((cfg.aleph.jobs.cron.period * 3600.0).max(1.0));
    let cron_runner = Arc::new(jobs::cron::cron_job::CronRunner::new(
        pool.clone(),
        vec![
            Arc::new(jobs::cron::balance_job::BalanceCronJob::new(
                cfg.storage.max_unauthenticated_upload_file_size as i64,
            )),
            Arc::new(jobs::cron::credit_balance_job::CreditBalanceCronJob::new(
                cfg.storage.max_unauthenticated_upload_file_size as i64,
            )),
        ],
    ));
    let cron_job = jobs::cron::cron_job::run(cron_runner, cron_period, cancel.clone());
    let garbage_collector_job = services::storage::garbage_collector::run(
        services::storage::garbage_collector::GarbageCollector::new(
            pool.clone(),
            storage_engine
                .clone()
                .unwrap_or_else(|| Arc::new(services::storage::in_memory::InMemoryStorageEngine::new())),
            ipfs_service.clone(),
            cfg.storage.grace_period,
        ),
        cfg.storage.garbage_collector_period,
        cancel.clone(),
    );
    let reconnect_ipfs_job = async {
        if let Some(ipfs) = ipfs_service.clone() {
            jobs::reconnect_ipfs::run(
                pool.clone(),
                ipfs,
                jobs::reconnect_ipfs::ReconnectConfig {
                    configured_peers: cfg.ipfs.peers.clone(),
                    reconnect_delay: Duration::from_secs(cfg.ipfs.reconnect_delay),
                    max_peer_age: Duration::from_secs(cfg.p2p.max_peer_age),
                },
                cancel.clone(),
            )
            .await
        } else {
            cancel.cancelled().await;
            Ok(())
        }
    };
    let p2p_client = Arc::new(services::p2p::protocol::HttpP2pClient::new(
        &cfg.p2p,
        Some(cfg.rabbitmq.clone()),
        "network-monitor",
    ));
    let p2p_ipfs_service = match ipfs_service.clone() {
        Some(ipfs) => ipfs,
        None => Arc::new(services::ipfs::IpfsService::new(&cfg.ipfs)?),
    };
    let peer_allowlist =
        services::peers::allowlist::PeerAllowlist::from_config(&cfg, pool.clone());
    let p2p_manager_job = async {
        let handles = services::p2p::manager::initialize_host(
            &cfg,
            pool.clone(),
            p2p_client,
            p2p_ipfs_service,
            peer_allowlist,
            node_cache,
            &host,
            cfg.p2p.port,
            true,
        )
        .await?;
        supervise_void_handles(handles, cancel.clone(), "p2p manager").await
    };
    let listener_job = async {
        let mut handles =
            network::listener_tasks(&cfg, message_publisher.clone(), pool.clone(), ipfs_service.clone())
                .await?;
        if handles.is_empty() {
            cancel.cancelled().await;
            return Ok::<(), AlephError>(());
        }
        tokio::select! {
            _ = cancel.cancelled() => {
                handles.abort_all();
                while handles.join_next().await.is_some() {}
                Ok::<(), AlephError>(())
            }
            result = async {
                match handles.join_next().await {
                    Some(Ok(result)) => result,
                    Some(Err(e)) => Err(AlephError::P2p(format!("listener join error: {e}"))),
                    None => Ok(()),
                }
            } => {
                handles.abort_all();
                while handles.join_next().await.is_some() {}
                result?;
                Err(AlephError::P2p("listener exited unexpectedly".into()))
            }
        }
    };
    tokio::pin!(pending_tx_job);
    tokio::pin!(fetch_job);
    tokio::pin!(process_job);
    tokio::pin!(cron_job);
    tokio::pin!(garbage_collector_job);
    tokio::pin!(reconnect_ipfs_job);
    tokio::pin!(p2p_manager_job);
    tokio::pin!(chain_connector_job);
    tokio::pin!(listener_job);

    tokio::select! {
        result = web::serve(state, &host, port) => {
            cancel.cancel();
            result?;
        },
        result = &mut pending_tx_job => {
            cancel.cancel();
            result?;
        },
        result = &mut fetch_job => {
            cancel.cancel();
            result?;
        },
        result = &mut process_job => {
            cancel.cancel();
            result?;
        },
        result = &mut cron_job => {
            cancel.cancel();
            result?;
        },
        result = &mut garbage_collector_job => {
            cancel.cancel();
            result?;
        },
        result = &mut reconnect_ipfs_job => {
            cancel.cancel();
            result?;
        },
        result = &mut p2p_manager_job => {
            cancel.cancel();
            result?;
        },
        result = &mut chain_connector_job => {
            cancel.cancel();
            result?;
        },
        result = &mut listener_job => {
            cancel.cancel();
            result?;
        },
        signal = tokio::signal::ctrl_c() => {
            cancel.cancel();
            signal.map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
            tracing::info!("shutdown signal received");
        }
    }
    Ok(())
}
