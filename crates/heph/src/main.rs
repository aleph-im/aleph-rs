use actix_cors::Cors;
use actix_web::{App, HttpServer, web};
use aleph_types::account::{Account, EvmAccount};
use aleph_types::chain::Chain;
use clap::Parser;

use std::sync::{Arc, Mutex};

use heph::api::{AppState, configure_routes};
use heph::config::HephConfig;
use heph::corechannel::CoreChannelState;
use heph::db::Db;
use heph::files::FileStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut config = HephConfig::parse();

    tracing_subscriber::fmt()
        .with_env_filter(&config.log_level)
        .init();

    // Set up data directory
    let data_dir = config.data_dir.clone().unwrap_or_else(|| {
        let dir = std::env::temp_dir().join("heph");
        std::fs::create_dir_all(&dir).ok();
        dir
    });
    std::fs::create_dir_all(&data_dir)?;

    let db = Arc::new(Db::open(&data_dir.join("heph.db"))?);
    let file_store = Arc::new(FileStore::new(&data_dir.join("files"))?);

    // Determine accounts: auto-generate if none were provided
    let generated_accounts: Vec<(String, String)>; // (private_key_hex, address)
    let print_private_keys: bool;

    if config.accounts.is_empty() {
        // Auto-generate 10 deterministic EVM accounts (same across runs, like Anvil)
        let mut accounts = Vec::new();
        for i in 0u64..10 {
            let mut key = [0u8; 32];
            key[24..32].copy_from_slice(&i.to_be_bytes());
            key[0] = 0xac; // prefix to avoid degenerate keys
            let account = EvmAccount::new(Chain::Ethereum, &key).unwrap();
            let addr = account.address().as_str().to_string();
            accounts.push((hex::encode(key), addr.clone()));
            config.accounts.push(addr);
        }
        generated_accounts = accounts;
        print_private_keys = true;
    } else {
        generated_accounts = vec![];
        print_private_keys = false;
    }

    // Pre-seed accounts with configured balance
    for addr in &config.accounts {
        db.with_conn(|c| heph::db::balances::set_credit_balance(c, addr, config.balance))?;
    }

    // Print banner
    print_banner(&config, &generated_accounts, print_private_keys);

    let host = config.host.clone();
    let port = config.port;

    // Initialize corechannel aggregate with heph itself as a CCN.
    // The first account is the node operator.
    let corechannel = {
        use aleph_sdk::corechannel::{CoreChannelAction, CreateNodeDetails};

        let mut cc = CoreChannelState::new();
        let node_addr = &config.accounts[0];
        cc.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "heph".to_string(),
                    multiaddress: format!("/ip4/{}/tcp/{}", config.host, config.port),
                },
            },
            node_addr,
            None,
            "0000000000000000000000000000000000000000000000000000000000000000",
            0.0,
        );
        heph::corechannel::persist_aggregate(&db, &cc, 0.0);
        Mutex::new(cc)
    };

    let state = web::Data::new(AppState {
        db,
        file_store,
        config,
        corechannel,
    });

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .wrap(
                Cors::default()
                    .allow_any_origin()
                    .allow_any_method()
                    .allow_any_header(),
            )
            .configure(configure_routes)
    })
    .bind((host.as_str(), port))?
    .run()
    .await?;

    Ok(())
}

fn print_banner(
    config: &HephConfig,
    generated_accounts: &[(String, String)],
    print_private_keys: bool,
) {
    let version = env!("CARGO_PKG_VERSION");
    println!("Heph - Local Aleph CCN v{version}");
    println!("{}", "=".repeat(30));
    println!();
    println!("Available Accounts");
    println!("{}", "=".repeat(18));
    for (i, addr) in config.accounts.iter().enumerate() {
        println!("({i}) {addr} ({} credits)", config.balance);
    }

    if print_private_keys {
        println!();
        println!("Private Keys");
        println!("{}", "=".repeat(12));
        for (i, (key_hex, _addr)) in generated_accounts.iter().enumerate() {
            println!("({i}) 0x{key_hex}");
        }
    }

    println!();
    println!("Listening on http://{}:{}", config.host, config.port);
    println!();
}
