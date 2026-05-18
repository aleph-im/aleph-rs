use aleph_ccn::{config::Settings, web::AppState};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use std::sync::Arc;
use tokio_postgres::{Config, NoTls};

fn main() {
    let cfg = Config::new();
    let mgr = Manager::from_config(
        cfg,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(mgr).max_size(0).build().unwrap();
    let state = AppState::new(pool, Arc::new(Settings::default()));
    let _ = aleph_ccn::web::build_router(state);
    println!("router built OK");
}
