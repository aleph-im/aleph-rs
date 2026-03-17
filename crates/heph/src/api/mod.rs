use actix_web::web;
use std::sync::Arc;

use crate::config::HephConfig;
use crate::db::Db;
use crate::files::FileStore;

pub mod addresses;
pub mod aggregates;
pub mod balances;
pub mod costs;
pub mod messages;
pub mod posts;
pub mod storage;

pub struct AppState {
    pub db: Arc<Db>,
    pub file_store: Arc<FileStore>,
    pub config: HephConfig,
}

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v0")
            .route("/messages", web::post().to(messages::post_message))
            .route("/messages.json", web::get().to(messages::list_messages))
            .route("/messages/hashes", web::get().to(messages::list_hashes))
            .route(
                "/messages/page/{page}.json",
                web::get().to(messages::list_messages_page),
            )
            // Specific sub-routes of /messages/{hash} BEFORE the catch-all
            .route(
                "/messages/{hash}/consumed_credits",
                web::get().to(costs::get_consumed_credits),
            )
            .route(
                "/messages/{hash}/status",
                web::get().to(messages::get_message_status),
            )
            .route(
                "/messages/{hash}/content",
                web::get().to(messages::get_message_content),
            )
            .route("/messages/{hash}", web::get().to(messages::get_message))
            // Aggregates
            .route(
                "/aggregates/{address}.json",
                web::get().to(aggregates::get_aggregates_for_address),
            )
            .route(
                "/aggregates.json",
                web::get().to(aggregates::list_aggregates),
            )
            .route("/aggregates", web::get().to(aggregates::list_aggregates))
            // Posts v0
            .route("/posts.json", web::get().to(posts::list_posts_v0))
            .route("/posts", web::get().to(posts::list_posts_v0))
            // Storage — specific paths before the catch-all /{hash}
            .route("/storage/raw/{hash}", web::get().to(storage::get_raw))
            .route("/storage/raw/{hash}", web::head().to(storage::get_raw))
            .route(
                "/storage/metadata/{hash}",
                web::get().to(storage::get_metadata),
            )
            .route(
                "/storage/by-message-hash/{hash}",
                web::get().to(storage::get_by_message_hash),
            )
            .route(
                "/storage/by-ref/{address}/{ref_}",
                web::get().to(storage::get_by_ref_with_address),
            )
            .route("/storage/by-ref/{ref_}", web::get().to(storage::get_by_ref))
            .route(
                "/storage/count/{hash}",
                web::get().to(storage::get_pin_count),
            )
            .route("/storage/add_file", web::post().to(storage::add_file))
            .route("/storage/add_json", web::post().to(storage::add_json))
            .route("/storage/{hash}", web::get().to(storage::get_base64))
            // Balances
            .route(
                "/addresses/{address}/balance",
                web::get().to(balances::get_balance),
            )
            .route("/balances", web::get().to(balances::list_balances))
            .route(
                "/credit_balances",
                web::get().to(balances::list_credit_balances),
            )
            .route(
                "/addresses/{address}/credit_history",
                web::get().to(balances::get_credit_history),
            )
            // Costs
            .route("/costs", web::get().to(costs::list_costs))
            .route("/price/estimate", web::post().to(costs::estimate_price))
            .route("/price/{hash}", web::get().to(costs::get_price))
            // Address utilities — specific paths before catch-all
            .route("/addresses/stats.json", web::get().to(addresses::get_stats))
            .route(
                "/addresses/{address}/files",
                web::get().to(addresses::get_files),
            )
            .route(
                "/addresses/{address}/post_types",
                web::get().to(addresses::get_post_types),
            )
            .route(
                "/addresses/{address}/channels",
                web::get().to(addresses::get_channels),
            )
            .route(
                "/channels/list.json",
                web::get().to(addresses::list_channels),
            )
            .route(
                "/authorizations/granted/{address}.json",
                web::get().to(addresses::get_granted_authorizations),
            )
            .route(
                "/authorizations/received/{address}.json",
                web::get().to(addresses::get_received_authorizations),
            ),
    );

    // Posts v1
    cfg.service(
        web::scope("/api/v1")
            .route("/posts.json", web::get().to(posts::list_posts_v1))
            .route("/posts", web::get().to(posts::list_posts_v1)),
    );
}
