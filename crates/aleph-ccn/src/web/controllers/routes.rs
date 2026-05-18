//! Route registration. Mirrors `aleph/web/controllers/routes.py`.

use axum::Router;

use super::{
    accounts, aggregates, authorizations, channels, info, ipfs, main, messages, p2p, posts, prices,
    programs, storage, version,
};
use crate::web::AppState;

pub fn router(state: AppState) -> Router<AppState> {
    Router::new()
        .merge(version::routes())
        .merge(main::routes())
        .merge(info::routes())
        .merge(channels::routes())
        .merge(aggregates::routes())
        .merge(authorizations::routes())
        .merge(messages::routes())
        .merge(posts::routes())
        .merge(programs::routes())
        .merge(p2p::routes())
        .merge(accounts::routes())
        // prices::routes needs the state to wire the auth-token middleware
        // on `/api/v0/price/recalculate` and `/api/v0/price/{item_hash}/recalculate`.
        .merge(prices::routes(state))
        .merge(storage::routes())
        .merge(ipfs::routes())
}
