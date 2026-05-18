//! Mirrors `aleph/web/controllers/channels.py`.

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;

use crate::db::accessors::messages::get_distinct_channels;
use crate::web::AppState;
use crate::web::controllers::error::WebResult;
use crate::web::controllers::utils::{get_db, json_response};

pub fn routes() -> Router<AppState> {
    Router::new().route("/api/v0/channels/list.json", get(used_channels))
}

async fn used_channels(State(state): State<AppState>) -> WebResult<Response> {
    let client = get_db(&state).await?;
    let rows = get_distinct_channels(&**client).await?;
    let channels: Vec<serde_json::Value> = rows
        .into_iter()
        .filter_map(|c| {
            c.map(|chan| serde_json::to_value(&chan).unwrap_or(serde_json::Value::Null))
        })
        .collect();
    let body = serde_json::json!({ "channels": channels });
    Ok(json_response(StatusCode::OK, &body))
}
