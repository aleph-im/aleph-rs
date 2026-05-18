//! Mirrors `aleph/web/controllers/programs.py`.

use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::db::accessors::messages::get_programs_triggered_by_messages;
use crate::types::sort_order::SortOrder;
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::utils::{get_db, json_text_response};

pub fn routes() -> Router<AppState> {
    Router::new().route("/api/v0/programs/on/message", get(get_programs_on_message))
}

#[derive(Debug, Default, Deserialize)]
struct ProgramQuery {
    #[serde(default)]
    sort_order: Option<i32>,
}

async fn get_programs_on_message(
    State(state): State<AppState>,
    Query(q): Query<ProgramQuery>,
) -> WebResult<Response> {
    let sort_order = match q.sort_order {
        Some(1) => SortOrder::Ascending,
        Some(-1) | None => SortOrder::Descending,
        Some(other) => {
            return Err(WebError::BadRequest(format!("Invalid sort_order: {other}")));
        }
    };
    let client = get_db(&state).await?;
    let rows = get_programs_triggered_by_messages(&**client, sort_order).await?;
    let messages: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            json!({
                "item_hash": r.item_hash,
                "content": {
                    "on": { "message": r.message_subscriptions },
                },
            })
        })
        .collect();
    Ok(json_text_response(
        StatusCode::OK,
        Value::Array(messages).to_string(),
    ))
}
