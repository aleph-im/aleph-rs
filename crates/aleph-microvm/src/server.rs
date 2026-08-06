use std::net::SocketAddr;

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::any;
use axum::Router;

use crate::asgi::{run_success_to_parts, scope_from_parts};
use crate::protocol::{RunCodePayload, RunResponse};
use crate::vsock::VsockChannel;

/// Serve the VM on `localhost:port`, forwarding every request over vsock as an ASGI scope.
/// Blocks until `shutdown` resolves.
pub async fn serve_localhost(
    port: u16,
    channel: VsockChannel,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let app = Router::new().fallback(any(handle)).with_state(channel);
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}

async fn handle(State(channel): State<VsockChannel>, req: Request) -> Response {
    let method = req.method().as_str().to_string();
    let target = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let headers: Vec<(Vec<u8>, Vec<u8>)> = req
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect();
    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024 * 1024).await {
        Ok(b) => b.to_vec(),
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "request body too large"),
    };

    let scope = scope_from_parts(&method, &target, &headers, body);
    let payload = match (RunCodePayload { scope }).to_msgpack() {
        Ok(p) => p,
        Err(e) => return error_response(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    let raw = match channel.send_run(&payload).await {
        Ok(r) => r,
        Err(e) => return error_response(StatusCode::BAD_GATEWAY, &format!("VM call failed: {e}")),
    };
    if raw.is_empty() {
        return error_response(
            StatusCode::BAD_GATEWAY,
            "VM produced no response (it may have crashed)",
        );
    }
    let parsed: RunResponse = match rmp_serde::from_slice(&raw) {
        Ok(p) => p,
        Err(e) => {
            return error_response(
                StatusCode::BAD_GATEWAY,
                &format!("invalid VM response: {e}"),
            )
        }
    };
    match parsed.into_success() {
        Ok(success) => build_response(run_success_to_parts(success)),
        Err(traceback) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &traceback),
    }
}

fn build_response(parts: (u16, Vec<(String, String)>, Vec<u8>)) -> Response {
    let (status, headers, body) = parts;
    let mut builder =
        Response::builder().status(StatusCode::from_u16(status).unwrap_or(StatusCode::OK));
    for (k, v) in headers {
        if let (Ok(name), Ok(val)) = (
            HeaderName::try_from(k.as_str()),
            HeaderValue::try_from(v.as_str()),
        ) {
            builder = builder.header(name, val);
        }
    }
    builder
        .body(axum::body::Body::from(Bytes::from(body)))
        .unwrap()
}

fn error_response(status: StatusCode, msg: &str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(axum::body::Body::from(msg.to_string()))
        .unwrap()
}
