use crate::protocol::{AsgiScope, RunSuccess};

/// Build an ASGI scope from HTTP request parts. `target` is the raw request target
/// (path plus optional `?query`).
pub fn scope_from_parts(
    method: &str,
    target: &str,
    headers: &[(Vec<u8>, Vec<u8>)],
    body: Vec<u8>,
) -> AsgiScope {
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.as_bytes().to_vec()),
        None => (target.to_string(), Vec::new()),
    };
    AsgiScope::http(method, &path, query, headers.to_vec(), body)
}

/// Lower a successful run response to HTTP parts, decoding header bytes lossily to strings.
/// Drops hop-by-hop headers the local server will recompute.
pub fn run_success_to_parts(s: RunSuccess) -> (u16, Vec<(String, String)>, Vec<u8>) {
    const DROP: [&str; 3] = ["content-length", "transfer-encoding", "content-encoding"];
    let headers = s
        .headers
        .into_iter()
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(&k).into_owned(),
                String::from_utf8_lossy(&v).into_owned(),
            )
        })
        .filter(|(k, _)| !DROP.contains(&k.to_ascii_lowercase().as_str()))
        .collect();
    (s.status, headers, s.body)
}
