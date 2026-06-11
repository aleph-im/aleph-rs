use aleph_microvm::asgi::{run_success_to_parts, scope_from_parts};

#[test]
fn scope_splits_path_and_query() {
    let scope = scope_from_parts("POST", "/api/items?limit=5&q=x", &[(b"h".to_vec(), b"v".to_vec())], b"payload".to_vec());
    assert_eq!(scope.method, "POST");
    assert_eq!(scope.path, "/api/items");
    assert_eq!(scope.query_string.as_ref(), b"limit=5&q=x");
    assert_eq!(scope.body.as_ref(), b"payload");
}

#[test]
fn scope_handles_no_query() {
    let scope = scope_from_parts("GET", "/", &[], b"".to_vec());
    assert_eq!(scope.path, "/");
    assert_eq!(scope.query_string.as_ref(), b"");
}

#[test]
fn run_success_maps_status_headers_body() {
    use aleph_microvm::protocol::RunSuccess;
    let s = RunSuccess { status: 200, headers: vec![(b"x-a".to_vec(), b"1".to_vec())], body: b"ok".to_vec() };
    let (status, headers, body) = run_success_to_parts(s);
    assert_eq!(status, 200);
    assert_eq!(headers, vec![("x-a".to_string(), "1".to_string())]);
    assert_eq!(body, b"ok");
}
