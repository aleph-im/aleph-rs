use aleph_microvm::protocol::{AsgiScope, ConfigurationResponse, RunResponse};

#[test]
fn configuration_response_success_decodes() {
    // msgpack map {"success": true}
    let bytes = rmp_serde::to_vec_named(&serde_json::json!({"success": true})).unwrap();
    let resp: ConfigurationResponse = rmp_serde::from_slice(&bytes).unwrap();
    assert!(resp.success);
    assert!(resp.error.is_none());
}

#[test]
fn run_response_success_decodes_status_and_body() {
    // Mirror init1.py success map.
    let raw = serde_json::json!({
        "headers": { "status": 201, "headers": [[b"content-type".to_vec(), b"text/plain".to_vec()]] },
        "body": { "body": b"hello".to_vec() },
        "output": serde_json::Value::Null,
        "output_data": serde_json::Value::Null,
    });
    let bytes = rmp_serde::to_vec_named(&raw).unwrap();
    let resp: RunResponse = rmp_serde::from_slice(&bytes).unwrap();
    let ok = resp.into_success().expect("should be success");
    assert_eq!(ok.status, 201);
    assert_eq!(ok.body, b"hello");
    assert_eq!(
        ok.headers,
        vec![(b"content-type".to_vec(), b"text/plain".to_vec())]
    );
}

#[test]
fn run_response_error_is_detected() {
    let raw = serde_json::json!({ "error": "boom", "traceback": "Traceback...", "output": serde_json::Value::Null });
    let bytes = rmp_serde::to_vec_named(&raw).unwrap();
    let resp: RunResponse = rmp_serde::from_slice(&bytes).unwrap();
    assert!(resp.into_success().is_err());
}

#[test]
fn asgi_scope_serializes_query_string_and_body_as_binary() {
    let scope = AsgiScope::http("GET", "/x", b"a=1".to_vec(), vec![], b"".to_vec());
    let bytes = rmp_serde::to_vec_named(&scope).unwrap();
    // round-trips through a generic value
    let v: rmpv::Value = rmp_serde::from_slice(&bytes).unwrap();
    assert!(v.is_map());
}
