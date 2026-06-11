use aleph_microvm::vsock::VsockChannel;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

/// Read the "CONNECT 52\n" line and ack with "OK 52\n".
async fn handshake(sock: &mut tokio::net::UnixStream) {
    let mut line = Vec::new();
    loop {
        let mut b = [0u8; 1];
        sock.read_exact(&mut b).await.unwrap();
        line.push(b[0]);
        if b[0] == b'\n' {
            break;
        }
    }
    assert_eq!(line, b"CONNECT 52\n");
    sock.write_all(b"OK 52\n").await.unwrap();
}

/// Read a "<len>\n" prefix and then exactly `len` payload bytes.
async fn read_len_prefixed(sock: &mut tokio::net::UnixStream) {
    let mut lenline = Vec::new();
    loop {
        let mut b = [0u8; 1];
        sock.read_exact(&mut b).await.unwrap();
        lenline.push(b[0]);
        if b[0] == b'\n' {
            break;
        }
    }
    let n: usize = String::from_utf8(lenline[..lenline.len() - 1].to_vec())
        .unwrap()
        .parse()
        .unwrap();
    let mut payload = vec![0u8; n];
    sock.read_exact(&mut payload).await.unwrap();
}

/// Mock config guest: handshake, read the length-prefixed payload, reply with
/// msgpack, then KEEP the connection open briefly. This proves the client
/// decodes the complete value via the incremental path without relying on EOF.
async fn mock_config_guest(listener: UnixListener, reply: Vec<u8>) {
    let (mut sock, _) = listener.accept().await.unwrap();
    handshake(&mut sock).await;
    read_len_prefixed(&mut sock).await;
    sock.write_all(&reply).await.unwrap();
    sock.flush().await.unwrap();
    // Hold the connection open. The client must return from send_config before
    // this sleep elapses, driven purely by a successful msgpack decode (no EOF).
    tokio::time::sleep(Duration::from_secs(2)).await;
    // Dropping `sock` here finally closes the connection.
}

/// Mock run guest: handshake, drain the request (no length prefix, the client
/// does NOT half-close), reply, then close the write side so the client's
/// read-to-EOF returns.
async fn mock_run_guest(listener: UnixListener, reply: Vec<u8>, request_len: usize) {
    let (mut sock, _) = listener.accept().await.unwrap();
    handshake(&mut sock).await;
    // The client writes the payload but does NOT shut down its write side, so we
    // read exactly the expected number of request bytes rather than to EOF.
    let mut payload = vec![0u8; request_len];
    sock.read_exact(&mut payload).await.unwrap();
    sock.write_all(&reply).await.unwrap();
    // Close our write side so the client's read_to_end observes EOF.
    sock.shutdown().await.unwrap();
}

#[tokio::test]
async fn send_config_parses_success_without_eof() {
    let dir = tempfile::tempdir().unwrap();
    let uds = dir.path().join("v.sock");
    let listener = UnixListener::bind(&uds).unwrap();
    let reply = rmp_serde::to_vec_named(&serde_json::json!({"success": true})).unwrap();
    let server = tokio::spawn(mock_config_guest(listener, reply));
    let chan = VsockChannel::new(uds);
    // This must complete well before the guest's 2s hold expires.
    let resp = tokio::time::timeout(Duration::from_secs(1), chan.send_config(&[1, 2, 3, 4]))
        .await
        .expect("send_config must return before the guest closes the connection")
        .unwrap();
    assert!(resp.success);
    server.await.unwrap();
}

#[tokio::test]
async fn send_run_reads_to_eof() {
    let dir = tempfile::tempdir().unwrap();
    let uds = dir.path().join("v.sock");
    let listener = UnixListener::bind(&uds).unwrap();
    let reply = rmp_serde::to_vec_named(&serde_json::json!({
        "headers": {"status": 200, "headers": []},
        "body": {"body": b"hi".to_vec()},
        "output": serde_json::Value::Null, "output_data": serde_json::Value::Null
    }))
    .unwrap();
    let request = [9u8, 9, 9];
    let server = tokio::spawn(mock_run_guest(listener, reply, request.len()));
    let chan = VsockChannel::new(uds);
    let raw = chan.send_run(&request).await.unwrap();
    let resp: aleph_microvm::protocol::RunResponse = rmp_serde::from_slice(&raw).unwrap();
    assert_eq!(resp.into_success().unwrap().body, b"hi");
    server.await.unwrap();
}
