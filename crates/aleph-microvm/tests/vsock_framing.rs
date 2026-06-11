use aleph_microvm::vsock::VsockChannel;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

async fn mock_guest(listener: UnixListener, reply: Vec<u8>, expect_len_prefix: bool) {
    let (mut sock, _) = listener.accept().await.unwrap();
    // Expect "CONNECT 52\n"
    let mut line = Vec::new();
    loop {
        let mut b = [0u8; 1];
        sock.read_exact(&mut b).await.unwrap();
        line.push(b[0]);
        if b[0] == b'\n' { break; }
    }
    assert_eq!(line, b"CONNECT 52\n");
    sock.write_all(b"OK 52\n").await.unwrap();
    if expect_len_prefix {
        // read "<len>\n"
        let mut lenline = Vec::new();
        loop {
            let mut b = [0u8; 1];
            sock.read_exact(&mut b).await.unwrap();
            lenline.push(b[0]);
            if b[0] == b'\n' { break; }
        }
        let n: usize = String::from_utf8(lenline[..lenline.len()-1].to_vec()).unwrap().parse().unwrap();
        let mut payload = vec![0u8; n];
        sock.read_exact(&mut payload).await.unwrap();
    } else {
        // send_run: client half-closes after writing; drain until client EOF before replying.
        let mut discard = Vec::new();
        sock.read_to_end(&mut discard).await.unwrap();
    }
    sock.write_all(&reply).await.unwrap();
    sock.shutdown().await.unwrap();
}

#[tokio::test]
async fn send_config_parses_success() {
    let dir = tempfile::tempdir().unwrap();
    let uds = dir.path().join("v.sock");
    let listener = UnixListener::bind(&uds).unwrap();
    let reply = rmp_serde::to_vec_named(&serde_json::json!({"success": true})).unwrap();
    let server = tokio::spawn(mock_guest(listener, reply, true));
    let chan = VsockChannel::new(uds);
    let resp = chan.send_config(&[1, 2, 3, 4]).await.unwrap();
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
    })).unwrap();
    let server = tokio::spawn(mock_guest(listener, reply, false));
    let chan = VsockChannel::new(uds);
    let raw = chan.send_run(&[9, 9, 9]).await.unwrap();
    let resp: aleph_microvm::protocol::RunResponse = rmp_serde::from_slice(&raw).unwrap();
    assert_eq!(resp.into_success().unwrap().body, b"hi");
    server.await.unwrap();
}
