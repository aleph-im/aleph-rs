use std::path::PathBuf;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;

use crate::error::{MicrovmError, Result};
use crate::protocol::ConfigurationResponse;

/// A client to the guest, multiplexed over the firecracker vsock unix socket.
#[derive(Clone)]
pub struct VsockChannel {
    uds_path: PathBuf,
}

impl VsockChannel {
    pub fn new(uds_path: PathBuf) -> Self {
        VsockChannel { uds_path }
    }

    async fn connect_port_52(&self) -> Result<UnixStream> {
        let mut stream = UnixStream::connect(&self.uds_path).await?;
        stream.write_all(b"CONNECT 52\n").await?;
        // Read the "OK <port>\n" ack line.
        read_line(&mut stream).await?;
        Ok(stream)
    }

    /// Length-prefixed config send; returns the parsed ConfigurationResponse.
    pub async fn send_config(&self, payload: &[u8]) -> Result<ConfigurationResponse> {
        let mut stream = self.connect_port_52().await?;
        let header = format!("{}\n", payload.len());
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(payload).await?;
        stream.flush().await?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        rmp_serde::from_slice(&buf).map_err(|e| MicrovmError::Msgpack(e.to_string()))
    }

    /// Run send: write msgpack, half-close write side, read raw reply to EOF.
    pub async fn send_run(&self, payload: &[u8]) -> Result<Vec<u8>> {
        let mut stream = self.connect_port_52().await?;
        stream.write_all(payload).await?;
        // Half-close our write side so the guest sees EOF and can start replying.
        stream.shutdown().await?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        Ok(buf)
    }
}

async fn read_line(stream: &mut UnixStream) -> Result<Vec<u8>> {
    let mut line = Vec::new();
    let mut b = [0u8; 1];
    loop {
        let n = stream.read(&mut b).await?;
        if n == 0 {
            break;
        }
        line.push(b[0]);
        if b[0] == b'\n' {
            break;
        }
    }
    Ok(line)
}

/// Listens on `UDS_52` for the guest's readiness announce, returning the runtime version.
pub struct ReadyServer {
    _listener_path: PathBuf,
    rx: oneshot::Receiver<String>,
}

impl ReadyServer {
    /// Bind `<uds>_52` and spawn an accept task. Call before booting firecracker.
    pub fn bind(uds_path: &PathBuf) -> Result<Self> {
        let listener_path = ready_path(uds_path);
        let _ = std::fs::remove_file(&listener_path);
        let listener = UnixListener::bind(&listener_path)?;
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = Vec::new();
                let _ = sock.read_to_end(&mut buf).await;
                let version = decode_version(&buf).unwrap_or_else(|| "1.0.0".to_string());
                let _ = tx.send(version);
            }
        });
        Ok(ReadyServer { _listener_path: listener_path, rx })
    }

    /// Await the guest announce, with a timeout.
    pub async fn wait(self, timeout: std::time::Duration) -> Result<String> {
        match tokio::time::timeout(timeout, self.rx).await {
            Ok(Ok(v)) => Ok(v),
            _ => Err(MicrovmError::InitTimeout),
        }
    }
}

pub fn ready_path(uds_path: &PathBuf) -> PathBuf {
    let mut s = uds_path.as_os_str().to_owned();
    s.push("_52");
    PathBuf::from(s)
}

fn decode_version(buf: &[u8]) -> Option<String> {
    let v: serde_json::Value = rmp_serde::from_slice(buf).ok()?;
    v.get("version")?.as_str().map(|s| s.to_string())
}
