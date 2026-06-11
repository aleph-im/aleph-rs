use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;

/// Upper bound on how long we wait for the guest to reply to a config or run request.
const REPLY_TIMEOUT: Duration = Duration::from_secs(60);

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
    ///
    /// The guest keeps the config connection open after replying, so we must not
    /// rely on EOF. Instead we read incrementally and return as soon as a complete
    /// msgpack value decodes. The whole exchange is bounded by `REPLY_TIMEOUT`.
    pub async fn send_config(&self, payload: &[u8]) -> Result<ConfigurationResponse> {
        let mut stream = self.connect_port_52().await?;
        let header = format!("{}\n", payload.len());
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(payload).await?;
        stream.flush().await?;

        let read = async {
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                // Try to decode what we have before blocking on more bytes.
                if !buf.is_empty() {
                    if let Ok(resp) = rmp_serde::from_slice::<ConfigurationResponse>(&buf) {
                        return Ok(resp);
                    }
                }
                let n = stream.read(&mut chunk).await?;
                if n == 0 {
                    // Guest closed without a complete value; surface the decode error.
                    return rmp_serde::from_slice(&buf)
                        .map_err(|e| MicrovmError::Msgpack(e.to_string()));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
        };

        match tokio::time::timeout(REPLY_TIMEOUT, read).await {
            Ok(res) => res,
            Err(_) => Err(MicrovmError::ReplyTimeout),
        }
    }

    /// Run send: write msgpack, then read the raw reply to EOF.
    ///
    /// The guest closes its write side after replying, which yields EOF. We do not
    /// half-close our side (that risks a reset before the guest replies). The read
    /// is bounded by `REPLY_TIMEOUT` so it can never hang forever.
    pub async fn send_run(&self, payload: &[u8]) -> Result<Vec<u8>> {
        let mut stream = self.connect_port_52().await?;
        stream.write_all(payload).await?;
        stream.flush().await?;

        let read = async {
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await?;
            Ok(buf)
        };

        match tokio::time::timeout(REPLY_TIMEOUT, read).await {
            Ok(res) => res,
            Err(_) => Err(MicrovmError::ReplyTimeout),
        }
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
    pub fn bind(uds_path: &Path) -> Result<Self> {
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

pub fn ready_path(uds_path: &Path) -> PathBuf {
    let mut s = uds_path.as_os_str().to_owned();
    s.push("_52");
    PathBuf::from(s)
}

fn decode_version(buf: &[u8]) -> Option<String> {
    let v: serde_json::Value = rmp_serde::from_slice(buf).ok()?;
    v.get("version")?.as_str().map(|s| s.to_string())
}
