//! Host-side driver for running aleph programs locally in Firecracker.

pub mod asgi;
pub mod cache;
pub mod config;
pub mod error;
pub mod firecracker;
pub mod preflight;
pub mod protocol;
pub mod server;
pub mod vsock;

pub use config::{Encoding, Interface, LocalVmConfig, Volume};
pub use error::MicrovmError;

use std::path::PathBuf;
use std::time::Duration;

use crate::error::Result;
use crate::firecracker::{FcConfig, FirecrackerProcess};
use crate::protocol::ConfigurationPayload;
use crate::vsock::{ReadyServer, VsockChannel};

/// A booted, configured program VM ready to serve requests.
pub struct RunningVm {
    pub runtime_version: String,
    pub channel: VsockChannel,
    fc: FirecrackerProcess,
    runtime_dir: PathBuf,
    stdout: Option<tokio::process::ChildStdout>,
}

impl RunningVm {
    /// Take the serial-console stream for log forwarding (call once).
    pub fn take_console(&mut self) -> Option<tokio::process::ChildStdout> {
        self.stdout.take()
    }

    /// Tear down firecracker and remove the private runtime dir. Idempotent-ish.
    pub async fn shutdown(self) {
        self.fc.shutdown().await;
        let _ = std::fs::remove_dir_all(&self.runtime_dir);
    }
}

pub struct LocalVm;

impl LocalVm {
    /// Boot the VM described by `cfg` using firecracker at `firecracker_bin`.
    /// `runtime_dir` is a private, writable directory for the vsock + api sockets.
    pub async fn launch(
        cfg: &LocalVmConfig,
        firecracker_bin: &PathBuf,
        runtime_dir: PathBuf,
    ) -> Result<RunningVm> {
        std::fs::create_dir_all(&runtime_dir)?;
        let vsock_uds = runtime_dir.join("v.sock");
        let api_sock = runtime_dir.join("fc-api.sock");
        let _ = std::fs::remove_file(&vsock_uds);

        // 1. Listen for the guest readiness announce BEFORE boot.
        let ready = ReadyServer::bind(&vsock_uds)?;

        // 2. Boot firecracker.
        let mut fc = FirecrackerProcess::spawn(firecracker_bin, api_sock).await?;
        let stdout = fc.take_stdout();
        let code_drive = match cfg.encoding {
            Encoding::Squashfs => Some(cfg.code_path.clone()),
            Encoding::Zip => None,
        };
        let fc_cfg = FcConfig {
            kernel: cfg.kernel_path.clone(),
            rootfs: cfg.rootfs_path.clone(),
            code_drive,
            vcpus: cfg.vcpus,
            mem_mib: cfg.mem_mib,
            vsock_uds: vsock_uds.clone(),
            enable_console: true,
        };
        fc.configure_and_start(&fc_cfg).await?;

        // 3. Await init, learn runtime version.
        let runtime_version = ready.wait(Duration::from_secs(30)).await?;

        // 4. Send configuration. For zip, code bytes are inline; for squashfs, code is the mounted drive.
        let code_bytes = match cfg.encoding {
            Encoding::Zip => std::fs::read(&cfg.code_path)?,
            Encoding::Squashfs => Vec::new(),
        };
        let payload = ConfigurationPayload::from_config(cfg, code_bytes)
            .to_msgpack()
            .map_err(|e| crate::error::MicrovmError::Msgpack(e.to_string()))?;
        let channel = VsockChannel::new(vsock_uds);
        let resp = channel.send_config(&payload).await?;
        if !resp.success {
            let detail = resp.traceback.or(resp.error).unwrap_or_default();
            fc.shutdown().await;
            let _ = std::fs::remove_dir_all(&runtime_dir);
            return Err(crate::error::MicrovmError::ConfigRejected(detail));
        }

        Ok(RunningVm {
            runtime_version,
            channel,
            fc,
            runtime_dir,
            stdout,
        })
    }
}
