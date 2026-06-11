use std::path::PathBuf;
use std::process::Stdio;

use serde_json::{json, Value};
use tokio::process::{Child, Command};

use crate::error::{MicrovmError, Result};

pub fn boot_args(enable_console: bool) -> String {
    let base = "reboot=k panic=1 pci=off nomodule swiotlb=noforce random.trust_cpu=on \
                i8042.noaux i8042.nomux i8042.dumbkbd ro";
    if enable_console {
        format!("console=ttyS0 {base}")
    } else {
        base.to_string()
    }
}

/// Inputs for the Firecracker machine JSON config.
pub struct FcConfig {
    pub kernel: PathBuf,
    pub rootfs: PathBuf,
    pub code_drive: Option<PathBuf>,
    pub vcpus: u32,
    pub mem_mib: u32,
    pub vsock_uds: PathBuf,
    pub enable_console: bool,
}

impl FcConfig {
    pub fn to_json(&self) -> Value {
        let mut drives = vec![json!({
            "drive_id": "rootfs",
            "path_on_host": self.rootfs.to_string_lossy().as_ref(),
            "is_root_device": true,
            "is_read_only": true,
        })];
        if let Some(code) = &self.code_drive {
            drives.push(json!({
                "drive_id": "code",
                "path_on_host": code.to_string_lossy().as_ref(),
                "is_root_device": false,
                "is_read_only": true,
            }));
        }
        json!({
            "boot-source": {
                "kernel_image_path": self.kernel.to_string_lossy().as_ref(),
                "boot_args": boot_args(self.enable_console),
            },
            "drives": drives,
            "machine-config": {
                "vcpu_count": self.vcpus,
                "mem_size_mib": self.mem_mib,
                "smt": false,
            },
            "vsock": {
                "vsock_id": "1",
                "guest_cid": 3,
                "uds_path": self.vsock_uds.to_string_lossy().as_ref(),
            },
        })
    }
}

/// A running firecracker process bound to an API + vsock unix socket.
pub struct FirecrackerProcess {
    child: Child,
    api_sock: PathBuf,
}

impl FirecrackerProcess {
    /// Spawn firecracker with `--api-sock`, piping its stdout/stderr (serial console) for log streaming.
    pub async fn spawn(firecracker_bin: &PathBuf, api_sock: PathBuf) -> Result<Self> {
        let _ = std::fs::remove_file(&api_sock);
        let child = Command::new(firecracker_bin)
            .arg("--api-sock")
            .arg(&api_sock)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        // Wait for the API socket to appear (firecracker creates it on startup).
        for _ in 0..200 {
            if api_sock.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok(FirecrackerProcess { child, api_sock })
    }

    /// Take the child's stdout for log streaming (call once).
    pub fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
        self.child.stdout.take()
    }

    /// PUT the full machine config, then start the instance, via the firecracker REST API
    /// over its unix socket (HTTP/1.1, hand-framed).
    pub async fn configure_and_start(&self, cfg: &FcConfig) -> Result<()> {
        // Firecracker accepts a single PUT to /machine-config, /boot-source, /drives/{id}, /vsock,
        // then PUT /actions {"action_type":"InstanceStart"}. We send them in order.
        let v = cfg.to_json();
        self.put("/boot-source", &v["boot-source"]).await?;
        self.put("/machine-config", &v["machine-config"]).await?;
        for drive in v["drives"].as_array().unwrap() {
            let id = drive["drive_id"].as_str().unwrap();
            self.put(&format!("/drives/{id}"), drive).await?;
        }
        self.put("/vsock", &v["vsock"]).await?;
        self.put("/actions", &json!({"action_type": "InstanceStart"})).await?;
        Ok(())
    }

    async fn put(&self, route: &str, body: &Value) -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::UnixStream;
        let mut stream = UnixStream::connect(&self.api_sock).await?;
        let payload = serde_json::to_vec(body).unwrap();
        let req = format!(
            "PUT {route} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload.len()
        );
        stream.write_all(req.as_bytes()).await?;
        stream.write_all(&payload).await?;
        stream.flush().await?;
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await?;
        let head = String::from_utf8_lossy(&resp);
        let status_ok = head.starts_with("HTTP/1.1 2") || head.starts_with("HTTP/1.0 2");
        if !status_ok {
            return Err(MicrovmError::FirecrackerApi(format!(
                "{route}: {}",
                head.lines().next().unwrap_or("?")
            )));
        }
        Ok(())
    }

    pub async fn shutdown(mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
        let _ = std::fs::remove_file(&self.api_sock);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn boot_args_match_reference() {
        let args = boot_args(true);
        assert!(args.starts_with("console=ttyS0 "));
        assert!(args.contains("reboot=k panic=1 pci=off"));
        assert!(args.ends_with(" ro"));
    }

    #[test]
    fn config_json_has_rootfs_code_and_vsock() {
        let cfg = FcConfig {
            kernel: PathBuf::from("/k/vmlinux"),
            rootfs: PathBuf::from("/r/rootfs.squashfs"),
            code_drive: Some(PathBuf::from("/c/code.squashfs")),
            vcpus: 1,
            mem_mib: 256,
            vsock_uds: PathBuf::from("/run/v.sock"),
            enable_console: true,
        };
        let v = cfg.to_json();
        assert_eq!(v["boot-source"]["kernel_image_path"], "/k/vmlinux");
        let drives = v["drives"].as_array().unwrap();
        assert!(drives.iter().any(|d| d["drive_id"] == "rootfs" && d["is_root_device"] == true));
        assert!(drives.iter().any(|d| d["drive_id"] == "code"));
        assert_eq!(v["vsock"]["uds_path"], "/run/v.sock");
        assert_eq!(v["machine-config"]["vcpu_count"], 1);
    }
}
