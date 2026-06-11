use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MicrovmError {
    #[error("firecracker binary not found on PATH; install it from https://github.com/firecracker-microvm/firecracker/releases")]
    FirecrackerMissing,
    #[error("/dev/kvm is not accessible: {0}. Add your user to the `kvm` group (`sudo usermod -aG kvm $USER`) and re-login")]
    KvmUnavailable(String),
    #[error("timed out waiting for the VM init to connect")]
    InitTimeout,
    #[error("the runtime rejected the program configuration:\n{0}")]
    ConfigRejected(String),
    #[error("VM produced no response (it may have crashed); serial console:\n{0}")]
    NoResponse(String),
    #[error("msgpack error: {0}")]
    Msgpack(String),
    #[error("firecracker API error: {0}")]
    FirecrackerApi(String),
    #[error("artifact download failed: {0}")]
    Download(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, MicrovmError>;
