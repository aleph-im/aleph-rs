use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interface {
    Asgi,
    Executable,
}

impl Interface {
    pub fn as_str(self) -> &'static str {
        match self {
            Interface::Asgi => "asgi",
            Interface::Executable => "executable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Zip,
    Squashfs,
}

impl Encoding {
    pub fn as_str(self) -> &'static str {
        match self {
            Encoding::Zip => "zip",
            Encoding::Squashfs => "squashfs",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Volume {
    pub mount: String,
    pub device: String,
    pub read_only: bool,
}

/// Everything the crate needs to boot one program VM. Assembled by the CLI.
#[derive(Debug, Clone)]
pub struct LocalVmConfig {
    pub kernel_path: PathBuf,
    pub rootfs_path: PathBuf,
    /// Path to the packaged code archive (zip) or squashfs image.
    pub code_path: PathBuf,
    pub encoding: Encoding,
    pub interface: Interface,
    pub entrypoint: String,
    pub vm_hash: String,
    pub vcpus: u32,
    pub mem_mib: u32,
    pub variables: Vec<(String, String)>,
    /// Extra drives mounted as volumes (M1: typically empty).
    pub volumes: Vec<(PathBuf, Volume)>,
}
