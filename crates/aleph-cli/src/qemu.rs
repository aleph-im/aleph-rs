//! Shell-out helper for `qemu-system-x86_64`, used by `aleph vprogram run`
//! to boot a V-PROGRAM locally WITHOUT SEV-SNP. Kept off the SDK so the SDK
//! stays library-clean (no subprocess invocations).
//!
//! The argv mirrors the production launch (`aleph-vm` supervisor-controller
//! `build_snp_argv`) where the guest can tell: direct kernel boot, the
//! materialized measured cmdline, and the positional read-only virtio disk
//! order (vda platform rootfs, vdb its hash tree, vdc workload, vdd its hash
//! tree, then one data+tree pair per verified volume). It deliberately
//! differs where the guest cannot tell: SeaBIOS instead of the bundle's
//! OVMF, `-cpu max` instead of the measured EPYC model, no SNP object, and
//! SLIRP user networking with one host port forward instead of a tap.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use aleph_sdk::vprogram::manifest::LOCAL_MODE_TOKEN;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QemuError {
    #[error(
        "qemu-system-x86_64 not found in PATH. Install QEMU (Debian/Ubuntu: qemu-system-x86; Fedora: qemu-system-x86; macOS: brew install qemu) and ensure it is executable."
    )]
    NotFound,
    #[error("failed to invoke qemu-system-x86_64: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct Qemu {
    pub(crate) path: PathBuf,
}

impl Qemu {
    /// Locate `qemu-system-x86_64` on PATH.
    pub fn find() -> Result<Self, QemuError> {
        Self::find_in(std::env::var_os("PATH"))
    }

    /// `find`, searching `path` (a PATH-style list) instead of the process
    /// environment.
    pub(crate) fn find_in<P: AsRef<OsStr>>(path: Option<P>) -> Result<Self, QemuError> {
        let path = path.as_ref().map(AsRef::as_ref);
        which::which_in_global("qemu-system-x86_64", path)
            .ok()
            .and_then(|mut found| found.next())
            .map(|path| Self { path })
            .ok_or(QemuError::NotFound)
    }
}

/// QEMU accelerator: KVM when `/dev/kvm` is usable, else pure emulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accel {
    Kvm,
    Tcg,
}

impl Accel {
    /// KVM if `/dev/kvm` can be opened read-write by this process (the same
    /// check QEMU itself performs), otherwise TCG.
    pub fn detect() -> Self {
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/kvm")
        {
            Ok(_) => Accel::Kvm,
            Err(_) => Accel::Tcg,
        }
    }

    fn qemu_name(self) -> &'static str {
        match self {
            Accel::Kvm => "kvm",
            Accel::Tcg => "tcg",
        }
    }
}

/// Everything the local boot needs; `argv` is pure so it can be pinned by
/// tests.
#[derive(Debug, Clone)]
pub struct LocalBootSpec {
    pub kernel: PathBuf,
    pub initrd: PathBuf,
    /// The materialized manifest cmdline, WITHOUT the local token (appended
    /// here, and only here).
    pub cmdline: String,
    /// Read-only raw disks in guest device order (vda, vdb, ...).
    pub disks: Vec<PathBuf>,
    pub vcpus: u32,
    pub mem_mib: u32,
    /// Host side of the SLIRP forward, bound on 127.0.0.1.
    pub host_port: u16,
    /// Guest side: the agent's port.
    pub guest_port: u16,
    /// False adds `restrict=on`: no outbound traffic, the forward still works.
    pub internet: bool,
    pub accel: Accel,
}

impl LocalBootSpec {
    pub fn argv(&self) -> Vec<OsString> {
        let mut argv: Vec<OsString> = Vec::new();
        let mut push = |items: &[&str]| {
            argv.extend(items.iter().map(OsString::from));
        };
        push(&[
            "-machine",
            &format!("q35,accel={}", self.accel.qemu_name()),
            "-cpu",
            "max",
            "-m",
            &self.mem_mib.to_string(),
            "-smp",
            &self.vcpus.to_string(),
            "-nographic",
            "-no-reboot",
        ]);
        argv.push("-kernel".into());
        argv.push(self.kernel.clone().into_os_string());
        argv.push("-initrd".into());
        argv.push(self.initrd.clone().into_os_string());
        argv.push("-append".into());
        argv.push(format!("{} {LOCAL_MODE_TOKEN}", self.cmdline).into());
        for disk in &self.disks {
            argv.push("-drive".into());
            argv.push(
                format!(
                    "file={},format=raw,if=virtio,readonly=on",
                    qemu_option_value(disk)
                )
                .into(),
            );
        }
        let mut netdev = format!(
            "user,id=n0,hostfwd=tcp:127.0.0.1:{}-:{}",
            self.host_port, self.guest_port
        );
        if !self.internet {
            netdev.push_str(",restrict=on");
        }
        argv.push("-netdev".into());
        argv.push(netdev.into());
        argv.push("-device".into());
        argv.push("virtio-net-pci,netdev=n0".into());
        argv
    }
}

/// QEMU splits option strings on commas; a literal comma in a value is
/// escaped by doubling it.
fn qemu_option_value(path: &Path) -> String {
    path.to_string_lossy().replace(',', ",,")
}

/// A running QEMU. `kill_on_drop` guarantees no error path leaks the VM;
/// `shutdown` is the polite path (SIGTERM, then SIGKILL after 5 s).
pub struct QemuProcess {
    child: tokio::process::Child,
}

impl QemuProcess {
    pub fn spawn(qemu: &Qemu, argv: &[OsString]) -> Result<Self, QemuError> {
        let child = tokio::process::Command::new(&qemu.path)
            .args(argv)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        Ok(Self { child })
    }

    pub fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
        self.child.stdout.take()
    }

    pub fn take_stderr(&mut self) -> Option<tokio::process::ChildStderr> {
        self.child.stderr.take()
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.wait().await
    }

    pub async fn shutdown(mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            // SAFETY: plain kill(2) on our own child's pid; no memory involved.
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
        }
        match tokio::time::timeout(std::time::Duration::from_secs(5), self.child.wait()).await {
            Ok(status) => status.map(|_| ()),
            Err(_) => self.child.kill().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> LocalBootSpec {
        LocalBootSpec {
            kernel: PathBuf::from("/c/kernel"),
            initrd: PathBuf::from("/c/initrd"),
            cmdline:
                "console=ttyS0 root=/dev/mapper/verity-root ro roothash=aa workload_roothash=bb"
                    .to_string(),
            disks: vec![
                PathBuf::from("/c/platform_rootfs"),
                PathBuf::from("/c/platform_hash_tree"),
                PathBuf::from("/w/workload.ext4"),
                PathBuf::from("/w/workload.verity"),
            ],
            vcpus: 2,
            mem_mib: 1024,
            host_port: 8080,
            guest_port: 8443,
            internet: true,
            accel: Accel::Kvm,
        }
    }

    fn strings(argv: Vec<OsString>) -> Vec<String> {
        argv.into_iter().map(|a| a.into_string().unwrap()).collect()
    }

    #[test]
    fn argv_pins_the_production_disk_order_and_the_local_token() {
        let argv = strings(spec().argv());
        assert_eq!(
            argv,
            vec![
                "-machine",
                "q35,accel=kvm",
                "-cpu",
                "max",
                "-m",
                "1024",
                "-smp",
                "2",
                "-nographic",
                "-no-reboot",
                "-kernel",
                "/c/kernel",
                "-initrd",
                "/c/initrd",
                "-append",
                "console=ttyS0 root=/dev/mapper/verity-root ro roothash=aa workload_roothash=bb aleph_local=1",
                "-drive",
                "file=/c/platform_rootfs,format=raw,if=virtio,readonly=on",
                "-drive",
                "file=/c/platform_hash_tree,format=raw,if=virtio,readonly=on",
                "-drive",
                "file=/w/workload.ext4,format=raw,if=virtio,readonly=on",
                "-drive",
                "file=/w/workload.verity,format=raw,if=virtio,readonly=on",
                "-netdev",
                "user,id=n0,hostfwd=tcp:127.0.0.1:8080-:8443",
                "-device",
                "virtio-net-pci,netdev=n0",
            ]
        );
    }

    #[test]
    fn argv_appends_volume_pairs_after_the_workload_pair() {
        let mut s = spec();
        s.disks.push(PathBuf::from("/v/0.ext4"));
        s.disks.push(PathBuf::from("/v/0.verity"));
        let argv = strings(s.argv());
        let drives: Vec<&String> = argv.iter().filter(|a| a.starts_with("file=")).collect();
        assert_eq!(drives.len(), 6);
        assert_eq!(drives[4], "file=/v/0.ext4,format=raw,if=virtio,readonly=on");
        assert_eq!(
            drives[5],
            "file=/v/0.verity,format=raw,if=virtio,readonly=on"
        );
    }

    #[test]
    fn argv_restricts_slirp_without_internet_and_falls_back_to_tcg() {
        let mut s = spec();
        s.internet = false;
        s.accel = Accel::Tcg;
        s.host_port = 9000;
        let argv = strings(s.argv());
        assert!(argv.contains(&"q35,accel=tcg".to_string()));
        assert!(
            argv.contains(&"user,id=n0,hostfwd=tcp:127.0.0.1:9000-:8443,restrict=on".to_string())
        );
    }

    #[test]
    fn argv_doubles_commas_in_disk_paths() {
        // QEMU option syntax: a literal comma inside a value is written ",,".
        let mut s = spec();
        s.disks[2] = PathBuf::from("/tmp/a,b/workload.ext4");
        let argv = strings(s.argv());
        assert!(argv.contains(
            &"file=/tmp/a,,b/workload.ext4,format=raw,if=virtio,readonly=on".to_string()
        ));
    }

    #[test]
    fn find_in_reports_a_missing_binary_with_an_install_hint() {
        let empty = tempfile::tempdir().unwrap();
        let err = Qemu::find_in(Some(empty.path().as_os_str())).unwrap_err();
        assert!(err.to_string().contains("qemu-system-x86_64"));
        assert!(err.to_string().contains("qemu-system-x86"));
    }
}
