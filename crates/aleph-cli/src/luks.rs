//! Local LUKS2 encryption helper for rootfs images.
//!
//! Wraps a plain ext4 rootfs image into a LUKS2 container on the caller's own
//! machine, so `instance create` does not require a separate root shell
//! script before uploading an encrypted image. Not gated behind the
//! `vprogram` feature: it has no attestation dependency, it is plain CLI
//! infrastructure for building an encrypted disk image locally.
//!
//! Loop devices and device-mapper are privileged, but the CLI process itself
//! must stay unprivileged: the keystore lives in the user's session keyring
//! (Secret Service over the session D-Bus, unreachable from a root shell),
//! and signing and uploading should not run as root either. So only the
//! individual commands that need root (`losetup`, `cryptsetup`, the `dd`
//! copy, `e2fsck`/`resize2fs` on the mapped device) are escalated, through
//! `sudo` when the CLI is not already root ([`PrivRunner`]). The privileged
//! path is not exercised by the test suite; only the pure planning logic
//! (`luks_plan`) and the sudo command assembly are unit tested.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const MIB: u64 = 1024 * 1024;

const SUDO_REQUIRED_MSG: &str = "encrypting a rootfs needs root for its cryptsetup + device-mapper \
steps, and no sudo was found to escalate them: install sudo, run as root, or pre-encrypt with \
cryptsetup and pass --image";

/// Runs the privileged commands of the encryption sequence: directly when
/// the CLI is already root, through `sudo <command>` otherwise. Escalating
/// per-command keeps the CLI process itself unprivileged, so the session
/// keyring, signing, and the upload all run as the invoking user.
pub(crate) struct PrivRunner {
    sudo: Option<PathBuf>,
}

impl PrivRunner {
    /// Detect the privilege mode. As root, commands run directly. Otherwise
    /// `sudo` must be on PATH, and its credential cache is primed with
    /// `sudo -v` right away, so any password prompt happens here, at a clean
    /// point, rather than in the middle of a command that has the LUKS
    /// passphrase piped to its stdin.
    #[cfg(unix)]
    pub(crate) async fn detect() -> Result<Self> {
        if unsafe { libc::geteuid() } == 0 {
            return Ok(Self { sudo: None });
        }
        let Ok(sudo) = which::which("sudo") else {
            bail!(SUDO_REQUIRED_MSG);
        };
        eprintln!(
            "Not running as root: the privileged encryption steps (losetup, cryptsetup, \
             dd, resize2fs) will run via sudo."
        );
        let status = Command::new(&sudo)
            .arg("-v")
            .status()
            .await
            .context("failed to invoke sudo -v")?;
        if !status.success() {
            bail!("sudo -v failed: could not obtain the privileges the encryption steps need");
        }
        Ok(Self { sudo: Some(sudo) })
    }

    /// Non-Unix platforms have no loop devices or device-mapper (and no
    /// sudo), so this path always refuses.
    #[cfg(not(unix))]
    pub(crate) async fn detect() -> Result<Self> {
        bail!(SUDO_REQUIRED_MSG)
    }

    /// A `Command` for `program`, escalated through sudo when not root.
    /// `program` may be a bare name for standard tools (`dd`, `e2fsck`);
    /// under sudo those resolve via sudo's own secure PATH.
    fn cmd(&self, program: &Path) -> Command {
        match &self.sudo {
            None => Command::new(program),
            Some(sudo) => {
                let mut cmd = Command::new(sudo);
                cmd.arg(program);
                cmd
            }
        }
    }
}

/// Wrap the plain ext4 image at `plain` into a new LUKS2 container at `out`.
///
/// `size_mib`, if given, fixes the output image size in MiB; otherwise the
/// output is sized to the plain image plus 64 MiB of LUKS2 header and
/// filesystem-resize slack (see `luks_plan`). `passphrase` unlocks the
/// container; it is passed to `cryptsetup` over stdin, never as an argv
/// argument, so it never appears in the process table.
///
/// Needs the `cryptsetup` and `losetup` binaries on PATH, and root for the
/// privileged steps: when the CLI is not root itself they run via `sudo`
/// (see [`PrivRunner`]). The loop device attached during this call, and the
/// `aleph-luks-<pid>` device-mapper entry it opens, are torn down before
/// returning on every exit path, success or failure.
pub async fn encrypt_rootfs(
    plain: &Path,
    out: &Path,
    size_mib: Option<u64>,
    passphrase: &str,
) -> Result<()> {
    let runner = PrivRunner::detect().await?;

    let plain_size = tokio::fs::metadata(plain)
        .await
        .with_context(|| format!("failed to stat plain rootfs image {}", plain.display()))?
        .len();
    let planned_mib = luks_plan(plain_size, size_mib)?;

    let cryptsetup = find_on_path("cryptsetup", "cryptsetup")?;
    let losetup = find_on_path("losetup", "util-linux (which includes losetup)")?;

    {
        let file = tokio::fs::File::create(out)
            .await
            .with_context(|| format!("failed to create {}", out.display()))?;
        file.set_len(planned_mib * MIB)
            .await
            .with_context(|| format!("failed to size {} to {planned_mib} MiB", out.display()))?;
    }

    let loop_dev = losetup_attach(&runner, &losetup, out).await?;
    let mapper_name = format!("aleph-luks-{}", std::process::id());

    let fill_result = fill_luks_container(
        &runner,
        &cryptsetup,
        &loop_dev,
        &mapper_name,
        plain,
        passphrase,
    )
    .await;

    // Cleanup runs unconditionally so the loop device and the device-mapper
    // entry never outlive this call, whether `fill_luks_container` above
    // succeeded or failed partway through (no async-drop guard is available,
    // so this explicit sequence is the reliable shape). `luksClose` is safe
    // to attempt even when `luksOpen` never ran or failed: it just reports
    // "not active" and fails, which we log and otherwise ignore, so it never
    // shadows the real error from `fill_result`.
    if let Err(e) = cryptsetup_luks_close(&runner, &cryptsetup, &mapper_name).await {
        eprintln!("warning: {e}");
    }
    if let Err(e) = losetup_detach(&runner, &losetup, &loop_dev).await {
        eprintln!("warning: {e}");
    }

    fill_result
}

/// The privileged middle of `encrypt_rootfs`: format the loop device as
/// LUKS2, open it, copy the plain image onto the mapped block device, and
/// check/resize its filesystem to fill the container. Isolated in its own
/// function so `encrypt_rootfs` can capture this step's result and still run
/// loop/mapper cleanup unconditionally afterward.
async fn fill_luks_container(
    runner: &PrivRunner,
    cryptsetup: &Path,
    loop_dev: &str,
    mapper_name: &str,
    plain: &Path,
    passphrase: &str,
) -> Result<()> {
    cryptsetup_luks_format(runner, cryptsetup, loop_dev, passphrase).await?;
    cryptsetup_luks_open(runner, cryptsetup, loop_dev, mapper_name, passphrase).await?;
    let mapper_path = format!("/dev/mapper/{mapper_name}");
    copy_plain_image(runner, plain, &mapper_path).await?;
    check_and_resize_filesystem(runner, &mapper_path).await?;
    Ok(())
}

/// Plan the output LUKS2 image size in MiB: `size_mib` if given, else the
/// plain image size (rounded up to whole MiB) plus 64 MiB of slack for the
/// LUKS2 header and filesystem growth. Errors if an explicit `size_mib` is
/// smaller than the plain image, since the plain rootfs would not fit.
pub(crate) fn luks_plan(plain_size_bytes: u64, size_mib: Option<u64>) -> Result<u64> {
    let plain_mib = plain_size_bytes.div_ceil(MIB);
    match size_mib {
        Some(explicit) if explicit < plain_mib => bail!(
            "requested LUKS image size ({explicit} MiB) is smaller than the plain rootfs image ({plain_mib} MiB)"
        ),
        Some(explicit) => Ok(explicit),
        None => Ok(plain_mib + 64),
    }
}

/// Locate `bin` on PATH. `install_hint` names the package to install,
/// mirroring the error style used by `veritysetup.rs` and `sevctl.rs`.
fn find_on_path(bin: &str, install_hint: &str) -> Result<PathBuf> {
    which::which(bin).map_err(|_| {
        anyhow::anyhow!(
            "{bin} not found in PATH. Install {install_hint} and ensure it is executable."
        )
    })
}

/// Run `cmd`, capture stdout, and error with the command's stderr on a
/// non-zero exit. Used for steps that need the command's output (only
/// `losetup --find --show`, to capture the assigned loop device).
async fn run_capturing_stdout(cmd: &mut Command, description: &str) -> Result<String> {
    let output = cmd
        .output()
        .await
        .with_context(|| format!("failed to invoke {description}"))?;
    if !output.status.success() {
        bail!(
            "{description} failed (exit code {}):\n{}",
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Run `cmd` and error with its stderr on a non-zero exit; stdout is
/// discarded.
async fn run_checked(cmd: &mut Command, description: &str) -> Result<()> {
    run_capturing_stdout(cmd, description).await.map(|_| ())
}

/// Run `cmd` with `stdin_data` written to its stdin (no trailing newline)
/// and then closed, so the child sees EOF after the data. Used for the two
/// `cryptsetup` steps that take the passphrase over stdin instead of argv,
/// so it never shows up in the process table. Safe under sudo escalation
/// too: sudo prompts on the controlling tty, not stdin, so the piped
/// passphrase is never mistaken for a sudo password (and `PrivRunner::detect`
/// primes the credential cache up front anyway).
async fn run_with_stdin(cmd: &mut Command, stdin_data: &str, description: &str) -> Result<()> {
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn {description}"))?;
    {
        let mut stdin = child
            .stdin
            .take()
            .expect("stdin was configured as piped above");
        stdin
            .write_all(stdin_data.as_bytes())
            .await
            .with_context(|| format!("failed to write to {description} stdin"))?;
        // `stdin` drops here, closing the pipe so the child sees EOF instead
        // of blocking for more input.
    }
    let output = child
        .wait_with_output()
        .await
        .with_context(|| format!("failed to wait for {description}"))?;
    if !output.status.success() {
        bail!(
            "{description} failed (exit code {}):\n{}",
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

async fn losetup_attach(runner: &PrivRunner, losetup: &Path, image: &Path) -> Result<String> {
    let mut cmd = runner.cmd(losetup);
    cmd.arg("--find").arg("--show").arg(image);
    let loop_dev = run_capturing_stdout(&mut cmd, "losetup --find --show").await?;
    if loop_dev.is_empty() {
        bail!("losetup --find --show produced no loop device path");
    }
    Ok(loop_dev)
}

async fn losetup_detach(runner: &PrivRunner, losetup: &Path, loop_dev: &str) -> Result<()> {
    let mut cmd = runner.cmd(losetup);
    cmd.arg("-d").arg(loop_dev);
    run_checked(&mut cmd, "losetup -d").await
}

async fn cryptsetup_luks_format(
    runner: &PrivRunner,
    cryptsetup: &Path,
    loop_dev: &str,
    passphrase: &str,
) -> Result<()> {
    let mut cmd = runner.cmd(cryptsetup);
    cmd.arg("luksFormat")
        .arg("--type")
        .arg("luks2")
        .arg("--batch-mode")
        .arg(loop_dev)
        .arg("-");
    run_with_stdin(&mut cmd, passphrase, "cryptsetup luksFormat").await
}

async fn cryptsetup_luks_open(
    runner: &PrivRunner,
    cryptsetup: &Path,
    loop_dev: &str,
    mapper_name: &str,
    passphrase: &str,
) -> Result<()> {
    let mut cmd = runner.cmd(cryptsetup);
    cmd.arg("luksOpen").arg(loop_dev).arg(mapper_name).arg("-");
    run_with_stdin(&mut cmd, passphrase, "cryptsetup luksOpen").await
}

async fn cryptsetup_luks_close(
    runner: &PrivRunner,
    cryptsetup: &Path,
    mapper_name: &str,
) -> Result<()> {
    let mut cmd = runner.cmd(cryptsetup);
    cmd.arg("luksClose").arg(mapper_name);
    run_checked(&mut cmd, "cryptsetup luksClose").await
}

async fn copy_plain_image(runner: &PrivRunner, plain: &Path, mapper_path: &str) -> Result<()> {
    let mut cmd = runner.cmd(Path::new("dd"));
    cmd.arg(format!("if={}", plain.display()))
        .arg(format!("of={mapper_path}"))
        .arg("bs=4M")
        .arg("conv=fsync");
    run_checked(&mut cmd, "dd").await
}

/// `e2fsck -fp` the mapped block device, then `resize2fs` it to fill the
/// container. `e2fsck` exit codes 1 (errors corrected) and 2 (errors
/// corrected, reboot advised) are expected and harmless on a freshly copied
/// loop image; only 4+ (uncorrected errors or worse) is a real failure.
async fn check_and_resize_filesystem(runner: &PrivRunner, mapper_path: &str) -> Result<()> {
    let mut fsck = runner.cmd(Path::new("e2fsck"));
    fsck.arg("-fp").arg(mapper_path);
    let output = fsck.output().await.context("failed to invoke e2fsck")?;
    let code = output.status.code().unwrap_or(-1);
    if !(0..=3).contains(&code) {
        bail!(
            "e2fsck -fp {mapper_path} failed (exit code {code}):\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let mut resize = runner.cmd(Path::new("resize2fs"));
    resize.arg(mapper_path);
    run_checked(&mut resize, "resize2fs").await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_defaults_to_plain_plus_64_mib() {
        assert_eq!(luks_plan(1024 * 1024 * 1024, None).unwrap(), 1024 + 64);
    }

    #[test]
    fn plan_honors_an_explicit_size() {
        assert_eq!(luks_plan(1024 * 1024, Some(4096)).unwrap(), 4096);
    }

    #[test]
    fn plan_rejects_a_size_smaller_than_the_plain_image() {
        assert!(luks_plan(10 * 1024 * 1024 * 1024, Some(1024)).is_err());
    }

    #[test]
    fn direct_mode_runs_the_program_itself() {
        let runner = PrivRunner { sudo: None };
        let cmd = runner.cmd(Path::new("/sbin/cryptsetup"));
        assert_eq!(cmd.as_std().get_program(), "/sbin/cryptsetup");
    }

    #[test]
    fn sudo_mode_prefixes_the_program_with_sudo() {
        let runner = PrivRunner {
            sudo: Some(PathBuf::from("/usr/bin/sudo")),
        };
        let cmd = runner.cmd(Path::new("/sbin/cryptsetup"));
        assert_eq!(cmd.as_std().get_program(), "/usr/bin/sudo");
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert_eq!(args, ["/sbin/cryptsetup"]);
    }
}
