//! Ctrl+C must terminate the process by SIGINT (WIFSIGNALED), not via a
//! normal exit(130). Bash only compensates for the kernel's "^C" echo by
//! printing a newline before the next prompt when the child was killed by
//! SIGINT; after a normal exit the prompt starts mid-line (right after
//! "^C") and readline's redisplay garbles the next typed command.
#![cfg(unix)]

use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::time::Duration;

#[test]
fn sigint_death_reports_signal_status() {
    // `post create` without --content blocks reading stdin. Keep the write
    // end of the pipe open so the process is still alive when the signal
    // arrives.
    let private_key_hex = hex::encode([1u8; 32]);
    let mut child = Command::new(env!("CARGO_BIN_EXE_aleph"))
        .args([
            "post",
            "create",
            "--type",
            "test",
            "--private-key",
            &private_key_hex,
            "--chain",
            "eth",
            "--dry-run",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn aleph binary");

    // Give the binary time to install its Ctrl+C handler and reach the
    // blocking stdin read; a SIGINT delivered before the handler exists
    // would kill the process by default action and mask the regression.
    std::thread::sleep(Duration::from_millis(500));
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };

    let status = child.wait().expect("failed to wait on aleph binary");
    assert_eq!(
        status.signal(),
        Some(libc::SIGINT),
        "expected death by SIGINT so the shell redraws its prompt on a fresh \
         line, got {status:?}"
    );
}
