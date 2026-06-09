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
    // `post create` without --content blocks reading stdin, keeping the
    // process alive until the signal arrives. A raw `--ccn` URL keeps the
    // test hermetic: `run()`'s URL resolution short-circuits on the raw URL
    // instead of depending on whatever default network the machine happens
    // to have configured. The URL is never contacted (--dry-run, and the
    // process blocks on stdin before any network call).
    let private_key_hex = hex::encode([1u8; 32]);
    let mut child = Command::new(env!("CARGO_BIN_EXE_aleph"))
        .args([
            "--ccn",
            "http://127.0.0.1:1/",
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

    // Take the stdin handle and hold it open. Child::wait() otherwise closes
    // the parent's stdin before waiting, which makes the child read EOF and
    // exit 1 from the stdin read - racing our SIGINT. On a slow runner that
    // EOF path can win (observed on macOS CI), masking the regression. With
    // stdin held open, the only way out for the child is the signal we send.
    let _stdin = child.stdin.take().expect("child stdin was piped");

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
