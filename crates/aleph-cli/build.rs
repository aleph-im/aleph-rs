//! Captures compile-time git metadata exposed via `aleph --version`:
//! - `ALEPH_GIT_COMMIT`: short SHA of HEAD, with `-dirty` suffix if the working
//!   tree has uncommitted changes.
//! - `ALEPH_COMMIT_DATE`: ISO date of the HEAD commit.
//!
//! Falls back to "unknown" when git is unavailable (e.g. building from a release
//! tarball without `.git`).
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/index");

    let commit = run_git(&["rev-parse", "--short=10", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let dirty = !Command::new("git")
        .args(["diff", "--quiet", "HEAD"])
        .status()
        .map(|s| s.success())
        .unwrap_or(true);
    let commit_marker = if dirty {
        format!("{commit}-dirty")
    } else {
        commit
    };
    println!("cargo:rustc-env=ALEPH_GIT_COMMIT={commit_marker}");

    let commit_date = run_git(&["show", "-s", "--format=%cd", "--date=short", "HEAD"])
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=ALEPH_COMMIT_DATE={commit_date}");
}

fn run_git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}
