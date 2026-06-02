use aleph_sdk::crn::CrnClient;
use anyhow::Result;
use futures_util::StreamExt;
use url::Url;

use crate::cli::{CrnArgs, CrnStartArgs, SigningArgs};
use crate::commands::instance_target::resolve_target;
use crate::common::resolve_account;

fn build_client(crn_url: &Url, signing: &SigningArgs) -> Result<CrnClient> {
    let account = resolve_account(&signing.identity)?;
    Ok(CrnClient::new(&account, crn_url.clone())?)
}

pub async fn handle_start(scheduler_url: Url, json: bool, args: CrnStartArgs) -> Result<()> {
    let (vm_id, crn_url) =
        resolve_target(&scheduler_url, &args.vm_id, args.crn.as_deref()).await?;
    let client = build_client(&crn_url, &args.signing)?;
    let response = client.start_instance(&vm_id).await?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "success": response.success,
                "successful": response.successful,
                "failing": response.failing,
                "errors": response.errors,
            }))?
        );
    } else if response.successful {
        eprintln!("Instance {vm_id} started on {crn_url}");
    } else {
        eprintln!("Instance {vm_id} failed to start");
        if !response.failing.is_empty() {
            eprintln!("  Failing: {}", response.failing.join(", "));
        }
        for (id, err) in &response.errors {
            eprintln!("  {id}: {err}");
        }
    }

    Ok(())
}

pub async fn handle_operation(
    scheduler_url: Url,
    json: bool,
    args: CrnArgs,
    operation: &str,
) -> Result<()> {
    let (vm_id, crn_url) =
        resolve_target(&scheduler_url, &args.vm_id, args.crn.as_deref()).await?;
    let client = build_client(&crn_url, &args.signing)?;

    match operation {
        "stop" => client.stop_instance(&vm_id).await?,
        "reboot" => client.reboot_instance(&vm_id).await?,
        "erase" => client.erase_instance(&vm_id).await?,
        _ => unreachable!(),
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "vm_id": vm_id.to_string(),
                "operation": operation,
                "status": "ok",
            }))?
        );
    } else {
        let past_tense = match operation {
            "stop" => "stopped",
            "reboot" => "rebooted",
            "erase" => "erased",
            _ => unreachable!(),
        };
        eprintln!("Instance {vm_id} {past_tense} on {crn_url}");
    }

    Ok(())
}

/// Strip ANSI escape sequences and control characters from log output.
/// QEMU serial console output contains terminal mode changes, cursor movement,
/// etc. that corrupt the user's terminal if forwarded raw.
fn sanitize_log(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '\x1b' => {
                if let Some(next) = chars.next()
                    && next == '['
                {
                    for c in chars.by_ref() {
                        if ('\x40'..='\x7e').contains(&c) {
                            break;
                        }
                    }
                }
            }
            '\n' | '\t' => result.push(c),
            c if c.is_control() => {}
            c => result.push(c),
        }
    }
    result
}

pub async fn handle_logs(scheduler_url: Url, json: bool, args: CrnArgs) -> Result<()> {
    let (vm_id, crn_url) =
        resolve_target(&scheduler_url, &args.vm_id, args.crn.as_deref()).await?;
    let client = build_client(&crn_url, &args.signing)?;
    let mut stream = std::pin::pin!(client.stream_logs(&vm_id).await?);

    while let Some(result) = stream.next().await {
        let entry = result?;
        if json {
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "type": format!("{:?}", entry.log_type).to_lowercase(),
                    "message": entry.message,
                }))?
            );
        } else {
            use aleph_sdk::crn::LogType;
            let msg = sanitize_log(&entry.message);
            match entry.log_type {
                LogType::Stdout => println!("{msg}"),
                LogType::Stderr => eprintln!("{msg}"),
                LogType::System => eprintln!("[system] {msg}"),
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_strips_csi_sequences() {
        assert_eq!(sanitize_log("\x1b[31mred\x1b[0m"), "red");
        assert_eq!(sanitize_log("\x1b[2J\x1b[Hhello"), "hello");
    }

    #[test]
    fn sanitize_strips_control_chars() {
        assert_eq!(sanitize_log("hello\rworld"), "helloworld");
        assert_eq!(sanitize_log("beep\x07!"), "beep!");
    }

    #[test]
    fn sanitize_preserves_newlines_and_tabs() {
        assert_eq!(sanitize_log("line1\nline2\tok"), "line1\nline2\tok");
    }

    #[test]
    fn sanitize_preserves_utf8() {
        assert_eq!(sanitize_log("café ☕"), "café ☕");
    }

    #[test]
    fn sanitize_handles_bare_esc() {
        assert_eq!(sanitize_log("a\x1bb"), "a");
    }

    #[test]
    fn sanitize_handles_trailing_esc() {
        assert_eq!(sanitize_log("hello\x1b"), "hello");
    }
}
