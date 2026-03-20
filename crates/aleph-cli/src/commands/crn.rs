use aleph_sdk::crn::CrnClient;
use futures_util::StreamExt;
use url::Url;

use crate::cli::{CrnArgs, CrnCommand, CrnStartArgs, SigningArgs};
use crate::common::resolve_account;

pub async fn handle_crn_command(
    json: bool,
    command: CrnCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        CrnCommand::Start(args) => handle_start(json, args).await,
        CrnCommand::Stop(args) => handle_operation(json, args, "stop").await,
        CrnCommand::Reboot(args) => handle_operation(json, args, "reboot").await,
        CrnCommand::Erase(args) => handle_operation(json, args, "erase").await,
        CrnCommand::Logs(args) => handle_logs(json, args).await,
    }
}

fn build_client(
    crn_url: &str,
    signing: &SigningArgs,
) -> Result<CrnClient, Box<dyn std::error::Error>> {
    let account = resolve_account(signing)?;
    let url = Url::parse(crn_url)?;
    Ok(CrnClient::new(&account, url)?)
}

async fn handle_start(json: bool, args: CrnStartArgs) -> Result<(), Box<dyn std::error::Error>> {
    let client = build_client(&args.crn_url, &args.signing)?;
    let response = client.start_instance(&args.vm_id).await?;

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
        eprintln!("Instance {} started on {}", args.vm_id, args.crn_url);
    } else {
        eprintln!("Instance {} failed to start", args.vm_id);
        if !response.failing.is_empty() {
            eprintln!("  Failing: {}", response.failing.join(", "));
        }
        for (id, err) in &response.errors {
            eprintln!("  {id}: {err}");
        }
    }

    Ok(())
}

async fn handle_operation(
    json: bool,
    args: CrnArgs,
    operation: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = build_client(&args.crn_url, &args.signing)?;

    match operation {
        "stop" => client.stop_instance(&args.vm_id).await?,
        "reboot" => client.reboot_instance(&args.vm_id).await?,
        "erase" => client.erase_instance(&args.vm_id).await?,
        _ => unreachable!(),
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "vm_id": args.vm_id.to_string(),
                "operation": operation,
                "status": "ok",
            }))?
        );
    } else {
        eprintln!("Instance {} {operation}ped on {}", args.vm_id, args.crn_url);
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
                // Skip ANSI escape sequence
                if let Some(next) = chars.next()
                    && next == '['
                {
                    // CSI sequence: skip until final byte (0x40-0x7E)
                    for c in chars.by_ref() {
                        if ('\x40'..='\x7e').contains(&c) {
                            break;
                        }
                    }
                }
                // else: 2-char escape — already consumed
            }
            '\n' | '\t' => result.push(c),
            c if c.is_control() => {} // strip CR, BEL, etc.
            c => result.push(c),
        }
    }
    result
}

async fn handle_logs(json: bool, args: CrnArgs) -> Result<(), Box<dyn std::error::Error>> {
    let client = build_client(&args.crn_url, &args.signing)?;
    let mut stream = std::pin::pin!(client.stream_logs(&args.vm_id).await?);

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
