//! `aleph instance confidential` command tree.

use crate::cli::{ConfidentialCommand, ConfidentialInitSessionArgs};
use crate::commands::instance_target::resolve_target;
use crate::common::{confirm_action, resolve_account};
use crate::config::store::ConfigStore;
use crate::sevctl::Sevctl;
use aleph_sdk::crn::CrnClient;
use anyhow::{Context, Result, anyhow};
use url::Url;

pub async fn dispatch(scheduler_url: Url, json: bool, cmd: ConfidentialCommand) -> Result<()> {
    match cmd {
        ConfidentialCommand::InitSession(args) => {
            handle_init_session(scheduler_url, json, args).await
        }
        ConfidentialCommand::Start(_) => Err(anyhow!(
            "`aleph instance confidential start` is not yet implemented"
        )),
        ConfidentialCommand::Create(_) => Err(anyhow!(
            "`aleph instance confidential create` is not yet implemented"
        )),
    }
}

async fn handle_init_session(
    scheduler_url: Url,
    _json: bool,
    args: ConfidentialInitSessionArgs,
) -> Result<()> {
    // 1. Resolve target (hash + CRN URL).
    let (vm_id, crn_url) =
        resolve_target(&scheduler_url, &args.vm_id, args.crn_url.as_deref()).await?;

    // 2. Confirm sevctl is on PATH before any CRN call.
    let sevctl = Sevctl::find()?;

    // 3. Create the per-VM session directory.
    let session_dir = ConfigStore::confidential_sessions_dir()?.join(vm_id.to_string());
    std::fs::create_dir_all(&session_dir)
        .with_context(|| format!("creating {}", session_dir.display()))?;

    // 4. Overwrite check. sevctl session emits `<prefix>_godh.b64`; with a
    //    prefix of `<session_dir>/vm` that resolves to `vm_godh.b64`.
    let godh_path = session_dir.join("vm_godh.b64");
    if godh_path.exists() {
        if args.keep_session {
            println!("Keeping existing session for {vm_id}.");
            return Ok(());
        }
        let prompt = format!(
            "Session already initialized for {vm_id}. Overwrite? \
             (You will lose the ability to communicate with the already-running VM.)"
        );
        if !confirm_action(&prompt, false)? {
            println!("Keeping existing session for {vm_id}.");
            return Ok(());
        }
    }

    // 5. Fetch the platform certificate and write it to disk.
    let account = resolve_account(&args.identity)?;
    let crn = CrnClient::new(&account, crn_url.clone())?;
    let cert_bytes = crn
        .get_platform_certificate()
        .await
        .with_context(|| format!("fetching platform certificate from {crn_url}"))?;
    let cert_path = session_dir.join("platform_certificate.pem");
    std::fs::write(&cert_path, &cert_bytes)
        .with_context(|| format!("writing {}", cert_path.display()))?;

    // 6. Verify the cert chain against AMD roots.
    sevctl.verify(&cert_path).await.with_context(|| {
        format!(
            "the CRN's platform certificate chain at {crn_url} did not validate against AMD's \
             roots. The node may be misconfigured or compromised. Refusing to derive session keys."
        )
    })?;

    // 7. Derive session keys (writes vm_{godh,session}.b64 + vm_{tek,tik}.bin).
    let prefix = session_dir.join("vm");
    sevctl
        .session(&prefix, &cert_path, args.policy)
        .await
        .context("sevctl session failed")?;

    // 8. POST session.b64 + godh.b64 to the CRN.
    let session_path = session_dir.join("vm_session.b64");
    let session_bytes = std::fs::read(&session_path)
        .with_context(|| format!("reading {}", session_path.display()))?;
    let godh_bytes =
        std::fs::read(&godh_path).with_context(|| format!("reading {}", godh_path.display()))?;
    crn.initialize_confidential(&vm_id, &session_bytes, &godh_bytes)
        .await
        .context("CRN rejected the initialize request")?;

    // 9. Done.
    println!("Confidential session initialized for {vm_id} on {crn_url}.");
    Ok(())
}
