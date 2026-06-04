//! `aleph instance confidential` command tree.

use crate::cli::{
    ConfidentialCommand, ConfidentialCreateArgs, ConfidentialInitSessionArgs, ConfidentialStartArgs,
};
use crate::commands::instance_target::resolve_target;
use crate::common::{confirm_action, resolve_account};
use crate::config::store::ConfigStore;
use crate::sevctl::Sevctl;
use aleph_sdk::confidential::{
    DEFAULT_CONFIDENTIAL_FIRMWARE_HASH_HEX, build_secret_packet, calculate_firmware_hash,
    compute_expected_measure,
};
use aleph_sdk::crn::{CrnClient, CrnError};
use aleph_types::item_hash::ItemHash;
use anyhow::{Context, Result, anyhow, bail};
use std::time::{Duration, Instant};
use subtle::ConstantTimeEq;
use url::Url;

pub async fn dispatch(scheduler_url: Url, json: bool, cmd: ConfidentialCommand) -> Result<()> {
    match cmd {
        ConfidentialCommand::InitSession(args) => handle_init_session(scheduler_url, args).await,
        ConfidentialCommand::Start(args) => handle_start(scheduler_url, json, args).await,
        ConfidentialCommand::Create(args) => handle_create(scheduler_url, json, args).await,
    }
}

async fn handle_init_session(scheduler_url: Url, args: ConfidentialInitSessionArgs) -> Result<()> {
    // 1. Resolve target (hash + CRN URL).
    let (vm_id, crn_url) = resolve_target(&scheduler_url, &args.vm_id, args.crn.as_deref()).await?;

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
    let files = sevctl
        .session(&prefix, &cert_path, args.policy)
        .await
        .context("sevctl session failed")?;

    // 8. POST session.b64 + godh.b64 to the CRN.
    let session_bytes = std::fs::read(&files.session)
        .with_context(|| format!("reading {}", files.session.display()))?;
    let godh_bytes =
        std::fs::read(&files.godh).with_context(|| format!("reading {}", files.godh.display()))?;
    crn.initialize_confidential(&vm_id, &session_bytes, &godh_bytes)
        .await
        .context("CRN rejected the initialize request")?;

    // 9. Done.
    println!("Confidential session initialized for {vm_id} on {crn_url}.");
    Ok(())
}

async fn handle_start(scheduler_url: Url, json: bool, args: ConfidentialStartArgs) -> Result<()> {
    // 1. Resolve target.
    let (vm_id, crn_url) = resolve_target(&scheduler_url, &args.vm_id, args.crn.as_deref()).await?;

    // 2. Session dir must exist.
    let session_dir = ConfigStore::confidential_sessions_dir()?.join(vm_id.to_string());
    if !session_dir.exists() {
        bail!(
            "no session found for {vm_id}. Run 'aleph instance confidential init-session {vm_id}' first."
        );
    }

    // 3. CRN client.
    let account = resolve_account(&args.identity)?;
    let crn = CrnClient::new(&account, crn_url.clone())?;

    // 4. Fetch measurement.
    let measurement = crn
        .get_measurement(&vm_id)
        .await
        .context("fetching VM measurement from CRN")?;
    let (vm_measure, nonce) = measurement.split_launch_measure()?;

    // 5. Resolve expected firmware hash.
    let firmware_hash_hex = if let Some(path) = args.firmware_file.as_deref() {
        calculate_firmware_hash(path).with_context(|| format!("hashing {}", path.display()))?
    } else if let Some(h) = args.firmware_hash.as_deref() {
        h.to_string()
    } else {
        DEFAULT_CONFIDENTIAL_FIRMWARE_HASH_HEX.to_string()
    };
    let firmware_hash: [u8; 32] = hex::decode(&firmware_hash_hex)
        .with_context(|| format!("decoding firmware hash hex: {firmware_hash_hex}"))?
        .try_into()
        .map_err(|v: Vec<u8>| {
            anyhow!(
                "firmware hash must be 32 bytes (got {}); was {firmware_hash_hex:?}",
                v.len()
            )
        })?;

    // 6. Read TIK. Read before the TEK because it is needed first, to validate
    //    the launch measurement in step 7; the TEK is only used later (step 11)
    //    to encrypt the secret table, and reading it now would be wasted work if
    //    the measurement check fails.
    let tik_path = session_dir.join("vm_tik.bin");
    let tik_bytes =
        std::fs::read(&tik_path).with_context(|| format!("reading {}", tik_path.display()))?;
    let tik: [u8; 16] = tik_bytes
        .try_into()
        .map_err(|v: Vec<u8>| anyhow!("vm_tik.bin must be 16 bytes (got {})", v.len()))?;

    // 7. Validate measurement (constant time).
    let expected = compute_expected_measure(&measurement.sev_info, &tik, &firmware_hash, &nonce);
    if expected.ct_eq(&vm_measure).unwrap_u8() == 0 {
        bail!(
            "VM measurement does not match expected firmware (hash {firmware_hash_hex}). \
             The VM may be running tampered code, or the firmware hash is wrong. \
             Pass --firmware-file to recompute locally. Refusing to inject secret."
        );
    }

    // 8. Read TEK.
    let tek_path = session_dir.join("vm_tek.bin");
    let tek_bytes =
        std::fs::read(&tek_path).with_context(|| format!("reading {}", tek_path.display()))?;
    let tek: [u8; 16] = tek_bytes
        .try_into()
        .map_err(|v: Vec<u8>| anyhow!("vm_tek.bin must be 16 bytes (got {})", v.len()))?;

    // 9. Acquire secret.
    let secret = match args.secret {
        Some(s) => s,
        None => {
            rpassword::prompt_password("VM disk-decryption secret: ").context("reading secret")?
        }
    };

    // 10. Random IV (16 bytes).
    use rand::RngCore;
    let mut iv = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut iv);

    // 11. Build + inject.
    let (packet_header, encrypted_secret) =
        build_secret_packet(&tek, &tik, &vm_measure, &secret, iv);
    crn.inject_secret(&vm_id, &packet_header, &encrypted_secret)
        .await
        .context("CRN rejected the secret injection")?;

    // 12. Output.
    if json {
        println!(
            "{}",
            serde_json::json!({
                "status": "ok",
                "vm_id": vm_id.to_string(),
                "crn_url": crn_url.to_string(),
            })
        );
    } else {
        println!("Instance {vm_id} is starting on {crn_url}.");
        println!("  Networking: aleph instance show {vm_id} --verbose");
        println!("  SSH:        aleph instance ssh {vm_id}");
        println!("  Logs:       aleph instance logs {vm_id}");
    }
    Ok(())
}

async fn handle_create(scheduler_url: Url, json: bool, args: ConfidentialCreateArgs) -> Result<()> {
    // 0. Sevctl available? Fail fast before doing anything.
    let _sevctl = Sevctl::find()?;

    // 1. Determine vm_id + crn_url.
    let vm_id_input = args.vm_id.as_deref().ok_or_else(|| {
        anyhow!(
            "creating a new VM from `confidential create` requires `instance create` flag forwarding, \
             which lands in a follow-up. For now, run `aleph instance create --confidential ...` first, \
             then call `aleph instance confidential create <vm-hash>`."
        )
    })?;
    let (vm_id, crn_url) = resolve_target(&scheduler_url, vm_id_input, args.crn.as_deref()).await?;

    // 2. Allocate on the CRN (the "start" step in Python parlance).
    let account = resolve_account(&args.identity)?;
    let crn = CrnClient::new(&account, crn_url.clone())?;
    crn.start_instance(&vm_id)
        .await
        .context("CRN failed to start the VM")?;

    // 3. Initialize the confidential session.
    let init_args = ConfidentialInitSessionArgs {
        vm_id: vm_id.to_string(),
        crn: Some(crn_url.to_string()),
        identity: args.identity.clone(),
        policy: args.policy,
        keep_session: args.keep_session,
        debug: args.debug,
    };
    handle_init_session(scheduler_url.clone(), init_args).await?;

    // 4. Poll until measurement-ready.
    println!("Waiting for {vm_id} to reach measurement-ready state...");
    let deadline = Instant::now() + Duration::from_secs(60);
    poll_measurement_ready(&crn, &vm_id, deadline).await?;

    // 5. Validate measurement + inject secret.
    let start_args = ConfidentialStartArgs {
        vm_id: vm_id.to_string(),
        crn: Some(crn_url.to_string()),
        identity: args.identity,
        firmware_hash: args.firmware_hash,
        firmware_file: args.firmware_file,
        secret: args.secret,
        json,
        debug: args.debug,
    };
    handle_start(scheduler_url, json, start_args).await
}

/// Polls `crn.get_measurement(vm_id)` until it returns 200 or `deadline` elapses.
/// Treats HTTP 404 and 425 as "VM not yet measurement-ready"; surfaces any other
/// error as a hard failure. Backoff ramps 1s, 2s, 4s, then holds at 5s; the
/// final entry repeats, so `deadline` is the sole loop terminator (not the
/// length of the schedule).
async fn poll_measurement_ready(
    crn: &CrnClient,
    vm_id: &ItemHash,
    deadline: Instant,
) -> Result<()> {
    let schedule: &[u64] = &[1, 2, 4, 5];
    let mut step = 0usize;
    // Remembers the most recent transport error so a deadline expiry caused by a
    // flaky connection surfaces the underlying cause rather than a bare timeout.
    let mut last_transport_err: Option<CrnError> = None;
    loop {
        match crn.get_measurement(vm_id).await {
            Ok(_) => return Ok(()),
            Err(CrnError::VmNotFound(_)) => {
                // 404 -> not ready yet
            }
            Err(CrnError::Api { status: 425, .. }) => {
                // 425 Too Early -> not ready yet
            }
            Err(e @ CrnError::Http(_)) => {
                // Transport-level failure (connection reset, timeout, DNS): the
                // CRN or the VM's networking may still be coming up, so keep
                // polling and only surface this if the deadline expires. HTTP
                // status errors (402/403/5xx) are deliberate rejections and
                // still abort immediately via the catch-all below.
                last_transport_err = Some(e);
            }
            Err(e) => return Err(e.into()),
        }
        if Instant::now() >= deadline {
            if let Some(e) = last_transport_err {
                return Err(anyhow::Error::new(e).context(format!(
                    "VM did not become measurement-ready within the timeout; the CRN was \
                     unreachable on the last attempt. Check connectivity to the node, then \
                     retry 'aleph instance confidential start {vm_id}'."
                )));
            }
            bail!(
                "VM did not become measurement-ready within the timeout. Retry \
                 'aleph instance confidential start {vm_id}' in a minute, or check \
                 'aleph instance logs {vm_id}'."
            );
        }
        let secs = schedule[step.min(schedule.len() - 1)];
        tokio::time::sleep(Duration::from_secs(secs)).await;
        step = step.saturating_add(1);
    }
}
