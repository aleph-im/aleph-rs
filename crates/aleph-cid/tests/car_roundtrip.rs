//! Tier-2 real-kubo round-trip test for CARv1 output.
//!
//! Marked #[ignore] by default. Requires a kubo daemon reachable at
//! the address in IPFS_GATEWAY_URL (default http://localhost:5001).
//!
//! Run manually:
//!
//!   docker run -d --name kubo-car-test -p 5001:5001 ipfs/kubo:v0.30.0
//!   cargo test -p aleph-cid --test car_roundtrip -- --ignored
//!   docker rm -f kubo-car-test

use aleph_cid::car::{write_block_frame, write_carv1_header};
use aleph_cid::folder_hash::build_folder_dag;
use aleph_cid::{UploadFolderOptions, collect_folder_files};

fn kubo_url() -> String {
    std::env::var("IPFS_GATEWAY_URL").unwrap_or_else(|_| "http://localhost:5001".into())
}

#[tokio::test]
#[ignore = "requires a real kubo daemon; see header for setup"]
async fn car_roundtrip_via_dag_import() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("a.txt"), b"hello").unwrap();
    std::fs::write(tmp.path().join("b.txt"), b"world").unwrap();
    let entries = collect_folder_files(tmp.path(), true).unwrap();
    let opts = UploadFolderOptions::default();

    // Build CAR: header + block-frame body.
    use std::io::Write;
    let mut blocks_buf = Vec::new();
    let mut last_cid: Option<Vec<u8>> = None;
    let root = build_folder_dag(&entries, &opts, &mut |cid, block| {
        write_block_frame(&mut blocks_buf, cid, block)?;
        last_cid = Some(cid.to_vec());
        Ok(())
    })
    .unwrap();
    let mut header_bytes = Vec::new();
    write_carv1_header(&mut header_bytes, last_cid.as_ref().unwrap()).unwrap();

    let mut car_file = tempfile::NamedTempFile::new().unwrap();
    car_file.write_all(&header_bytes).unwrap();
    car_file.write_all(&blocks_buf).unwrap();
    car_file.flush().unwrap();

    // POST to kubo dag/import.
    let bytes = std::fs::read(car_file.path()).unwrap();
    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/v0/dag/import?pin-roots=true&silent=false",
        kubo_url()
    );
    let resp = client
        .post(url)
        .multipart(
            reqwest::multipart::Form::new().part(
                "file",
                reqwest::multipart::Part::bytes(bytes)
                    .file_name("upload.car")
                    .mime_str("application/vnd.ipld.car")
                    .unwrap(),
            ),
        )
        .send()
        .await
        .expect("POST to kubo");
    assert!(
        resp.status().is_success(),
        "kubo dag/import failed: {:?}",
        resp.text().await
    );

    // Verify pin exists.
    let pin_url = format!("{}/api/v0/pin/ls?type=recursive&arg={}", kubo_url(), root);
    let pin_resp = client.post(pin_url).send().await.expect("POST pin/ls");
    let pin_body = pin_resp.text().await.unwrap();
    assert!(
        pin_body.contains(&root.to_string()),
        "root not pinned: body={pin_body}"
    );
}
