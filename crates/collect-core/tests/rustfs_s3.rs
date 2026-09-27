//! Round-trips `S3Storage` against a real local RustFS instance, covering
//! the shared upload/download/list code path all four collectors and both
//! batch parsers use.
//!
//! Spawns a real subprocess, so this is `#[ignore]`d by default — run it
//! explicitly with `cargo test -- --ignored` on a machine that has `rustfs`
//! on `PATH` (installed at `/opt/homebrew/bin/rustfs` on this project's dev
//! machine). It skips cleanly, rather than failing, when `rustfs` isn't
//! found, so it's safe to leave in a suite that also runs where it isn't
//! installed.

mod support;

use collect_core::S3Storage;
use std::io::Write;

#[tokio::test]
#[ignore = "spawns a real rustfs subprocess; run with `cargo test -- --ignored`"]
async fn put_get_list_round_trip() {
    let Some(rustfs) = support::Rustfs::spawn().await else {
        eprintln!("rustfs not found on PATH; skipping");
        return;
    };

    let storage = S3Storage::new(
        "collect-core-test".to_string(),
        String::new(),
        "us-east-1".to_string(),
        Some(rustfs.endpoint()),
        Some(rustfs.access_key.clone()),
        Some(rustfs.secret_key.clone()),
        true, // keep_local: irrelevant here, no local cleanup involved
        true, // disable_tls
    )
    .await
    .expect("connect to rustfs and ensure the bucket");

    let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
    tmp.write_all(b"hello from the rustfs integration test\n")
        .expect("write temp file");
    let key = "roundtrip/hello.txt";
    storage
        .upload_file(tmp.path(), key)
        .await
        .expect("upload to rustfs");

    let listed = storage
        .list_keys_with_prefix("roundtrip/")
        .await
        .expect("list objects");
    assert!(
        listed.iter().any(|o| o.key == key),
        "uploaded key not found in listing: {listed:?}"
    );

    let download_dir = tempfile::tempdir().expect("create download dir");
    let download_path = download_dir.path().join("hello-downloaded.txt");
    let bytes = storage
        .download_to_path(key, &download_path)
        .await
        .expect("download from rustfs");
    let contents = std::fs::read_to_string(&download_path).expect("read downloaded file");
    assert_eq!(contents, "hello from the rustfs integration test\n");
    assert_eq!(bytes, contents.len() as u64);
}
