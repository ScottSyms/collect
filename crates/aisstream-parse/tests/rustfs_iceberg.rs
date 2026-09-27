//! End-to-end: S3-sourced bronze input, decoded and committed straight into
//! Apache Iceberg via RustFS's own built-in REST catalog with
//! `--iceberg-sigv4` — mirroring the reference invocation this test
//! coverage was requested against, just pointed at a spawned local RustFS
//! instead of a real deployment.
//!
//! Spawns a real subprocess, so this is `#[ignore]`d by default — run it
//! explicitly with `cargo test -- --ignored` on a machine that has
//! `rustfs` on `PATH`. Skips cleanly, not a failure, when `rustfs` isn't
//! found.

mod support;

use collect_core::S3Storage;
use std::path::Path;
use std::process::Command;

/// A handful of files from the repo's real aisstream bronze fixture —
/// enough to exercise a genuine multi-file partition without uploading all
/// 72 files on every run.
const FIXTURE_FILE_COUNT: usize = 5;

#[tokio::test]
#[ignore = "spawns a real rustfs subprocess; run with `cargo test -- --ignored`"]
async fn s3_input_to_iceberg_with_sigv4() {
    let Some(rustfs) = support::Rustfs::spawn().await else {
        eprintln!("rustfs not found on PATH; skipping");
        return;
    };

    let storage = S3Storage::new(
        "aisstream-bronze".to_string(),
        String::new(),
        "us-east-1".to_string(),
        Some(rustfs.endpoint()),
        Some(rustfs.access_key.clone()),
        Some(rustfs.secret_key.clone()),
        true,
        true,
    )
    .await
    .expect("connect to rustfs and ensure the bucket");

    let fixture_root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../data/source=aisstream");
    assert!(
        fixture_root.is_dir(),
        "expected fixture at {}",
        fixture_root.display()
    );
    let files = collect_parquet_files(&fixture_root);
    assert!(
        files.len() >= FIXTURE_FILE_COUNT,
        "fixture has fewer files than expected: {}",
        files.len()
    );

    for path in files.iter().take(FIXTURE_FILE_COUNT) {
        let rel = path
            .strip_prefix(fixture_root.parent().expect("fixture has a parent"))
            .expect("fixture file under fixture root")
            .to_string_lossy()
            .replace('\\', "/"); // Windows-safe key separator; irrelevant on this dev machine
        storage
            .upload_file(path, &rel)
            .await
            .unwrap_or_else(|e| panic!("uploading {rel}: {e:#}"));
    }

    assert!(
        rustfs.enable_table_bucket("aisstream-bronze"),
        "enabling the bucket as an Iceberg table bucket (PUT /iceberg/v1/buckets/<bucket>) failed"
    );

    let scratch = tempfile::tempdir().expect("create scratch dir");
    let output_dir = tempfile::tempdir().expect("create output dir");

    let output = Command::new(env!("CARGO_BIN_EXE_aisstream-parse"))
        .arg("--input-s3-bucket")
        .arg("aisstream-bronze")
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--scratch-dir")
        .arg(scratch.path())
        .arg("--s3-endpoint")
        .arg(rustfs.endpoint())
        .arg("--s3-access-key")
        .arg(&rustfs.access_key)
        .arg("--s3-secret-key")
        .arg(&rustfs.secret_key)
        .arg("--s3-disable-tls")
        .arg("--iceberg-catalog-uri")
        .arg(rustfs.iceberg_catalog_uri())
        .arg("--iceberg-warehouse")
        .arg("aisstream-bronze")
        .arg("--iceberg-sigv4")
        // Two separate pieces of Iceberg machinery — SigV4-signing the
        // catalog requests, and opendal's S3 backend for the actual data
        // files a commit writes — each resolve S3 config from these
        // environment variables only, independently of the --s3-* flags
        // already passed above. Matches the reference invocation this
        // test is modeled on, which sets them the same way.
        .env("S3_ENDPOINT", rustfs.endpoint())
        .env("S3_ACCESS_KEY", &rustfs.access_key)
        .env("S3_SECRET_KEY", &rustfs.secret_key)
        .env("S3_REGION", "us-east-1")
        .env("S3_PATH_STYLE", "true")
        .output()
        .expect("run aisstream-parse");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "aisstream-parse exited {:?}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        output.status.code()
    );
    let log = format!("{stdout}{stderr}");
    assert!(
        log.contains("Committed") && log.contains("Iceberg"),
        "expected a successful Iceberg commit in the run's output:\n{log}"
    );

    // Re-run against the same input: the commit manifest should recognize
    // every object as already committed and skip re-appending it, proving
    // the run is idempotent rather than double-counting rows on a retry.
    let second = Command::new(env!("CARGO_BIN_EXE_aisstream-parse"))
        .arg("--input-s3-bucket")
        .arg("aisstream-bronze")
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--scratch-dir")
        .arg(scratch.path())
        .arg("--s3-endpoint")
        .arg(rustfs.endpoint())
        .arg("--s3-access-key")
        .arg(&rustfs.access_key)
        .arg("--s3-secret-key")
        .arg(&rustfs.secret_key)
        .arg("--s3-disable-tls")
        .arg("--iceberg-catalog-uri")
        .arg(rustfs.iceberg_catalog_uri())
        .arg("--iceberg-warehouse")
        .arg("aisstream-bronze")
        .arg("--iceberg-sigv4")
        .env("S3_ENDPOINT", rustfs.endpoint())
        .env("S3_ACCESS_KEY", &rustfs.access_key)
        .env("S3_SECRET_KEY", &rustfs.secret_key)
        .env("S3_REGION", "us-east-1")
        .env("S3_PATH_STYLE", "true")
        .output()
        .expect("re-run aisstream-parse");

    let second_log = format!(
        "{}{}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    assert!(
        second.status.success(),
        "second aisstream-parse run exited {:?}\n{second_log}",
        second.status.code()
    );
    assert!(
        second_log.contains("already fully committed"),
        "expected the re-run to recognize already-committed objects and skip them:\n{second_log}"
    );
}

fn collect_parquet_files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}
