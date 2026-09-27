//! End-to-end: S3-sourced NMEA bronze input, decoded and committed straight
//! into Apache Iceberg via RustFS's own built-in REST catalog with
//! `--iceberg-sigv4` — the `ais-parse` counterpart of
//! `aisstream-parse/tests/rustfs_iceberg.rs`; see that file's comments for
//! the RustFS-specific setup this also relies on (table-bucket enable, S3
//! config resolved from environment variables independently of `--s3-*`).
//!
//! `ais-parse` has no bronze fixture already in the repo (unlike
//! `aisstream-parse`, which decodes `collect-aisstream`'s JSON and has one
//! checked in), so this test builds a tiny one from two AIVDM sentences
//! already used and checksum-verified in `ais-parse`'s own decode.rs unit
//! tests: a class A position report and a type 5 static/voyage report.
//!
//! Spawns a real subprocess, so this is `#[ignore]`d by default — run it
//! explicitly with `cargo test -- --ignored` on a machine that has
//! `rustfs` on `PATH`. Skips cleanly, not a failure, when `rustfs` isn't
//! found.

mod support;

use arrow::array::{StringBuilder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use collect_core::S3Storage;
use parquet::arrow::ArrowWriter;
use std::process::Command;
use std::sync::Arc;

/// Same class A position report `ais-parse/src/decode.rs`'s own
/// `decodes_class_a_position` test decodes — known-valid AIVDM (checksum
/// included, MMSI 371798000). A second sentence
/// (`decodes_static_voyage_data_from_combined_sentence` in that file)
/// exercises a type 5 static/voyage report, but that test itself accepts
/// either a decoded row *or* a checksum failure for it — not reliable
/// enough to assert on here, so this fixture sticks to the position
/// report, repeated, which is enough to prove the S3-to-Iceberg pipeline.
const POSITION_SENTENCE: &str = "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A";

fn write_bronze_fixture(path: &std::path::Path, base_ts_ms: i64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("payload", DataType::Utf8, false),
    ]));

    let mut ts = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut payload = StringBuilder::new();
    // Ten rows, a second apart, so the partition has more than a single
    // trivial row.
    for i in 0..10i64 {
        ts.append_value(base_ts_ms + i * 1000);
        payload.append_value(POSITION_SENTENCE);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ts.finish()), Arc::new(payload.finish())],
    )
    .expect("build fixture record batch");

    std::fs::create_dir_all(path.parent().expect("fixture path has a parent"))
        .expect("create fixture directory");
    let file = std::fs::File::create(path).expect("create fixture file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("create fixture writer");
    writer.write(&batch).expect("write fixture batch");
    writer.close().expect("close fixture writer");
}

#[tokio::test]
#[ignore = "spawns a real rustfs subprocess; run with `cargo test -- --ignored`"]
async fn s3_input_to_iceberg_with_sigv4() {
    let Some(rustfs) = support::Rustfs::spawn().await else {
        eprintln!("rustfs not found on PATH; skipping");
        return;
    };

    let storage = S3Storage::new(
        "ais-bronze".to_string(),
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

    assert!(
        rustfs.enable_table_bucket("ais-bronze"),
        "enabling the bucket as an Iceberg table bucket (PUT /iceberg/v1/buckets/<bucket>) failed"
    );

    let fixture_dir = tempfile::tempdir().expect("create fixture dir");
    let base_ts_ms = chrono::Utc::now().timestamp_millis();
    let fixture_path = fixture_dir
        .path()
        .join("source=test-nmea/year=2026/month=09/day=27/part-0.parquet");
    write_bronze_fixture(&fixture_path, base_ts_ms);

    let key = "source=test-nmea/year=2026/month=09/day=27/part-0.parquet";
    storage
        .upload_file(&fixture_path, key)
        .await
        .unwrap_or_else(|e| panic!("uploading fixture: {e:#}"));

    let scratch = tempfile::tempdir().expect("create scratch dir");
    let output_dir = tempfile::tempdir().expect("create output dir");

    let env_and_run = |extra_args: &[&str]| -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_ais-parse"))
            .arg("--input-s3-bucket")
            .arg("ais-bronze")
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
            .arg("ais-bronze")
            .arg("--iceberg-sigv4")
            .args(extra_args)
            // Iceberg mode (both --iceberg-sigv4 signing and the actual
            // data-file writes a commit does) resolves S3 config from the
            // environment only, independently of the --s3-* flags above.
            .env("S3_ENDPOINT", rustfs.endpoint())
            .env("S3_ACCESS_KEY", &rustfs.access_key)
            .env("S3_SECRET_KEY", &rustfs.secret_key)
            .env("S3_REGION", "us-east-1")
            .env("S3_PATH_STYLE", "true")
            .output()
            .expect("run ais-parse")
    };

    let output = env_and_run(&[]);
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "ais-parse exited {:?}\n{log}",
        output.status.code()
    );
    assert!(
        log.contains("Committed") && log.contains("Iceberg"),
        "expected a successful Iceberg commit in the run's output:\n{log}"
    );
    assert!(
        log.contains("pos=10"),
        "expected 10 decoded positions in the run's output:\n{log}"
    );

    // Re-run against the same input: the commit manifest should recognize
    // the object as already committed and skip it, proving the run is
    // idempotent rather than double-appending rows on a retry.
    let second = env_and_run(&[]);
    let second_log = format!(
        "{}{}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    assert!(
        second.status.success(),
        "second ais-parse run exited {:?}\n{second_log}",
        second.status.code()
    );
    assert!(
        second_log.contains("already fully committed"),
        "expected the re-run to recognize the already-committed object and skip it:\n{second_log}"
    );
}
