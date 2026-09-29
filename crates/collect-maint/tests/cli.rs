//! End-to-end run of the `ais-compact` binary against a real RustFS:
//! dry-run, compact, idempotent re-run, manifest consolidation, expire and
//! orphan cleanup, with the table read back after each destructive step.
//! `cargo test -p collect-maint -- --ignored`; needs `rustfs` on PATH.

mod support;

use std::collections::HashSet;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use collect_core::iceberg::{
    commit_batches, ensure_namespace, ensure_table, open_catalog, partition_spec_for, table_ident,
    table_schemas, IcebergConfig, TABLE_RAW,
};
use collect_core::S3Storage;
use collect_maint::rewrite::{live_files, read_files, sort_batches};
use iceberg::Catalog;

fn batch(ts: i64, payload: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("source", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![ts]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec!["s"])),
            Arc::new(StringArray::from(vec![payload])),
        ],
    )
    .unwrap()
}

fn run(rustfs: &support::Rustfs, args: &[&str]) -> (i32, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_ais-compact"))
        .args(["--iceberg-catalog-uri", &rustfs.iceberg_catalog_uri()])
        .args([
            "--iceberg-warehouse",
            "cli-test",
            "--iceberg-sigv4",
            "--table",
            "raw",
        ])
        .args(args)
        .env("S3_ENDPOINT", rustfs.endpoint())
        .env("S3_ACCESS_KEY", &rustfs.access_key)
        .env("S3_SECRET_KEY", &rustfs.secret_key)
        .env("S3_REGION", "us-east-1")
        .env("S3_PATH_STYLE", "true")
        .output()
        .expect("run ais-compact");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    eprintln!("$ ais-compact {}\n{text}", args.join(" "));
    (out.status.code().unwrap_or(-1), text)
}

#[tokio::test]
#[ignore = "spawns a real rustfs subprocess"]
async fn compact_expire_orphans_end_to_end() {
    let Some(rustfs) = support::Rustfs::spawn().await else {
        eprintln!("rustfs not found on PATH; skipping");
        return;
    };
    let bucket = "cli-test";
    S3Storage::new(
        bucket.into(),
        String::new(),
        "us-east-1".into(),
        Some(rustfs.endpoint()),
        Some(rustfs.access_key.clone()),
        Some(rustfs.secret_key.clone()),
        true,
        true,
    )
    .await
    .unwrap();
    assert!(rustfs.enable_table_bucket(bucket));
    for (k, v) in [
        ("S3_ENDPOINT", rustfs.endpoint()),
        ("S3_ACCESS_KEY", rustfs.access_key.clone()),
        ("S3_SECRET_KEY", rustfs.secret_key.clone()),
        ("S3_REGION", "us-east-1".into()),
        ("S3_PATH_STYLE", "true".into()),
    ] {
        std::env::set_var(k, v);
    }
    let config = IcebergConfig {
        catalog_uri: rustfs.iceberg_catalog_uri(),
        warehouse: bucket.into(),
        namespace: "ais".into(),
        table_prefix: None,
        token: None,
        sigv4: true,
    };
    let catalog = open_catalog(&config).await.unwrap();
    ensure_namespace(&catalog, &config).await.unwrap();
    let schema = table_schemas::raw_schema();
    let spec = partition_spec_for(&schema, "day").unwrap();
    let ident = table_ident(&config, TABLE_RAW);
    let mut table = ensure_table(&catalog, &config, TABLE_RAW, schema, spec)
        .await
        .unwrap();

    // Six one-row commits in a long-closed day, written newest-first.
    let day = 1_780_000_000_000i64;
    for i in (0..6).rev() {
        commit_batches(
            &catalog,
            &table,
            vec![batch(day + i * 1000, &format!("p{i}"))],
            3,
            TABLE_RAW,
        )
        .await
        .unwrap();
        table = catalog.load_table(&ident).await.unwrap();
    }
    assert_eq!(live_files(&table).await.unwrap().len(), 6);

    // Dry run changes nothing.
    let (code, text) = run(&rustfs, &["compact", "--min-age-hours", "0"]);
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("would rewrite"), "{text}");
    let table = catalog.load_table(&ident).await.unwrap();
    assert_eq!(live_files(&table).await.unwrap().len(), 6);

    // Apply: one sorted file, sort order registered, small manifests merged.
    let (code, text) = run(
        &rustfs,
        &[
            "compact",
            "--apply",
            "--min-age-hours",
            "0",
            "--consolidate-manifests",
            "2",
        ],
    );
    assert_eq!(code, 0, "{text}");
    assert!(
        text.contains("rewrote") && text.contains("sort order registered"),
        "{text}"
    );
    assert!(text.contains("consolidated"), "{text}");
    let table = catalog.load_table(&ident).await.unwrap();
    let files = live_files(&table).await.unwrap();
    assert_eq!(files.len(), 1);
    assert!(files[0]
        .path
        .rsplit('/')
        .next()
        .unwrap()
        .starts_with("compact-"));
    assert_eq!(table.metadata().default_sort_order().fields.len(), 1);
    let paths: HashSet<String> = files.iter().map(|f| f.path.clone()).collect();
    let rows = sort_batches(&read_files(&table, &paths).await.unwrap(), &[]).unwrap();
    let payload = rows.column_by_name("payload").unwrap();
    let payload = payload.as_any().downcast_ref::<StringArray>().unwrap();
    let got: Vec<&str> = (0..payload.len()).map(|i| payload.value(i)).collect();
    assert_eq!(got, ["p0", "p1", "p2", "p3", "p4", "p5"]);

    // Idempotent: nothing left to do (exit 2 = NOTHING_TO_DO).
    let (code, text) = run(&rustfs, &["compact", "--apply", "--min-age-hours", "0"]);
    assert_eq!(code, 2, "{text}");

    // Expire everything but the newest snapshot, then free the objects.
    let (code, text) = run(
        &rustfs,
        &[
            "expire",
            "--apply",
            "--older-than-days",
            "0",
            "--retain-last",
            "1",
        ],
    );
    assert_eq!(code, 0, "{text}");
    let table = catalog.load_table(&ident).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 1);

    let s3 = [
        "--s3-endpoint",
        &rustfs.endpoint(),
        "--s3-access-key",
        &rustfs.access_key,
        "--s3-secret-key",
        &rustfs.secret_key,
        "--s3-disable-tls",
    ];
    let mut dry = vec!["orphans", "--older-than-days", "0"];
    dry.extend(s3);
    let (code, text) = run(&rustfs, &dry);
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("would delete"), "{text}");
    let mut apply = vec!["orphans", "--apply", "--older-than-days", "0"];
    apply.extend(s3);
    let (code, text) = run(&rustfs, &apply);
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("deleted"), "{text}");

    // Table still reads after all that cleanup.
    let table = catalog.load_table(&ident).await.unwrap();
    let files = live_files(&table).await.unwrap();
    let paths: HashSet<String> = files.iter().map(|f| f.path.clone()).collect();
    let rows = sort_batches(&read_files(&table, &paths).await.unwrap(), &[]).unwrap();
    assert_eq!(rows.num_rows(), 6);
}
