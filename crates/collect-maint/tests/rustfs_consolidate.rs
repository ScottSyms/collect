//! Manifest consolidation against a real RustFS, including the race it has to
//! survive in production: ingest appends while the merge is being prepared,
//! so the first publish gets a 409 and the retry must keep the new manifest.
//! Needs `rustfs` on PATH; `cargo test -p collect-maint -- --ignored`.

mod support;

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use collect_core::iceberg::{
    commit_batches, ensure_namespace, ensure_table, open_catalog, partition_spec_for, table_ident,
    table_schemas, IcebergConfig, TABLE_RAW,
};
use collect_core::S3Storage;
use collect_maint::commit::{merge_small_manifests, seal_consolidation, RestClient};
use collect_maint::compact::manifest_counts;
use collect_maint::rewrite::{live_files, read_files, sort_batches};
use iceberg::Catalog;

fn raw_batch(ts: i64, payload: &str) -> RecordBatch {
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

#[tokio::test]
#[ignore = "spawns a real rustfs subprocess"]
async fn consolidation_survives_a_concurrent_append() {
    let Some(rustfs) = support::Rustfs::spawn().await else {
        eprintln!("rustfs not found on PATH; skipping");
        return;
    };
    let bucket = "consolidate-test";
    S3Storage::new(
        bucket.to_string(),
        String::new(),
        "us-east-1".to_string(),
        Some(rustfs.endpoint()),
        Some(rustfs.access_key.clone()),
        Some(rustfs.secret_key.clone()),
        true,
        true,
    )
    .await
    .expect("create bucket");
    assert!(rustfs.enable_table_bucket(bucket), "enable table bucket");
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
        warehouse: bucket.to_string(),
        namespace: "ais".into(),
        table_prefix: None,
        token: None,
        sigv4: true,
    };
    let catalog = open_catalog(&config).await.expect("open catalog");
    ensure_namespace(&catalog, &config).await.unwrap();
    let schema = table_schemas::raw_schema();
    let spec = partition_spec_for(&schema, "day").unwrap();
    let ident = table_ident(&config, TABLE_RAW);
    let mut table = ensure_table(&catalog, &config, TABLE_RAW, schema, spec)
        .await
        .unwrap();

    // One-row fast_append commits, as live ingest makes: one manifest each.
    let day = 1_780_000_000_000i64;
    const COMMITS: i64 = 25;
    for i in 0..COMMITS {
        commit_batches(
            &catalog,
            &table,
            vec![raw_batch(day + i * 1000, &format!("r{i}"))],
            3,
            TABLE_RAW,
        )
        .await
        .unwrap();
        table = catalog.load_table(&ident).await.unwrap();
    }
    let before = manifest_counts(&table).await.unwrap();
    assert!(before.small >= COMMITS as usize, "{before:?}");

    // Merge on this snapshot, then let "ingest" append before publishing.
    let merged = merge_small_manifests(&table, 2).await.unwrap().unwrap();
    assert!(merged.replaced() >= COMMITS as usize);
    let stale = table.clone();
    commit_batches(
        &catalog,
        &table,
        vec![raw_batch(day + 99_000, "late")],
        3,
        TABLE_RAW,
    )
    .await
    .unwrap();

    let rest = RestClient::connect(&config).await.unwrap();
    let (req, upd) = seal_consolidation(&stale, &merged, 1).await.unwrap();
    assert!(
        !rest.commit(&ident, &req, &upd).await.unwrap(),
        "publishing on the stale snapshot must conflict"
    );

    // Retry on the reloaded table: no re-merge, and the late append survives.
    let fresh = catalog.load_table(&ident).await.unwrap();
    let (req, upd) = seal_consolidation(&fresh, &merged, 2).await.unwrap();
    assert!(
        rest.commit(&ident, &req, &upd).await.unwrap(),
        "retry must commit"
    );

    let table = catalog.load_table(&ident).await.unwrap();
    let after = manifest_counts(&table).await.unwrap();
    assert!(
        after.total <= 2,
        "merged + the late append's manifest: {after:?}"
    );
    let files = live_files(&table).await.unwrap();
    assert_eq!(files.len(), COMMITS as usize + 1);
    let paths: HashSet<String> = files.iter().map(|f| f.path.clone()).collect();
    let rows = sort_batches(&read_files(&table, &paths).await.unwrap(), &[]).unwrap();
    assert_eq!(rows.num_rows(), COMMITS as usize + 1);

    // A plain append still commits on top of the consolidated snapshot.
    commit_batches(
        &catalog,
        &table,
        vec![raw_batch(day + 100_000, "after")],
        3,
        TABLE_RAW,
    )
    .await
    .unwrap();
}
