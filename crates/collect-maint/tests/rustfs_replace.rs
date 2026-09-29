//! Go/no-go for the pure-Rust compactor: does RustFS's REST catalog accept a
//! hand-built `replace` snapshot, and does the table still read correctly
//! afterwards? Needs `rustfs` on PATH; `cargo test -p collect-maint -- --ignored`.

mod support;

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use collect_core::iceberg::{
    commit_batches, ensure_namespace, ensure_table, open_catalog, partition_spec_for,
    table_schemas, table_ident, IcebergConfig, TABLE_RAW,
};
use collect_core::S3Storage;
use collect_maint::commit::{prepare_replace, RestClient};
use collect_maint::rewrite::{live_files, read_files, sort_batches, write_partition};
use iceberg::Catalog;

fn raw_batch(rows: &[(i64, &str)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        Field::new("source", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(
                TimestampMillisecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec!["s"; rows.len()])),
            Arc::new(StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
        ],
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "spawns a real rustfs subprocess"]
async fn replace_commit_is_accepted_and_readable() {
    let Some(rustfs) = support::Rustfs::spawn().await else {
        eprintln!("rustfs not found on PATH; skipping");
        return;
    };
    let bucket = "maint-test";
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
    let mut table = ensure_table(&catalog, &config, TABLE_RAW, schema, spec).await.unwrap();

    // Three tiny commits, deliberately out of order across commits, one day.
    let day = 1_780_000_000_000i64; // fixed instant; all rows fall on one UTC day
    for rows in [
        vec![(day + 3000, "c"), (day + 2000, "b")],
        vec![(day + 1000, "a")],
        vec![(day + 5000, "e"), (day + 4000, "d")],
    ] {
        commit_batches(&catalog, &table, vec![raw_batch(&rows)], 3, TABLE_RAW)
            .await
            .unwrap();
        table = catalog.load_table(&table_ident(&config, TABLE_RAW)).await.unwrap();
    }

    let before = live_files(&table).await.unwrap();
    assert_eq!(before.len(), 3);
    let remove: HashSet<String> = before.iter().map(|f| f.path.clone()).collect();

    let batches = read_files(&table, &remove).await.unwrap();
    let sorted = sort_batches(&batches, &["ts".to_string()]).unwrap();
    let added = write_partition(&table, before[0].partition.clone(), sorted, &["source"])
        .await
        .unwrap();
    assert_eq!(added.len(), 1);

    let prepared = prepare_replace(&table, &remove, added).await.unwrap();
    let rest = RestClient::connect(&config).await.unwrap();
    let ident = table_ident(&config, TABLE_RAW);
    let ok = rest
        .commit(&ident, &prepared.requirements, &prepared.updates)
        .await
        .expect("catalog must accept the replace commit");
    assert!(ok, "unexpected 409");

    let table = catalog.load_table(&ident).await.unwrap();
    let after = live_files(&table).await.unwrap();
    assert_eq!(after.len(), 1, "3 small files compacted into 1");
    assert_eq!(after[0].records, 5);
    let paths: HashSet<String> = after.iter().map(|f| f.path.clone()).collect();
    let rows = read_files(&table, &paths).await.unwrap();
    let all = sort_batches(&rows, &[]).unwrap();
    let payload = all.column_by_name("payload").unwrap();
    let payload = payload.as_any().downcast_ref::<StringArray>().unwrap();
    let got: Vec<&str> = (0..payload.len()).map(|i| payload.value(i)).collect();
    assert_eq!(got, ["a", "b", "c", "d", "e"], "rows stored in ts order");
    let snap = table.metadata().current_snapshot().unwrap();
    assert_eq!(format!("{:?}", snap.summary().operation), "Replace");
}
