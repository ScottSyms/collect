//! Where a day of raw reports comes from: the silver `positions` Iceberg
//! table, or a local directory of Parquet files (for development and for
//! testing at scale without a catalog). Both give a bounded, streaming scan of
//! only the columns the router needs.

use std::path::Path;
use std::pin::Pin;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures_util::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Reference;
use iceberg::spec::Datum;
use iceberg::table::Table;

use crate::router::SOURCE_COLUMNS;

pub type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// Files read at once. Each in-flight file holds a row group in memory.
const FILE_CONCURRENCY: usize = 2;
const BATCH_ROWS: usize = 8192;

pub fn day_bounds_us(day: chrono::NaiveDate) -> (i64, i64) {
    let start = Utc.from_utc_datetime(&day.and_hms_opt(0, 0, 0).expect("midnight"));
    let end = start + chrono::Duration::days(1);
    (start.timestamp() * 1_000_000, end.timestamp() * 1_000_000)
}

fn lit(us: i64) -> String {
    let t: DateTime<Utc> = Utc.timestamp_micros(us).single().expect("valid time");
    t.format("%Y-%m-%dT%H:%M:%S+00:00").to_string()
}

/// One day of the `positions` Iceberg table, plus the row count the plan
/// implies (used to choose a bucket count).
pub async fn iceberg_day(
    table: &Table,
    day_start_us: i64,
    day_end_us: i64,
) -> Result<(BatchStream, u64)> {
    let pred = Reference::new("ts")
        .greater_than_or_equal_to(Datum::timestamptz_micros(day_start_us))
        .and(Reference::new("ts").less_than(Datum::timestamptz_micros(day_end_us)));
    let scan = table
        .scan()
        .select(SOURCE_COLUMNS)
        .with_filter(pred)
        .build()
        .context("planning the silver scan")?;
    let tasks: Vec<_> = scan
        .plan_files()
        .await
        .context("listing silver files")?
        .try_collect()
        .await?;
    let est: u64 = tasks.iter().filter_map(|t| t.record_count).sum();
    let reader = ArrowReaderBuilder::new(table.file_io().clone())
        .with_data_file_concurrency_limit(FILE_CONCURRENCY)
        .with_batch_size(BATCH_ROWS)
        .build();
    let stream = reader.read(futures_util::stream::iter(tasks.into_iter().map(Ok)).boxed())?;
    Ok((stream.map_err(anyhow::Error::from).boxed(), est))
}

/// One day from a directory of Parquet files (searched recursively).
pub async fn parquet_dir_day(
    dir: &Path,
    day_start_us: i64,
    day_end_us: i64,
) -> Result<(BatchStream, u64)> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(FILE_CONCURRENCY)
            .with_batch_size(BATCH_ROWS),
    );
    ctx.register_parquet(
        "silver",
        dir.to_str().context("non-UTF-8 path")?,
        ParquetReadOptions::default(),
    )
    .await
    .with_context(|| format!("reading Parquet under {}", dir.display()))?;
    let filter = format!("ts >= '{}' AND ts < '{}'", lit(day_start_us), lit(day_end_us));
    let counted = ctx
        .sql(&format!("SELECT count(*) FROM silver WHERE {filter}"))
        .await?
        .collect()
        .await?;
    let est = counted
        .first()
        .and_then(|b| {
            arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Int64)
                .ok()
                .and_then(|c| {
                    c.as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .map(|a| a.value(0) as u64)
                })
        })
        .unwrap_or(0);
    let stream = ctx
        .sql(&format!(
            "SELECT {} FROM silver WHERE {filter}",
            SOURCE_COLUMNS.join(", ")
        ))
        .await?
        .execute_stream()
        .await?;
    Ok((
        stream.map_err(anyhow::Error::from).boxed(),
        est,
    ))
}
