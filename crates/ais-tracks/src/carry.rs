//! State handed from one day to the next: each vessel's last row of the
//! previous day, so the next day can continue an id across midnight.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

pub fn lit(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S+00:00").to_string()
}

fn schema(id_col: &str) -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("mmsi", DataType::Int64, false),
        Field::new(id_col, DataType::Utf8, false),
        Field::new(
            "ts_end",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            false,
        ),
    ]))
}

/// Registers `target` (columns `mmsi`, `id_col`, `ts_end`) holding the last
/// row per vessel of `source`, optionally only rows starting in `[from, to)`.
/// `None` registers an empty table.
pub async fn set_previous(
    ctx: &SessionContext,
    target: &str,
    id_col: &str,
    source: Option<(&str, Option<(DateTime<Utc>, DateTime<Utc>)>)>,
) -> Result<()> {
    let _ = ctx.deregister_table(target)?;
    let table = match source {
        None => MemTable::try_new(schema(id_col), vec![vec![]])?,
        Some((name, range)) => {
            let filter = match range {
                Some((from, to)) => format!("WHERE ts >= '{}' AND ts < '{}'", lit(from), lit(to)),
                None => String::new(),
            };
            let batches = ctx
                .sql(&format!(
                    "SELECT mmsi, {id_col}, ts_end FROM (
                       SELECT mmsi, {id_col}, ts_end,
                              row_number() OVER (PARTITION BY mmsi ORDER BY ts_end DESC, ts DESC) AS rn
                       FROM {name} {filter}) t
                     WHERE rn = 1"
                ))
                .await
                .context("planning previous-day state")?
                .collect()
                .await
                .context("reading previous-day state")?;
            let s = batches.first().map(|b| b.schema()).unwrap_or_else(|| schema(id_col));
            MemTable::try_new(s, vec![batches])?
        }
    };
    ctx.register_table(target, Arc::new(table))?;
    Ok(())
}

/// Registers `batches` (one day's output) under `name` so it can be the
/// source for [`set_previous`]. Returns false for an empty day.
pub fn register_output(ctx: &SessionContext, name: &str, batches: &[RecordBatch]) -> Result<bool> {
    let _ = ctx.deregister_table(name)?;
    let Some(first) = batches.first() else {
        return Ok(false);
    };
    ctx.register_table(
        name,
        Arc::new(MemTable::try_new(first.schema(), vec![batches.to_vec()])?),
    )?;
    Ok(true)
}

/// Every batch has `schema`'s columns in order and no null in a required one.
pub fn check_batches(schema: &iceberg::spec::Schema, batches: &[RecordBatch]) -> Result<()> {
    let want = iceberg::arrow::schema_to_arrow_schema(schema)?;
    for b in batches {
        anyhow::ensure!(
            b.num_columns() == want.fields().len(),
            "expected {} columns, got {}",
            want.fields().len(),
            b.num_columns()
        );
        for (i, (w, g)) in want.fields().iter().zip(b.schema().fields()).enumerate() {
            anyhow::ensure!(w.name() == g.name(), "column {i}: {} != {}", w.name(), g.name());
            anyhow::ensure!(
                w.is_nullable() || b.column(i).null_count() == 0,
                "required column {} contains nulls",
                w.name()
            );
        }
    }
    Ok(())
}
