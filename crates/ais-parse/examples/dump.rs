//! Debug helper: print each row of a decoded Parquet file as loose key=value
//! text. Usage: `cargo run -p ais-parse --example dump -- <file.parquet>`

use arrow::array::{Array, TimestampMillisecondArray};
use chrono::TimeZone;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).expect("usage: dump <file.parquet>");
    let file = std::fs::File::open(&path)?;
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    for batch in reader {
        let batch = batch?;
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let mut parts = Vec::new();
            for (col, field) in batch.columns().iter().zip(schema.fields()) {
                let value = if col.is_null(row) {
                    "null".to_string()
                } else if let Some(ts) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
                    chrono::Utc
                        .timestamp_millis_opt(ts.value(row))
                        .single()
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_else(|| ts.value(row).to_string())
                } else {
                    arrow::util::display::array_value_to_string(col, row)?
                };
                parts.push(format!("{}={}", field.name(), value));
            }
            println!("{}", parts.join(" "));
        }
    }
    Ok(())
}
