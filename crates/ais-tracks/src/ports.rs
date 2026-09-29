//! `ref_ports`: the NGA World Port Index (Publication 150) as a reference
//! table. Each load is appended under its own `wpi_release` label and nothing
//! is overwritten, so a stop matched last year can be reproduced against the
//! release it used; matching uses the newest release.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use iceberg::spec::{NestedField, PrimitiveType, Schema};

pub const TABLE_REF_PORTS: &str = "ref_ports";

fn required(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::required(id, name, ty.into()))
}

fn optional(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::optional(id, name, ty.into()))
}

/// Column order here is the column order of [`load_sql`]'s result.
pub fn ref_ports_schema() -> Schema {
    use PrimitiveType::*;
    let fields = vec![
        required(1, "wpi_release", String),
        required(2, "loaded_at", Timestamptz),
        required(3, "port_id", Long),
        optional(4, "name", String),
        optional(5, "alt_name", String),
        optional(6, "unlocode", String),
        optional(7, "country", String),
        optional(8, "region", String),
        optional(9, "harbor_size", String),
        optional(10, "harbor_type", String),
        optional(11, "harbor_use", String),
        optional(12, "latitude", Double),
        optional(13, "longitude", Double),
        optional(14, "channel_depth_m", Double),
        optional(15, "max_vessel_draft_m", Double),
        optional(16, "tidal_range_m", Double),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building ref_ports schema")
}

fn text(column: &str) -> String {
    format!("NULLIF(trim(CAST(\"{column}\" AS VARCHAR)), '')")
}

fn number(column: &str) -> String {
    format!("TRY_CAST(NULLIF(trim(CAST(\"{column}\" AS VARCHAR)), '') AS DOUBLE)")
}

/// SQL over the registered `wpi_raw` table (every column read as text).
pub fn load_sql(release: &str) -> String {
    let release = release.replace('\'', "''");
    format!(
        "SELECT '{release}' AS wpi_release, now() AS loaded_at,
           CAST({pid} AS BIGINT) AS port_id,
           {name} AS name, {alt} AS alt_name, {loc} AS unlocode,
           {country} AS country, {region} AS region,
           {size} AS harbor_size, {htype} AS harbor_type, {use_} AS harbor_use,
           {lat} AS latitude, {lon} AS longitude,
           {depth} AS channel_depth_m, {draft} AS max_vessel_draft_m, {tide} AS tidal_range_m
         FROM wpi_raw
         WHERE {pid} IS NOT NULL
         ORDER BY port_id",
        pid = number("World Port Index Number"),
        name = text("Main Port Name"),
        alt = text("Alternate Port Name"),
        loc = text("UN/LOCODE"),
        country = text("Country Code"),
        region = text("Region Name"),
        size = text("Harbor Size"),
        htype = text("Harbor Type"),
        use_ = text("Harbor Use"),
        lat = number("Latitude"),
        lon = number("Longitude"),
        depth = number("Channel Depth (m)"),
        draft = number("Maximum Vessel Draft (m)"),
        tide = number("Tidal Range (m)"),
    )
}

/// Reads the Pub 150 CSV at `path` into `ref_ports` rows labelled `release`.
pub async fn load_csv(path: &str, release: &str) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new();
    // Zero inference rows: every column stays text, so a stray value in a
    // numeric-looking column can't fail the read.
    ctx.register_csv(
        "wpi_raw",
        path,
        CsvReadOptions::new().has_header(true).schema_infer_max_records(0),
    )
    .await
    .with_context(|| format!("reading {path}"))?;
    ctx.sql(&load_sql(release))
        .await
        .context("planning the port load (is this the Pub 150 CSV?)")?
        .collect()
        .await
        .context("loading ports")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::carry::check_batches;
    use arrow::array::Array;

    const CSV: &str = "\u{feff}OID_,World Port Index Number,Region Name,Main Port Name,Alternate Port Name,UN/LOCODE,Country Code,Harbor Size,Harbor Type,Harbor Use,Channel Depth (m),Maximum Vessel Draft (m),Tidal Range (m),Latitude,Longitude
1,7950.0,New Jersey,Maurer,,\" \",United States,Very Small,River (Natural),Unknown,,,1.2,40.5333,-74.25
2,1234.0,Singapore,Singapore,Port of Singapore,SGSIN,Singapore,Large,Coastal (Natural),Unknown,15.5,14.0,,1.26,103.83
3,99.0,Nowhere,No Coordinates,,,Atlantis,Small,Unknown,Unknown,,,,,
";

    #[tokio::test]
    async fn loads_the_pub150_layout_keeping_every_port() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pub150.csv");
        std::fs::write(&path, CSV).unwrap();
        let batches = load_csv(path.to_str().unwrap(), "2026-03-01").await.unwrap();
        check_batches(&ref_ports_schema(), &batches).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("p", arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap())
            .unwrap();
        let rows = ctx
            .sql("SELECT port_id, unlocode, latitude, longitude FROM p ORDER BY port_id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let b = &rows[0];
        assert_eq!(b.num_rows(), 3, "every port is kept, coordinates or not");
        let cell = |col: usize, row: usize| {
            let c = b.column(col);
            if c.is_null(row) {
                "NULL".to_string()
            } else {
                arrow::util::display::array_value_to_string(c, row).unwrap()
            }
        };
        // Ordered by port_id: 99, 1234, 7950.
        assert_eq!((cell(1, 0), cell(2, 0)), ("NULL".into(), "NULL".into()), "no code, no coordinates");
        assert_eq!((cell(1, 1), cell(2, 1), cell(3, 1)), ("SGSIN".into(), "1.26".into(), "103.83".into()));
        assert_eq!(cell(1, 2), "NULL", "a blank ' ' UN/LOCODE is null");
        assert_eq!(cell(2, 2), "40.5333");
    }

    /// Runs against a real download when `PUB150_CSV` points at one.
    #[tokio::test]
    async fn real_pub150_loads_when_available() {
        let Ok(path) = std::env::var("PUB150_CSV") else {
            return;
        };
        let batches = load_csv(&path, "test").await.unwrap();
        check_batches(&ref_ports_schema(), &batches).unwrap();
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        let ctx = SessionContext::new();
        ctx.register_batch("p", arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap())
            .unwrap();
        let q = ctx
            .sql("SELECT count(*), count(latitude), count(unlocode), count(harbor_size),
                         min(latitude), max(latitude), min(longitude), max(longitude),
                         count(DISTINCT port_id) FROM p")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        eprintln!("{}", arrow::util::pretty::pretty_format_batches(&q).unwrap());
        assert!(n > 3000, "{n} ports");
    }
}
