//! `vessels` and `vessel_attributes`: vessel identity derived from the silver
//! `statics` and `positions` tables.
//!
//! Nothing is dropped or de-duplicated. Every distinct value a vessel ever
//! reported for an identity attribute is kept, with counts, in
//! `vessel_attributes`; `vessels` holds one row per MMSI with the winning
//! value of each attribute and boolean flags for anything suspicious (an MMSI
//! that reports two IMOs, an IMO that fails its check digit, ...). Whoever
//! consumes the tables decides what to trust.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use iceberg::spec::{NestedField, PrimitiveType, Schema};

pub const TABLE_VESSELS: &str = "vessels";
pub const TABLE_VESSEL_ATTRIBUTES: &str = "vessel_attributes";

/// Attributes tracked per MMSI, as they appear in `vessel_attributes.attribute`.
pub const ATTRIBUTES: [&str; 6] = [
    "name",
    "call_sign",
    "imo",
    "ship_type",
    "length_m",
    "beam_m",
];

fn required(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::required(id, name, ty.into()))
}

fn optional(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::optional(id, name, ty.into()))
}

/// Column order here is the column order of [`VESSELS_SQL`]'s result.
pub fn vessels_schema() -> Schema {
    let fields = vec![
        required(1, "mmsi", PrimitiveType::Long),
        required(2, "mmsi_class", PrimitiveType::String),
        optional(3, "mid", PrimitiveType::Int),
        required(4, "vessel_key", PrimitiveType::String),
        optional(5, "imo_number", PrimitiveType::Int),
        optional(6, "call_sign", PrimitiveType::String),
        optional(7, "name", PrimitiveType::String),
        optional(8, "ship_type", PrimitiveType::String),
        optional(9, "length_m", PrimitiveType::Int),
        optional(10, "beam_m", PrimitiveType::Int),
        optional(11, "ais_class", PrimitiveType::String),
        optional(12, "first_seen", PrimitiveType::Timestamptz),
        optional(13, "last_seen", PrimitiveType::Timestamptz),
        required(14, "n_positions", PrimitiveType::Long),
        optional(15, "first_static_seen", PrimitiveType::Timestamptz),
        optional(16, "last_static_seen", PrimitiveType::Timestamptz),
        required(17, "n_statics", PrimitiveType::Long),
        required(18, "mmsi_valid", PrimitiveType::Boolean),
        required(19, "imo_valid", PrimitiveType::Boolean),
        required(20, "multiple_imos", PrimitiveType::Boolean),
        required(21, "multiple_names", PrimitiveType::Boolean),
        required(22, "multiple_call_signs", PrimitiveType::Boolean),
        required(23, "computed_at", PrimitiveType::Timestamptz),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building vessels schema")
}

/// Column order here is the column order of [`ATTRIBUTES_SQL`]'s result.
pub fn vessel_attributes_schema() -> Schema {
    let fields = vec![
        required(1, "mmsi", PrimitiveType::Long),
        required(2, "attribute", PrimitiveType::String),
        required(3, "value", PrimitiveType::String),
        required(4, "n_obs", PrimitiveType::Long),
        required(5, "first_seen", PrimitiveType::Timestamptz),
        required(6, "last_seen", PrimitiveType::Timestamptz),
        required(7, "rank", PrimitiveType::Int),
        required(8, "is_current", PrimitiveType::Boolean),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building vessel_attributes schema")
}

/// IMO check digit: the first six digits weighted 7..2, summed; the last digit
/// of the sum must equal the seventh digit.
const IMO_VALID: &str = "(\
    {v} BETWEEN 1000000 AND 9999999 AND \
    ( ({v} / 1000000) % 10 * 7 + ({v} / 100000) % 10 * 6 + ({v} / 10000) % 10 * 5 \
    + ({v} / 1000) % 10 * 4 + ({v} / 100) % 10 * 3 + ({v} / 10) % 10 * 2 ) % 10 = {v} % 10)";

fn imo_valid(expr: &str) -> String {
    IMO_VALID.replace("{v}", expr)
}

/// One row per distinct (mmsi, attribute, value) ever reported in `statics`.
/// Reads the registered `statics` table.
///
/// `rank` orders a vessel's candidates for one attribute: for `imo`, a value
/// that passes the check digit beats one that doesn't; then the most often
/// reported value wins, then the most recently reported.
pub fn attributes_sql() -> String {
    let imo_pref = imo_valid("TRY_CAST(value AS BIGINT)");
    format!(
        "
WITH s AS (
  SELECT mmsi, ts,
    CASE WHEN imo_number > 0 THEN imo_number END AS imo,
    NULLIF(regexp_replace(trim(call_sign), '[@ ]+$', ''), '') AS call_sign,
    NULLIF(regexp_replace(trim(name), '[@ ]+$', ''), '') AS name,
    NULLIF(trim(ship_type), '') AS ship_type,
    CASE WHEN dimension_to_bow + dimension_to_stern > 0
         THEN dimension_to_bow + dimension_to_stern END AS length_m,
    CASE WHEN dimension_to_port + dimension_to_starboard > 0
         THEN dimension_to_port + dimension_to_starboard END AS beam_m
  FROM statics
),
obs AS (
  SELECT mmsi, ts, 'name' AS attribute, name AS value FROM s WHERE name IS NOT NULL
  UNION ALL SELECT mmsi, ts, 'call_sign', call_sign FROM s WHERE call_sign IS NOT NULL
  UNION ALL SELECT mmsi, ts, 'imo', CAST(imo AS VARCHAR) FROM s WHERE imo IS NOT NULL
  UNION ALL SELECT mmsi, ts, 'ship_type', ship_type FROM s WHERE ship_type IS NOT NULL
  UNION ALL SELECT mmsi, ts, 'length_m', CAST(length_m AS VARCHAR) FROM s WHERE length_m IS NOT NULL
  UNION ALL SELECT mmsi, ts, 'beam_m', CAST(beam_m AS VARCHAR) FROM s WHERE beam_m IS NOT NULL
),
agg AS (
  SELECT mmsi, attribute, value, count(*) AS n_obs,
         min(ts) AS first_seen, max(ts) AS last_seen
  FROM obs GROUP BY mmsi, attribute, value
),
scored AS (
  SELECT *, CASE WHEN attribute = 'imo' AND {imo_pref} THEN 1 ELSE 0 END AS pref
  FROM agg
),
ranked AS (
  SELECT mmsi, attribute, value, n_obs, first_seen, last_seen,
    CAST(row_number() OVER (
      PARTITION BY mmsi, attribute
      ORDER BY pref DESC, n_obs DESC, last_seen DESC, value
    ) AS INT) AS rank
  FROM scored
)
SELECT mmsi, attribute, value, n_obs, first_seen, last_seen, rank, rank = 1 AS is_current
FROM ranked
ORDER BY mmsi, attribute, rank"
    )
}

/// One row per MMSI seen in `positions` or `statics`. Reads the registered
/// `positions`, `statics` and `vessel_attributes` tables.
pub fn vessels_sql() -> String {
    let imo_ok = imo_valid("imo_number");
    format!(
        "
WITH pos AS (
  SELECT mmsi, min(ts) AS first_seen, max(ts) AS last_seen, count(*) AS n_positions
  FROM positions GROUP BY mmsi
),
sta AS (
  SELECT mmsi, min(ts) AS first_static_seen, max(ts) AS last_static_seen,
         count(*) AS n_statics, min(ais_class) AS ais_class
  FROM statics GROUP BY mmsi
),
cur AS (
  SELECT mmsi,
    max(CASE WHEN attribute = 'name' AND is_current THEN value END) AS name,
    max(CASE WHEN attribute = 'call_sign' AND is_current THEN value END) AS call_sign,
    max(CASE WHEN attribute = 'imo' AND is_current THEN value END) AS imo,
    max(CASE WHEN attribute = 'ship_type' AND is_current THEN value END) AS ship_type,
    max(CASE WHEN attribute = 'length_m' AND is_current THEN value END) AS length_m,
    max(CASE WHEN attribute = 'beam_m' AND is_current THEN value END) AS beam_m,
    count(*) FILTER (WHERE attribute = 'imo') AS n_imos,
    count(*) FILTER (WHERE attribute = 'name') AS n_names,
    count(*) FILTER (WHERE attribute = 'call_sign') AS n_call_signs
  FROM vessel_attributes GROUP BY mmsi
),
ids AS (
  SELECT COALESCE(pos.mmsi, sta.mmsi) AS mmsi,
    pos.first_seen, pos.last_seen, COALESCE(pos.n_positions, 0) AS n_positions,
    sta.first_static_seen, sta.last_static_seen, COALESCE(sta.n_statics, 0) AS n_statics,
    sta.ais_class
  FROM pos FULL OUTER JOIN sta ON pos.mmsi = sta.mmsi
),
typed AS (
  SELECT ids.*, CAST(cur.imo AS INT) AS imo_number, cur.call_sign, cur.name, cur.ship_type,
    CAST(cur.length_m AS INT) AS length_m, CAST(cur.beam_m AS INT) AS beam_m,
    COALESCE(cur.n_imos, 0) > 1 AS multiple_imos,
    COALESCE(cur.n_names, 0) > 1 AS multiple_names,
    COALESCE(cur.n_call_signs, 0) > 1 AS multiple_call_signs,
    CASE
      WHEN ids.mmsi BETWEEN 200000000 AND 799999999 THEN 'ship'
      WHEN ids.mmsi BETWEEN 800000000 AND 899999999 THEN 'handheld'
      WHEN ids.mmsi BETWEEN 970000000 AND 974999999 THEN 'distress_beacon'
      WHEN ids.mmsi BETWEEN 982000000 AND 987999999 THEN 'craft_associated'
      WHEN ids.mmsi BETWEEN 992000000 AND 997999999 THEN 'aton'
      WHEN ids.mmsi BETWEEN 111200000 AND 111799999 THEN 'sar_aircraft'
      WHEN ids.mmsi BETWEEN 20000000 AND 79999999 THEN 'group'
      WHEN ids.mmsi BETWEEN 2000000 AND 7999999 THEN 'coast_station'
      ELSE 'other'
    END AS mmsi_class
  FROM ids LEFT JOIN cur ON ids.mmsi = cur.mmsi
)
SELECT mmsi, mmsi_class,
  CAST(CASE mmsi_class
    WHEN 'ship' THEN mmsi / 1000000
    WHEN 'handheld' THEN (mmsi / 100000) % 1000
    WHEN 'craft_associated' THEN (mmsi / 10000) % 1000
    WHEN 'aton' THEN (mmsi / 10000) % 1000
    WHEN 'sar_aircraft' THEN (mmsi / 1000) % 1000
    WHEN 'group' THEN mmsi / 100000
    WHEN 'coast_station' THEN mmsi / 10000
  END AS INT) AS mid,
  CASE WHEN COALESCE({imo_ok}, false) THEN 'imo:' || CAST(imo_number AS VARCHAR)
       ELSE 'mmsi:' || CAST(mmsi AS VARCHAR) END AS vessel_key,
  imo_number, call_sign, name, ship_type, length_m, beam_m, ais_class,
  first_seen, last_seen, n_positions,
  first_static_seen, last_static_seen, n_statics,
  mmsi_class <> 'other' AS mmsi_valid,
  COALESCE({imo_ok}, false) AS imo_valid,
  multiple_imos, multiple_names, multiple_call_signs,
  now() AS computed_at
FROM typed
ORDER BY mmsi"
    )
}

/// The two derived tables' rows.
pub struct Vessels {
    pub vessels: Vec<RecordBatch>,
    pub attributes: Vec<RecordBatch>,
}

/// Runs both queries against a context in which `positions` and `statics`
/// are registered (Iceberg-backed in production, in-memory in tests).
pub async fn build(ctx: &SessionContext) -> Result<Vessels> {
    let attributes = ctx
        .sql(&attributes_sql())
        .await
        .context("planning vessel_attributes")?
        .collect()
        .await
        .context("computing vessel_attributes")?;

    let schema = match attributes.first() {
        Some(b) => b.schema(),
        None => ctx
            .sql(&attributes_sql())
            .await?
            .schema()
            .as_arrow()
            .clone()
            .into(),
    };
    ctx.register_table(
        "vessel_attributes",
        Arc::new(MemTable::try_new(schema, vec![attributes.clone()])?),
    )?;

    let vessels = ctx
        .sql(&vessels_sql())
        .await
        .context("planning vessels")?
        .collect()
        .await
        .context("computing vessels")?;
    Ok(Vessels {
        vessels,
        attributes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use arrow::util::display::array_value_to_string;

    fn ts_type() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
    }

    fn ts(sec: i64) -> i64 {
        (1_700_000_000 + sec) * 1_000_000
    }

    /// (mmsi, ts_sec, imo, call_sign, name, ship_type, bow, stern, port, starboard)
    type Static<'a> = (
        i64,
        i64,
        Option<i32>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
    );

    fn statics_table(rows: &[Static]) -> MemTable {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", ts_type(), false),
            Field::new("mmsi", DataType::Int64, false),
            Field::new("ais_class", DataType::Utf8, true),
            Field::new("imo_number", DataType::Int32, true),
            Field::new("call_sign", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("ship_type", DataType::Utf8, true),
            Field::new("dimension_to_bow", DataType::Int32, true),
            Field::new("dimension_to_stern", DataType::Int32, true),
            Field::new("dimension_to_port", DataType::Int32, true),
            Field::new("dimension_to_starboard", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| ts(r.1)))
                        .with_timezone("+00:00"),
                ),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter(rows.iter().map(|_| Some("Class A")))),
                Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.2))),
                Arc::new(StringArray::from_iter(rows.iter().map(|r| r.3))),
                Arc::new(StringArray::from_iter(rows.iter().map(|r| r.4))),
                Arc::new(StringArray::from_iter(rows.iter().map(|r| r.5))),
                Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.6))),
                Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.7))),
                Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.8))),
                Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.9))),
            ],
        )
        .unwrap();
        MemTable::try_new(schema, vec![vec![batch]]).unwrap()
    }

    fn positions_table(rows: &[(i64, i64)]) -> MemTable {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", ts_type(), false),
            Field::new("mmsi", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| ts(r.1)))
                        .with_timezone("+00:00"),
                ),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            ],
        )
        .unwrap();
        MemTable::try_new(schema, vec![vec![batch]]).unwrap()
    }

    /// Cell of the single row matching `where_clause`, as text ("NULL" for null).
    async fn cell(ctx: &SessionContext, table: &str, col: &str, where_clause: &str) -> String {
        let batches = ctx
            .sql(&format!("SELECT {col} FROM {table} WHERE {where_clause}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "{table} WHERE {where_clause} matched {rows} rows");
        let arr = batches[0].column(0);
        if arr.is_null(0) {
            "NULL".into()
        } else {
            array_value_to_string(arr, 0).unwrap()
        }
    }

    async fn fixture() -> (SessionContext, Vessels) {
        let ctx = SessionContext::new();
        let s: Vec<Static> = vec![
            // A ship whose type 24 arrives as two halves (A: name; B: type,
            // callsign, dimensions), with a padded and a misspelt name.
            (366123456, 0, None, None, Some("EVER GIVEN@@@@"), None, None, None, None, None),
            (366123456, 1, None, Some("KABC@@"), None, Some("Cargo"), Some(300), Some(100), Some(20), Some(40)),
            (366123456, 10, None, None, Some("EVER GIVEN@@@@"), None, None, None, None, None),
            (366123456, 20, None, None, Some("EVER GIVN"), None, None, None, None, None),
            // Two IMOs: the more frequent one fails the check digit.
            (211000001, 0, Some(1234568), None, Some("TWO IMOS"), None, None, None, None, None),
            (211000001, 5, Some(1234568), None, Some("TWO IMOS"), None, None, None, None, None),
            (211000001, 9, Some(1234567), None, Some("TWO IMOS"), None, None, None, None, None),
            // Placeholder name and IMO 0 mean "not available".
            (538000002, 0, Some(0), None, Some("@@@@@@@@"), None, None, None, None, None),
        ];
        ctx.register_table("statics", Arc::new(statics_table(&s))).unwrap();
        ctx.register_table(
            "positions",
            Arc::new(positions_table(&[
                (366123456, 0),
                (366123456, 100),
                (366123456, 50),
                (211000001, 3),
                (111111111, 7), // never sent statics; not a valid MMSI
                (992471234, 8), // an AtoN
            ])),
        )
        .unwrap();
        let v = build(&ctx).await.unwrap();
        for (name, batches) in [("v", &v.vessels), ("a", &v.attributes)] {
            ctx.register_table(
                name,
                Arc::new(MemTable::try_new(batches[0].schema(), vec![batches.clone()]).unwrap()),
            )
            .unwrap();
        }
        (ctx, v)
    }

    #[tokio::test]
    async fn merges_type24_halves_and_picks_most_common_value() {
        let (ctx, _) = fixture().await;
        let w = "mmsi = 366123456";
        assert_eq!(cell(&ctx, "v", "name", w).await, "EVER GIVEN");
        assert_eq!(cell(&ctx, "v", "call_sign", w).await, "KABC");
        assert_eq!(cell(&ctx, "v", "ship_type", w).await, "Cargo");
        assert_eq!(cell(&ctx, "v", "length_m", w).await, "400");
        assert_eq!(cell(&ctx, "v", "beam_m", w).await, "60");
        assert_eq!(cell(&ctx, "v", "mmsi_class", w).await, "ship");
        assert_eq!(cell(&ctx, "v", "mid", w).await, "366");
        assert_eq!(cell(&ctx, "v", "n_positions", w).await, "3");
        assert_eq!(cell(&ctx, "v", "n_statics", w).await, "4");
        assert_eq!(cell(&ctx, "v", "multiple_names", w).await, "true");
        // The losing spelling is kept, with its count.
        assert_eq!(
            cell(&ctx, "a", "n_obs", &format!("{w} AND value = 'EVER GIVN'")).await,
            "1"
        );
    }

    #[tokio::test]
    async fn conflicting_imos_prefer_valid_check_digit_and_are_flagged() {
        let (ctx, _) = fixture().await;
        let w = "mmsi = 211000001";
        assert_eq!(cell(&ctx, "v", "imo_number", w).await, "1234567");
        assert_eq!(cell(&ctx, "v", "imo_valid", w).await, "true");
        assert_eq!(cell(&ctx, "v", "vessel_key", w).await, "imo:1234567");
        assert_eq!(cell(&ctx, "v", "multiple_imos", w).await, "true");
        // Both candidates survive, the invalid one included.
        assert_eq!(
            cell(&ctx, "a", "n_obs", &format!("{w} AND attribute = 'imo' AND value = '1234568'")).await,
            "2"
        );
    }

    #[tokio::test]
    async fn placeholders_become_null_and_vessels_are_never_dropped() {
        let (ctx, v) = fixture().await;
        let w = "mmsi = 538000002";
        assert_eq!(cell(&ctx, "v", "name", w).await, "NULL");
        assert_eq!(cell(&ctx, "v", "imo_number", w).await, "NULL");
        assert_eq!(cell(&ctx, "v", "vessel_key", w).await, "mmsi:538000002");
        assert_eq!(cell(&ctx, "v", "n_positions", w).await, "0");

        // Seen only in positions: still present, flagged invalid.
        let p = "mmsi = 111111111";
        assert_eq!(cell(&ctx, "v", "mmsi_valid", p).await, "false");
        assert_eq!(cell(&ctx, "v", "n_statics", p).await, "0");
        assert_eq!(cell(&ctx, "v", "mmsi_class", "mmsi = 992471234").await, "aton");
        assert_eq!(cell(&ctx, "v", "mid", "mmsi = 992471234").await, "247");

        let n: usize = v.vessels.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 5, "every MMSI in either input gets a row");
    }

    #[tokio::test]
    async fn output_matches_the_iceberg_schemas() {
        let (_, v) = fixture().await;
        for (schema, batches) in [
            (vessels_schema(), &v.vessels),
            (vessel_attributes_schema(), &v.attributes),
        ] {
            let want = iceberg::arrow::schema_to_arrow_schema(&schema).unwrap();
            let got = batches[0].schema();
            assert_eq!(want.fields().len(), got.fields().len());
            for (i, (w, g)) in want.fields().iter().zip(got.fields()).enumerate() {
                assert_eq!(w.name(), g.name(), "column {i}");
                if !w.is_nullable() {
                    let nulls: usize = batches.iter().map(|b| b.column(i).null_count()).sum();
                    assert_eq!(nulls, 0, "required column {} has nulls", w.name());
                }
            }
        }
    }
}
