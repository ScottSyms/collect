//! GeoNames (<https://www.geonames.org>, CC-BY 4.0) dump files as Iceberg
//! tables. Every file is UTF-8, tab-separated and unquoted (names can contain
//! `"`), so lines are split on tabs rather than read as CSV. Each table is the
//! file's columns in file order plus `loaded_at` and `source_file`.

use std::collections::HashSet;
use std::io::BufRead;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Utc};
use iceberg::spec::{NestedField, PrimitiveType, Schema};

/// Rows per Arrow batch while parsing.
const BATCH_ROWS: usize = 500_000;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    Str,
    Int,
    Long,
    Double,
    /// `yyyy-MM-dd`.
    Date,
    /// `1` is true; empty or anything else is false.
    Flag,
}

pub struct Col {
    pub name: &'static str,
    pub kind: Kind,
    pub required: bool,
}

const fn req(name: &'static str, kind: Kind) -> Col {
    Col { name, kind, required: true }
}

const fn opt(name: &'static str, kind: Kind) -> Col {
    Col { name, kind, required: false }
}

/// One GeoNames file and the table it loads into.
pub struct Dataset {
    pub table: &'static str,
    /// File name inside the dump directory (or inside its zip).
    pub file: &'static str,
    /// Columns in file order.
    pub cols: &'static [Col],
    /// The first field is `<class>.<code>` and splits into two columns
    /// (`featureCodes_en.txt`).
    pub split_first_on_dot: bool,
}

use Kind::*;

pub const PLACENAMES: Dataset = Dataset {
    table: "placenames",
    file: "allCountries.txt",
    cols: &[
        req("geonameid", Long),
        opt("name", Str),
        opt("asciiname", Str),
        opt("alternatenames", Str),
        opt("latitude", Double),
        opt("longitude", Double),
        opt("feature_class", Str),
        opt("feature_code", Str),
        opt("country_code", Str),
        opt("cc2", Str),
        opt("admin1_code", Str),
        opt("admin2_code", Str),
        opt("admin3_code", Str),
        opt("admin4_code", Str),
        opt("population", Long),
        opt("elevation", Int),
        opt("dem", Int),
        opt("timezone", Str),
        opt("modification_date", Date),
    ],
    split_first_on_dot: false,
};

pub const ALTERNATE_NAMES: Dataset = Dataset {
    table: "placename_alternate_names",
    file: "alternateNamesV2.txt",
    cols: &[
        req("alternate_name_id", Long),
        req("geonameid", Long),
        opt("isolanguage", Str),
        opt("alternate_name", Str),
        req("is_preferred_name", Flag),
        req("is_short_name", Flag),
        req("is_colloquial", Flag),
        req("is_historic", Flag),
        opt("from_period", Str),
        opt("to_period", Str),
    ],
    split_first_on_dot: false,
};

pub const FEATURE_CODES: Dataset = Dataset {
    table: "placename_feature_codes",
    file: "featureCodes_en.txt",
    cols: &[
        req("feature_class", Str),
        req("feature_code", Str),
        opt("name", Str),
        opt("description", Str),
    ],
    split_first_on_dot: true,
};

pub const COUNTRIES: Dataset = Dataset {
    table: "placename_countries",
    file: "countryInfo.txt",
    cols: &[
        req("iso", Str),
        opt("iso3", Str),
        opt("iso_numeric", Int),
        opt("fips", Str),
        opt("country", Str),
        opt("capital", Str),
        opt("area_sq_km", Double),
        opt("population", Long),
        opt("continent", Str),
        opt("tld", Str),
        opt("currency_code", Str),
        opt("currency_name", Str),
        opt("phone", Str),
        opt("postal_code_format", Str),
        opt("postal_code_regex", Str),
        opt("languages", Str),
        opt("geonameid", Long),
        opt("neighbours", Str),
        opt("equivalent_fips_code", Str),
    ],
    split_first_on_dot: false,
};

/// `code` is `<country>.<admin1>`, e.g. `CA.08`.
pub const ADMIN1_CODES: Dataset = Dataset {
    table: "placename_admin1_codes",
    file: "admin1CodesASCII.txt",
    cols: &[
        req("code", Str),
        opt("name", Str),
        opt("asciiname", Str),
        opt("geonameid", Long),
    ],
    split_first_on_dot: false,
};

/// `code` is `<country>.<admin1>.<admin2>`, e.g. `CA.08.3528`.
pub const ADMIN2_CODES: Dataset = Dataset {
    table: "placename_admin2_codes",
    file: "admin2Codes.txt",
    cols: &[
        req("code", Str),
        opt("name", Str),
        opt("asciiname", Str),
        opt("geonameid", Long),
    ],
    split_first_on_dot: false,
};

/// The small lookup tables loaded with every `placenames load`.
pub const LOOKUPS: [&Dataset; 4] = [&FEATURE_CODES, &COUNTRIES, &ADMIN1_CODES, &ADMIN2_CODES];

fn iceberg_type(kind: Kind) -> PrimitiveType {
    match kind {
        Str => PrimitiveType::String,
        Int => PrimitiveType::Int,
        Long => PrimitiveType::Long,
        Double => PrimitiveType::Double,
        Date => PrimitiveType::Date,
        Flag => PrimitiveType::Boolean,
    }
}

fn arrow_type(kind: Kind) -> DataType {
    match kind {
        Str => DataType::Utf8,
        Int => DataType::Int32,
        Long => DataType::Int64,
        Double => DataType::Float64,
        Date => DataType::Date32,
        Flag => DataType::Boolean,
    }
}

fn loaded_at_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
}

impl Dataset {
    /// The file's columns, then `loaded_at` and `source_file`.
    pub fn schema(&self) -> Schema {
        let mut fields: Vec<Arc<NestedField>> = self
            .cols
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let id = i as i32 + 1;
                let ty = iceberg_type(c.kind).into();
                Arc::new(if c.required {
                    NestedField::required(id, c.name, ty)
                } else {
                    NestedField::optional(id, c.name, ty)
                })
            })
            .collect();
        let n = self.cols.len() as i32;
        fields.push(Arc::new(NestedField::required(n + 1, "loaded_at", PrimitiveType::Timestamptz.into())));
        fields.push(Arc::new(NestedField::required(n + 2, "source_file", PrimitiveType::String.into())));
        Schema::builder()
            .with_schema_id(1)
            .with_fields(fields)
            .build()
            .expect("building GeoNames table schema")
    }

    fn arrow_schema(&self) -> Arc<ArrowSchema> {
        let mut fields: Vec<Field> = self
            .cols
            .iter()
            .map(|c| Field::new(c.name, arrow_type(c.kind), !c.required))
            .collect();
        fields.push(Field::new("loaded_at", loaded_at_type(), false));
        fields.push(Field::new("source_file", DataType::Utf8, false));
        Arc::new(ArrowSchema::new(fields))
    }

    fn col_index(&self, name: &str) -> Option<usize> {
        self.cols.iter().position(|c| c.name == name)
    }
}

/// Which rows to keep.
#[derive(Default)]
pub struct Filter {
    /// Keep only these feature classes (`placenames` only).
    pub feature_classes: Option<HashSet<String>>,
    /// Keep only these geonameids (`placename_alternate_names` only).
    pub geonameids: Option<HashSet<i64>>,
}

/// What a parse produced.
#[derive(Default)]
pub struct Parsed {
    pub batches: Vec<RecordBatch>,
    pub rows: usize,
    /// Rows dropped by the filter.
    pub filtered: usize,
    /// Lines skipped because a required field was missing or unparseable.
    pub rejected: usize,
    /// Optional fields that held something unparseable, stored as null.
    pub bad_values: usize,
}

impl Parsed {
    pub fn extend(&mut self, other: Parsed) {
        self.batches.extend(other.batches);
        self.rows += other.rows;
        self.filtered += other.filtered;
        self.rejected += other.rejected;
        self.bad_values += other.bad_values;
    }
}

enum Builder {
    Str(StringBuilder),
    Int(Int32Builder),
    Long(Int64Builder),
    Double(Float64Builder),
    Date(Date32Builder),
    Flag(BooleanBuilder),
}

/// A parsed field: `None` for empty, `Err` for unparseable.
enum Value<'a> {
    Str(&'a str),
    Int(i32),
    Long(i64),
    Double(f64),
    Date(i32),
    Flag(bool),
}

fn parse_field(kind: Kind, raw: &str) -> Result<Option<Value<'_>>, ()> {
    let s = raw.trim();
    if kind == Flag {
        return Ok(Some(Value::Flag(s == "1")));
    }
    if s.is_empty() {
        return Ok(None);
    }
    Ok(Some(match kind {
        Str => Value::Str(s),
        Int => Value::Int(s.parse().map_err(|_| ())?),
        Long => Value::Long(s.parse().map_err(|_| ())?),
        Double => Value::Double(s.parse().map_err(|_| ())?),
        Date => {
            let d = NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| ())?;
            Value::Date((d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
        }
        Flag => unreachable!(),
    }))
}

impl Builder {
    fn new(kind: Kind) -> Self {
        match kind {
            Str => Builder::Str(StringBuilder::new()),
            Int => Builder::Int(Int32Builder::new()),
            Long => Builder::Long(Int64Builder::new()),
            Double => Builder::Double(Float64Builder::new()),
            Date => Builder::Date(Date32Builder::new()),
            Flag => Builder::Flag(BooleanBuilder::new()),
        }
    }

    fn append(&mut self, v: Option<&Value>) {
        match (self, v) {
            (Builder::Str(b), Some(Value::Str(s))) => b.append_value(s),
            (Builder::Int(b), Some(Value::Int(x))) => b.append_value(*x),
            (Builder::Long(b), Some(Value::Long(x))) => b.append_value(*x),
            (Builder::Double(b), Some(Value::Double(x))) => b.append_value(*x),
            (Builder::Date(b), Some(Value::Date(x))) => b.append_value(*x),
            (Builder::Flag(b), Some(Value::Flag(x))) => b.append_value(*x),
            (Builder::Str(b), _) => b.append_null(),
            (Builder::Int(b), _) => b.append_null(),
            (Builder::Long(b), _) => b.append_null(),
            (Builder::Double(b), _) => b.append_null(),
            (Builder::Date(b), _) => b.append_null(),
            (Builder::Flag(b), _) => b.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Builder::Str(b) => Arc::new(b.finish()),
            Builder::Int(b) => Arc::new(b.finish()),
            Builder::Long(b) => Arc::new(b.finish()),
            Builder::Double(b) => Arc::new(b.finish()),
            Builder::Date(b) => Arc::new(b.finish()),
            Builder::Flag(b) => Arc::new(b.finish()),
        }
    }
}

struct Batcher<'a> {
    ds: &'a Dataset,
    schema: Arc<ArrowSchema>,
    builders: Vec<Builder>,
    pending: usize,
    loaded_at_us: i64,
    source_file: &'a str,
}

impl<'a> Batcher<'a> {
    fn new(ds: &'a Dataset, loaded_at: DateTime<Utc>, source_file: &'a str) -> Self {
        Batcher {
            ds,
            schema: ds.arrow_schema(),
            builders: ds.cols.iter().map(|c| Builder::new(c.kind)).collect(),
            pending: 0,
            loaded_at_us: loaded_at.timestamp_micros(),
            source_file,
        }
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending == 0 {
            return Ok(None);
        }
        let mut cols: Vec<ArrayRef> = self.builders.iter_mut().map(Builder::finish).collect();
        cols.push(Arc::new(
            TimestampMicrosecondArray::from(vec![self.loaded_at_us; self.pending])
                .with_timezone("+00:00"),
        ));
        cols.push(Arc::new(arrow::array::StringArray::from(vec![self.source_file; self.pending])));
        self.pending = 0;
        Ok(Some(RecordBatch::try_new(self.schema.clone(), cols).context(self.ds.table)?))
    }
}

/// Parses one GeoNames file. Blank lines and `#` comment lines are skipped.
pub fn parse(
    ds: &Dataset,
    reader: impl BufRead,
    source_file: &str,
    loaded_at: DateTime<Utc>,
    filter: &Filter,
) -> Result<Parsed> {
    let class_col = ds.col_index("feature_class");
    let id_col = ds.col_index("geonameid");
    let mut out = Parsed::default();
    let mut batcher = Batcher::new(ds, loaded_at, source_file);

    for (n, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("{source_file}: reading line {}", n + 1))?;
        let line = line.trim_end_matches('\r');
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields: Vec<&str> = line.split('\t').collect();
        if ds.split_first_on_dot {
            let (a, b) = fields[0].split_once('.').unwrap_or((fields[0], ""));
            fields.splice(0..1, [a, b]);
        }

        let mut values: Vec<Option<Value>> = Vec::with_capacity(ds.cols.len());
        let mut ok = true;
        let mut bad = 0;
        for (i, col) in ds.cols.iter().enumerate() {
            match parse_field(col.kind, fields.get(i).copied().unwrap_or("")) {
                Ok(v) => {
                    if col.required && v.is_none() {
                        ok = false;
                        break;
                    }
                    values.push(v);
                }
                Err(()) if col.required => {
                    ok = false;
                    break;
                }
                Err(()) => {
                    bad += 1;
                    values.push(None);
                }
            }
        }
        if !ok {
            out.rejected += 1;
            continue;
        }

        let keep_class = match (&filter.feature_classes, class_col) {
            (Some(set), Some(i)) => matches!(&values[i], Some(Value::Str(s)) if set.contains(*s)),
            _ => true,
        };
        let keep_id = match (&filter.geonameids, id_col) {
            (Some(set), Some(i)) => matches!(&values[i], Some(Value::Long(id)) if set.contains(id)),
            _ => true,
        };
        if !(keep_class && keep_id) {
            out.filtered += 1;
            continue;
        }

        out.bad_values += bad;
        for (b, v) in batcher.builders.iter_mut().zip(&values) {
            b.append(v.as_ref());
        }
        batcher.pending += 1;
        out.rows += 1;
        if batcher.pending >= BATCH_ROWS {
            out.batches.extend(batcher.flush()?);
        }
    }
    out.batches.extend(batcher.flush()?);
    Ok(out)
}

/// The `geonameid`s in parsed `placenames` batches.
pub fn geonameids(batches: &[RecordBatch]) -> HashSet<i64> {
    let mut ids = HashSet::new();
    for b in batches {
        let col = b
            .column_by_name("geonameid")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int64Array>());
        if let Some(col) = col {
            ids.extend(col.iter().flatten());
        }
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringArray};

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn run(ds: &Dataset, text: &str, filter: &Filter) -> Parsed {
        let p = parse(ds, text.as_bytes(), "test.txt", now(), filter).unwrap();
        ais_tracks::carry::check_batches(&ds.schema(), &p.batches).unwrap();
        p
    }

    fn col<'a, T: 'static>(b: &'a RecordBatch, name: &str) -> &'a T {
        b.column_by_name(name).unwrap().as_any().downcast_ref::<T>().unwrap()
    }

    const PLACES: &str = "\
6167865\tToronto\tToronto\tTO,Toronto\t43.70011\t-79.4163\tP\tPPLA\tCA\t\t08\t3520\t\t\t2600000\t\t175\tAmerica/Toronto\t2024-03-01
3133880\tTrondheimsfjorden \"the fjord\"\tTrondheimsfjorden\t\t63.6\t10.5\tH\tFJD\tNO\t\t\t\t\t\t0\tnot-a-number\t-9999\tEurope/Oslo\t2019-11-05
\tNo id\tNo id\t\t1\t2\tP\tPPL\tXX\t\t\t\t\t\t\t\t\t\t
5\tBad coords\tBad coords\t\tabc\t2\tT\tMT\tXX\t\t\t\t\t\t\t\t\t\t2020-01-01
";

    #[test]
    fn parses_placenames() {
        let p = run(&PLACENAMES, PLACES, &Filter::default());
        assert_eq!(p.rows, 3);
        assert_eq!(p.rejected, 1, "line without a geonameid");
        assert_eq!(p.bad_values, 2, "elevation 'not-a-number' and latitude 'abc'");
        let b = &p.batches[0];
        assert_eq!(b.num_columns(), PLACENAMES.cols.len() + 2);
        assert_eq!(col::<Int64Array>(b, "geonameid").value(0), 6167865);
        assert_eq!(col::<StringArray>(b, "name").value(1), "Trondheimsfjorden \"the fjord\"");
        assert!(col::<StringArray>(b, "alternatenames").is_null(1));
        assert!(col::<StringArray>(b, "cc2").is_null(0));
        assert_eq!(col::<Float64Array>(b, "longitude").value(0), -79.4163);
        assert!(col::<Float64Array>(b, "latitude").is_null(2));
        assert_eq!(col::<Int64Array>(b, "population").value(0), 2_600_000);
        assert!(col::<Int32Array>(b, "elevation").is_null(1));
        assert_eq!(col::<Int32Array>(b, "dem").value(1), -9999);
        let d = col::<Date32Array>(b, "modification_date");
        assert_eq!(d.value_as_date(0), NaiveDate::from_ymd_opt(2024, 3, 1));
        assert!(!d.is_null(1));
        assert_eq!(col::<StringArray>(b, "source_file").value(2), "test.txt");
    }

    #[test]
    fn filters_feature_class() {
        let filter = Filter {
            feature_classes: Some(["H".to_string()].into()),
            ..Default::default()
        };
        let p = run(&PLACENAMES, PLACES, &filter);
        assert_eq!(p.rows, 1);
        assert_eq!(p.filtered, 2);
        assert_eq!(col::<StringArray>(&p.batches[0], "feature_code").value(0), "FJD");
    }

    #[test]
    fn alternate_names_flags_and_id_filter() {
        let text = "1\t6167865\ten\tToronto\t1\t\t\t\t\t\n2\t6167865\tfr\tToronto\t\t\t\t1\t1800\t1834\n3\t42\tde\tElsewhere\t\t\t\t\t\t\n";
        let filter = Filter {
            geonameids: Some([6167865].into()),
            ..Default::default()
        };
        let p = run(&ALTERNATE_NAMES, text, &filter);
        assert_eq!((p.rows, p.filtered), (2, 1));
        let b = &p.batches[0];
        let pref = col::<BooleanArray>(b, "is_preferred_name");
        assert!(pref.value(0) && !pref.value(1));
        assert!(col::<BooleanArray>(b, "is_historic").value(1));
        assert_eq!(col::<StringArray>(b, "from_period").value(1), "1800");
    }

    #[test]
    fn feature_codes_split_class_and_code() {
        let text = "A.ADM1\tfirst-order administrative division\ta primary division\nnull\tnot available\t\n";
        let p = run(&FEATURE_CODES, text, &Filter::default());
        assert_eq!((p.rows, p.rejected), (1, 1));
        let b = &p.batches[0];
        assert_eq!(col::<StringArray>(b, "feature_class").value(0), "A");
        assert_eq!(col::<StringArray>(b, "feature_code").value(0), "ADM1");
    }

    #[test]
    fn country_info_skips_comments() {
        let text = "# GeoNames country info\n#ISO\tISO3\tISO-Numeric\n\
CA\tCAN\t124\tCA\tCanada\tOttawa\t9984670\t37058856\tNA\t.ca\tCAD\tDollar\t1\tA0A 0A0\t^x$\ten-CA,fr-CA,iu\t6251999\tUS\t\n";
        let p = run(&COUNTRIES, text, &Filter::default());
        assert_eq!(p.rows, 1);
        let b = &p.batches[0];
        assert_eq!(col::<StringArray>(b, "country").value(0), "Canada");
        assert_eq!(col::<Int64Array>(b, "geonameid").value(0), 6251999);
        assert!(col::<StringArray>(b, "equivalent_fips_code").is_null(0));
    }

    #[test]
    fn collects_geonameids() {
        let p = run(&PLACENAMES, PLACES, &Filter::default());
        assert_eq!(geonameids(&p.batches), [6167865, 3133880, 5].into());
    }
}
