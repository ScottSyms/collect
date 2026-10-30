//! Column lists that drive both a table's Iceberg schema and its Arrow
//! batches, so the two can't drift apart, plus typed readers for batches
//! read back from Iceberg.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray, ListBuilder,
    StringArray, StringBuilder, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema as ArrowSchema, TimeUnit, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use iceberg::spec::{ListType, NestedField, PrimitiveType, Schema, Type};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    Str,
    Int,
    Long,
    Double,
    Bool,
    Ts,
    StrList,
}

pub enum Val<'a> {
    Str(Option<&'a str>),
    Int(Option<i32>),
    Long(Option<i64>),
    Double(Option<f64>),
    Bool(Option<bool>),
    Ts(Option<DateTime<Utc>>),
    StrList(&'a [String]),
}

pub struct Column<T> {
    pub name: &'static str,
    pub kind: Kind,
    pub required: bool,
    pub get: for<'a> fn(&'a T) -> Val<'a>,
}

/// `col!(field, Kind)` for an optional column, `col!(field, Kind, req)` for a
/// required one; the struct field has the column's name. Exported so other
/// crates (vessel-imagery) can declare tables the same way.
#[macro_export]
macro_rules! col {
    ($name:ident, Str, req) => {
        $crate::columns::Column { name: stringify!($name), kind: $crate::columns::Kind::Str, required: true, get: |r| $crate::columns::Val::Str(Some(r.$name.as_str())) }
    };
    ($name:ident, Str) => {
        $crate::columns::Column { name: stringify!($name), kind: $crate::columns::Kind::Str, required: false, get: |r| $crate::columns::Val::Str(r.$name.as_deref()) }
    };
    ($name:ident, StrList) => {
        $crate::columns::Column { name: stringify!($name), kind: $crate::columns::Kind::StrList, required: false, get: |r| $crate::columns::Val::StrList(&r.$name) }
    };
    ($name:ident, $kind:ident, req) => {
        $crate::columns::Column { name: stringify!($name), kind: $crate::columns::Kind::$kind, required: true, get: |r| $crate::columns::Val::$kind(Some(r.$name)) }
    };
    ($name:ident, $kind:ident) => {
        $crate::columns::Column { name: stringify!($name), kind: $crate::columns::Kind::$kind, required: false, get: |r| $crate::columns::Val::$kind(r.$name) }
    };
}
pub use crate::col;

/// Field ids for list elements start here, clear of the column ids.
const LIST_ELEMENT_IDS: i32 = 1000;

fn primitive(kind: Kind) -> PrimitiveType {
    match kind {
        Kind::Str | Kind::StrList => PrimitiveType::String,
        Kind::Int => PrimitiveType::Int,
        Kind::Long => PrimitiveType::Long,
        Kind::Double => PrimitiveType::Double,
        Kind::Bool => PrimitiveType::Boolean,
        Kind::Ts => PrimitiveType::Timestamptz,
    }
}

fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
}

fn arrow_type(kind: Kind) -> DataType {
    match kind {
        Kind::Str => DataType::Utf8,
        Kind::Int => DataType::Int32,
        Kind::Long => DataType::Int64,
        Kind::Double => DataType::Float64,
        Kind::Bool => DataType::Boolean,
        Kind::Ts => ts_type(),
        Kind::StrList => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
    }
}

pub fn schema<T>(cols: &[Column<T>]) -> Schema {
    let fields: Vec<Arc<NestedField>> = cols
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let id = i as i32 + 1;
            let ty: Type = match c.kind {
                Kind::StrList => Type::List(ListType {
                    element_field: Arc::new(NestedField::list_element(
                        LIST_ELEMENT_IDS + id,
                        PrimitiveType::String.into(),
                        false,
                    )),
                }),
                k => primitive(k).into(),
            };
            Arc::new(if c.required { NestedField::required(id, c.name, ty) } else { NestedField::optional(id, c.name, ty) })
        })
        .collect();
    Schema::builder().with_schema_id(1).with_fields(fields).build().expect("building table schema")
}

/// `rows` as one batch with `schema(cols)`'s columns.
pub fn to_batch<T>(cols: &[Column<T>], rows: &[T]) -> Result<RecordBatch> {
    let fields: Vec<Field> = cols.iter().map(|c| Field::new(c.name, arrow_type(c.kind), !c.required)).collect();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for c in cols {
        macro_rules! gather {
            ($variant:ident) => {
                rows.iter().map(|r| match (c.get)(r) {
                    Val::$variant(v) => v,
                    _ => panic!("column {} is not {:?}", c.name, c.kind),
                })
            };
        }
        let array: ArrayRef = match c.kind {
            Kind::Str => Arc::new(gather!(Str).collect::<StringArray>()),
            Kind::Int => Arc::new(gather!(Int).collect::<Int32Array>()),
            Kind::Long => Arc::new(gather!(Long).collect::<Int64Array>()),
            Kind::Double => Arc::new(gather!(Double).collect::<Float64Array>()),
            Kind::Bool => Arc::new(gather!(Bool).collect::<BooleanArray>()),
            Kind::Ts => Arc::new(
                gather!(Ts)
                    .map(|t| t.map(|t| t.timestamp_micros()))
                    .collect::<TimestampMicrosecondArray>()
                    .with_timezone("+00:00"),
            ),
            Kind::StrList => {
                let mut b = ListBuilder::new(StringBuilder::new());
                for list in gather!(StrList) {
                    for s in list {
                        b.values().append_value(s);
                    }
                    b.append(true);
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(array);
    }
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays).context("building batch")
}

// ---------------------------------------------------------------------------
// Reading batches back.

/// Typed access to one batch's columns by name.
pub struct Reader<'a> {
    batch: &'a RecordBatch,
}

impl<'a> Reader<'a> {
    pub fn new(batch: &'a RecordBatch) -> Self {
        Reader { batch }
    }

    pub fn rows(&self) -> usize {
        self.batch.num_rows()
    }

    fn col(&self, name: &str) -> Result<&'a ArrayRef> {
        self.batch.column_by_name(name).with_context(|| format!("no column {name}"))
    }

    pub fn str(&self, name: &str) -> Result<impl Fn(usize) -> Option<String> + 'a> {
        let c = self.col(name)?;
        let a = arrow::compute::cast(c, &DataType::Utf8)?;
        Ok(move |i: usize| {
            let s = a.as_string::<i32>();
            (!s.is_null(i)).then(|| s.value(i).to_string())
        })
    }

    pub fn ts(&self, name: &str) -> Result<impl Fn(usize) -> Option<DateTime<Utc>> + 'a> {
        let c = self.col(name)?.clone();
        Ok(move |i: usize| {
            let a = c.as_primitive::<TimestampMicrosecondType>();
            (!a.is_null(i)).then(|| DateTime::from_timestamp_micros(a.value(i)).expect("valid timestamp"))
        })
    }

    pub fn int(&self, name: &str) -> Result<impl Fn(usize) -> Option<i32> + 'a> {
        let c = self.col(name)?.clone();
        Ok(move |i: usize| {
            let a = c.as_primitive::<Int32Type>();
            (!a.is_null(i)).then(|| a.value(i))
        })
    }

    pub fn long(&self, name: &str) -> Result<impl Fn(usize) -> Option<i64> + 'a> {
        let c = self.col(name)?.clone();
        Ok(move |i: usize| {
            let a = c.as_primitive::<Int64Type>();
            (!a.is_null(i)).then(|| a.value(i))
        })
    }

    pub fn list(&self, name: &str) -> Result<impl Fn(usize) -> Vec<String> + 'a> {
        let c = self.col(name)?.clone();
        Ok(move |i: usize| {
            let a: &ListArray = c.as_list::<i32>();
            if a.is_null(i) {
                return Vec::new();
            }
            let v = a.value(i);
            let s = v.as_string::<i32>();
            (0..s.len()).filter(|&j| !s.is_null(j)).map(|j| s.value(j).to_string()).collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Row {
        id: String,
        note: Option<String>,
        n: Option<i32>,
        at: Option<DateTime<Utc>>,
        tags: Vec<String>,
    }

    const COLS: &[Column<Row>] = &[col!(id, Str, req), col!(note, Str), col!(n, Int), col!(at, Ts), col!(tags, StrList)];

    #[test]
    fn round_trips_through_a_batch() {
        let now = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let rows = vec![
            Row { id: "a".into(), note: None, n: Some(3), at: Some(now), tags: vec!["x".into(), "y".into()] },
            Row { id: "b".into(), note: Some("hi".into()), n: None, at: None, tags: vec![] },
        ];
        let b = to_batch(COLS, &rows).unwrap();
        ais_tracks::carry::check_batches(&schema(COLS), std::slice::from_ref(&b)).unwrap();
        let r = Reader::new(&b);
        assert_eq!(r.str("id").unwrap()(1).as_deref(), Some("b"));
        assert_eq!(r.str("note").unwrap()(0), None);
        assert_eq!(r.int("n").unwrap()(0), Some(3));
        assert_eq!(r.ts("at").unwrap()(0), Some(now));
        assert_eq!(r.list("tags").unwrap()(0), ["x", "y"]);
        assert!(r.list("tags").unwrap()(1).is_empty());
    }
}
