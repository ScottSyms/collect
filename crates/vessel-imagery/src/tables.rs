//! The imagery tables, all in one namespace (`imagery` by default):
//!
//! | Table                   | Written by | How                                        |
//! |-------------------------|------------|--------------------------------------------|
//! | `vessel_images`         | fetch, import | appended (deduplicated on sha256)       |
//! | `image_observations`    | describe   | appended, by question-set version          |
//! | `vessel_visual_profile` | profile    | replaced (materialized from observations)  |
//!
//! Image bytes live outside the tables, under
//! `s3://<bucket>/raw_archive/imagery/{free|restricted}/<sha256>.<ext>`.

use anyhow::{bail, Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use collect_core::iceberg::{ensure_namespace, ensure_table, table_ident, IcebergConfig};
use collect_maint::commit::RestClient;
use collect_maint::rewrite::write_partition;
use iceberg::spec::{Literal, PartitionSpecBuilder, Schema, Struct, Transform};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::Catalog;
use vessel_identity::col;
use vessel_identity::columns::{schema, to_batch, Column};

pub const IMAGES: &str = "vessel_images";
pub const OBSERVATIONS: &str = "image_observations";
pub const PROFILE: &str = "vessel_visual_profile";

const TARGET_FILE_BYTES: u64 = 256 << 20;
const MAX_COMMIT_ATTEMPTS: usize = 4;

/// One photo of one vessel, with where it came from and what its licence
/// allows.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ImageRow {
    /// sha256 of the stored bytes.
    pub image_id: String,
    pub vessel_id: String,
    pub imo_number: Option<String>,
    pub mmsi: Option<String>,
    /// How the photo was tied to the vessel: `commons_imo_category`,
    /// `manifest_imo`, `manifest_mmsi`, `manifest_vessel_id`.
    pub match_basis: String,
    /// `wikimedia_commons`, or the manifest's source name.
    pub source: String,
    pub source_page_url: Option<String>,
    pub original_url: Option<String>,
    pub object_uri: String,
    pub mime: String,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub licence_short: Option<String>,
    pub licence_url: Option<String>,
    /// `public_domain`, `cc0`, `cc_by`, `cc_by_sa`, `other_free`,
    /// `restricted`, `unknown`.
    pub licence_class: String,
    pub restricted: bool,
    pub attribution_required: bool,
    pub author: Option<String>,
    pub credit: Option<String>,
    pub captured_at: Option<DateTime<Utc>>,
    pub retrieved_at: DateTime<Utc>,
}

pub const IMAGE_COLUMNS: &[Column<ImageRow>] = &[
    col!(image_id, Str, req),
    col!(vessel_id, Str, req),
    col!(imo_number, Str),
    col!(mmsi, Str),
    col!(match_basis, Str, req),
    col!(source, Str, req),
    col!(source_page_url, Str),
    col!(original_url, Str),
    col!(object_uri, Str, req),
    col!(mime, Str, req),
    col!(width, Int),
    col!(height, Int),
    col!(licence_short, Str),
    col!(licence_url, Str),
    col!(licence_class, Str, req),
    col!(restricted, Bool, req),
    col!(attribution_required, Bool, req),
    col!(author, Str),
    col!(credit, Str),
    col!(captured_at, Ts),
    col!(retrieved_at, Ts, req),
];

/// One model answer to one question about one image.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ObservationRow {
    pub image_id: String,
    pub vessel_id: String,
    pub model_id: String,
    pub question_set_version: String,
    pub question: String,
    /// The chosen label (`yes`/`no` for yes/no questions); the score as
    /// text for score questions.
    pub answer: String,
    pub probability: Option<f64>,
    /// The runner-up label and its probability, when the top choice was
    /// below the question set's confidence threshold.
    pub runner_up: Option<String>,
    pub runner_up_probability: Option<f64>,
    pub calibrated: bool,
    pub observed_at: DateTime<Utc>,
}

pub const OBSERVATION_COLUMNS: &[Column<ObservationRow>] = &[
    col!(image_id, Str, req),
    col!(vessel_id, Str, req),
    col!(model_id, Str, req),
    col!(question_set_version, Str, req),
    col!(question, Str, req),
    col!(answer, Str, req),
    col!(probability, Double),
    col!(runner_up, Str),
    col!(runner_up_probability, Double),
    col!(calibrated, Bool, req),
    col!(observed_at, Ts, req),
];

/// What a vessel looks like, as far as its photos agree.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProfileRow {
    pub vessel_id: String,
    pub imo_number: Option<String>,
    pub attribute: String,
    pub value: String,
    /// Images whose answer was `value`.
    pub support_images: i32,
    /// Images that passed the gate and answered the question at all.
    pub total_images: i32,
    pub mean_probability: f64,
    /// Another value has at least half `value`'s support.
    pub conflicting: bool,
    /// Every supporting image is restricted.
    pub restricted_only: bool,
    pub model_id: String,
    pub question_set_version: String,
    pub updated_at: DateTime<Utc>,
}

pub const PROFILE_COLUMNS: &[Column<ProfileRow>] = &[
    col!(vessel_id, Str, req),
    col!(imo_number, Str),
    col!(attribute, Str, req),
    col!(value, Str, req),
    col!(support_images, Int, req),
    col!(total_images, Int, req),
    col!(mean_probability, Double, req),
    col!(conflicting, Bool, req),
    col!(restricted_only, Bool, req),
    col!(model_id, Str, req),
    col!(question_set_version, Str, req),
    col!(updated_at, Ts, req),
];

pub struct TableDef {
    pub name: &'static str,
    pub schema: fn() -> Schema,
    /// Identity-partitioned on this string column, when set.
    pub partition: Option<&'static str>,
    pub bloom: &'static [&'static str],
}

pub const TABLES: &[TableDef] = &[
    TableDef { name: IMAGES, schema: || schema(IMAGE_COLUMNS), partition: None, bloom: &["image_id", "vessel_id", "imo_number"] },
    TableDef {
        name: OBSERVATIONS,
        schema: || schema(OBSERVATION_COLUMNS),
        partition: Some("question_set_version"),
        bloom: &["image_id", "vessel_id"],
    },
    TableDef { name: PROFILE, schema: || schema(PROFILE_COLUMNS), partition: None, bloom: &["vessel_id", "imo_number"] },
];

pub fn def(name: &str) -> &'static TableDef {
    TABLES.iter().find(|t| t.name == name).expect("known table")
}

fn spec(d: &TableDef) -> Result<PartitionSpecBuilder> {
    let b = PartitionSpecBuilder::new((d.schema)());
    Ok(match d.partition {
        None => b,
        Some(c) => b.add_partition_field(c, c, Transform::Identity)?,
    })
}

pub fn string_partition(value: &str) -> Struct {
    Struct::from_iter([Some(Literal::string(value))])
}

/// The imagery tables in one catalog and namespace.
pub struct Store<C> {
    pub catalog: C,
    pub rest: RestClient,
    pub config: IcebergConfig,
}

impl<C: Catalog> Store<C> {
    /// Creates the namespace and every table that's missing.
    pub async fn init(&self) -> Result<Vec<&'static str>> {
        ensure_namespace(&self.catalog, &self.config).await?;
        let mut created = Vec::new();
        for d in TABLES {
            if self.catalog.table_exists(&table_ident(&self.config, d.name)).await.unwrap_or(false) {
                continue;
            }
            ensure_table(&self.catalog, &self.config, d.name, (d.schema)(), spec(d)?).await?;
            created.push(d.name);
        }
        Ok(created)
    }

    pub async fn table(&self, name: &str) -> Result<Table> {
        let d = def(name);
        ensure_namespace(&self.catalog, &self.config).await?;
        if let Ok(t) = self.catalog.load_table(&table_ident(&self.config, name)).await {
            return Ok(t);
        }
        ensure_table(&self.catalog, &self.config, d.name, (d.schema)(), spec(d)?).await
    }

    pub async fn existing(&self, name: &str) -> Result<Option<Table>> {
        let ident = table_ident(&self.config, name);
        if !self.catalog.table_exists(&ident).await? {
            return Ok(None);
        }
        Ok(Some(self.catalog.load_table(&ident).await?))
    }

    /// All rows of `name`, or none when the table doesn't exist yet.
    pub async fn read(&self, name: &str) -> Result<Vec<RecordBatch>> {
        let Some(table) = self.existing(name).await? else {
            return Ok(Vec::new());
        };
        ais_tracks::state::scan_all(&table, None).await.with_context(|| format!("reading {name}"))
    }

    /// Appends `rows` in one snapshot; a partitioned table needs `partition`.
    pub async fn append<T>(&self, name: &str, cols: &[Column<T>], rows: &[T], partition: Option<Struct>) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let d = def(name);
        let batch = to_batch(cols, rows)?;
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            let table = self.table(name).await?;
            let files = write_partition(&table, partition.clone(), batch.clone(), TARGET_FILE_BYTES, d.bloom).await?;
            let txn = Transaction::new(&table);
            let txn = txn.fast_append().add_data_files(files.clone()).apply(txn)?;
            match txn.commit(&self.catalog).await {
                Ok(_) => return Ok(rows.len()),
                Err(e) => {
                    for f in &files {
                        let _ = table.file_io().delete(f.file_path()).await;
                    }
                    if attempt == MAX_COMMIT_ATTEMPTS {
                        return Err(e).with_context(|| format!("appending to {name}"));
                    }
                    eprintln!("  {name}: append failed ({e}), retrying ({attempt}/{MAX_COMMIT_ATTEMPTS})");
                }
            }
        }
        unreachable!()
    }

    /// Replaces the whole of an unpartitioned table; refuses to empty it.
    pub async fn replace<T>(&self, name: &str, cols: &[Column<T>], rows: &[T]) -> Result<()> {
        let d = def(name);
        debug_assert!(d.partition.is_none());
        if rows.is_empty() {
            bail!("{name}: computed 0 rows; leaving the table untouched");
        }
        let batch = to_batch(cols, rows)?;
        self.table(name).await?;
        ais_tracks::output::replace_table(&self.catalog, &self.rest, &self.config, name, (d.schema)(), &[batch], d.bloom)
            .await
            .with_context(|| format!("replacing {name}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemas_build_and_partition() {
        for d in TABLES {
            spec(d).unwrap().build().unwrap();
        }
    }

    #[test]
    fn rows_make_batches() {
        let now = Utc::now();
        let img = ImageRow { image_id: "a".into(), vessel_id: "v".into(), match_basis: "m".into(), source: "s".into(), object_uri: "u".into(), mime: "image/jpeg".into(), licence_class: "cc_by".into(), retrieved_at: now, ..Default::default() };
        let b = to_batch(IMAGE_COLUMNS, &[img]).unwrap();
        ais_tracks::carry::check_batches(&schema(IMAGE_COLUMNS), std::slice::from_ref(&b)).unwrap();
        let obs = ObservationRow { observed_at: now, ..Default::default() };
        to_batch(OBSERVATION_COLUMNS, &[obs]).unwrap();
        let p = ProfileRow { updated_at: now, ..Default::default() };
        to_batch(PROFILE_COLUMNS, &[p]).unwrap();
    }
}
