use anyhow::{Context, Result};
use clap::Args;
use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::{PartitionSpecBuilder, Schema, Transform};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

pub mod table_schemas;

/// CLI args for Iceberg REST catalog output.
#[derive(Clone, Debug, Args)]
pub struct IcebergCliArgs {
    /// Iceberg REST catalog URI (e.g. http://lakekeeper:8181/catalog).
    #[arg(long, env = "ICEBERG_CATALOG_URI")]
    pub iceberg_catalog_uri: Option<String>,

    /// Iceberg warehouse location (e.g. s3://my-bucket/warehouse).
    #[arg(long, env = "ICEBERG_WAREHOUSE")]
    pub iceberg_warehouse: Option<String>,

    /// Iceberg namespace (database) to write into.
    #[arg(long, env = "ICEBERG_NAMESPACE", default_value = "ais")]
    pub iceberg_namespace: String,

    /// Optional prefix added to each table name (e.g. "ais" -> "ais_positions").
    #[arg(long, env = "ICEBERG_TABLE_PREFIX")]
    pub iceberg_table_prefix: Option<String>,

    /// Bearer token for Lakekeeper / REST catalog authentication.
    #[arg(long, env = "ICEBERG_TOKEN")]
    pub iceberg_token: Option<String>,
}

impl IcebergCliArgs {
    pub fn validate(&self) -> Result<()> {
        if self.iceberg_catalog_uri.is_some() {
            anyhow::ensure!(
                self.iceberg_warehouse.is_some(),
                "--iceberg-warehouse is required when using Iceberg output"
            );
        }
        Ok(())
    }

    pub fn is_iceberg_mode(&self) -> bool {
        self.iceberg_catalog_uri.is_some()
    }
}

/// Resolved Iceberg configuration.
pub struct IcebergConfig {
    pub catalog_uri: String,
    pub warehouse: String,
    pub namespace: String,
    pub table_prefix: Option<String>,
    pub token: Option<String>,
}

impl From<&IcebergCliArgs> for IcebergConfig {
    fn from(args: &IcebergCliArgs) -> Self {
        IcebergConfig {
            catalog_uri: args.iceberg_catalog_uri.clone().unwrap_or_default(),
            warehouse: args.iceberg_warehouse.clone().unwrap_or_default(),
            namespace: args.iceberg_namespace.clone(),
            table_prefix: args.iceberg_table_prefix.clone(),
            token: args.iceberg_token.clone(),
        }
    }
}

pub async fn open_catalog(config: &IcebergConfig) -> Result<impl Catalog> {
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(
        iceberg_catalog_rest::REST_CATALOG_PROP_URI.to_string(),
        config.catalog_uri.clone(),
    );
    props.insert(
        iceberg_catalog_rest::REST_CATALOG_PROP_WAREHOUSE.to_string(),
        config.warehouse.clone(),
    );

    if let Some(token) = &config.token {
        props.insert("token".to_string(), token.clone());
    }

    // Pass S3 configuration from env vars to the storage factory.
    // Checks both project-specific and AWS-standard env var names.
    for (key, vars) in [
        ("s3.endpoint", &["S3_ENDPOINT"] as &[_]),
        ("s3.access-key-id", &["S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"]),
        ("s3.secret-access-key", &["S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"]),
        ("s3.region", &["S3_REGION"]),
        ("s3.path-style-access", &["S3_PATH_STYLE"]),
        ("s3.disable-ec2-metadata", &["S3_DISABLE_EC2_METADATA"]),
        ("s3.disable-config-load", &["S3_DISABLE_CONFIG_LOAD"]),
    ] {
        for var in vars {
            if let Ok(val) = std::env::var(var) {
                props.insert(key.to_string(), val);
                break;
            }
        }
    }

    let factory = Arc::new(
        iceberg_storage_opendal::OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        },
    );

    let catalog = iceberg_catalog_rest::RestCatalogBuilder::default()
        .with_storage_factory(factory)
        .load("rest", props)
        .await
        .context("Failed to connect to Iceberg REST catalog")?;

    Ok(catalog)
}

pub const TABLE_POSITIONS: &str = "positions";
pub const TABLE_STATICS: &str = "statics";
pub const TABLE_METEO: &str = "meteo";
pub const TABLE_BINARY: &str = "binary";
pub const TABLE_ATONS: &str = "atons";
pub const TABLE_OTHER: &str = "other";
/// Table collectors register bronze uploads into directly (see
/// `register_raw_upload`), independent of `collect-orchestrator`'s six
/// decoded tables above.
pub const TABLE_RAW: &str = "raw";

pub const ALL_TABLES: &[&str] = &[
    TABLE_POSITIONS,
    TABLE_STATICS,
    TABLE_METEO,
    TABLE_BINARY,
    TABLE_ATONS,
    TABLE_OTHER,
];

fn table_name(prefix: Option<&str>, base: &str) -> String {
    match prefix {
        Some(p) if !p.is_empty() => format!("{}_{}", p, base),
        _ => base.to_string(),
    }
}

pub fn table_ident(config: &IcebergConfig, base: &str) -> TableIdent {
    let name = table_name(config.table_prefix.as_deref(), base);
    TableIdent::new(NamespaceIdent::new(config.namespace.clone()), name)
}

/// Partition spec for a timestamp column at the given granularity.
/// Iceberg supports year/month/day/hour — minute is NOT supported.
pub fn partition_spec_for(schema: &Schema, granularity: &str) -> Result<PartitionSpecBuilder> {
    let has_ts = schema
        .as_struct()
        .fields()
        .iter()
        .any(|f| f.name == "ts");
    anyhow::ensure!(has_ts, "schema must have a 'ts' timestamp field");

    let mut builder = PartitionSpecBuilder::new(schema.clone());

    match granularity {
        "year" => {
            builder = builder.add_partition_field("ts", "ts_year", Transform::Year)?;
        }
        "month" => {
            builder = builder.add_partition_field("ts", "ts_month", Transform::Month)?;
        }
        "day" => {
            builder = builder.add_partition_field("ts", "ts_day", Transform::Day)?;
        }
        "hour" | "minute" => {
            builder = builder.add_partition_field("ts", "ts_hour", Transform::Hour)?;
        }
        _ => anyhow::bail!("unsupported partition granularity: {granularity}"),
    }

    Ok(builder)
}

pub async fn ensure_namespace(catalog: &impl Catalog, config: &IcebergConfig) -> Result<()> {
    let ns = NamespaceIdent::new(config.namespace.clone());
    match catalog.create_namespace(&ns, HashMap::new()).await {
        Ok(_) => {
            eprintln!("Created Iceberg namespace '{}'", config.namespace);
            Ok(())
        }
        Err(err) if is_namespace_exists_error(&err) => Ok(()),
        Err(err) => Err(err).context("creating Iceberg namespace"),
    }
}

fn is_namespace_exists_error(err: &iceberg::Error) -> bool {
    let msg = err.to_string();
    msg.contains("already exists")
        || msg.contains("NamespaceAlreadyExists")
        || msg.contains("409")
}

fn is_table_exists_error(err: &iceberg::Error) -> bool {
    let msg = err.to_string();
    msg.contains("already exists")
        || msg.contains("TableAlreadyExists")
        || msg.contains("409")
}

pub async fn ensure_table(
    catalog: &impl Catalog,
    config: &IcebergConfig,
    base_name: &str,
    iceberg_schema: Schema,
    partition_spec: PartitionSpecBuilder,
) -> Result<iceberg::table::Table> {
    let ident = table_ident(config, base_name);
    let bound_spec = partition_spec.build().context("building partition spec")?;

    let creation = TableCreation::builder()
        .name(ident.name().to_string())
        .schema(iceberg_schema)
        .partition_spec(bound_spec)
        .build();

    match catalog.create_table(ident.namespace(), creation).await {
        Ok(table) => {
            eprintln!(
                "Created Iceberg table '{}.{}'",
                config.namespace,
                base_name
            );
            return Ok(table);
        }
        Err(err) if is_table_exists_error(&err) => {}
        Err(err) => return Err(err).context("creating Iceberg table"),
    }

    let table = catalog
        .load_table(&ident)
        .await
        .context("loading existing Iceberg table")?;
    eprintln!(
        "Using existing Iceberg table '{}.{}'",
        config.namespace,
        base_name
    );
    Ok(table)
}

pub async fn commit_batches(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    batches: Vec<arrow::record_batch::RecordBatch>,
    compression_level: i32,
    table_name: &str,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    use chrono::{Datelike, Timelike, TimeZone};
    use iceberg::spec::{DataFileFormat, PartitionKey};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let metadata = table.metadata();
    let iceberg_schema = metadata.current_schema();
    let location_gen = DefaultLocationGenerator::new(metadata.clone())?;
    let file_name_gen = DefaultFileNameGenerator::new(
        "part".to_string(),
        Some("iceberg".to_string()),
        DataFileFormat::Parquet,
    );
    let level = ZstdLevel::try_new(compression_level).context("invalid zstd")?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(level))
        .build();
    let writer_builder = ParquetWriterBuilder::new(props, iceberg_schema.clone());
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        writer_builder,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );
    let builder = DataFileWriterBuilder::new(rolling);

    let pk = {
        let spec = metadata.default_partition_spec();
        if spec.fields().is_empty() {
            None
        } else {
            let first = &batches[0];
            let ts_col = first
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .context("ts col")?;
            let first_ts = ts_col.value(0);
            let dt = chrono::Utc
                .timestamp_millis_opt(first_ts)
                .single()
                .context("invalid ts")?;
            let epoch_days = (first_ts / 86400000) as i32;
            let mut vals: Vec<i32> = Vec::new();
            for f in spec.fields() {
                match f.transform {
                    iceberg::spec::Transform::Year => vals.push(dt.year()),
                    iceberg::spec::Transform::Month => {
                        vals.push((dt.year() - 1970) * 12 + dt.month() as i32 - 1)
                    }
                    iceberg::spec::Transform::Day => vals.push(epoch_days),
                    iceberg::spec::Transform::Hour => {
                        vals.push(epoch_days * 24 + dt.hour() as i32)
                    }
                    _ => {}
                }
            }
            let data = iceberg::spec::Struct::from_iter(
                vals.into_iter()
                    .map(|v| Some(iceberg::spec::Literal::int(v))),
            );
            Some(PartitionKey::new(
                spec.as_ref().clone(),
                metadata.current_schema().clone(),
                data,
            ))
        }
    };

    let mut writer = builder.build(pk).await.context("build writer")?;
    let target_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(iceberg_schema).context("arrow schema")?,
    );
    for batch in &batches {
        let projected = {
            let cols: Result<Vec<_>, _> = target_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let col = batch.column(i);
                    if col.data_type() == f.data_type() {
                        Ok(col.clone())
                    } else {
                        arrow::compute::cast(col, f.data_type()).context("cast")
                    }
                })
                .collect();
            arrow::record_batch::RecordBatch::try_new(target_schema.clone(), cols?)?
        };
        writer.write(projected).await.context("write")?;
    }
    let files = writer.close().await.context("close")?;
    if files.is_empty() {
        return Ok(());
    }
    let txn = Transaction::new(table);
    let txn = txn.fast_append().add_data_files(files).apply(txn)?;
    txn.commit(catalog).await.context("commit")?;
    eprintln!(
        "  committed {} to {}",
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        table_name
    );
    Ok(())
}

/// Resolved Iceberg catalog + table handle a collector holds for the
/// lifetime of the process, used to register each successfully-uploaded
/// bronze Parquet file as a row in the `raw` table.
#[derive(Clone, Debug)]
pub struct IcebergHandle {
    pub catalog: Arc<dyn Catalog>,
    pub table: iceberg::table::Table,
    pub compression_level: i32,
}

/// Build the three-column `[ts, source, payload]` batch registered into
/// `raw`, from the two-column `[ts, payload]` batch written to the bronze
/// Parquet file. Column order matches `raw_schema()`'s field order. Pure and
/// network-free so it's unit-testable on its own.
fn build_raw_batch(
    batch: &arrow::record_batch::RecordBatch,
    source: &str,
) -> Result<arrow::record_batch::RecordBatch> {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    anyhow::ensure!(
        batch.num_columns() >= 2,
        "expected a [ts, payload] batch, got {} columns",
        batch.num_columns()
    );
    let num_rows = batch.num_rows();
    let ts_col = batch.column(0).clone();
    let payload_col = batch.column(1).clone();
    let source_col: Arc<dyn arrow::array::Array> =
        Arc::new(StringArray::from(vec![source; num_rows]));

    let raw_arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("source", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    arrow::record_batch::RecordBatch::try_new(
        raw_arrow_schema,
        vec![ts_col, source_col, payload_col],
    )
    .context("building raw registration batch")
}

/// Register one successfully-uploaded bronze batch into the `raw` table.
///
/// `batch` is the same two-column `[ts, payload]` `RecordBatch` written to
/// the bronze Parquet file (safe to pass a clone — this does not consume or
/// affect the bronze write path). A `source` column is added so `raw` stays
/// queryable across sources without inspecting each file's S3 key.
pub async fn register_raw_upload(
    handle: &IcebergHandle,
    batch: arrow::record_batch::RecordBatch,
    source: &str,
) -> Result<()> {
    let raw_batch = build_raw_batch(&batch, source)?;

    commit_batches(
        handle.catalog.as_ref(),
        &handle.table,
        vec![raw_batch],
        handle.compression_level,
        TABLE_RAW,
    )
    .await
}

/// Validate `args`, and — if `--iceberg-catalog-uri` is set — connect to the
/// catalog and ensure the `raw` table exists, returning a handle ready to
/// hand to `IngestOptions::iceberg`. Returns `None` when Iceberg output
/// isn't configured (the collector's default, fully-inert state).
pub async fn init_raw_handle(
    args: &IcebergCliArgs,
    partition_granularity: &str,
    compression_level: i32,
) -> Result<Option<IcebergHandle>> {
    args.validate()?;
    if !args.is_iceberg_mode() {
        return Ok(None);
    }

    let config = IcebergConfig::from(args);
    let catalog = open_catalog(&config).await?;
    ensure_namespace(&catalog, &config).await?;

    let schema = table_schemas::raw_schema();
    let partition_spec = partition_spec_for(&schema, partition_granularity)?;
    let table = ensure_table(&catalog, &config, TABLE_RAW, schema, partition_spec).await?;

    Ok(Some(IcebergHandle {
        catalog: Arc::new(catalog),
        table,
        compression_level,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    #[test]
    fn raw_schema_has_expected_fields_in_order() {
        let schema = table_schemas::raw_schema();
        let fields = schema.as_struct().fields();
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["ts", "source", "payload"]);
        assert_eq!(fields[0].id, 1);
        assert_eq!(fields[1].id, 2);
        assert_eq!(fields[2].id, 3);
    }

    #[test]
    fn raw_schema_supports_day_partitioning() {
        let schema = table_schemas::raw_schema();
        let spec = partition_spec_for(&schema, "day").expect("day partitioning");
        let built = spec.build().expect("build spec");
        assert_eq!(built.fields().len(), 1);
    }

    fn bronze_batch(rows: &[(i64, &str)]) -> arrow::record_batch::RecordBatch {
        let bronze_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let ts = TimestampMillisecondArray::from(rows.iter().map(|(ts, _)| *ts).collect::<Vec<_>>())
            .with_timezone_opt(Some(Arc::from("UTC")));
        let payload = StringArray::from(rows.iter().map(|(_, p)| *p).collect::<Vec<_>>());
        arrow::record_batch::RecordBatch::try_new(bronze_schema, vec![Arc::new(ts), Arc::new(payload)])
            .expect("build bronze batch")
    }

    #[test]
    fn build_raw_batch_adds_source_column_in_schema_order() {
        let batch = bronze_batch(&[
            (1_700_000_000_000, "!AIVDM,1,1"),
            (1_700_000_001_000, "!AIVDM,1,2"),
        ]);

        let raw = build_raw_batch(&batch, "norway").expect("build raw batch");

        assert_eq!(raw.num_columns(), 3);
        assert_eq!(raw.num_rows(), 2);
        let raw_schema = raw.schema();
        let names: Vec<&str> = raw_schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["ts", "source", "payload"]);

        let source_col = raw
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("source column is Utf8");
        assert_eq!(source_col.value(0), "norway");
        assert_eq!(source_col.value(1), "norway");

        let payload_col = raw
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload column is Utf8");
        assert_eq!(payload_col.value(0), "!AIVDM,1,1");
        assert_eq!(payload_col.value(1), "!AIVDM,1,2");

        let ts_col = raw
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("ts column stays a TimestampMillisecondArray");
        assert_eq!(ts_col.value(0), 1_700_000_000_000);
    }

    #[test]
    fn build_raw_batch_rejects_batches_with_too_few_columns() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        )]));
        let ts = TimestampMillisecondArray::from(vec![1_700_000_000_000i64])
            .with_timezone_opt(Some(Arc::from("UTC")));
        let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(ts)])
            .expect("build single-column batch");

        assert!(build_raw_batch(&batch, "norway").is_err());
    }
}
