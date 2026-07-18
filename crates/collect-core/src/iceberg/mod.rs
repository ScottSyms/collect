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

    // Pass S3 configuration from env vars to the storage factory
    for (key, var) in [
        ("s3.endpoint", "S3_ENDPOINT"),
        ("s3.access-key-id", "S3_ACCESS_KEY"),
        ("s3.secret-access-key", "S3_SECRET_KEY"),
        ("s3.region", "S3_REGION"),
    ] {
        if let Ok(val) = std::env::var(var) {
            props.insert(key.to_string(), val);
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

pub const ALL_TABLES: &[&str] = &[
    TABLE_POSITIONS,
    TABLE_STATICS,
    TABLE_METEO,
    TABLE_BINARY,
    TABLE_ATONS,
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
            builder = builder.add_partition_field("ts", "year", Transform::Year)?;
        }
        "month" => {
            builder = builder.add_partition_field("ts", "month", Transform::Month)?;
        }
        "day" => {
            builder = builder.add_partition_field("ts", "day", Transform::Day)?;
        }
        "hour" | "minute" => {
            builder = builder.add_partition_field("ts", "hour", Transform::Hour)?;
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
