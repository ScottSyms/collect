//! Maintenance for Iceberg tables served by catalogs (such as RustFS's) that
//! ship no compactor. iceberg-rust 0.9.1 has no rewrite/replace action, so
//! [`commit`] builds the `replace` snapshot itself and posts it to the REST
//! catalog.

pub mod commit;
pub mod compact;
pub mod orphans;
pub mod plan;
pub mod rewrite;
