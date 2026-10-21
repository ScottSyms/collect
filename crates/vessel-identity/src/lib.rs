//! Maritime vessel identity registry: collects public vessel identity data
//! (national registers, classification societies, port state systems) into
//! Apache Iceberg, keeps every source's claims with their provenance and
//! history, and reconciles them into one record per physical vessel with a
//! permanent id. See `VESSEL_IDENTITY.md` at the repository root.

pub mod assertions;
pub mod columns;
pub mod config;
pub mod countries;
pub mod fetch;
pub mod ingest;
pub mod load;
pub mod normalize;
pub mod reconcile;
pub mod records;
pub mod registry;
pub mod sources;
pub mod tables;
