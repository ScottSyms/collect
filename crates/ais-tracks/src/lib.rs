//! Derived, query-ready Iceberg tables built from the silver layer
//! (`positions`, `statics`) with DataFusion SQL: vessel identity today,
//! tracks and voyages next. Derived tables annotate and never drop rows.

pub mod output;
pub mod vessels;
pub mod track_points;
pub mod tracks;
pub mod carry;
pub mod ports;
pub mod stops;
pub mod voyages;
pub mod params;
pub mod reduce;
pub mod router;
pub mod source;
pub mod reduce_day;
pub mod state;
pub mod daily;
pub mod legs;
