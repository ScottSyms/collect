use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema};

fn required(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::required(id, name, ty.into()))
}

fn optional(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::optional(id, name, ty.into()))
}

pub fn positions_schema() -> Schema {
    let fields: Vec<Arc<NestedField>> = vec![
        required(1, "ts", PrimitiveType::Timestamptz),
        required(2, "source", PrimitiveType::String),
        required(3, "msg_type", PrimitiveType::Int),
        required(4, "mmsi", PrimitiveType::Long),
        optional(5, "ais_class", PrimitiveType::String),
        optional(6, "latitude", PrimitiveType::Double),
        optional(7, "longitude", PrimitiveType::Double),
        optional(8, "sog_knots", PrimitiveType::Double),
        optional(9, "cog", PrimitiveType::Double),
        optional(10, "heading_true", PrimitiveType::Double),
        optional(11, "rot", PrimitiveType::Double),
        optional(12, "altitude_m", PrimitiveType::Double),
        optional(13, "h3", PrimitiveType::Long),
        optional(14, "hilbert", PrimitiveType::Long),
        optional(15, "nav_status", PrimitiveType::String),
        optional(16, "high_accuracy", PrimitiveType::Boolean),
        optional(17, "raim", PrimitiveType::Boolean),
        optional(18, "special_manoeuvre", PrimitiveType::Boolean),
        optional(19, "station", PrimitiveType::String),
        optional(20, "payload", PrimitiveType::String),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building positions schema")
}

pub fn statics_schema() -> Schema {
    let fields: Vec<Arc<NestedField>> = vec![
        required(1, "ts", PrimitiveType::Timestamptz),
        required(2, "source", PrimitiveType::String),
        required(3, "msg_type", PrimitiveType::Int),
        required(4, "mmsi", PrimitiveType::Long),
        optional(5, "ais_class", PrimitiveType::String),
        optional(6, "imo_number", PrimitiveType::Int),
        optional(7, "call_sign", PrimitiveType::String),
        optional(8, "name", PrimitiveType::String),
        optional(9, "ship_type", PrimitiveType::String),
        optional(10, "dimension_to_bow", PrimitiveType::Int),
        optional(11, "dimension_to_stern", PrimitiveType::Int),
        optional(12, "dimension_to_port", PrimitiveType::Int),
        optional(13, "dimension_to_starboard", PrimitiveType::Int),
        optional(14, "draught_m", PrimitiveType::Double),
        optional(15, "destination", PrimitiveType::String),
        optional(16, "eta", PrimitiveType::Timestamptz),
        optional(17, "mothership_mmsi", PrimitiveType::Int),
        optional(18, "station", PrimitiveType::String),
        optional(19, "payload", PrimitiveType::String),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building statics schema")
}

pub fn meteo_schema() -> Schema {
    let fields: Vec<Arc<NestedField>> = vec![
        required(1, "ts", PrimitiveType::Timestamptz),
        required(2, "source", PrimitiveType::String),
        required(3, "msg_type", PrimitiveType::Int),
        required(4, "mmsi", PrimitiveType::Long),
        optional(5, "dac", PrimitiveType::Int),
        optional(6, "fid", PrimitiveType::Int),
        optional(7, "latitude", PrimitiveType::Double),
        optional(8, "longitude", PrimitiveType::Double),
        optional(9, "hilbert", PrimitiveType::Long),
        optional(10, "position_accuracy", PrimitiveType::Boolean),
        optional(11, "day", PrimitiveType::Int),
        optional(12, "hour", PrimitiveType::Int),
        optional(13, "minute", PrimitiveType::Int),
        optional(14, "wind_speed_kn", PrimitiveType::Int),
        optional(15, "wind_gust_kn", PrimitiveType::Int),
        optional(16, "wind_dir_deg", PrimitiveType::Int),
        optional(17, "wind_gust_dir_deg", PrimitiveType::Int),
        optional(18, "air_temp_c", PrimitiveType::Double),
        optional(19, "humidity_pct", PrimitiveType::Int),
        optional(20, "dew_point_c", PrimitiveType::Double),
        optional(21, "pressure_hpa", PrimitiveType::Int),
        optional(22, "pressure_tendency", PrimitiveType::Int),
        optional(23, "visibility_nm", PrimitiveType::Double),
        optional(24, "visibility_greater", PrimitiveType::Boolean),
        optional(25, "water_level_m", PrimitiveType::Double),
        optional(26, "water_level_trend", PrimitiveType::Int),
        optional(27, "surface_current_speed_kn", PrimitiveType::Double),
        optional(28, "surface_current_dir_deg", PrimitiveType::Int),
        optional(29, "current2_speed_kn", PrimitiveType::Double),
        optional(30, "current2_dir_deg", PrimitiveType::Int),
        optional(31, "current2_depth_m", PrimitiveType::Double),
        optional(32, "current3_speed_kn", PrimitiveType::Double),
        optional(33, "current3_dir_deg", PrimitiveType::Int),
        optional(34, "current3_depth_m", PrimitiveType::Double),
        optional(35, "wave_height_m", PrimitiveType::Double),
        optional(36, "wave_period_s", PrimitiveType::Int),
        optional(37, "wave_dir_deg", PrimitiveType::Int),
        optional(38, "swell_height_m", PrimitiveType::Double),
        optional(39, "swell_period_s", PrimitiveType::Int),
        optional(40, "swell_dir_deg", PrimitiveType::Int),
        optional(41, "sea_state", PrimitiveType::Int),
        optional(42, "water_temp_c", PrimitiveType::Double),
        optional(43, "precipitation_type", PrimitiveType::Int),
        optional(44, "salinity_pct", PrimitiveType::Double),
        optional(45, "ice", PrimitiveType::Int),
        optional(46, "station", PrimitiveType::String),
        optional(47, "payload", PrimitiveType::String),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building meteo schema")
}

pub fn binary_schema() -> Schema {
    let fields: Vec<Arc<NestedField>> = vec![
        required(1, "ts", PrimitiveType::Timestamptz),
        required(2, "source", PrimitiveType::String),
        required(3, "msg_type", PrimitiveType::Int),
        required(4, "mmsi", PrimitiveType::Long),
        optional(5, "dac", PrimitiveType::Int),
        optional(6, "fid", PrimitiveType::Int),
        optional(7, "payload_hex", PrimitiveType::String),
        optional(8, "payload_bits", PrimitiveType::Int),
        optional(9, "station", PrimitiveType::String),
        optional(10, "payload", PrimitiveType::String),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building binary schema")
}

pub fn other_schema() -> Schema {
    let fields: Vec<Arc<NestedField>> = vec![
        required(1, "ts", PrimitiveType::Timestamptz),
        required(2, "source", PrimitiveType::String),
        optional(3, "station", PrimitiveType::String),
        required(4, "msg_type", PrimitiveType::String),
        required(5, "payload", PrimitiveType::String),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building other schema")
}

pub fn atons_schema() -> Schema {
    let fields: Vec<Arc<NestedField>> = vec![
        required(1, "ts", PrimitiveType::Timestamptz),
        required(2, "source", PrimitiveType::String),
        required(3, "msg_type", PrimitiveType::Int),
        required(4, "mmsi", PrimitiveType::Long),
        optional(5, "ais_class", PrimitiveType::String),
        optional(6, "aid_type", PrimitiveType::String),
        optional(7, "name", PrimitiveType::String),
        optional(8, "name_extension", PrimitiveType::String),
        optional(9, "latitude", PrimitiveType::Double),
        optional(10, "longitude", PrimitiveType::Double),
        optional(11, "h3", PrimitiveType::Long),
        optional(12, "hilbert", PrimitiveType::Long),
        optional(13, "dimension_to_bow", PrimitiveType::Int),
        optional(14, "dimension_to_stern", PrimitiveType::Int),
        optional(15, "dimension_to_port", PrimitiveType::Int),
        optional(16, "dimension_to_starboard", PrimitiveType::Int),
        optional(17, "off_position", PrimitiveType::Boolean),
        optional(18, "virtual_aid", PrimitiveType::Boolean),
        optional(19, "assigned_mode", PrimitiveType::Boolean),
        optional(20, "high_accuracy", PrimitiveType::Boolean),
        optional(21, "raim", PrimitiveType::Boolean),
        optional(22, "station", PrimitiveType::String),
        optional(23, "payload", PrimitiveType::String),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building atons schema")
}
