use chrono::{Datelike, TimeZone, Utc};
use fast_hilbert::xy2h;
use h3o::{LatLng, Resolution};
use std::borrow::Cow;
use std::sync::Arc;

use crate::ais_stream::{
    AidsToNavigationReport, BaseStationReport, ExtendedClassBPositionReport, PositionReport,
    ShipStaticData, StandardClassBPositionReport, StandardSearchAndRescueAircraftReport,
    StaticDataReport,
};
use crate::output::OtherRow;

fn lat_lon_to_h3(lat: f64, lon: f64) -> Option<u64> {
    let ll = LatLng::new(lat.to_radians(), lon.to_radians()).ok()?;
    Some(ll.to_cell(Resolution::Ten).into())
}

const HILBERT_BITS: u32 = 31;

fn lat_lon_to_hilbert(lat: f64, lon: f64) -> Option<u64> {
    if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
        return None;
    }
    let x = ((lon + 180.0) / 360.0 * ((1u64 << HILBERT_BITS) as f64)) as u32;
    let y = ((lat + 90.0) / 180.0 * ((1u64 << HILBERT_BITS) as f64)) as u32;
    Some(xy2h(x, y, HILBERT_BITS as u8))
}

/// One decoded position report (AIS types 1-3, 9, 18).
pub struct PositionRow {
    pub ts_ms: i64,
    pub source: Arc<str>,
    pub station: Option<String>,
    pub msg_type: u8,
    pub mmsi: u32,
    pub ais_class: Cow<'static, str>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub sog_knots: Option<f64>,
    pub cog: Option<f64>,
    pub heading_true: Option<f64>,
    pub rot: Option<f64>,
    pub altitude_m: Option<f64>,
    pub h3: Option<u64>,
    pub hilbert: Option<u64>,
    pub nav_status: Cow<'static, str>,
    pub high_accuracy: bool,
    pub raim: bool,
    pub special_manoeuvre: Option<bool>,
}

/// One decoded static/voyage report (AIS types 5 and 24).
pub struct StaticRow {
    pub ts_ms: i64,
    pub source: Arc<str>,
    pub station: Option<String>,
    pub msg_type: u8,
    pub mmsi: u32,
    pub ais_class: Cow<'static, str>,
    pub imo_number: Option<u32>,
    pub call_sign: Option<String>,
    pub name: Option<String>,
    pub ship_type: Cow<'static, str>,
    pub dimension_to_bow: Option<u16>,
    pub dimension_to_stern: Option<u16>,
    pub dimension_to_port: Option<u16>,
    pub dimension_to_starboard: Option<u16>,
    pub draught_m: Option<f64>,
    pub destination: Option<String>,
    pub eta_ms: Option<i64>,
    pub mothership_mmsi: Option<u32>,
}

pub struct MeteoRow {
    pub ts_ms: i64,
    pub source: Arc<str>,
    pub station: Option<String>,
    pub msg_type: u8,
    pub mmsi: u32,
    pub dac: u16,
    pub fid: u8,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub hilbert: Option<u64>,
    pub position_accuracy: Option<bool>,
    pub day: Option<u8>,
    pub hour: Option<u8>,
    pub minute: Option<u8>,
    pub wind_speed_kn: Option<u16>,
    pub wind_gust_kn: Option<u16>,
    pub wind_dir_deg: Option<u16>,
    pub wind_gust_dir_deg: Option<u16>,
    pub air_temp_c: Option<f64>,
    pub humidity_pct: Option<u8>,
    pub dew_point_c: Option<f64>,
    pub pressure_hpa: Option<u16>,
    pub pressure_tendency: Option<u8>,
    pub visibility_nm: Option<f64>,
    pub visibility_greater: Option<bool>,
    pub water_level_m: Option<f64>,
    pub water_level_trend: Option<u8>,
    pub surface_current_speed_kn: Option<f64>,
    pub surface_current_dir_deg: Option<u16>,
    pub current2_speed_kn: Option<f64>,
    pub current2_dir_deg: Option<u16>,
    pub current2_depth_m: Option<f64>,
    pub current3_speed_kn: Option<f64>,
    pub current3_dir_deg: Option<u16>,
    pub current3_depth_m: Option<f64>,
    pub wave_height_m: Option<f64>,
    pub wave_period_s: Option<u16>,
    pub wave_dir_deg: Option<u16>,
    pub swell_height_m: Option<f64>,
    pub swell_period_s: Option<u16>,
    pub swell_dir_deg: Option<u16>,
    pub sea_state: Option<u8>,
    pub water_temp_c: Option<f64>,
    pub precipitation_type: Option<u8>,
    pub salinity_pct: Option<f64>,
    pub ice: Option<u8>,
}

pub struct BinaryRow {
    pub ts_ms: i64,
    pub source: Arc<str>,
    pub station: Option<String>,
    pub msg_type: u8,
    pub mmsi: u32,
    pub dac: u16,
    pub fid: u8,
    pub payload_hex: String,
    pub payload_bits: u32,
}

pub struct AtonRow {
    pub ts_ms: i64,
    pub source: Arc<str>,
    pub station: Option<String>,
    pub msg_type: u8,
    pub mmsi: u32,
    pub ais_class: Cow<'static, str>,
    pub aid_type: String,
    pub name: Option<String>,
    pub name_extension: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub h3: Option<u64>,
    pub hilbert: Option<u64>,
    pub dimension_to_bow: Option<u16>,
    pub dimension_to_stern: Option<u16>,
    pub dimension_to_port: Option<u16>,
    pub dimension_to_starboard: Option<u16>,
    pub off_position: Option<bool>,
    pub virtual_aid: Option<bool>,
    pub assigned_mode: Option<bool>,
    pub high_accuracy: bool,
    pub raim: bool,
}

#[allow(dead_code)]
pub enum Decoded {
    Position(PositionRow),
    Static(StaticRow),
    Meteo(MeteoRow),
    Binary(BinaryRow),
    Aton(AtonRow),
    Other(OtherRow),
    Failed,
}

pub fn decode_row(
    ts_ms: i64,
    source: &str,
    msg_type: &str,
    raw_payload: &str,
    message: &mut serde_json::Value,
) -> Decoded {
    let src: Arc<str> = Arc::from(source);
    let payload = if let Some(obj) = message.as_object_mut() {
        obj.remove(msg_type).unwrap_or_else(|| std::mem::take(message))
    } else {
        std::mem::take(message)
    };
    match msg_type {
        "PositionReport" => {
            match serde_json::from_value::<PositionReport>(payload) {
                Ok(pr) => Decoded::Position(from_position_report(ts_ms, &src, pr)),
                Err(_) => Decoded::Failed,
            }
        }
        "StandardClassBPositionReport" => {
            match serde_json::from_value::<StandardClassBPositionReport>(payload) {
                Ok(pr) => Decoded::Position(from_standard_class_b(ts_ms, &src, pr)),
                Err(_) => Decoded::Failed,
            }
        }
        "ShipStaticData" => {
            match serde_json::from_value::<ShipStaticData>(payload) {
                Ok(sd) => Decoded::Static(from_ship_static_data(ts_ms, &src, sd)),
                Err(_) => Decoded::Failed,
            }
        }
        "StaticDataReport" => {
            match serde_json::from_value::<StaticDataReport>(payload) {
                Ok(sr) => Decoded::Static(from_static_data_report(ts_ms, &src, sr)),
                Err(_) => Decoded::Failed,
            }
        }
        "BaseStationReport" => {
            match serde_json::from_value::<BaseStationReport>(payload) {
                Ok(br) => Decoded::Position(from_base_station_report(ts_ms, &src, br)),
                Err(_) => Decoded::Failed,
            }
        }
        "StandardSearchAndRescueAircraftReport" => {
            match serde_json::from_value::<StandardSearchAndRescueAircraftReport>(payload) {
                Ok(sar) => Decoded::Position(from_standard_sar_aircraft(ts_ms, &src, sar)),
                Err(_) => Decoded::Failed,
            }
        }
        "ExtendedClassBPositionReport" => {
            match serde_json::from_value::<ExtendedClassBPositionReport>(payload) {
                Ok(pr) => Decoded::Position(from_extended_class_b(ts_ms, &src, pr)),
                Err(_) => Decoded::Failed,
            }
        }
        "AidsToNavigationReport" => {
            match serde_json::from_value::<AidsToNavigationReport>(payload) {
                Ok(aid) => Decoded::Aton(from_aids_to_navigation(ts_ms, &src, aid)),
                Err(_) => Decoded::Failed,
            }
        }
        "LongRangeAisBroadcastMessage"
        | "SafetyBroadcastMessage" | "AddressedSafetyMessage"
        | "AddressedBinaryMessage" | "SingleSlotBinaryMessage" | "MultiSlotBinaryMessage"
        | "DataLinkManagementMessage"
        | "Interrogation" | "BinaryAcknowledge" | "ChannelManagement"
        | "AssignedModeCommand" | "CoordinatedUTCInquiry" | "GnssBroadcastBinaryMessage"
        | "GroupAssignmentCommand" => Decoded::Other(OtherRow {
            ts_ms,
            source: source.to_string(),
            msg_type: msg_type.to_string(),
            payload: raw_payload.to_string(),
        }),
        _ => Decoded::Other(OtherRow {
            ts_ms,
            source: source.to_string(),
            msg_type: msg_type.to_string(),
            payload: raw_payload.to_string(),
        }),
    }
}

fn from_position_report(ts_ms: i64, source: &Arc<str>, pr: PositionReport) -> PositionRow {
    PositionRow {
        ts_ms,
        source: source.clone(),
        station: None,
        msg_type: pr.MessageID,
        mmsi: pr.UserID,
        ais_class: Cow::Borrowed("Class A"),
        latitude: pr.Latitude,
        longitude: pr.Longitude,
        sog_knots: pr.Sog,
        cog: pr.Cog,
        heading_true: pr.TrueHeading.and_then(heading_to_f64),
        rot: pr.RateOfTurn.and_then(rot_to_f64),
        altitude_m: None,
        h3: pr.Latitude.and_then(|lat| pr.Longitude.and_then(|lon| lat_lon_to_h3(lat, lon))),
        hilbert: pr.Latitude.and_then(|lat| pr.Longitude.and_then(|lon| lat_lon_to_hilbert(lat, lon))),
        nav_status: Cow::Owned(
            pr.NavigationalStatus
                .map(nav_status_str)
                .unwrap_or("(notDefined)")
                .to_string(),
        ),
        high_accuracy: pr.PositionAccuracy,
        raim: pr.Raim,
        special_manoeuvre: pr
            .SpecialManoeuvreIndicator
            .and_then(special_manoeuvre_to_bool),
    }
}

fn from_standard_class_b(
    ts_ms: i64,
    source: &Arc<str>,
    pr: StandardClassBPositionReport,
) -> PositionRow {
    PositionRow {
        ts_ms,
        source: source.clone(),
        station: None,
        msg_type: pr.MessageID,
        mmsi: pr.UserID,
        ais_class: Cow::Borrowed("Class B"),
        latitude: pr.Latitude,
        longitude: pr.Longitude,
        sog_knots: pr.Sog,
        cog: pr.Cog,
        heading_true: pr.TrueHeading.and_then(heading_to_f64),
        rot: None,
        altitude_m: None,
        h3: pr.Latitude.and_then(|lat| pr.Longitude.and_then(|lon| lat_lon_to_h3(lat, lon))),
        hilbert: pr.Latitude.and_then(|lat| pr.Longitude.and_then(|lon| lat_lon_to_hilbert(lat, lon))),
        nav_status: Cow::Borrowed("under way using engine"),
        high_accuracy: pr.PositionAccuracy,
        raim: pr.Raim,
        special_manoeuvre: None,
    }
}

fn from_ship_static_data(ts_ms: i64, source: &Arc<str>, sd: ShipStaticData) -> StaticRow {
    let (bow, stern, port, starboard) = sd.Dimension.map_or((None, None, None, None), |d| {
        (d.A, d.B, d.C, d.D)
    });

    StaticRow {
        ts_ms,
        source: source.clone(),
        station: None,
        msg_type: sd.MessageID,
        mmsi: sd.UserID,
        ais_class: Cow::Borrowed("Class A"),
        imo_number: sd.ImoNumber.filter(|&n| n != 0),
        call_sign: trim_ais_str(sd.CallSign.as_deref()),
        name: trim_ais_str(sd.Name.as_deref()),
        ship_type: Cow::from(sd.ship_type.map(ship_type_str).unwrap_or_default()),
        dimension_to_bow: bow,
        dimension_to_stern: stern,
        dimension_to_port: port,
        dimension_to_starboard: starboard,
        draught_m: sd.MaximumStaticDraught,
        destination: trim_ais_str(sd.Destination.as_deref()).filter(|d| !d.is_empty()),
        eta_ms: sd.Eta.and_then(eta_to_ms),
        mothership_mmsi: None,
    }
} 

fn from_static_data_report(ts_ms: i64, source: &Arc<str>, sr: StaticDataReport) -> StaticRow {
    let (name, call_sign, ship_type, bow, stern, port, starboard) = if !sr.PartNumber {
        let name = sr
            .ReportA
            .and_then(|a| trim_ais_str(a.Name.as_deref()));
        (name, None, None, None, None, None, None)
    } else {
        let rb = sr.ReportB;
        let call_sign = rb
            .as_ref()
            .and_then(|b| trim_ais_str(b.CallSign.as_deref()));
        let ship_type = rb
            .as_ref()
            .and_then(|b| b.ShipType)
            .map(ship_type_str);
        let dims = rb.as_ref().and_then(|b| b.Dimension.clone());
        let (bow, stern, port, starboard) = dims.map_or((None, None, None, None), |d| {
            (d.A, d.B, d.C, d.D)
        });
        (None, call_sign, ship_type, bow, stern, port, starboard)
    };

    StaticRow {
        ts_ms,
        source: source.clone(),
        station: None,
        msg_type: sr.MessageID,
        mmsi: sr.UserID,
        ais_class: Cow::Borrowed("Class B"),
        imo_number: None,
        call_sign,
        name,
        ship_type: Cow::from(ship_type.unwrap_or_default()),
        dimension_to_bow: bow,
        dimension_to_stern: stern,
        dimension_to_port: port,
        dimension_to_starboard: starboard,
        draught_m: None,
        destination: None,
        eta_ms: None,
        mothership_mmsi: None,
    }
}

fn from_standard_sar_aircraft(
    ts_ms: i64,
    source: &Arc<str>,
    sar: StandardSearchAndRescueAircraftReport,
) -> PositionRow {
    PositionRow {
        ts_ms,
        source: source.clone(),
        station: None,
        msg_type: sar.MessageID,
        mmsi: sar.UserID,
        ais_class: Cow::Borrowed("Class A"),
        latitude: sar.Latitude,
        longitude: sar.Longitude,
        sog_knots: sar.Sog,
        cog: sar.Cog,
        heading_true: None,
        rot: None,
        altitude_m: sar.Altitude,
        h3: sar.Latitude.and_then(|lat| sar.Longitude.and_then(|lon| lat_lon_to_h3(lat, lon))),
        hilbert: sar.Latitude.and_then(|lat| sar.Longitude.and_then(|lon| lat_lon_to_hilbert(lat, lon))),
        nav_status: Cow::Borrowed("under way using engine"),
        high_accuracy: sar.PositionAccuracy,
        raim: sar.Raim,
        special_manoeuvre: None,
    }
}

fn from_aids_to_navigation(
    ts_ms: i64,
    source: &Arc<str>,
    aid: AidsToNavigationReport,
) -> AtonRow {
    let (bow, stern, port, starboard) = aid.Dimension.map_or((None, None, None, None), |d| {
        (d.A, d.B, d.C, d.D)
    });

    AtonRow {
        ts_ms,
        source: source.clone(),
        station: None,
        msg_type: aid.MessageID,
        mmsi: aid.UserID,
        ais_class: Cow::Borrowed("AtoN"),
        aid_type: aid.aid_type.map(navaid_type_str).unwrap_or("not specified").to_string(),
        name: trim_ais_str(aid.Name.as_deref()),
        name_extension: trim_ais_str(aid.NameExtension.as_deref()),
        latitude: aid.Latitude,
        longitude: aid.Longitude,
        h3: aid.Latitude.and_then(|lat| aid.Longitude.and_then(|lon| lat_lon_to_h3(lat, lon))),
        hilbert: aid.Latitude.and_then(|lat| aid.Longitude.and_then(|lon| lat_lon_to_hilbert(lat, lon))),
        dimension_to_bow: bow,
        dimension_to_stern: stern,
        dimension_to_port: port,
        dimension_to_starboard: starboard,
        off_position: aid.OffPosition,
        virtual_aid: aid.VirtualAtoN,
        assigned_mode: aid.AssignedMode,
        high_accuracy: aid.PositionAccuracy,
        raim: aid.Raim,
    }
}

fn from_base_station_report(ts_ms: i64, source: &Arc<str>, br: BaseStationReport) -> PositionRow {
    PositionRow {
        ts_ms,
        source: source.clone(),
        station: None,
        msg_type: br.MessageID,
        mmsi: br.UserID,
        ais_class: Cow::Borrowed("Base Station"),
        latitude: br.Latitude,
        longitude: br.Longitude,
        sog_knots: None,
        cog: None,
        heading_true: None,
        rot: None,
        altitude_m: None,
        h3: br.Latitude.and_then(|lat| br.Longitude.and_then(|lon| lat_lon_to_h3(lat, lon))),
        hilbert: br.Latitude.and_then(|lat| br.Longitude.and_then(|lon| lat_lon_to_hilbert(lat, lon))),
        nav_status: Cow::Borrowed(""),
        high_accuracy: br.PositionAccuracy,
        raim: br.Raim,
        special_manoeuvre: None,
    }
}

fn from_extended_class_b(
    ts_ms: i64,
    source: &Arc<str>,
    pr: ExtendedClassBPositionReport,
) -> PositionRow {
    PositionRow {
        ts_ms,
        source: source.clone(),
        station: None,
        msg_type: pr.MessageID,
        mmsi: pr.UserID,
        ais_class: Cow::Borrowed("Class B"),
        latitude: pr.Latitude,
        longitude: pr.Longitude,
        sog_knots: pr.Sog,
        cog: pr.Cog,
        heading_true: pr.TrueHeading.and_then(heading_to_f64),
        rot: None,
        altitude_m: None,
        h3: pr.Latitude.and_then(|lat| pr.Longitude.and_then(|lon| lat_lon_to_h3(lat, lon))),
        hilbert: pr.Latitude.and_then(|lat| pr.Longitude.and_then(|lon| lat_lon_to_hilbert(lat, lon))),
        nav_status: Cow::Borrowed("under way using engine"),
        high_accuracy: pr.PositionAccuracy,
        raim: pr.Raim,
        special_manoeuvre: None,
    }
}

/// TrueHeading 511 → None (not available).
fn heading_to_f64(v: u16) -> Option<f64> {
    if v == 511 {
        None
    } else {
        Some(f64::from(v))
    }
}

/// RateOfTurn -128 → None (not available).
fn rot_to_f64(v: i8) -> Option<f64> {
    if v == -128 {
        None
    } else {
        Some(f64::from(v))
    }
}

/// SpecialManoeuvreIndicator: 0 → None, 1 → Some(false), 2 → Some(true).
fn special_manoeuvre_to_bool(v: u8) -> Option<bool> {
    match v {
        0 => None,
        1 => Some(false),
        2 => Some(true),
        _ => None,
    }
}

fn eta_to_ms(eta: crate::ais_stream::Eta) -> Option<i64> {
    // ETA month and day are 0 when not available.
    let month = eta.Month.unwrap_or(0);
    let day = eta.Day.unwrap_or(0);
    if month == 0 || day == 0 {
        return None;
    }
    let hour = eta.Hour.unwrap_or(24).min(23);
    let minute = eta.Minute.unwrap_or(60).min(59);
    // Use a reference year — AIS doesn't transmit the year in ETA;
    // use the current year as a reasonable default since ETA is near-term.
    let now = Utc::now();
    let year = now.year_ce().1 as i32;
    Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
        .single()
        .map(|dt| dt.timestamp_millis())
}

/// Trim AIS string padding (trailing spaces and `@` characters).
fn trim_ais_str(s: Option<&str>) -> Option<String> {
    let s = s?;
    let trimmed = s.trim_end_matches(|c| c == ' ' || c == '@');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn nav_status_str(v: u8) -> &'static str {
    match v {
        0 => "under way using engine",
        1 => "at anchor",
        2 => "not under command",
        3 => "restricted manoeuverability",
        4 => "constrained by draught",
        5 => "moored",
        6 => "aground",
        7 => "engaged in fishing",
        8 => "under way sailing",
        9 => "(reserved9)",
        10 => "(reserved10)",
        11 => "(reserved11)",
        12 => "(reserved12)",
        13 => "(reserved13)",
        14 => "ais sart is active",
        15 => "(notDefined)",
        _ => "(notDefined)",
    }
}

/// Convert an AIS ship type code to the standard English category string,
/// matching the style ais-parse produces via nmea-parser.
fn ship_type_str(v: u8) -> &'static str {
    match v {
        0..=19 => "(not available)",
        20..=29 => "wing in ground",
        30..=39 => "fishing",
        40..=49 => "high-speed craft",
        50 => "pilot",
        51 => "search and rescue",
        52 => "tug",
        53 => "port tender",
        54 => "anti-pollution equipment",
        55 => "law enforcement",
        56..=57 => "(local)",
        58 => "medical transport",
        59 => "noncombatant",
        60..=69 => "passenger",
        70..=79 => "cargo",
        80..=89 => "tanker",
        90..=99 => "other",
        _ => "(not available)",
    }
}

fn navaid_type_str(v: u8) -> &'static str {
    match v {
        0 => "not specified",
        1 => "reference point",
        2 => "RACON",
        3 => "FixedStructure",
        4 => "(reserved)",
        5 => "light without sectors",
        6 => "light with sectors",
        7 => "leading light front",
        8 => "leading light rear",
        9 => "cardinal beacon, north",
        10 => "cardinal beacon, east",
        11 => "cardinal beacon, south",
        12 => "cardinal beacon, west",
        13 => "lateral beacon, port side",
        14 => "lateral beacon, starboard side",
        15 => "lateral beacon, preferred channel, port side",
        16 => "lateral beacon, preferred channel, starboard side",
        17 => "isolated danger beacon",
        18 => "safe water",
        19 => "special mark",
        20 => "cardinal mark, north",
        21 => "cardinal mark, east",
        22 => "cardinal mark, south",
        23 => "cardinal mark, west",
        24 => "port hand mark",
        25 => "starboard hand mark",
        26 => "preferred channel, port side",
        27 => "preferred channel, starboard side",
        28 => "isolated danger",
        29 => "safe water",
        30 => "special mark",
        31 => "light vessel",
        _ => "not specified",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nav_status_mapping() {
        assert_eq!(nav_status_str(0), "under way using engine");
        assert_eq!(nav_status_str(5), "moored");
        assert_eq!(nav_status_str(15), "(notDefined)");
        assert_eq!(nav_status_str(99), "(notDefined)");
    }

    #[test]
    fn test_ship_type_mapping() {
        assert_eq!(ship_type_str(30), "fishing");
        assert_eq!(ship_type_str(52), "tug");
        assert_eq!(ship_type_str(70), "cargo");
        assert_eq!(ship_type_str(80), "tanker");
        assert_eq!(ship_type_str(0), "(not available)");
    }

    #[test]
    fn test_navaid_type_mapping() {
        assert_eq!(navaid_type_str(0), "not specified");
        assert_eq!(navaid_type_str(5), "light without sectors");
        assert_eq!(navaid_type_str(29), "safe water");
    }

    #[test]
    fn test_heading_not_available() {
        assert_eq!(heading_to_f64(511), None);
        assert_eq!(heading_to_f64(180), Some(180.0));
    }

    #[test]
    fn test_rot_not_available() {
        assert_eq!(rot_to_f64(-128), None);
        assert_eq!(rot_to_f64(0), Some(0.0));
        assert_eq!(rot_to_f64(127), Some(127.0));
    }

    #[test]
    fn test_special_manoeuvre_mapping() {
        assert_eq!(special_manoeuvre_to_bool(0), None);
        assert_eq!(special_manoeuvre_to_bool(1), Some(false));
        assert_eq!(special_manoeuvre_to_bool(2), Some(true));
    }

    #[test]
    fn test_trim_ais_str() {
        assert_eq!(
            trim_ais_str(Some("  VESSEL NAME  ")),
            Some("  VESSEL NAME".to_string())
        );
        assert_eq!(trim_ais_str(Some("@@@@@@")), None);
        assert_eq!(trim_ais_str(Some("ROTTERDAM")), Some("ROTTERDAM".to_string()));
        assert_eq!(trim_ais_str(None), None);
    }

    #[test]
    fn test_eta_with_zero_month() {
        let eta = crate::ais_stream::Eta {
            Month: Some(0),
            Day: Some(15),
            Hour: Some(12),
            Minute: Some(30),
        };
        assert_eq!(eta_to_ms(eta), None);
    }

    #[test]
    fn test_eta_with_zero_day() {
        let eta = crate::ais_stream::Eta {
            Month: Some(6),
            Day: Some(0),
            Hour: Some(12),
            Minute: Some(30),
        };
        assert_eq!(eta_to_ms(eta), None);
    }
}
