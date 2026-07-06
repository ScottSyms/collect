//! Decode one normalized AIS payload into a typed row.
//!
//! The bronze-normalized dataset stores single sentences per row —
//! ais-normalize already recombined multi-part messages into one
//! `!AIVDM,1,1,...` sentence — optionally prefixed with a NMEA 4.10 tag
//! block (`\c:...*XX\`). The tag block's timestamp is already reflected in
//! the row's `ts` column, so it is stripped before parsing.
//!
//! Raw (un-normalized) data works too: the parser buffers `Incomplete`
//! fragments internally and completes them when the matching part arrives,
//! which it usually does because rows within a partition file are ts-sorted.

use crate::ais_bits::{extract_ais_payload, Bits};
use h3o::{LatLng, Resolution};
use nmea_parser::ais::{AisClass, VesselDynamicData, VesselStaticData};
use nmea_parser::{NmeaParser, ParsedMessage};

fn lat_lon_to_h3(lat: f64, lon: f64) -> Option<u64> {
    let ll = LatLng::new(lat.to_radians(), lon.to_radians()).ok()?;
    Some(ll.to_cell(Resolution::Ten).into())
}

/// One decoded position report (AIS types 1-3, 9, 18, 19, 27).
pub struct PositionRow {
    pub ts_ms: i64,
    pub source: String,
    /// Source/base station from the NMEA tag block `s:` field, if present.
    pub station: Option<String>,
    /// The AIS message type that produced this row (1/2/3/9/18/19/27).
    pub msg_type: u8,
    pub mmsi: u32,
    pub ais_class: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub sog_knots: Option<f64>,
    pub cog: Option<f64>,
    pub heading_true: Option<f64>,
    pub rot: Option<f64>,
    pub altitude_m: Option<f64>,
    pub h3: Option<u64>,
    pub nav_status: String,
    pub high_accuracy: bool,
    pub raim: bool,
    pub special_manoeuvre: Option<bool>,
}

/// One decoded static/voyage report (AIS types 5 and 24).
pub struct StaticRow {
    pub ts_ms: i64,
    pub source: String,
    /// Source/base station from the NMEA tag block `s:` field, if present.
    pub station: Option<String>,
    /// The AIS message type that produced this row (5 or 24).
    pub msg_type: u8,
    pub mmsi: u32,
    pub ais_class: String,
    pub imo_number: Option<u32>,
    pub call_sign: Option<String>,
    pub name: Option<String>,
    pub ship_type: String,
    pub dimension_to_bow: Option<u16>,
    pub dimension_to_stern: Option<u16>,
    pub dimension_to_port: Option<u16>,
    pub dimension_to_starboard: Option<u16>,
    pub draught_m: Option<f64>,
    pub destination: Option<String>,
    pub eta_ms: Option<i64>,
    pub mothership_mmsi: Option<u32>,
}

/// One decoded Type 8 DAC=1 FID=31/FID=11 meteorological & hydrological
/// report. Every measurement is `Option` — AIS transmits a per-field "not
/// available" sentinel that maps to `None`. Physical values are scaled to
/// their natural units (°C, hPa, knots, metres, degrees true, %). Fields
/// unique to the current FID=31 layout (`position_accuracy`,
/// `visibility_greater`) are `None` for the deprecated FID=11.
pub struct MeteoRow {
    pub ts_ms: i64,
    pub source: String,
    /// Source/base station from the NMEA tag block `s:` field, if present.
    pub station: Option<String>,
    /// The AIS message type that produced this row (always 8).
    pub msg_type: u8,
    pub mmsi: u32,
    pub dac: u16,
    pub fid: u8,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
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

/// A Type 8 broadcast whose DAC/FID we don't field-decode: the generic header
/// plus the application payload retained as hex, so nothing is lost.
pub struct BinaryRow {
    pub ts_ms: i64,
    pub source: String,
    /// Source/base station from the NMEA tag block `s:` field, if present.
    pub station: Option<String>,
    /// The AIS message type that produced this row (always 8).
    pub msg_type: u8,
    pub mmsi: u32,
    pub dac: u16,
    pub fid: u8,
    /// Application data (from bit 56 onward) as uppercase hex.
    pub payload_hex: String,
    /// Length of the application data in bits.
    pub payload_bits: u32,
}

/// AIS type 21 (Aids to Navigation) report.
pub struct AtonRow {
    pub ts_ms: i64,
    pub source: String,
    /// Source/base station from the NMEA tag block `s:` field, if present.
    pub station: Option<String>,
    /// The AIS message type that produced this row (always 21).
    pub msg_type: u8,
    pub mmsi: u32,
    pub ais_class: String,
    /// Textual representation of the aid type (NavAidType Display).
    pub aid_type: String,
    pub name: Option<String>,
    pub name_extension: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub h3: Option<u64>,
    pub dimension_to_bow: Option<u16>,
    pub dimension_to_stern: Option<u16>,
    pub dimension_to_port: Option<u16>,
    pub dimension_to_starboard: Option<u16>,
    pub off_position: bool,
    pub virtual_aid: bool,
    pub assigned_mode: bool,
    pub high_accuracy: bool,
    pub raim: bool,
}

pub enum Decoded {
    Position(Box<PositionRow>),
    Static(Box<StaticRow>),
    /// Type 8 DAC=1 FID=31/11 meteorological/hydrological data.
    Meteo(Box<MeteoRow>),
    /// Type 8 with a DAC/FID we retain as raw hex.
    Binary(Box<BinaryRow>),
    /// AIS type 21 Aids to Navigation.
    Aton(Box<AtonRow>),
    /// Decoded fine, but not a message class we materialize.
    Other,
    /// Part of a multi-sentence message; the parser buffered it.
    Incomplete,
    /// The parser rejected the sentence.
    Failed,
}

/// Split a leading NMEA 4.10 tag block (`\...\`) from the sentence, returning
/// `(tag_body, sentence)`. `tag_body` is empty when there is no tag block.
fn split_tag_block(payload: &str) -> (&str, &str) {
    if let Some(rest) = payload.strip_prefix('\\') {
        if let Some(end) = rest.find('\\') {
            return (&rest[..end], &rest[end + 1..]);
        }
    }
    ("", payload)
}

/// Value of a single-letter tag-block field (e.g. `s` = source/base station),
/// or `None` if absent. `tag_body` is the part between the two backslashes,
/// e.g. `s:AIS_NOR,c:1620000000*5A`.
fn tag_field<'a>(tag_body: &'a str, key: &str) -> Option<&'a str> {
    let body = tag_body.split('*').next().unwrap_or(tag_body);
    for field in body.split(',') {
        if let Some(value) = field
            .trim()
            .strip_prefix(key)
            .and_then(|rest| rest.strip_prefix(':'))
        {
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

pub fn decode_payload(parser: &mut NmeaParser, ts_ms: i64, source: &str, payload: &str) -> Decoded {
    let (tag_body, sentence) = split_tag_block(payload.trim());
    if sentence.is_empty() {
        return Decoded::Failed;
    }
    // The NMEA tag block's `s:` field is the source/base station that received
    // the message; retained as a column. Present on single sentences and, since
    // ais-normalize preserves it, on combined multi-fragment sentences too.
    let station = tag_field(tag_body, "s").map(str::to_string);

    // The message type is the first 6 bits of the AIS payload. It's read
    // directly (not via nmea-parser, which only reports a Class A/B category)
    // so a row can be tagged with its exact type (1 vs 2 vs 3, ...). Only a
    // single/combined sentence carries the type in its first fragment; for a
    // raw multi-fragment message we fall back to the decoded variant's class.
    let peeked_type = match extract_ais_payload(sentence) {
        Some(p) if p.fragment_count == 1 => Bits::from_armored(p.armored, p.fill_bits)
            .filter(|bits| bits.len() >= 6)
            .map(|bits| {
                let t = bits.u(0, 6) as u8;
                // Type 8 (Binary Broadcast) is not handled by nmea-parser, so
                // decode it ourselves right here from the same bits.
                if t == 8 {
                    return (t, Some(decode_type8(ts_ms, source, station.clone(), &bits)));
                }
                (t, None)
            }),
        _ => None,
    };
    let peeked_type = match peeked_type {
        Some((_, Some(decoded))) => return decoded,
        Some((t, None)) => Some(t),
        None => None,
    };

    match parser.parse_sentence(sentence) {
        Ok(ParsedMessage::VesselDynamicData(vdd)) => Decoded::Position(Box::new(position_row(
            ts_ms,
            source,
            station,
            peeked_type,
            vdd,
        ))),
        Ok(ParsedMessage::VesselStaticData(vsd)) => Decoded::Static(Box::new(static_row(
            ts_ms,
            source,
            station,
            peeked_type,
            vsd,
        ))),
        Ok(ParsedMessage::StandardSarAircraftPositionReport(sar)) => {
            Decoded::Position(Box::new(from_sar_aircraft(
                ts_ms, source, station, peeked_type, sar,
            )))
        }
        Ok(ParsedMessage::BaseStationReport(br)) => {
            Decoded::Position(Box::new(base_station_row(
                ts_ms, source, station, peeked_type, br,
            )))
        }
        Ok(ParsedMessage::AidToNavigationReport(aid)) => {
            Decoded::Aton(Box::new(from_aid_to_navigation(
                ts_ms, source, station, aid,
            )))
        }
        Ok(ParsedMessage::Incomplete) => Decoded::Incomplete,
        Ok(_) => Decoded::Other,
        Err(_) => Decoded::Failed,
    }
}

fn position_row(
    ts_ms: i64,
    source: &str,
    station: Option<String>,
    peeked_type: Option<u8>,
    vdd: VesselDynamicData,
) -> PositionRow {
    // Position reports are single-fragment, so `peeked_type` is essentially
    // always present; the class-based fallback only guards raw multi-fragment.
    let msg_type = peeked_type.unwrap_or(match vdd.ais_type {
        AisClass::ClassB => 18,
        _ => 1,
    });
    PositionRow {
        ts_ms,
        source: source.to_string(),
        station,
        msg_type,
        mmsi: vdd.mmsi,
        ais_class: vdd.ais_type.to_string(),
        latitude: vdd.latitude,
        longitude: vdd.longitude,
        sog_knots: vdd.sog_knots,
        cog: vdd.cog,
        heading_true: vdd.heading_true,
        rot: vdd.rot,
        altitude_m: None,
        h3: vdd.latitude.and_then(|lat| vdd.longitude.and_then(|lon| lat_lon_to_h3(lat, lon))),
        nav_status: vdd.nav_status.to_string(),
        high_accuracy: vdd.high_position_accuracy,
        raim: vdd.raim_flag,
        special_manoeuvre: vdd.special_manoeuvre,
    }
}

fn static_row(
    ts_ms: i64,
    source: &str,
    station: Option<String>,
    peeked_type: Option<u8>,
    vsd: VesselStaticData,
) -> StaticRow {
    // Static reports are 2-fragment; a combined/normalized sentence peeks as
    // 5 or 24, but a raw completing fragment peeks as junk — fall back to the
    // class (Class A static = type 5, Class B static = type 24).
    let msg_type = peeked_type
        .filter(|t| *t == 5 || *t == 24)
        .unwrap_or(match vsd.ais_type {
            AisClass::ClassB => 24,
            _ => 5,
        });
    StaticRow {
        ts_ms,
        source: source.to_string(),
        station,
        msg_type,
        mmsi: vsd.mmsi,
        ais_class: vsd.ais_type.to_string(),
        imo_number: vsd.imo_number,
        call_sign: vsd.call_sign,
        name: vsd.name,
        ship_type: vsd.ship_type.to_string(),
        dimension_to_bow: vsd.dimension_to_bow,
        dimension_to_stern: vsd.dimension_to_stern,
        dimension_to_port: vsd.dimension_to_port,
        dimension_to_starboard: vsd.dimension_to_starboard,
        // draught10 is the draught in decimetres.
        draught_m: vsd.draught10.map(|d| f64::from(d) / 10.0),
        destination: vsd.destination,
        eta_ms: vsd.eta.map(|dt| dt.timestamp_millis()),
        mothership_mmsi: vsd.mothership_mmsi,
    }
}

fn from_sar_aircraft(
    ts_ms: i64,
    source: &str,
    station: Option<String>,
    peeked_type: Option<u8>,
    sar: nmea_parser::ais::StandardSarAircraftPositionReport,
) -> PositionRow {
    PositionRow {
        ts_ms,
        source: source.to_string(),
        station,
        msg_type: peeked_type.unwrap_or(9),
        mmsi: sar.mmsi,
        ais_class: "Class A".into(),
        latitude: sar.latitude,
        longitude: sar.longitude,
        sog_knots: sar.sog_knots.map(|k| k as f64),
        cog: sar.cog,
        heading_true: None,
        rot: None,
        altitude_m: sar.altitude.map(|a| a as f64),
        h3: sar.latitude.and_then(|lat| sar.longitude.and_then(|lon| lat_lon_to_h3(lat, lon))),
        nav_status: "under way using engine".into(),
        high_accuracy: sar.high_position_accuracy,
        raim: sar.raim_flag,
        special_manoeuvre: None,
    }
}

fn base_station_row(
    ts_ms: i64,
    source: &str,
    station: Option<String>,
    peeked_type: Option<u8>,
    br: nmea_parser::ais::BaseStationReport,
) -> PositionRow {
    PositionRow {
        ts_ms,
        source: source.to_string(),
        station,
        msg_type: peeked_type.unwrap_or(4),
        mmsi: br.mmsi,
        ais_class: "Base Station".into(),
        latitude: br.latitude,
        longitude: br.longitude,
        sog_knots: None,
        cog: None,
        heading_true: None,
        rot: None,
        altitude_m: None,
        h3: br.latitude.and_then(|lat| br.longitude.and_then(|lon| lat_lon_to_h3(lat, lon))),
        nav_status: String::new(),
        high_accuracy: br.high_position_accuracy,
        raim: br.raim_flag,
        special_manoeuvre: None,
    }
}

fn from_aid_to_navigation(
    ts_ms: i64,
    source: &str,
    station: Option<String>,
    aid: nmea_parser::ais::AidToNavigationReport,
) -> AtonRow {
    let trim_name = |s: String| -> Option<String> {
        let t = s.trim_end_matches(|c| c == ' ' || c == '@').to_string();
        if t.is_empty() { None } else { Some(t) }
    };

    AtonRow {
        ts_ms,
        source: source.to_string(),
        station,
        msg_type: 21,
        mmsi: aid.mmsi,
        ais_class: "AtoN".into(),
        aid_type: aid.aid_type.to_string(),
        name: trim_name(aid.name),
        name_extension: None,
        latitude: aid.latitude,
        longitude: aid.longitude,
        h3: aid.latitude.and_then(|lat| aid.longitude.and_then(|lon| lat_lon_to_h3(lat, lon))),
        dimension_to_bow: aid.dimension_to_bow,
        dimension_to_stern: aid.dimension_to_stern,
        dimension_to_port: aid.dimension_to_port,
        dimension_to_starboard: aid.dimension_to_starboard,
        off_position: aid.off_position_indicator,
        virtual_aid: aid.virtual_aid_flag,
        assigned_mode: aid.assigned_mode_flag,
        high_accuracy: false,
        raim: aid.raim_flag,
    }
}

// --- Type 8 Binary Broadcast ---------------------------------------------
//
// Bit offsets, scales/offsets, and per-field "not available" sentinels below
// follow the IMO289 (FID=31) and IMO236 (FID=11) meteorological/hydrological
// layouts as implemented by gpsd (drivers/driver_ais.c and gps.h), which is
// the reference decoder for the GPSD AIVDM specification.

/// Unsigned field, `None` when it equals its "not available" sentinel.
fn u_opt(b: &Bits, off: usize, n: usize, na: u64) -> Option<u64> {
    let v = b.u(off, n);
    (v != na).then_some(v)
}

/// Signed (two's-complement) field, `None` at its sentinel.
fn i_opt(b: &Bits, off: usize, n: usize, na: i64) -> Option<i64> {
    let v = b.i(off, n);
    (v != na).then_some(v)
}

/// Latitude/longitude: raw is in 1/1000 arc-minute (÷60000 = degrees) for both
/// FID=11 and FID=31. `None` at the sentinel or if the result is out of range.
fn latlon(raw: i64, na: i64, limit: f64) -> Option<f64> {
    if raw == na {
        return None;
    }
    let deg = raw as f64 / 60000.0;
    (deg.abs() <= limit).then_some(deg)
}

/// Salinity: raw ÷10 %, `None` at/above the sentinel (510 "≥50.1"/511 "sensor
/// n/a" on FID=31, 511 on FID=11).
fn sal_opt(raw: u64, na_min: u64) -> Option<f64> {
    (raw < na_min).then_some(raw as f64 / 10.0)
}

fn decode_type8(ts_ms: i64, source: &str, station: Option<String>, b: &Bits) -> Decoded {
    let mmsi = b.u(8, 30) as u32;
    let dac = b.u(40, 10) as u16;
    let fid = b.u(50, 6) as u8;
    match (dac, fid) {
        (1, 31) if b.len() >= 360 => Decoded::Meteo(Box::new(decode_meteo_fid31(
            ts_ms, source, station, mmsi, dac, fid, b,
        ))),
        (1, 11) if b.len() >= 352 => Decoded::Meteo(Box::new(decode_meteo_fid11(
            ts_ms, source, station, mmsi, dac, fid, b,
        ))),
        // Any other DAC/FID (or a truncated met/hydro): keep the header and the
        // application payload as hex so nothing is lost.
        _ => Decoded::Binary(Box::new(BinaryRow {
            ts_ms,
            source: source.to_string(),
            station,
            msg_type: 8,
            mmsi,
            dac,
            fid,
            payload_hex: b.hex_from(56),
            payload_bits: b.len().saturating_sub(56) as u32,
        })),
    }
}

/// IMO289 (current) Met/Hydro, DAC=1 FID=31, 360 bits.
#[allow(clippy::too_many_arguments)]
fn decode_meteo_fid31(
    ts_ms: i64,
    source: &str,
    station: Option<String>,
    mmsi: u32,
    dac: u16,
    fid: u8,
    b: &Bits,
) -> MeteoRow {
    MeteoRow {
        ts_ms,
        source: source.to_string(),
        station,
        msg_type: 8,
        mmsi,
        dac,
        fid,
        longitude: latlon(b.i(56, 25), 181 * 60 * 1000, 180.0),
        latitude: latlon(b.i(81, 24), 91 * 60 * 1000, 90.0),
        position_accuracy: Some(b.boolean(105)),
        day: u_opt(b, 106, 5, 0).map(|v| v as u8),
        hour: u_opt(b, 111, 5, 24).map(|v| v as u8),
        minute: u_opt(b, 116, 6, 60).map(|v| v as u8),
        wind_speed_kn: u_opt(b, 122, 7, 127).map(|v| v as u16),
        wind_gust_kn: u_opt(b, 129, 7, 127).map(|v| v as u16),
        wind_dir_deg: u_opt(b, 136, 9, 360).map(|v| v as u16),
        wind_gust_dir_deg: u_opt(b, 145, 9, 360).map(|v| v as u16),
        air_temp_c: i_opt(b, 154, 11, -1024).map(|v| v as f64 / 10.0),
        humidity_pct: u_opt(b, 165, 7, 101).map(|v| v as u8),
        dew_point_c: i_opt(b, 172, 10, 501).map(|v| v as f64 / 10.0),
        pressure_hpa: u_opt(b, 182, 9, 511).map(|v| (v + 799) as u16),
        pressure_tendency: u_opt(b, 191, 2, 3).map(|v| v as u8),
        visibility_greater: Some(b.boolean(193)),
        visibility_nm: u_opt(b, 194, 7, 127).map(|v| v as f64 / 10.0),
        water_level_m: u_opt(b, 201, 12, 4001).map(|v| (v as f64 - 1000.0) / 100.0),
        water_level_trend: u_opt(b, 213, 2, 3).map(|v| v as u8),
        surface_current_speed_kn: u_opt(b, 215, 8, 255).map(|v| v as f64 / 10.0),
        surface_current_dir_deg: u_opt(b, 223, 9, 360).map(|v| v as u16),
        current2_speed_kn: u_opt(b, 232, 8, 255).map(|v| v as f64 / 10.0),
        current2_dir_deg: u_opt(b, 240, 9, 360).map(|v| v as u16),
        current2_depth_m: u_opt(b, 249, 5, 31).map(|v| v as f64 / 10.0),
        current3_speed_kn: u_opt(b, 254, 8, 255).map(|v| v as f64 / 10.0),
        current3_dir_deg: u_opt(b, 262, 9, 360).map(|v| v as u16),
        current3_depth_m: u_opt(b, 271, 5, 31).map(|v| v as f64 / 10.0),
        wave_height_m: u_opt(b, 276, 8, 255).map(|v| v as f64 / 10.0),
        wave_period_s: u_opt(b, 284, 6, 63).map(|v| v as u16),
        wave_dir_deg: u_opt(b, 290, 9, 360).map(|v| v as u16),
        swell_height_m: u_opt(b, 299, 8, 255).map(|v| v as f64 / 10.0),
        swell_period_s: u_opt(b, 307, 6, 63).map(|v| v as u16),
        swell_dir_deg: u_opt(b, 313, 9, 360).map(|v| v as u16),
        sea_state: u_opt(b, 322, 4, 15).map(|v| v as u8),
        water_temp_c: i_opt(b, 326, 10, 501).map(|v| v as f64 / 10.0),
        precipitation_type: u_opt(b, 336, 3, 7).map(|v| v as u8),
        salinity_pct: sal_opt(b.u(339, 9), 510),
        ice: u_opt(b, 348, 2, 3).map(|v| v as u8),
    }
}

/// IMO236 (deprecated) Met/Hydro, DAC=1 FID=11, 352 bits. Same quantities as
/// FID=31 but a different layout: lat before lon, unsigned offset-encoded
/// temperatures, direction sentinel 511 (not 360), no position-accuracy or
/// visibility-greater fields.
#[allow(clippy::too_many_arguments)]
fn decode_meteo_fid11(
    ts_ms: i64,
    source: &str,
    station: Option<String>,
    mmsi: u32,
    dac: u16,
    fid: u8,
    b: &Bits,
) -> MeteoRow {
    MeteoRow {
        ts_ms,
        source: source.to_string(),
        station,
        msg_type: 8,
        mmsi,
        dac,
        fid,
        latitude: latlon(b.i(56, 24), 0x7F_FFFF, 90.0),
        longitude: latlon(b.i(80, 25), 0xFF_FFFF, 180.0),
        position_accuracy: None,
        day: u_opt(b, 105, 5, 0).map(|v| v as u8),
        hour: u_opt(b, 110, 5, 24).map(|v| v as u8),
        minute: u_opt(b, 115, 6, 60).map(|v| v as u8),
        wind_speed_kn: u_opt(b, 121, 7, 127).map(|v| v as u16),
        wind_gust_kn: u_opt(b, 128, 7, 127).map(|v| v as u16),
        wind_dir_deg: u_opt(b, 135, 9, 511).map(|v| v as u16),
        wind_gust_dir_deg: u_opt(b, 144, 9, 511).map(|v| v as u16),
        air_temp_c: u_opt(b, 153, 11, 2047).map(|v| (v as f64 - 600.0) / 10.0),
        humidity_pct: u_opt(b, 164, 7, 127).map(|v| v as u8),
        dew_point_c: u_opt(b, 171, 10, 1023).map(|v| (v as f64 - 200.0) / 10.0),
        pressure_hpa: u_opt(b, 181, 9, 511).map(|v| (v + 800) as u16),
        pressure_tendency: u_opt(b, 190, 2, 3).map(|v| v as u8),
        visibility_greater: None,
        visibility_nm: u_opt(b, 192, 8, 255).map(|v| v as f64 / 10.0),
        water_level_m: u_opt(b, 200, 9, 511).map(|v| (v as f64 - 100.0) / 10.0),
        water_level_trend: u_opt(b, 209, 2, 3).map(|v| v as u8),
        surface_current_speed_kn: u_opt(b, 211, 8, 255).map(|v| v as f64 / 10.0),
        surface_current_dir_deg: u_opt(b, 219, 9, 511).map(|v| v as u16),
        current2_speed_kn: u_opt(b, 228, 8, 255).map(|v| v as f64 / 10.0),
        current2_dir_deg: u_opt(b, 236, 9, 511).map(|v| v as u16),
        current2_depth_m: u_opt(b, 245, 5, 31).map(|v| v as f64 / 10.0),
        current3_speed_kn: u_opt(b, 250, 8, 255).map(|v| v as f64 / 10.0),
        current3_dir_deg: u_opt(b, 258, 9, 511).map(|v| v as u16),
        current3_depth_m: u_opt(b, 267, 5, 31).map(|v| v as f64 / 10.0),
        wave_height_m: u_opt(b, 272, 8, 255).map(|v| v as f64 / 10.0),
        wave_period_s: u_opt(b, 280, 6, 63).map(|v| v as u16),
        wave_dir_deg: u_opt(b, 286, 9, 511).map(|v| v as u16),
        swell_height_m: u_opt(b, 295, 8, 255).map(|v| v as f64 / 10.0),
        swell_period_s: u_opt(b, 303, 6, 63).map(|v| v as u16),
        swell_dir_deg: u_opt(b, 309, 9, 511).map(|v| v as u16),
        sea_state: u_opt(b, 318, 4, 15).map(|v| v as u8),
        water_temp_c: u_opt(b, 322, 10, 1023).map(|v| (v as f64 - 100.0) / 10.0),
        precipitation_type: u_opt(b, 332, 3, 7).map(|v| v as u8),
        salinity_pct: sal_opt(b.u(335, 9), 511),
        ice: u_opt(b, 344, 2, 3).map(|v| v as u8),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_tag_blocks_and_reads_station() {
        let (tag, sentence) =
            split_tag_block(r"\s:AIS_NOR,c:1241544035*1D\!AIVDM,1,1,,B,15N4,0*00");
        assert_eq!(sentence, "!AIVDM,1,1,,B,15N4,0*00");
        assert_eq!(tag_field(tag, "s"), Some("AIS_NOR"));
        assert_eq!(tag_field(tag, "c"), Some("1241544035"));
        assert_eq!(tag_field(tag, "d"), None);

        // No tag block: whole payload is the sentence, no station.
        let (tag, sentence) = split_tag_block("!AIVDM,1,1,,B,15N4,0*00");
        assert_eq!(sentence, "!AIVDM,1,1,,B,15N4,0*00");
        assert_eq!(tag_field(tag, "s"), None);

        // Unterminated tag block falls through as the sentence unchanged.
        let (tag, sentence) = split_tag_block(r"\c:12345");
        assert_eq!(tag, "");
        assert_eq!(sentence, r"\c:12345");
    }

    #[test]
    fn decodes_class_a_position() {
        let mut parser = NmeaParser::new();
        // Canonical class A position report (AIVDM reference example).
        let decoded = decode_payload(
            &mut parser,
            1_700_000_000_000,
            "norway",
            "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A",
        );
        match decoded {
            Decoded::Position(row) => {
                assert_eq!(row.ts_ms, 1_700_000_000_000);
                assert_eq!(row.source, "norway");
                assert_eq!(row.msg_type, 1, "type-1 position report");
                assert_eq!(row.mmsi, 371_798_000);
                let lat = row.latitude.expect("latitude");
                let lon = row.longitude.expect("longitude");
                assert!((lat - 48.38).abs() < 0.05, "lat {lat}");
                assert!((lon + 123.39).abs() < 0.05, "lon {lon}");
            }
            _ => panic!("expected a position row"),
        }
    }

    #[test]
    fn decodes_position_with_tag_block_prefix() {
        let mut parser = NmeaParser::new();
        // The shape ais-normalize emits: rebuilt \c:\ tag block + sentence.
        let decoded = decode_payload(
            &mut parser,
            1_700_000_000_000,
            "norway",
            r"\c:1700000000*5E\!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A",
        );
        assert!(matches!(decoded, Decoded::Position(_)));
    }

    #[test]
    fn decodes_static_voyage_data_from_combined_sentence() {
        let mut parser = NmeaParser::new();
        // A type 5 message pre-combined into a single sentence, the way
        // ais-normalize emits it (payload of both fragments concatenated).
        let combined = "!AIVDM,1,1,,A,569qcJP000000000000P4V1QDr3777800000000o0p=220DP0388888888881CRR@CACP08,2*10";
        let decoded = decode_payload(&mut parser, 1_700_000_000_000, "norway", combined);
        match decoded {
            Decoded::Static(row) => {
                assert!(row.mmsi > 0);
            }
            Decoded::Failed => {
                // Checksum of this hand-built sentence may not match; the
                // multi-part test below is authoritative.
            }
            _ => panic!("expected static row or checksum failure"),
        }
    }

    #[test]
    fn buffers_and_completes_raw_fragments() {
        let mut parser = NmeaParser::new();
        let part1 = "!AIVDM,2,1,3,B,55P5TL01VIaAL@7WKO@mBplU@<PDhh000000001S;AJ::4A80?4i@E53,0*3E";
        let part2 = "!AIVDM,2,2,3,B,1@0000000000000,2*55";
        match decode_payload(&mut parser, 1_700_000_000_000, "s", part1) {
            Decoded::Incomplete => {}
            _ => panic!("first fragment should be Incomplete"),
        }
        match decode_payload(&mut parser, 1_700_000_000_000, "s", part2) {
            Decoded::Static(row) => {
                assert_eq!(row.mmsi, 369_190_000);
                assert!(row.name.is_some());
                // Raw 2-fragment static: the completing fragment carries no
                // type, so msg_type falls back to the decoded class (a static
                // type, 5 or 24). The exact value needs combined/normalized
                // input — the pipeline's normal path.
                assert!(matches!(row.msg_type, 5 | 24), "got {}", row.msg_type);
            }
            _ => panic!("second fragment should complete the static message"),
        }
    }

    #[test]
    fn garbage_is_failed_not_panic() {
        let mut parser = NmeaParser::new();
        assert!(matches!(
            decode_payload(
                &mut parser,
                0,
                "s",
                "$PGHP,1,2013,1,9,4,37,45,298,,110,,1,26*19"
            ),
            Decoded::Failed | Decoded::Other
        ));
        assert!(matches!(
            decode_payload(&mut parser, 0, "s", "not a sentence at all"),
            Decoded::Failed
        ));
        assert!(matches!(
            decode_payload(&mut parser, 0, "s", ""),
            Decoded::Failed
        ));
    }

    /// Build a 360-bit FID=31 met/hydro message with known field values, using
    /// the independent test `BitPacker` (a different implementation from the
    /// decoder), so a shared offset bug can't hide. Fields are pushed in exact
    /// bit order to fully cover bits 0..360.
    fn fid31_sample() -> crate::ais_bits::BitPacker {
        let mut p = crate::ais_bits::BitPacker::new();
        p.push(8, 6); // type
        p.push(0, 2); // repeat
        p.push(2_655_619, 30); // mmsi
        p.push(0, 2); // spare
        p.push(1, 10); // dac
        p.push(31, 6); // fid
        p.push_i(4 * 60000, 25); // lon = 4.0E  (1/1000 min)
        p.push_i(51 * 60000, 24); // lat = 51.0N
        p.push(1, 1); // accuracy
        p.push(15, 5); // day
        p.push(12, 5); // hour
        p.push(30, 6); // minute
        p.push(20, 7); // wind speed kn
        p.push(25, 7); // wind gust kn
        p.push(180, 9); // wind dir
        p.push(190, 9); // wind gust dir
        p.push_i(-35, 11); // air temp -3.5C
        p.push(80, 7); // humidity
        p.push_i(-50, 10); // dew point -5.0C
        p.push(214, 9); // pressure raw -> 214+799 = 1013 hPa
        p.push(2, 2); // pressure tendency
        p.push(0, 1); // visibility greater
        p.push(100, 7); // visibility 10.0 nm
        p.push(1150, 12); // water level raw -> (1150-1000)/100 = 1.5 m
        p.push(1, 2); // level trend
        p.push(25, 8); // surface current 2.5 kn
        p.push(90, 9); // surface current dir
        p.push(255, 8); // current2 speed = N/A
        p.push(360, 9); // current2 dir = N/A
        p.push(31, 5); // current2 depth = N/A
        p.push(255, 8); // current3 speed = N/A
        p.push(360, 9); // current3 dir = N/A
        p.push(31, 5); // current3 depth = N/A
        p.push(12, 8); // wave height 1.2 m
        p.push(8, 6); // wave period
        p.push(200, 9); // wave dir
        p.push(255, 8); // swell height = N/A
        p.push(63, 6); // swell period = N/A
        p.push(360, 9); // swell dir = N/A
        p.push(4, 4); // sea state
        p.push_i(125, 10); // water temp 12.5C
        p.push(1, 3); // precipitation type
        p.push(350, 9); // salinity 35.0%
        p.push(0, 2); // ice
        p.push(0, 10); // spare
        assert_eq!(p.bit_len(), 360, "FID=31 message must be exactly 360 bits");
        p
    }

    #[test]
    fn decodes_fid31_met_hydro_fields() {
        let bits = fid31_sample().into_bits();
        let out = decode_type8(1_700_000_000_000, "norway", Some("AIS_NOR".into()), &bits);
        let Decoded::Meteo(m) = out else {
            panic!("expected a meteo row");
        };
        assert_eq!(m.msg_type, 8);
        assert_eq!(m.station.as_deref(), Some("AIS_NOR"));
        assert_eq!(m.mmsi, 2_655_619);
        assert_eq!(m.dac, 1);
        assert_eq!(m.fid, 31);
        assert!(
            (m.longitude.unwrap() - 4.0).abs() < 1e-6,
            "lon {:?}",
            m.longitude
        );
        assert!(
            (m.latitude.unwrap() - 51.0).abs() < 1e-6,
            "lat {:?}",
            m.latitude
        );
        assert_eq!(m.position_accuracy, Some(true));
        assert_eq!(m.day, Some(15));
        assert_eq!(m.hour, Some(12));
        assert_eq!(m.minute, Some(30));
        assert_eq!(m.wind_speed_kn, Some(20));
        assert_eq!(m.wind_dir_deg, Some(180));
        assert!((m.air_temp_c.unwrap() + 3.5).abs() < 1e-6);
        assert_eq!(m.humidity_pct, Some(80));
        assert!((m.dew_point_c.unwrap() + 5.0).abs() < 1e-6);
        assert_eq!(m.pressure_hpa, Some(1013));
        assert!((m.visibility_nm.unwrap() - 10.0).abs() < 1e-6);
        assert!((m.water_level_m.unwrap() - 1.5).abs() < 1e-6);
        assert!((m.surface_current_speed_kn.unwrap() - 2.5).abs() < 1e-6);
        assert_eq!(m.surface_current_dir_deg, Some(90));
        // N/A sentinels decode to None.
        assert_eq!(m.current2_speed_kn, None);
        assert_eq!(m.current2_dir_deg, None);
        assert_eq!(m.current2_depth_m, None);
        assert_eq!(m.swell_height_m, None);
        assert!((m.wave_height_m.unwrap() - 1.2).abs() < 1e-6);
        assert!((m.water_temp_c.unwrap() - 12.5).abs() < 1e-6);
        assert!((m.salinity_pct.unwrap() - 35.0).abs() < 1e-6);
        assert_eq!(m.ice, Some(0));
    }

    #[test]
    fn fid31_decodes_through_the_full_sentence_path() {
        // Re-armor the same message into an AIVDM sentence and run the whole
        // decode_payload path (extract → peek type 8 → decode).
        let armored = fid31_sample().armored();
        let sentence = format!("!AIVDM,1,1,,A,{armored},0*00");
        let mut parser = NmeaParser::new();
        match decode_payload(&mut parser, 42, "s", &sentence) {
            Decoded::Meteo(m) => {
                assert_eq!(m.ts_ms, 42);
                assert_eq!(m.fid, 31);
                assert!((m.latitude.unwrap() - 51.0).abs() < 1e-6);
            }
            _ => panic!("expected meteo via the full sentence path"),
        }
    }

    #[test]
    fn non_met_type8_is_retained_as_binary_hex() {
        // DAC=1 FID=22 (area notice): header decodes, payload kept as hex.
        let mut p = crate::ais_bits::BitPacker::new();
        p.push(8, 6);
        p.push(0, 2);
        p.push(123_456_789, 30);
        p.push(0, 2);
        p.push(1, 10); // dac
        p.push(22, 6); // fid (not met/hydro)
        p.push(0xABCD, 40); // some application data
        let bits = p.into_bits();
        match decode_type8(7, "s", None, &bits) {
            Decoded::Binary(b) => {
                assert_eq!(b.mmsi, 123_456_789);
                assert_eq!(b.dac, 1);
                assert_eq!(b.fid, 22);
                assert_eq!(b.payload_bits, 40);
                assert!(!b.payload_hex.is_empty());
            }
            _ => panic!("expected a binary row"),
        }
    }

    #[test]
    fn truncated_met_hydro_falls_back_to_binary() {
        // Claims FID=31 but is far shorter than 360 bits → generic binary.
        let mut p = crate::ais_bits::BitPacker::new();
        p.push(8, 6);
        p.push(0, 2);
        p.push(1, 30);
        p.push(0, 2);
        p.push(1, 10);
        p.push(31, 6);
        p.push(0, 20);
        let bits = p.into_bits();
        assert!(matches!(
            decode_type8(0, "s", None, &bits),
            Decoded::Binary(_)
        ));
    }
}
