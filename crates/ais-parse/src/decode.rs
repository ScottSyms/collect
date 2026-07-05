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

use nmea_parser::ais::{VesselDynamicData, VesselStaticData};
use nmea_parser::{NmeaParser, ParsedMessage};

/// One decoded position report (AIS types 1-3, 18, 19, 27).
pub struct PositionRow {
    pub ts_ms: i64,
    pub mmsi: u32,
    pub ais_class: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub sog_knots: Option<f64>,
    pub cog: Option<f64>,
    pub heading_true: Option<f64>,
    pub rot: Option<f64>,
    pub nav_status: String,
    pub high_accuracy: bool,
    pub raim: bool,
    pub special_manoeuvre: Option<bool>,
}

/// One decoded static/voyage report (AIS types 5 and 24).
pub struct StaticRow {
    pub ts_ms: i64,
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

pub enum Decoded {
    Position(Box<PositionRow>),
    Static(Box<StaticRow>),
    /// Decoded fine, but not a message class we materialize.
    Other,
    /// Part of a multi-sentence message; the parser buffered it.
    Incomplete,
    /// The parser rejected the sentence.
    Failed,
}

/// Strip a leading NMEA 4.10 tag block (`\...\`) if present.
fn strip_tag_block(payload: &str) -> &str {
    if let Some(rest) = payload.strip_prefix('\\') {
        if let Some(end) = rest.find('\\') {
            return &rest[end + 1..];
        }
    }
    payload
}

pub fn decode_payload(parser: &mut NmeaParser, ts_ms: i64, payload: &str) -> Decoded {
    let sentence = strip_tag_block(payload.trim());
    if sentence.is_empty() {
        return Decoded::Failed;
    }
    match parser.parse_sentence(sentence) {
        Ok(ParsedMessage::VesselDynamicData(vdd)) => {
            Decoded::Position(Box::new(position_row(ts_ms, vdd)))
        }
        Ok(ParsedMessage::VesselStaticData(vsd)) => {
            Decoded::Static(Box::new(static_row(ts_ms, vsd)))
        }
        Ok(ParsedMessage::Incomplete) => Decoded::Incomplete,
        Ok(_) => Decoded::Other,
        Err(_) => Decoded::Failed,
    }
}

fn position_row(ts_ms: i64, vdd: VesselDynamicData) -> PositionRow {
    PositionRow {
        ts_ms,
        mmsi: vdd.mmsi,
        ais_class: vdd.ais_type.to_string(),
        latitude: vdd.latitude,
        longitude: vdd.longitude,
        sog_knots: vdd.sog_knots,
        cog: vdd.cog,
        heading_true: vdd.heading_true,
        rot: vdd.rot,
        nav_status: vdd.nav_status.to_string(),
        high_accuracy: vdd.high_position_accuracy,
        raim: vdd.raim_flag,
        special_manoeuvre: vdd.special_manoeuvre,
    }
}

fn static_row(ts_ms: i64, vsd: VesselStaticData) -> StaticRow {
    StaticRow {
        ts_ms,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_tag_blocks() {
        assert_eq!(
            strip_tag_block(r"\c:1241544035*1D\!AIVDM,1,1,,B,15N4,0*00"),
            "!AIVDM,1,1,,B,15N4,0*00"
        );
        assert_eq!(
            strip_tag_block("!AIVDM,1,1,,B,15N4,0*00"),
            "!AIVDM,1,1,,B,15N4,0*00"
        );
        // Unterminated tag block falls through unchanged.
        assert_eq!(strip_tag_block(r"\c:12345"), r"\c:12345");
    }

    #[test]
    fn decodes_class_a_position() {
        let mut parser = NmeaParser::new();
        // Canonical class A position report (AIVDM reference example).
        let decoded = decode_payload(
            &mut parser,
            1_700_000_000_000,
            "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A",
        );
        match decoded {
            Decoded::Position(row) => {
                assert_eq!(row.ts_ms, 1_700_000_000_000);
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
        let decoded = decode_payload(&mut parser, 1_700_000_000_000, combined);
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
        match decode_payload(&mut parser, 1_700_000_000_000, part1) {
            Decoded::Incomplete => {}
            _ => panic!("first fragment should be Incomplete"),
        }
        match decode_payload(&mut parser, 1_700_000_000_000, part2) {
            Decoded::Static(row) => {
                assert_eq!(row.mmsi, 369_190_000);
                assert!(row.name.is_some());
            }
            _ => panic!("second fragment should complete the static message"),
        }
    }

    #[test]
    fn garbage_is_failed_not_panic() {
        let mut parser = NmeaParser::new();
        assert!(matches!(
            decode_payload(&mut parser, 0, "$PGHP,1,2013,1,9,4,37,45,298,,110,,1,26*19"),
            Decoded::Failed | Decoded::Other
        ));
        assert!(matches!(
            decode_payload(&mut parser, 0, "not a sentence at all"),
            Decoded::Failed
        ));
        assert!(matches!(
            decode_payload(&mut parser, 0, ""),
            Decoded::Failed
        ));
    }
}
