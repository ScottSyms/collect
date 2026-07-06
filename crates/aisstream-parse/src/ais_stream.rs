use serde::Deserialize;

// ── Top-level message envelope ──────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case, dead_code)]
pub struct AisStreamMessage {
    pub MessageType: String,
    pub MetaData: MetaData,
    pub Message: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case, dead_code)]
pub struct MetaData {
    #[serde(default)]
    pub MMSI: u32,
    #[serde(default)]
    pub ShipName: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub time_utc: Option<String>,
}

// ── PositionReport (AIS types 1, 2, 3) ─────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct PositionReport {
    pub MessageID: u8,
    pub UserID: u32,
    pub Latitude: Option<f64>,
    pub Longitude: Option<f64>,
    pub Sog: Option<f64>,
    pub Cog: Option<f64>,
    pub TrueHeading: Option<u16>,
    pub RateOfTurn: Option<i8>,
    pub NavigationalStatus: Option<u8>,
    pub PositionAccuracy: bool,
    pub Raim: bool,
    pub SpecialManoeuvreIndicator: Option<u8>,
}

// ── StandardClassBPositionReport (AIS type 18) ─────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct StandardClassBPositionReport {
    pub MessageID: u8,
    pub UserID: u32,
    pub Latitude: Option<f64>,
    pub Longitude: Option<f64>,
    pub Sog: Option<f64>,
    pub Cog: Option<f64>,
    pub TrueHeading: Option<u16>,
    pub PositionAccuracy: bool,
    pub Raim: bool,
}

// ── ShipStaticData (AIS type 5) ────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case, dead_code)]
pub struct ShipStaticData {
    pub MessageID: u8,
    pub UserID: u32,
    pub ImoNumber: Option<u32>,
    pub CallSign: Option<String>,
    pub Name: Option<String>,
    #[serde(rename = "Type")]
    pub ship_type: Option<u8>,
    pub Dimension: Option<ShipDimensions>,
    pub FixType: Option<u8>,
    pub Eta: Option<Eta>,
    pub MaximumStaticDraught: Option<f64>,
    pub Destination: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ShipDimensions {
    pub A: Option<u16>,
    pub B: Option<u16>,
    pub C: Option<u16>,
    pub D: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct Eta {
    pub Month: Option<u32>,
    pub Day: Option<u32>,
    pub Hour: Option<u32>,
    pub Minute: Option<u32>,
}

// ── StaticDataReport (AIS type 24) ─────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct StaticDataReport {
    pub MessageID: u8,
    pub UserID: u32,
    pub PartNumber: bool,
    pub ReportA: Option<ReportA>,
    pub ReportB: Option<ReportB>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ReportA {
    pub Name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ReportB {
    pub CallSign: Option<String>,
    pub Dimension: Option<ShipDimensions>,
    pub ShipType: Option<u8>,
}

// ── BaseStationReport (AIS type 4) ─────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case, dead_code)]
pub struct BaseStationReport {
    pub MessageID: u8,
    pub UserID: u32,
    pub Latitude: Option<f64>,
    pub Longitude: Option<f64>,
    pub PositionAccuracy: bool,
    pub Raim: bool,
}

// ── StandardSearchAndRescueAircraftReport (AIS type 9) ─────────────────

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct StandardSearchAndRescueAircraftReport {
    pub MessageID: u8,
    pub UserID: u32,
    pub Altitude: Option<f64>,
    pub Sog: Option<f64>,
    pub Cog: Option<f64>,
    pub Latitude: Option<f64>,
    pub Longitude: Option<f64>,
    pub PositionAccuracy: bool,
    pub Raim: bool,
    pub AltFromBaro: Option<bool>,
    pub Dte: Option<bool>,
    pub AssignedMode: Option<bool>,
}

// ── AidsToNavigationReport (AIS type 21) ───────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct AidsToNavigationReport {
    pub MessageID: u8,
    pub UserID: u32,
    #[serde(rename = "Type")]
    pub aid_type: Option<u8>,
    pub Name: Option<String>,
    pub Latitude: Option<f64>,
    pub Longitude: Option<f64>,
    pub Dimension: Option<ShipDimensions>,
    pub PositionAccuracy: bool,
    pub Raim: bool,
    pub OffPosition: Option<bool>,
    pub VirtualAtoN: Option<bool>,
    pub AssignedMode: Option<bool>,
    pub NameExtension: Option<String>,
}
