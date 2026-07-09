#[derive(Clone, Copy, Debug, Default)]
pub struct ParseStats {
    pub partitions_processed: u64,
    pub rows_in: u64,
    pub positions_out: u64,
    pub statics_out: u64,
    pub meteo_out: u64,
    pub binary_out: u64,
    pub atons_out: u64,
    pub other_decoded: u64,
    pub unknown_type: u64,
    pub failed: u64,
    /// Rows dropped because an identical (ts, mmsi, source-keyed) row was
    /// already emitted for this partition in this run.
    pub rows_deduped: u64,
}

impl ParseStats {
    pub fn merge(&mut self, other: &ParseStats) {
        self.partitions_processed += other.partitions_processed;
        self.rows_in += other.rows_in;
        self.positions_out += other.positions_out;
        self.statics_out += other.statics_out;
        self.meteo_out += other.meteo_out;
        self.binary_out += other.binary_out;
        self.atons_out += other.atons_out;
        self.other_decoded += other.other_decoded;
        self.unknown_type += other.unknown_type;
        self.failed += other.failed;
        self.rows_deduped += other.rows_deduped;
    }

    pub fn print_summary(&self) {
        eprintln!("--- aisstream-parse summary ---");
        eprintln!("  partitions processed : {}", self.partitions_processed);
        eprintln!("  input rows           : {}", self.rows_in);
        eprintln!("  position rows        : {}", self.positions_out);
        eprintln!("  static rows          : {}", self.statics_out);
        eprintln!("  meteo rows           : {}", self.meteo_out);
        eprintln!("  binary rows          : {}", self.binary_out);
        eprintln!("  aton rows            : {}", self.atons_out);
        eprintln!("  other decoded        : {}", self.other_decoded);
        eprintln!("  unknown type         : {}", self.unknown_type);
        eprintln!("  unparsed             : {}", self.failed);
        eprintln!("  deduped (dropped)    : {}", self.rows_deduped);
    }
}
