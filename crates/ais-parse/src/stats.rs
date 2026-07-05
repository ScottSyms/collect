/// Counters accumulated across all processed partitions.
#[derive(Clone, Copy, Debug, Default)]
pub struct ParseStats {
    pub partitions_processed: u64,
    pub rows_in: u64,
    pub positions_out: u64,
    pub statics_out: u64,
    /// Sentences that decoded fine but aren't materialized (base station
    /// reports, aids to navigation, safety messages, GNSS sentences, ...).
    pub other_decoded: u64,
    /// Fragments of a multi-part message whose remaining parts never arrived
    /// within the partition.
    pub incomplete: u64,
    /// Sentences the parser rejected (checksum errors, unsupported talkers,
    /// non-NMEA payloads such as `$PGHP` capture wrappers).
    pub failed: u64,
}

impl ParseStats {
    pub fn merge(&mut self, other: &ParseStats) {
        self.partitions_processed += other.partitions_processed;
        self.rows_in += other.rows_in;
        self.positions_out += other.positions_out;
        self.statics_out += other.statics_out;
        self.other_decoded += other.other_decoded;
        self.incomplete += other.incomplete;
        self.failed += other.failed;
    }

    pub fn print_summary(&self) {
        eprintln!("--- ais-parse summary ---");
        eprintln!("  partitions processed : {}", self.partitions_processed);
        eprintln!("  input rows           : {}", self.rows_in);
        eprintln!("  position rows        : {}", self.positions_out);
        eprintln!("  static rows          : {}", self.statics_out);
        eprintln!("  other decoded        : {}", self.other_decoded);
        eprintln!("  incomplete fragments : {}", self.incomplete);
        eprintln!("  unparsed             : {}", self.failed);
    }
}
