#[derive(Debug, Default, Clone)]
pub struct NormalizeStats {
    pub input_rows: u64,
    pub output_rows: u64,
    pub retimestamped_rows: u64,
    pub repartitioned_rows: u64,
    pub combined_messages: u64,
    pub incomplete_groups: u64,
    pub partitions_processed: u64,
}

impl NormalizeStats {
    pub fn merge(&mut self, other: &NormalizeStats) {
        self.input_rows += other.input_rows;
        self.output_rows += other.output_rows;
        self.retimestamped_rows += other.retimestamped_rows;
        self.repartitioned_rows += other.repartitioned_rows;
        self.combined_messages += other.combined_messages;
        self.incomplete_groups += other.incomplete_groups;
        self.partitions_processed += other.partitions_processed;
    }

    pub fn print_summary(&self) {
        eprintln!("--- ais-normalize summary ---");
        eprintln!("  partitions processed : {}", self.partitions_processed);
        eprintln!("  input rows           : {}", self.input_rows);
        eprintln!("  output rows          : {}", self.output_rows);
        eprintln!("  re-timestamped       : {}", self.retimestamped_rows);
        eprintln!("  re-partitioned       : {}", self.repartitioned_rows);
        eprintln!("  combined messages    : {}", self.combined_messages);
        if self.incomplete_groups > 0 {
            eprintln!("  incomplete groups    : {} (emitted as-is)", self.incomplete_groups);
        }
    }
}
