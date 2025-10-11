# Quick Profiling Guide

## Immediate Resource Monitoring

### 1. Basic Resource Usage
```bash
# Get basic CPU time and memory usage
/usr/bin/time -l ./target/release/hive_parquet_ingest --health-check

# Real-time monitoring
./monitor_resources.sh ./target/release/hive_parquet_ingest --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp

# Memory tracking with CSV output
./memory_tracker.sh ./target/release/hive_parquet_ingest --input norway.nmea --source test
```

### 2. Quick Benchmarks
```bash
# Run comprehensive benchmark suite
./benchmark.sh

# Monitor specific scenarios
top -pid $(pgrep hive_parquet_ingest)  # Real-time process monitoring
```

### 3. macOS-Specific Tools
```bash
# Sample CPU profiling
sample $(pgrep hive_parquet_ingest) 10 -file cpu_profile.txt

# Activity Monitor (GUI)
open -a "Activity Monitor"

# Instruments profiling
xcrun xctrace record --template 'Time Profiler' --launch ./target/release/hive_parquet_ingest -- --health-check
```

### 4. Key Metrics to Watch

| Metric | Tool | Normal Range | Red Flag |
|--------|------|--------------|----------|
| **Memory (RSS)** | `time -l`, `top` | 10-500MB | >1GB or growing |
| **CPU Usage** | `top`, `monitor_resources.sh` | 10-80% | Sustained 100% |
| **Execution Time** | `time` | <1s (health), varies (processing) | Unexpectedly slow |
| **Page Faults** | `time -l` | <1000 | >10000 (swapping) |

### 5. Quick Commands Reference

```bash
# Build optimized version
cargo build --release

# Profile health check
/usr/bin/time -l ./target/release/hive_parquet_ingest --health-check

# Monitor file processing
./monitor_resources.sh ./target/release/hive_parquet_ingest --input norway.nmea --source test

# Monitor TCP stream (background)
./target/release/hive_parquet_ingest --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp &
top -pid $!

# Memory analysis
./memory_tracker.sh ./target/release/hive_parquet_ingest --input norway.nmea --source memory-test

# Complete benchmark
./benchmark.sh
```

### 6. Interpreting Results

**Good Performance:**
- Health check: <50ms, <10MB memory
- File processing: Scales linearly with file size
- TCP stream: Steady memory usage, no growth
- CPU: Proportional to data processing rate

**Performance Issues:**
- Memory continuously growing (memory leak)
- CPU at 100% with no progress (infinite loop)
- High page faults (insufficient memory)
- Slow execution compared to file size

See `PROFILING_GUIDE.md` for detailed analysis and optimization recommendations.