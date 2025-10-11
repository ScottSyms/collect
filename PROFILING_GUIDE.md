# Profiling Guide for Hive Parquet Ingest

This guide covers various methods to profile the compute and memory resources used by the `hive_parquet_ingest` executable.

## Quick Overview

| Tool | Purpose | Platform | Detail Level |
|------|---------|----------|--------------|
| `time` | Basic CPU/memory usage | All Unix | Low |
| `top`/`htop` | Real-time monitoring | All Unix | Medium |
| `Activity Monitor` | GUI monitoring | macOS | Medium |
| `Instruments` | Detailed profiling | macOS | High |
| `perf` | CPU profiling | Linux | High |
| `valgrind` | Memory profiling | Linux | High |
| Rust built-ins | Code-level profiling | All | High |

## 1. Basic Resource Monitoring

### Using `time` Command
Get basic CPU time, memory usage, and system calls:

```bash
# Basic timing
/usr/bin/time -l ./target/release/hive_parquet_ingest --input norway.nmea --source test

# Detailed output on macOS
/usr/bin/time -l ./target/release/hive_parquet_ingest --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp
```

Output includes:
- Real/user/system time
- Maximum resident set size (RSS)
- Page faults, swaps, context switches
- I/O operations

### Using `top` or `htop`
Monitor in real-time:

```bash
# In one terminal, start the application
./target/release/hive_parquet_ingest --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp

# In another terminal, monitor specific process
top -pid $(pgrep hive_parquet_ingest)

# Or use htop for better interface (install with: brew install htop)
htop -p $(pgrep hive_parquet_ingest)
```

## 2. macOS-Specific Tools

### Activity Monitor (GUI)
1. Open Activity Monitor (`/Applications/Utilities/Activity Monitor.app`)
2. Search for "hive_parquet_ingest"
3. Monitor CPU, Memory, Energy, Disk, and Network tabs

### Instruments (Advanced Profiling)
Apple's powerful profiling tool:

```bash
# Build with debug symbols for better profiling
cargo build --release

# Profile with Instruments Time Profiler
xcrun xctrace record --template 'Time Profiler' --launch ./target/release/hive_parquet_ingest -- --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp

# Profile memory allocations
xcrun xctrace record --template 'Allocations' --launch ./target/release/hive_parquet_ingest -- --input norway.nmea --source test
```

Or launch Instruments GUI:
```bash
open -a Instruments
# Then attach to running process or launch new one
```

### Using `sample` Command
Profile a running process for CPU usage:

```bash
# Start your application
./target/release/hive_parquet_ingest --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp &

# Sample CPU usage for 30 seconds
sample $(pgrep hive_parquet_ingest) 30 -file profile_output.txt
```

## 3. Cross-Platform Monitoring Scripts

### Real-time Resource Monitor Script

```bash
#!/bin/bash
# monitor_resources.sh

if [ -z "$1" ]; then
    echo "Usage: $0 <command_and_args>"
    echo "Example: $0 ./target/release/hive_parquet_ingest --health-check"
    exit 1
fi

echo "Starting resource monitoring..."
echo "Command: $@"
echo "=========================="

# Start the command in background
"$@" &
PID=$!

echo "Process PID: $PID"
echo "Monitoring every 2 seconds..."
echo

# Monitor loop
while kill -0 $PID 2>/dev/null; do
    # Get process stats
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        ps -p $PID -o pid,pcpu,pmem,rss,vsz,time,comm
    else
        # Linux
        ps -p $PID -o pid,pcpu,pmem,rss,vsz,time,comm
    fi
    sleep 2
done

echo "Process completed."
```

### Memory Growth Detector

```bash
#!/bin/bash
# memory_tracker.sh

if [ -z "$1" ]; then
    echo "Usage: $0 <command_and_args>"
    exit 1
fi

echo "Memory usage tracking"
echo "===================="

# Start command in background
"$@" &
PID=$!

# Track memory over time
echo "Time,RSS_MB,VSZ_MB,CPU%" > memory_log.csv

while kill -0 $PID 2>/dev/null; do
    TIMESTAMP=$(date '+%H:%M:%S')
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS - RSS in KB
        STATS=$(ps -p $PID -o rss,vsz,pcpu | tail -1)
        RSS_KB=$(echo $STATS | awk '{print $1}')
        VSZ_KB=$(echo $STATS | awk '{print $2}')
        CPU=$(echo $STATS | awk '{print $3}')
        
        RSS_MB=$((RSS_KB / 1024))
        VSZ_MB=$((VSZ_KB / 1024))
    else
        # Linux
        STATS=$(ps -p $PID -o rss,vsz,pcpu | tail -1)
        RSS_KB=$(echo $STATS | awk '{print $1}')
        VSZ_KB=$(echo $STATS | awk '{print $2}')
        CPU=$(echo $STATS | awk '{print $3}')
        
        RSS_MB=$((RSS_KB / 1024))
        VSZ_MB=$((VSZ_KB / 1024))
    fi
    
    echo "$TIMESTAMP,$RSS_MB,$VSZ_MB,$CPU" >> memory_log.csv
    echo "$TIMESTAMP: RSS=${RSS_MB}MB, VSZ=${VSZ_MB}MB, CPU=${CPU}%"
    
    sleep 1
done

echo "Memory tracking complete. Results in memory_log.csv"
```

## 4. Rust-Specific Profiling

### Enable Profiling in Cargo.toml

Add to your `Cargo.toml`:

```toml
[profile.profiling]
inherits = "release"
debug = true  # Include debug symbols for better profiling
```

Build with profiling:
```bash
cargo build --profile profiling
```

### Built-in Memory Allocator Stats

Add jemalloc for better memory profiling:

```toml
# Add to Cargo.toml
[dependencies]
jemallocator = "0.5"

[features]
jemalloc = ["jemallocator"]
```

### CPU Profiling with cargo-flamegraph

Install and use flamegraph:

```bash
# Install
cargo install flamegraph

# Profile (may need sudo on some systems)
cargo flamegraph --bin hive_parquet_ingest -- --input norway.nmea --source test

# This generates flamegraph.svg showing CPU hotspots
```

### Memory Profiling with DHAT

Add to `Cargo.toml`:
```toml
[dependencies]
dhat = "0.3"
```

Add to your `main.rs`:
```rust
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    
    // Your existing code...
}
```

Build and run:
```bash
cargo run --features dhat-heap --bin hive_parquet_ingest -- --input norway.nmea --source test
# Generates dhat-heap.json for analysis
```

## 5. Docker Resource Monitoring

### Docker Stats
If running in Docker:

```bash
# Monitor container resources
docker stats <container_name>

# Get detailed container metrics
docker exec <container_name> cat /proc/meminfo
docker exec <container_name> cat /proc/cpuinfo
```

### Docker Resource Limits
Add to your docker-compose.yml:

```yaml
services:
  data-ingest:
    # ... existing config
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: '0.5'
        reservations:
          memory: 512M
          cpus: '0.25'
    
    # Enable resource monitoring
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

## 6. Performance Testing Scenarios

### Scenario 1: File Processing
```bash
# Create test data
head -1000 norway.nmea > test_small.nmea
head -10000 norway.nmea > test_medium.nmea

# Profile different file sizes
/usr/bin/time -l ./target/release/hive_parquet_ingest --input test_small.nmea --source small
/usr/bin/time -l ./target/release/hive_parquet_ingest --input test_medium.nmea --source medium
/usr/bin/time -l ./target/release/hive_parquet_ingest --input norway.nmea --source full
```

### Scenario 2: TCP Stream Performance
```bash
# Monitor long-running TCP ingestion
./monitor_resources.sh ./target/release/hive_parquet_ingest --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp
```

### Scenario 3: S3 Upload Performance
```bash
# Profile with S3 operations
/usr/bin/time -l ./target/release/hive_parquet_ingest \
  --input norway.nmea \
  --source test \
  --s3-bucket test-bucket \
  --s3-region us-east-1
```

## 7. Interpreting Results

### Key Metrics to Watch

**Memory:**
- **RSS (Resident Set Size)**: Physical memory currently used
- **VSZ (Virtual Size)**: Total virtual memory used
- **Peak RSS**: Maximum memory used during execution

**CPU:**
- **User Time**: Time spent in user space
- **System Time**: Time spent in kernel space
- **Real Time**: Wall clock time
- **CPU %**: Percentage of CPU used

**I/O:**
- **Page Faults**: Minor (soft) vs Major (hard)
- **Disk I/O**: Read/write operations
- **Network I/O**: TCP/WebSocket operations

### Performance Baselines

Expected ranges for different workloads:

| Workload | RSS Memory | CPU Usage | Notes |
|----------|------------|-----------|--------|
| Health Check | <10MB | <1% | Minimal overhead |
| File Processing | 50-200MB | 10-50% | Depends on file size |
| TCP Stream | 100-500MB | 20-80% | Depends on data rate |
| S3 Upload | 200MB-1GB | 30-90% | Network dependent |

### Red Flags

- **Memory growth**: RSS continuously increasing (memory leak)
- **High page faults**: Insufficient memory, excessive swapping
- **100% CPU sustained**: Potential infinite loop or inefficient processing
- **High system time**: Excessive system calls or I/O blocking

## 8. Optimization Recommendations

Based on profiling results:

### If Memory Usage is High:
- Reduce `MAX_ROWS` buffer size
- Implement streaming processing
- Optimize Parquet compression settings
- Profile allocations with DHAT

### If CPU Usage is High:
- Profile with flamegraph to find hotspots
- Optimize parsing logic
- Consider parallel processing
- Review compression algorithms

### If I/O is Bottleneck:
- Increase buffer sizes
- Use async I/O more effectively
- Optimize network settings
- Consider batch operations

This comprehensive guide should give you multiple approaches to profile your application's resource usage effectively!