#!/bin/bash
# Real-time resource monitor for hive_parquet_ingest

if [ -z "$1" ]; then
    echo "Usage: $0 <command_and_args>"
    echo "Example: $0 ./target/release/hive_parquet_ingest --health-check"
    echo "Example: $0 ./target/release/hive_parquet_ingest --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp"
    exit 1
fi

echo "Starting resource monitoring for: $@"
echo "========================================="
echo

# Start the command in background
"$@" &
PID=$!

if ! kill -0 $PID 2>/dev/null; then
    echo "Error: Failed to start process"
    exit 1
fi

echo "Process PID: $PID"
echo "Press Ctrl+C to stop monitoring"
echo
printf "%-8s %-8s %-8s %-10s %-10s %-8s\n" "Time" "CPU%" "MEM%" "RSS(MB)" "VSZ(MB)" "Status"
echo "----------------------------------------------------------------"

# Trap Ctrl+C to cleanup
trap 'echo; echo "Stopping monitoring..."; kill $PID 2>/dev/null; exit 0' INT

# Monitor loop
while kill -0 $PID 2>/dev/null; do
    TIMESTAMP=$(date '+%H:%M:%S')
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        STATS=$(ps -p $PID -o pcpu,pmem,rss,vsz 2>/dev/null | tail -1)
        if [ -n "$STATS" ]; then
            CPU=$(echo $STATS | awk '{print $1}')
            MEM=$(echo $STATS | awk '{print $2}')
            RSS_KB=$(echo $STATS | awk '{print $3}')
            VSZ_KB=$(echo $STATS | awk '{print $4}')
            
            RSS_MB=$((RSS_KB / 1024))
            VSZ_MB=$((VSZ_KB / 1024))
            
            printf "%-8s %-8s %-8s %-10s %-10s %-8s\n" "$TIMESTAMP" "${CPU}%" "${MEM}%" "${RSS_MB}" "${VSZ_MB}" "Running"
        fi
    else
        # Linux
        STATS=$(ps -p $PID -o pcpu,pmem,rss,vsz 2>/dev/null | tail -1)
        if [ -n "$STATS" ]; then
            CPU=$(echo $STATS | awk '{print $1}')
            MEM=$(echo $STATS | awk '{print $2}')
            RSS_KB=$(echo $STATS | awk '{print $3}')
            VSZ_KB=$(echo $STATS | awk '{print $4}')
            
            RSS_MB=$((RSS_KB / 1024))
            VSZ_MB=$((VSZ_KB / 1024))
            
            printf "%-8s %-8s %-8s %-10s %-10s %-8s\n" "$TIMESTAMP" "${CPU}%" "${MEM}%" "${RSS_MB}" "${VSZ_MB}" "Running"
        fi
    fi
    
    sleep 2
done

echo
echo "Process completed."