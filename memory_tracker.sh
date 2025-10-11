#!/bin/bash
# Memory growth tracker with CSV output

if [ -z "$1" ]; then
    echo "Usage: $0 <command_and_args>"
    echo "Example: $0 ./target/release/hive_parquet_ingest --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp"
    exit 1
fi

LOG_FILE="memory_usage_$(date +%Y%m%d_%H%M%S).csv"

echo "Memory usage tracking"
echo "Output file: $LOG_FILE"
echo "Command: $@"
echo "===================="

# Create CSV header
echo "Timestamp,Elapsed_Seconds,CPU_Percent,Memory_Percent,RSS_MB,VSZ_MB,Page_Faults" > "$LOG_FILE"

# Start command in background
"$@" &
PID=$!
START_TIME=$(date +%s)

if ! kill -0 $PID 2>/dev/null; then
    echo "Error: Failed to start process"
    exit 1
fi

echo "Process PID: $PID"
echo "Monitoring memory usage every second..."
echo "Press Ctrl+C to stop and view summary"
echo

# Variables for tracking
MAX_RSS=0
MAX_CPU=0
TOTAL_SAMPLES=0

# Trap Ctrl+C for cleanup and summary
trap 'cleanup_and_summary' INT

cleanup_and_summary() {
    echo
    echo "Stopping monitoring and generating summary..."
    kill $PID 2>/dev/null
    
    echo "===================="
    echo "SUMMARY"
    echo "===================="
    echo "Log file: $LOG_FILE"
    echo "Total samples: $TOTAL_SAMPLES"
    echo "Peak RSS memory: ${MAX_RSS}MB"
    echo "Peak CPU usage: ${MAX_CPU}%"
    echo
    
    if [ $TOTAL_SAMPLES -gt 0 ]; then
        echo "Memory usage over time (last 10 samples):"
        tail -10 "$LOG_FILE" | column -t -s,
        echo
        
        # Generate simple statistics
        echo "Statistics:"
        tail -n +2 "$LOG_FILE" | awk -F, '
        BEGIN { sum_rss=0; sum_cpu=0; count=0; min_rss=999999; max_rss=0 }
        { 
            sum_rss+=$5; sum_cpu+=$3; count++
            if($5 < min_rss) min_rss=$5
            if($5 > max_rss) max_rss=$5
        }
        END { 
            if(count > 0) {
                printf "Average RSS: %.1fMB\n", sum_rss/count
                printf "Average CPU: %.1f%%\n", sum_cpu/count  
                printf "Min RSS: %dMB\n", min_rss
                printf "Max RSS: %dMB\n", max_rss
            }
        }'
    fi
    
    echo
    echo "To analyze further, you can:"
    echo "  - Import $LOG_FILE into Excel/Numbers"
    echo "  - Plot with: python3 -c \"import pandas as pd; import matplotlib.pyplot as plt; df=pd.read_csv('$LOG_FILE'); df.plot(x='Elapsed_Seconds', y=['RSS_MB','CPU_Percent'], subplots=True); plt.show()\""
    
    exit 0
}

# Monitor loop
while kill -0 $PID 2>/dev/null; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
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
            
            # Get page faults (approximation on macOS)
            PAGE_FAULTS=$(ps -p $PID -o majflt 2>/dev/null | tail -1 | tr -d ' ')
            PAGE_FAULTS=${PAGE_FAULTS:-0}
        fi
    else
        # Linux  
        STATS=$(ps -p $PID -o pcpu,pmem,rss,vsz,majflt 2>/dev/null | tail -1)
        if [ -n "$STATS" ]; then
            CPU=$(echo $STATS | awk '{print $1}')
            MEM=$(echo $STATS | awk '{print $2}')
            RSS_KB=$(echo $STATS | awk '{print $3}')
            VSZ_KB=$(echo $STATS | awk '{print $4}')
            PAGE_FAULTS=$(echo $STATS | awk '{print $5}')
            
            RSS_MB=$((RSS_KB / 1024))
            VSZ_MB=$((VSZ_KB / 1024))
        fi
    fi
    
    if [ -n "$STATS" ]; then
        # Update maximums
        if (( $(echo "$CPU > $MAX_CPU" | bc -l 2>/dev/null || echo "0") )); then
            MAX_CPU=$CPU
        fi
        if [ $RSS_MB -gt $MAX_RSS ]; then
            MAX_RSS=$RSS_MB
        fi
        
        # Log to CSV
        echo "$TIMESTAMP,$ELAPSED,$CPU,$MEM,$RSS_MB,$VSZ_MB,$PAGE_FAULTS" >> "$LOG_FILE"
        
        # Display current stats
        printf "\r[%s] RSS: %4dMB | CPU: %5s%% | Elapsed: %4ds | Samples: %d" \
               "$TIMESTAMP" "$RSS_MB" "$CPU" "$ELAPSED" "$((++TOTAL_SAMPLES))"
    fi
    
    sleep 1
done

cleanup_and_summary