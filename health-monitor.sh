#!/bin/bash

# Simple health monitoring script for the data ingest application
# This can be run on the host or in a separate monitoring container

APP_NAME="data-ingest"
LOGFILE="/var/log/health-monitor.log"

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOGFILE"
}

check_container_health() {
    # Check Docker container health status
    local health_status=$(docker inspect --format='{{.State.Health.Status}}' "$APP_NAME" 2>/dev/null)
    
    case "$health_status" in
        "healthy")
            log_message "✅ Container $APP_NAME is healthy"
            return 0
            ;;
        "unhealthy")
            log_message "❌ Container $APP_NAME is unhealthy"
            return 1
            ;;
        "starting")
            log_message "🔄 Container $APP_NAME is starting up"
            return 2
            ;;
        "")
            log_message "❓ Container $APP_NAME not found or no health check configured"
            return 3
            ;;
        *)
            log_message "❓ Container $APP_NAME has unknown health status: $health_status"
            return 4
            ;;
    esac
}

check_output_directory() {
    # Check if output directory has recent files (data is being written)
    local output_dir="./output"
    local recent_files=$(find "$output_dir" -name "*.parquet" -mmin -5 2>/dev/null | wc -l)
    
    if [ "$recent_files" -gt 0 ]; then
        log_message "✅ Found $recent_files recent parquet files in output directory"
        return 0
    else
        log_message "⚠️  No recent parquet files found in output directory"
        return 1
    fi
}

restart_container() {
    log_message "🔄 Attempting to restart container $APP_NAME"
    docker-compose restart "$APP_NAME"
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        log_message "✅ Container $APP_NAME restarted successfully"
    else
        log_message "❌ Failed to restart container $APP_NAME"
    fi
    
    return $exit_code
}

main() {
    log_message "Starting health check for $APP_NAME"
    
    check_container_health
    local container_health=$?
    
    check_output_directory
    local output_health=$?
    
    # If container is unhealthy, try to restart it
    if [ $container_health -eq 1 ]; then
        restart_container
    fi
    
    # Exit with appropriate code for monitoring systems
    if [ $container_health -eq 0 ] && [ $output_health -eq 0 ]; then
        log_message "✅ All health checks passed"
        exit 0
    else
        log_message "❌ Some health checks failed"
        exit 1
    fi
}

# Run main function
main "$@"