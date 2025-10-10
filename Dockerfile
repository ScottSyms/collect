# Multi-stage build for smaller final image
FROM rust:1.75 AS builder

WORKDIR /usr/src/app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -m -u 1000 appuser

# Copy the binary from builder stage
COPY --from=builder /usr/src/app/target/release/hive_parquet_ingest /usr/local/bin/hive_parquet_ingest

# Create data directory
RUN mkdir -p /data && chown appuser:appuser /data

# Switch to non-root user
USER appuser

# Set working directory
WORKDIR /data

# Add health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /usr/local/bin/hive_parquet_ingest --health-check

# Default command (override with docker run arguments)
ENTRYPOINT ["/usr/local/bin/hive_parquet_ingest"]