# Multi-stage build for smaller final image
FROM rust:1.91.1 AS builder

WORKDIR /usr/src/app
COPY Cargo.toml Cargo.lock ./
COPY src ./src


RUN apt-get update && apt-get install -y musl-tools pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*

#RUN rustup target add x86_64-unknown-linux-musl
# ENV RUSTFLAGS="-C target-feature=-crt-static"
# ENV OPENSSL_DIR=/usr/lib/x86_64-linux-gnu

# Build the application
RUN cargo build --release # --target=x86_64-unknown-linux-musl

# Runtime stage
FROM debian:latest

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates iputils-ping openssl \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -m -u 1000 appuser

# Copy the binary from builder stage
COPY --from=builder /usr/src/app/target/release/capture /usr/local/bin/capture

# Create data directory
RUN mkdir -p /data && chown appuser:appuser /data

# Switch to non-root user
USER appuser

# Set working directory
WORKDIR /data

# Add health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /usr/local/bin/capture --health-check

# Default command (override with docker run arguments)
ENTRYPOINT ["/usr/local/bin/capture"]
