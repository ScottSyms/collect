# Multi-stage build for smaller final image
FROM rust:1.91.1 AS builder

WORKDIR /usr/src/app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates


RUN apt-get update && apt-get install -y musl-tools pkg-config build-essential && rm -rf /var/lib/apt/lists/*

#RUN rustup target add x86_64-unknown-linux-musl
# ENV RUSTFLAGS="-C target-feature=-crt-static"
# ENV OPENSSL_DIR=/usr/lib/x86_64-linux-gnu

# Build the workspace binaries
RUN cargo build --release --workspace

# Runtime stage
FROM debian:latest

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -m -u 1000 appuser

# Copy the binaries from the builder stage
COPY --from=builder /usr/src/app/target/release/collect-file /usr/local/bin/collect-file
COPY --from=builder /usr/src/app/target/release/collect-socket /usr/local/bin/collect-socket
COPY --from=builder /usr/src/app/target/release/collect-maint /usr/local/bin/collect-maint

# Create data directory
RUN mkdir -p /data && chown appuser:appuser /data

# Switch to non-root user
USER appuser

# Set working directory
WORKDIR /data

# Default command (override with docker run arguments)
ENTRYPOINT ["/usr/local/bin/collect-socket"]
