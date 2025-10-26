# Multi-stage build for smaller final image

ARG RUST_VERSION=1.90.0
ARG APP_NAME=hive_parquet_ingest

FROM rust:${RUST_VERSION}-alpine AS builder

WORKDIR /app
RUN apk add --no-cache clang lld musl-dev git

RUN --mount=type=bind,source=src,target=src \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock \
    --mount=type=cache,target=/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
cargo build --locked --release && \
cp ./target/release/hive_parquet_ingest /bin/server

FROM alpine:latest AS final

ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser
USER appuser

# Copy the executable from the "builder" stage.
COPY --from=builder /bin/server /bin/

# What the container should run when it is started.
CMD ["/bin/server"]
