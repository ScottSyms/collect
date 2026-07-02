//! Ingest metrics and the optional HTTP endpoint that serves them.
//!
//! The endpoint is deliberately dependency-free: it answers exactly two
//! GET routes over HTTP/1.1 and closes the connection, which is all a
//! Prometheus scraper or a Nomad `check { type = "http" }` needs.
//!
//! - `GET /metrics` — Prometheus text exposition format
//! - `GET /healthz` — 200 while the ingest loop heartbeat is fresh, 503 once
//!   it goes stale (same staleness window as the health file)

use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Counters and gauges updated by the ingest pipeline. All operations use
/// relaxed ordering: these are statistics, not synchronization.
#[derive(Debug, Default)]
pub struct IngestMetrics {
    pub rows_processed: AtomicU64,
    pub batches_sealed: AtomicU64,
    pub batches_durable: AtomicU64,
    pub buffered_bytes: AtomicU64,
    pub uploads_succeeded: AtomicU64,
    pub uploads_failed: AtomicU64,
    pub upload_retries: AtomicU64,
    pub orphan_files_swept: AtomicU64,
    pub last_row_unix_ms: AtomicU64,
    pub last_heartbeat_unix_ms: AtomicU64,
}

impl IngestMetrics {
    pub fn touch_heartbeat(&self) {
        self.last_heartbeat_unix_ms
            .store(now_unix_ms(), Ordering::Relaxed);
    }

    pub fn touch_last_row(&self) {
        self.last_row_unix_ms
            .store(now_unix_ms(), Ordering::Relaxed);
    }

    /// Healthy while the ingest loop's heartbeat is fresh — the loop ticks it
    /// every second, so staleness means the loop is stuck or gone.
    fn is_healthy(&self) -> bool {
        let last = self.last_heartbeat_unix_ms.load(Ordering::Relaxed);
        last > 0
            && now_unix_ms().saturating_sub(last)
                < super::DEFAULT_HEALTH_STALE_WINDOW_SECONDS * 1000
    }

    fn render_prometheus(&self, source: &str) -> String {
        let label = format!("{{source=\"{}\"}}", source.replace(['\\', '"'], ""));
        let mut out = String::with_capacity(2048);
        let mut metric = |name: &str, kind: &str, help: &str, value: u64| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} {kind}\n{name}{label} {value}\n"
            ));
        };

        metric(
            "collect_rows_processed_total",
            "counter",
            "Rows ingested since process start",
            self.rows_processed.load(Ordering::Relaxed),
        );
        metric(
            "collect_batches_sealed_total",
            "counter",
            "Batches sealed and queued for parquet writing",
            self.batches_sealed.load(Ordering::Relaxed),
        );
        metric(
            "collect_batches_durable_total",
            "counter",
            "Batches durably written to local disk",
            self.batches_durable.load(Ordering::Relaxed),
        );
        metric(
            "collect_buffered_bytes",
            "gauge",
            "Payload bytes currently buffered in the open batch",
            self.buffered_bytes.load(Ordering::Relaxed),
        );
        metric(
            "collect_uploads_succeeded_total",
            "counter",
            "S3 uploads completed successfully",
            self.uploads_succeeded.load(Ordering::Relaxed),
        );
        metric(
            "collect_uploads_failed_total",
            "counter",
            "S3 uploads abandoned after exhausting retries",
            self.uploads_failed.load(Ordering::Relaxed),
        );
        metric(
            "collect_upload_retries_total",
            "counter",
            "S3 upload attempts that failed and were retried",
            self.upload_retries.load(Ordering::Relaxed),
        );
        metric(
            "collect_orphan_files_swept_total",
            "counter",
            "Orphaned parquet files from previous runs queued for upload",
            self.orphan_files_swept.load(Ordering::Relaxed),
        );
        metric(
            "collect_last_row_unix_ms",
            "gauge",
            "Unix timestamp (ms) of the most recently ingested row",
            self.last_row_unix_ms.load(Ordering::Relaxed),
        );
        metric(
            "collect_last_heartbeat_unix_ms",
            "gauge",
            "Unix timestamp (ms) of the ingest loop's last heartbeat",
            self.last_heartbeat_unix_ms.load(Ordering::Relaxed),
        );
        out
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Bind `addr` and serve `/metrics` and `/healthz` until the returned task is
/// aborted. Returns the bound address so callers (and tests) can use port 0.
pub(crate) async fn spawn_metrics_server(
    addr: &str,
    source: String,
    metrics: Arc<IngestMetrics>,
) -> Result<(tokio::task::JoinHandle<()>, SocketAddr)> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding metrics endpoint {addr}"))?;
    let local_addr = listener.local_addr().context("metrics endpoint address")?;

    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    let source = source.clone();
                    let metrics = metrics.clone();
                    tokio::spawn(async move {
                        handle_connection(socket, &source, &metrics).await;
                    });
                }
                Err(error) => {
                    eprintln!("Metrics endpoint accept failed: {error}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    });

    Ok((handle, local_addr))
}

async fn handle_connection(mut socket: TcpStream, source: &str, metrics: &IngestMetrics) {
    let mut buf = [0u8; 1024];
    let read = match socket.read(&mut buf).await {
        Ok(read) if read > 0 => read,
        _ => return,
    };

    let request = String::from_utf8_lossy(&buf[..read]);
    let path = request
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/");

    let (status_line, content_type, body) = match path {
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            metrics.render_prometheus(source),
        ),
        "/healthz" | "/health" => {
            if metrics.is_healthy() {
                ("200 OK", "text/plain; charset=utf-8", "healthy\n".to_string())
            } else {
                (
                    "503 Service Unavailable",
                    "text/plain; charset=utf-8",
                    "unhealthy\n".to_string(),
                )
            }
        }
        _ => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".to_string(),
        ),
    };

    let response = format!(
        "HTTP/1.1 {status_line}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn http_get(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes())
            .await
            .expect("send request");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("read response");
        response
    }

    #[tokio::test]
    async fn serves_metrics_and_health() {
        let metrics = Arc::new(IngestMetrics::default());
        metrics.rows_processed.store(42, Ordering::Relaxed);
        metrics.touch_heartbeat();

        let (handle, addr) =
            spawn_metrics_server("127.0.0.1:0", "test-source".to_string(), metrics.clone())
                .await
                .expect("bind metrics server");

        let body = http_get(addr, "/metrics").await;
        assert!(body.starts_with("HTTP/1.1 200 OK"), "{body}");
        assert!(
            body.contains("collect_rows_processed_total{source=\"test-source\"} 42"),
            "{body}"
        );

        let health = http_get(addr, "/healthz").await;
        assert!(health.starts_with("HTTP/1.1 200 OK"), "{health}");

        // A stale heartbeat flips /healthz to 503.
        metrics.last_heartbeat_unix_ms.store(1, Ordering::Relaxed);
        let unhealthy = http_get(addr, "/healthz").await;
        assert!(
            unhealthy.starts_with("HTTP/1.1 503"),
            "{unhealthy}"
        );

        let missing = http_get(addr, "/nope").await;
        assert!(missing.starts_with("HTTP/1.1 404"), "{missing}");

        handle.abort();
    }
}
