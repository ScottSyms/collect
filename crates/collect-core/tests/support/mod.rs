//! Shared helper for integration tests that need a real local RustFS
//! instance — a self-contained S3-compatible store with its own built-in
//! Iceberg REST catalog, so a test can exercise both without Lakekeeper or
//! Postgres.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

pub struct Rustfs {
    child: Child,
    port: u16,
    pub access_key: String,
    pub secret_key: String,
    // Held for its lifetime only — RustFS's data directory, removed on drop.
    _data_dir: tempfile::TempDir,
}

impl Rustfs {
    /// Spawns `rustfs server` on a free localhost port with fixed test
    /// credentials, and waits for it to answer `/health`. Returns `None`
    /// (never panics) when `rustfs` isn't on `PATH` or never becomes
    /// healthy, so callers can skip cleanly instead of failing a suite run
    /// on a machine without it installed.
    pub async fn spawn() -> Option<Self> {
        if !has_rustfs() {
            return None;
        }

        let port = pick_free_port()?;
        let data_dir = tempfile::tempdir().ok()?;
        let access_key = "collect-test-access".to_string();
        let secret_key = "collect-test-secret".to_string();

        let child = Command::new("rustfs")
            .arg("server")
            .arg("--address")
            .arg(format!("127.0.0.1:{port}"))
            .arg(data_dir.path())
            .env("RUSTFS_ACCESS_KEY", &access_key)
            .env("RUSTFS_SECRET_KEY", &secret_key)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;

        let mut rustfs = Rustfs {
            child,
            port,
            access_key,
            secret_key,
            _data_dir: data_dir,
        };

        if rustfs.wait_healthy().await {
            Some(rustfs)
        } else {
            None
        }
    }

    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// RustFS serves its own Iceberg REST catalog directly, with no
    /// separate Lakekeeper/Postgres — this is the URI `--iceberg-catalog-uri`
    /// points at, signed the same way as S3 itself via `--iceberg-sigv4`.
    /// Unused by this crate's own S3-only test; kept for parity with the
    /// copy of this module in `ais-parse`/`aisstream-parse`'s test suites.
    #[allow(dead_code)]
    pub fn iceberg_catalog_uri(&self) -> String {
        format!("{}/iceberg", self.endpoint())
    }

    /// RustFS requires a bucket to be explicitly "table-enabled" before it
    /// can serve as an Iceberg warehouse — undocumented anywhere we could
    /// find except the web console's own "Enable this bucket" action;
    /// reverse-engineered from the server binary's embedded route table as
    /// `PUT /iceberg/v1/buckets/<bucket>`, SigV4-signed the same way as any
    /// other catalog request. One-time per bucket. Shells out to `curl`
    /// rather than reimplementing SigV4 signing here; returns `false`
    /// (never panics) if `curl` is missing or the call fails, so callers
    /// can skip cleanly.
    #[allow(dead_code)]
    pub fn enable_table_bucket(&self, bucket: &str) -> bool {
        Command::new("curl")
            .arg("-sf")
            .arg("-X")
            .arg("PUT")
            .arg(format!("{}/iceberg/v1/buckets/{bucket}", self.endpoint()))
            .arg("--user")
            .arg(format!("{}:{}", self.access_key, self.secret_key))
            .arg("--aws-sigv4")
            .arg("aws:amz:us-east-1:s3")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    async fn wait_healthy(&mut self) -> bool {
        for _ in 0..100 {
            if raw_http_get_ok(self.port, "/health") {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }
}

/// A bare-minimum blocking HTTP/1.1 GET, so this module needs no HTTP
/// client dependency — just enough to poll a health endpoint.
fn raw_http_get_ok(port: u16, path: &str) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    if stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n").as_bytes())
        .is_err()
    {
        return false;
    }
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    response.starts_with("HTTP/1.1 2") || response.starts_with("HTTP/1.0 2")
}

impl Drop for Rustfs {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn has_rustfs() -> bool {
    Command::new("rustfs")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn pick_free_port() -> Option<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").ok()?;
    listener.local_addr().ok().map(|addr| addr.port())
}
