//! SigV4 signing for Iceberg REST catalogs that require AWS-style request
//! signatures (RustFS S3 Tables, AWS S3 Tables / Glue).
//!
//! `iceberg-catalog-rest` only supports bearer/OAuth authentication and offers
//! no per-request hook, so the catalog is pointed at a loopback proxy that
//! signs each request and forwards it to the real catalog. The proxy lives for
//! the rest of the process; one is started per upstream.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use anyhow::{anyhow, Context, Result};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest,
    SigningSettings, UriPathNormalizationMode,
};
use aws_sigv4::sign::v4;
use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{header, HeaderName, HeaderValue, Request, Response, StatusCode};
use axum::Router;

/// Largest request body the proxy will buffer (catalog commits are small).
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// SigV4 service name RustFS expects (AWS S3 Tables uses `s3tables`).
const SIGNING_NAME: &str = "s3";

/// Request headers that must not be copied to the upstream request: either
/// hop-by-hop, or recomputed by the signer / HTTP client.
const SKIPPED_HEADERS: &[HeaderName] = &[
    header::HOST,
    header::CONNECTION,
    header::CONTENT_LENGTH,
    header::TRANSFER_ENCODING,
    header::ACCEPT_ENCODING,
    header::AUTHORIZATION,
];

#[derive(Clone)]
struct ProxyState {
    upstream: String,
    credentials: Credentials,
    region: String,
    client: reqwest::Client,
}

/// Credentials used to sign catalog requests.
pub struct SigV4Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

impl SigV4Credentials {
    /// Read `S3_ACCESS_KEY` / `S3_SECRET_KEY` (or the AWS-standard names) and
    /// `S3_REGION` from the environment — the same variables `open_catalog`
    /// already uses for the data files.
    pub fn from_env() -> Result<Self> {
        let first = |vars: &[&str]| vars.iter().find_map(|v| std::env::var(v).ok());
        Ok(Self {
            access_key: first(&["S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"])
                .context("--iceberg-sigv4 needs S3_ACCESS_KEY (or AWS_ACCESS_KEY_ID)")?,
            secret_key: first(&["S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"])
                .context("--iceberg-sigv4 needs S3_SECRET_KEY (or AWS_SECRET_ACCESS_KEY)")?,
            region: first(&["S3_REGION", "AWS_REGION"]).unwrap_or_else(|| "us-east-1".into()),
        })
    }
}

/// Return the URI the catalog should use so its requests get signed.
///
/// `catalog_uri` is the real catalog (e.g. `http://localhost:9000/iceberg`);
/// the result is `http://127.0.0.1:<port>/iceberg`. Proxies are cached per
/// upstream, so repeated calls in one process reuse the same listener.
pub async fn signed_catalog_uri(catalog_uri: &str, creds: SigV4Credentials) -> Result<String> {
    static PROXIES: OnceLock<Mutex<HashMap<String, SocketAddr>>> = OnceLock::new();
    let proxies = PROXIES.get_or_init(Default::default);

    let (origin, path) = split_origin(catalog_uri)?;
    let cached = proxies.lock().unwrap().get(origin).copied();
    let addr = match cached {
        Some(addr) => addr,
        None => {
            let addr = spawn_proxy(origin, creds).await?;
            proxies.lock().unwrap().insert(origin.to_string(), addr);
            addr
        }
    };
    Ok(format!("http://{addr}{path}"))
}

/// Split `scheme://authority/path` into (`scheme://authority`, `/path`).
fn split_origin(uri: &str) -> Result<(&str, &str)> {
    let after_scheme = uri
        .find("://")
        .map(|i| i + 3)
        .ok_or_else(|| anyhow!("Iceberg catalog URI must include a scheme: {uri}"))?;
    let end = uri[after_scheme..]
        .find('/')
        .map_or(uri.len(), |i| after_scheme + i);
    Ok((&uri[..end], uri[end..].trim_end_matches('/')))
}

async fn spawn_proxy(upstream: &str, creds: SigV4Credentials) -> Result<SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("binding Iceberg SigV4 signing proxy")?;
    let addr = listener.local_addr()?;
    let state = Arc::new(ProxyState {
        upstream: upstream.to_string(),
        credentials: Credentials::new(
            creds.access_key,
            creds.secret_key,
            None,
            None,
            "collect-iceberg-sigv4",
        ),
        region: creds.region,
        client: reqwest::Client::new(),
    });
    let app = Router::new().fallback(forward).with_state(state);
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            eprintln!("Iceberg SigV4 signing proxy stopped: {err}");
        }
    });
    Ok(addr)
}

async fn forward(State(state): State<Arc<ProxyState>>, req: Request<Body>) -> Response<Body> {
    match sign_and_forward(&state, req).await {
        Ok(resp) => resp,
        Err(err) => Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Body::from(format!("SigV4 signing proxy error: {err:#}")))
            .expect("static response"),
    }
}

async fn sign_and_forward(state: &ProxyState, req: Request<Body>) -> Result<Response<Body>> {
    let (parts, body) = req.into_parts();
    let body = to_bytes(body, MAX_BODY_BYTES)
        .await
        .context("reading catalog request body")?;
    let path_and_query = parts.uri.path_and_query().map_or("/", |p| p.as_str());
    let url = format!("{}{}", state.upstream, path_and_query);

    let mut headers: Vec<(String, String)> = parts
        .headers
        .iter()
        .filter(|(name, _)| !SKIPPED_HEADERS.contains(*name))
        .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_string())))
        .collect();

    // Mirror S3's signing rules: single percent-encoding, no path
    // normalisation, and an explicit x-amz-content-sha256 header (RustFS
    // rejects requests without it).
    let mut settings = SigningSettings::default();
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;

    let identity = state.credentials.clone().into();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(&state.region)
        .name(SIGNING_NAME)
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .context("building SigV4 signing params")?
        .into();

    let signable = SignableRequest::new(
        parts.method.as_str(),
        url.as_str(),
        headers.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        SignableBody::Bytes(&body),
    )
    .context("building signable request")?;
    let (instructions, _signature) = sign(signable, &params)
        .context("signing catalog request")?
        .into_parts();
    for (name, value) in instructions.headers() {
        headers.retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
        headers.push((name.to_string(), value.to_string()));
    }

    let mut upstream_req = state.client.request(parts.method, &url).body(body);
    for (name, value) in &headers {
        upstream_req = upstream_req.header(name, value);
    }
    let upstream_resp = upstream_req
        .send()
        .await
        .with_context(|| format!("forwarding to {url}"))?;

    let mut resp = Response::builder().status(upstream_resp.status());
    for (name, value) in upstream_resp.headers() {
        if name != header::TRANSFER_ENCODING && name != header::CONNECTION {
            if let Ok(value) = HeaderValue::from_bytes(value.as_bytes()) {
                resp = resp.header(name.as_str(), value);
            }
        }
    }
    let bytes = upstream_resp
        .bytes()
        .await
        .context("reading upstream catalog response")?;
    Ok(resp.body(Body::from(bytes))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_origin_keeps_path() {
        assert_eq!(
            split_origin("http://localhost:9000/iceberg").unwrap(),
            ("http://localhost:9000", "/iceberg")
        );
        assert_eq!(
            split_origin("https://host/a/b/").unwrap(),
            ("https://host", "/a/b")
        );
        assert_eq!(
            split_origin("http://host:9000").unwrap(),
            ("http://host:9000", "")
        );
        assert!(split_origin("localhost:9000").is_err());
    }
}
