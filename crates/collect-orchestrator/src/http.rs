use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{extract_source_from_key, parser_for_source};
use crate::db;

#[derive(Clone)]
pub struct AppState {
    pub pool: sqlx::PgPool,
    pub source_map: Arc<HashMap<String, String>>,
    pub ingest_token: Option<String>,
}

#[derive(Deserialize)]
pub struct FlatIngest {
    pub s3_bucket: Option<String>,
    pub s3_key: Option<String>,
    pub source: Option<String>,
    pub parser: Option<String>,
}

#[derive(Serialize)]
pub struct IngestResp {
    pub accepted: usize,
    pub duplicates: usize,
}

fn extract_keys_from_value(v: &Value) -> Vec<(String, String)> {
    let mut out = Vec::new();
    // AWS S3 event shape: { "Records": [ { "eventName":"s3:ObjectCreated:*", "s3": { "bucket": {"name":"b"}, "object": {"key":"k"} } } ] }
    if let Some(records) = v.get("Records").and_then(|r| r.as_array()) {
        for rec in records {
            if let Some(s3) = rec.get("s3") {
                let bucket = s3.get("bucket").and_then(|b| b.get("name")).and_then(|n| n.as_str()).unwrap_or("");
                let key = s3.get("object").and_then(|o| o.get("key")).and_then(|k| k.as_str()).unwrap_or("");
                if !bucket.is_empty() && !key.is_empty() {
                    // S3 keys are URL-encoded
                    let decoded = urlencoding::decode(key).map(|s| s.into_owned()).unwrap_or_else(|_| key.to_string());
                    out.push((bucket.to_string(), decoded));
                }
            }
        }
        if !out.is_empty() {
            return out;
        }
    }
    // Flat single
    if let Ok(flat) = serde_json::from_value::<FlatIngest>(v.clone()) {
        if let (Some(b), Some(k)) = (flat.s3_bucket, flat.s3_key) {
            out.push((b, k));
            return out;
        }
    }
    // Array of flats
    if let Some(arr) = v.as_array() {
        for item in arr {
            if let Ok(flat) = serde_json::from_value::<FlatIngest>(item.clone()) {
                if let (Some(b), Some(k)) = (flat.s3_bucket, flat.s3_key) {
                    out.push((b,k));
                }
            }
        }
    }
    out
}

pub async fn ingest_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    if let Some(token) = &state.ingest_token {
        let provided = headers.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("");
        // Accept "Bearer <token>" or bare token
        let ok = provided == token || provided == format!("Bearer {token}");
        if !ok {
            return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error":"unauthorized"}))).into_response();
        }
    }
    let pairs = extract_keys_from_value(&body);
    if pairs.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error":"no s3_bucket/s3_key or Records found"}))).into_response();
    }
    let mut accepted = 0usize;
    let mut duplicates = 0usize;
    for (bucket, key) in pairs {
        if !key.ends_with(".parquet") {
            continue;
        }
        let source = extract_source_from_key(&key).unwrap_or_else(|| "unknown".to_string());
        let parser = parser_for_source(&source, &state.source_map).to_string();
        match db::enqueue(&state.pool, &bucket, &key, &source, &parser).await {
            Ok(true) => accepted += 1,
            Ok(false) => duplicates += 1,
            Err(e) => {
                eprintln!("enqueue error {bucket}/{key}: {e:#}");
                continue;
            }
        }
    }
    (StatusCode::OK, Json(serde_json::json!(IngestResp{accepted, duplicates}))).into_response()
}

pub async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    match sqlx::query("SELECT 1").execute(&state.pool).await {
        Ok(_) => (StatusCode::OK, "ok"),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, Box::leak(format!("db unavailable: {e}").into_boxed_str()) as &str),
    }
}

pub async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let depths = db::queue_depth(&state.pool).await.unwrap_or_default();
    let mut out = String::new();
    for (status, count) in depths {
        out.push_str(&format!("orchestrator_queue_depth{{status=\"{status}\"}} {count}\n"));
    }
    // Add basic process metrics placeholder
    (StatusCode::OK, out)
}

pub async fn queue_handler(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let status = params.get("status").cloned().unwrap_or_else(|| "pending".to_string());
    let limit: i64 = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(100);
    let rows = sqlx::query_as::<_, db::QueueRow>(
        "SELECT * FROM parse_queue WHERE status=$1 ORDER BY created_at LIMIT $2",
    )
    .bind(&status)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();
    Json(rows)
}
