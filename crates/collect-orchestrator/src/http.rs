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
    pub callback_token: Option<String>,
}

#[derive(Deserialize)]
pub struct CompletePayload {
    pub s3_bucket: Option<String>,
    pub s3_key: String,
    pub duration_ms: Option<i64>,
    pub stats: Option<WorkerStats>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct WorkerStats {
    pub rows_in: Option<i64>,
    pub positions_out: Option<i64>,
    pub statics_out: Option<i64>,
    pub meteo_out: Option<i64>,
    pub binary_out: Option<i64>,
    pub atons_out: Option<i64>,
    pub other_out: Option<i64>,
    pub incomplete: Option<i64>,
    pub unparsed: Option<i64>,
    pub deduped: Option<i64>,
}

#[derive(Deserialize)]
pub struct FailPayload {
    pub s3_bucket: Option<String>,
    pub s3_key: String,
    pub error: Option<String>,
}

fn check_auth(state: &AppState, headers: &axum::http::HeaderMap, for_callback: bool) -> bool {
    let token = if for_callback {
        state.callback_token.as_ref().or(state.ingest_token.as_ref())
    } else {
        state.ingest_token.as_ref()
    };
    let Some(token) = token else { return true; };
    let provided = headers.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("");
    provided == token || provided == format!("Bearer {token}")
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
    if !check_auth(&state, &headers, false) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error":"unauthorized"}))).into_response();
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

pub async fn complete_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<CompletePayload>,
) -> impl IntoResponse {
    if !check_auth(&state, &headers, true) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error":"unauthorized"}))).into_response();
    }
    let s3_key = body.s3_key.clone();
    // Idempotent: if already in history, succeed
    let in_history: Option<(String,)> = sqlx::query_as("SELECT s3_key FROM parse_history WHERE s3_key=$1")
        .bind(&s3_key)
        .fetch_optional(&state.pool)
        .await
        .unwrap_or(None);
    if in_history.is_some() {
        return (StatusCode::OK, Json(serde_json::json!({"status":"already_complete"}))).into_response();
    }
    let row: Option<db::QueueRow> = sqlx::query_as::<_, db::QueueRow>("SELECT * FROM parse_queue WHERE s3_key=$1")
        .bind(&s3_key)
        .fetch_optional(&state.pool)
        .await
        .unwrap_or(None);
    let Some(row) = row else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error":"not found in queue"}))).into_response();
    };
    let stats_in = body.stats.clone().unwrap_or(WorkerStats { rows_in: None, positions_out: None, statics_out: None, meteo_out: None, binary_out: None, atons_out: None, other_out: None, incomplete: None, unparsed: None, deduped: None });
    let stats = db::ArchiveStats {
        rows_in: stats_in.rows_in.unwrap_or(0),
        positions_out: stats_in.positions_out.unwrap_or(0),
        statics_out: stats_in.statics_out.unwrap_or(0),
        meteo_out: stats_in.meteo_out.unwrap_or(0),
        binary_out: stats_in.binary_out.unwrap_or(0),
        atons_out: stats_in.atons_out.unwrap_or(0),
        other_out: stats_in.other_out.unwrap_or(0),
        incomplete: stats_in.incomplete.unwrap_or(0),
        unparsed: stats_in.unparsed.unwrap_or(0),
        deduped: stats_in.deduped.unwrap_or(0),
    };
    let duration_ms = body.duration_ms.unwrap_or(0);
    match db::archive_success(&state.pool, &row, duration_ms, &stats).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"status":"archived"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": format!("{e:#}")}))).into_response(),
    }
}

pub async fn fail_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<FailPayload>,
) -> impl IntoResponse {
    if !check_auth(&state, &headers, true) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error":"unauthorized"}))).into_response();
    }
    let s3_key = body.s3_key.clone();
    let row: Option<db::QueueRow> = sqlx::query_as::<_, db::QueueRow>("SELECT * FROM parse_queue WHERE s3_key=$1")
        .bind(&s3_key)
        .fetch_optional(&state.pool)
        .await
        .unwrap_or(None);
    let Some(row) = row else {
        // Check history for idempotency: already succeeded, don't re-fail
        let in_history: Option<(String,)> = sqlx::query_as("SELECT s3_key FROM parse_history WHERE s3_key=$1")
            .bind(&s3_key)
            .fetch_optional(&state.pool)
            .await
            .unwrap_or(None);
        if in_history.is_some() {
            return (StatusCode::OK, Json(serde_json::json!({"status":"already_complete"}))).into_response();
        }
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error":"not found in queue"}))).into_response();
    };
    let msg = body.error.clone().unwrap_or_else(|| "unknown error".to_string());
    match db::mark_failed(&state.pool, &s3_key, &msg, row.max_attempts, row.attempts).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"status":"marked_failed"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": format!("{e:#}")}))).into_response(),
    }
}
