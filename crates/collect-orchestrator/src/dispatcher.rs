use anyhow::{Context, Result};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

use crate::db;

#[derive(Clone, Debug)]
pub struct DispatcherConfig {
    pub nomad_addr: String,
    pub nomad_token: Option<String>,
    pub nomad_job: String,
    pub dispatch_concurrency: usize,
    pub poll_interval_ms: u64,
    pub reclaim_secs: i64,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            nomad_addr: "http://nomad.service.consul:4646".to_string(),
            nomad_token: None,
            nomad_job: "parse-file".to_string(),
            dispatch_concurrency: 32,
            poll_interval_ms: 500,
            reclaim_secs: 1800,
        }
    }
}

pub async fn run_dispatcher_loop(pool: PgPool, cfg: DispatcherConfig) {
    let sem = Arc::new(Semaphore::new(cfg.dispatch_concurrency.max(1)));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let client = Arc::new(client);

    loop {
        let _ = db::reclaim_stale(&pool, 600).await;
        let _ = db::reclaim_stale_dispatched(&pool, cfg.reclaim_secs).await;

        let row = match db::fetch_pending_for_dispatch(&pool).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("dispatcher fetch error: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        let Some(row) = row else {
            tokio::time::sleep(Duration::from_millis(cfg.poll_interval_ms.max(200))).await;
            continue;
        };

        // Acquire permit for Nomad dispatch concurrency
        let permit = match sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break,
        };
        let pool2 = pool.clone();
        let cfg2 = cfg.clone();
        let client2 = client.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let s3_key = row.s3_key.clone();
            match dispatch_one(&client2, &cfg2, &row).await {
                Ok(job_id) => {
                    if let Err(e) = db::mark_dispatched(&pool2, &s3_key, &job_id).await {
                        eprintln!("mark_dispatched {s3_key} failed: {e:#}");
                    } else {
                        eprintln!("dispatched {s3_key} -> Nomad job {job_id}");
                    }
                }
                Err(e) => {
                    eprintln!("dispatch failed for {s3_key}: {e:#}; resetting to pending");
                    // Reset to pending so next attempt can retry; also record error via mark_failed backoff?
                    // For dispatch transport failures we reset to pending (no backoff) but increment already happened.
                    // Convert to failed with error so backoff applies.
                    let _ = db::mark_failed(
                        &pool2,
                        &s3_key,
                        &format!("nomad dispatch failed: {e:#}"),
                        row.max_attempts,
                        row.attempts,
                    )
                    .await;
                }
            }
        });
    }
}

async fn dispatch_one(
    client: &reqwest::Client,
    cfg: &DispatcherConfig,
    row: &db::QueueRow,
) -> Result<String> {
    let url = format!(
        "{}/v1/job/{}/dispatch",
        cfg.nomad_addr.trim_end_matches('/'),
        cfg.nomad_job
    );
    let payload = serde_json::json!({
        "Meta": {
            "s3_bucket": row.s3_bucket,
            "s3_key": row.s3_key,
            "source": row.source,
            "parser": row.parser,
        }
    });
    let mut req = client.post(&url).json(&payload);
    if let Some(tok) = &cfg.nomad_token {
        req = req.header("X-Nomad-Token", tok);
    }
    let resp = req.send().await.context("sending dispatch to Nomad")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Nomad dispatch {status}: {body}");
    }
    let v: serde_json::Value = resp.json().await.context("parsing Nomad dispatch response")?;
    let job_id = v
        .get("DispatchedJobID")
        .and_then(|x| x.as_str())
        .or_else(|| v.get("dispatched_job_id").and_then(|x| x.as_str()))
        .unwrap_or("")
        .to_string();
    if job_id.is_empty() {
        // Fallback: use EvalID
        let eval = v.get("EvalID").and_then(|x| x.as_str()).unwrap_or("unknown");
        Ok(eval.to_string())
    } else {
        Ok(job_id)
    }
}
