use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct QueueRow {
    pub s3_bucket: String,
    pub s3_key: String,
    pub source: String,
    pub parser: String,
    pub status: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub last_error: Option<String>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub locked_at: Option<DateTime<Utc>>,
    pub locked_by: Option<String>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub nomad_job_id: Option<String>,
    pub nomad_alloc_id: Option<String>,
}

pub async fn init_pool(database_url: &str) -> Result<PgPool> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
        .context("connecting to postgres")?;
    Ok(pool)
}

pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    for sql in [
        include_str!("../migrations/001_queue.sql"),
        include_str!("../migrations/002_dispatch.sql"),
    ] {
        sqlx::raw_sql(sql).execute(pool).await.context("running migrations")?;
    }
    Ok(())
}

pub async fn enqueue(pool: &PgPool, bucket: &str, key: &str, source: &str, parser: &str) -> Result<bool> {
    let res = sqlx::query(
        "INSERT INTO parse_queue(s3_bucket,s3_key,source,parser) VALUES($1,$2,$3,$4) ON CONFLICT (s3_key) DO NOTHING",
    )
    .bind(bucket)
    .bind(key)
    .bind(source)
    .bind(parser)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn fetch_pending(pool: &PgPool, locked_by: &str) -> Result<Option<QueueRow>> {
    // SKIP LOCKED ensures multiple workers don't contend.
    let row = sqlx::query_as::<_, QueueRow>(
        "UPDATE parse_queue SET status='processing', locked_at=now(), locked_by=$1, attempts=attempts+1, updated_at=now()
         WHERE s3_key = (
           SELECT s3_key FROM parse_queue
           WHERE status IN ('pending','failed') AND (next_retry_at IS NULL OR next_retry_at <= now())
           ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1
         ) RETURNING *",
    )
    .bind(locked_by)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

pub async fn fetch_pending_for_dispatch(pool: &PgPool) -> Result<Option<QueueRow>> {
    let row = sqlx::query_as::<_, QueueRow>(
        "UPDATE parse_queue SET status='dispatched', dispatched_at=now(), updated_at=now(), attempts=attempts+1
         WHERE s3_key = (
           SELECT s3_key FROM parse_queue
           WHERE status IN ('pending','failed') AND (next_retry_at IS NULL OR next_retry_at <= now())
           ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1
         ) RETURNING *",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

pub async fn mark_dispatched(
    pool: &PgPool,
    s3_key: &str,
    nomad_job_id: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE parse_queue SET nomad_job_id=$1, updated_at=now() WHERE s3_key=$2",
    )
    .bind(nomad_job_id)
    .bind(s3_key)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_failed(pool: &PgPool, s3_key: &str, error: &str, max_attempts: i32, attempts: i32) -> Result<()> {
    let backoff_secs = (5u64.saturating_mul(1u64 << attempts.min(10))).min(3600);
    let jitter: u64 = rand::random::<u64>() % backoff_secs.max(1);
    let next_retry = if attempts >= max_attempts {
        None
    } else {
        Some(chrono::Duration::seconds((backoff_secs + jitter) as i64))
    };
    if attempts >= max_attempts {
        sqlx::query(
            "UPDATE parse_queue SET status='dead_letter', last_error=$1, locked_at=NULL, locked_by=NULL, dispatched_at=NULL, nomad_job_id=NULL, nomad_alloc_id=NULL, updated_at=now(), next_retry_at=NULL WHERE s3_key=$2",
        )
        .bind(error)
        .bind(s3_key)
        .execute(pool)
        .await?;
    } else {
        let next_at = Utc::now() + next_retry.unwrap();
        sqlx::query(
            "UPDATE parse_queue SET status='failed', last_error=$1, locked_at=NULL, locked_by=NULL, dispatched_at=NULL, nomad_job_id=NULL, nomad_alloc_id=NULL, updated_at=now(), next_retry_at=$2 WHERE s3_key=$3",
        )
        .bind(error)
        .bind(next_at)
        .bind(s3_key)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn archive_success(
    pool: &PgPool,
    row: &QueueRow,
    duration_ms: i64,
    stats: &ArchiveStats,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM parse_queue WHERE s3_key=$1")
        .bind(&row.s3_key)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO parse_history(s3_bucket,s3_key,source,parser,attempts,duration_ms,rows_in,positions_out,statics_out,meteo_out,binary_out,atons_out,other_out,incomplete,unparsed,deduped,created_at)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
         ON CONFLICT (s3_key) DO NOTHING",
    )
    .bind(&row.s3_bucket)
    .bind(&row.s3_key)
    .bind(&row.source)
    .bind(&row.parser)
    .bind(row.attempts)
    .bind(duration_ms as i32)
    .bind(stats.rows_in)
    .bind(stats.positions_out)
    .bind(stats.statics_out)
    .bind(stats.meteo_out)
    .bind(stats.binary_out)
    .bind(stats.atons_out)
    .bind(stats.other_out)
    .bind(stats.incomplete)
    .bind(stats.unparsed)
    .bind(stats.deduped)
    .bind(row.created_at)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct ArchiveStats {
    pub rows_in: i64,
    pub positions_out: i64,
    pub statics_out: i64,
    pub meteo_out: i64,
    pub binary_out: i64,
    pub atons_out: i64,
    pub other_out: i64,
    pub incomplete: i64,
    pub unparsed: i64,
    pub deduped: i64,
}

pub async fn reclaim_stale(pool: &PgPool, lease_secs: i64) -> Result<u64> {
    let res = sqlx::query(
        "UPDATE parse_queue SET status='pending', locked_at=NULL, locked_by=NULL, updated_at=now()
         WHERE status='processing' AND locked_at < now() - make_interval(secs => $1)",
    )
    .bind(lease_secs as f64)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

pub async fn reclaim_stale_dispatched(pool: &PgPool, lease_secs: i64) -> Result<u64> {
    let res = sqlx::query(
        "UPDATE parse_queue SET status='pending', dispatched_at=NULL, nomad_job_id=NULL, nomad_alloc_id=NULL, updated_at=now()
         WHERE status='dispatched' AND dispatched_at < now() - make_interval(secs => $1)",
    )
    .bind(lease_secs as f64)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

pub async fn queue_depth(pool: &PgPool) -> Result<Vec<(String, i64)>> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT status, count(*)::bigint FROM parse_queue GROUP BY status",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
