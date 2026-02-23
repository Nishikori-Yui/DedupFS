use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::config::WorkerConfig;

#[derive(Debug, Clone, Copy)]
pub enum JobKind {
    Scan,
    Hash,
}

impl JobKind {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "scan" => Some(JobKind::Scan),
            "hash" => Some(JobKind::Hash),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobRecord {
    pub id: String,
    pub kind: JobKind,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct ThumbnailTaskRecord {
    pub id: i64,
    pub thumb_key: String,
    pub file_id: i64,
    pub relative_path: String,
    pub root_path: String,
    pub media_type: String,
    pub format: String,
    pub max_dimension: i64,
    pub source_size_bytes: i64,
    pub source_mtime_ns: i64,
    pub output_relpath: String,
    pub error_count: i64,
}

#[derive(Debug, Clone)]
pub struct ThumbnailCleanupRecord {
    pub id: i64,
    pub group_key: String,
}

#[derive(Debug, Clone, Copy)]
pub enum WalCheckpointMode {
    Passive,
    Restart,
    Truncate,
}

impl WalCheckpointMode {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "passive" => Some(WalCheckpointMode::Passive),
            "restart" => Some(WalCheckpointMode::Restart),
            "truncate" => Some(WalCheckpointMode::Truncate),
            _ => None,
        }
    }

    fn as_sql_keyword(self) -> &'static str {
        match self {
            WalCheckpointMode::Passive => "PASSIVE",
            WalCheckpointMode::Restart => "RESTART",
            WalCheckpointMode::Truncate => "TRUNCATE",
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalMaintenanceRecord {
    pub id: i64,
    pub requested_mode: WalCheckpointMode,
    pub retry_count: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct WalCheckpointStats {
    pub busy: i64,
    pub log_frames: i64,
    pub checkpointed_frames: i64,
}

pub fn open_connection(database_path: &Path) -> Result<Connection> {
    if let Some(parent) = database_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create database directory: {}", parent.display())
        })?;
    }

    let conn = Connection::open(database_path)
        .with_context(|| format!("failed to open database: {}", database_path.display()))?;

    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA temp_store=MEMORY;
        PRAGMA foreign_keys=ON;
        ",
    )?;

    Ok(conn)
}

pub fn has_runnable_scan_hash_work(conn: &Connection) -> Result<bool> {
    let exists = conn
        .query_row(
            "
            SELECT 1
            FROM jobs
            WHERE kind IN ('scan', 'hash')
              AND (
                status = 'pending'
                OR (
                    status = 'running'
                    AND (lease_expires_at IS NULL OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP)
                )
              )
            LIMIT 1
            ",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    Ok(exists)
}

pub fn has_runnable_thumbnail_work(conn: &Connection) -> Result<bool> {
    let exists = conn
        .query_row(
            "
            SELECT 1
            FROM thumbnails
            WHERE (
                status = 'pending'
                AND (retry_after IS NULL OR datetime(retry_after) <= CURRENT_TIMESTAMP)
            ) OR (
                status = 'running'
                AND (lease_expires_at IS NULL OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP)
            )
            LIMIT 1
            ",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    Ok(exists)
}

pub fn has_runnable_thumbnail_cleanup_work(conn: &Connection) -> Result<bool> {
    let exists = conn
        .query_row(
            "
            SELECT 1
            FROM thumbnail_cleanup_jobs
            WHERE (
                status = 'pending'
                AND datetime(execute_after) <= CURRENT_TIMESTAMP
            ) OR (
                status = 'running'
                AND (lease_expires_at IS NULL OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP)
            )
            LIMIT 1
            ",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    Ok(exists)
}

pub fn has_runnable_wal_maintenance_work(conn: &Connection) -> Result<bool> {
    let exists = conn
        .query_row(
            "
            SELECT 1
            FROM wal_maintenance_jobs
            WHERE (
                status = 'pending'
                AND datetime(execute_after) <= CURRENT_TIMESTAMP
            ) OR (
                status = 'retryable'
                AND (retry_after IS NULL OR datetime(retry_after) <= CURRENT_TIMESTAMP)
            ) OR (
                status = 'running'
                AND (lease_expires_at IS NULL OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP)
            )
            LIMIT 1
            ",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    Ok(exists)
}

pub fn claim_scan_hash_job(
    conn: &mut Connection,
    config: &WorkerConfig,
    requested_job_id: Option<&str>,
) -> Result<Option<JobRecord>> {
    let tx = conn.transaction()?;
    tx.execute(
        "
        UPDATE jobs
        SET status = 'retryable',
            worker_id = NULL,
            worker_heartbeat_at = NULL,
            lease_expires_at = NULL,
            error_code = CASE
                WHEN error_code IS NULL OR trim(error_code) = ''
                THEN 'LEASE_EXPIRED'
                ELSE error_code
            END,
            error_message = CASE
                WHEN error_message IS NULL OR trim(error_message) = ''
                THEN 'Lease expired and recovered by rust worker claim path'
                ELSE error_message
            END,
            finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP),
            updated_at = CURRENT_TIMESTAMP
        WHERE status = 'running'
          AND kind IN ('scan', 'hash')
          AND (lease_expires_at IS NULL OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP)
        ",
        [],
    )?;

    let target_id = if let Some(job_id) = requested_job_id {
        tx.query_row(
            "SELECT id FROM jobs WHERE id = ?1 AND status = 'pending' AND kind IN ('scan', 'hash')",
            params![job_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    } else {
        tx.query_row(
            "SELECT id FROM jobs WHERE status = 'pending' AND kind IN ('scan', 'hash') ORDER BY created_at ASC LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    };

    let Some(job_id) = target_id else {
        tx.commit()?;
        return Ok(None);
    };

    let lease_modifier = format!("+{} seconds", config.job_lock_ttl_seconds);
    let updated = tx.execute(
        "
        UPDATE jobs
        SET status = 'running',
            worker_id = ?1,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = datetime('now', ?2),
            started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?3
          AND status = 'pending'
          AND kind IN ('scan', 'hash')
        ",
        params![config.worker_id, lease_modifier, job_id],
    )?;

    if updated != 1 {
        tx.commit()?;
        return Ok(None);
    }

    let row = tx
        .query_row(
            "SELECT id, kind, COALESCE(payload, '{}') FROM jobs WHERE id = ?1",
            params![job_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;

    tx.commit()?;

    let Some((id, kind_raw, payload_raw)) = row else {
        return Ok(None);
    };

    let kind =
        JobKind::parse(&kind_raw).ok_or_else(|| anyhow!("unsupported job kind: {kind_raw}"))?;
    let payload =
        serde_json::from_str::<Value>(&payload_raw).unwrap_or(Value::Object(Default::default()));
    Ok(Some(JobRecord { id, kind, payload }))
}

pub fn refresh_job_lease(
    conn: &Connection,
    config: &WorkerConfig,
    job_id: &str,
    processed_items: i64,
    progress: f64,
) -> Result<()> {
    let lease_modifier = format!("+{} seconds", config.job_lock_ttl_seconds);
    let updated = conn.execute(
        "
        UPDATE jobs
        SET processed_items = ?1,
            progress = ?2,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = datetime('now', ?3),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?4
          AND status = 'running'
          AND kind IN ('scan', 'hash')
          AND worker_id = ?5
          AND datetime(lease_expires_at) > CURRENT_TIMESTAMP
        ",
        params![
            processed_items,
            progress,
            lease_modifier,
            job_id,
            config.worker_id
        ],
    )?;

    if updated != 1 {
        bail!("job {job_id} lease update rejected");
    }

    Ok(())
}

pub fn finish_job(
    conn: &mut Connection,
    config: &WorkerConfig,
    job_id: &str,
    success: bool,
    error_message: Option<&str>,
) -> Result<()> {
    let status = if success { "completed" } else { "failed" };
    let error_code = if success {
        None
    } else {
        Some("WORKER_FAILURE")
    };
    let tx = conn.transaction()?;

    let updated = tx.execute(
        "
        UPDATE jobs
        SET status = ?1,
            progress = CASE WHEN ?1 = 'completed' THEN 1.0 ELSE progress END,
            error_code = ?2,
            error_message = ?3,
            finished_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = NULL
        WHERE id = ?4
          AND status = 'running'
          AND kind IN ('scan', 'hash')
          AND worker_id = ?5
        ",
        params![status, error_code, error_message, job_id, config.worker_id],
    )?;

    if updated != 1 {
        bail!("failed to finish running job {job_id}");
    }

    tx.commit()?;
    Ok(())
}

pub fn claim_thumbnail_task(
    conn: &mut Connection,
    config: &WorkerConfig,
) -> Result<Option<ThumbnailTaskRecord>> {
    let tx = conn.transaction()?;
    tx.execute(
        "
        UPDATE thumbnails
        SET status = 'pending',
            worker_id = NULL,
            worker_heartbeat_at = NULL,
            lease_expires_at = NULL,
            error_code = CASE
                WHEN error_code IS NULL OR trim(error_code) = ''
                THEN 'LEASE_EXPIRED'
                ELSE error_code
            END,
            error_message = CASE
                WHEN error_message IS NULL OR trim(error_message) = ''
                THEN 'Lease expired and requeued by rust worker claim path'
                ELSE error_message
            END,
            updated_at = CURRENT_TIMESTAMP
        WHERE status = 'running'
          AND (lease_expires_at IS NULL OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP)
        ",
        [],
    )?;

    let candidate = tx
        .query_row(
            "
            SELECT t.id
            FROM thumbnails t
            WHERE t.status = 'pending'
              AND (t.retry_after IS NULL OR datetime(t.retry_after) <= CURRENT_TIMESTAMP)
              AND (
                (
                  t.media_type = 'image' AND (
                    SELECT COUNT(1)
                    FROM thumbnails r
                    WHERE r.status = 'running'
                      AND r.media_type = 'image'
                      AND datetime(r.lease_expires_at) > CURRENT_TIMESTAMP
                  ) < ?1
                )
                OR
                (
                  t.media_type = 'video' AND (
                    SELECT COUNT(1)
                    FROM thumbnails r
                    WHERE r.status = 'running'
                      AND r.media_type = 'video'
                      AND datetime(r.lease_expires_at) > CURRENT_TIMESTAMP
                  ) < ?2
                )
              )
            ORDER BY t.created_at ASC, t.id ASC
            LIMIT 1
            ",
            params![
                config.thumbnail_image_concurrency as i64,
                config.thumbnail_video_concurrency as i64
            ],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;

    let Some(task_id) = candidate else {
        tx.commit()?;
        return Ok(None);
    };

    let lease_modifier = format!("+{} seconds", config.job_lock_ttl_seconds);
    let claimed = tx.execute(
        "
        UPDATE thumbnails
        SET status = 'running',
            worker_id = ?1,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = datetime('now', ?2),
            started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?3
          AND status = 'pending'
        ",
        params![config.worker_id, lease_modifier, task_id],
    )?;

    if claimed != 1 {
        tx.commit()?;
        return Ok(None);
    }

    let row = tx
        .query_row(
            "
            SELECT
                t.id,
                t.thumb_key,
                t.file_id,
                f.relative_path,
                r.root_path,
                t.media_type,
                t.format,
                t.max_dimension,
                t.source_size_bytes,
                t.source_mtime_ns,
                COALESCE(t.output_relpath, ''),
                COALESCE(t.error_count, 0)
            FROM thumbnails t
            JOIN library_files f ON f.id = t.file_id
            JOIN library_roots r ON r.id = f.library_id
            WHERE t.id = ?1
            ",
            params![task_id],
            |row| {
                Ok(ThumbnailTaskRecord {
                    id: row.get::<_, i64>(0)?,
                    thumb_key: row.get::<_, String>(1)?,
                    file_id: row.get::<_, i64>(2)?,
                    relative_path: row.get::<_, String>(3)?,
                    root_path: row.get::<_, String>(4)?,
                    media_type: row.get::<_, String>(5)?,
                    format: row.get::<_, String>(6)?,
                    max_dimension: row.get::<_, i64>(7)?,
                    source_size_bytes: row.get::<_, i64>(8)?,
                    source_mtime_ns: row.get::<_, i64>(9)?,
                    output_relpath: row.get::<_, String>(10)?,
                    error_count: row.get::<_, i64>(11)?,
                })
            },
        )
        .optional()?;

    tx.commit()?;
    Ok(row)
}

pub fn refresh_thumbnail_lease(
    conn: &Connection,
    config: &WorkerConfig,
    task_id: i64,
) -> Result<()> {
    let lease_modifier = format!("+{} seconds", config.job_lock_ttl_seconds);
    let updated = conn.execute(
        "
        UPDATE thumbnails
        SET worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = datetime('now', ?1),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?2
          AND status = 'running'
          AND worker_id = ?3
          AND datetime(lease_expires_at) > CURRENT_TIMESTAMP
        ",
        params![lease_modifier, task_id, config.worker_id],
    )?;

    if updated != 1 {
        bail!("thumbnail task {task_id} lease update rejected");
    }

    Ok(())
}

pub fn finish_thumbnail_success(
    conn: &mut Connection,
    config: &WorkerConfig,
    task_id: i64,
    width: i64,
    height: i64,
    bytes_size: i64,
) -> Result<()> {
    let tx = conn.transaction()?;
    let updated = tx.execute(
        "
        UPDATE thumbnails
        SET status = 'ready',
            width = ?1,
            height = ?2,
            bytes_size = ?3,
            error_code = NULL,
            error_message = NULL,
            error_count = 0,
            retry_after = NULL,
            finished_at = CURRENT_TIMESTAMP,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?4
          AND status = 'running'
          AND worker_id = ?5
        ",
        params![width, height, bytes_size, task_id, config.worker_id],
    )?;

    if updated != 1 {
        bail!("failed to finish thumbnail task {task_id}");
    }

    tx.commit()?;
    Ok(())
}

pub fn finish_thumbnail_failure(
    conn: &mut Connection,
    config: &WorkerConfig,
    task_id: i64,
    previous_error_count: i64,
    error_code: &str,
    error_message: &str,
) -> Result<()> {
    let next_error_count = previous_error_count.saturating_add(1);
    let retry_seconds = calculate_retry_delay_seconds(
        config.thumbnail_retry_base_seconds,
        config.thumbnail_retry_max_seconds,
        next_error_count as u64,
    );
    let retry_modifier = format!("+{} seconds", retry_seconds);

    let tx = conn.transaction()?;
    let updated = tx.execute(
        "
        UPDATE thumbnails
        SET status = 'failed',
            error_count = ?1,
            error_code = ?2,
            error_message = ?3,
            retry_after = datetime('now', ?4),
            finished_at = CURRENT_TIMESTAMP,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?5
          AND status = 'running'
          AND worker_id = ?6
        ",
        params![
            next_error_count,
            error_code,
            error_message,
            retry_modifier,
            task_id,
            config.worker_id
        ],
    )?;

    if updated != 1 {
        bail!("failed to mark thumbnail task {task_id} as failed");
    }

    tx.commit()?;
    Ok(())
}

pub fn claim_thumbnail_cleanup_job(
    conn: &mut Connection,
    config: &WorkerConfig,
) -> Result<Option<ThumbnailCleanupRecord>> {
    let tx = conn.transaction()?;
    tx.execute(
        "
        UPDATE thumbnail_cleanup_jobs
        SET status = 'pending',
            worker_id = NULL,
            worker_heartbeat_at = NULL,
            lease_expires_at = NULL,
            error_code = CASE
                WHEN error_code IS NULL OR trim(error_code) = ''
                THEN 'LEASE_EXPIRED'
                ELSE error_code
            END,
            error_message = CASE
                WHEN error_message IS NULL OR trim(error_message) = ''
                THEN 'Lease expired and requeued by rust worker claim path'
                ELSE error_message
            END,
            finished_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE status = 'running'
          AND (lease_expires_at IS NULL OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP)
        ",
        [],
    )?;

    let candidate = tx
        .query_row(
            "
            SELECT id, group_key
            FROM thumbnail_cleanup_jobs c
            WHERE c.status = 'pending'
              AND datetime(c.execute_after) <= CURRENT_TIMESTAMP
              AND NOT EXISTS (
                SELECT 1
                FROM thumbnails t
                WHERE t.group_key = c.group_key
                  AND t.status IN ('pending', 'running')
              )
            ORDER BY c.execute_after ASC, c.id ASC
            LIMIT 1
            ",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;

    let Some((job_id, group_key)) = candidate else {
        tx.commit()?;
        return Ok(None);
    };

    let lease_modifier = format!("+{} seconds", config.job_lock_ttl_seconds);
    let claimed = tx.execute(
        "
        UPDATE thumbnail_cleanup_jobs
        SET status = 'running',
            worker_id = ?1,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = datetime('now', ?2),
            updated_at = CURRENT_TIMESTAMP,
            finished_at = NULL
        WHERE id = ?3
          AND status = 'pending'
        ",
        params![config.worker_id, lease_modifier, job_id],
    )?;

    if claimed != 1 {
        tx.commit()?;
        return Ok(None);
    }

    tx.commit()?;
    Ok(Some(ThumbnailCleanupRecord {
        id: job_id,
        group_key,
    }))
}

pub fn claim_wal_maintenance_job(
    conn: &mut Connection,
    config: &WorkerConfig,
) -> Result<Option<WalMaintenanceRecord>> {
    let tx = conn.transaction()?;
    let retry_modifier = format!("+{} seconds", config.wal_checkpoint_retry_seconds);
    tx.execute(
        "
        UPDATE wal_maintenance_jobs
        SET status = 'retryable',
            retry_count = COALESCE(retry_count, 0) + 1,
            retry_after = datetime('now', ?1),
            worker_id = NULL,
            worker_heartbeat_at = NULL,
            lease_expires_at = NULL,
            error_code = CASE
                WHEN error_code IS NULL OR trim(error_code) = ''
                THEN 'LEASE_EXPIRED'
                ELSE error_code
            END,
            error_message = CASE
                WHEN error_message IS NULL OR trim(error_message) = ''
                THEN 'Lease expired and requeued by rust worker claim path'
                ELSE error_message
            END,
            finished_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE status = 'running'
          AND (lease_expires_at IS NULL OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP)
        ",
        params![retry_modifier],
    )?;

    let candidate = tx
        .query_row(
            "
            SELECT id, requested_mode, COALESCE(retry_count, 0)
            FROM wal_maintenance_jobs
            WHERE (
                status = 'pending'
                AND datetime(execute_after) <= CURRENT_TIMESTAMP
            ) OR (
                status = 'retryable'
                AND (retry_after IS NULL OR datetime(retry_after) <= CURRENT_TIMESTAMP)
            )
            ORDER BY COALESCE(retry_after, execute_after) ASC, id ASC
            LIMIT 1
            ",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?;

    let Some((job_id, mode_raw, retry_count)) = candidate else {
        tx.commit()?;
        return Ok(None);
    };

    let lease_modifier = format!("+{} seconds", config.job_lock_ttl_seconds);
    let claimed = tx.execute(
        "
        UPDATE wal_maintenance_jobs
        SET status = 'running',
            worker_id = ?1,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = datetime('now', ?2),
            started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
            updated_at = CURRENT_TIMESTAMP,
            finished_at = NULL
        WHERE id = ?3
          AND status IN ('pending', 'retryable')
        ",
        params![config.worker_id, lease_modifier, job_id],
    )?;

    if claimed != 1 {
        tx.commit()?;
        return Ok(None);
    }

    tx.commit()?;
    let requested_mode = WalCheckpointMode::parse(&mode_raw)
        .ok_or_else(|| anyhow!("unsupported wal checkpoint mode: {mode_raw}"))?;
    Ok(Some(WalMaintenanceRecord {
        id: job_id,
        requested_mode,
        retry_count,
    }))
}

pub fn finish_thumbnail_cleanup_job(
    conn: &mut Connection,
    config: &WorkerConfig,
    job_id: i64,
    success: bool,
    error_code: Option<&str>,
    error_message: Option<&str>,
) -> Result<()> {
    let status = if success { "completed" } else { "failed" };
    let tx = conn.transaction()?;
    let updated = tx.execute(
        "
        UPDATE thumbnail_cleanup_jobs
        SET status = ?1,
            error_code = ?2,
            error_message = ?3,
            finished_at = CURRENT_TIMESTAMP,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?4
          AND status = 'running'
          AND worker_id = ?5
        ",
        params![status, error_code, error_message, job_id, config.worker_id],
    )?;

    if updated != 1 {
        bail!("failed to finish thumbnail cleanup job {job_id}");
    }

    tx.commit()?;
    Ok(())
}

pub fn refresh_thumbnail_cleanup_lease(
    conn: &Connection,
    config: &WorkerConfig,
    job_id: i64,
) -> Result<()> {
    let lease_modifier = format!("+{} seconds", config.job_lock_ttl_seconds);
    let updated = conn.execute(
        "
        UPDATE thumbnail_cleanup_jobs
        SET worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = datetime('now', ?1),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?2
          AND status = 'running'
          AND worker_id = ?3
          AND datetime(lease_expires_at) > CURRENT_TIMESTAMP
        ",
        params![lease_modifier, job_id, config.worker_id],
    )?;

    if updated != 1 {
        bail!("thumbnail cleanup job {job_id} lease update rejected");
    }
    Ok(())
}

pub fn execute_wal_checkpoint(
    conn: &Connection,
    mode: WalCheckpointMode,
) -> Result<WalCheckpointStats> {
    let sql = format!("PRAGMA wal_checkpoint({})", mode.as_sql_keyword());
    let stats = conn.query_row(&sql, [], |row| {
        Ok(WalCheckpointStats {
            busy: row.get::<_, i64>(0)?,
            log_frames: row.get::<_, i64>(1)?,
            checkpointed_frames: row.get::<_, i64>(2)?,
        })
    })?;
    Ok(stats)
}

pub fn finish_wal_maintenance_success(
    conn: &mut Connection,
    config: &WorkerConfig,
    job_id: i64,
    stats: WalCheckpointStats,
) -> Result<()> {
    let tx = conn.transaction()?;
    let updated = tx.execute(
        "
        UPDATE wal_maintenance_jobs
        SET status = 'completed',
            checkpoint_busy = ?1,
            checkpoint_log_frames = ?2,
            checkpointed_frames = ?3,
            error_code = NULL,
            error_message = NULL,
            finished_at = CURRENT_TIMESTAMP,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?4
          AND status = 'running'
          AND worker_id = ?5
        ",
        params![
            stats.busy,
            stats.log_frames,
            stats.checkpointed_frames,
            job_id,
            config.worker_id
        ],
    )?;

    if updated != 1 {
        bail!("failed to finish wal maintenance job {job_id}");
    }
    tx.commit()?;
    Ok(())
}

pub fn requeue_wal_maintenance_retry(
    conn: &mut Connection,
    config: &WorkerConfig,
    job_id: i64,
    previous_retry_count: i64,
    error_code: &str,
    error_message: &str,
    stats: WalCheckpointStats,
) -> Result<()> {
    let tx = conn.transaction()?;
    let next_retry_count = previous_retry_count.saturating_add(1);
    let retry_modifier = format!("+{} seconds", config.wal_checkpoint_retry_seconds);
    let updated = tx.execute(
        "
        UPDATE wal_maintenance_jobs
        SET status = 'retryable',
            retry_count = ?1,
            retry_after = datetime('now', ?2),
            checkpoint_busy = ?3,
            checkpoint_log_frames = ?4,
            checkpointed_frames = ?5,
            error_code = ?6,
            error_message = ?7,
            finished_at = NULL,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?8
          AND status = 'running'
          AND worker_id = ?9
        ",
        params![
            next_retry_count,
            retry_modifier,
            stats.busy,
            stats.log_frames,
            stats.checkpointed_frames,
            error_code,
            error_message,
            job_id,
            config.worker_id
        ],
    )?;

    if updated != 1 {
        bail!("failed to requeue wal maintenance job {job_id}");
    }
    tx.commit()?;
    Ok(())
}

pub fn finish_wal_maintenance_failure(
    conn: &mut Connection,
    config: &WorkerConfig,
    job_id: i64,
    error_code: &str,
    error_message: &str,
) -> Result<()> {
    let tx = conn.transaction()?;
    let updated = tx.execute(
        "
        UPDATE wal_maintenance_jobs
        SET status = 'failed',
            error_code = ?1,
            error_message = ?2,
            finished_at = CURRENT_TIMESTAMP,
            worker_heartbeat_at = CURRENT_TIMESTAMP,
            lease_expires_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?3
          AND status = 'running'
          AND worker_id = ?4
        ",
        params![error_code, error_message, job_id, config.worker_id],
    )?;
    if updated != 1 {
        bail!("failed to mark wal maintenance job {job_id} as failed");
    }
    tx.commit()?;
    Ok(())
}

pub fn list_group_thumbnail_outputs(
    conn: &Connection,
    group_key: &str,
) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare(
        "
        SELECT id, COALESCE(output_relpath, '')
        FROM thumbnails
        WHERE group_key = ?1
          AND status IN ('ready', 'failed')
        ORDER BY id ASC
        ",
    )?;

    let rows = stmt.query_map(params![group_key], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;

    let mut outputs = Vec::new();
    for row in rows {
        outputs.push(row?);
    }
    Ok(outputs)
}

pub fn delete_group_thumbnail_rows(conn: &Connection, group_key: &str) -> Result<usize> {
    let deleted = conn.execute(
        "DELETE FROM thumbnails WHERE group_key = ?1 AND status IN ('ready', 'failed')",
        params![group_key],
    )?;
    Ok(deleted)
}

pub fn reserve_global_io_budget(
    conn: &Connection,
    bucket_key: &str,
    bytes: u64,
    mib_per_sec: Option<u64>,
) -> Result<Duration> {
    let Some(limit_mib) = mib_per_sec else {
        return Ok(Duration::ZERO);
    };
    if bytes == 0 {
        return Ok(Duration::ZERO);
    }
    let bytes_per_second = u128::from(limit_mib).saturating_mul(1024 * 1024);
    if bytes_per_second == 0 {
        return Ok(Duration::ZERO);
    }

    conn.execute(
        "
        CREATE TABLE IF NOT EXISTS io_rate_limits (
            bucket_key VARCHAR(64) PRIMARY KEY,
            next_available_at_ms BIGINT NOT NULL DEFAULT 0,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        ",
        [],
    )?;

    conn.execute(
        "
        INSERT INTO io_rate_limits(bucket_key, next_available_at_ms, updated_at)
        VALUES (?1, 0, CURRENT_TIMESTAMP)
        ON CONFLICT(bucket_key) DO NOTHING
        ",
        params![bucket_key],
    )?;

    let bytes_u128 = u128::from(bytes);
    let budget_ms_u128 = bytes_u128
        .saturating_mul(1000)
        .saturating_add(bytes_per_second.saturating_sub(1))
        / bytes_per_second;
    let budget_ms = i64::try_from(budget_ms_u128.max(1)).unwrap_or(i64::MAX / 2);

    let now_ms_u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before UNIX_EPOCH")?
        .as_millis();
    let now_ms = i64::try_from(now_ms_u128).unwrap_or(i64::MAX / 2);

    let new_next_ms = conn.query_row(
        "
        UPDATE io_rate_limits
        SET next_available_at_ms = CASE
                WHEN next_available_at_ms > ?2
                THEN next_available_at_ms + ?3
                ELSE ?2 + ?3
            END,
            updated_at = CURRENT_TIMESTAMP
        WHERE bucket_key = ?1
        RETURNING next_available_at_ms
        ",
        params![bucket_key, now_ms, budget_ms],
        |row| row.get::<_, i64>(0),
    )?;

    let start_ms = new_next_ms.saturating_sub(budget_ms);
    let delay_ms = start_ms.saturating_sub(now_ms).max(0);
    let delay = Duration::from_millis(u64::try_from(delay_ms).unwrap_or(u64::MAX / 2));
    Ok(delay)
}

fn calculate_retry_delay_seconds(base_seconds: u64, max_seconds: u64, error_count: u64) -> u64 {
    let capped_power = error_count.saturating_sub(1).min(10);
    let delay = base_seconds.saturating_mul(1_u64 << capped_power);
    delay.min(max_seconds)
}

#[cfg(test)]
mod tests {
    use super::delete_group_thumbnail_rows;
    use rusqlite::Connection;

    #[test]
    fn cleanup_delete_only_removes_terminal_rows() {
        let conn = Connection::open_in_memory().expect("open sqlite in-memory");
        conn.execute_batch(
            "
            CREATE TABLE thumbnails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_key VARCHAR(256),
                status VARCHAR(16) NOT NULL
            );
            ",
        )
        .expect("create thumbnails table");

        conn.execute(
            "INSERT INTO thumbnails(group_key, status) VALUES ('sha256:g', 'ready')",
            [],
        )
        .expect("insert ready row");
        conn.execute(
            "INSERT INTO thumbnails(group_key, status) VALUES ('sha256:g', 'failed')",
            [],
        )
        .expect("insert failed row");
        conn.execute(
            "INSERT INTO thumbnails(group_key, status) VALUES ('sha256:g', 'running')",
            [],
        )
        .expect("insert running row");
        conn.execute(
            "INSERT INTO thumbnails(group_key, status) VALUES ('sha256:g', 'pending')",
            [],
        )
        .expect("insert pending row");

        let deleted = delete_group_thumbnail_rows(&conn, "sha256:g").expect("delete terminal rows");
        assert_eq!(deleted, 2);

        let running_remaining: i64 = conn
            .query_row(
                "SELECT COUNT(1) FROM thumbnails WHERE group_key = 'sha256:g' AND status = 'running'",
                [],
                |row| row.get(0),
            )
            .expect("count running");
        let pending_remaining: i64 = conn
            .query_row(
                "SELECT COUNT(1) FROM thumbnails WHERE group_key = 'sha256:g' AND status = 'pending'",
                [],
                |row| row.get(0),
            )
            .expect("count pending");
        assert_eq!(running_remaining, 1);
        assert_eq!(pending_remaining, 1);
    }
}
